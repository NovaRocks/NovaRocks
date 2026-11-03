// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Actual checked Scan/Partial/Grouped producer integration. The original
//! aggregate fixture authors every state call; the session only records the
//! original public port requests and canonical contributions. This component
//! does not attest a native process or execute the future ordered scan lane.
use super::*;
use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use crate::exec::pipeline::operator::InputConstructionFactory;
use crate::exec::pipeline::test_driver::TestDriver;
use crate::runtime_filter as rf;
use novarocks_functions::KernelFailure;
use novarocks_physical_plan::*;
use std::sync::Mutex;

pub(super) const FILTER: RuntimeFilterId = RuntimeFilterId::new(31);
const WITNESS: RuntimeFilterWitnessId = RuntimeFilterWitnessId::new(51);

pub(super) fn physical_filter(
    fragment: FragmentId,
    aggregate: NodeId,
    scan: NodeId,
    topn: NodeId,
    phase: TopNPhase,
    value: ValueId,
    ty: ValueType,
    direction: SortDirection,
    null_ordering: NullOrdering,
    limit: u64,
) -> RuntimeFilter {
    let bound = RuntimeFilterCoverage {
        nodes: Box::from([
            RuntimeFilterCoverageNode::Witness(WITNESS),
            RuntimeFilterCoverageNode::AllOf {
                children: Box::from([0]),
            },
        ]),
        root: 1,
    };
    RuntimeFilter {
        id: FILTER,
        kind: RuntimeFilterKind::MinMax,
        domain: RuntimeFilterDomain::Ordered {
            key: RuntimeFilterOrderKey {
                ty,
                direction,
                null_ordering,
            },
            inclusive: true,
            comparator: novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
        },
        lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
        reduction: RuntimeFilterReduction::TightenOrderedBound,
        availability_coverage: bound.clone(),
        terminal_coverage: bound,
        equality_witnesses: Box::default(),
        producers: Box::from([RuntimeFilterProducer {
            witness: WITNESS,
            endpoint: RuntimeFilterEndpoint {
                fragment,
                node: aggregate,
                values: Box::from([value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
            contribution_kinds: Box::from([
                RuntimeFilterContributionKind::OrderedBoundUpdate,
                RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::ProducerClosed,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::default(),
                non_build_edges: Box::default(),
            },
            target: RuntimeFilterProducerTarget::AggregateTopNKey {
                group_key_ordinal: 0,
                topn,
                phase,
                order_key_ordinal: 0,
                limit,
                offset: 0,
                direction,
                null_ordering,
            },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment,
                node: scan,
                values: Box::from([value]),
            },
            apply_point: RuntimeFilterApplyPoint::ScanSource,
            capabilities: Box::from([RuntimeFilterArtifactCapability::OrderedRange]),
            activation: RuntimeFilterConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            target: RuntimeFilterConsumerTarget::AggregateTopNScanField {
                producer: WITNESS,
                lineage: Box::default(),
            },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    }
}

fn ordered_fixture() -> Fixture {
    ordered_fixture_with_residual(None)
}
fn ordered_fixture_with_residual(residual: Option<ScanResidualAuthor>) -> Fixture {
    ordered_fixture_with_parameters(
        residual,
        novarocks_type_contract::SemanticParameters::try_new([]).unwrap(),
    )
}
fn ordered_fixture_with_parameters(
    residual: Option<ScanResidualAuthor>,
    parameters: novarocks_type_contract::SemanticParameters,
) -> Fixture {
    let key =
        |value: Option<&str>| value.map_or(LiteralValue::Null, |v| LiteralValue::Utf8(v.into()));
    let rows = [
        (Some("c"), 30),
        (Some("a"), 10),
        (Some("c"), 3),
        (None, 7),
        (Some("b"), 20),
        (None, 2),
        (Some("d"), 40),
        (Some("a"), 1),
    ]
    .into_iter()
    .map(|(k, v)| vec![key(k), LiteralValue::Int64(v)])
    .collect::<Vec<_>>();
    fixture_with_source_parameters(
        &[FunctionValueType::new(DataType::Utf8, true)],
        &rows,
        &[(0, true, true)],
        2,
        false,
        false,
        true,
        residual,
        parameters,
    )
}

#[derive(Debug)]
enum Event {
    Submit(u32, u64, rf::RuntimeFilterContribution),
    Close(u32, u64),
    Fail(rf::RuntimeFilterProducerFailure),
}
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<Event>>,
}
impl rf::RuntimeFilterProducer for Recorder {
    fn max_contribution_bytes(&self) -> usize {
        1024
    }
    fn submit(
        &self,
        partition: rf::PartitionId,
        sequence: rf::ProducerSequence,
        contribution: rf::RuntimeFilterContribution,
    ) -> Result<rf::RuntimeFilterSubmitOutcome, rf::RuntimeFilterContractViolation> {
        self.events.lock().unwrap().push(Event::Submit(
            partition.get(),
            sequence.get(),
            contribution,
        ));
        Ok(rf::RuntimeFilterSubmitOutcome::Applied)
    }
    fn close_partition(
        &self,
        partition: rf::PartitionId,
        terminal: rf::ProducerSequence,
    ) -> Result<rf::RuntimeFilterSubmitOutcome, rf::RuntimeFilterContractViolation> {
        self.events
            .lock()
            .unwrap()
            .push(Event::Close(partition.get(), terminal.get()));
        Ok(rf::RuntimeFilterSubmitOutcome::Applied)
    }
    fn fail(
        &self,
        reason: rf::RuntimeFilterProducerFailure,
    ) -> Result<rf::RuntimeFilterSubmitOutcome, rf::RuntimeFilterContractViolation> {
        self.events.lock().unwrap().push(Event::Fail(reason));
        Ok(rf::RuntimeFilterSubmitOutcome::Applied)
    }
}
struct Session {
    expected: rf::RuntimeFilterProducerContract,
    opened: Mutex<Vec<rf::RuntimeFilterProducerOpenRequest>>,
    recorder: Arc<Recorder>,
}
fn refused() -> rf::RuntimeFilterContractViolation {
    rf::RuntimeFilterContractViolation::new(
        rf::RuntimeFilterContractViolationKind::UnauthorizedBinding,
        "the producer-only fixture has no such endpoint",
    )
}
impl rf::RuntimeFilterSession for Session {
    fn open_producer(
        &self,
        request: rf::RuntimeFilterProducerOpenRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::SharedRuntimeFilterProducer>,
        rf::RuntimeFilterContractViolation,
    > {
        if request.contract() != &self.expected {
            return Err(refused());
        }
        self.opened.lock().unwrap().push(request);
        Ok(rf::RuntimeFilterBindOutcome::Bound(self.recorder.clone()))
    }
    fn subscribe(
        &self,
        _: rf::RuntimeFilterSubscriptionRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::RuntimeFilterSubscriptionPin>,
        rf::RuntimeFilterContractViolation,
    > {
        Err(refused())
    }
    fn open_final_domain_completion(
        &self,
        _: rf::RuntimeFilterFinalDomainOpenRequest,
    ) -> Result<
        rf::RuntimeFilterBindOutcome<rf::SharedRuntimeFilterFinalDomainCompletion>,
        rf::RuntimeFilterContractViolation,
    > {
        Err(refused())
    }
}
fn session(f: &Fixture) -> Arc<Session> {
    session_for(&f.program, f.partial)
}
fn session_for(program: &LocalProgram, producer: ProgramNodeId) -> Arc<Session> {
    let ProgramNodeKind::Aggregate { topn_filters, .. } =
        program.graph().nodes()[producer.index()].kind()
    else {
        unreachable!()
    };
    let spec = &topn_filters[0];
    let novarocks_local_program::StaticFilterContract::Ordered {
        keys,
        comparator_digest,
        contract_digest,
    } = spec.producer.contract()
    else {
        unreachable!()
    };
    let order = rf::contribution::RuntimeOrderContract::from_fragment_contract(
        [rf::contribution::RuntimeOrderKey::with_value_type(
            keys[0].value_type.clone(),
            rf::contribution::RuntimeOrderSortDirection::Ascending,
            rf::contribution::RuntimeOrderNullOrder::First,
        )],
        *comparator_digest,
        *contract_digest,
    )
    .unwrap();
    Arc::new(Session {
        expected: rf::RuntimeFilterProducerContract::ordered_bound(
            rf::RuntimeFilterBindingId::new(spec.producer.binding_id()),
            rf::RuntimeFilterChannelId::new(spec.producer.channel_id()),
            rf::RuntimeFilterExecutionContract::Ordered(Arc::new(order).into()),
        )
        .unwrap(),
        opened: Mutex::new(vec![]),
        recorder: Arc::new(Recorder::default()),
    })
}
fn state(tracker: Arc<MemTracker>) -> RuntimeState {
    RuntimeState::for_local_execution(
        None,
        None,
        None,
        None,
        None,
        Some(tracker),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    )
}
fn construction(
    tracker: &Arc<MemTracker>,
    state: &RuntimeState,
) -> Arc<dyn InputConstructionFactory> {
    super::super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        state.error_state(),
    )
}
fn initialize(
    operator: &mut dyn Operator,
    tracker: &Arc<MemTracker>,
    creation: &Arc<dyn InputConstructionFactory>,
    state: &RuntimeState,
) {
    operator.set_mem_tracker(tracker.clone());
    operator.set_construction_factory(creation.clone());
    operator.prepare().unwrap();
    operator.bind_runtime_state(state).unwrap();
}
fn slice(
    input: &Chunk,
    offset: usize,
    rows: usize,
    creation: &dyn InputConstructionFactory,
) -> Chunk {
    let owner = creation.begin_input_construction(input).unwrap();
    input
        .slice_typed_observed::<crate::exec::pipeline::operator::ConstructionFailure, _>(
            offset,
            rows,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap()
}
fn drain<O: Operator + ?Sized>(
    operator: &mut TestDriver<'_, O>,
    _state: &RuntimeState,
) -> Vec<Chunk> {
    operator.signal_eos().unwrap();
    let output = operator.collect_until_finished().unwrap();
    assert!(operator.drive().unwrap().output.is_none());
    output
}

#[test]
fn compiled_ordered_partial_uses_checked_scan_and_preserves_all_group_contributions() {
    let mut f = ordered_fixture();
    let tracker = MemTracker::new_root("compiled-ordered-partial-real-input");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    f.input.try_transfer_to(&tracker).unwrap();
    let state = state(tracker.clone());
    let creation = construction(&tracker, &state);
    let session = session(&f);
    let host = rf::OrderedTopNAllocationHost::new(tracker.clone());
    let mut control = RuntimeKernelControl::new(state.error_state());
    control.bind_mem_tracker(tracker.clone());
    let factory = CompiledAggregateProcessorFactory::try_new_ordered(
        f.program.clone(),
        f.partial,
        state.error_state(),
        session.clone(),
        2,
        &host,
        &control,
    )
    .unwrap();
    let mut partials = vec![];
    // Both declared local partitions actually run the same legal eight-row
    // input. The independent final oracle therefore includes both contributions.
    for driver in 0..2 {
        let mut operator = factory.create(2, driver);
        initialize(operator.as_mut(), &tracker, &creation, &state);
        let mut processor = TestDriver::new(operator.as_mut(), &state);
        for (offset, rows) in [(0, 3), (3, 3), (6, 2)] {
            processor
                .input_without_output(slice(&f.input, offset, rows, creation.as_ref()))
                .unwrap();
        }
        let output = drain(&mut processor, &state);
        assert_eq!(
            output.iter().map(Chunk::len).sum::<usize>(),
            5,
            "the producer boundary does not discard any actual aggregate group"
        );
        partials.extend(output);
    }
    {
        let requests = session.opened.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|r| r.contract() == &session.expected && r.local_partition_count() == 2)
        );
        let events = session.recorder.events.lock().unwrap();
        for partition in 0..2 {
            let submitted = events
                .iter()
                .filter_map(|event| match event {
                    Event::Submit(p, sequence, value) if *p == partition => {
                        Some((*sequence, value))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(submitted.len(), 1);
            assert_eq!(submitted[0].0, 0);
            let rf::RuntimeFilterExecutionContract::Ordered(order) = session.expected.contract()
            else {
                unreachable!()
            };
            let decoded = rf::contribution::decode_contribution(
                submitted[0].1.canonical_bytes(),
                &order.digest(),
                rf::contribution::ContributionCodecExpectation::OrderedBound(order),
                1024,
            )
            .unwrap();
            let rf::contribution::RuntimeFilterContribution::OrderedBound(bound) = decoded else {
                panic!("actual bound update")
            };
            assert_eq!(
                bound.bound(),
                &rf::contribution::OrderedTuple::new([Some(
                    rf::contribution::OrderedScalar::Utf8("a".into())
                )])
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|e| matches!(e, Event::Close(p,1) if *p == partition))
                    .count(),
                1
            );
        }
        assert!(!events.iter().any(|e| matches!(e, Event::Fail(_))));
    }
    let grouped_factory = CompiledGroupedTopNProcessorFactory::try_new(
        f.program.clone(),
        f.grouped,
        state.error_state(),
    )
    .unwrap();
    let mut grouped_operator = grouped_factory.processor();
    initialize(&mut grouped_operator, &tracker, &creation, &state);
    let mut grouped = TestDriver::new(&mut grouped_operator, &state);
    for chunk in partials {
        grouped.input_without_output(chunk).unwrap();
    }
    let intermediate = drain(&mut grouped, &state);
    let final_factory = CompiledAggregateProcessorFactory::try_new(
        f.program.clone(),
        f.final_node,
        state.error_state(),
    )
    .unwrap();
    let mut final_operator = final_factory.create(1, 0);
    initialize(final_operator.as_mut(), &tracker, &creation, &state);
    let mut final_processor = TestDriver::new(final_operator.as_mut(), &state);
    for chunk in intermediate {
        final_processor.input_without_output(chunk).unwrap();
    }
    let output = drain(&mut final_processor, &state);
    let mut actual = vec![];
    for chunk in &output {
        let keys = chunk.columns()[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let numbers = chunk.columns()[1..]
            .iter()
            .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect::<Vec<_>>();
        for row in 0..chunk.len() {
            actual.push((
                (!keys.is_null(row)).then(|| keys.value(row).to_owned()),
                numbers[0].value(row),
                numbers[1].value(row),
                numbers[2].value(row),
                numbers[3].value(row),
            ));
        }
    }
    actual.sort();
    assert_eq!(
        actual,
        vec![(None, 4, 18, 2, 7), (Some("a".into()), 4, 22, 1, 10)]
    );
    drop(output);
    drop(final_operator);
    drop(final_factory);
    drop(grouped);
    drop(grouped_operator);
    drop(grouped_factory);
    drop(factory);
    drop(session);
    drop(f);
    drop(creation);
    drop(control);
    drop(host);
    drop(state);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_ordered_partial_keeps_the_first_failure_and_releases_actual_groups() {
    let mut f = ordered_fixture();
    let tracker = MemTracker::new_root("compiled-ordered-partial-first-failure");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    f.input.try_transfer_to(&tracker).unwrap();
    let state = state(tracker.clone());
    let creation = construction(&tracker, &state);
    let session = session(&f);
    let host = rf::OrderedTopNAllocationHost::new(tracker.clone());
    let mut control = RuntimeKernelControl::new(state.error_state());
    control.bind_mem_tracker(tracker.clone());
    let factory = CompiledAggregateProcessorFactory::try_new_ordered(
        f.program.clone(),
        f.partial,
        state.error_state(),
        session.clone(),
        2,
        &host,
        &control,
    )
    .unwrap();
    let mut operator = factory.create(2, 0);
    initialize(operator.as_mut(), &tracker, &creation, &state);
    let mut processor = TestDriver::new(operator.as_mut(), &state);
    processor
        .input_without_output(slice(&f.input, 0, 3, creation.as_ref()))
        .unwrap();
    state.error_state().set_error(KernelFailure::Cancelled);
    // Create no new storage after cancellation: the independently retained
    // complete input is moved directly into the actual failed processor.
    let first = processor.input_without_output(f.input.clone()).unwrap_err();
    assert_eq!(
        first.cause(),
        &ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
    );
    state
        .error_state()
        .set_error(KernelFailure::DeadlineExceeded);
    assert_eq!(processor.signal_eos().unwrap_err().cause(), first.cause());
    assert_eq!(
        processor.advance_to_boundary().unwrap_err().cause(),
        first.cause()
    );
    let failure_notifications = {
        session
            .recorder
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Event::Fail(rf::RuntimeFilterProducerFailure::ExecutionFailed)
                )
            })
            .count()
    };
    assert_eq!(failure_notifications, 1);
    drop(operator);
    drop(factory);
    drop(session);
    drop(f);
    drop(creation);
    drop(control);
    drop(host);
    drop(state);
    assert_eq!(tracker.current(), 0);
}

#[path = "compiled_ordered_scan_tests.rs"]
mod scan_consumer_tests;
