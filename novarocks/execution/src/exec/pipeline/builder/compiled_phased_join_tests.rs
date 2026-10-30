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

//! Actual E/F/Q components over packages compiled by the original compiler.
//! Literal inputs here do not claim whole Values-plan execution.
use super::*;
use crate::exec::{
    operators::hashjoin::{
        JoinParticipants,
        compiled_hash_join::{
            CompiledHashJoinPlan, CompiledJoinFinalizeSourceFactory, CompiledJoinFrame,
            CompiledJoinInputSinkFactory, CompiledJoinProbeProcessorFactory,
        },
    },
    pipeline::{
        input::RawInput,
        operator::{ConstructionFailure, InputConstructionFactory, Operator},
        operator_factory::OperatorFactory,
        shape::ControlStep,
        step::{InputResult, NextStep},
        step_context::{StepContext, TurnWork},
    },
};
use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeState};
use std::num::NonZeroU64;

fn input(
    program: &LocalProgram,
    factory: &Arc<dyn InputConstructionFactory>,
    probe: bool,
    rows: &[Row],
) -> crate::exec::chunk::Chunk {
    let layout = program
        .graph()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            ProgramNodeKind::Join {
                left_layout,
                right_layout,
                ..
            }
            | ProgramNodeKind::NestedLoopJoin {
                left_layout,
                right_layout,
                ..
            } => Some(if probe { left_layout } else { right_layout }),
            _ => None,
        })
        .unwrap();
    let schema = factory.create_compiled_chunk_schema(layout).unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    let columns: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(arrow::array::Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(arrow::array::Int64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    crate::exec::chunk::Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        columns,
        rows.len(),
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap()
}
fn bind(
    operator: &mut dyn Operator,
    runtime: &RuntimeState,
    tracker: &Arc<MemTracker>,
    construction: &Arc<dyn InputConstructionFactory>,
) {
    operator.set_mem_tracker(tracker.clone());
    operator.set_construction_factory(construction.clone());
    operator.prepare().unwrap();
    operator.bind_runtime_state(runtime).unwrap();
    operator.activate(runtime).unwrap();
}
fn finalize(sources: &mut [Box<dyn Operator>], runtime: &RuntimeState) {
    let error = runtime.error_state();
    let mut done = vec![false; sources.len()];
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    for _ in 0..100_000 {
        for (i, source) in sources.iter_mut().enumerate() {
            if done[i] {
                continue;
            }
            let mut turn = TurnWork::new(NonZeroU64::MIN, 13);
            let step = source
                .drive(&mut StepContext::new(&error, &mut turn, &mut conversions))
                .unwrap();
            assert!(step.output.is_none());
            done[i] = matches!(step.next, NextStep::Finished);
        }
        if done.iter().all(|d| *d) {
            break;
        }
    }
    assert!(done.iter().all(|d| *d));
}
fn declaration(
    program: &Arc<LocalProgram>,
    participants: JoinParticipants,
) -> Arc<CompiledJoinFrame> {
    let node = program.graph().root();
    Arc::new(
        if matches!(
            program.graph().nodes()[node.index()].kind(),
            ProgramNodeKind::NestedLoopJoin { .. }
        ) {
            CompiledJoinFrame::new_nested_loop(program.clone(), node, participants, None).unwrap()
        } else {
            CompiledJoinFrame::new(
                program.clone(),
                Arc::new(CompiledHashJoinPlan::try_new(program, node).unwrap()),
                participants,
                None,
            )
        },
    )
}
#[test]
fn b3_phased_e2_f2_q4_preserves_compiled_hash_and_nl_multisets() {
    let specs = [
        Spec::hash(JoinKind::Inner, false),
        Spec::hash(JoinKind::Inner, true),
        Spec::hash(JoinKind::LeftOuter, true),
        Spec::hash(JoinKind::LeftSemi, false),
        Spec::hash(JoinKind::LeftAnti, false),
        Spec::hash(JoinKind::LeftSemi, true),
        Spec::hash(JoinKind::LeftAnti, true),
        Spec::hash(JoinKind::NullAwareLeftAnti, false),
        Spec::nested(JoinKind::Inner, true),
        Spec::nested(JoinKind::LeftOuter, true),
        Spec::nested(JoinKind::LeftSemi, true),
        Spec::nested(JoinKind::LeftAnti, true),
    ];
    for spec in specs {
        let mut packages = packages(&single_plan_with_dop(spec, &LEFT_ROWS, &RIGHT_ROWS, 4));
        let program = compile(packages.remove(&SINGLE).unwrap(), 4, true);
        let declaration = declaration(
            &program,
            JoinParticipants {
                inputs: 2,
                finalizers: NonZeroUsize::new(2).unwrap(),
                probes: 4,
                tails: 0,
            },
        );
        let runtime = RuntimeState::default();
        let error = runtime.error_state();
        let tracker = MemTracker::new_root("B3 actual phased probe fixture");
        let construction = super::super::aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            error.clone(),
        );
        let e = CompiledJoinInputSinkFactory::new(declaration.clone(), error.clone(), None);
        let f = CompiledJoinFinalizeSourceFactory::new(declaration.clone(), error.clone());
        let q = CompiledJoinProbeProcessorFactory::new(declaration.clone(), error.clone());
        assert!(q.bind_pipeline_dop(3).is_err());
        let mut inputs: Vec<_> = (0..2).map(|id| e.create(2, id)).collect();
        let mut finalizers: Vec<_> = (0..2).map(|id| f.create(2, id)).collect();
        let mut probes: Vec<_> = (0..4).map(|id| q.create(4, id)).collect();
        for op in inputs
            .iter_mut()
            .chain(finalizers.iter_mut())
            .chain(probes.iter_mut())
        {
            bind(op.as_mut(), &runtime, &tracker, &construction);
        }
        for (id, rows) in [&RIGHT_ROWS[..4], &RIGHT_ROWS[4..]].into_iter().enumerate() {
            let mut driver = TestDriver::new(inputs[id].as_mut(), &runtime);
            driver
                .input_without_output(input(&program, &construction, false, rows))
                .unwrap();
            driver.signal_eos().unwrap();
            assert!(matches!(
                driver.advance_to_boundary().unwrap().next,
                NextStep::Finished
            ));
        }
        finalize(&mut finalizers, &runtime);
        let mut output = Vec::new();
        for (id, op) in probes.iter_mut().enumerate() {
            let mut driver = TestDriver::new(op.as_mut(), &runtime);
            let start = (id * 3).min(LEFT_ROWS.len());
            let end = (start + 3).min(LEFT_ROWS.len());
            if start < end {
                driver
                    .accept_without_output(input(
                        &program,
                        &construction,
                        true,
                        &LEFT_ROWS[start..end],
                    ))
                    .unwrap();
                loop {
                    let step = driver.advance_to_boundary().unwrap();
                    if let Some(page) = step.output {
                        output.push(page);
                    }
                    if matches!(step.next, NextStep::NeedInput) {
                        break;
                    }
                }
            }
            driver.signal_eos().unwrap();
            assert!(matches!(
                driver.advance_to_boundary().unwrap().next,
                NextStep::Finished
            ));
        }
        assert_eq!(
            sorted(int64_rows(&output)),
            sorted(oracle(spec, &LEFT_ROWS, &RIGHT_ROWS)),
            "{spec:?}"
        );
        for op in inputs
            .iter_mut()
            .chain(finalizers.iter_mut())
            .chain(probes.iter_mut())
        {
            op.begin_exit().unwrap();
            op.close().unwrap();
        }
        assert!(runtime.error().is_none());
    }
}
fn empty_build_probe(
    program: &Arc<LocalProgram>,
) -> (
    Box<dyn Operator>,
    Arc<CompiledJoinFrame>,
    RuntimeState,
    Arc<dyn InputConstructionFactory>,
) {
    let declaration = declaration(
        program,
        JoinParticipants {
            inputs: 1,
            finalizers: NonZeroUsize::MIN,
            probes: 1,
            tails: 0,
        },
    );
    let runtime = RuntimeState::default();
    let error = runtime.error_state();
    let tracker = MemTracker::new_root("B3 zero physical build control");
    let construction = super::super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        error.clone(),
    );
    let mut e =
        CompiledJoinInputSinkFactory::new(declaration.clone(), error.clone(), None).create(1, 0);
    let mut f =
        CompiledJoinFinalizeSourceFactory::new(declaration.clone(), error.clone()).create(1, 0);
    let mut q =
        CompiledJoinProbeProcessorFactory::new(declaration.clone(), error.clone()).create(1, 0);
    for op in [&mut e, &mut f, &mut q] {
        bind(op.as_mut(), &runtime, &tracker, &construction);
    }
    let mut driver = TestDriver::new(e.as_mut(), &runtime);
    driver.signal_eos().unwrap();
    driver.advance_to_boundary().unwrap();
    finalize(std::slice::from_mut(&mut f), &runtime);
    e.begin_exit().unwrap();
    f.begin_exit().unwrap();
    (q, declaration, runtime, construction)
}
#[test]
fn b3_b0_control_finishes_without_unadmitted_probe_input_or_upstream_eos() {
    let mut packages = packages(&single_plan(
        Spec::hash(JoinKind::Inner, false),
        &LEFT_ROWS,
        &[],
    ));
    let program = compile(packages.remove(&SINGLE).unwrap(), 1, true);
    let (mut q, declaration, runtime, _) = empty_build_probe(&program);
    let error = runtime.error_state();
    let mut turn = TurnWork::new(NonZeroU64::MIN, 0);
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&error, &mut turn, &mut conversions);
    assert!(matches!(
        q.poll_control(&mut ctx).unwrap(),
        ControlStep::Runnable
    ));
    let finished = q.drive(&mut ctx).unwrap();
    assert!(matches!(finished.next, NextStep::Finished));
    assert!(finished.output.is_none());
    assert!(
        declaration
            .prepared()
            .unwrap()
            .completion
            .published()
            .unwrap()
            .is_some()
    );
    q.begin_exit().unwrap();
    assert!(runtime.error().is_none());
}
#[test]
fn b3_b0_retains_receiver_owned_throwing_probe_key_and_original_required_error() {
    let mut packages = packages(&overflow_plan(Overflow::ProbeKey));
    let program = compile(packages.remove(&SINGLE).unwrap(), 1, true);
    let (mut q, declaration, runtime, construction) = empty_build_probe(&program);
    let error = runtime.error_state();
    let mut turn = TurnWork::new(NonZeroU64::MIN, 30);
    let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&error, &mut turn, &mut conversions);
    assert!(matches!(
        q.try_push(
            &mut ctx,
            RawInput::new(input(&program, &construction, true, &LEFT_ROWS[..1]), None)
        ),
        InputResult::Accepted(_)
    ));
    assert!(matches!(
        q.poll_control(&mut ctx).unwrap(),
        ControlStep::Runnable
    ));
    let failure = match q.drive(&mut ctx) {
        Err(cause) => cause,
        Ok(_) => panic!("B0 must evaluate the already accepted key"),
    };
    let ExecutionFailureCause::RequiredRow(required) = failure.cause() else {
        panic!("original required key error: {failure:?}");
    };
    assert_eq!(required.batch_row(), 0);
    assert_eq!(
        required.root(),
        ProgramExpressionRootSite::Node {
            node: program.graph().root(),
            role: ProgramNodeExpressionRole::JoinProbeKey { key: 0 }
        }
    );
    assert!(declaration.prepared().unwrap().completion.check().is_err());
}

#[test]
fn b3_independent_t_sources_wait_for_q_and_last_page_handoff_with_partial_consumer_close() {
    use crate::exec::operators::{
        LocalExchangeSinkFactory, LocalExchangeSourceFactory,
        hashjoin::compiled_hash_join::CompiledJoinTailSourceFactory,
        local_exchanger::LocalExchanger,
    };
    for kind in [
        novarocks_local_program::JoinType::RightOuter,
        novarocks_local_program::JoinType::FullOuter,
    ] {
        let physical = if kind == novarocks_local_program::JoinType::RightOuter {
            JoinKind::RightOuter
        } else {
            JoinKind::FullOuter
        };
        let mut packages = packages(&single_plan(
            Spec::hash(physical, true),
            &LEFT_ROWS,
            &RIGHT_ROWS,
        ));
        let program = compile(packages.remove(&SINGLE).unwrap(), 1, true);
        let plan =
            Arc::new(CompiledHashJoinPlan::try_new(&program, program.graph().root()).unwrap());
        let handoff = LocalExchanger::new_handoff(6, 2, 1);
        let declaration = Arc::new(CompiledJoinFrame::new(
            program.clone(),
            plan,
            JoinParticipants {
                inputs: 2,
                finalizers: NonZeroUsize::new(2).unwrap(),
                probes: 4,
                tails: 2,
            },
            Some(handoff.clone()),
        ));
        let runtime = RuntimeState::default();
        let error = runtime.error_state();
        let tracker = MemTracker::new_root("B3 actual Q T handoff fixture");
        let construction = super::super::aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            error.clone(),
        );
        let e = CompiledJoinInputSinkFactory::new(declaration.clone(), error.clone(), None);
        let f = CompiledJoinFinalizeSourceFactory::new(declaration.clone(), error.clone());
        let q = CompiledJoinProbeProcessorFactory::new(declaration.clone(), error.clone());
        let t = CompiledJoinTailSourceFactory::new(declaration.clone(), error.clone());
        let mut inputs: Vec<_> = (0..2).map(|id| e.create(2, id)).collect();
        let mut finalizers: Vec<_> = (0..2).map(|id| f.create(2, id)).collect();
        let mut probes: Vec<_> = (0..4).map(|id| q.create(4, id)).collect();
        let mut tails: Vec<_> = (0..2).map(|id| t.create(2, id)).collect();
        let sink = LocalExchangeSinkFactory::new(0, handoff.clone());
        let mut sinks: Vec<_> = (0..6).map(|id| sink.create(6, id)).collect();
        let consumer = LocalExchangeSourceFactory::new(0, 1, handoff.clone());
        let mut consumers: Vec<_> = (0..2).map(|id| consumer.create(2, id)).collect();
        for op in inputs
            .iter_mut()
            .chain(finalizers.iter_mut())
            .chain(probes.iter_mut())
            .chain(tails.iter_mut())
            .chain(sinks.iter_mut())
            .chain(consumers.iter_mut())
        {
            bind(op.as_mut(), &runtime, &tracker, &construction);
        }
        consumers[0].begin_exit().unwrap();
        assert!(!handoff.all_consumers_closed());
        for (id, rows) in [&RIGHT_ROWS[..4], &RIGHT_ROWS[4..]].into_iter().enumerate() {
            let mut driver = TestDriver::new(inputs[id].as_mut(), &runtime);
            driver
                .input_without_output(input(&program, &construction, false, rows))
                .unwrap();
            driver.signal_eos().unwrap();
            driver.advance_to_boundary().unwrap();
        }
        finalize(&mut finalizers, &runtime);
        let mut output = Vec::new();
        for (id, op) in probes.iter_mut().enumerate() {
            let mut driver = TestDriver::new(op.as_mut(), &runtime);
            let start = (id * 3).min(LEFT_ROWS.len());
            let end = (start + 3).min(LEFT_ROWS.len());
            if start < end {
                driver
                    .accept_without_output(input(
                        &program,
                        &construction,
                        true,
                        &LEFT_ROWS[start..end],
                    ))
                    .unwrap();
                loop {
                    let step = driver.advance_to_boundary().unwrap();
                    if let Some(page) = step.output {
                        TestDriver::new(sinks[id].as_mut(), &runtime)
                            .input_without_output(page)
                            .unwrap();
                        let received = TestDriver::new(consumers[1].as_mut(), &runtime)
                            .advance_to_boundary()
                            .unwrap();
                        output.push(
                            received
                                .output
                                .expect("actual handoff retains probe output"),
                        );
                    }
                    if matches!(step.next, NextStep::NeedInput) {
                        break;
                    }
                }
            }
            if id < 3 {
                driver.signal_eos().unwrap();
                assert!(matches!(
                    driver.advance_to_boundary().unwrap().next,
                    NextStep::Finished
                ));
                let mut sink = TestDriver::new(sinks[id].as_mut(), &runtime);
                sink.signal_eos().unwrap();
                sink.advance_to_boundary().unwrap();
            }
        }
        let mut turn = TurnWork::new(NonZeroU64::MIN, 64);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let pending = tails[0]
            .drive(&mut StepContext::new(&error, &mut turn, &mut conversions))
            .unwrap();
        assert!(matches!(pending.next, NextStep::Wait(_)));
        assert!(pending.output.is_none());
        let mut last_q = TestDriver::new(probes[3].as_mut(), &runtime);
        last_q.signal_eos().unwrap();
        assert!(matches!(
            last_q.advance_to_boundary().unwrap().next,
            NextStep::Finished
        ));
        let mut sink = TestDriver::new(sinks[3].as_mut(), &runtime);
        sink.signal_eos().unwrap();
        sink.advance_to_boundary().unwrap();
        let mut done = [false; 2];
        let mut last_page_checked = false;
        for _ in 0..10_000 {
            for i in 0..2 {
                if done[i] {
                    continue;
                }
                let mut turn = TurnWork::new(NonZeroU64::MIN, 64);
                let step = tails[i]
                    .drive(&mut StepContext::new(&error, &mut turn, &mut conversions))
                    .unwrap();
                if let Some(page) = step.output {
                    assert!(
                        matches!(step.next, NextStep::Runnable),
                        "last page cannot publish EOS"
                    );
                    if !last_page_checked {
                        let other = 1 - i;
                        let mut turn = TurnWork::new(NonZeroU64::MIN, 64);
                        let before_transfer = tails[other]
                            .drive(&mut StepContext::new(&error, &mut turn, &mut conversions))
                            .unwrap();
                        assert!(before_transfer.output.is_none());
                        assert!(matches!(before_transfer.next, NextStep::Wait(_)));
                        last_page_checked = true;
                    }
                    TestDriver::new(sinks[4 + i].as_mut(), &runtime)
                        .input_without_output(page)
                        .unwrap();
                    let received = TestDriver::new(consumers[1].as_mut(), &runtime)
                        .advance_to_boundary()
                        .unwrap();
                    output.push(received.output.expect("actual handoff retains tail output"));
                }
                if matches!(step.next, NextStep::Finished) {
                    done[i] = true;
                    let mut sink = TestDriver::new(sinks[4 + i].as_mut(), &runtime);
                    sink.signal_eos().unwrap();
                    sink.advance_to_boundary().unwrap();
                }
            }
            if done.iter().all(|d| *d) {
                break;
            }
        }
        assert!(done.iter().all(|d| *d));
        assert!(last_page_checked);
        assert_eq!(handoff.remaining_producers(), 0);
        assert_eq!(
            sorted(int64_rows(&output)),
            sorted(oracle(Spec::hash(physical, true), &LEFT_ROWS, &RIGHT_ROWS))
        );
        assert!(matches!(
            TestDriver::new(consumers[1].as_mut(), &runtime)
                .advance_to_boundary()
                .unwrap()
                .next,
            NextStep::Finished
        ));
        for op in inputs
            .iter_mut()
            .chain(finalizers.iter_mut())
            .chain(probes.iter_mut())
            .chain(tails.iter_mut())
            .chain(sinks.iter_mut())
            .chain(consumers.iter_mut())
        {
            op.begin_exit().unwrap();
            op.close().unwrap();
        }
        assert!(runtime.error().is_none());
    }
}
