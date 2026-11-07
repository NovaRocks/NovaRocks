// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::plan::FragmentParts;
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
    WriteTargetOrdinal,
};
use novarocks_type_contract::OrderedComparisonAlgorithm;

#[derive(Clone, Debug, Eq, PartialEq)]
enum OwnerError {
    Frozen(FrozenCallError),
    Attachment(u32),
}
impl From<FrozenCallError> for OwnerError {
    fn from(error: FrozenCallError) -> Self {
        Self::Frozen(error)
    }
}
#[derive(Default)]
struct TraceControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl TraceControl {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for TraceControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if let Some((position, cause)) = self.refusal.clone()
            && trace.len() == position
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn with_nodes(fragment: &Fragment, nodes: BTreeMap<NodeId, PhysicalNode>) -> Fragment {
    FragmentParts {
        summaries: Box::default(),
        stage_placement: fragment.stage_placement().clone(),
        id: fragment.id(),
        root: fragment.root(),
        values: fragment.values().clone(),
        expressions: fragment.expressions().clone(),
        nodes,
        sink: fragment.sink().clone(),
        dop_domain: fragment.dop_domain(),
        runtime_filters: fragment.runtime_filters().into(),
        call_requests: fragment.call_requests().clone(),
    }
    .into()
}

// Use the original fixture's arena, roots and binding authors. Extra real node
// payloads exercise representation traversal only; this is not a writer or
// grouped-TopN fragment semantic-admission fixture.
fn all_sites() -> (Fragment, PhysicalRootUses) {
    let base = special_fixture();
    let mut definitions: Vec<_> = base
        .fragment
        .expressions()
        .iter()
        .map(|(_, expression)| expression.clone())
        .collect();
    for expression in &mut definitions {
        match &mut expression.kind {
            ExprKind::Literal(_) => {
                expression.kind = ExprKind::FunctionCall {
                    function: function(FunctionKind::Scalar, expression.ty.clone()),
                    args: Box::default(),
                };
            }
            ExprKind::WindowCall {
                function: selected,
                aggregate_binding,
                ..
            } => {
                selected.kind = FunctionKind::Aggregate;
                *aggregate_binding = Some(Box::new(AggregateBinding {
                    state_interpretation: None,
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    function: selected.clone(),
                    phase: AggregatePhase::Single,
                    logical_argument_count: 0,
                    intermediate_type: ValueType::new(DataType::Binary, false),
                    state_format: AggregateStateFormatId::try_new("fixture/count/state-v1")
                        .unwrap(),
                }));
            }
            _ => panic!("original special fixture contains only literal and window roots"),
        }
    }
    let expressions = ExprArena::try_from_definitions_observed(
        definitions.into_iter(),
        &PlanLimits::default(),
        &Control::default(),
    )
    .unwrap();
    let mut nodes = base.fragment.nodes().clone();
    let calls = nodes
        .values()
        .find_map(|node| match &node.kind {
            NodeKind::Aggregate { calls, .. } => Some(calls.clone()),
            _ => None,
        })
        .unwrap();
    let writer_calls: Box<[_]> = calls
        .iter()
        .map(|call| WriterAggregateCall {
            input: call.output,
            binding: call.binding.clone(),
            output: call.output,
        })
        .collect();
    let schema = WriterRelationSchema {
        revision: 1,
        fields: Box::default(),
    };
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::parse("visitor-fixture").unwrap(),
                CatalogVersion::from_bytes([3; 32]),
            ),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7].into(),
    );
    for (id, kind) in [
        (
            20,
            NodeKind::TopN {
                order_by: Box::default(),
                limit: 5,
                offset: 0,
                phase: TopNPhase::Single,
                reduction: TopNReduction::GroupedStates {
                    group_by: Box::default(),
                    calls,
                    comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
                },
            },
        ),
        (
            30,
            NodeKind::TableWriter {
                target: WriterTarget {
                    handle: payload,
                    write_target_ordinal: WriteTargetOrdinal::try_new(0).unwrap(),
                    input: Box::default(),
                    required_distribution: Distribution::Singleton,
                    target_fields: Box::default(),
                    output_schema: schema.clone(),
                    partial_aggregates: writer_calls.clone(),
                },
            },
        ),
        (
            u32::MAX,
            NodeKind::TableFinish(WriterFinishSpec {
                count_mode: crate::FinishCountMode::WriterReported,
                inputs: crate::FinishInputs {
                    writer_results: 0,
                    summaries: Default::default(),
                },
                summaries: Default::default(),
                expected_target_ordinals: Box::from([WriteTargetOrdinal::try_new(0).unwrap()]),
                input_schema: schema.clone(),
                output_schema: schema,
                final_aggregates: writer_calls,
                grouped_unpivot: None,
            }),
        ),
    ] {
        let id = NodeId::new(id);
        nodes.insert(
            id,
            PhysicalNode {
                id,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties(),
                output: OutputPort {
                    node: id,
                    columns: Box::default(),
                },
                kind,
            },
        );
    }
    let fragment: Fragment = FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: base.fragment.id(),
        root: base.fragment.root(),
        values: base.fragment.values().clone(),
        expressions,
        nodes,
        sink: base.fragment.sink().clone(),
        dop_domain: base.fragment.dop_domain(),
        runtime_filters: Box::default(),
        call_requests: base.fragment.call_requests().clone(),
    }
    .into();
    let uses = leaf_roots(&fragment);
    (fragment, uses)
}

fn run(
    fragment: &Fragment,
    uses: &PhysicalRootUses,
    control: &TraceControl,
    attachment_failure: Option<usize>,
) -> Result<Vec<PhysicalCallSite>, OwnerError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
        .map_err(FrozenCallError::Control)?;
    let mut sites = Vec::new();
    let result = visit_physical_calls_observed(fragment, uses, &mut work, |site, _, work| {
        sites.push(site);
        work.step().map_err(FrozenCallError::Control)?;
        if attachment_failure == Some(sites.len()) {
            return Err(OwnerError::Attachment(41));
        }
        Ok(())
    });
    if matches!(result, Err(OwnerError::Frozen(FrozenCallError::Control(_)))) {
        return result.map(|()| sites);
    }
    work.finish().map_err(FrozenCallError::Control)?;
    result.map(|()| sites)
}
fn check_prefixes(fragment: &Fragment, uses: &PhysicalRootUses, failure: Option<usize>) {
    let success = TraceControl::default();
    let result = run(fragment, uses, &success, failure);
    if failure.is_some() {
        assert_eq!(result, Err(OwnerError::Attachment(41)));
    } else {
        assert!(result.is_ok());
    }
    let baseline = success.trace();
    assert!(
        baseline
            .iter()
            .any(|(phase, _)| *phase == CompilePhase::Validate)
    );
    assert_eq!(baseline.last().unwrap().0, CompilePhase::LowerProgram);
    assert!(baseline.last().unwrap().1 > 0);
    for position in 1..=baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = TraceControl {
                refusal: Some((position, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                run(fragment, uses, &control, failure),
                Err(OwnerError::Frozen(FrozenCallError::Control(cause)))
            );
            assert_eq!(control.trace(), baseline[..position]);
        }
    }
}

#[test]
fn observed_visitor_preserves_every_real_site_order_and_source_binding_address() {
    let (fragment, uses) = all_sites();
    let control = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let mut sites = Vec::new();
    visit_physical_calls_observed::<OwnerError>(
        &fragment,
        &uses,
        &mut work,
        |site, binding, scope| {
            assert!(std::ptr::addr_eq(
                scope.control(),
                &control as &dyn PureCompileControl
            ));
            match (site, binding) {
                (PhysicalCallSite::Expression(id), PhysicalCallBinding::Scalar(actual)) => {
                    let ExprKind::FunctionCall { function, .. } = &fragment
                        .expressions()
                        .get(uses.flow().uses()[&id].definition)
                        .unwrap()
                        .kind
                    else {
                        panic!("scalar")
                    };
                    assert!(std::ptr::eq(actual, function));
                }
                (
                    PhysicalCallSite::Expression(id),
                    PhysicalCallBinding::Window {
                        function: actual,
                        aggregate,
                    },
                ) => {
                    let ExprKind::WindowCall {
                        function,
                        aggregate_binding,
                        ..
                    } = &fragment
                        .expressions()
                        .get(uses.flow().uses()[&id].definition)
                        .unwrap()
                        .kind
                    else {
                        panic!("window")
                    };
                    assert!(std::ptr::eq(actual, function));
                    assert!(std::ptr::eq(
                        aggregate.unwrap(),
                        aggregate_binding.as_deref().unwrap()
                    ));
                    assert_eq!(actual.kind, FunctionKind::Aggregate);
                }
                (
                    PhysicalCallSite::Aggregate { node, call },
                    PhysicalCallBinding::Aggregate(actual),
                ) => {
                    let NodeKind::Aggregate { calls, .. } = &fragment.nodes()[&node].kind else {
                        panic!("aggregate")
                    };
                    assert!(std::ptr::eq(actual, &calls[call as usize].binding));
                    assert_eq!(
                        calls[call as usize].id.get(),
                        if call == 0 { 0 } else { u32::MAX }
                    );
                }
                (
                    PhysicalCallSite::TopNState { node, call },
                    PhysicalCallBinding::Aggregate(actual),
                ) => {
                    let NodeKind::TopN {
                        reduction: TopNReduction::GroupedStates { calls, .. },
                        ..
                    } = &fragment.nodes()[&node].kind
                    else {
                        panic!("topn")
                    };
                    assert!(std::ptr::eq(actual, &calls[call as usize].binding));
                }
                (
                    PhysicalCallSite::WriterPartial { node, call },
                    PhysicalCallBinding::Aggregate(actual),
                ) => {
                    let NodeKind::TableWriter { target } = &fragment.nodes()[&node].kind else {
                        panic!("writer")
                    };
                    assert!(std::ptr::eq(
                        actual,
                        &target.partial_aggregates[call as usize].binding
                    ));
                }
                (
                    PhysicalCallSite::WriterFinal { node, call },
                    PhysicalCallBinding::Aggregate(actual),
                ) => {
                    let NodeKind::TableFinish(finish) = &fragment.nodes()[&node].kind else {
                        panic!("finish")
                    };
                    assert!(std::ptr::eq(
                        actual,
                        &finish.final_aggregates[call as usize].binding
                    ));
                }
                (PhysicalCallSite::Table { node }, PhysicalCallBinding::Table(actual)) => {
                    let NodeKind::TableFunction { function, .. } = &fragment.nodes()[&node].kind
                    else {
                        panic!("table")
                    };
                    assert!(std::ptr::eq(actual, function));
                }
                _ => panic!("site and original binding must agree"),
            }
            sites.push(site);
            Ok(())
        },
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(
        sites,
        vec![
            PhysicalCallSite::Expression(ExpressionUseId::new(0)),
            PhysicalCallSite::Expression(ExpressionUseId::new(u32::MAX)),
            PhysicalCallSite::Aggregate {
                node: NodeId::new(1),
                call: 0
            },
            PhysicalCallSite::Aggregate {
                node: NodeId::new(1),
                call: 1
            },
            PhysicalCallSite::Table {
                node: NodeId::new(3)
            },
            PhysicalCallSite::TopNState {
                node: NodeId::new(20),
                call: 0
            },
            PhysicalCallSite::TopNState {
                node: NodeId::new(20),
                call: 1
            },
            PhysicalCallSite::WriterPartial {
                node: NodeId::new(30),
                call: 0
            },
            PhysicalCallSite::WriterPartial {
                node: NodeId::new(30),
                call: 1
            },
            PhysicalCallSite::WriterFinal {
                node: NodeId::new(u32::MAX),
                call: 0
            },
            PhysicalCallSite::WriterFinal {
                node: NodeId::new(u32::MAX),
                call: 1
            },
        ]
    );
}

#[test]
fn observed_visitor_keeps_guarded_shared_definition_occurrences_independent() {
    let fixture = case_fixture();
    let control = TraceControl::default();
    let sites = run(&fixture.fragment, &fixture.uses, &control, None).unwrap();
    assert_eq!(
        sites,
        vec![
            PhysicalCallSite::Expression(ExpressionUseId::new(0)),
            PhysicalCallSite::Expression(ExpressionUseId::new(u32::MAX))
        ]
    );
    let first = &fixture.uses.flow().uses()[&ExpressionUseId::new(0)];
    let second = &fixture.uses.flow().uses()[&ExpressionUseId::new(u32::MAX)];
    assert_eq!(first.definition, second.definition);
    assert_eq!(first.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(second.context.demand, EvaluationDemand::Value);
    assert_ne!(first.context.domain, second.context.domain);
    assert_ne!(
        fixture.uses.flow().domains()[&first.context.domain].guard,
        fixture.uses.flow().domains()[&second.context.domain].guard
    );
}

#[test]
fn observed_visitor_leaves_pending_tail_to_caller_and_preserves_attachment_error() {
    let fixture = scalar_fixture(2, 91);
    let control = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    visit_physical_calls_observed::<OwnerError>(
        &fixture.fragment,
        &fixture.uses,
        &mut work,
        |_, _, scope| {
            scope.step().map_err(FrozenCallError::Control)?;
            Ok(())
        },
    )
    .unwrap();
    let before_finish = control.trace();
    assert_eq!(before_finish.last(), Some(&(CompilePhase::LowerProgram, 0)));
    work.finish().unwrap();
    assert_eq!(
        control.trace().last(),
        Some(&(CompilePhase::LowerProgram, 5))
    );
    assert_eq!(control.trace().len(), before_finish.len() + 1);
    let failed = TraceControl::default();
    assert_eq!(
        run(&fixture.fragment, &fixture.uses, &failed, Some(1)),
        Err(OwnerError::Attachment(41))
    );
    assert_eq!(
        failed.trace().last(),
        Some(&(CompilePhase::LowerProgram, 1))
    );
}

#[test]
fn observed_visitor_all_small_callback_prefixes_preserve_three_original_causes() {
    let fixture = scalar_fixture(2, 92);
    check_prefixes(&fixture.fragment, &fixture.uses, None);
    check_prefixes(&fixture.fragment, &fixture.uses, Some(1));
}

#[test]
fn observed_visitor_rejects_changed_roots_before_exposing_bindings_and_finishes_ordinary_tail() {
    let fixture = scalar_fixture(2, 93);
    let foreign = scalar_fixture(2, 94);
    let control = TraceControl::default();
    assert_eq!(
        run(&fixture.fragment, &foreign.uses, &control, None),
        Err(OwnerError::Frozen(FrozenCallError::Roots(
            RootUseBindingError::WrongFragment
        )))
    );
    assert_eq!(
        control.trace(),
        vec![
            (CompilePhase::LowerProgram, 0),
            (CompilePhase::LowerProgram, 0)
        ]
    );
    let mut nodes = fixture.fragment.nodes().clone();
    let NodeKind::Values { rows } = &mut nodes.get_mut(&fixture.fragment.root()).unwrap().kind
    else {
        panic!("values")
    };
    *rows = Box::from([rows[0].clone()]);
    let changed = with_nodes(&fixture.fragment, nodes);
    let check = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&check, CompilePhase::LowerProgram).unwrap();
    let mut exposed = 0;
    let result = visit_physical_calls_observed::<OwnerError>(
        &changed,
        &fixture.uses,
        &mut work,
        |_, _, _| {
            exposed += 1;
            Ok(())
        },
    );
    work.finish().unwrap();
    assert_eq!(
        result,
        Err(OwnerError::Frozen(FrozenCallError::Roots(
            RootUseBindingError::ChangedRoots,
        )))
    );
    assert_eq!(exposed, 0);
    let baseline = TraceControl::default();
    assert_eq!(
        run(&changed, &fixture.uses, &baseline, None),
        Err(OwnerError::Frozen(FrozenCallError::Roots(
            RootUseBindingError::ChangedRoots
        )))
    );
    for position in 1..=baseline.trace().len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refused = TraceControl {
                refusal: Some((position, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                run(&changed, &fixture.uses, &refused, None),
                Err(OwnerError::Frozen(FrozenCallError::Control(cause)))
            );
            assert_eq!(refused.trace(), baseline.trace()[..position]);
        }
    }
}

#[test]
fn observed_visitor_wide_actual_occurrences_observe_quantum_without_definition_dedup() {
    let fixture = scalar_fixture(320, 95);
    let control = TraceControl::default();
    let sites = run(&fixture.fragment, &fixture.uses, &control, None).unwrap();
    assert_eq!(sites.len(), 320);
    assert_eq!(fixture.fragment.expressions().len(), 1);
    let baseline = control.trace();
    let positions: Vec<_> = baseline
        .iter()
        .enumerate()
        .filter_map(|(index, (_, units))| (*units == 256).then_some(index + 1))
        .collect();
    assert!(!positions.is_empty());
    assert!(
        baseline
            .iter()
            .any(|(phase, units)| *phase == CompilePhase::LowerProgram && *units == 256)
    );
    for position in positions.into_iter().chain([1, baseline.len()]) {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refused = TraceControl {
                refusal: Some((position, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                run(&fixture.fragment, &fixture.uses, &refused, None),
                Err(OwnerError::Frozen(FrozenCallError::Control(cause)))
            );
            assert_eq!(refused.trace(), baseline[..position]);
        }
    }
}

#[test]
fn observed_visitor_empty_occurrences_and_plain_window_keep_original_representation() {
    // A true empty Values source has no unreachable scalar definition.
    let mut builder = FragmentBuilder::new(FragmentId::new(96));
    let node = builder.reserve_node_id().unwrap();
    let output = builder
        .add_value(
            integer(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    install_node(
        &mut builder,
        node,
        vec![],
        vec![output],
        NodeKind::Values {
            rows: Box::default(),
        },
    );
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    validate_fragment_definition(&fragment).unwrap();
    let empty = Fixture {
        uses: leaf_roots(&fragment),
        fragment,
        calls: vec![],
    };
    let control = TraceControl::default();
    assert!(
        run(&empty.fragment, &empty.uses, &control, None)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        control.trace().last(),
        Some(&(CompilePhase::LowerProgram, 1))
    );
    check_prefixes(&empty.fragment, &empty.uses, None);
    let fixture = special_fixture();
    let control = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let mut windows = 0;
    visit_physical_calls_observed::<OwnerError>(
        &fixture.fragment,
        &fixture.uses,
        &mut work,
        |_, binding, _| {
            if let PhysicalCallBinding::Window {
                function,
                aggregate,
            } = binding
            {
                assert_eq!(function.kind, FunctionKind::Window);
                assert!(aggregate.is_none());
                windows += 1;
            }
            Ok(())
        },
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(windows, 1);
}
#[cfg(test)]
#[path = "relational_visit_tests.rs"]
mod relational_visit_tests;
