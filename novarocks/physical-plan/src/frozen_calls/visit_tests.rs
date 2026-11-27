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

        nodes,
        sink: fragment.sink().clone(),
        dop_domain: fragment.dop_domain(),
        runtime_filters: fragment.runtime_filters().into(),
        call_requests: fragment.call_requests().clone(),
        calls: fragment.calls().clone(),
    }
    .into()
}

// Real unit uses and relation bindings are traversed independently. Extra node
// payloads exercise representation traversal, not full writer/TopN admission.
fn all_sites(constants: &mut crate::tests::constant_fixture::ConstantFixture) -> Fragment {
    let mut base = special_fixture(constants);
    base.edit_unit(|parts| {
        parts.definitions[0].kind = ExprKind::FunctionCall {
            function: function(FunctionKind::Scalar, integer()),
            args: Box::default(),
        };
        parts
            .requests
            .insert(ExprId::new(0), zero_argument_request());
        for (id, invocation) in parts.control.uses() {
            parts.claims.insert(*id, scalar_claim(invocation.context));
        }
    });
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
                expressions: None,
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

    for node in nodes.values_mut() {
        if let NodeKind::Window(spec) = &mut node.kind {
            let item = &mut spec.calls[0];
            item.binding = function(FunctionKind::Aggregate, integer());
            item.aggregate_binding = Some(Box::new(AggregateBinding {
                function: item.binding.clone(),
                phase: AggregatePhase::Single,
                logical_argument_count: 0,
                merge_state: Some(crate::AggregateMergeBinding {
                    state_interpretation: None,
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_type: ValueType::new(DataType::Binary, false),
                    state_format: AggregateStateFormatId::try_new("fixture/count/state-v1")
                        .unwrap(),
                }),
            }));
        }
    }
    let mut fragment = with_nodes(&base.fragment, nodes);
    let control = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut calls = Vec::new();
    visit_relational_calls_observed::<FrozenCallError>(&fragment, &mut work, |site, binding, _| {
        let context = novarocks_type_contract::RelationEffectContext::call(
            novarocks_type_contract::RelationCallId::new(u32::try_from(calls.len()).unwrap()),
        );
        let writer = matches!(
            site,
            PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. }
        );
        calls.push(FrozenPhysicalCall {
            site,
            context,
            materialized_writer_input: writer.then(|| {
                novarocks_type_contract::RelationEffectContext::materialized_input(
                    context.identity.call,
                )
            }),
            effects: declared_effects(binding.kind(), CallProofScope::Relation(context.identity)),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        });
        Ok(())
    })
    .unwrap();
    work.finish().unwrap();
    fragment = fragment
        .with_call_requests_observed(
            calls
                .iter()
                .map(|c| (c.site, zero_argument_request()))
                .collect(),
            &Control::default(),
        )
        .unwrap();
    fragment
        .with_calls_observed(calls, &Control::default())
        .unwrap()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VisitSite {
    Scalar {
        node: NodeId,
        invocation: ExpressionUseId,
    },
    Relation(PhysicalCallSite),
}
#[derive(Clone, Copy)]
enum VisitBinding<'a> {
    Scalar(&'a BoundFunction),
    Relation(PhysicalCallBinding<'a>),
}
fn visit_owned_calls<'a, E: From<FrozenCallError>>(
    fragment: &'a Fragment,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(VisitSite, VisitBinding<'a>, &mut CompileCheckpoints<'_>) -> Result<(), E>,
) -> Result<(), E> {
    super::super::property_proof::visit_occurrence_calls(
        fragment,
        fragment.calls(),
        work,
        |site, _, effects, work| {
            let (site, binding) = match site {
                super::super::property_proof::OccurrenceSite::Expression { node, invocation } => {
                    let unit = fragment.unit(node).ok_or(FrozenCallError::InvalidSite)?;
                    let source = &unit.control_flow().uses()[&invocation];
                    let ExprKind::FunctionCall { function, .. } = &unit
                        .definition(source.definition)
                        .ok_or(FrozenCallError::InvalidSite)?
                        .kind
                    else {
                        return Err(FrozenCallError::InvalidSite.into());
                    };
                    assert!(std::ptr::eq(effects, &unit.claims()[&invocation].effects));
                    (
                        VisitSite::Scalar { node, invocation },
                        VisitBinding::Scalar(function),
                    )
                }
                super::super::property_proof::OccurrenceSite::Relation(site) => {
                    assert!(std::ptr::eq(
                        effects,
                        &fragment.calls().entries()[&site].effects
                    ));
                    (
                        VisitSite::Relation(site),
                        VisitBinding::Relation(
                            fragment
                                .calls()
                                .binding(fragment, site)
                                .ok_or(FrozenCallError::InvalidSite)?,
                        ),
                    )
                }
            };
            visit(site, binding, work)
        },
    )
}

fn run(
    fragment: &Fragment,
    control: &TraceControl,
    attachment_failure: Option<usize>,
) -> Result<Vec<VisitSite>, OwnerError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
        .map_err(FrozenCallError::Control)?;
    let mut sites = Vec::new();
    let result = visit_owned_calls(fragment, &mut work, |site, _, work| {
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
fn check_prefixes(fragment: &Fragment, failure: Option<usize>) {
    let control = TraceControl::default();
    let result = run(fragment, &control, failure);
    if failure.is_some() {
        assert_eq!(result, Err(OwnerError::Attachment(41)));
    } else {
        assert!(result.is_ok());
    }
    let baseline = control.trace();
    assert!(
        baseline
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::LowerProgram)
    );
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
                run(fragment, &control, failure),
                Err(OwnerError::Frozen(FrozenCallError::Control(cause)))
            );
            assert_eq!(control.trace(), baseline[..position]);
        }
    }
}

#[test]
fn observed_visitor_preserves_every_real_site_order_and_source_binding_address() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fragment = all_sites(&mut constants);
    let control = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let mut sites = Vec::new();
    visit_owned_calls::<OwnerError>(&fragment, &mut work, |site, binding, scope| {
        assert!(std::ptr::addr_eq(
            scope.control(),
            &control as &dyn PureCompileControl
        ));
        match (site, binding) {
            (VisitSite::Scalar { node, invocation }, VisitBinding::Scalar(actual)) => {
                let unit = fragment.unit(node).unwrap();
                let source = &unit.control_flow().uses()[&invocation];
                let ExprKind::FunctionCall { function, .. } =
                    &unit.definition(source.definition).unwrap().kind
                else {
                    panic!("scalar")
                };
                assert!(std::ptr::eq(actual, function));
            }
            (
                VisitSite::Relation(PhysicalCallSite::Window { node, call }),
                VisitBinding::Relation(PhysicalCallBinding::Window {
                    function: actual,
                    aggregate,
                }),
            ) => {
                let NodeKind::Window(spec) = &fragment.nodes()[&node].kind else {
                    panic!("window")
                };
                let source = &spec.calls[call as usize];
                assert!(std::ptr::eq(actual, &source.binding));
                assert!(std::ptr::eq(
                    aggregate.unwrap(),
                    source.aggregate_binding.as_deref().unwrap()
                ));
                assert_eq!(actual.kind, FunctionKind::Aggregate);
            }
            (
                VisitSite::Relation(PhysicalCallSite::Aggregate { node, call }),
                VisitBinding::Relation(PhysicalCallBinding::Aggregate(actual)),
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
                VisitSite::Relation(PhysicalCallSite::TopNState { node, call }),
                VisitBinding::Relation(PhysicalCallBinding::Aggregate(actual)),
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
                VisitSite::Relation(PhysicalCallSite::WriterPartial { node, call }),
                VisitBinding::Relation(PhysicalCallBinding::Aggregate(actual)),
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
                VisitSite::Relation(PhysicalCallSite::WriterFinal { node, call }),
                VisitBinding::Relation(PhysicalCallBinding::Aggregate(actual)),
            ) => {
                let NodeKind::TableFinish(finish) = &fragment.nodes()[&node].kind else {
                    panic!("finish")
                };
                assert!(std::ptr::eq(
                    actual,
                    &finish.final_aggregates[call as usize].binding
                ));
            }
            (
                VisitSite::Relation(PhysicalCallSite::Table { node }),
                VisitBinding::Relation(PhysicalCallBinding::Table(actual)),
            ) => {
                let NodeKind::TableFunction { function, .. } = &fragment.nodes()[&node].kind else {
                    panic!("table")
                };
                assert!(std::ptr::eq(actual, function));
            }

            _ => panic!("site and original binding must agree"),
        }
        sites.push(site);
        Ok(())
    })
    .unwrap();
    work.finish().unwrap();
    assert_eq!(
        sites,
        vec![
            VisitSite::Scalar {
                node: NodeId::new(0),
                invocation: ExpressionUseId::new(0)
            },
            VisitSite::Relation(PhysicalCallSite::Aggregate {
                node: NodeId::new(1),
                call: 0
            }),
            VisitSite::Relation(PhysicalCallSite::Aggregate {
                node: NodeId::new(1),
                call: 1
            }),
            VisitSite::Relation(PhysicalCallSite::Window {
                node: NodeId::new(2),
                call: 0
            }),
            VisitSite::Relation(PhysicalCallSite::Table {
                node: NodeId::new(3)
            }),
            VisitSite::Relation(PhysicalCallSite::TopNState {
                node: NodeId::new(20),
                call: 0
            }),
            VisitSite::Relation(PhysicalCallSite::TopNState {
                node: NodeId::new(20),
                call: 1
            }),
            VisitSite::Relation(PhysicalCallSite::WriterPartial {
                node: NodeId::new(30),
                call: 0
            }),
            VisitSite::Relation(PhysicalCallSite::WriterPartial {
                node: NodeId::new(30),
                call: 1
            }),
            VisitSite::Relation(PhysicalCallSite::WriterFinal {
                node: NodeId::new(u32::MAX),
                call: 0
            }),
            VisitSite::Relation(PhysicalCallSite::WriterFinal {
                node: NodeId::new(u32::MAX),
                call: 1
            })
        ]
    );
    // Independently exercise the actual binding-returning relational visitor.
    let mut relation_sites = Vec::new();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    visit_relational_calls_observed::<OwnerError>(&fragment, &mut work, |site, binding, _| {
        match (binding, fragment.calls().binding(&fragment, site).unwrap()) {
            (PhysicalCallBinding::Aggregate(a), PhysicalCallBinding::Aggregate(b)) => {
                assert!(std::ptr::eq(a, b))
            }
            (PhysicalCallBinding::Table(a), PhysicalCallBinding::Table(b)) => {
                assert!(std::ptr::eq(a, b))
            }
            (
                PhysicalCallBinding::Window {
                    function: a,
                    aggregate: aa,
                },
                PhysicalCallBinding::Window {
                    function: b,
                    aggregate: bb,
                },
            ) => {
                assert!(std::ptr::eq(a, b));
                assert!(std::ptr::eq(aa.unwrap(), bb.unwrap()));
            }
            _ => panic!("binding kind"),
        };
        relation_sites.push(VisitSite::Relation(site));
        Ok(())
    })
    .unwrap();
    work.finish().unwrap();
    assert_eq!(&sites[1..], relation_sites);
}

#[test]
fn observed_visitor_keeps_guarded_shared_definition_occurrences_independent() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = case_fixture(&mut constants);
    let sites = run(&fixture.fragment, &TraceControl::default(), None).unwrap();
    assert_eq!(
        sites,
        vec![
            VisitSite::Scalar {
                node: fixture.primary,
                invocation: ExpressionUseId::new(0)
            },
            VisitSite::Scalar {
                node: fixture.primary,
                invocation: ExpressionUseId::new(u32::MAX)
            }
        ]
    );
    let uses = fixture.unit().control_flow().uses();
    let first = &uses[&ExpressionUseId::new(0)];
    let second = &uses[&ExpressionUseId::new(u32::MAX)];
    assert_eq!(first.definition, second.definition);
    assert_eq!(first.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(second.context.demand, EvaluationDemand::Value);
    assert_ne!(first.context.region, second.context.region);
    assert_ne!(
        fixture.unit().control_flow().regions()[&first.context.region].guard,
        fixture.unit().control_flow().regions()[&second.context.region].guard
    );
}

#[test]
fn observed_visitor_leaves_pending_tail_to_caller_and_preserves_attachment_error() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(2, 91, &mut constants);
    let control = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    visit_owned_calls::<OwnerError>(&fixture.fragment, &mut work, |_, _, scope| {
        scope.step().map_err(FrozenCallError::Control)?;
        Ok(())
    })
    .unwrap();
    let before = control.trace();
    assert_eq!(before.last(), Some(&(CompilePhase::LowerProgram, 0)));
    work.finish().unwrap();
    assert_eq!(
        control.trace().last(),
        Some(&(CompilePhase::LowerProgram, 6))
    );
    assert_eq!(control.trace().len(), before.len() + 1);
    let failed = TraceControl::default();
    assert_eq!(
        run(&fixture.fragment, &failed, Some(1)),
        Err(OwnerError::Attachment(41))
    );
    assert_eq!(
        failed.trace().last(),
        Some(&(CompilePhase::LowerProgram, 1))
    );
}

#[test]
fn observed_visitor_all_small_callback_prefixes_preserve_three_original_causes() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(2, 92, &mut constants);
    check_prefixes(&fixture.fragment, None);
    check_prefixes(&fixture.fragment, Some(1));
}

#[test]
fn observed_visitor_rejects_changed_roots_before_exposing_bindings_and_finishes_ordinary_tail() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(2, 93, &mut constants);
    let mut nodes = fixture.fragment.nodes().clone();
    let NodeKind::Values { rows } = &mut nodes.get_mut(&fixture.primary).unwrap().kind else {
        panic!("values")
    };
    *rows = Box::from([rows[0].clone()]);
    let changed = with_nodes(&fixture.fragment, nodes);
    // There is no foreign global root table in the new representation. The
    // fragment owner validates node-local root coverage before exposing bindings.
    let validate = |control: &TraceControl| {
        validate_fragment_output_properties_observed(
            &changed,
            PlanLimits::FROZEN,
            4 * 1024 * 1024,
            PropertyProofProjectionLimits {
                max_request_bytes: 1024 * 1024,
                max_coexisting_bytes: 8 * 1024 * 1024,
                max_projection_work: 1024 * 1024,
            },
            control,
        )
    };
    let control = TraceControl::default();
    let error = validate(&control).unwrap_err();
    assert!(
        matches!(&error,FragmentPropertyError::Structure(errors) if errors.errors().iter().any(|e|e.message().contains("exactly cover")))
    );
    let mut exposed = 0;
    let gate = validate(&TraceControl::default());
    if gate.is_ok() {
        let work_control = TraceControl::default();
        let mut work =
            CompileCheckpoints::try_new(&work_control, CompilePhase::LowerProgram).unwrap();
        visit_owned_calls::<OwnerError>(&changed, &mut work, |_, _, _| {
            exposed += 1;
            Ok(())
        })
        .unwrap();
        work.finish().unwrap();
    }
    assert_eq!(exposed, 0);
    let baseline = control.trace();
    assert_eq!(baseline.last().unwrap().0, CompilePhase::Validate);
    // The completed structural owner has already observed its positive work;
    // the outer caller still performs the exact ordinary zero-pending footer.
    assert_eq!(baseline.last(), Some(&(CompilePhase::Validate, 0)));
    assert!(
        baseline
            .iter()
            .any(|(phase, units)| *phase == CompilePhase::Validate && *units > 0)
    );
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
            assert!(
                matches!(validate(&control),Err(FragmentPropertyError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), baseline[..position]);
        }
    }
}

#[test]
fn observed_visitor_wide_actual_occurrences_observe_quantum_without_definition_dedup() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(320, 95, &mut fixture_constants);
    let control = TraceControl::default();
    let sites = run(&fixture.fragment, &control, None).unwrap();
    assert_eq!(sites.len(), 320);
    assert_eq!(fixture.unit().definitions().len(), 1);
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
                run(&fixture.fragment, &refused, None),
                Err(OwnerError::Frozen(FrozenCallError::Control(cause)))
            );
            assert_eq!(refused.trace(), baseline[..position]);
        }
    }
}

#[test]
fn observed_visitor_empty_occurrences_and_plain_window_keep_original_representation() {
    let empty = empty_fixture();
    validate_fragment_definition(&empty.fragment).unwrap();
    let control = TraceControl::default();
    assert!(run(&empty.fragment, &control, None).unwrap().is_empty());
    assert_eq!(
        control.trace().last(),
        Some(&(CompilePhase::LowerProgram, 2))
    );
    check_prefixes(&empty.fragment, None);
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut constants);
    let control = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let mut windows = 0;
    visit_relational_calls_observed::<OwnerError>(
        &fixture.fragment,
        &mut work,
        |site, binding, _| {
            if let PhysicalCallBinding::Window {
                function,
                aggregate,
            } = binding
            {
                assert!(matches!(site, PhysicalCallSite::Window { .. }));
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
