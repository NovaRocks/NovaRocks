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

fn expected_relational_sites() -> Vec<PhysicalCallSite> {
    vec![
        PhysicalCallSite::Aggregate {
            node: NodeId::new(1),
            call: 0,
        },
        PhysicalCallSite::Aggregate {
            node: NodeId::new(1),
            call: 1,
        },
        PhysicalCallSite::Table {
            node: NodeId::new(3),
        },
        PhysicalCallSite::TopNState {
            node: NodeId::new(20),
            call: 0,
        },
        PhysicalCallSite::TopNState {
            node: NodeId::new(20),
            call: 1,
        },
        PhysicalCallSite::WriterPartial {
            node: NodeId::new(30),
            call: 0,
        },
        PhysicalCallSite::WriterPartial {
            node: NodeId::new(30),
            call: 1,
        },
        PhysicalCallSite::WriterFinal {
            node: NodeId::new(u32::MAX),
            call: 0,
        },
        PhysicalCallSite::WriterFinal {
            node: NodeId::new(u32::MAX),
            call: 1,
        },
    ]
}
fn relational_run(
    fragment: &Fragment,
    control: &TraceControl,
    failure: Option<usize>,
) -> Result<Vec<PhysicalCallSite>, OwnerError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
        .map_err(FrozenCallError::Control)?;
    let mut sites = Vec::new();
    let result =
        visit_relational_calls_observed::<OwnerError>(fragment, &mut work, |site, _, work| {
            sites.push(site);
            work.step().map_err(FrozenCallError::Control)?;
            if failure == Some(sites.len()) {
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
fn assert_relational_prefixes(fragment: &Fragment, failure: Option<usize>) {
    let control = TraceControl::default();
    let result = relational_run(fragment, &control, failure);
    if failure.is_some() {
        assert_eq!(result, Err(OwnerError::Attachment(41)));
    } else {
        assert!(result.is_ok());
    }
    let baseline = control.trace();
    assert_eq!(baseline[0], (CompilePhase::LowerProgram, 0));
    assert!(baseline.last().unwrap().1 > 0);
    assert!(
        baseline
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::LowerProgram && *units <= 256)
    );
    for position in 1..=baseline.len() {
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
                relational_run(fragment, &refused, failure),
                Err(OwnerError::Frozen(FrozenCallError::Control(cause)))
            );
            assert_eq!(refused.trace(), baseline[..position]);
        }
    }
}

#[test]
fn relational_visitor_preserves_all_actual_site_order_and_original_binding_addresses() {
    let (fragment, uses) = all_sites();
    let control = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let mut actual = Vec::new();
    visit_relational_calls_observed::<OwnerError>(&fragment, &mut work, |site, binding, scope| {
        assert!(std::ptr::addr_eq(
            scope.control(),
            &control as &dyn PureCompileControl
        ));
        match (site, binding) {
            (
                PhysicalCallSite::Aggregate { node, call },
                PhysicalCallBinding::Aggregate(binding),
            ) => {
                let NodeKind::Aggregate { calls, .. } = &fragment.nodes()[&node].kind else {
                    panic!("aggregate")
                };
                assert!(std::ptr::eq(binding, &calls[call as usize].binding));
                assert_eq!(
                    calls[call as usize].id.get(),
                    if call == 0 { 0 } else { u32::MAX }
                );
                assert!(calls[call as usize].arguments.is_empty());
            }
            (
                PhysicalCallSite::TopNState { node, call },
                PhysicalCallBinding::Aggregate(binding),
            ) => {
                let NodeKind::TopN {
                    reduction: TopNReduction::GroupedStates { calls, .. },
                    ..
                } = &fragment.nodes()[&node].kind
                else {
                    panic!("topn state")
                };
                assert!(std::ptr::eq(binding, &calls[call as usize].binding));
            }
            (
                PhysicalCallSite::WriterPartial { node, call },
                PhysicalCallBinding::Aggregate(binding),
            ) => {
                let NodeKind::TableWriter { target } = &fragment.nodes()[&node].kind else {
                    panic!("writer")
                };
                assert!(std::ptr::eq(
                    binding,
                    &target.partial_aggregates[call as usize].binding
                ));
            }
            (
                PhysicalCallSite::WriterFinal { node, call },
                PhysicalCallBinding::Aggregate(binding),
            ) => {
                let NodeKind::TableFinish(finish) = &fragment.nodes()[&node].kind else {
                    panic!("finish")
                };
                assert!(std::ptr::eq(
                    binding,
                    &finish.final_aggregates[call as usize].binding
                ));
            }
            (PhysicalCallSite::Table { node }, PhysicalCallBinding::Table(binding)) => {
                let NodeKind::TableFunction {
                    function,
                    arguments,
                    ..
                } = &fragment.nodes()[&node].kind
                else {
                    panic!("table")
                };
                assert!(std::ptr::eq(binding, function));
                assert!(arguments.is_empty());
            }
            _ => panic!("raw relational visitor must not expose expression calls"),
        }
        actual.push(site);
        Ok(())
    })
    .unwrap();
    work.finish().unwrap();
    assert_eq!(actual, expected_relational_sites());
    // The complete visitor exposes expressions first and delegates this same
    // relational ordering, without using AggregateCallId as its array ordinal.
    let complete = run(&fragment, &uses, &TraceControl::default(), None).unwrap();
    let mut expected = vec![
        PhysicalCallSite::Expression(ExpressionUseId::new(0)),
        PhysicalCallSite::Expression(ExpressionUseId::new(u32::MAX)),
    ];
    expected.extend(expected_relational_sites());
    assert_eq!(complete, expected);
}

#[test]
fn relational_visitor_needs_no_expression_roots_for_zero_argument_occurrences() {
    let (source, _) = all_sites();
    // This is a raw representation fixture, deliberately not a full writer
    // placement/admission claim. The visitor has no RootUses prerequisite.
    let mut nodes = source.nodes().clone();
    nodes.retain(|_, node| {
        matches!(
            node.kind,
            NodeKind::Aggregate { .. }
                | NodeKind::TopN {
                    reduction: TopNReduction::GroupedStates { .. },
                    ..
                }
                | NodeKind::TableWriter { .. }
                | NodeKind::TableFinish(_)
                | NodeKind::TableFunction { .. }
        )
    });
    let expressions = ExprArena::try_from_definitions_observed(
        std::iter::empty(),
        &PlanLimits::default(),
        &Control::default(),
    )
    .unwrap();
    let rootless: Fragment = FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: source.id(),
        root: source.root(),
        values: source.values().clone(),
        expressions,
        nodes,
        sink: source.sink().clone(),
        dop_domain: source.dop_domain(),
        runtime_filters: Box::default(),
        call_requests: source.call_requests().clone(),
    }
    .into();
    assert!(rootless.expressions().is_empty());
    assert_eq!(
        relational_run(&rootless, &TraceControl::default(), None).unwrap(),
        expected_relational_sites()
    );
    let empty = with_nodes(&rootless, BTreeMap::new());
    let control = TraceControl::default();
    assert_eq!(relational_run(&empty, &control, None), Ok(vec![]));
    assert_eq!(
        control.trace(),
        vec![
            (CompilePhase::LowerProgram, 0),
            (CompilePhase::LowerProgram, 0)
        ]
    );
}

#[test]
fn relational_visitor_leaves_success_and_ordinary_error_tails_to_original_caller() {
    let (fragment, _) = all_sites();
    let control = TraceControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let mut count = 0;
    visit_relational_calls_observed::<OwnerError>(&fragment, &mut work, |_, _, _| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 9);
    assert_eq!(control.trace(), vec![(CompilePhase::LowerProgram, 0)]);
    work.finish().unwrap();
    // Eight aggregate-array occurrences and seven node visits were completed;
    // the Table occurrence shares its single node visit, not an extra step.
    assert_eq!(
        control.trace(),
        vec![
            (CompilePhase::LowerProgram, 0),
            (CompilePhase::LowerProgram, 15)
        ]
    );
    let ordinary = TraceControl::default();
    assert_eq!(
        relational_run(&fragment, &ordinary, Some(1)),
        Err(OwnerError::Attachment(41))
    );
    assert_eq!(
        ordinary.trace(),
        vec![
            (CompilePhase::LowerProgram, 0),
            (CompilePhase::LowerProgram, 2)
        ]
    );
}

#[test]
fn relational_visitor_all_small_actual_callback_prefixes_preserve_three_first_causes() {
    let (fragment, _) = all_sites();
    assert_relational_prefixes(&fragment, None);
    assert_relational_prefixes(&fragment, Some(1));
    assert_relational_prefixes(&fragment, Some(9));
}

#[test]
fn relational_visitor_wide_real_call_array_observes_quantum_without_binding_dedup() {
    let fixture = special_fixture();
    let mut nodes = fixture.fragment.nodes().clone();
    let aggregate = nodes
        .values_mut()
        .find(|node| matches!(node.kind, NodeKind::Aggregate { .. }))
        .unwrap();
    let aggregate_id = aggregate.id;
    let NodeKind::Aggregate { calls, .. } = &mut aggregate.kind else {
        unreachable!()
    };
    let original = calls[0].clone();
    *calls = vec![original; 320].into_boxed_slice();
    // Repeated public binding/call identity is not deduplicated by a raw
    // visitor. Mandatory full Fragment semantic admission is a later owner.
    let wide = with_nodes(&fixture.fragment, nodes);
    let control = TraceControl::default();
    let actual = relational_run(&wide, &control, None).unwrap();
    let mut expected: Vec<_> = (0..320)
        .map(|call| PhysicalCallSite::Aggregate {
            node: aggregate_id,
            call,
        })
        .collect();
    expected.push(PhysicalCallSite::Table {
        node: fixture.fragment.root(),
    });
    assert_eq!(actual, expected);
    let baseline = control.trace();
    let positions: Vec<_> = baseline
        .iter()
        .enumerate()
        .filter_map(|(index, (_, units))| (*units == 256).then_some(index + 1))
        .chain([1, baseline.len()])
        .collect();
    assert!(baseline.iter().any(|(_, units)| *units == 256));
    for position in positions {
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
                relational_run(&wide, &refused, None),
                Err(OwnerError::Frozen(FrozenCallError::Control(cause)))
            );
            assert_eq!(refused.trace(), baseline[..position]);
        }
    }
}
