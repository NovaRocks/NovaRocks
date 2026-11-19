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
use super::*;
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_semantics_v2::pruning::{
    decode_frozen_pruning_observed, encode_frozen_pruning_observed,
};
use novarocks_connector_contract::ScanColumnId;
use novarocks_physical_plan::{
    FrozenFragmentPruning, FrozenPruningError, PredicateResponsibilityRef,
    ProviderReadOccurrenceId, PruningColumnTrace, PruningDomainField, PruningDomainSite,
    PruningDomainWitness, PruningInputEdge, PruningSourceWitness,
};
use novarocks_type_contract::{SemanticParameterId, SemanticParameterKey, SemanticParameterRef};
use std::alloc::Layout;
const SOURCE: usize = 64 * 1024 * 1024;
fn ceilings() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: usize::MAX,
        max_value_references: usize::MAX,
        max_list_items: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: usize::MAX,
            max_allocation_requests: usize::MAX,
            max_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_work: usize::MAX,
        },
    }
}
fn exact(f: NodeProjectionFacts) -> NodeProjectionLimits {
    let mut l = ceilings();
    l.max_input_nodes = f.input_node_count;
    l.max_value_references = f.value_reference_count;
    l.max_list_items = f.list_item_count;
    l.max_allocation_requests = f.allocation_requests_upper_bound;
    l.max_allocation_request_bytes = f.allocation_request_bytes_upper_bound;
    l.max_coexisting_source_and_request_bytes = f.coexisting_source_and_request_bytes_upper_bound;
    l.max_work = f.cumulative_work_upper_bound;
    l
}
fn footer<T>(work: CompileCheckpoints<'_>, result: Result<T, E>) -> Result<T, E> {
    if matches!(&result, Err(E::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn scope<T>(
    control: &TestControl,
    phase: CompilePhase,
    run: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, E>,
) -> Result<T, E> {
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let result = run(&mut work);
    footer(work, result)
}
fn trace(c: &TestControl) -> Vec<(CompilePhase, u32)> {
    c.events.lock().unwrap().clone()
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn prefixes<T>(phase: CompilePhase, mut run: impl FnMut(&TestControl) -> Result<T, E>) {
    let control = TestControl::default();
    let result = run(&control);
    let baseline = trace(&control);
    assert!(!baseline.is_empty());
    assert!(baseline.iter().all(|(p, _)| *p == phase));
    let success = result.is_ok();
    for at in 1..=baseline.len() {
        for cause in causes() {
            let c = TestControl {
                failure: Some((at, cause.clone())),
                events: Mutex::new(Vec::new()),
            };
            assert!(
                matches!(run(&c),Err(E::Control(actual)) if actual==cause),
                "success={success} callback={at} cause={cause:?}"
            );
            assert_eq!(trace(&c), baseline[..at]);
        }
    }
}
fn one_pruning(values: usize) -> FrozenFragmentPruning {
    let witness = PruningDomainWitness {
        target: PruningDomainSite {
            fragment: FragmentId::new(0),
            scan: NodeId::new(u32::MAX),
            occurrence: ProviderReadOccurrenceId::new(0),
            field: PruningDomainField::Unenforced,
        },
        sources: Box::from([PruningSourceWitness {
            responsibility: PredicateResponsibilityRef {
                fragment: FragmentId::new(u32::MAX),
                site: PredicateSourceSite {
                    node: NodeId::new(0),
                    root: novarocks_expression_contract::RootOrdinal::new(0),
                },
                use_id: ExpressionUseId::new(u32::MAX),
            },
            context: context(0, u32::MAX, EvaluationDemand::TruthOnly),
            conjunct_path: Box::from([0]),
            input_path: Box::from([PruningInputEdge {
                consumer: NodeId::new(u32::MAX),
                input_ordinal: 0,
                producer: NodeId::new(0),
            }]),
            columns: Box::from([PruningColumnTrace {
                column: ScanColumnId::new(u32::MAX as usize),
                values: (0..values)
                    .map(|i| ValueId::new(if i % 2 == 0 { 0 } else { u32::MAX }))
                    .collect(),
            }]),
        }]),
    };
    FrozenFragmentPruning::try_new(FragmentId::new(0), vec![witness], &TestControl::default())
        .unwrap()
}
fn pruning_wire() -> wire::FrozenPruning {
    wire::FrozenPruning {
        witnesses: vec![wire::PruningDomainWitness {
            target: Some(wire::PruningDomainSite {
                fragment_id: Some(0),
                scan_id: Some(u32::MAX),
                occurrence_id: Some(0),
                field: wire::PruningDomainField::Unenforced as i32,
            }),
            sources: vec![wire::PruningSourceWitness {
                responsibility: Some(wire::PredicateResponsibilityRef {
                    fragment_id: Some(u32::MAX),
                    node_id: Some(0),
                    root_ordinal: Some(0),
                    use_id: Some(u32::MAX),
                }),
                context: Some(wire::ExpressionEffectContext {
                    use_id: Some(0),
                    region_id: Some(u32::MAX),
                    demand: novarocks_proto_models::physical_control_v2::EvaluationDemand::TruthOnly
                        as i32,
                }),
                conjunct_path: vec![0],
                input_path: vec![wire::PruningInputEdge {
                    consumer_id: Some(u32::MAX),
                    input_ordinal: 0,
                    producer_id: Some(0),
                }],
                columns: vec![wire::PruningColumnTrace {
                    column_ordinal: u32::MAX,
                    value_ids: vec![0, u32::MAX, 0],
                }],
            }],
        }],
    }
}
#[test]
fn pruning_observed_has_independent_wire_and_layout_oracles() {
    let source = one_pruning(3);
    let control = TestControl::default();
    let (wire, facts) = scope(&control, CompilePhase::Encode, |w| {
        encode_frozen_pruning_observed(&source, SOURCE, ceilings(), &mut |_| Ok(()), w)
    })
    .unwrap();
    assert_eq!(wire, pruning_wire());
    assert_eq!(facts.input_node_count, 0);
    assert_eq!(facts.value_reference_count, 0);
    assert_eq!(facts.list_item_count, 8);
    assert_eq!(facts.allocation_requests_upper_bound, 6);
    let layout = Layout::array::<wire::PruningDomainWitness>(1)
        .unwrap()
        .size()
        + Layout::array::<wire::PruningSourceWitness>(1)
            .unwrap()
            .size()
        + Layout::array::<u32>(1).unwrap().size()
        + Layout::array::<wire::PruningInputEdge>(1).unwrap().size()
        + Layout::array::<wire::PruningColumnTrace>(1).unwrap().size()
        + Layout::array::<u32>(3).unwrap().size();
    assert_eq!(facts.allocation_request_bytes_upper_bound, layout);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + layout
    );
    let (owned, read) = scope(&control, CompilePhase::Decode, |w| {
        decode_frozen_pruning_observed(
            FragmentId::new(0),
            &wire,
            SOURCE,
            ceilings(),
            &mut |_| Ok(()),
            w,
        )
    })
    .unwrap();
    assert_eq!(owned, source);
    // Root Vec once; five real nested Vec->Box pairs; target tree and Arc once.
    assert_eq!(read.allocation_requests_upper_bound, 13);
    let target = novarocks_type_contract::owned_resources::btree::node_layout_typed::<
        (NodeId, ProviderReadOccurrenceId, u8),
        (),
    >()
    .unwrap()
    .size();
    let arc = novarocks_type_contract::owned_resources::layout::arc_layout(
        Layout::array::<PruningDomainWitness>(1).unwrap(),
    )
    .unwrap()
    .size();
    let bytes = Layout::array::<PruningDomainWitness>(1).unwrap().size()
        + 2 * (Layout::array::<PruningSourceWitness>(1).unwrap().size()
            + Layout::array::<u32>(1).unwrap().size()
            + Layout::array::<PruningInputEdge>(1).unwrap().size()
            + Layout::array::<PruningColumnTrace>(1).unwrap().size()
            + Layout::array::<ValueId>(3).unwrap().size())
        + target
        + arc;
    assert_eq!(read.allocation_request_bytes_upper_bound, bytes);
}
#[test]
fn calls_observed_reuses_actual_checked_fragment_and_all_nine_fields() {
    let mut f = relation_fixture(2, 0);
    f.calls[0].effects.environment = Box::from([SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::StatementStartUtc,
    }]);
    let calls = f.checked().unwrap();
    let control = TestControl::default();
    let (encoded, facts) = scope(&control, CompilePhase::Encode, |w| {
        encode_frozen_calls_observed(&calls, SOURCE, ceilings(), &mut |_| Ok(()), w)
    })
    .unwrap();
    assert_eq!(
        encoded,
        encode_relation_checked(&calls, &TestControl::default()).unwrap()
    );
    assert_eq!(facts.allocation_requests_upper_bound, 2);
    assert_eq!(facts.list_item_count, 3);
    let (decoded, _) = scope(&control, CompilePhase::Decode, |w| {
        decode_frozen_calls_observed(
            &f.fragment,
            &encoded,
            SOURCE,
            ceilings(),
            &mut |_| Ok(()),
            w,
        )
    })
    .unwrap();
    assert_eq!(decoded, calls);
    let fother = relation_fixture(1, u32::MAX);
    assert!(matches!(
        scope(&control, CompilePhase::Decode, |w| {
            decode_frozen_calls_observed(
                &fother.fragment,
                &encoded,
                SOURCE,
                ceilings(),
                &mut |_| Ok(()),
                w,
            )
        }),
        Err(E::Calls(FrozenCallError::InvalidSite))
    ));
    assert!(
        trace(&control)
            .iter()
            .any(|(p, _)| *p == CompilePhase::Decode)
    );
}
#[test]
fn semantic_projection_exact_and_under_live_axes_preserve_typed_numeric_origin() {
    let source = one_pruning(3);
    let c = TestControl::default();
    let (_, facts) = scope(&c, CompilePhase::Encode, |w| {
        encode_frozen_pruning_observed(&source, SOURCE, ceilings(), &mut |_| Ok(()), w)
    })
    .unwrap();
    let e = exact(facts);
    scope(&c, CompilePhase::Encode, |w| {
        encode_frozen_pruning_observed(&source, SOURCE, e, &mut |_| Ok(()), w)
    })
    .unwrap();
    // Two absence axes are truly zero. Each other actual component axis is live.
    assert_eq!((e.max_input_nodes, e.max_value_references), (0, 0));
    for axis in 0..5 {
        let mut l = e;
        match axis {
            0 => l.max_list_items -= 1,
            1 => l.max_allocation_requests -= 1,
            2 => l.max_allocation_request_bytes -= 1,
            3 => l.max_coexisting_source_and_request_bytes -= 1,
            _ => l.max_work -= 1,
        };
        assert!(matches!(
            scope(
                &c,
                CompilePhase::Encode,
                |w| encode_frozen_pruning_observed(&source, SOURCE, l, &mut |_| Ok(()), w)
            ),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
    }
    assert!(matches!(
        scope(
            &c,
            CompilePhase::Encode,
            |w| encode_frozen_pruning_observed(&source, 0, ceilings(), &mut |_| Ok(()), w)
        ),
        Err(E::InvalidShape(_))
    ));
}
#[test]
fn semantic_owned_small_success_and_ordinary_tails_keep_every_original_control_prefix() {
    let f = relation_fixture(1, 0);
    let calls = f.checked().unwrap();
    let (wire, _) = scope(&TestControl::default(), CompilePhase::Encode, |w| {
        encode_frozen_calls_observed(&calls, SOURCE, ceilings(), &mut |_| Ok(()), w)
    })
    .unwrap();
    prefixes(CompilePhase::Encode, |c| {
        scope(c, CompilePhase::Encode, |w| {
            encode_frozen_calls_observed(&calls, SOURCE, ceilings(), &mut |_| Ok(()), w)
        })
    });
    prefixes(CompilePhase::Decode, |c| {
        scope(c, CompilePhase::Decode, |w| {
            decode_frozen_calls_observed(&f.fragment, &wire, SOURCE, ceilings(), &mut |_| Ok(()), w)
        })
    });
    let mut bad = wire.clone();
    bad.entries[0].decimal_overflow_policy = None;
    prefixes(CompilePhase::Decode, |c| {
        scope(c, CompilePhase::Decode, |w| {
            decode_frozen_calls_observed(&f.fragment, &bad, SOURCE, ceilings(), &mut |_| Ok(()), w)
        })
    });
    let source = one_pruning(3);
    let wire = pruning_wire();
    prefixes(CompilePhase::Encode, |c| {
        scope(c, CompilePhase::Encode, |w| {
            encode_frozen_pruning_observed(&source, SOURCE, ceilings(), &mut |_| Ok(()), w)
        })
    });
    prefixes(CompilePhase::Decode, |c| {
        scope(c, CompilePhase::Decode, |w| {
            decode_frozen_pruning_observed(
                FragmentId::new(0),
                &wire,
                SOURCE,
                ceilings(),
                &mut |_| Ok(()),
                w,
            )
        })
    });
    let mut bad = wire.clone();
    bad.witnesses[0].sources.clear();
    prefixes(CompilePhase::Decode, |c| {
        scope(c, CompilePhase::Decode, |w| {
            decode_frozen_pruning_observed(
                FragmentId::new(0),
                &bad,
                SOURCE,
                ceilings(),
                &mut |_| Ok(()),
                w,
            )
        })
    });
    assert!(matches!(
        scope(&TestControl::default(), CompilePhase::Decode, |w| {
            decode_frozen_pruning_observed(
                FragmentId::new(0),
                &bad,
                SOURCE,
                ceilings(),
                &mut |_| Ok(()),
                w,
            )
        }),
        Err(E::Pruning(FrozenPruningError::EmptySources))
    ));
}
#[test]
fn semantic_known_root_requests_win_before_caller_pending_255_late_causes() {
    let f = relation_fixture(1, 0);
    let calls = f.checked().unwrap();
    let (wire, _) = scope(&TestControl::default(), CompilePhase::Encode, |w| {
        encode_frozen_calls_observed(&calls, SOURCE, ceilings(), &mut |_| Ok(()), w)
    })
    .unwrap();
    for cause in causes() {
        let c = TestControl {
            failure: Some((2, cause.clone())),
            events: Mutex::new(Vec::new()),
        };
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let mut l = ceilings();
        l.max_allocation_requests = 1;
        assert!(matches!(
            decode_frozen_calls_observed(&f.fragment, &wire, SOURCE, l, &mut |_| Ok(()), &mut work),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(trace(&c), [(CompilePhase::Decode, 0)]);
        let c = TestControl {
            failure: Some((2, cause)),
            events: Mutex::new(Vec::new()),
        };
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let mut l = ceilings();
        l.max_allocation_requests = 1;
        assert!(matches!(
            decode_frozen_pruning_observed(
                FragmentId::new(0),
                &pruning_wire(),
                SOURCE,
                l,
                &mut |_| Ok(()),
                &mut work
            ),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(trace(&c), [(CompilePhase::Decode, 0)]);
    }
}
#[test]
fn semantic_real_wide_paths_preserve_order_and_sample_actual_quantum() {
    let source = one_pruning(320);
    let c = TestControl::default();
    let (wire, _) = scope(&c, CompilePhase::Encode, |w| {
        encode_frozen_pruning_observed(&source, SOURCE, ceilings(), &mut |_| Ok(()), w)
    })
    .unwrap();
    assert_eq!(wire.witnesses[0].sources[0].columns[0].value_ids.len(), 320);
    for (i, id) in wire.witnesses[0].sources[0].columns[0]
        .value_ids
        .iter()
        .enumerate()
    {
        assert_eq!(*id, if i % 2 == 0 { 0 } else { u32::MAX });
    }
    let baseline = trace(&c);
    let quantum = baseline
        .iter()
        .position(|(_, n)| *n == 256)
        .expect("the real 320-element value loop reaches a quantum");
    for at in [1, quantum + 1, baseline.len()] {
        for cause in causes() {
            let c = TestControl {
                failure: Some((at, cause.clone())),
                events: Mutex::new(Vec::new()),
            };
            assert!(
                matches!(scope(&c,CompilePhase::Encode,|w|encode_frozen_pruning_observed(&source,SOURCE,ceilings(),&mut |_|Ok(()),w)),Err(E::Control(actual)) if actual==cause)
            );
            assert_eq!(trace(&c), baseline[..at]);
        }
    }
    let c = TestControl::default();
    let (decoded, _) = scope(&c, CompilePhase::Decode, |w| {
        decode_frozen_pruning_observed(
            FragmentId::new(0),
            &wire,
            SOURCE,
            ceilings(),
            &mut |_| Ok(()),
            w,
        )
    })
    .unwrap();
    assert_eq!(decoded, source);
    assert!(trace(&c).iter().all(|(p, _)| *p == CompilePhase::Decode));
}

#[test]
fn pruning_original_constructor_prefunds_actual_dynamic_walk_work_independently() {
    let template = one_pruning(3);
    let mut witness = template.witnesses()[0].clone();
    witness.sources = vec![witness.sources[0].clone(); 320].into_boxed_slice();
    let source =
        FrozenFragmentPruning::try_new(FragmentId::new(0), vec![witness], &TestControl::default())
            .unwrap();
    let root = FrozenFragmentPruning::construction_resources(1).unwrap();
    let control = TestControl::default();
    let mut seen = Vec::new();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let owned = FrozenFragmentPruning::try_new_in(
        source.fragment(),
        source.witnesses().to_vec(),
        &mut |facts| {
            seen.push(*facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(owned, source);
    // One witness, 320 actual source occurrences, each with three path/column
    // entries and three values. Source sharing does not deduplicate work.
    let dynamic = 32 * (1 + 320 + 320 * (1 + 1 + 1 + 3));
    let final_facts = *seen.last().unwrap();
    assert_eq!(
        final_facts.allocation_requests_upper_bound,
        root.allocation_requests_upper_bound
    );
    assert_eq!(
        final_facts.allocation_request_bytes_upper_bound,
        root.allocation_request_bytes_upper_bound
    );
    assert_eq!(
        final_facts.cumulative_work_upper_bound,
        root.cumulative_work_upper_bound + dynamic
    );
    let baseline = trace(&control);
    for at in 1..=baseline.len() {
        for cause in causes() {
            let c = TestControl {
                failure: Some((at, cause.clone())),
                events: Mutex::new(Vec::new()),
            };
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode);
            let result = match &mut w {
                Err(cause) => Err(FrozenPruningError::Control(cause.clone())),
                Ok(w) => FrozenFragmentPruning::try_new_in(
                    source.fragment(),
                    source.witnesses().to_vec(),
                    &mut |_| Ok(()),
                    w,
                ),
            };
            let result = if matches!(result, Err(FrozenPruningError::Control(_))) {
                result
            } else {
                w.unwrap()
                    .finish()
                    .map_err(FrozenPruningError::Control)
                    .and(result)
            };
            assert!(matches!(result,Err(FrozenPruningError::Control(actual)) if actual==cause));
            assert_eq!(trace(&c), baseline[..at]);
        }
    }
    for cause in causes() {
        let c = TestControl {
            failure: Some((2, cause)),
            events: Mutex::new(Vec::new()),
        };
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            w.step().unwrap();
        }
        // The complete source-count contribution is captured before the first
        // target-set opaque callback. It is independent of the codec Model.
        let max = root.cumulative_work_upper_bound + 32;
        assert!(matches!(
            FrozenFragmentPruning::try_new_in(
                source.fragment(),
                source.witnesses().to_vec(),
                &mut |facts| {
                    if facts.cumulative_work_upper_bound > max {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
                &mut w
            ),
            Err(FrozenPruningError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(trace(&c), [(CompilePhase::Decode, 0)]);
    }
}
