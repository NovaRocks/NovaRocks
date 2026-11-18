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
use novarocks_expression_contract::RootOrdinal;
use novarocks_physical_plan::PredicateSourceSite;
use novarocks_proto_models::physical_control_v2 as control_wire;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, EvaluationDemand, EvaluationRegionId,
    ExpressionEffectContext,
};
use std::sync::Mutex;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn source() -> PruningSourceWitness {
    PruningSourceWitness {
        responsibility: PredicateResponsibilityRef {
            fragment: FragmentId::new(0),
            site: PredicateSourceSite {
                node: NodeId::new(u32::MAX),
                root: RootOrdinal::new(0),
            },
            use_id: ExpressionUseId::new(0),
        },
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(0),
            region: EvaluationRegionId::new(u32::MAX),
            demand: EvaluationDemand::TruthOnly,
        },
        conjunct_path: Box::from([1, 0, 1]),
        input_path: Box::from([
            PruningInputEdge {
                consumer: NodeId::new(u32::MAX),
                input_ordinal: 0,
                producer: NodeId::new(0),
            },
            PruningInputEdge {
                consumer: NodeId::new(0),
                input_ordinal: u32::MAX,
                producer: NodeId::new(u32::MAX),
            },
        ]),
        columns: Box::from([
            PruningColumnTrace {
                column: ScanColumnId::new(0),
                values: Box::from([
                    ValueId::new(u32::MAX),
                    ValueId::new(0),
                    ValueId::new(u32::MAX),
                ]),
            },
            PruningColumnTrace {
                column: ScanColumnId::new(u32::MAX as usize),
                values: Box::from([ValueId::new(0)]),
            },
        ]),
    }
}

fn frozen_fixture() -> FrozenFragmentPruning {
    FrozenFragmentPruning::try_new(
        FragmentId::new(0),
        vec![
            PruningDomainWitness {
                target: PruningDomainSite {
                    fragment: FragmentId::new(0),
                    scan: NodeId::new(u32::MAX),
                    occurrence: ProviderReadOccurrenceId::new(0),
                    field: PruningDomainField::Enforced,
                },
                sources: Box::from([source()]),
            },
            PruningDomainWitness {
                target: PruningDomainSite {
                    fragment: FragmentId::new(0),
                    scan: NodeId::new(0),
                    occurrence: ProviderReadOccurrenceId::new(u32::MAX),
                    field: PruningDomainField::Unenforced,
                },
                sources: Box::from([source()]),
            },
        ],
        &Control,
    )
    .unwrap()
}

// These are declaration DTOs, not evidence of an actual package path or
// source-to-scan implication. The package checks those separate contracts.
fn expected_source() -> wire::PruningSourceWitness {
    wire::PruningSourceWitness {
        responsibility: Some(wire::PredicateResponsibilityRef {
            fragment_id: Some(0),
            node_id: Some(u32::MAX),
            root_ordinal: Some(0),
            use_id: Some(0),
        }),
        context: Some(wire::ExpressionEffectContext {
            use_id: Some(0),
            region_id: Some(u32::MAX),
            demand: control_wire::EvaluationDemand::TruthOnly as i32,
        }),
        conjunct_path: vec![1, 0, 1],
        input_path: vec![
            wire::PruningInputEdge {
                consumer_id: Some(u32::MAX),
                input_ordinal: 0,
                producer_id: Some(0),
            },
            wire::PruningInputEdge {
                consumer_id: Some(0),
                input_ordinal: u32::MAX,
                producer_id: Some(u32::MAX),
            },
        ],
        columns: vec![
            wire::PruningColumnTrace {
                column_ordinal: 0,
                value_ids: vec![u32::MAX, 0, u32::MAX],
            },
            wire::PruningColumnTrace {
                column_ordinal: u32::MAX,
                value_ids: vec![0],
            },
        ],
    }
}

fn expected_wire() -> wire::FrozenPruning {
    wire::FrozenPruning {
        witnesses: vec![
            wire::PruningDomainWitness {
                target: Some(wire::PruningDomainSite {
                    fragment_id: Some(0),
                    scan_id: Some(u32::MAX),
                    occurrence_id: Some(0),
                    field: wire::PruningDomainField::Enforced as i32,
                }),
                sources: vec![expected_source()],
            },
            wire::PruningDomainWitness {
                target: Some(wire::PruningDomainSite {
                    fragment_id: Some(0),
                    scan_id: Some(0),
                    occurrence_id: Some(u32::MAX),
                    field: wire::PruningDomainField::Unenforced as i32,
                }),
                sources: vec![expected_source()],
            },
        ],
    }
}

fn encode(input: &FrozenFragmentPruning) -> Result<wire::FrozenPruning, Error> {
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode)?;
    let output = encode_pruning(input, &mut work)?;
    work.finish()?;
    Ok(output)
}

fn decode(
    fragment: FragmentId,
    input: &wire::FrozenPruning,
) -> Result<FrozenFragmentPruning, Error> {
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode)?;
    let output = decode_pruning(fragment, input, &mut work, &Control)?;
    work.finish()?;
    Ok(output)
}

#[test]
fn pruning_projection_matches_independent_sparse_ordered_dto_with_zero_and_max_presence() {
    let input = frozen_fixture();
    let expected = expected_wire();
    assert_eq!(encode(&input).unwrap(), expected);
    assert_eq!(decode(FragmentId::new(0), &expected).unwrap(), input);
    let mut max_fragment = expected;
    for witness in &mut max_fragment.witnesses {
        witness.target.as_mut().unwrap().fragment_id = Some(u32::MAX);
        for source in &mut witness.sources {
            source.responsibility.as_mut().unwrap().fragment_id = Some(u32::MAX);
            source.responsibility.as_mut().unwrap().use_id = Some(u32::MAX);
            source.responsibility.as_mut().unwrap().root_ordinal = Some(u32::MAX);
            source.context.as_mut().unwrap().use_id = Some(u32::MAX);
        }
    }
    let decoded = decode(FragmentId::new(u32::MAX), &max_fragment).unwrap();
    assert_eq!(encode(&decoded).unwrap(), max_fragment);
    assert_eq!(
        decoded.witnesses()[0].sources[0]
            .responsibility
            .use_id
            .get(),
        u32::MAX
    );
    assert_eq!(
        decoded.witnesses()[0].sources[0]
            .responsibility
            .site
            .root
            .get(),
        u32::MAX
    );
}

#[test]
fn pruning_missing_mandatory_fields_and_closed_vocabularies_are_rejected() {
    type Mutation = fn(&mut wire::FrozenPruning);
    let mutations: &[Mutation] = &[
        |dto| dto.witnesses[0].target = None,
        |dto| dto.witnesses[0].target.as_mut().unwrap().fragment_id = None,
        |dto| dto.witnesses[0].target.as_mut().unwrap().scan_id = None,
        |dto| dto.witnesses[0].target.as_mut().unwrap().occurrence_id = None,
        |dto| dto.witnesses[0].target.as_mut().unwrap().field = 0,
        |dto| dto.witnesses[0].target.as_mut().unwrap().field = i32::MAX,
        |dto| dto.witnesses[0].sources[0].responsibility = None,
        |dto| {
            dto.witnesses[0].sources[0]
                .responsibility
                .as_mut()
                .unwrap()
                .fragment_id = None
        },
        |dto| {
            dto.witnesses[0].sources[0]
                .responsibility
                .as_mut()
                .unwrap()
                .node_id = None
        },
        |dto| {
            dto.witnesses[0].sources[0]
                .responsibility
                .as_mut()
                .unwrap()
                .root_ordinal = None
        },
        |dto| {
            dto.witnesses[0].sources[0]
                .responsibility
                .as_mut()
                .unwrap()
                .use_id = None
        },
        |dto| dto.witnesses[0].sources[0].context = None,
        |dto| dto.witnesses[0].sources[0].context.as_mut().unwrap().use_id = None,
        |dto| {
            dto.witnesses[0].sources[0]
                .context
                .as_mut()
                .unwrap()
                .region_id = None
        },
        |dto| dto.witnesses[0].sources[0].context.as_mut().unwrap().demand = 0,
        |dto| dto.witnesses[0].sources[0].context.as_mut().unwrap().demand = i32::MAX,
        |dto| dto.witnesses[0].sources[0].input_path[0].consumer_id = None,
        |dto| dto.witnesses[0].sources[0].input_path[0].producer_id = None,
    ];
    for mutate in mutations {
        let mut input = expected_wire();
        mutate(&mut input);
        assert!(decode(FragmentId::new(0), &input).is_err());
    }
}

#[test]
fn pruning_owner_rejects_wrong_fragment_duplicate_target_and_empty_sources() {
    let input = expected_wire();
    assert!(matches!(
        decode(FragmentId::new(1), &input),
        Err(Error::Pruning(FrozenPruningError::WrongFragment))
    ));
    let mut duplicate = input.clone();
    duplicate.witnesses.push(duplicate.witnesses[0].clone());
    assert!(matches!(
        decode(FragmentId::new(0), &duplicate),
        Err(Error::Pruning(FrozenPruningError::DuplicateTarget))
    ));
    let mut empty = input;
    empty.witnesses[0].sources.clear();
    assert!(matches!(
        decode(FragmentId::new(0), &empty),
        Err(Error::Pruning(FrozenPruningError::EmptySources))
    ));
    let empty = wire::FrozenPruning::default();
    assert!(
        decode(FragmentId::new(u32::MAX), &empty)
            .unwrap()
            .witnesses()
            .is_empty()
    );
}

#[test]
fn pruning_conjunct_depth_uses_actual_root_inclusive_boundary() {
    let mut input = expected_wire();
    input.witnesses[0].sources[0].conjunct_path = vec![1; MAX_CONTROL_DEPTH - 1];
    let frozen = decode(FragmentId::new(0), &input).unwrap();
    assert_eq!(encode(&frozen).unwrap(), input);
    input.witnesses[0].sources[0].conjunct_path.push(0);
    assert!(matches!(
        decode(FragmentId::new(0), &input),
        Err(Error::Pruning(FrozenPruningError::TooLarge))
    ));
}

fn budget_wire(first_values: usize, second_values: usize) -> wire::FrozenPruning {
    let witness = |scan: u32, count| wire::PruningDomainWitness {
        target: Some(wire::PruningDomainSite {
            fragment_id: Some(0),
            scan_id: Some(scan),
            occurrence_id: Some(0),
            field: wire::PruningDomainField::Enforced as i32,
        }),
        sources: vec![wire::PruningSourceWitness {
            conjunct_path: vec![],
            input_path: vec![],
            columns: vec![wire::PruningColumnTrace {
                column_ordinal: 0,
                value_ids: vec![u32::MAX; count],
            }],
            ..expected_source()
        }],
    };
    wire::FrozenPruning {
        witnesses: vec![witness(0, first_values), witness(u32::MAX, second_values)],
    }
}

#[test]
fn pruning_combined_budget_is_not_reset_per_witness_or_source() {
    // Two witnesses + two sources + two columns plus their actual values.
    let half = (MAX_CONTROL_USE_REFERENCES - 6) / 2;
    let near = budget_wire(half, half);
    let frozen = decode(FragmentId::new(0), &near).unwrap();
    assert_eq!(
        frozen.dynamic_items_observed(&Control).unwrap(),
        MAX_CONTROL_USE_REFERENCES
    );
    assert_eq!(encode(&frozen).unwrap(), near);
    let over = budget_wire(half, half + 1);
    assert!(matches!(
        decode(FragmentId::new(0), &over),
        Err(Error::Pruning(FrozenPruningError::TooLarge))
    ));
    let mut combined_source = near;
    let second_source = combined_source
        .witnesses
        .pop()
        .unwrap()
        .sources
        .pop()
        .unwrap();
    combined_source.witnesses[0].sources.push(second_source);
    // Removing one witness leaves 65535 items; two additional path edges
    // cross the same aggregate table bound even within a single target.
    combined_source.witnesses[0].sources[0].conjunct_path = vec![0, 1];
    assert!(matches!(
        decode(FragmentId::new(0), &combined_source),
        Err(Error::Pruning(FrozenPruningError::TooLarge))
    ));
}

#[cfg(target_pointer_width = "64")]
#[test]
fn pruning_encode_refuses_unrepresentable_host_column_ordinal_before_projection() {
    let mut witness = frozen_fixture().witnesses()[0].clone();
    witness.sources[0].columns[0].column = ScanColumnId::new(u32::MAX as usize + 1);
    let frozen =
        FrozenFragmentPruning::try_new(FragmentId::new(0), vec![witness], &Control).unwrap();
    assert!(matches!(
        encode(&frozen),
        Err(Error::InvalidShape(
            "pruning column ordinal does not fit uint32"
        ))
    ));
}

#[derive(Clone, Copy)]
enum Stop {
    Entry,
    Quantum,
    Tail,
}
struct Refuse {
    phase: CompilePhase,
    error: CompileControlError,
    stop: Stop,
    calls: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Refuse {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.calls.lock().unwrap().push((phase, units));
        if phase == self.phase
            && match self.stop {
                Stop::Entry => units == 0,
                Stop::Quantum => units == 256,
                Stop::Tail => units > 0 && units < 256,
            }
        {
            Err(self.error.clone())
        } else {
            Ok(())
        }
    }
}

#[test]
fn pruning_preserves_all_three_control_failures_at_entry_quantum_and_tail() {
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for phase in [CompilePhase::Encode, CompilePhase::Decode] {
            for stop in [Stop::Entry, Stop::Quantum, Stop::Tail] {
                let control = Refuse {
                    phase,
                    error: error.clone(),
                    stop,
                    calls: Mutex::new(vec![]),
                };
                let input = match stop {
                    Stop::Quantum => budget_wire(320, 0),
                    _ => expected_wire(),
                };
                let frozen = decode(FragmentId::new(0), &input).unwrap();
                let result = (|| -> Result<(), Error> {
                    let mut work = CompileCheckpoints::try_new(&control, phase)?;
                    if phase == CompilePhase::Encode {
                        encode_pruning(&frozen, &mut work)?;
                    } else {
                        decode_pruning(FragmentId::new(0), &input, &mut work, &control)?;
                    }
                    work.finish()?;
                    Ok(())
                })();
                assert!(matches!(result, Err(Error::Control(actual)) if actual == error));
                let calls = control.calls.lock().unwrap();
                let last = calls.last().unwrap();
                assert_eq!(last.0, phase);
                match stop {
                    Stop::Entry => assert_eq!(last.1, 0),
                    Stop::Quantum => assert_eq!(last.1, 256),
                    Stop::Tail => assert!((1..256).contains(&last.1)),
                }
            }
        }
    }
}

#[test]
fn pruning_real_owner_validation_control_is_flattened_after_dto_projection() {
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Refuse {
            phase: CompilePhase::Validate,
            error: error.clone(),
            stop: Stop::Entry,
            calls: Mutex::new(vec![]),
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let result = decode_pruning(FragmentId::new(0), &expected_wire(), &mut work, &control);
        assert!(matches!(result, Err(Error::Control(actual)) if actual == error));
        assert_eq!(
            control.calls.lock().unwrap().last(),
            Some(&(CompilePhase::Validate, 0))
        );
    }
}
