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
use crate::{NodeId, PlanLimits};
use std::{
    cell::Cell,
    error::Error,
    sync::{Arc, Mutex},
};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
    phase: Option<CompilePhase>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, self.phase.unwrap_or(CompilePhase::Validate));
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        if let Some((at, _)) = self.refusal {
            assert!(index <= at, "callback after primary refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((at, cause)) if at == index => Err(cause),
            _ => Ok(()),
        }
    }
}
fn definition(id: u32) -> ExprNode {
    ExprNode {
        id: ExprId::new(id),
        owner: NodeId::new(999),
        lambda_scope: None,
        ty: ValueType::new(DataType::Int64, true),
        kind: ExprKind::Value(ValueId::new(id)),
    }
}
fn check_prefixes(
    definitions: &[ExprNode],
    limits: &PlanLimits,
    ordinary: Option<ExprArenaConstructionError>,
) -> Vec<u32> {
    let control = Control::default();
    let result =
        ExprArena::try_from_definitions_observed(definitions.iter().cloned(), limits, &control);
    match ordinary {
        Some(expected) => assert_eq!(result.unwrap_err(), expected),
        None => assert!(result.is_ok()),
    }
    let trace = control.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    assert!(trace.iter().all(|units| *units <= 256));
    // Wide trees contain opaque std operations. Sample their real boundaries
    // without turning one wide acceptance case into quadratic replay work.
    let mut samples: Vec<_> = if definitions.len() <= 64 {
        (0..trace.len()).collect()
    } else {
        vec![0, 1, 2, trace.len() / 2, trace.len() - 2, trace.len() - 1]
    };
    samples.sort_unstable();
    samples.dedup();
    for at in samples {
        for cause in CAUSES {
            let refusal = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((at, cause.clone())),
                phase: None,
            };
            let error = ExprArena::try_from_definitions_observed(
                definitions.iter().cloned(),
                limits,
                &refusal,
            )
            .unwrap_err();
            assert_eq!(error, ExprArenaConstructionError::Control(cause.clone()));
            assert_eq!(*refusal.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
    }
    trace
}

#[test]
fn sparse_definition_constructor_preserves_zero_max_full_source_and_ordered_occurrences() {
    let child = Arc::new(
        arrow_schema::Field::new("authored-child", DataType::Int64, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider".into(),
                "exact".into(),
            )]),
        ),
    );
    let mut root = definition(u32::MAX);
    root.lambda_scope = Some(ExprId::new(41));
    root.ty = ValueType::new(DataType::List(Arc::clone(&child)), true);
    let DataType::List(child) = root.ty.data_type() else {
        unreachable!()
    };
    let child = Arc::clone(child);
    root.kind = ExprKind::Conjunction {
        args: vec![ExprId::new(0), ExprId::new(41), ExprId::new(0)].into_boxed_slice(),
    };
    let definitions = vec![root, definition(0), definition(41)];
    let arena = ExprArena::try_from_definitions_observed(
        definitions.into_iter(),
        &PlanLimits::FROZEN,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        arena.iter().map(|(id, _)| id.get()).collect::<Vec<_>>(),
        [0, 41, u32::MAX]
    );
    let stored = arena.get(ExprId::new(u32::MAX)).unwrap();
    assert_eq!(stored.owner, NodeId::new(999));
    assert_eq!(stored.lambda_scope, Some(ExprId::new(41)));
    let DataType::List(stored_child) = &stored.ty.data_type() else {
        panic!("complete source carrier changed")
    };
    assert!(Arc::ptr_eq(stored_child, &child));
    let ExprKind::Conjunction { args } = &stored.kind else {
        panic!("definition kind changed")
    };
    assert_eq!(
        args.as_ref(),
        [ExprId::new(0), ExprId::new(41), ExprId::new(0)]
    );
    // This API does not reinterpret types, lexical scopes or graph shape.
    // The deliberately unvalidated shape is left to the original fragment author.
}

#[test]
fn sparse_definition_constructor_observes_real_opaque_boundaries_and_success_tail() {
    let mut definitions: Vec<_> = (0..319).map(definition).collect();
    definitions.push(definition(u32::MAX));
    let trace = check_prefixes(&definitions, &PlanLimits::FROZEN, None);
    // Count and actual iterator pulls contribute the same completed units;
    // std entry/insertion is now observed before and after every operation.
    // No callback represents cooperative work inside that opaque library.
    assert_eq!(trace.iter().sum::<u32>(), 641);
    assert_eq!(trace[0], 0);
    assert_eq!(trace[1], 2);
    assert_eq!(trace.last(), Some(&0));
    assert!(trace[2..trace.len() - 1].iter().all(|units| *units == 1));
    assert_eq!(check_prefixes(&[], &PlanLimits::FROZEN, None), [0, 1]);
}

#[test]
fn sparse_definition_duplicate_refusal_keeps_first_id_and_ordinary_tail() {
    let mut definitions: Vec<_> = (0..129).map(definition).collect();
    let mut duplicate = definition(1);
    duplicate.ty = ValueType::new(DataType::Float64, false);
    definitions.push(duplicate);
    assert_eq!(
        check_prefixes(
            &definitions,
            &PlanLimits::FROZEN,
            Some(ExprArenaConstructionError::DuplicateDefinition(
                ExprId::new(1)
            )),
        )
        .iter()
        .sum::<u32>(),
        261
    );
    assert_eq!(
        check_prefixes(
            &[definition(u32::MAX), definition(u32::MAX)],
            &PlanLimits::FROZEN,
            Some(ExprArenaConstructionError::DuplicateDefinition(
                ExprId::new(u32::MAX)
            )),
        )
        .iter()
        .sum::<u32>(),
        5
    );
}

#[test]
fn sparse_definition_count_gate_uses_caller_limits_before_source_pulls() {
    let pulls = Cell::new(0usize);
    let control = Control::default();
    let source = std::iter::repeat_n(definition(0), PlanLimits::FROZEN.fragment_expressions + 1)
        .inspect(|_| pulls.set(pulls.get() + 1));
    assert_eq!(
        ExprArena::try_from_definitions_observed(source, &PlanLimits::FROZEN, &control)
            .unwrap_err(),
        ExprArenaConstructionError::TooManyDefinitions
    );
    assert_eq!(pulls.get(), 0);
    assert_eq!(*control.trace.lock().unwrap(), [0, 1]);
    let limits = PlanLimits {
        fragment_expressions: 3,
        ..PlanLimits::FROZEN
    };
    check_prefixes(
        &[definition(0), definition(1), definition(u32::MAX)],
        &limits,
        None,
    );
    check_prefixes(
        &[
            definition(0),
            definition(1),
            definition(2),
            definition(u32::MAX),
        ],
        &limits,
        Some(ExprArenaConstructionError::TooManyDefinitions),
    );
    let empty_only = PlanLimits {
        fragment_expressions: 0,
        ..PlanLimits::FROZEN
    };
    check_prefixes(&[], &empty_only, None);
    check_prefixes(
        &[definition(u32::MAX)],
        &empty_only,
        Some(ExprArenaConstructionError::TooManyDefinitions),
    );
}

#[test]
fn sparse_definition_constructor_rejects_lying_lengths_before_excess_insertion_or_publication() {
    struct Lying<'a> {
        declared: usize,
        actual: usize,
        pulls: &'a Cell<usize>,
    }
    impl Iterator for Lying<'_> {
        type Item = ExprNode;
        fn next(&mut self) -> Option<Self::Item> {
            let next = self.pulls.get();
            if next == self.actual {
                return None;
            }
            self.pulls.set(next + 1);
            Some(definition(next as u32))
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.declared, Some(self.declared))
        }
    }
    impl ExactSizeIterator for Lying<'_> {
        fn len(&self) -> usize {
            self.declared
        }
    }
    for (declared, actual, expected_pulls, expected_units) in [
        (0, 320, 1, 2),
        (1, 320, 2, 4),
        (128, 320, 129, 258),
        (321, 320, 320, 641),
        (1, 0, 0, 1),
    ] {
        let pulls = Cell::new(0);
        let control = Control::default();
        let error = ExprArena::try_from_definitions_observed(
            Lying {
                declared,
                actual,
                pulls: &pulls,
            },
            &PlanLimits::FROZEN,
            &control,
        )
        .unwrap_err();
        assert_eq!(
            error,
            ExprArenaConstructionError::DefinitionCountMismatch {
                declared,
                actual: expected_pulls,
            }
        );
        // The first excess source object may be produced by the caller, but
        // its map entry is never allocated or inserted. No partial arena exits.
        assert_eq!(pulls.get(), expected_pulls);
        let expected_trace = control.trace.lock().unwrap().clone();
        assert_eq!(expected_trace.iter().sum::<u32>(), expected_units);
        let mut samples = vec![0, expected_trace.len() / 2, expected_trace.len() - 1];
        if expected_pulls <= 64 {
            samples = (0..expected_trace.len()).collect();
        }
        samples.sort_unstable();
        samples.dedup();
        for at in samples {
            for cause in CAUSES {
                let pulls = Cell::new(0);
                let refusal = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((at, cause.clone())),
                    phase: None,
                };
                assert_eq!(
                    ExprArena::try_from_definitions_observed(
                        Lying {
                            declared,
                            actual,
                            pulls: &pulls
                        },
                        &PlanLimits::FROZEN,
                        &refusal,
                    )
                    .unwrap_err(),
                    ExprArenaConstructionError::Control(cause)
                );
                assert_eq!(*refusal.trace.lock().unwrap(), expected_trace[..=at]);
                assert!(pulls.get() <= expected_pulls);
            }
        }
    }
}

#[test]
fn sparse_definition_composition_lends_original_decode_scope_without_entry_or_footer() {
    for duplicate in [false, true] {
        let definitions = vec![
            definition(0),
            definition(if duplicate { 0 } else { u32::MAX }),
        ];
        let run = |c: &Control| -> Result<ExprArena, ExprArenaConstructionError> {
            let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
            work.step()?;
            let result = ExprArena::try_from_definitions_in(
                definitions.clone().into_iter(),
                &PlanLimits::FROZEN,
                &mut work,
            );
            if matches!(&result, Err(ExprArenaConstructionError::Control(_))) {
                return result;
            }
            let before = c.trace.lock().unwrap().clone();
            // The delegated constructor did not open another phase or finish.
            assert_eq!(before.iter().filter(|u| **u == 0).count(), 1);
            assert_eq!(before.iter().sum::<u32>(), 6);
            work.finish()?;
            result
        };
        let c = Control {
            phase: Some(CompilePhase::Decode),
            ..Control::default()
        };
        let result = run(&c);
        if duplicate {
            assert_eq!(
                result.unwrap_err(),
                ExprArenaConstructionError::DuplicateDefinition(ExprId::new(0))
            );
        } else {
            assert_eq!(
                result
                    .unwrap()
                    .iter()
                    .map(|(id, _)| id.get())
                    .collect::<Vec<_>>(),
                [0, u32::MAX]
            );
        }
        let trace = c.trace.lock().unwrap().clone();
        assert_eq!(trace.last(), Some(&0));
        for at in 0..trace.len() {
            for cause in CAUSES {
                let c = Control {
                    phase: Some(CompilePhase::Decode),
                    refusal: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert_eq!(
                    run(&c).unwrap_err(),
                    ExprArenaConstructionError::Control(cause)
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
