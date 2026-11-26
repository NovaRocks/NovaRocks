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
use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::{collections::HashMap, sync::Mutex};

const SOURCE: usize = 1024 * 1024;
const WORK: usize = 3 * 1024 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn run(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source: usize,
    maximum: usize,
    control: &Control,
) -> Result<VerifiedTypeBinding, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = verify_type_binding(left, right, source, maximum, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    // Only the surrounding caller owns entry and ordinary/success publication.
    work.finish()?;
    result
}

fn nested(changed: bool) -> FunctionValueType {
    let mut metadata = HashMap::with_capacity(256);
    for index in 0..256 {
        metadata.insert(format!("deleted-{index}"), "temporary".to_owned());
    }
    metadata.retain(|key, _| key == "deleted-0");
    metadata.insert(
        "k".repeat(2051),
        if changed {
            "different".into()
        } else {
            "v".repeat(3073)
        },
    );
    FunctionValueType::new(
        DataType::Struct(
            vec![Field::new("payload", DataType::Int64, true).with_metadata(metadata)].into(),
        ),
        true,
    )
}

#[test]
fn borrowed_type_binding_ignores_source_attributes_but_preserves_scalar_parameters() {
    let left = nested(false);
    let independently_authored = nested(false);
    assert!(
        run(
            &left,
            &independently_authored,
            SOURCE,
            WORK,
            &Control::default()
        )
        .unwrap()
        .matches()
    );
    assert!(
        run(&left, &nested(true), SOURCE, WORK, &Control::default())
            .unwrap()
            .matches()
    );
    let no_zone = FunctionValueType::new(DataType::Timestamp(TimeUnit::Nanosecond, None), true);
    let empty_zone = FunctionValueType::new(
        DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
        true,
    );
    assert!(
        !run(&no_zone, &empty_zone, SOURCE, WORK, &Control::default())
            .unwrap()
            .matches()
    );
    #[allow(deprecated)]
    let dictionaries = [7, 9].map(|id| {
        FunctionValueType::new(
            DataType::Struct(
                vec![Field::new_dict(
                    "encoded",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                    id,
                    false,
                )]
                .into(),
            ),
            true,
        )
    });
    assert!(
        run(
            &dictionaries[0],
            &dictionaries[1],
            SOURCE,
            WORK,
            &Control::default()
        )
        .unwrap()
        .matches()
    );
}

#[test]
fn borrowed_type_binding_checks_nested_nominal_semantics_before_publication() {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let author = |json: bool| {
        let storage = ValueTypeStorageLifetime::pure();
        let child = ValueTypeRef::try_new(
            if json {
                ValueKind::Nominal(ValueLogicalType::Json)
            } else {
                ValueKind::Binary
            },
            true,
            storage.clone(),
        )
        .unwrap();
        let semantic = ValueTypeRef::try_new(
            ValueKind::Struct(vec![ValueField::new("payload", child, storage.clone())].into()),
            true,
            storage.clone(),
        )
        .unwrap();
        let layout = ValueLayout::try_new(
            &DataType::Struct(vec![Field::new("payload", DataType::LargeBinary, true)].into()),
            storage,
        )
        .unwrap();
        FunctionValueType::try_from_value(semantic, layout).unwrap()
    };
    let json = author(true);
    let binary = author(false);
    assert_eq!(json.data_type(), binary.data_type());
    every_boundary(&json, &binary, SOURCE, WORK, Some(false));
    every_boundary(&json, &author(true), SOURCE, WORK, Some(true));
}

#[test]
fn borrowed_type_binding_flags_stop_before_cached_pair_without_skipping_equal_pointer_compare() {
    let left = FunctionValueType::new(DataType::Int64, false);
    let right = FunctionValueType::new(DataType::Int64, true);
    let early = run(&left, &right, SOURCE, 2, &Control::default()).unwrap();
    assert!(!early.matches());
    assert_eq!(early.work_upper_bound(), 2);
    assert!(matches!(
        run(&left, &left, SOURCE, 2, &Control::default()),
        Err(TypeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    let same = run(&left, &left, SOURCE, WORK, &Control::default()).unwrap();
    assert!(same.matches());
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
    assert!(
        !run(&uuid, &physical, SOURCE, WORK, &Control::default())
            .unwrap()
            .matches()
    );
}

#[test]
fn borrowed_type_binding_source_floor_is_separate_from_actual_comparison_work() {
    let left = nested(false);
    let right = nested(false);
    let facts = run(&left, &right, SOURCE, WORK, &Control::default()).unwrap();
    assert!(
        run(
            &left,
            &right,
            SOURCE,
            facts.work_upper_bound(),
            &Control::default()
        )
        .unwrap()
        .matches()
    );
    assert!(matches!(
        run(
            &left,
            &right,
            SOURCE,
            facts.work_upper_bound() - 1,
            &Control::default()
        ),
        Err(TypeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    let larger = run(&left, &right, SOURCE + 4096, WORK, &Control::default()).unwrap();
    assert_eq!(larger.work_upper_bound(), facts.work_upper_bound());
    assert!(matches!(
        run(&left, &right, 0, WORK, &Control::default()),
        Err(TypeCodecError::InvalidShape(_))
    ));
}

fn every_boundary(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source: usize,
    maximum: usize,
    expected_matches: Option<bool>,
) -> Vec<u32> {
    let recording = Control::default();
    let result = run(left, right, source, maximum, &recording);
    match expected_matches {
        Some(expected) => assert_eq!(result.unwrap().matches(), expected),
        None => assert!(matches!(result, Err(TypeCodecError::InvalidShape(_)))),
    }
    let expected = recording.trace.lock().unwrap().clone();
    for stop in 0..expected.len() {
        for cause in CAUSES {
            let refusing = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(matches!(
                run(left, right, source, maximum, &refusing),
                Err(TypeCodecError::Control(actual)) if actual == cause
            ));
            assert_eq!(*refusing.trace.lock().unwrap(), expected[..=stop]);
        }
    }
    expected
}

#[test]
fn borrowed_type_binding_success_mismatch_and_ordinary_tails_preserve_every_control_prefix() {
    let left = nested(false);
    every_boundary(&left, &nested(false), SOURCE, WORK, Some(true));
    every_boundary(&left, &nested(true), SOURCE, WORK, Some(true));
    let renamed = FunctionValueType::new(
        DataType::Struct(vec![Field::new("renamed", DataType::Int64, true)].into()),
        true,
    );
    every_boundary(&left, &renamed, SOURCE, WORK, Some(false));
    every_boundary(&left, &nested(false), 0, WORK, None);
}

#[test]
fn borrowed_type_binding_wide_real_comparison_observes_quantum_and_every_control_prefix() {
    let author = || {
        FunctionValueType::new(
            DataType::Struct(
                (0..320)
                    .map(|index| Field::new(format!("column-{index}"), DataType::Int64, false))
                    .collect(),
            ),
            true,
        )
    };
    let left = author();
    let right = author();
    let trace = every_boundary(&left, &right, SOURCE, WORK, Some(true));
    assert!(
        trace.contains(&256),
        "actual comparison must reach its quantum"
    );
}

#[test]
fn numeric_work_refusal_preserves_resource_before_later_controller_or_footer() {
    let left = nested(false);
    let right = nested(false);
    let upper = run(&left, &right, SOURCE, WORK, &Control::default())
        .unwrap()
        .work_upper_bound();
    for maximum in [0, 2, upper - 1] {
        let baseline = Control::default();
        assert!(matches!(
            run(&left, &right, SOURCE, maximum, &baseline),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        let trace = baseline.trace.into_inner().unwrap();
        assert_eq!(trace.first(), Some(&0));
        // A later callback would introduce a second cause after numerical
        // admission already refused. It must never execute, including finish.
        for cause in CAUSES {
            let forbidden = Control {
                refusal: Some((trace.len(), cause)),
                ..Control::default()
            };
            assert!(matches!(
                run(&left, &right, SOURCE, maximum, &forbidden),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*forbidden.trace.lock().unwrap(), trace);
        }
        for stop in 0..trace.len() {
            for cause in CAUSES {
                let refusing = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(matches!(run(&left, &right, SOURCE, maximum, &refusing),
                    Err(TypeCodecError::Control(actual)) if actual == cause));
                assert_eq!(*refusing.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

fn preflight_with_pending(
    left: &FunctionValueType,
    right: &FunctionValueType,
    source: usize,
    maximum: usize,
    pending: usize,
    control: &Control,
) -> Result<BoundTypeComparisonFacts, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    // This unit-level priming exercises the actual borrowed owner's first
    // flush; the original caller's pending work is not a new type traversal.
    for _ in 0..pending {
        work.step()?;
    }
    let result = preflight_type_binding(left, right, source, maximum, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn borrowed_type_known_prefix_refusal_precedes_pending_255_for_all_root_flags() {
    let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
    let same_flags_other_carrier = FunctionValueType::new(DataType::Int64, false);
    let nullable = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    for (right, matching_flags) in [
        (&physical, true),
        (&same_flags_other_carrier, true),
        (&nullable, false),
        (&uuid, false),
    ] {
        let expected_prefix = 2;
        let bound = type_binding_prefix_work_upper_bound(&physical, right, SOURCE).unwrap();
        assert_eq!(bound.work_upper_bound(), expected_prefix);
        assert_eq!(bound.flags_match(), matching_flags);
        for cause in CAUSES {
            let control = Control {
                refusal: Some((1, cause)),
                ..Control::default()
            };
            assert!(matches!(
                preflight_with_pending(
                    &physical,
                    right,
                    SOURCE,
                    expected_prefix - 1,
                    255,
                    &control,
                ),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*control.trace.lock().unwrap(), [0]);
        }
    }
}

#[test]
fn borrowed_type_exact_cached_bound_and_original_success_tails_keep_their_trace() {
    let left = FunctionValueType::new(DataType::Int64, false);
    let nullable = FunctionValueType::new(DataType::Int64, true);
    let full = 2 + 8 + 2 * left.comparison_work_upper_bound().unwrap();
    for pending in [0, 255] {
        for (right, maximum, bound, flags) in [(&left, full, full, true), (&nullable, 2, 2, false)]
        {
            let control = Control::default();
            let proof =
                preflight_with_pending(&left, right, SOURCE, maximum, pending, &control).unwrap();
            assert_eq!(proof.work_upper_bound(), bound);
            assert_eq!(proof.flags_match(), flags);
            let trace = control.trace.lock().unwrap().clone();
            assert_eq!(trace[0], 0);
            assert_eq!(trace[1], pending as u32);
            for at in 0..trace.len() {
                for cause in CAUSES {
                    let control = Control {
                        refusal: Some((at, cause.clone())),
                        ..Default::default()
                    };
                    assert!(
                        matches!(preflight_with_pending(&left, right, SOURCE, maximum, pending, &control), Err(TypeCodecError::Control(actual)) if actual == cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
        // The complete cached pair is already known before the pending flush.
        for cause in CAUSES {
            let control = Control {
                refusal: Some((1, cause)),
                ..Default::default()
            };
            assert!(matches!(
                preflight_with_pending(&left, &left, SOURCE, full - 1, pending, &control),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*control.trace.lock().unwrap(), [0]);
        }
    }
}

#[test]
fn borrowed_type_pure_prefix_keeps_original_ordinary_missing_source_observations() {
    let left = FunctionValueType::new(DataType::Int64, false);
    for (source, maximum, expected_message, expected_trace) in [(
        0,
        0,
        "borrowed type source invoice omits original inline roots",
        vec![0, 255, 1],
    )] {
        let control = Control::default();
        assert!(matches!(
            preflight_with_pending(&left, &left, source, maximum, 255, &control),
            Err(TypeCodecError::InvalidShape(message)) if message == expected_message
        ));
        assert_eq!(*control.trace.lock().unwrap(), expected_trace);
        for at in 0..expected_trace.len() {
            for cause in CAUSES {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert!(matches!(
                    preflight_with_pending(&left, &left, source, maximum, 255, &control),
                    Err(TypeCodecError::Control(actual)) if actual == cause
                ));
                assert_eq!(*control.trace.lock().unwrap(), expected_trace[..=at]);
            }
        }
    }
}

#[test]
fn admitted_comparison_preserves_original_bound_trace_and_every_actual_control_exit() {
    let left = nested(false);
    let right = nested(false);
    let run_admitted = |control: &Control| -> Result<VerifiedTypeBinding, TypeCodecError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let mut last = 0;
        let result = verify_type_binding_admitted(
            &left,
            &right,
            SOURCE,
            WORK,
            &mut |facts| {
                assert!(facts.work_upper_bound() >= last);
                last = facts.work_upper_bound();
                Ok::<_, TypeCodecError>(())
            },
            &mut work,
        );
        if matches!(&result, Err(TypeCodecError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let original_control = Control::default();
    let original = run(&left, &right, SOURCE, WORK, &original_control).unwrap();
    assert!(original.matches());
    let trace = original_control.trace.lock().unwrap().clone();
    let admitted_control = Control::default();
    let admitted = run_admitted(&admitted_control).unwrap();
    assert!(admitted.matches());
    assert_eq!(admitted.work_upper_bound(), original.work_upper_bound());
    assert_eq!(*admitted_control.trace.lock().unwrap(), trace);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(run_admitted(&control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn admitted_comparison_known_cached_pair_refusal_precedes_pending_observation() {
    let left = nested(false);
    let full = 2 + 8 + 2 * left.comparison_work_upper_bound().unwrap();
    for stop_at in [1, full - 1] {
        for cause in CAUSES {
            let control = Control {
                refusal: Some((1, cause)),
                ..Default::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let result = verify_type_binding_admitted(
                &left,
                &left,
                SOURCE,
                WORK,
                &mut |facts| {
                    if facts.work_upper_bound() > stop_at {
                        Err(TypeCodecError::Control(
                            CompileControlError::ResourceExhausted,
                        ))
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            );
            assert!(matches!(
                result,
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*control.trace.lock().unwrap(), [0]);
        }
    }
}

#[test]
fn independent_deep_owners_cover_foreign_equal_and_mismatching_comparison_work() {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let author = |json: bool, timezone: &str| {
        let storage = ValueTypeStorageLifetime::pure();
        let document = ValueTypeRef::try_new(
            if json {
                ValueKind::Nominal(ValueLogicalType::Json)
            } else {
                ValueKind::Binary
            },
            true,
            storage.clone(),
        )
        .unwrap();
        let list = ValueTypeRef::try_new(ValueKind::List(document), true, storage.clone()).unwrap();
        let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone.into()));
        let stamp =
            ValueTypeRef::try_new(ValueKind::Scalar(timestamp.clone()), false, storage.clone())
                .unwrap();
        let name = "界".repeat(300);
        let semantic = ValueTypeRef::try_new(
            ValueKind::Struct(Box::from([
                ValueField::new(name.clone(), list, storage.clone()),
                ValueField::new("stamp", stamp, storage.clone()),
            ])),
            true,
            storage.clone(),
        )
        .unwrap();
        let layout = ValueLayout::try_new(
            &DataType::Struct(
                vec![
                    Field::new(
                        name,
                        DataType::List(std::sync::Arc::new(Field::new(
                            "source-role",
                            DataType::Dictionary(
                                Box::new(DataType::Int8),
                                Box::new(DataType::LargeBinary),
                            ),
                            true,
                        ))),
                        true,
                    ),
                    Field::new("stamp", timestamp, false),
                ]
                .into(),
            ),
            storage,
        )
        .unwrap();
        FunctionValueType::try_from_value(semantic, layout).unwrap()
    };
    let count = |left: &FunctionValueType, right: &FunctionValueType| {
        let mut callbacks = 0usize;
        let matches = left
            .exactly_equals_observed::<TypeCodecError>(right, || {
                callbacks += 1;
                Ok(())
            })
            .unwrap();
        (matches, callbacks)
    };
    let left = author(true, "UTC");
    let right = author(true, "UTC");
    assert!(!std::ptr::eq(left.data_type(), right.data_type()));
    assert!(!left.value_type().shares_storage(right.value_type()));
    let (_, self_steps) = count(&left, &left);
    assert!(self_steps > 900);
    assert_eq!(count(&left, &right), (true, self_steps));
    for other in [right, author(false, "UTC"), author(true, "PST")] {
        let (matches, paired_steps) = count(&left, &other);
        let (_, other_steps) = count(&other, &other);
        assert!(paired_steps <= self_steps + other_steps);
        let facts = run(&left, &other, SOURCE, WORK, &Control::default()).unwrap();
        assert_eq!(facts.matches(), matches);
        let mut actual_weighted = 0usize;
        assert_eq!(
            left.exactly_equals_weighted_observed::<TypeCodecError>(&other, |step| {
                actual_weighted += step.work_units_upper();
                Ok(())
            })
            .unwrap(),
            matches
        );
        assert!(facts.work_upper_bound() >= actual_weighted);
        assert_eq!(
            run(&left, &other, SOURCE * 8, WORK, &Control::default())
                .unwrap()
                .work_upper_bound(),
            facts.work_upper_bound()
        );
        assert_eq!(
            run(
                &left,
                &other,
                SOURCE,
                facts.work_upper_bound(),
                &Control::default()
            )
            .unwrap()
            .matches(),
            matches
        );
        let under = Control::default();
        assert!(matches!(
            run(&left, &other, SOURCE, facts.work_upper_bound() - 1, &under),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        let trace = under.trace.into_inner().unwrap();
        for cause in CAUSES {
            let forbidden = Control {
                refusal: Some((trace.len(), cause)),
                ..Default::default()
            };
            assert!(matches!(
                run(
                    &left,
                    &other,
                    SOURCE,
                    facts.work_upper_bound() - 1,
                    &forbidden
                ),
                Err(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(*forbidden.trace.lock().unwrap(), trace);
        }
        every_boundary(&left, &other, SOURCE, WORK, Some(matches));
    }
}

#[test]
fn wide_scalar_comparisons_keep_original_two_billion_boundary_without_source_work() {
    let left = FunctionValueType::new(DataType::Int64, false);
    let right = FunctionValueType::new(DataType::Int64, false);
    let mut total = 0usize;
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    for _ in 0..320 {
        let proof =
            verify_type_binding(&left, &right, 2 * SOURCE, 2_000_000_000 - total, &mut work)
                .unwrap();
        assert!(proof.matches());
        total = total.checked_add(proof.work_upper_bound()).unwrap();
    }
    work.finish().unwrap();
    let single = run(&left, &right, 2 * SOURCE, WORK, &Control::default()).unwrap();
    assert_eq!(total, 320 * single.work_upper_bound());
    assert!(total < 2_000_000_000);
    // A huge truthful enclosing source does not invent a comparison traversal.
    assert_eq!(
        run(&left, &right, usize::MAX, WORK, &Control::default())
            .unwrap()
            .work_upper_bound(),
        single.work_upper_bound()
    );
}

#[test]
fn actual_comparison_model_each_parent_refusal_keeps_original_payload_without_tail() {
    #[derive(Debug)]
    enum Refusal {
        Codec(TypeCodecError),
        Host(std::sync::Arc<usize>),
    }
    impl From<TypeCodecError> for Refusal {
        fn from(error: TypeCodecError) -> Self {
            Self::Codec(error)
        }
    }
    let left = nested(false);
    let right = nested(false);
    let recording = Control::default();
    let mut work = CompileCheckpoints::try_new(&recording, CompilePhase::Encode).unwrap();
    let mut baseline = Vec::new();
    let mut before_comparison = Vec::new();
    let proof = verify_type_binding_admitted::<Refusal>(
        &left,
        &right,
        SOURCE,
        WORK,
        &mut |facts| {
            assert!(facts.flags_match());
            baseline.push(facts.work_upper_bound());
            before_comparison.push(recording.trace.lock().unwrap().clone());
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    // The immutable cache exposes exactly the closed-root admission and the
    // complete pair admission. Both precede the actual comparison; there is
    // no retained per-node growth walk or replay of predicted callbacks.
    let root = type_binding_prefix_work_upper_bound(&left, &right, SOURCE).unwrap();
    let paired = root.work_upper_bound()
        + 8
        + left.comparison_work_upper_bound().unwrap()
        + right.comparison_work_upper_bound().unwrap();
    assert_eq!(baseline, [root.work_upper_bound(), paired]);
    assert_eq!(proof.work_upper_bound(), paired);
    assert!(proof.matches());
    assert_eq!(before_comparison, [vec![0], vec![0]]);
    for stop in 0..baseline.len() {
        let original = std::sync::Arc::new(stop);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut seen = Vec::new();
        let mut at_refusal = Vec::new();
        let result = verify_type_binding_admitted::<Refusal>(
            &left,
            &right,
            SOURCE,
            WORK,
            &mut |facts| {
                seen.push(facts.work_upper_bound());
                if seen.len() - 1 == stop {
                    at_refusal = control.trace.lock().unwrap().clone();
                    Err(Refusal::Host(std::sync::Arc::clone(&original)))
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        match result {
            Err(Refusal::Host(actual)) => assert!(std::sync::Arc::ptr_eq(&actual, &original)),
            Err(Refusal::Codec(error)) => panic!("unexpected codec refusal: {error}"),
            Ok(_) => panic!("expected the original parent refusal"),
        }
        assert_eq!(seen, baseline[..=stop]);
        assert_eq!(*control.trace.lock().unwrap(), at_refusal);
    }
}
