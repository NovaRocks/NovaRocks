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
use arrow_array::{Int64Array, StringArray};
use arrow_schema::Field;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn control_values_full_and_compact_share_first_non_null_value_assembly() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, None, Some(3)]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(9), Some(2), None, None]));
    let full = assemble_values(
        AssemblyPlan::FillNulls {
            left: &left,
            right: &right,
        },
        &LegacyControl,
    )
    .unwrap();
    let left: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(3)]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), None]));
    let compact = assemble_values(
        AssemblyPlan::Indexed {
            result_type: &DataType::Int64,
            sources: &[Some(&left), Some(&right)],
            choices: &[Some((0, 0)), Some((1, 0)), None, Some((0, 1))],
        },
        &LegacyControl,
    )
    .unwrap();
    let expected = Int64Array::from(vec![Some(1), Some(2), None, Some(3)]);
    assert_eq!(full.to_data(), expected.to_data());
    assert_eq!(compact.to_data(), expected.to_data());
}
#[test]
fn control_values_indexed_repeated_sources_selected_nulls_and_typed_nulls() {
    let left: ArrayRef = Arc::new(StringArray::from(vec![Some("中"), None]));
    let right: ArrayRef = Arc::new(StringArray::from(vec![Some("a\0b")]));
    let output = assemble_values(
        AssemblyPlan::Indexed {
            result_type: &DataType::Utf8,
            sources: &[Some(&left), Some(&right)],
            choices: &[Some((0, 0)), None, Some((1, 0)), Some((0, 1)), Some((0, 0))],
        },
        &LegacyControl,
    )
    .unwrap();
    assert_eq!(
        output.to_data(),
        StringArray::from(vec![Some("中"), None, Some("a\0b"), None, Some("中")]).to_data()
    );
    let empty = assemble_values(
        AssemblyPlan::Indexed {
            result_type: &DataType::Utf8,
            sources: &[],
            choices: &[],
        },
        &LegacyControl,
    )
    .unwrap();
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.data_type(), &DataType::Utf8);
    let nulls = assemble_values(
        AssemblyPlan::Indexed {
            result_type: &DataType::Int64,
            sources: &[],
            choices: &[None, None],
        },
        &LegacyControl,
    )
    .unwrap();
    assert_eq!(
        nulls.to_data(),
        Int64Array::from(vec![None::<i64>; 2]).to_data()
    );
}
#[test]
fn control_values_legacy_tail_action_retains_original_first_round_backings() {
    let first: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 2]));
    let weak = Arc::downgrade(&first);
    let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(9), None]));
    let action = coalesce_legacy(vec![first, right], &DataType::Null).unwrap();
    let CoalesceStep::ReevaluateTail(keep_alive) = action else {
        panic!("original all-NULL head must re-evaluate its tail");
    };
    assert!(weak.upgrade().is_some());
    assert_eq!(keep_alive._arrays[1].data_type(), &DataType::Utf8);
    assert_eq!(
        keep_alive._arrays[1].to_data(),
        StringArray::from(vec![Some("9"), None]).to_data()
    );
    drop(keep_alive);
    assert!(weak.upgrade().is_none());
    let first: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let later: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>; 2]));
    let CoalesceStep::Complete(output) =
        coalesce_legacy(vec![first.clone(), later], &DataType::Int64).unwrap()
    else {
        panic!("non-NULL first array returns directly");
    };
    assert!(Arc::ptr_eq(&first, &output));
}
#[test]
fn control_values_legacy_arrow_errors_remain_complete_and_unprefixed() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let short: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let error = assemble_values(
        AssemblyPlan::FillNulls {
            left: &left,
            right: &short,
        },
        &LegacyControl,
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid argument error: all arrays should have the same length"
    );
    let bad: ArrayRef = Arc::new(arrow_array::StructArray::new(
        vec![Arc::new(Field::new(
            "bad".repeat(300),
            DataType::Int64,
            false,
        ))]
        .into(),
        vec![left.clone()],
        None,
    ));
    let expected = cast(bad.as_ref(), &DataType::Int64)
        .unwrap_err()
        .to_string();
    assert!(expected.len() > 512);
    assert_eq!(ifnull_legacy(left, bad).unwrap_err(), expected);
}
#[test]
fn control_values_indexed_rejects_dedicated_carriers_without_fallback() {
    for ty in [
        DataType::Null,
        DataType::FixedSizeBinary(16),
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
    ] {
        assert!(!supports_indexed_result(&ty));
        let error = assemble_values(
            AssemblyPlan::Indexed {
                result_type: &ty,
                sources: &[],
                choices: &[None],
            },
            &LegacyControl,
        )
        .unwrap_err();
        let AssemblyFailure::Kernel(error) = error else {
            panic!("exact carrier admission is typed");
        };
        assert_eq!(
            error,
            invalid("guarded result requires its dedicated carrier protocol")
        );
    }
}
struct RefusingControl {
    calls: AtomicUsize,
    refuse: usize,
}
impl KernelEvaluationControl for RefusingControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        if call == self.refuse {
            Err(invalid("original assembly control refusal"))
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        unreachable!()
    }
}
#[test]
fn control_values_every_control_refusal_is_primary_in_both_representations() {
    let long = "中".repeat(1400);
    let left: ArrayRef = Arc::new(StringArray::from(vec![
        Some(long.as_str()),
        None,
        Some("a"),
    ]));
    let right: ArrayRef = Arc::new(StringArray::from(vec![
        None,
        Some(long.as_str()),
        Some("b"),
    ]));
    for compact in [false, true] {
        let call = |control: &dyn KernelEvaluationControl| {
            if compact {
                assemble_values(
                    AssemblyPlan::Indexed {
                        result_type: &DataType::Utf8,
                        sources: &[Some(&left), Some(&right)],
                        choices: &[Some((0, 0)), Some((1, 1)), Some((0, 2))],
                    },
                    control,
                )
            } else {
                assemble_values(
                    AssemblyPlan::FillNulls {
                        left: &left,
                        right: &right,
                    },
                    control,
                )
            }
        };
        let control = RefusingControl {
            calls: AtomicUsize::new(0),
            refuse: usize::MAX,
        };
        call(&control).unwrap();
        let calls = control.calls.load(Ordering::Relaxed);
        assert!(calls > 4);
        for refuse in 1..=calls {
            let control = RefusingControl {
                calls: AtomicUsize::new(0),
                refuse,
            };
            let error = call(&control).unwrap_err();
            let AssemblyFailure::Kernel(error) = error else {
                panic!("control refusal must not become an Arrow diagnostic");
            };
            assert_eq!(error, invalid("original assembly control refusal"));
            assert_eq!(control.calls.load(Ordering::Relaxed), refuse);
        }
    }
}
