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
//! Actual MAP_ENTRIES exact binding, selected addressing and first-cause tests.
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionSpecializationFailure,
    FunctionValueType, ScalarEvaluationInstance, Selection,
};
use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray, new_empty_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{CompileControlError, CompilePhase, DecimalOverflowPolicy};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(n);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("map_entries never waits")
    }
}
#[derive(Default)]
struct Compile {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Compile {
    fn checkpoint(&self, phase: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first cause");
        }
        trace.push((phase, n));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn fixture(sorted: bool) -> ArrayRef {
    let entries = StructArray::new(
        vec![
            Arc::new(
                Field::new("authored-key", DataType::Int32, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "opaque".into(),
                        "key".into(),
                    )]),
                ),
            ),
            Arc::new(Field::new("authored-value", DataType::Utf8, true)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![9, 1, 9, 4, 7])),
            Arc::new(StringArray::from(vec![
                Some("九\0"),
                None,
                Some("last"),
                Some("hidden"),
                Some("七"),
            ])),
        ],
        None,
    );
    let external: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new(
            "authored-entries",
            entries.data_type().clone(),
            false,
        )),
        OffsetBuffer::new(vec![0, 3, 3, 4, 5].into()),
        entries,
        Some(NullBuffer::from(vec![true, true, false, true])),
        sorted,
    ));
    let binding = FunctionValueType::try_with_logical_type(
        external.data_type().clone(),
        true,
        novarocks_type_contract::ValueLogicalType::Physical,
    )
    .unwrap();
    crate::test_value_ingress::array_for_binding(&external, &binding).unwrap()
}
fn source(a: &ArrayRef) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true)
}
fn instance(a: &ArrayRef) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(
            "map_entries",
            &[source(a)],
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap(),
    )
    .unwrap()
}
fn keys(a: &ArrayRef, row: usize) -> Vec<Option<i32>> {
    let list = a.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list.value(row);
    let values = values.as_any().downcast_ref::<StructArray>().unwrap();
    values
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn map_entries_owner_exact_original_order_duplicates_nulls_slices_empty_and_canonical_fields() {
    for sorted in [false, true] {
        let a = fixture(sorted);
        let args = [EvaluatedArgument::Column(&a)];
        let rows = [0, 2, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let out = instance(&a)
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(keys(out.values(), 0), vec![Some(9), Some(1), Some(9)]);
        assert!(out.values().is_null(1));
        assert_eq!(keys(out.values(), 2), vec![Some(7)]);
        let DataType::List(item) = out.values().data_type() else {
            panic!("actual List");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("actual Struct");
        };
        assert_eq!(item.name(), "item");
        assert_eq!(fields[0].name(), "key");
        assert_eq!(fields[1].name(), "value");
        assert!(fields[0].is_nullable());
        assert!(out.errors().is_empty());
        let sliced = a.slice(1, 3);
        let args = [EvaluatedArgument::Column(&sliced)];
        let out = instance(&sliced)
            .evaluate(Selection::all(3), &args, &Control::default())
            .unwrap();
        assert_eq!(keys(out.values(), 0), Vec::<Option<i32>>::new());
        assert!(out.values().is_null(1));
        assert_eq!(keys(out.values(), 2), vec![Some(7)]);
        let empty = new_empty_array(a.data_type());
        let args = [EvaluatedArgument::Column(&empty)];
        let out = instance(&empty)
            .evaluate(Selection::all(0), &args, &Control::default())
            .unwrap();
        assert!(out.values().is_empty());
    }
}
#[test]
fn map_entries_owner_constant_nonzero_pool_null_and_selected_column_use_actual_addresses() {
    let a = fixture(false);
    let ty = source(&a);
    let policy = ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 64,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 24,
        max_library_validation_bytes: 1 << 24,
    };
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("authored-pool").unwrap()),
        ty,
        a.to_data(),
        policy,
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    for ordinal in [2, 3] {
        let v = pool.value(ordinal).unwrap();
        let args = [EvaluatedArgument::Constant(&v)];
        let out = instance(&a)
            .evaluate(Selection::all(257), &args, &Control::default())
            .unwrap();
        for row in 0..257 {
            if ordinal == 2 {
                assert!(out.values().is_null(row));
            } else {
                assert_eq!(keys(out.values(), row), vec![Some(7)]);
            }
        }
    }
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let projected = arrow_select::take::take(
        a.as_ref(),
        &arrow_array::UInt64Array::from(vec![1, 3]),
        None,
    )
    .unwrap();
    let prior = crate::SelectedValues::try_new(selection, a.data_type(), projected, Box::default())
        .unwrap();
    let args = [EvaluatedArgument::SelectedColumn(&prior)];
    let out = instance(&a)
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert_eq!(keys(out.values(), 0), Vec::<Option<i32>>::new());
    assert_eq!(keys(out.values(), 1), vec![Some(7)]);
}
#[test]
fn map_entries_owner_full_nested_profiles_preserve_no_ordered_decoder_or_nominal_guess() {
    for item in [
        DataType::Null,
        DataType::UInt64,
        DataType::List(Arc::new(Field::new("nested", DataType::Utf8, true))),
        DataType::Map(
            Arc::new(Field::new(
                "nested-entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("nested-key", DataType::Int32, false)),
                        Arc::new(Field::new("nested-value", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            true,
        ),
    ] {
        let values = arrow_array::new_null_array(&item, 3);
        let entries = StructArray::new(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", item, true)),
            ]
            .into(),
            vec![Arc::new(StringArray::from(vec!["z", "a", "z"])), values],
            None,
        );
        let a: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 3].into()),
            entries,
            None,
            false,
        ));
        let binding = source(&a);
        let a = crate::test_value_ingress::array_for_binding(&a, &binding).unwrap();
        let args = [EvaluatedArgument::Column(&a)];
        let out = instance(&a)
            .evaluate(Selection::all(1), &args, &Control::default())
            .unwrap();
        let list = out.values().as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.value(0);
        let entries = values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("z"), Some("a"), Some("z")]
        );
        assert_eq!(entries.column(1).logical_null_count(), 3);
    }
}
#[test]
fn map_entries_owner_all_seven_actual_causes_exact_prefix_failed_latch_and_real_row_quanta() {
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
        None,
    );
    let a: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0; 322].into()),
        entries,
        None,
        false,
    ));
    for a in [a, fixture(false)] {
        let args = [EvaluatedArgument::Column(&a)];
        let good = Control::default();
        instance(&a)
            .evaluate(Selection::all(a.len()), &args, &good)
            .unwrap();
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        if a.len() > 256 {
            assert!(trace.contains(&256));
        }
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                invalid("original invalid cause"),
                crate::kernel_control::internal("original internal cause"),
                KernelFailure::Operational(crate::KernelDiagnostic::new(
                    "original operational cause",
                )),
                KernelFailure::InstanceFailed,
            ] {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = instance(&a);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(a.len()), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(a.len()), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn map_entries_owner_compile_success_and_original_admission_rejection_all_actual_causes() {
    let a = fixture(false);
    for ty in [source(&a), FunctionValueType::new(DataType::Int32, true)] {
        let good = Compile::default();
        assert_eq!(
            prepared_for_test_with_control(
                "map_entries",
                std::slice::from_ref(&ty),
                DecimalOverflowPolicy::OutputNull,
                &good
            )
            .is_ok(),
            matches!(ty.data_type(), DataType::Map(..))
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Compile {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let error = prepared_for_test_with_control(
                    "map_entries",
                    std::slice::from_ref(&ty),
                    DecimalOverflowPolicy::OutputNull,
                    &control,
                )
                .err()
                .and_then(|e| match e {
                    FunctionSpecializationFailure::Control(c) => Some(c),
                    FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                        Some(CompileControlError::Cancelled)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                        Some(CompileControlError::DeadlineExceeded)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                        Some(CompileControlError::ResourceExhausted)
                    }
                    _ => None,
                });
                assert_eq!(error, Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

fn check_raw_projection_causes(a: &ArrayRef) {
    let mut calls = 0;
    crate::builtin::map_entries_core::project_observed(a, None, &mut |_| {
        calls += 1;
        Ok::<_, KernelFailure>(())
    })
    .unwrap();
    for stop in 0..calls {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            invalid("original invalid cause"),
            crate::kernel_control::internal("original internal cause"),
            KernelFailure::Operational(crate::KernelDiagnostic::new("original operational cause")),
            KernelFailure::InstanceFailed,
        ] {
            let mut at = 0;
            let result = crate::builtin::map_entries_core::project_observed(a, None, &mut |_| {
                assert!(at <= stop);
                let current = at;
                at += 1;
                if current == stop {
                    Err(cause.clone())
                } else {
                    Ok(())
                }
            });
            assert!(
                matches!(result, Err(crate::builtin::map_entries_core::MapEntriesFailure::Control(actual)) if actual == cause)
            );
            assert_eq!(at, stop + 1);
        }
    }
}
fn reject_raw_installed(a: &ArrayRef, selection: Selection<'_>, args: &[EvaluatedArgument<'_>]) {
    let mut kernel = instance(a);
    assert!(matches!(
        kernel.evaluate(selection, args, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let after = Control::default();
    assert_eq!(
        kernel.evaluate(selection, args, &after).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
}
fn wide_dictionary(rows: usize, dictionary_len: usize) -> ArrayRef {
    use arrow_array::{DictionaryArray, Int8Array, types::Int8Type};
    let dictionary: ArrayRef = Arc::new(StringArray::from(
        (0..dictionary_len)
            .map(|i| format!("entry-{i}"))
            .collect::<Vec<_>>(),
    ));
    let values: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0; rows]), dictionary).unwrap(),
    );
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", values.data_type().clone(), true)),
        ]
        .into(),
        vec![Arc::new(Int32Array::from(vec![1; rows])), values],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new((0..=rows).map(|i| i as i32).collect::<Vec<_>>().into()),
        entries,
        None,
        false,
    ))
}
fn assert_dictionary_extent(a: &ArrayRef, expected: usize) {
    use arrow_array::{DictionaryArray, types::Int8Type};
    let list = a.as_any().downcast_ref::<ListArray>().unwrap();
    let entries = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let dictionary = entries
        .column(1)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(dictionary.values().len(), expected);
}
#[test]
fn raw_arrow_map_entries_contiguous_projection_keeps_dictionary128_and_strict_installed_gate() {
    let a = wide_dictionary(4, 128);
    for a in [a.clone(), a.slice(1, 3), a.slice(1, 2)] {
        let out = crate::builtin::map_entries_core::project(&a, None).unwrap();
        assert_eq!(out.len(), a.len());
        assert_dictionary_extent(&out, 128);
        check_raw_projection_causes(&a);
        reject_raw_installed(
            &a,
            Selection::all(a.len()),
            &[EvaluatedArgument::Column(&a)],
        );
    }
}
#[test]
fn raw_arrow_map_entries_noncontiguous_copy_preserves_dictionary_constructor_guard() {
    use crate::selected_copy::{CopyError, ExtendSegment, preflight_extend_multi};
    use arrow_data::transform::MutableArrayData;
    let segments = [0, 2].map(|start| ExtendSegment {
        source: 0,
        start,
        len: 1,
        repeats: 1,
        nulls: 0,
    });
    for dictionary_len in [127, 128] {
        let a = wide_dictionary(4, dictionary_len);
        let list = crate::builtin::map_entries_core::project(&a, None).unwrap();
        let run = |observer: &mut dyn FnMut(bool) -> Result<(), KernelFailure>| {
            preflight_extend_multi(&[list.as_ref()], &segments, 2, observer)
        };
        let mut calls = 0;
        let result = run(&mut |_| {
            calls += 1;
            Ok(())
        });
        if dictionary_len == 127 {
            result.unwrap();
            let data = list.to_data();
            let mut copy = MutableArrayData::new(vec![&data], false, 2);
            copy.try_extend(0, 0, 1).unwrap();
            copy.try_extend(0, 2, 3).unwrap();
            let out = arrow_array::make_array(copy.freeze());
            assert_dictionary_extent(&out, 127);
        } else {
            assert!(matches!(result, Err(CopyError::Extent(_))));
        }
        for stop in 0..calls {
            let cause =
                KernelFailure::Operational(crate::KernelDiagnostic::new("raw copy original cause"));
            let mut at = 0;
            let result = run(&mut |_| {
                assert!(at <= stop);
                let current = at;
                at += 1;
                if current == stop {
                    Err(cause.clone())
                } else {
                    Ok(())
                }
            });
            assert!(matches!(result, Err(CopyError::Control(actual)) if actual == cause));
            assert_eq!(at, stop + 1);
        }
    }
}
#[test]
fn raw_arrow_map_entries_pool_and_selected_addresses_keep_backing_without_kernel_capability() {
    let a = wide_dictionary(4, 128);
    let ty = source(&a);
    let policy = ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 64,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 24,
        max_library_validation_bytes: 1 << 24,
    };
    let field = ty
        .try_to_field("actual-pool")
        .unwrap()
        .with_data_type(a.data_type().clone());
    let pool = ConstantPool::try_new(
        Arc::new(field),
        ty,
        a.to_data(),
        policy,
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let value = pool.value(3).unwrap();
    let selected = value.pool().array().slice(value.ordinal() as usize, 1);
    let out = crate::builtin::map_entries_core::project(&selected, None).unwrap();
    assert_dictionary_extent(&out, 128);
    check_raw_projection_causes(&selected);
    reject_raw_installed(
        &a,
        Selection::all(1),
        &[EvaluatedArgument::Constant(&value)],
    );
    let rows = [0, 2];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let prior =
        crate::SelectedValues::try_new(selection, a.data_type(), a.slice(0, 2), Box::default())
            .unwrap();
    let out = crate::builtin::map_entries_core::project(prior.values(), None).unwrap();
    assert_eq!(out.len(), 2);
    assert_dictionary_extent(&out, 128);
    reject_raw_installed(&a, selection, &[EvaluatedArgument::SelectedColumn(&prior)]);
}
