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
use crate::{ConstantPolicy, ConstantPool, ConstantValue};
use arrow_array::types::{Int8Type, Int16Type};
use arrow_array::{
    ArrayRef, DictionaryArray, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    RunArray, StringArray, StructArray, UInt64Array,
};
use arrow_schema::Field;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

struct ConstructionControl;
impl PureCompileControl for ConstructionControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[derive(Default)]
struct Control {
    checks: Mutex<Vec<u32>>,
    fail_at: Option<(usize, KernelFailure)>,
}
impl Control {
    fn fail_at(check: usize, error: KernelFailure) -> Self {
        Self {
            checks: Mutex::new(Vec::new()),
            fail_at: Some((check, error)),
        }
    }
    fn checks(&self) -> Vec<u32> {
        self.checks.lock().unwrap().clone()
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= MAX_UNOBSERVED_KERNEL_WORK);
        let mut checks = self.checks.lock().unwrap();
        let index = checks.len();
        checks.push(units);
        if let Some((at, error)) = &self.fail_at
            && index == *at
        {
            Err(error.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("argument validation must not wait")
    }
}

fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 32,
        max_array_nodes: 512,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 8 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4 * 1024 * 1024,
        max_library_validation_work: 128 * 1024 * 1024,
        max_library_validation_bytes: 64 * 1024 * 1024,
    }
}

fn pool(array: ArrayRef, nullable: bool, logical: ValueLogicalType) -> ConstantPool {
    let ty = FunctionValueType::try_with_logical_type(array.data_type().clone(), nullable, logical)
        .unwrap();
    pool_for_binding(array, ty)
}
fn pool_for_binding(array: ArrayRef, ty: FunctionValueType) -> ConstantPool {
    let array = if matches!(
        array.data_type(),
        DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _)
    ) {
        // Pure constant backing can be encoded; generic kernels still require
        // an explicit installed access path before consuming this carrier.
        array
    } else {
        crate::test_value_ingress::array_for_binding(&array, &ty).unwrap()
    };
    let field = Arc::new(
        ty.try_to_field("constant-input")
            .unwrap()
            .with_data_type(array.data_type().clone()),
    );
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &ConstructionControl,
    )
    .unwrap()
}

fn validate(
    value: &ConstantValue,
    selection: Selection<'_>,
    expected: &FunctionValueType,
    control: &Control,
) -> Result<(), KernelFailure> {
    validate_argument_observed(
        EvaluatedArgument::Constant(value),
        selection,
        expected,
        control,
    )
}

#[test]
fn multivalue_pool_broadcasts_only_the_checked_ordinal_and_keeps_backing() {
    let p = pool(
        Arc::new(Int64Array::from(vec![None, Some(41), Some(99)])),
        true,
        ValueLogicalType::Physical,
    );
    let value = p.value(1).unwrap();
    let argument = EvaluatedArgument::Constant(&value);
    let selection = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
    assert_eq!(argument.array().len(), 3);
    assert!(Arc::ptr_eq(argument.array(), p.array()));
    assert!(Arc::ptr_eq(value.pool().array(), p.array()));
    for (ordinal, row) in selection.iter().enumerate() {
        assert_eq!(argument.value_row(ordinal, row), 1);
        assert_eq!(
            argument
                .array()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(argument.value_row(ordinal, row)),
            41
        );
    }
    validate(&value, selection, p.value_type(), &Control::default()).unwrap();
    assert!(p.value(3).is_err());
    assert!(p.value(u32::MAX).is_err());
}

#[test]
fn constant_nullability_widens_only_from_the_checked_source() {
    let p = pool(
        Arc::new(Int64Array::from(vec![11, 22, 33])),
        false,
        ValueLogicalType::Physical,
    );
    let value = p.value(2).unwrap();
    let nonnull = p.value_type().clone();
    let nullable = nonnull
        .clone()
        .with_nullable(true)
        .expect("valid function nullability declaration");
    for expected in [&nonnull, &nullable] {
        validate(&value, Selection::all(4), expected, &Control::default()).unwrap();
    }
    let nullable_pool = pool(
        Arc::new(Int64Array::from(vec![None, Some(22), Some(33)])),
        true,
        ValueLogicalType::Physical,
    );
    let selected_nonnull = nullable_pool.value(1).unwrap();
    validate(
        &selected_nonnull,
        Selection::all(4),
        &nullable,
        &Control::default(),
    )
    .unwrap();
    for selection in [Selection::all(4), Selection::all(0)] {
        assert!(matches!(
            validate(&selected_nonnull, selection, &nonnull, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn typed_null_and_empty_selection_keep_the_complete_logical_identity() {
    let p = pool(
        Arc::new(StringArray::from(vec![
            Some("{\"unused\":1}"),
            None,
            Some("{\"unused\":2}"),
        ])),
        true,
        ValueLogicalType::Json,
    );
    let value = p.value(1).unwrap();
    assert_eq!(value.value_type().logical_type(), ValueLogicalType::Json);
    assert_eq!(value.try_utf8().unwrap(), None);
    validate(
        &value,
        Selection::all(0),
        p.value_type(),
        &Control::default(),
    )
    .unwrap();
    validate(
        &value,
        Selection::all(3),
        p.value_type(),
        &Control::default(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        EvaluatedArgument::Constant(&value).array(),
        p.array()
    ));
    let wrong = FunctionValueType::new(DataType::Utf8, true);
    assert!(matches!(
        validate(&value, Selection::all(0), &wrong, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn equal_arrow_carriers_do_not_exchange_json_variant_and_physical_roots() {
    for (array, logical) in [
        (
            Arc::new(StringArray::from(vec![
                "{\"a\":1}",
                "{\"b\":2}",
                "{\"c\":3}",
            ])) as ArrayRef,
            ValueLogicalType::Json,
        ),
        (
            Arc::new(LargeBinaryArray::from(vec![
                &b"a"[..],
                &b"b"[..],
                &b"c"[..],
            ])) as ArrayRef,
            ValueLogicalType::Variant,
        ),
    ] {
        let tagged = pool(array.clone(), false, logical);
        let physical = pool(array, false, ValueLogicalType::Physical);
        for (source, wrong) in [
            (&tagged, physical.value_type()),
            (&physical, tagged.value_type()),
        ] {
            let value = source.value(1).unwrap();
            validate(
                &value,
                Selection::all(2),
                source.value_type(),
                &Control::default(),
            )
            .unwrap();
            assert!(matches!(
                validate(&value, Selection::all(2), wrong, &Control::default()),
                Err(KernelFailure::InvalidProgram(_))
            ));
            assert_eq!(
                value.value_type().logical_type(),
                source.value_type().logical_type()
            );
        }
    }
}

#[test]
fn nested_field_metadata_logical_identity_and_nullability_are_exact() {
    let child =
        Field::new("payload", DataType::Utf8, false).with_metadata(std::collections::HashMap::<
            String,
            String,
        >::from([
            ("nr_logical_type".into(), "json".into()),
            ("provider.field-id".into(), "17".into()),
        ]));
    let array = StructArray::new(
        vec![Arc::new(child.clone())].into(),
        vec![Arc::new(StringArray::from(vec![
            "{\"n\":1}",
            "{\"n\":2}",
            "{\"n\":3}",
        ]))],
        None,
    );
    let array: ArrayRef = Arc::new(array);
    let storage = novarocks_type_contract::ValueTypeStorageLifetime::pure();
    let nominal =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let semantic = novarocks_type_contract::ValueTypeRef::try_new(
        novarocks_type_contract::ValueKind::Struct(
            vec![novarocks_type_contract::ValueField::new(
                "payload",
                nominal.value_type().clone(),
                storage.clone(),
            )]
            .into(),
        ),
        false,
        storage.clone(),
    )
    .unwrap();
    let binding = FunctionValueType::try_from_value(
        semantic,
        novarocks_type_contract::ValueLayout::try_new(array.data_type(), storage).unwrap(),
    )
    .unwrap();
    let p = pool_for_binding(array, binding);
    let value = p.value(2).unwrap();
    validate(
        &value,
        Selection::all(2),
        p.value_type(),
        &Control::default(),
    )
    .unwrap();
    let mut metadata = child.metadata().clone();
    metadata.insert("provider.field-id".to_owned(), "18".to_owned());
    let mut physical_metadata = child.metadata().clone();
    physical_metadata.remove("nr_logical_type");
    let same_semantics = FunctionValueType::from_legacy_declared_projection(
        DataType::Struct(vec![Arc::new(child.clone().with_metadata(metadata))].into()),
        false,
    );
    validate(
        &value,
        Selection::all(2),
        &same_semantics,
        &Control::default(),
    )
    .unwrap();
    for wrong_child in [
        child.clone().with_metadata(physical_metadata),
        child.clone().with_nullable(true),
    ] {
        let wrong = FunctionValueType::from_legacy_declared_projection(
            DataType::Struct(vec![Arc::new(wrong_child)].into()),
            false,
        );
        assert!(matches!(
            validate(&value, Selection::all(2), &wrong, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    // Raw source metadata is independently preserved by the checked CV; the
    // canonical view is a paired execution loan, not another CV declaration.
    let raw: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(child.clone())].into(),
        vec![Arc::new(StringArray::from(vec![
            "{\"unused\":1}",
            "{\"selected\":2}",
            "{\"future\":3}",
        ]))],
        None,
    ));
    let raw_pool = ConstantPool::try_new(
        Arc::new(Field::new(
            "original-source",
            raw.data_type().clone(),
            false,
        )),
        p.value_type().clone(),
        raw.to_data(),
        policy(),
        CompilePhase::Validate,
        &ConstructionControl,
    )
    .unwrap();
    let original = raw_pool.value(1).unwrap();
    assert!(matches!(
        validate(
            &original,
            Selection::all(3),
            p.value_type(),
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let canonical = crate::array_projection::normalize_execution_array(&raw).unwrap();
    let view = crate::CanonicalConstantView::from_authored_projection(&original, &canonical);
    let argument = EvaluatedArgument::ConstantView(view);
    assert!(std::ptr::eq(argument.constant_value().unwrap(), &original));
    assert_eq!(view.original().ordinal(), 1);
    assert!(Arc::ptr_eq(
        view.original().pool().array(),
        raw_pool.array()
    ));
    let sparse_rows = [2usize, 8, 10];
    for selection in [
        Selection::all(0),
        Selection::all(3),
        Selection::try_sparse(11, &sparse_rows).unwrap(),
    ] {
        validate_argument_observed(argument, selection, p.value_type(), &Control::default())
            .unwrap();
        for (ordinal, row) in selection.iter().enumerate() {
            assert_eq!(argument.value_row(ordinal, row), 1);
            let actual = argument
                .array()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(
                actual
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(argument.value_row(ordinal, row)),
                "{\"selected\":2}"
            );
        }
    }
    let DataType::Struct(fields) = original.field().data_type() else {
        panic!("raw Struct")
    };
    assert_eq!(fields[0].metadata()["provider.field-id"], "17");
    assert_eq!(fields[0].metadata()["nr_logical_type"], "json");
    // A different semantic child/nullable source cannot hide behind an exact
    // canonical carrier. The SAME old Constant semantic checks apply.
    let wrong = FunctionValueType::new(canonical.data_type().clone(), false);
    assert!(matches!(
        validate_argument_observed(argument, Selection::all(3), &wrong, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let successful = Control::default();
    validate_argument_observed(argument, Selection::all(3), p.value_type(), &successful).unwrap();
    let expected = successful.checks();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for at in 0..expected.len() {
            let control = Control::fail_at(at, cause.clone());
            assert_eq!(
                validate_argument_observed(argument, Selection::all(3), p.value_type(), &control),
                Err(cause.clone())
            );
            assert_eq!(control.checks(), expected[..=at]);
        }
    }
}

#[test]
fn dictionary_ordinal_is_a_pool_row_and_keeps_encoded_backing() {
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 1, 0]),
        Arc::new(StringArray::from(vec!["first", "second"])),
    )
    .unwrap();
    let p = pool(Arc::new(dictionary), false, ValueLogicalType::Physical);
    let value = p.value(1).unwrap();
    let argument = EvaluatedArgument::Constant(&value);
    let selection = Selection::try_sparse(11, &[2, 8, 10]).unwrap();
    let dictionary = argument
        .array()
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(dictionary.key(argument.value_row(2, 10)), Some(1));
    assert!(Arc::ptr_eq(argument.array(), p.array()));
    assert!(
        matches!(
            validate(&value, selection, p.value_type(), &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ),
        "constant backing does not mint installed dictionary access"
    );
    let wrong = FunctionValueType::new(DataType::Utf8, false);
    assert!(matches!(
        validate(&value, selection, &wrong, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn run_end_ordinal_uses_logical_address_without_slicing_or_expanding() {
    let runs = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![2, 5]),
        &Int64Array::from(vec![11, 77]),
    )
    .unwrap();
    let p = pool(Arc::new(runs), false, ValueLogicalType::Physical);
    let value = p.value(4).unwrap();
    let argument = EvaluatedArgument::Constant(&value);
    assert_eq!(argument.array().len(), 5);
    assert_eq!(argument.value_row(0, 99), 4);
    let runs = argument
        .array()
        .as_any()
        .downcast_ref::<RunArray<Int16Type>>()
        .unwrap();
    assert_eq!(runs.get_physical_index(argument.value_row(0, 99)), 1);
    assert_eq!(
        runs.values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1),
        77
    );
    assert!(Arc::ptr_eq(argument.array(), p.array()));
    assert!(matches!(
        validate(
            &value,
            Selection::all(3),
            p.value_type(),
            &Control::default(),
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let DataType::RunEndEncoded(ends, values) = p.array().data_type() else {
        panic!("run carrier");
    };
    let wrong = FunctionValueType::new(
        DataType::RunEndEncoded(
            Arc::new(
                ends.as_ref().clone().with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.id".into(),
                        "other".into(),
                    )]),
                ),
            ),
            values.clone(),
        ),
        false,
    );
    assert!(matches!(
        validate(&value, Selection::all(3), &wrong, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn constant_string_and_integer_reads_keep_seed_carriers_without_numeric_casts() {
    let text = pool(
        Arc::new(StringArray::from(vec!["unused", "23", "future"])),
        false,
        ValueLogicalType::Physical,
    )
    .value(1)
    .unwrap();
    assert_eq!(text.try_utf8().unwrap(), Some("23"));
    assert!(text.try_i64().is_err());
    let signed = pool(
        Arc::new(Int8Array::from(vec![1, -7, 9])),
        false,
        ValueLogicalType::Physical,
    )
    .value(1)
    .unwrap();
    assert_eq!(signed.try_i64().unwrap(), Some(-7));
    assert_eq!(signed.value_type().data_type().clone(), DataType::Int8);
    assert!(signed.try_utf8().is_err());
    let unsigned = pool(
        Arc::new(UInt64Array::from(vec![0, u64::MAX, 3])),
        false,
        ValueLogicalType::Physical,
    )
    .value(1)
    .unwrap();
    assert_eq!(unsigned.try_u64().unwrap(), Some(u64::MAX));
    assert!(unsigned.try_i64().is_err());
}

fn wide_immutable_binding_value() -> ConstantValue {
    let fields = (0..320)
        .map(|index| Arc::new(Field::new(format!("field-{index}"), DataType::Int32, false)))
        .collect::<Vec<_>>();
    let columns = (0..320)
        .map(|index| Arc::new(Int32Array::from(vec![index])) as ArrayRef)
        .collect();
    pool(
        Arc::new(StructArray::new(fields.into(), columns, None)),
        false,
        ValueLogicalType::Physical,
    )
    .value(0)
    .unwrap()
}

#[test]
fn actual_complete_binding_walk_observes_256_work_and_success_completion() {
    let value = wide_immutable_binding_value();
    let control = Control::default();
    validate(&value, Selection::all(1), value.value_type(), &control).unwrap();
    let checks = control.checks();
    assert_eq!(checks[0], 0);
    assert!(checks.contains(&256));
    assert!(checks.len() >= 3);
    assert!(*checks.last().unwrap() < 256);
}

#[test]
fn entry_256_and_final_failures_keep_all_three_outer_control_categories() {
    let value = wide_immutable_binding_value();
    let good = Control::default();
    validate(&value, Selection::all(1), value.value_type(), &good).unwrap();
    let checks = good.checks();
    let full_block = checks.iter().position(|units| *units == 256).unwrap();
    let final_check = checks.len() - 1;
    assert!(final_check > full_block);
    for error in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for failure_at in [0, full_block, final_check] {
            let control = Control::fail_at(failure_at, error.clone());
            assert_eq!(
                validate(&value, Selection::all(1), value.value_type(), &control),
                Err(error.clone())
            );
            assert_eq!(control.checks(), checks[..=failure_at]);
            assert_eq!(
                value.value_type().logical_type(),
                ValueLogicalType::Physical
            );
        }
    }
}

#[test]
fn ordinary_argument_type_mismatch_is_primary_without_an_optional_tail() {
    let value = pool(
        Arc::new(Int64Array::from(vec![3, 7])),
        false,
        ValueLogicalType::Physical,
    )
    .value(1)
    .unwrap();
    let wrong = FunctionValueType::new(DataType::UInt64, false);
    let baseline = Control::default();
    assert!(matches!(
        validate(&value, Selection::all(3), &wrong, &baseline),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let checks = baseline.checks();
    assert_eq!(checks, [0]);
    for error in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InstanceFailed,
        KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("original type refusal")),
        KernelFailure::Internal(crate::KernelDiagnostic::new("original type refusal")),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original type refusal")),
    ] {
        // Every hypothetical later callback remains armed, but the actual
        // type author has already refused before any completion observation.
        let control = Control::fail_at(1, error.clone());
        assert!(matches!(
            validate(&value, Selection::all(3), &wrong, &control),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert_eq!(control.checks(), checks);
    }
}

#[test]
fn selected_nonnull_argument_null_rejection_is_primary_without_an_optional_tail() {
    let column: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(4), None]));
    let nonnull = FunctionValueType::new(DataType::Int64, false);
    // The unselected first NULL is not input. The later selected NULL must be
    // rejected only after checking the successful selected row before it.
    let selection = Selection::try_sparse(3, &[1, 2]).unwrap();
    let baseline = Control::default();
    assert!(matches!(
        validate_argument_observed(
            EvaluatedArgument::Column(&column),
            selection,
            &nonnull,
            &baseline
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let checks = baseline.checks();
    assert_eq!(checks, [0]);
    for error in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InstanceFailed,
        KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("original NULL refusal")),
        KernelFailure::Internal(crate::KernelDiagnostic::new("original NULL refusal")),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original NULL refusal")),
    ] {
        let control = Control::fail_at(1, error.clone());
        assert!(matches!(
            validate_argument_observed(
                EvaluatedArgument::Column(&column),
                selection,
                &nonnull,
                &control
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert_eq!(control.checks(), checks);
    }
}
