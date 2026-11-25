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
use crate::{
    ConstantPolicy, ConstantPool, DataFailureReason, KernelCapacityRefusal, KernelDiagnostic,
    SelectedValues, Selection,
};
use arrow_array::{ArrayRef, LargeBinaryArray, ListArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::Field;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, SharedFailureCause,
};
use novarocks_function_contract::kernel_failure::{
    DIVISION_BY_ZERO, INVALID_CAST_ARGUMENT, NUMERIC_OUT_OF_RANGE,
};
use novarocks_type_contract::ArrayCastFailureMode;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after original refusal");
        }
        calls.push(units);
        if let Some((stop, cause)) = &self.refusal
            && at == *stop
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("array CAST never waits")
    }
}
fn ty(item: DataType, child_nullable: bool, root_nullable: bool, tag: &str) -> FunctionValueType {
    FunctionValueType::new(
        DataType::List(Arc::new(
            Field::new("actual_element", item, child_nullable).with_metadata(HashMap::from([
                ("PARQUET:field_id".to_owned(), "31".to_owned()),
                ("original_owner".to_owned(), tag.to_owned()),
            ])),
        )),
        root_nullable,
    )
}
fn list(
    ty: &FunctionValueType,
    values: ArrayRef,
    offsets: &[i32],
    validity: Option<&[bool]>,
) -> ArrayRef {
    let DataType::List(field) = ty.data_type() else {
        panic!("actual List binding");
    };
    assert_eq!(values.data_type(), field.data_type());
    Arc::new(ListArray::new(
        Arc::clone(field),
        OffsetBuffer::new(ScalarBuffer::from(offsets.to_vec())),
        values,
        validity.map(|validity| NullBuffer::from(validity.to_vec())),
    ))
}
fn prepare(
    source: &FunctionValueType,
    target: &FunctionValueType,
    mode: ArrayCastFailureMode,
) -> PreparedCastRecipe {
    let recipe = PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        source,
        target,
        DecimalOverflowPolicy::ReportError,
        false,
        Some(mode),
        crate::binding_test_control(),
    )
    .unwrap();
    assert_eq!(recipe.source_type(), source);
    assert_eq!(recipe.result_type(), target);
    assert_eq!(recipe.array_failure_mode(), Some(mode));
    recipe
}
fn evaluate<'a>(
    recipe: &PreparedCastRecipe,
    input: &ArrayRef,
    selection: Selection<'a>,
) -> SelectedValues<'a> {
    let host: Arc<dyn crate::AggregateStateAllocator> =
        Arc::new(crate::source_custody_direct_tests::Host::default());
    recipe
        .evaluate_selected_scoped(
            EvaluatedArgument::Column(input),
            selection,
            &[],
            &Control::default(),
            Some(&host),
        )
        .unwrap()
}
fn same(actual: &SelectedValues<'_>, expected: &ArrayRef, target: &FunctionValueType) {
    assert_eq!(actual.values().data_type(), target.data_type());
    assert_eq!(actual.values().to_data(), expected.to_data());
}
fn domain_errors(
    actual: &SelectedValues<'_>,
    expected: &[(usize, novarocks_failure_contract::FailureReason)],
) {
    assert_eq!(actual.errors().len(), expected.len());
    for (error, &(ordinal, reason)) in actual.errors().iter().zip(expected) {
        assert_eq!(error.selected_ordinal(), ordinal);
        assert_eq!(error.reason(), DataFailureReason::Domain(reason));
        assert_eq!(error.projected_fact().class(), FailureClass::Data);
        assert!(std::error::Error::source(error).is_some());
    }
}
fn modes() -> [ArrayCastFailureMode; 2] {
    [
        ArrayCastFailureMode::ReportError,
        ArrayCastFailureMode::OutputNull,
    ]
}

#[test]
fn explicit_integer_array_keeps_element_null_empty_and_parent_null_distinct() {
    let source = ty(DataType::Int32, true, true, "source-int");
    let target = ty(DataType::Int64, true, true, "target-int");
    let input = list(
        &source,
        Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(i32::MIN),
            Some(i32::MAX),
        ])),
        &[0, 3, 4, 4, 4],
        Some(&[true, true, true, false]),
    );
    let expected = list(
        &target,
        Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            Some(i64::from(i32::MIN)),
            Some(i64::from(i32::MAX)),
        ])),
        &[0, 3, 4, 4, 4],
        Some(&[true, true, true, false]),
    );
    for mode in modes() {
        let recipe = prepare(&source, &target, mode);
        assert_eq!(recipe.source_type(), &source);
        assert_eq!(recipe.result_type(), &target);
        let actual = evaluate(&recipe, &input, Selection::all(4));
        same(&actual, &expected, &target);
        assert!(actual.errors().is_empty());
        let out = actual
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!out.is_null(2));
        assert_eq!(out.value(2).len(), 0);
        assert!(out.is_null(3));
        let child = out.value(0);
        assert!(child.is_null(1));
        assert_eq!(out.value_offsets(), &[0, 3, 4, 4, 4]);
    }
}

#[test]
fn double_decimal_array_keeps_successful_rounding_and_actual_failure_reasons() {
    let source = ty(DataType::Float64, true, true, "source-double");
    let target = ty(DataType::Decimal128(20, 0), true, true, "target-decimal");
    let input = list(
        &source,
        Arc::new(Float64Array::from(vec![
            Some(9_007_199_254_740_992.0),
            Some(1.0),
            None,
            Some(4_503_599_627_370_497.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(1e20),
        ])),
        &[0, 3, 4, 5, 6, 7, 7],
        None,
    );
    let expected = list(
        &target,
        Arc::new(
            Decimal128Array::from(vec![
                Some(9_007_199_254_740_992),
                Some(1),
                None,
                Some(4_503_599_627_370_497),
            ])
            .with_precision_and_scale(20, 0)
            .unwrap(),
        ),
        &[0, 3, 4, 4, 4, 4, 4],
        Some(&[true, true, false, false, false, true]),
    );
    for mode in modes() {
        let actual = evaluate(&prepare(&source, &target, mode), &input, Selection::all(6));
        same(&actual, &expected, &target);
        let expected_errors = if mode == ArrayCastFailureMode::ReportError {
            vec![
                (2, INVALID_CAST_ARGUMENT),
                (3, NUMERIC_OUT_OF_RANGE),
                (4, NUMERIC_OUT_OF_RANGE),
            ]
        } else {
            vec![]
        };
        domain_errors(&actual, &expected_errors);
    }
}

#[test]
fn decimal_double_array_preserves_independent_correct_rounding_bits() {
    let source = ty(DataType::Decimal128(20, 1), true, true, "source-decimal");
    let target = ty(DataType::Float64, true, true, "target-double");
    let input = list(
        &source,
        Arc::new(
            Decimal128Array::from(vec![
                Some(90_071_992_547_409_930),
                Some(45_035_996_273_704_965),
                None,
                Some(1),
                Some(35),
            ])
            .with_precision_and_scale(20, 1)
            .unwrap(),
        ),
        &[0, 3, 5, 5],
        None,
    );
    let expected = list(
        &target,
        Arc::new(Float64Array::from(vec![
            Some(f64::from_bits(0x4340_0000_0000_0000)),
            Some(f64::from_bits(0x4330_0000_0000_0000)),
            None,
            Some(f64::from_bits(0x3fb9_9999_9999_999a)),
            Some(3.5),
        ])),
        &[0, 3, 5, 5],
        None,
    );
    for mode in modes() {
        let actual = evaluate(&prepare(&source, &target, mode), &input, Selection::all(3));
        same(&actual, &expected, &target);
        assert!(actual.errors().is_empty());
        let values = actual
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.value(0).to_bits(), 0x4340_0000_0000_0000);
        assert_eq!(values.value(1).to_bits(), 0x4330_0000_0000_0000);
    }
}

#[test]
fn text_array_uses_java_trim_bmp_digits_and_rejects_real_invalid_values() {
    let source = ty(DataType::Utf8, true, true, "source-text");
    let target = ty(DataType::Int32, true, true, "target-text");
    let input = list(
        &source,
        Arc::new(StringArray::from(vec![
            Some(" 17 "),
            Some("١٢"),
            Some("1٢3"),
            Some("\0\t+17\r\u{1f}"),
            Some("１２"),
            Some("१२"),
            Some("-١٢"),
            None,
            Some("bad"),
            Some("2147483648"),
            Some("\u{a0}17\u{a0}"),
            Some("𝟏"),
            Some("17 2"),
            Some(" "),
        ])),
        &[0, 8, 9, 10, 11, 12, 13, 14, 14],
        None,
    );
    let expected = list(
        &target,
        Arc::new(Int32Array::from(vec![
            Some(17),
            Some(12),
            Some(123),
            Some(17),
            Some(12),
            Some(12),
            Some(-12),
            None,
        ])),
        &[0, 8, 8, 8, 8, 8, 8, 8, 8],
        Some(&[true, false, false, false, false, false, false, true]),
    );
    for mode in modes() {
        let actual = evaluate(&prepare(&source, &target, mode), &input, Selection::all(8));
        same(&actual, &expected, &target);
        let errors = if mode == ArrayCastFailureMode::ReportError {
            (1..=6)
                .map(|ordinal| (ordinal, INVALID_CAST_ARGUMENT))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        domain_errors(&actual, &errors);
    }
    // Scalar CAST is a different admitted owner with its original safe NULL.
    let scalar_source = FunctionValueType::new(DataType::Utf8, true);
    let scalar_target = FunctionValueType::new(DataType::Int32, true);
    let legacy = PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &scalar_source,
        &scalar_target,
        DecimalOverflowPolicy::ReportError,
        false,
        None,
        crate::binding_test_control(),
    )
    .unwrap();
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("bad"), Some(" 17 ")]));
    let actual = evaluate(&legacy, &values, Selection::all(2));
    let expected: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
    assert_eq!(actual.values().to_data(), expected.to_data());
    assert!(actual.errors().is_empty());
}

#[test]
fn nested_bad_conversion_reaches_the_outer_whole_array_try_boundary() {
    let inner_source = ty(DataType::Utf8, true, true, "nested-source-item");
    let source = ty(
        inner_source.data_type().clone(),
        true,
        true,
        "outer-source-item",
    );
    let inner_target = ty(DataType::Int32, true, true, "nested-target-item");
    let target = ty(
        inner_target.data_type().clone(),
        true,
        true,
        "outer-target-item",
    );
    let inners = list(
        &inner_source,
        Arc::new(StringArray::from(vec![Some("1"), Some("bad"), Some("3")])),
        &[0, 1, 2, 3],
        None,
    );
    let input = list(&source, inners, &[0, 2, 3], None);
    let expected_inners = list(
        &inner_target,
        Arc::new(Int32Array::from(vec![Some(3)])),
        &[0, 1],
        None,
    );
    let expected = list(&target, expected_inners, &[0, 0, 1], Some(&[false, true]));
    for mode in modes() {
        let actual = evaluate(&prepare(&source, &target, mode), &input, Selection::all(2));
        same(&actual, &expected, &target);
        domain_errors(
            &actual,
            if mode == ArrayCastFailureMode::ReportError {
                &[(0, INVALID_CAST_ARGUMENT)]
            } else {
                &[]
            },
        );
        assert!(actual.values().is_null(0));
    }
}

#[test]
fn selected_sliced_and_constant_arrays_keep_original_addresses_and_unreachable_children() {
    let source = ty(DataType::Utf8, true, true, "address-source");
    let target = ty(DataType::Int32, true, true, "address-target");
    let full = list(
        &source,
        Arc::new(StringArray::from(vec![
            Some("bad-prefix"),
            Some("2"),
            Some("bad-hidden"),
            Some("4"),
            Some("5"),
            Some("bad-tail"),
        ])),
        &[0, 1, 2, 3, 4, 5, 6],
        Some(&[true, true, false, true, true, true]),
    );
    let sliced = full.slice(1, 4);
    assert_eq!(
        sliced
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_offsets(),
        &[1, 2, 3, 4, 5]
    );
    let rows = [0, 1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    assert!(Selection::try_sparse(4, &[0, 0]).is_err());
    for mode in modes() {
        let recipe = prepare(&source, &target, mode);
        let actual = evaluate(&recipe, &sliced, selection);
        let expected = list(
            &target,
            Arc::new(Int32Array::from(vec![Some(2), Some(5)])),
            &[0, 1, 1, 2],
            Some(&[true, false, true]),
        );
        same(&actual, &expected, &target);
        assert_eq!(actual.selection().iter().collect::<Vec<_>>(), rows);
        assert!(actual.errors().is_empty());
        let empty = evaluate(&recipe, &sliced, Selection::try_sparse(4, &[]).unwrap());
        let expected_empty = list(
            &target,
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            &[0],
            None,
        );
        same(&empty, &expected_empty, &target);
        assert!(empty.errors().is_empty());

        let pool = ConstantPool::try_new(
            Arc::new(source.try_to_field("actual-array-pool").unwrap()),
            source.clone(),
            full.to_data(),
            ConstantPolicy {
                max_rows: 16,
                max_array_nodes: 64,
                max_logical_elements: 4096,
                max_retained_buffer_bytes: 65536,
                max_type_depth: 8,
                max_type_nodes: 64,
                max_dictionary_depth: 4,
                max_metadata_bytes: 4096,
                max_library_validation_work: 65536,
                max_library_validation_bytes: 131072,
            },
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let constant = pool.value(4).unwrap();
        assert_eq!(constant.ordinal(), 4);
        let actual = recipe
            .evaluate_selected(
                EvaluatedArgument::Constant(&constant),
                selection,
                &[],
                &Control::default(),
            )
            .unwrap();
        let expected = list(
            &target,
            Arc::new(Int32Array::from(vec![Some(5), Some(5), Some(5)])),
            &[0, 1, 2, 3],
            None,
        );
        same(&actual, &expected, &target);
    }
}

#[derive(Debug)]
struct OriginalData {
    id: u64,
}
impl fmt::Display for OriginalData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original source division failure")
    }
}
impl Error for OriginalData {}
impl FailureProjection for OriginalData {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(FailureClass::Data, DIVISION_BY_ZERO)
    }
}

#[test]
fn whole_try_preserves_the_actual_inherited_source_and_requires_its_original_journal() {
    let source = ty(DataType::Utf8, true, true, "original-source");
    let target = ty(DataType::Int32, true, true, "original-target");
    let compact = list(
        &source,
        Arc::new(StringArray::from(vec![
            Some("hidden-bad"),
            Some("bad"),
            Some("3"),
        ])),
        &[0, 1, 2, 3],
        Some(&[false, true, true]),
    );
    let rows = [1, 3, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let original = SharedFailureCause::new(OriginalData { id: 731 });
    let original_error = RowDataError::from_domain(0, original.clone()).unwrap();
    let selected = SelectedValues::try_new(
        selection,
        source.data_type(),
        compact,
        vec![original_error.clone()].into_boxed_slice(),
    )
    .unwrap();
    for mode in modes() {
        let recipe = prepare(&source, &target, mode);
        let actual = recipe
            .evaluate_selected(
                EvaluatedArgument::SelectedColumn(&selected),
                selection,
                selected.errors(),
                &Control::default(),
            )
            .unwrap();
        let expected = list(
            &target,
            Arc::new(Int32Array::from(vec![Some(3)])),
            &[0, 0, 0, 1],
            Some(&[false, false, true]),
        );
        same(&actual, &expected, &target);
        assert_eq!(actual.errors()[0], original_error);
        let retained = std::error::Error::source(&actual.errors()[0])
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        assert!(std::ptr::eq(
            retained.downcast_ref::<OriginalData>().unwrap(),
            original.downcast_ref::<OriginalData>().unwrap()
        ));
        assert_eq!(retained.downcast_ref::<OriginalData>().unwrap().id, 731);
        let expected_errors = if mode == ArrayCastFailureMode::ReportError {
            vec![(0, DIVISION_BY_ZERO), (1, INVALID_CAST_ARGUMENT)]
        } else {
            vec![(0, DIVISION_BY_ZERO)]
        };
        domain_errors(&actual, &expected_errors);
        let copied = selected.errors().to_vec();
        assert!(matches!(
            recipe.evaluate_selected(
                EvaluatedArgument::SelectedColumn(&selected),
                selection,
                &copied,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let foreign_rows = [0, 2, 4];
        assert!(matches!(
            recipe.evaluate_selected(
                EvaluatedArgument::SelectedColumn(&selected),
                Selection::try_sparse(6, &foreign_rows).unwrap(),
                selected.errors(),
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn whole_try_never_catches_actual_control_capacity_or_contract_refusals() {
    let source = ty(DataType::Utf8, true, true, "control-source");
    let target = ty(DataType::Int32, true, true, "control-target");
    let input = list(
        &source,
        Arc::new(StringArray::from(vec![Some("١٢"), Some("bad")])),
        &[0, 1, 2],
        None,
    );
    let recipe = prepare(&source, &target, ArrayCastFailureMode::OutputNull);
    let completed = Control::default();
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            Selection::all(2),
            &[],
            &completed,
        )
        .unwrap();
    let expected = list(
        &target,
        Arc::new(Int32Array::from(vec![Some(12)])),
        &[0, 1, 1],
        Some(&[true, false]),
    );
    same(&actual, &expected, &target);
    assert!(actual.errors().is_empty());
    let callbacks = completed.calls.lock().unwrap().len();
    let capacity_reason = novarocks_failure_contract::FailureReason::new(
        novarocks_failure_contract::domain::MEMORY,
        std::num::NonZeroU16::new(65000).unwrap(),
    );
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::Capacity(KernelCapacityRefusal::new(capacity_reason, 17, 16, 8)),
        invalid("original invalid program"),
        internal("original internal refusal"),
        KernelFailure::Operational(KernelDiagnostic::new("original external failure")),
        KernelFailure::InstanceFailed,
    ] {
        for stop in 0..callbacks {
            let control = Control {
                calls: Mutex::default(),
                refusal: Some((stop, cause.clone())),
            };
            let error = recipe
                .evaluate_selected(
                    EvaluatedArgument::Column(&input),
                    Selection::all(2),
                    &[],
                    &control,
                )
                .unwrap_err();
            assert_eq!(error, cause);
            assert_eq!(control.calls.lock().unwrap().len(), stop + 1);
        }
    }
}

#[derive(Debug)]
struct ForeignList(ArrayRef);
// SAFETY: Every Arrow layout/buffer method delegates to the immutable real
// List. Only Any identity differs to exercise the required concrete-class gate.
unsafe impl Array for ForeignList {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn to_data(&self) -> arrow_data::ArrayData {
        self.0.to_data()
    }
    fn into_data(self) -> arrow_data::ArrayData {
        self.0.to_data()
    }
    fn data_type(&self) -> &DataType {
        self.0.data_type()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        self.0.slice(offset, length)
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn offset(&self) -> usize {
        self.0.offset()
    }
    fn nulls(&self) -> Option<&NullBuffer> {
        self.0.nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.0.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.0.get_array_memory_size()
    }
}

#[test]
fn array_mode_rejects_required_child_metadata_and_forged_concrete_carriers() {
    let source = ty(DataType::Int32, true, true, "trusted-source");
    let target = ty(DataType::Int64, true, true, "trusted-target");
    let required = ty(DataType::Int64, false, true, "required-target");
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &required,
            DecimalOverflowPolicy::ReportError,
            false,
            Some(ArrayCastFailureMode::OutputNull),
            crate::binding_test_control()
        ),
        Err(CastPrepareError::TypeMismatch)
    );
    let input = list(
        &source,
        Arc::new(Int32Array::from(vec![Some(7)])),
        &[0, 1],
        None,
    );
    let recipe = prepare(&source, &target, ArrayCastFailureMode::OutputNull);
    // The original FVT normalizer intentionally discards incidental metadata.
    // Forge the actual Arrow input instead of normalizing two equal fixtures.
    let DataType::List(original_field) = source.data_type() else {
        unreachable!()
    };
    let foreign: ArrayRef = Arc::new(ListArray::new(
        Arc::new(
            original_field
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(
                    "foreign_owner".into(),
                    "unbound-input".into(),
                )])),
        ),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        Arc::new(Int32Array::from(vec![Some(7)])),
        None,
    ));
    assert!(matches!(
        recipe.evaluate_selected(
            EvaluatedArgument::Column(&foreign),
            Selection::all(1),
            &[],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let null_input = list(
        &source,
        Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
        &[0, 0],
        Some(&[false]),
    );
    let forged_null: ArrayRef = Arc::new(ForeignList(null_input));
    assert!(matches!(
        recipe.evaluate_selected(
            EvaluatedArgument::Column(&forged_null),
            Selection::all(1),
            &[],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let forged: ArrayRef = Arc::new(ForeignList(input));
    assert!(matches!(
        recipe.evaluate_selected(
            EvaluatedArgument::Column(&forged),
            Selection::all(1),
            &[],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn strict_array_failure_reason_follows_actual_element_order() {
    let source = ty(DataType::Float64, true, true, "ordered-double-source");
    let target = ty(
        DataType::Decimal128(20, 0),
        true,
        true,
        "ordered-decimal-target",
    );
    let input = list(
        &source,
        Arc::new(Float64Array::from(vec![
            f64::INFINITY,
            f64::NAN,
            f64::NAN,
            f64::INFINITY,
        ])),
        &[0, 2, 4],
        None,
    );
    let expected = list(
        &target,
        Arc::new(
            Decimal128Array::from(Vec::<Option<i128>>::new())
                .with_precision_and_scale(20, 0)
                .unwrap(),
        ),
        &[0, 0, 0],
        Some(&[false, false]),
    );
    for mode in modes() {
        let actual = evaluate(&prepare(&source, &target, mode), &input, Selection::all(2));
        same(&actual, &expected, &target);
        domain_errors(
            &actual,
            if mode == ArrayCastFailureMode::ReportError {
                &[(0, NUMERIC_OUT_OF_RANGE), (1, INVALID_CAST_ARGUMENT)]
            } else {
                &[]
            },
        );
    }
}

#[test]
fn explicit_unknown_arrays_keep_json_nominal_and_text_converts_through_its_author() {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let json = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let semantics = ValueTypeRef::try_new(ValueKind::List(json), true, storage.clone()).unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new(
            "json_target",
            DataType::LargeBinary,
            true,
        ))),
        storage,
    )
    .unwrap();
    let target = FunctionValueType::try_from_value(semantics, layout).unwrap();
    assert_eq!(
        target.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let source = ty(DataType::Null, true, true, "unknown-list-source");
    for operation in [CastOperation::Time, CastOperation::TimeFromDatetime] {
        for (mode, expected) in [
            (None, CastPrepareError::Unsupported),
            (
                Some(ArrayCastFailureMode::ReportError),
                CastPrepareError::TypeMismatch,
            ),
            (
                Some(ArrayCastFailureMode::OutputNull),
                CastPrepareError::TypeMismatch,
            ),
        ] {
            assert_eq!(
                PreparedCastRecipe::try_new(
                    operation,
                    &source,
                    &target,
                    DecimalOverflowPolicy::OutputNull,
                    false,
                    mode,
                    crate::binding_test_control(),
                ),
                Err(expected),
                "{operation:?} cannot prepare a NULL-list to JSON-list cast with {mode:?}"
            );
        }
    }
    let input = list(
        &source,
        Arc::new(arrow_array::NullArray::new(1)),
        &[0, 0, 1, 1],
        Some(&[true, true, false]),
    );
    let expected = list(
        &target,
        Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>])),
        &[0, 0, 1, 1],
        Some(&[true, true, false]),
    );
    for mode in modes() {
        let actual = evaluate(&prepare(&source, &target, mode), &input, Selection::all(3));
        same(&actual, &expected, &target);
        assert!(actual.errors().is_empty());
        let unknown_root = FunctionValueType::new(DataType::Null, true);
        let root_input: ArrayRef = Arc::new(arrow_array::NullArray::new(1));
        let actual = evaluate(
            &prepare(&unknown_root, &target, mode),
            &root_input,
            Selection::all(1),
        );
        let expected = list(
            &target,
            Arc::new(LargeBinaryArray::from(Vec::<Option<&[u8]>>::new())),
            &[0, 0],
            Some(&[false]),
        );
        same(&actual, &expected, &target);
        assert!(actual.errors().is_empty());
        let text = ty(DataType::Utf8, true, true, "ordinary-text");
        let raw = list(
            &text,
            Arc::new(StringArray::from(vec![
                Some("not json"),
                Some("null"),
                None,
            ])),
            &[0, 3],
            None,
        );
        let conversion = prepare(&text, &target, mode);
        let actual = evaluate(&conversion, &raw, Selection::all(1));
        let expected = list(
            &target,
            Arc::new(LargeBinaryArray::from(vec![
                Some(&b"\x01\x44\x08not json"[..]),
                Some(&b"\x01\x44\x04null"[..]),
                None,
            ])),
            &[0, 3],
            None,
        );
        same(&actual, &expected, &target);
        assert!(actual.errors().is_empty());
        // A real converter owns the bytes/domain change. Legacy carrier CAST
        // with absence cannot acquire the nominal JSON target by retagging.
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &text,
                &target,
                DecimalOverflowPolicy::ReportError,
                false,
                None,
                crate::binding_test_control()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
}

#[test]
fn json_array_conversion_keeps_json_and_sql_null_separate_from_owned_failures() {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let semantics = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
        storage,
    )
    .unwrap();
    let source = FunctionValueType::try_from_value(semantics, layout).unwrap();
    let target = ty(DataType::Utf8, true, true, "varchar-target");
    let input = list(
        &source,
        Arc::new(LargeBinaryArray::from(vec![
            Some(&b"\x01\x50\x00\x00\x00\x00\x00\x00\x59\x40"[..]),
            Some(&b"\x01\x52\x04\x01\x02\xe8\x03"[..]),
            Some(&b"\x01\x20"[..]),
            Some(&b"\x01\x44\x04text"[..]),
            None,
            Some(&b"\x01\x80\x01\x00"[..]),
        ])),
        &[0, 5, 6],
        None,
    );
    let damaged = list(
        &source,
        Arc::new(LargeBinaryArray::from(vec![Some(&b"\x02\x20"[..])])),
        &[0, 1],
        None,
    );
    for mode in modes() {
        let recipe = prepare(&source, &target, mode);
        let actual = evaluate(&recipe, &input, Selection::all(2));
        let expected = list(
            &target,
            Arc::new(StringArray::from(vec![
                Some("1.0E2"),
                Some("100.0"),
                None,
                Some("text"),
                None,
            ])),
            &[0, 5, 5],
            Some(&[true, false]),
        );
        same(&actual, &expected, &target);
        if mode == ArrayCastFailureMode::ReportError {
            let [error] = actual.errors() else {
                panic!("one failed original array row");
            };
            assert_eq!(error.selected_ordinal(), 1);
            assert_eq!(
                error.reason(),
                DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
            );
            assert_eq!(error.projected_fact().class(), FailureClass::Data);
        } else {
            assert!(actual.errors().is_empty());
        }
        let host: Arc<dyn crate::AggregateStateAllocator> =
            Arc::new(crate::source_custody_direct_tests::Host::default());
        let error = recipe
            .evaluate_selected_scoped(
                EvaluatedArgument::Column(&damaged),
                Selection::all(1),
                &[],
                &Control::default(),
                Some(&host),
            )
            .unwrap_err();
        assert!(matches!(error, KernelFailure::Internal(_)));
    }
}

#[test]
fn explicit_json_text_guards_distinguish_json_null_from_required_string_encoding() {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        false,
        storage.clone(),
    )
    .unwrap();
    let value = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new(
            "declared_json",
            DataType::LargeBinary,
            false,
        ))),
        storage,
    )
    .unwrap();
    let required_json = FunctionValueType::try_from_value(value, layout).unwrap();
    let required_text = ty(DataType::Utf8, false, true, "required-text");
    let optional_text = ty(DataType::Utf8, true, true, "optional-text");
    let json_input = list(
        &required_json,
        Arc::new(LargeBinaryArray::from(vec![
            Some(&b"\x01\x20"[..]),
            Some(&b"\x01\x44\x01x"[..]),
        ])),
        &[0, 2],
        None,
    );
    for (source, target) in [
        (&required_json, &optional_text),
        (&required_text, &required_json),
    ] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                source,
                target,
                DecimalOverflowPolicy::ReportError,
                false,
                None,
                crate::binding_test_control()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
    for mode in modes() {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &required_json,
                &required_text,
                DecimalOverflowPolicy::ReportError,
                false,
                Some(mode),
                crate::binding_test_control()
            ),
            Err(CastPrepareError::TypeMismatch)
        );
        let actual = evaluate(
            &prepare(&required_json, &optional_text, mode),
            &json_input,
            Selection::all(1),
        );
        let expected = list(
            &optional_text,
            Arc::new(StringArray::from(vec![None, Some("x")])),
            &[0, 2],
            None,
        );
        same(&actual, &expected, &optional_text);
        assert!(actual.errors().is_empty());
        let input = list(
            &required_text,
            Arc::new(StringArray::from(vec![Some("null"), Some("bad")])),
            &[0, 2],
            None,
        );
        let actual = evaluate(
            &prepare(&required_text, &required_json, mode),
            &input,
            Selection::all(1),
        );
        let expected = list(
            &required_json,
            Arc::new(LargeBinaryArray::from(vec![
                Some(&b"\x01\x44\x04null"[..]),
                Some(&b"\x01\x44\x03bad"[..]),
            ])),
            &[0, 2],
            None,
        );
        same(&actual, &expected, &required_json);
        assert!(actual.errors().is_empty());
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &optional_text,
                &required_json,
                DecimalOverflowPolicy::ReportError,
                false,
                Some(mode),
                crate::binding_test_control()
            ),
            Err(CastPrepareError::TypeMismatch)
        );
    }
}

#[test]
fn trino_array_date_uses_original_selected_date32_author_and_whole_array_try() {
    let source = ty(DataType::Utf8, true, true, "date-text-source");
    let target = ty(DataType::Date32, true, true, "date-text-target");
    let input = list(
        &source,
        Arc::new(StringArray::from(vec![
            Some("1970-01-01"),
            Some("bad"),
            Some("2000-02-29"),
            None,
            Some("-5877641-06-23"),
            Some("+5881580-07-11"),
        ])),
        &[0, 2, 4, 6],
        None,
    );
    let strict = prepare(&source, &target, ArrayCastFailureMode::ReportError);
    let output = evaluate(&strict, &input, Selection::all(3));
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 0);
    assert_eq!(
        output.errors()[0].reason(),
        DataFailureReason::Domain(INVALID_CAST_ARGUMENT)
    );
    assert!(std::error::Error::source(&output.errors()[0]).is_some());
    let expected = list(
        &target,
        Arc::new(Date32Array::from(vec![
            Some(11016),
            None,
            Some(i32::MIN),
            Some(i32::MAX),
        ])),
        &[0, 0, 2, 4],
        Some(&[false, true, true]),
    );
    same(&output, &expected, &target);
    let safe = prepare(&source, &target, ArrayCastFailureMode::OutputNull);
    let output = evaluate(&safe, &input, Selection::all(3));
    assert!(output.errors().is_empty());
    same(&output, &expected, &target);
}
#[test]
fn trino_date_array_preserves_selected_control_refusals_and_original_source_errors() {
    let source = ty(DataType::Utf8, true, true, "date-control-source");
    let target = ty(DataType::Date32, true, true, "date-control-target");
    let input = list(
        &source,
        Arc::new(StringArray::from(vec![format!(
            "{}2000-02-29",
            " ".repeat(1024)
        )])),
        &[0, 1],
        None,
    );
    let recipe = prepare(&source, &target, ArrayCastFailureMode::ReportError);
    let observed = Control::default();
    recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            Selection::all(1),
            &[],
            &observed,
        )
        .unwrap();
    let count = observed.calls.lock().unwrap().len();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original date source refusal")),
    ] {
        for at in 0..count {
            let control = Control {
                calls: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(
                recipe
                    .evaluate_selected(
                        EvaluatedArgument::Column(&input),
                        Selection::all(1),
                        &[],
                        &control
                    )
                    .unwrap_err(),
                cause
            );
            assert_eq!(control.calls.lock().unwrap().len(), at + 1);
        }
    }
    let failed = list(
        &source,
        Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        &[0, 0],
        Some(&[false]),
    );
    let original = RowDataError::with_reason(
        0,
        DataFailureReason::DivisionByZero,
        "original DATE source failed",
    );
    let source = SelectedValues::try_new(
        Selection::all(1),
        source.data_type(),
        failed,
        vec![original.clone()].into_boxed_slice(),
    )
    .unwrap();
    let safe = prepare(
        recipe.source_type(),
        &target,
        ArrayCastFailureMode::OutputNull,
    );
    let output = safe
        .evaluate_selected(
            EvaluatedArgument::SelectedColumn(&source),
            Selection::all(1),
            source.errors(),
            &Control::default(),
        )
        .unwrap();
    assert_eq!(output.errors(), &[original]);
    assert!(output.values().is_null(0));
}

#[test]
fn signed_array_to_varchar_reuses_original_text_author_at_all_exact_widths() {
    let cases: Vec<(DataType, ArrayRef, Vec<Option<&str>>)> = vec![
        (
            DataType::Int8,
            Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
            vec![Some("-128"), None, Some("127")],
        ),
        (
            DataType::Int16,
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
            vec![Some("-32768"), None, Some("32767")],
        ),
        (
            DataType::Int32,
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)])),
            vec![Some("-2147483648"), None, Some("2147483647")],
        ),
        (
            DataType::Int64,
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
            vec![
                Some("-9223372036854775808"),
                None,
                Some("9223372036854775807"),
            ],
        ),
    ];
    for (carrier, values, expected) in cases {
        let source = ty(carrier, true, true, "source-signed-text");
        let target = ty(DataType::Utf8, true, true, "target-signed-text");
        let input = list(&source, values, &[0, 3], None);
        for mode in [
            ArrayCastFailureMode::ReportError,
            ArrayCastFailureMode::OutputNull,
        ] {
            let recipe = prepare(&source, &target, mode);
            let actual = evaluate(&recipe, &input, Selection::all(1));
            assert!(actual.errors().is_empty());
            assert!(!actual.values().is_null(0));
            let values = actual
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0);
            assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }
}
#[test]
fn signed_array_text_preserves_sparse_parent_null_and_inherited_source_journal() {
    let source = ty(DataType::Int64, true, true, "signed-text-sparse-source");
    let target = ty(DataType::Utf8, true, true, "signed-text-sparse-target");
    let input = list(
        &source,
        Arc::new(Int64Array::from(vec![
            Some(7),
            None,
            Some(i64::MIN),
            Some(i64::MAX),
        ])),
        &[0, 1, 2, 4],
        Some(&[true, false, true]),
    );
    let rows = [1, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let recipe = prepare(&source, &target, ArrayCastFailureMode::ReportError);
    let output = evaluate(&recipe, &input, selection);
    let expected = list(
        &target,
        Arc::new(StringArray::from(vec![
            Some("-9223372036854775808"),
            Some("9223372036854775807"),
        ])),
        &[0, 0, 2],
        Some(&[false, true]),
    );
    same(&output, &expected, &target);
    let failed = list(
        &source,
        Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
        &[0, 0],
        Some(&[false]),
    );
    let original = RowDataError::with_reason(
        0,
        DataFailureReason::DivisionByZero,
        "original producer failed",
    );
    let selected = SelectedValues::try_new(
        Selection::all(1),
        source.data_type(),
        failed,
        vec![original.clone()].into_boxed_slice(),
    )
    .unwrap();
    let recipe = prepare(&source, &target, ArrayCastFailureMode::OutputNull);
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::SelectedColumn(&selected),
            Selection::all(1),
            selected.errors(),
            &Control::default(),
        )
        .unwrap();
    assert_eq!(actual.errors(), &[original]);
    assert!(actual.values().is_null(0));
}
#[test]
fn signed_text_profile_does_not_enable_other_scalar_text_families() {
    for carrier in [
        DataType::UInt32,
        DataType::Boolean,
        DataType::Float64,
        DataType::Decimal128(20, 1),
    ] {
        let source = ty(carrier, true, true, "unadmitted-other-text");
        let target = ty(DataType::Utf8, true, true, "signed-text-only");
        assert!(matches!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &target,
                DecimalOverflowPolicy::ReportError,
                false,
                Some(ArrayCastFailureMode::ReportError),
                crate::binding_test_control()
            ),
            Err(CastPrepareError::Unsupported)
        ));
    }
}
