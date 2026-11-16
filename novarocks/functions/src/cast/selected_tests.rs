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
use crate::{ConstantPolicy, ConstantPool, KernelDiagnostic, SelectedValues, Selection};
use arrow_array::{ArrayRef, Time64MicrosecondArray};
use std::{
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
            assert!(at <= *stop, "callback after the original refusal");
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
        panic!("selected CAST never waits")
    }
}
fn ty(carrier: DataType) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(carrier, true)
}
fn prepare(
    source: DataType,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &ty(source),
        &ty(target),
        policy,
        allow,
        None,
        crate::binding_test_control(),
    )
    .unwrap()
}
fn same(actual: &SelectedValues<'_>, expected: &ArrayRef) {
    assert_eq!(actual.values().data_type(), expected.data_type());
    assert_eq!(actual.values().to_data(), expected.to_data());
}
fn policies() -> [DecimalOverflowPolicy; 2] {
    [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ]
}

#[test]
fn selected_scalar_result_author_keeps_independent_numeric_text_decimal_and_time_values() {
    let fixtures: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(Int64Array::from(vec![
                Some(-129),
                Some(-7),
                None,
                Some(256),
                Some(127),
            ])),
            Arc::new(Int8Array::from(vec![None, Some(-7), None, None, Some(127)])),
        ),
        (
            Arc::new(Int64Array::from(vec![Some(12), Some(-3), None])),
            Arc::new(Float64Array::from(vec![Some(12.0), Some(-3.0), None])),
        ),
        (
            Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-2.0)])),
            Arc::new(Float32Array::from(vec![Some(1.5), None, Some(-2.0)])),
        ),
        (
            Arc::new(UInt64Array::from(vec![Some(7), None, Some(255)])),
            Arc::new(UInt8Array::from(vec![Some(7), None, Some(255)])),
        ),
        (
            Arc::new(Int32Array::from(vec![Some(0), None, Some(-1)])),
            Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
        ),
        // Existing text-to-integer invalid input is a successful NULL under
        // these policies. Extraction does not introduce strict CAST semantics.
        (
            Arc::new(StringArray::from(vec![
                Some("17"),
                Some("bad"),
                None,
                Some("-9"),
            ])),
            Arc::new(Int64Array::from(vec![Some(17), None, None, Some(-9)])),
        ),
        (
            Arc::new(Int64Array::from(vec![Some(12), Some(-3), None])),
            Arc::new(StringArray::from(vec![Some("12"), Some("-3"), None])),
        ),
        (
            Arc::new(Int64Array::from(vec![Some(12), Some(-3), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(1200), Some(-300), None])
                    .with_precision_and_scale(8, 2)
                    .unwrap(),
            ),
        ),
        (
            Arc::new(Date32Array::from(vec![Some(-1), None, Some(1)])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-86_400_000_000),
                None,
                Some(86_400_000_000),
            ])),
        ),
        (
            Arc::new(TimestampSecondArray::from(vec![Some(-1), None, Some(2)])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-1_000_000),
                None,
                Some(2_000_000),
            ])),
        ),
        (
            Arc::new(StringArray::from(vec![
                Some("01:02:03"),
                None,
                Some("00:00:00"),
            ])),
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(3_723_000_000),
                None,
                Some(0),
            ])),
        ),
    ];
    for (input, expected) in fixtures {
        for policy in policies() {
            for allow in [false, true] {
                let recipe = prepare(
                    input.data_type().clone(),
                    expected.data_type().clone(),
                    policy,
                    allow,
                );
                assert_eq!(recipe.source_type(), &ty(input.data_type().clone()));
                assert_eq!(recipe.result_type(), &ty(expected.data_type().clone()));
                assert_eq!(recipe.policy(), policy);
                assert_eq!(recipe.allow_throw_exception(), allow);
                let actual = recipe
                    .evaluate_selected(
                        EvaluatedArgument::Column(&input),
                        Selection::all(input.len()),
                        &[],
                        &Control::default(),
                    )
                    .unwrap();
                same(&actual, &expected);
                assert!(actual.errors().is_empty());
            }
        }
    }
}

#[test]
fn selected_scalar_result_author_keeps_sparse_zero_selection_and_nonzero_constant_ordinal() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(11), None, Some(256), Some(-7)]));
    let recipe = prepare(
        DataType::Int64,
        DataType::Float64,
        DecimalOverflowPolicy::ReportError,
        false,
    );
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            selection,
            &[],
            &Control::default(),
        )
        .unwrap();
    let expected: ArrayRef = Arc::new(Float64Array::from(vec![None, Some(-7.0)]));
    same(&actual, &expected);
    assert_eq!(actual.selection().iter().collect::<Vec<_>>(), rows);
    let empty = Selection::try_sparse(4, &[]).unwrap();
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            empty,
            &[],
            &Control::default(),
        )
        .unwrap();
    let expected: ArrayRef = Arc::new(Float64Array::from(Vec::<Option<f64>>::new()));
    same(&actual, &expected);
    assert!(actual.errors().is_empty());

    let source = ty(DataType::Int64);
    let pool = ConstantPool::try_new(
        Arc::new(source.try_to_field("actual-pool").unwrap()),
        source,
        input.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 8,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 8,
            max_type_nodes: 64,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let constant = pool.value(2).unwrap();
    assert_eq!(constant.ordinal(), 2);
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Constant(&constant),
            selection,
            &[],
            &Control::default(),
        )
        .unwrap();
    let expected: ArrayRef = Arc::new(Float64Array::from(vec![Some(256.0), Some(256.0)]));
    same(&actual, &expected);
    let scalar = input.slice(3, 1);
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Scalar(&scalar),
            selection,
            &[],
            &Control::default(),
        )
        .unwrap();
    let expected: ArrayRef = Arc::new(Float64Array::from(vec![Some(-7.0), Some(-7.0)]));
    same(&actual, &expected);
}

#[test]
fn selected_scalar_result_author_keeps_original_inherited_and_decimal_error_ordinals() {
    let rows = [1, 3, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let input: ArrayRef = Arc::new(Float64Array::from(vec![None, Some(9999.0), Some(12.5)]));
    let original_error = RowDataError::with_reason(
        0,
        crate::DataFailureReason::DivisionByZero,
        "original input failure",
    );
    let selected = SelectedValues::try_new(
        selection,
        &DataType::Float64,
        input,
        vec![original_error.clone()].into_boxed_slice(),
    )
    .unwrap();
    for policy in policies() {
        let recipe = prepare(DataType::Float64, DataType::Decimal128(3, 1), policy, false);
        let actual = recipe
            .evaluate_selected(
                EvaluatedArgument::SelectedColumn(&selected),
                selection,
                selected.errors(),
                &Control::default(),
            )
            .unwrap();
        let expected: ArrayRef = Arc::new(
            Decimal128Array::from(vec![None, None, Some(125)])
                .with_precision_and_scale(3, 1)
                .unwrap(),
        );
        same(&actual, &expected);
        let mut expected_errors = vec![original_error.clone()];
        if policy == DecimalOverflowPolicy::ReportError {
            expected_errors.push(RowDataError::new(
                1,
                "Expr evaluate meet error: The numeric type cast involving decimal overflows",
            ));
        }
        assert_eq!(actual.errors(), expected_errors.as_slice());
        assert_eq!(actual.selection().iter().collect::<Vec<_>>(), rows);
        // Matching diagnostic text does not grant custody of another journal.
        assert!(matches!(
            recipe.evaluate_selected(
                EvaluatedArgument::SelectedColumn(&selected),
                selection,
                &selected.errors().to_vec(),
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let foreign_rows = [0, 2, 4];
        let foreign = Selection::try_sparse(5, &foreign_rows).unwrap();
        assert!(matches!(
            recipe.evaluate_selected(
                EvaluatedArgument::SelectedColumn(&selected),
                foreign,
                selected.errors(),
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn selected_scalar_result_author_preserves_each_original_control_refusal_without_footer() {
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(-9)]));
    let recipe = prepare(
        DataType::Int64,
        DataType::Utf8,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let complete = Control::default();
    let output = recipe
        .evaluate_selected(EvaluatedArgument::Column(&input), selection, &[], &complete)
        .unwrap();
    let expected: ArrayRef = Arc::new(StringArray::from(vec![Some("2"), Some("-9")]));
    same(&output, &expected);
    let callbacks = complete.calls.lock().unwrap().len();
    assert!(callbacks > 4);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original invalid cause"),
        internal("original internal cause"),
        KernelFailure::Operational(KernelDiagnostic::new("original external operation")),
        KernelFailure::InstanceFailed,
    ] {
        for stop in 0..callbacks {
            let control = Control {
                calls: Mutex::default(),
                refusal: Some((stop, cause.clone())),
            };
            assert_eq!(
                recipe
                    .evaluate_selected(EvaluatedArgument::Column(&input), selection, &[], &control)
                    .unwrap_err(),
                cause.clone()
            );
            assert_eq!(control.calls.lock().unwrap().len(), stop + 1);
        }
    }
}

#[test]
fn selected_scalar_result_author_refuses_foreign_argument_shape_and_frozen_type() {
    let recipe = prepare(
        DataType::Int64,
        DataType::Float64,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let selection = Selection::all(2);
    let short: ArrayRef = Arc::new(Int64Array::from(vec![Some(7)]));
    let shape_error = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&short),
            selection,
            &[],
            &Control::default(),
        )
        .unwrap_err();
    crate::kernel_control::test_contract::exact(
        &shape_error,
        crate::EvaluationContractError::ArgumentLength,
    );
    let foreign: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), Some(8)]));
    assert!(matches!(
        recipe.evaluate_selected(
            EvaluatedArgument::Column(&foreign),
            selection,
            &[],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let inherited = [RowDataError::new(0, "not from this column")];
    assert!(matches!(
        recipe.evaluate_selected(
            EvaluatedArgument::Scalar(&short),
            selection,
            &inherited,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn unified_selected_port_keeps_the_existing_selected_array_body() {
    let input: ArrayRef = Arc::new(arrow_array::LargeStringArray::from(vec![
        Some("字"),
        None,
        Some("x"),
    ]));
    let recipe = prepare(
        DataType::LargeUtf8,
        DataType::Utf8,
        DecimalOverflowPolicy::ReportError,
        false,
    );
    assert!(recipe.uses_selected_array());
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            Selection::all(3),
            &[],
            &Control::default(),
        )
        .unwrap();
    let expected: ArrayRef = Arc::new(StringArray::from(vec![Some("字"), None, Some("x")]));
    same(&actual, &expected);
    assert!(actual.errors().is_empty());
}
