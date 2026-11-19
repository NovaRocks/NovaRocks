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
//! Exact intrinsic preparation/selection/control; no ordinary bitnot rebind.
use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::{Int8Array, Int16Array, Int32Array, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_functions::*;
use novarocks_type_contract::FunctionValueType;
use novarocks_type_contract::PureCompileControl;
use novarocks_type_contract::ValueLogicalType;
use novarocks_type_contract::{CompileControlError, CompilePhase};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Compile {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Compile {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = self.fail {
            assert!(at <= stop);
        }
        t.push((phase, units));
        match self.fail.clone() {
            Some((stop, e)) if stop == at => Err(e),
            _ => Ok(()),
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    fail: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.fail {
            assert!(at <= *stop, "callback after original refusal");
        }
        t.push(units);
        match &self.fail {
            Some((stop, e)) if *stop == at => Err(e.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("BitwiseNot never waits")
    }
}
fn prepared(ty: FunctionValueType) -> PreparedNativeBitNotRecipe {
    PreparedNativeBitNotRecipe::try_new(&ty, &ty, &Compile::default()).unwrap()
}
#[test]
fn native_bitnot_kernel_signed_four_profiles_preserve_exact_nonnull_nullable_empty_and_sparse() {
    let pairs: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, 1, i8::MAX])),
            Arc::new(Int8Array::from(vec![i8::MAX, 0, -1, -2, i8::MIN])),
        ),
        (
            Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, 1, i16::MAX])),
            Arc::new(Int16Array::from(vec![i16::MAX, 0, -1, -2, i16::MIN])),
        ),
        (
            Arc::new(Int32Array::from(vec![i32::MIN, -1, 0, 1, i32::MAX])),
            Arc::new(Int32Array::from(vec![i32::MAX, 0, -1, -2, i32::MIN])),
        ),
        (
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
            Arc::new(Int64Array::from(vec![i64::MAX, 0, -1, -2, i64::MIN])),
        ),
    ];
    for (input, expected) in pairs {
        for nullable in [false, true] {
            let ty = FunctionValueType::from_legacy_declared_projection(
                input.data_type().clone(),
                nullable,
            );
            let recipe = prepared(ty.clone());
            assert_eq!(recipe.source_type(), &ty);
            assert_eq!(recipe.result_type(), &ty);
            for (input, expected) in [
                (input.clone(), expected.clone()),
                (input.slice(1, 3), expected.slice(1, 3)),
                (input.slice(0, 0), expected.slice(0, 0)),
            ] {
                let actual = recipe
                    .evaluate_selected(
                        EvaluatedArgument::Column(&input),
                        novarocks_functions::Selection::all(input.len()),
                        &Control::default(),
                    )
                    .unwrap();
                assert!(actual.errors().is_empty());
                assert_eq!(actual.values().to_data(), expected.to_data());
            }
            let rows = [0, 4];
            let selection = novarocks_functions::Selection::try_sparse(5, &rows).unwrap();
            let actual = recipe
                .evaluate_selected(
                    EvaluatedArgument::Column(&input),
                    selection,
                    &Control::default(),
                )
                .unwrap();
            let expected = arrow_select::concat::concat(&[
                expected.slice(0, 1).as_ref(),
                expected.slice(4, 1).as_ref(),
            ])
            .unwrap();
            assert_eq!(actual.values().to_data(), expected.to_data());
        }
    }
}
#[test]
fn native_bitnot_kernel_largeint_full128_bits_nulls_and_scalar_domain() {
    let input = novarocks_functions::largeint::array_from_i128(&[
        Some(i128::MIN),
        Some(i128::MAX),
        Some(1_i128 << 100),
        None,
    ])
    .unwrap();
    let expected = novarocks_functions::largeint::array_from_i128(&[
        Some(i128::MAX),
        Some(i128::MIN),
        Some(-(1_i128 << 100) - 1),
        None,
    ])
    .unwrap();
    let ty = FunctionValueType::try_with_logical_type(
        input.data_type().clone(),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let recipe = prepared(ty);
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            novarocks_functions::Selection::all(4),
            &Control::default(),
        )
        .unwrap();
    assert_eq!(actual.values().to_data(), expected.to_data());
    let scalar = input.slice(0, 1);
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Scalar(&scalar),
            novarocks_functions::Selection::all(3),
            &Control::default(),
        )
        .unwrap();
    assert_eq!(
        actual.values().to_data(),
        novarocks_functions::largeint::array_from_i128(&[Some(i128::MAX); 3])
            .unwrap()
            .to_data()
    );
}
#[test]
fn native_bitnot_kernel_compact_inherited_errors_and_hidden_inactive_null_are_not_recomputed() {
    let rows = [1, 3];
    let selection = novarocks_functions::Selection::try_sparse(4, &rows).unwrap();
    let input: ArrayRef = Arc::new(Int8Array::from(vec![None, Some(-1), None, Some(1)]));
    let recipe = prepared(FunctionValueType::new(DataType::Int8, false));
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            selection,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(
        actual.values().to_data(),
        Int8Array::from(vec![0, -2]).to_data()
    );
    let compact = novarocks_functions::SelectedValues::try_new(
        selection,
        &DataType::Int8,
        Arc::new(Int8Array::from(vec![Some(-1), None])),
        Box::from([novarocks_functions::RowDataError::new(
            1,
            "original child failure",
        )]),
    )
    .unwrap();
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::SelectedColumn(&compact),
            selection,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(actual.errors(), compact.errors());
    assert_eq!(
        actual.values().to_data(),
        Int8Array::from(vec![Some(0), None]).to_data()
    );
}
#[test]
fn native_bitnot_kernel_every_runtime_callback_preserves_seven_causes_without_footer() {
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)]));
    let recipe = prepared(FunctionValueType::new(DataType::Int64, true));
    let success = Control::default();
    recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            novarocks_functions::Selection::all(3),
            &success,
        )
        .unwrap();
    let trace = success.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(novarocks_functions::KernelDiagnostic::new(
                "original invalid",
            )),
            KernelFailure::Internal(novarocks_functions::KernelDiagnostic::new(
                "original internal",
            )),
            KernelFailure::Operational(novarocks_functions::KernelDiagnostic::new(
                "original operational",
            )),
            KernelFailure::InstanceFailed,
        ] {
            let c = Control {
                trace: Mutex::new(Vec::new()),
                fail: Some((at, cause.clone())),
            };
            assert_eq!(
                recipe
                    .evaluate_selected(
                        EvaluatedArgument::Column(&input),
                        novarocks_functions::Selection::all(3),
                        &c
                    )
                    .unwrap_err(),
                cause
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn native_bitnot_kernel_every_preparation_callback_keeps_three_control_causes() {
    for ty in [
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Null, true),
    ] {
        let good = Compile::default();
        let _ = PreparedNativeBitNotRecipe::try_new(&ty, &ty, &good);
        let trace = good.trace.lock().unwrap().clone();
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let c = Compile {
                    trace: Mutex::new(Vec::new()),
                    fail: Some((at, cause.clone())),
                };
                assert_eq!(
                    PreparedNativeBitNotRecipe::try_new(&ty, &ty, &c)
                        .unwrap_err()
                        .control_error(),
                    Some(cause)
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
#[test]
fn native_bitnot_kernel_unsigned_physical_null_and_opaque_tags_are_explicit_prepare_refusals() {
    for ty in [
        FunctionValueType::new(DataType::UInt8, false),
        FunctionValueType::new(DataType::UInt16, true),
        FunctionValueType::new(DataType::UInt32, false),
        FunctionValueType::new(DataType::UInt64, true),
        FunctionValueType::new(DataType::Null, true),
        FunctionValueType::new(DataType::FixedSizeBinary(16), false),
    ] {
        assert!(!native_bitnot_source_supported(&ty));
        assert!(
            PreparedNativeBitNotRecipe::try_new(&ty, &ty, &Compile::default())
                .unwrap_err()
                .to_string()
                .contains("native BitwiseNot unsupported source shape")
        );
    }
    // No successful NULL/non-null declaration widening is introduced.
    let source = FunctionValueType::new(DataType::Int8, true);
    assert!(
        PreparedNativeBitNotRecipe::try_new(
            &source,
            &FunctionValueType::new(DataType::Int8, false),
            &Compile::default()
        )
        .is_err()
    );
    let empty_foreign: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    assert!(matches!(
        prepared(FunctionValueType::new(DataType::Int64, false)).evaluate_selected(
            EvaluatedArgument::Column(&empty_foreign),
            novarocks_functions::Selection::all(0),
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let foreign: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
    assert!(
        prepared(FunctionValueType::new(DataType::Int64, false))
            .evaluate_selected(
                EvaluatedArgument::Column(&foreign),
                novarocks_functions::Selection::all(1),
                &Control::default()
            )
            .is_err()
    );
}
