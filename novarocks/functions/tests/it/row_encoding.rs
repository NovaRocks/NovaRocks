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

use std::{sync::Arc, time::Duration};

use arrow_array::{
    ArrayRef, BinaryArray, Decimal128Array, Float32Array, Float64Array, Int16Array, ListArray,
    NullArray, StringArray, TimestampMicrosecondArray, types::Int32Type,
};
use arrow_schema::DataType;
use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionIntrinsicRowError, FunctionKind, KernelEvaluationControl, KernelFailure,
    MAX_UNOBSERVED_KERNEL_WORK, PreparedPureKernel, PureCallPreparation, ScalarEvaluationInstance,
    ScopedExpressionEffects, Selection, builtin::catalogue::builtin_engine_function_catalog,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionValueType,
    PureCompileControl, SemanticParameters,
};

struct Control;

impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= MAX_UNOBSERVED_KERNEL_WORK);
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("row encoders never wait")
    }
}

fn argument_types(values: &[EvaluatedArgument<'_>]) -> Vec<FunctionArgument> {
    values
        .iter()
        .map(|value| FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(
                value.array().data_type().clone(),
                true,
            ),
            constant: None,
        })
        .collect()
}

fn prepare(name: &str, values: &[EvaluatedArgument<'_>]) -> ScalarEvaluationInstance {
    let catalog = builtin_engine_function_catalog();
    let arguments = argument_types(values);
    let uses = (1..=arguments.len())
        .map(|index| Some(ExpressionUseId::new(u32::try_from(index).unwrap())))
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
        .unwrap();
    assert_eq!(
        bound.selected.argument_types.as_ref(),
        arguments
            .iter()
            .map(FunctionArgument::argument_type)
            .collect::<Vec<_>>()
    );
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(100),
        region: EvaluationRegionId::new(1),
        demand: EvaluationDemand::Value,
    };
    let prepared = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                context: context.into(),
                argument_uses: CallArgumentUses::SelectedChannels(&uses),
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &SemanticParameters::default(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Unconditional,
            },
            Arc::clone(&selected),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &Control,
        )
        .unwrap();
    let PreparedPureKernel::Scalar(prepared) = prepared.into_prepared() else {
        panic!("row encoding must prepare an exact scalar kernel");
    };
    assert_eq!(
        prepared.contract().result_type().data_type(),
        &DataType::Binary
    );
    assert!(!prepared.contract().result_type().nullable());
    assert_eq!(
        prepared.contract().effects().own_row_error,
        FunctionIntrinsicRowError::NoRowError
    );
    ScalarEvaluationInstance::instantiate(prepared).unwrap()
}

fn encode(name: &str, values: &[EvaluatedArgument<'_>], selection: Selection<'_>) -> Vec<String> {
    let mut instance = prepare(name, values);
    let output = instance.evaluate(selection, values, &Control).unwrap();
    assert!(output.errors().is_empty());
    let encoded = output
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    encoded
        .iter()
        .map(|value| hex::encode(value.unwrap()))
        .collect()
}

#[test]
fn fingerprint_aliases_preserve_marker_bytes_and_explicit_nulls() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![Some("abc"), None]));
    let values = [EvaluatedArgument::Column(&text)];
    for name in ["encode_row_id", "encode_fingerprint_sha256"] {
        // SHA-256 of the goldens' original marker/byte sequences 08616263 and 00.
        assert_eq!(
            encode(name, &values, Selection::all(2)),
            [
                "5a4eb9341eb46a7aa15be680888769ee984b1e23e4d1e798ae6c761deefce69e",
                "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
            ]
        );
        let null: ArrayRef = Arc::new(NullArray::new(1));
        assert_eq!(
            encode(name, &[EvaluatedArgument::Scalar(&null)], Selection::all(1)),
            ["6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d"]
        );
    }
}

#[test]
fn fingerprint_preserves_decimal_widths_little_endian_numbers_and_fixed_largeint_bytes() {
    let input: Vec<ArrayRef> = vec![
        Arc::new(Int16Array::from(vec![-2])),
        Arc::new(
            Decimal128Array::from(vec![123])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![-45])
                .with_precision_and_scale(18, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![1])
                .with_precision_and_scale(19, 2)
                .unwrap(),
        ),
        Arc::new(Float32Array::from(vec![-0.0])),
        Arc::new(BinaryArray::from(vec![b"a\0".as_slice()])),
        Arc::new(TimestampMicrosecondArray::from(vec![-1])),
        novarocks_functions::largeint::array_from_i128(&[Some(-3)]).unwrap(),
    ];
    let values = input
        .iter()
        .map(EvaluatedArgument::Scalar)
        .collect::<Vec<_>>();
    // SHA-256 of 02feff 037b000000 04d3ffffffffffffff
    // 0901000000000000000000000000000000 0600000080 086100
    // 0bffffffffffffffff 05fffffffffffffffffffffffffffffffd.
    assert_eq!(
        encode("encode_row_id", &values, Selection::all(1)),
        ["68e47252e0733c9fc2cc82234a6177b6a02e08ae1873c37924e3d630a2482ec3"]
    );
}

#[test]
fn fingerprint_ignores_null_and_nonnull_container_values_but_sort_key_rejects_them() {
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), None]),
        None,
    ]));
    let values = [EvaluatedArgument::Column(&lists)];
    for name in ["encode_row_id", "encode_fingerprint_sha256"] {
        assert_eq!(
            encode(name, &values, Selection::all(2)),
            [
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ]
        );
    }
    let arguments = argument_types(&values);
    assert!(
        builtin_engine_function_catalog()
            .resolve_bound_user(
                "encode_sort_key",
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &Control,
            )
            .is_err()
    );
}

#[test]
fn sort_key_preserves_escape_terminator_separator_and_last_field_bytes_in_sparse_rows() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a\0b"),
        Some("outside selection"),
        None,
    ]));
    let ints: ArrayRef = Arc::new(Int16Array::from(vec![-2, 99, 0]));
    let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![[0, 255].as_slice()]));
    let values = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&ints),
        EvaluatedArgument::Scalar(&bytes),
    ];
    assert_eq!(
        encode(
            "encode_sort_key",
            &values,
            Selection::try_sparse(3, &[0, 2]).unwrap()
        ),
        ["0161000162000000017ffe000100ff", "0000018000000100ff"]
    );
}

#[test]
fn sort_key_bytes_order_floats_and_keep_null_before_nonnull() {
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![
        None,
        Some(f64::NEG_INFINITY),
        Some(-2.0),
        Some(-0.0),
        Some(0.0),
        Some(2.0),
        Some(f64::INFINITY),
    ]));
    let values = [EvaluatedArgument::Column(&floats)];
    let encoded = encode("encode_sort_key", &values, Selection::all(7));
    assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(encoded[0], "00");
    assert_eq!(encoded[3], "017fffffffffffffff");
    assert_eq!(encoded[4], "018000000000000000");
}

#[test]
fn fingerprint_checks_total_binary_extent_before_reading_rows() {
    let text: ArrayRef = Arc::new(StringArray::from(vec!["one broadcast value"]));
    let arguments = [EvaluatedArgument::Scalar(&text)];
    let mut instance = prepare("encode_row_id", &arguments);
    let error = instance
        .evaluate(
            Selection::all(i32::MAX as usize / 32 + 1),
            &arguments,
            &Control,
        )
        .unwrap_err();
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Representation);
    let extent = fact.representation().unwrap();
    assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
    assert_eq!(extent.required, Some(i32::MAX as u64 + 1));
    assert_eq!(extent.extent_limit, Some(i32::MAX as u64));
}
