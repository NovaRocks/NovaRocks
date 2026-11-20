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
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array,
};
use arrow_buffer::i256;
use arrow_schema::DataType;
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, DataFailureReason, EvaluatedArgument, FunctionArgument,
    FunctionArgumentType, FunctionBindingError, FunctionBindingRequest, FunctionIntrinsicRowError,
    FunctionKind, FunctionResultType, KernelEvaluationControl, KernelFailure, PreparedPureKernel,
    PureCallPreparation, PureKernelAbi, ScalarEvaluationInstance, ScopedExpressionEffects,
    SelectedValues, Selection, largeint, testkit,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionNullBehavior,
    FunctionValueType, OperatorFunction, PureCompileControl, SemanticParameters, ValueLogicalType,
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("negation must not wait")
    }
}

fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    }
}

fn value_argument(value_type: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type,
        constant: None,
    }
}

fn evaluate<'a>(
    arguments: &'a [EvaluatedArgument<'a>],
    selection: Selection<'a>,
    value_type: FunctionValueType,
    expected_row_error: FunctionIntrinsicRowError,
) -> SelectedValues<'a> {
    let catalog = testkit::shared_installed_builtin_owner_catalogue();
    let operator = OperatorFunction::Negation.function_id();
    let value_arguments = [value_argument(value_type.clone())];
    let bound = catalog
        .metadata()
        .resolve_bound_trusted_by_id(
            &operator,
            FunctionKind::Scalar,
            request(&value_arguments),
            &Control,
        )
        .unwrap();
    assert_eq!(bound.function_id, operator);
    assert_eq!(bound.semantics.intrinsic_row_error, expected_row_error);
    assert_eq!(
        bound.selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(value_type.clone())]
    );
    assert_eq!(
        bound.selected.result_type,
        FunctionResultType::Scalar(value_type)
    );
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(1),
        region: EvaluationRegionId::new(1),
        demand: EvaluationDemand::Value,
    };
    let uses = [Some(ExpressionUseId::new(2))];
    let parameters = SemanticParameters::default();
    let call = CallEffectInput {
        context: context.into(),
        argument_uses: CallArgumentUses::SelectedChannels(&uses),
        function_id: &operator,
        kind: FunctionKind::Scalar,
        selected: &selected,
        request: request(&value_arguments),
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    };
    let prepared = catalog
        .prepare_fresh(
            call,
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &Control,
        )
        .unwrap();
    assert_eq!(prepared.implementation().abi, PureKernelAbi::ScalarV1);
    let PreparedPureKernel::Scalar(kernel) = prepared.into_prepared() else {
        panic!("negation must install an evaluated scalar kernel");
    };
    assert_eq!(
        kernel.contract().effects().own_row_error,
        expected_row_error
    );
    assert_eq!(
        kernel.contract().effects().null_behavior,
        FunctionNullBehavior::Strict
    );
    ScalarEvaluationInstance::instantiate(kernel)
        .unwrap()
        .evaluate(selection, arguments, &Control)
        .unwrap()
}

fn check_overflow(output: &SelectedValues<'_>, name: &str, value: impl std::fmt::Display) {
    let [error] = output.errors() else {
        panic!("each selected signed minimum must report exactly one row error");
    };
    assert_eq!(error.reason(), DataFailureReason::NumericOutOfRange);
    assert_eq!(error.selected_ordinal(), 2);
    assert_eq!(output.selection().row(error.selected_ordinal()), Some(3));
    let expected = format!("{name} negation overflow: {value}");
    assert_eq!(error.message(), expected);
    assert_eq!(error.public_diagnostic(), Some(expected.as_str()));
}

#[test]
fn negation_checks_all_signed_minima_at_the_actual_selected_ordinal() {
    let selection = Selection::try_sparse(5, &[1, 2, 3, 4]).unwrap();
    macro_rules! check {
        ($array:ty, $native:ty, $carrier:expr, $name:literal) => {{
            let input: ArrayRef = Arc::new(<$array>::from(vec![
                Some(<$native>::MIN),
                Some(7),
                None,
                Some(<$native>::MIN),
                Some(-5),
            ]));
            let arguments = [EvaluatedArgument::Column(&input)];
            let output = evaluate(
                &arguments,
                selection,
                FunctionValueType::new($carrier, true),
                FunctionIntrinsicRowError::MayRaise,
            );
            assert_eq!(
                output.values().as_any().downcast_ref::<$array>().unwrap(),
                &<$array>::from(vec![Some(-7), None, None, Some(5)])
            );
            check_overflow(&output, $name, <$native>::MIN);
        }};
    }

    check!(Int8Array, i8, DataType::Int8, "tinyint");
    check!(Int16Array, i16, DataType::Int16, "smallint");
    check!(Int32Array, i32, DataType::Int32, "integer");
    check!(Int64Array, i64, DataType::Int64, "bigint");

    let input =
        largeint::array_from_i128(&[Some(i128::MIN), Some(7), None, Some(i128::MIN), Some(-5)])
            .unwrap();
    let arguments = [EvaluatedArgument::Column(&input)];
    let output = evaluate(
        &arguments,
        selection,
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
        FunctionIntrinsicRowError::MayRaise,
    );
    let expected = largeint::array_from_i128(&[Some(-7), None, None, Some(5)]).unwrap();
    assert_eq!(output.values().to_data(), expected.to_data());
    check_overflow(&output, "largeint", i128::MIN);
}

#[test]
fn negation_preserves_ieee_values_and_sql_nulls_in_both_float_widths() {
    macro_rules! check {
        ($array:ty, $native:ty, $carrier:expr) => {{
            let input: ArrayRef = Arc::new(<$array>::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(<$native>::NAN),
                Some(-13.5),
                None,
            ]));
            let arguments = [EvaluatedArgument::Column(&input)];
            let output = evaluate(
                &arguments,
                Selection::all(input.len()),
                FunctionValueType::new($carrier, true),
                FunctionIntrinsicRowError::NoRowError,
            );
            let values = output.values().as_any().downcast_ref::<$array>().unwrap();
            let negative_zero: $native = -0.0;
            let positive_zero: $native = 0.0;
            assert_eq!(values.value(0).to_bits(), negative_zero.to_bits());
            assert_eq!(values.value(1).to_bits(), positive_zero.to_bits());
            assert!(values.value(2).is_nan());
            assert_eq!(values.value(3), 13.5);
            assert!(values.is_null(4));
            assert!(output.errors().is_empty());
        }};
    }
    check!(Float32Array, f32, DataType::Float32);
    check!(Float64Array, f64, DataType::Float64);
}

#[test]
fn negation_preserves_decimal_precision_scale_nullability_and_carrier() {
    let positive: i256 = "128000000000000000000000000000000000005".parse().unwrap();
    let negative: i256 = "-128000000000000000000000000000000000005".parse().unwrap();
    for nullable in [true, false] {
        let middle128 = (!nullable).then_some(0);
        let middle256 = (!nullable).then_some(i256::ZERO);
        let input128: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(9_876), middle128, Some(-12_345)])
                .with_precision_and_scale(18, 4)
                .unwrap(),
        );
        let input256: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(positive), middle256, Some(negative)])
                .with_precision_and_scale(54, 7)
                .unwrap(),
        );
        let expected128: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(-9_876), middle128, Some(12_345)])
                .with_precision_and_scale(18, 4)
                .unwrap(),
        );
        let expected256: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(negative), middle256, Some(positive)])
                .with_precision_and_scale(54, 7)
                .unwrap(),
        );

        for (input, expected) in [(input128, expected128), (input256, expected256)] {
            let arguments = [EvaluatedArgument::Column(&input)];
            let output = evaluate(
                &arguments,
                Selection::all(input.len()),
                FunctionValueType::new(expected.data_type().clone(), nullable),
                FunctionIntrinsicRowError::NoRowError,
            );
            assert_eq!(output.values().data_type(), expected.data_type());
            assert_eq!(output.values().to_data(), expected.to_data());
            assert!(output.errors().is_empty());
        }
    }
}

#[test]
fn negation_rejects_hidden_names_unsupported_types_and_exact_overload_changes() {
    let catalog = testkit::shared_installed_builtin_owner_catalogue();
    let metadata = catalog.metadata();
    let operator = OperatorFunction::Negation;
    let id = operator.function_id();
    let small = [value_argument(FunctionValueType::new(
        DataType::Int16,
        false,
    ))];
    assert!(matches!(
        metadata.resolve_bound_user(
            operator.catalog_name(),
            FunctionKind::Scalar,
            request(&small),
            &Control
        ),
        Err(FunctionBindingError::HiddenFunction)
    ));
    let initial = metadata
        .resolve_bound_trusted_by_id(&id, FunctionKind::Scalar, request(&small), &Control)
        .unwrap();
    let late = [value_argument(FunctionValueType::new(
        DataType::Int16,
        true,
    ))];
    let final_binding = metadata
        .resolve_bound_trusted_at_overload(
            &id,
            FunctionKind::Scalar,
            &initial.selected.overload,
            request(&late),
            &Control,
        )
        .unwrap();
    assert_eq!(final_binding.selected.overload, initial.selected.overload);
    assert_eq!(
        final_binding.selected.result_type,
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Int16, true))
    );
    let wide = [value_argument(FunctionValueType::new(
        DataType::Int64,
        false,
    ))];
    assert!(matches!(
        metadata.resolve_bound_trusted_at_overload(
            &id,
            FunctionKind::Scalar,
            &initial.selected.overload,
            request(&wide),
            &Control
        ),
        Err(FunctionBindingError::NoMatchingOverload)
    ));

    for carrier in [
        DataType::Null,
        DataType::Boolean,
        DataType::Utf8,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Decimal32(8, 4),
        DataType::Decimal64(15, 3),
        DataType::FixedSizeBinary(16),
    ] {
        let arguments = [value_argument(FunctionValueType::new(
            carrier.clone(),
            true,
        ))];
        assert!(
            matches!(
                metadata.resolve_bound_trusted_by_id(
                    &id,
                    FunctionKind::Scalar,
                    request(&arguments),
                    &Control
                ),
                Err(FunctionBindingError::NoMatchingOverload)
            ),
            "unexpected selection for {carrier:?}"
        );
    }
}
