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

mod largeint_text;

mod date_float;

mod float_date;

mod decimal_float;

mod decimal_float32;

mod float_decimal128;

mod decimal_largeint;
mod float_bool_largeint;

use std::{sync::Arc, time::Duration};

use arrow_array::ArrayRef;
use arrow_schema::DataType;
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionIntrinsicRowError, FunctionKind, KernelEvaluationControl, KernelFailure,
    PreparedPureKernel, PureCallPreparation, RowDataError, ScalarEvaluationInstance,
    ScopedExpressionEffects, Selection,
    builtin::{
        catalogue::builtin_engine_function_catalog, value_conversion::VALUE_CONVERSION_NAME,
    },
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionValueType,
    PureCompileControl, SemanticParameters, ValueLogicalType,
};

struct LargeIntConversionControl;
impl PureCompileControl for LargeIntConversionControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for LargeIntConversionControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric value conversion never waits")
    }
}

fn evaluate_to_largeint(
    values: &ArrayRef,
    selection: Selection<'_>,
    policy: DecimalOverflowPolicy,
) -> (
    Vec<Option<i128>>,
    Box<[RowDataError]>,
    FunctionIntrinsicRowError,
) {
    let catalog = builtin_engine_function_catalog();
    let arguments = [FunctionArgument::Value {
        value_type: FunctionValueType::from_legacy_declared_projection(
            values.data_type().clone(),
            true,
        ),
        constant: None,
    }];
    let target = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: Some(&target),
    };
    let control = LargeIntConversionControl;
    let bound = catalog
        .resolve_bound_trusted(
            VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            request,
            &control,
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(1),
        region: EvaluationRegionId::new(2),
        demand: EvaluationDemand::Value,
    };
    let input = CallEffectInput {
        context: context.into(),
        argument_uses: CallArgumentUses::SelectedChannels(&[Some(ExpressionUseId::new(3))]),
        function_id: &bound.function_id,
        kind: FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &SemanticParameters::default(),
        decimal_overflow_policy: policy,
        proof_scope: CallProofScope::Region(context.region),
    };
    let prepared = catalog
        .prepare_fresh_selected(
            input,
            Arc::clone(&selected),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &control,
        )
        .unwrap();
    let PreparedPureKernel::Scalar(prepared) = prepared.into_prepared() else {
        panic!("numeric value conversion must prepare a scalar kernel");
    };
    assert_eq!(prepared.contract().result_type(), &target);
    let effects = prepared.contract().effects().own_row_error;
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let args = [EvaluatedArgument::Column(values)];
    let (_, values, errors) = instance
        .evaluate(selection, &args, &control)
        .unwrap()
        .into_parts();
    let array =
        novarocks_functions::largeint::as_fixed_size_binary_array(&values, "LARGEINT").unwrap();
    let values = array
        .iter()
        .map(|value| value.map(|bytes| i128::from_be_bytes(bytes.try_into().unwrap())))
        .collect();
    (values, errors, effects)
}
