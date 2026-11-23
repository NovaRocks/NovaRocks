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

use novarocks_functions::{
    CallArgumentUses, CallEffectInput, EngineFunctionCatalog, FunctionArgument,
    FunctionBindingRequest, FunctionKind, KernelEvaluationControl, KernelFailure,
    PreparedPureKernel, PureCallPreparation, ResolvedFunctionBinding, ScalarEvaluationInstance,
    ScopedExpressionEffects, builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameters,
};

pub(super) struct Control;

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
        panic!("selected row kernels must not wait");
    }
}

pub(super) fn prepare(name: &str, arguments: &[FunctionArgument]) -> ScalarEvaluationInstance {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments,
        logical_argument_count: arguments.len(),
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
        .unwrap();
    prepare_bound(&catalog, bound, request)
}

pub(super) fn prepare_bound(
    catalog: &EngineFunctionCatalog,
    bound: ResolvedFunctionBinding,
    request: FunctionBindingRequest<'_>,
) -> ScalarEvaluationInstance {
    let selected = Arc::new(bound.selected);
    let uses = (1..=request.arguments.len())
        .map(|ordinal| Some(ExpressionUseId::new(ordinal as u32)))
        .collect::<Vec<_>>();
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(100),
        region: EvaluationRegionId::new(1),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let specialized = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                context: context.into(),
                argument_uses: CallArgumentUses::SelectedChannels(&uses),
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
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
    let PreparedPureKernel::Scalar(prepared) = specialized.into_prepared() else {
        panic!("expected a scalar preparation");
    };
    ScalarEvaluationInstance::instantiate(prepared).unwrap()
}

#[test]
fn array_subscript_keeps_sparse_errors_and_original_element_bytes() {
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray, StringArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::Field;
    use novarocks_failure_contract::kernel_reason::INVALID_FUNCTION_ARGUMENT;
    use novarocks_functions::{
        DataFailureReason, EvaluatedArgument, FunctionIntrinsicRowError, Selection,
    };
    use novarocks_type_contract::{FunctionValueType, OperatorFunction};

    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some(r#""first""#),
        Some("unused"),
        Some("out of range"),
        None,
        Some("zero"),
        Some("negative"),
        Some("null index"),
        Some(r#""last""#),
    ]));
    let array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new((0..=8).collect::<Vec<_>>().into()),
        values,
        None,
    ));
    let index: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(0),
        Some(i64::MAX),
        Some(1),
        Some(0),
        Some(i64::MIN),
        None,
        Some(1),
    ]));
    let selection = Selection::try_sparse(8, &[0, 2, 3, 4, 5, 6, 7]).unwrap();
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let operator = OperatorFunction::ArraySubscript;
    let arguments = [array.data_type(), index.data_type()].map(|ty| FunctionArgument::Value {
        value_type: FunctionValueType::from_legacy_declared_projection(ty.clone(), true),
        constant: None,
    });
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: arguments.len(),
    };
    assert!(
        catalog
            .resolve_bound_user(
                operator.catalog_name(),
                FunctionKind::Scalar,
                request,
                &Control
            )
            .is_err()
    );
    let bound = catalog
        .resolve_bound_trusted_by_id(
            &operator.function_id(),
            FunctionKind::Scalar,
            request,
            &Control,
        )
        .unwrap();
    assert_eq!(
        bound.semantics.intrinsic_row_error,
        FunctionIntrinsicRowError::MayRaise
    );
    let args = [
        EvaluatedArgument::Column(&array),
        EvaluatedArgument::Column(&index),
    ];
    let result = prepare_bound(&catalog, bound, request)
        .evaluate(selection, &args, &Control)
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some(r#""first""#),
            None,
            None,
            None,
            None,
            None,
            Some(r#""last""#)
        ],
    );
    assert_eq!(
        result
            .errors()
            .iter()
            .map(|error| (error.selected_ordinal(), error.reason()))
            .collect::<Vec<_>>(),
        vec![
            (1, DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)),
            (3, DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)),
            (4, DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)),
        ],
    );
}
