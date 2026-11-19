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
    FunctionArgument, FunctionSpecializationFailure, FunctionValueType, ScopedExpressionEffects,
    specialize_scalar,
};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters, ValueLogicalType,
};
fn owner(name: &str) -> StringNullOrEmptyOwner {
    let (declaration, resolver) = super::super::catalogue::dynamic_definition_parts(name).unwrap();
    StringNullOrEmptyOwner::new(name, declaration, resolver).unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    }
}

fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        expected_result_type: None,
        arguments,
        logical_argument_count: arguments.len(),
    }
}

fn input<'a>(
    owner: &'a StringNullOrEmptyOwner,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    parameters: &'a SemanticParameters,
    uses: &'a [Option<ExpressionUseId>],
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: (context()).into(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected,
        request: request(arguments),
        environment: &[],
        parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    }
}

fn argument(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}

pub(crate) fn prepared_for_test(
    name: &str,
    sources: &[FunctionValueType],
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    if !operation(name) {
        return Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction,
        ));
    }
    let owner = owner(name);
    let arguments = sources.iter().cloned().map(argument).collect::<Vec<_>>();
    let selected = Arc::new(owner.resolve(request(&arguments), crate::binding_test_control())?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = sources
        .iter()
        .enumerate()
        .map(|(i, _)| Some(ExpressionUseId::new(i as u32 + 42)))
        .collect::<Vec<_>>();
    specialize_scalar(
        &owner,
        input(&owner, &selected, &arguments, &parameters, &uses),
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        crate::binding_test_control(),
    )
    .map(|call| call.into_prepared())
}

#[test]
fn null_or_empty_owner_stable_exact_profiles_and_called_on_null_effects() {
    let mut types = vec![
        FunctionValueType::new(DataType::Null, true),
        FunctionValueType::new(DataType::Utf8, false),
        FunctionValueType::from_legacy_declared_projection(
            DataType::List(Arc::new(Field::new(
                "payload",
                DataType::Struct(vec![Arc::new(Field::new("v", DataType::Int64, true))].into()),
                true,
            ))),
            true,
        ),
    ];
    types.push(
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    );
    for ty in types {
        let prepared = prepared_for_test("null_or_empty", std::slice::from_ref(&ty)).unwrap();
        assert_eq!(
            prepared.contract().selected().argument_types.as_ref(),
            &[crate::FunctionArgumentType::Value(ty)]
        );
        assert_eq!(
            prepared.contract().result_type().data_type().clone(),
            DataType::Boolean
        );
        assert_eq!(
            prepared.contract().result_type().logical_type(),
            ValueLogicalType::Physical
        );
        assert_eq!(
            prepared.contract().effects().null_behavior,
            FunctionNullBehavior::CalledOnNull
        );
        assert_eq!(
            prepared.contract().effects().argument_control,
            ArgumentControl::Eager
        );
        assert_eq!(
            prepared.contract().effects().own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
    }
}
#[test]
fn null_or_empty_owner_does_not_infer_unsupported_whole_array_null_fact() {
    assert!(prepared_for_test("null_or_empty", &[]).is_err());
    for ty in [
        DataType::Int64,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8, true)), 2),
    ] {
        assert!(
            prepared_for_test(
                "null_or_empty",
                &[FunctionValueType::from_legacy_declared_projection(ty, true)]
            )
            .is_err()
        );
    }
    assert!(
        prepared_for_test(
            "null_or_empty",
            &[
                FunctionValueType::new(DataType::Utf8, true),
                FunctionValueType::new(DataType::Utf8, true)
            ]
        )
        .is_err()
    );
}
