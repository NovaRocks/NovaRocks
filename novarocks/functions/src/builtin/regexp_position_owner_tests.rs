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
use arrow_schema::DataType;
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters,
};
fn owner(name: &str) -> RegexpPositionOwner {
    let (_, signatures) = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(n, _)| n == name)
        .unwrap();
    let (declaration, resolver) =
        super::super::catalogue::scalar_definition_parts(name, &signatures, FunctionKind::Scalar)
            .unwrap();
    RegexpPositionOwner::new(declaration, resolver).unwrap()
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
    owner: &'a RegexpPositionOwner,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    parameters: &'a SemanticParameters,
    uses: &'a [Option<ExpressionUseId>],
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: context(),
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
fn regexp_position_owner_all_three_fixed_profiles_preserve_exact_contract_and_effects() {
    let owner = owner("regexp_position");
    assert_eq!(owner.declaration.overloads().len(), 3);
    for arity in 2..=4 {
        let sources = (0..arity)
            .map(|i| {
                FunctionValueType::from_legacy_declared_projection(
                    if i < 2 {
                        DataType::Utf8
                    } else {
                        DataType::Int64
                    },
                    true,
                )
            })
            .collect::<Vec<_>>();
        let prepared = prepared_for_test("regexp_position", &sources).unwrap();
        assert_eq!(
            prepared.contract().result_type().data_type().clone(),
            DataType::Int32
        );
        assert_eq!(prepared.contract().selected().argument_types.len(), arity);
        assert_eq!(
            prepared.contract().effects().own_row_error,
            FunctionIntrinsicRowError::MayRaise
        );
        assert_eq!(
            prepared.contract().effects().argument_control,
            ArgumentControl::Eager
        );
        assert_eq!(
            prepared.contract().effects().null_behavior,
            FunctionNullBehavior::Strict
        );
        assert!(prepared.contract().effects().environment.is_empty());
        for (i, ty) in prepared
            .contract()
            .selected()
            .argument_types
            .iter()
            .enumerate()
        {
            let crate::FunctionArgumentType::Value(ty) = ty else {
                panic!("unexpected lambda")
            };
            assert_eq!(
                ty.data_type().clone(),
                if i < 2 {
                    DataType::Utf8
                } else {
                    DataType::Int64
                }
            );
        }
    }
}
#[test]
fn regexp_position_owner_rejects_arity_and_effect_environment_drift() {
    let owner = owner("regexp_position");
    for arity in [0, 1, 5] {
        let sources = vec![FunctionValueType::new(DataType::Utf8, true); arity];
        assert!(prepared_for_test("regexp_position", &sources).is_err());
    }
    let arguments = [
        argument(FunctionValueType::new(DataType::Utf8, true)),
        argument(FunctionValueType::new(DataType::Utf8, true)),
    ];
    let selected = owner
        .resolve(request(&arguments), crate::binding_test_control())
        .unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [
        Some(ExpressionUseId::new(42)),
        Some(ExpressionUseId::new(43)),
    ];
    let mut exact = input(&owner, &selected, &arguments, &parameters, &uses);
    let environment = [novarocks_type_contract::SemanticParameterRef {
        id: novarocks_type_contract::SemanticParameterId::new(1),
        expected_key: novarocks_type_contract::SemanticParameterKey::TimeZone,
    }];
    exact.environment = &environment;
    assert!(
        owner
            .validate_and_refine(exact, crate::binding_test_control())
            .is_err()
    );
}
