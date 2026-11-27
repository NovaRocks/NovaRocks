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

use std::{collections::BTreeSet, fmt};

use novarocks_type_contract::{OperatorFunction, PureCompileControl};

use crate::{
    FunctionId, FunctionKind, FunctionOverloadId, FunctionSpecializationFailure,
    FunctionVisibility, PureEngineFunctionCatalog, PureKernelAbi,
};

#[derive(Debug)]
pub enum OperatorFunctionCatalogError {
    DuplicateIdentity(FunctionId),
    MissingDefinition(FunctionId),
    InvalidDefinition(FunctionId),
    InvalidAbi(FunctionOverloadId),
    OverloadLookup(FunctionSpecializationFailure),
}

impl fmt::Display for OperatorFunctionCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateIdentity(id) => {
                write!(
                    formatter,
                    "operator function identity `{}` is not unique",
                    id.as_str()
                )
            }
            Self::MissingDefinition(id) => {
                write!(
                    formatter,
                    "operator function `{}` is missing from the pure catalog",
                    id.as_str()
                )
            }
            Self::InvalidDefinition(id) => {
                write!(
                    formatter,
                    "operator function `{}` must have its exact hidden scalar definition",
                    id.as_str()
                )
            }
            Self::InvalidAbi(overload) => {
                write!(
                    formatter,
                    "operator overload `{}` has an unexpected installed ABI",
                    overload.as_str()
                )
            }
            Self::OverloadLookup(cause) => {
                write!(formatter, "operator overload lookup failed: {cause}")
            }
        }
    }
}

impl std::error::Error for OperatorFunctionCatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OverloadLookup(cause) => Some(cause),
            _ => None,
        }
    }
}

/// Require the complete built-in operator set at the composition roots.
pub fn validate_installed_operator_functions(
    catalog: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<(), OperatorFunctionCatalogError> {
    let metadata = catalog.metadata();
    let mut seen = BTreeSet::new();
    for operator in OperatorFunction::ALL {
        let id = operator.function_id();
        if !seen.insert(id.clone()) {
            return Err(OperatorFunctionCatalogError::DuplicateIdentity(id));
        }
        let definition = metadata
            .definition_by_id(&id)
            .ok_or_else(|| OperatorFunctionCatalogError::MissingDefinition(id.clone()))?;
        if definition.kind() != FunctionKind::Scalar
            || definition.visibility() != FunctionVisibility::Hidden
            || definition.canonical_name() != operator.catalog_name()
        {
            return Err(OperatorFunctionCatalogError::InvalidDefinition(id));
        }
        let declaration = definition
            .binding_declaration()
            .ok_or_else(|| OperatorFunctionCatalogError::InvalidDefinition(id.clone()))?;
        for overload in declaration.overloads() {
            let actual = metadata
                .pure_overload_declaration_observed(
                    &id,
                    FunctionKind::Scalar,
                    &overload.identity,
                    control,
                )
                .map_err(OperatorFunctionCatalogError::OverloadLookup)?;
            let expected = match operator {
                OperatorFunction::InList => PureKernelAbi::ControlIntrinsicV1,
                _ => PureKernelAbi::ScalarV1,
            };
            if actual.implementation().abi != expected {
                return Err(OperatorFunctionCatalogError::InvalidAbi(
                    overload.identity.clone(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EngineFunctionCatalogBuilder, InstalledPureKernel};

    #[test]
    fn candidate_group_prepares_above_the_ordinary_argument_limit() {
        use crate::{
            CallArgumentUses, CallEffectInput, FunctionArgument, FunctionArgumentType,
            FunctionBindingRequest, FunctionCandidate, FunctionIntrinsicRowError,
            FunctionValueType, MAX_CALL_EFFECT_ARGUMENTS, PreparedPureKernel, PureCallPreparation,
            ScopedExpressionEffects,
        };
        use arrow_schema::DataType;
        use novarocks_type_contract::{
            CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
            ExpressionEffectContext, ExpressionUseId, SemanticParameters,
        };
        use std::sync::Arc;

        let catalog = crate::builtin::catalogue::builtin_engine_function_catalog();
        let function = OperatorFunction::InList.function_id();
        let control = crate::binding_test_control();
        let probe_type = FunctionValueType::new(DataType::Int64, false);
        let element_type = FunctionValueType::new(DataType::Int64, true);
        let member_count = MAX_CALL_EFFECT_ARGUMENTS + 1;
        let mut members = vec![
            FunctionCandidate {
                value_type: probe_type.clone(),
                constant: None,
            };
            member_count
        ];
        members[0].value_type = element_type.clone();
        let arguments = [
            FunctionArgument::Value {
                value_type: probe_type,
                constant: None,
            },
            FunctionArgument::CandidateGroup {
                element_type,
                members: members.into_boxed_slice(),
            },
        ];
        let request = FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: 2,
            expected_result_type: None,
        };
        let binding = catalog
            .resolve_bound_trusted_by_id(&function, FunctionKind::Scalar, request, control)
            .unwrap();
        assert_eq!(binding.logical_argument_count, 2);
        assert!(matches!(
            binding.selected.argument_types.as_ref(),
            [
                FunctionArgumentType::Value(_),
                FunctionArgumentType::CandidateGroup { .. }
            ]
        ));

        let selected = Arc::new(binding.selected);
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(0),
            region: EvaluationRegionId::new(0),
            demand: EvaluationDemand::Value,
        };
        let parameters = SemanticParameters::default();
        let uses: Vec<_> = (2..2 + member_count as u32)
            .map(ExpressionUseId::new)
            .collect();
        let input = CallEffectInput {
            context: context.into(),
            argument_uses: CallArgumentUses::CandidateGroup {
                probe: ExpressionUseId::new(1),
                candidates: &uses,
            },
            function_id: &function,
            kind: FunctionKind::Scalar,
            selected: &selected,
            request,
            environment: &[],
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            proof_scope: CallProofScope::Unconditional,
        };
        let options = || PureCallPreparation::ControlIntrinsic {
            arguments: ScopedExpressionEffects::pure_value(context),
        };
        let prepared = catalog
            .prepare_fresh_selected(input, Arc::clone(&selected), options(), control)
            .unwrap();
        let PreparedPureKernel::Membership(membership) = prepared.prepared() else {
            panic!("expected installed membership");
        };
        assert_eq!(membership.call().logical_argument_count(), 2);
        assert_eq!(
            membership.call().effects().own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert!(
            membership
                .comparison()
                .streaming_equality()
                .unwrap()
                .right_type()
                .nullable()
        );
        assert_eq!(membership.call().selected().argument_types.len(), 2);

        let wrong_use = CallEffectInput {
            argument_uses: CallArgumentUses::CandidateGroup {
                probe: ExpressionUseId::new(1),
                candidates: &uses[1..],
            },
            ..input
        };
        assert!(
            catalog
                .prepare_fresh_selected(wrong_use, Arc::clone(&selected), options(), control,)
                .is_err()
        );
    }

    #[test]
    fn visible_operator_is_not_accepted_as_an_installed_operator() {
        let builtin = crate::builtin::catalogue::builtin_engine_function_catalog();
        let id = OperatorFunction::Negation.function_id();
        let control = crate::binding_test_control();
        let mut definition = builtin.definition_by_id(&id).unwrap().clone();
        let installed: Vec<_> = definition
            .binding_declaration()
            .unwrap()
            .overloads()
            .iter()
            .map(|overload| {
                let actual = builtin
                    .pure_overload_declaration_observed(
                        &id,
                        FunctionKind::Scalar,
                        &overload.identity,
                        control,
                    )
                    .unwrap();
                InstalledPureKernel {
                    function: id.clone(),
                    kind: FunctionKind::Scalar,
                    implementation: actual.implementation().clone(),
                    aggregate_state_format: None,
                }
            })
            .collect();
        definition.visibility = FunctionVisibility::Public;
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder.register(definition).unwrap();
        let catalog = builder.seal_pure(installed).unwrap();
        assert!(matches!(
            validate_installed_operator_functions(&catalog, control),
            Err(OperatorFunctionCatalogError::InvalidDefinition(actual)) if actual == id
        ));
    }
}
