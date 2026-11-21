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

use std::sync::Arc;

use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    OperatorFunction, PureCompileControl, ValueLogicalType, wider_comparison_value_type,
};

use super::binding_control;
use super::catalogue::request_types_with_constants;
use super::operator_arithmetic::{ArithmeticSignature, arithmetic_types};
use super::operator_kernel::BinaryOperatorKernel;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionArgument, FunctionArgumentType, FunctionBindingDeclaration,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionBindingResolverOwner, FunctionBindingSelection, FunctionCatalogError,
    FunctionDefinition, FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior,
    FunctionId, FunctionIntrinsicRowError, FunctionKind, FunctionOverloadDeclaration,
    FunctionOverloadId, FunctionResultType, FunctionValueType, FunctionVisibility,
    FunctionVolatility, KernelFailure, PreparedScalarKernel, PureFunctionMetadataOwner,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, PureScalarImplementation,
    ScalarCallContract,
};

pub(super) fn definition(
    operator: OperatorFunction,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let id = operator.function_id();
    let declaration_error =
        |error: FunctionBindingError| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin binary operator declaration",
            value: error.to_string().into(),
        };
    let overloads = if operator.arithmetic().is_some() {
        ArithmeticSignature::ALL
            .iter()
            .map(|signature| {
                overload(
                    operator,
                    signature.name(),
                    signature.pattern(),
                    signature.result_pattern(),
                    signature.row_error(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![overload(
            operator,
            "comparison",
            "(comparable T,comparable T)",
            "boolean",
            FunctionIntrinsicRowError::NoRowError,
        )?]
    };
    let declaration = FunctionBindingDeclaration::try_new(id, FunctionKind::Scalar, overloads)
        .map_err(declaration_error)?;
    let implementation = PureImplementationId::try_new(format!(
        "builtin.scalar/{}/selected-v1",
        operator.catalog_name(),
    ))?;
    let implementations = declaration
        .overloads()
        .iter()
        .map(|overload| PureImplementationDeclaration {
            overload: overload.identity.clone(),
            implementation: implementation.clone(),
            abi: PureKernelAbi::ScalarV1,
        })
        .collect();
    let owner = BinaryOperatorOwner {
        operator,
        declaration,
        implementations,
    };
    FunctionDefinition::try_new_pure_scalar(
        operator.catalog_name(),
        FunctionVisibility::Hidden,
        Arc::new(owner),
    )
    .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin binary operator owner",
        value: error.to_string().into(),
    })
}

fn overload(
    operator: OperatorFunction,
    name: &str,
    arguments: &str,
    result: &str,
    own_row_error: FunctionIntrinsicRowError,
) -> Result<FunctionOverloadDeclaration, FunctionCatalogError> {
    let identity = FunctionOverloadId::try_new(format!(
        "builtin.scalar/{}/{name}-v1",
        operator.catalog_name(),
    ))
    .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin binary operator overload",
        value: error.to_string().into(),
    })?;
    Ok(FunctionOverloadDeclaration::from_effects(
        identity,
        arguments,
        result,
        None,
        FunctionEffectDeclaration {
            value_stability: FunctionVolatility::Immutable,
            own_row_error,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: if operator.is_null_safe_comparison() {
                FunctionNullBehavior::CalledOnNull
            } else {
                FunctionNullBehavior::Strict
            },
            argument_control: ArgumentControl::Eager,
            instance_state: FunctionInstanceState::None,
            observable_effects: ObservableEffects::NONE,
            environment_dependencies: Box::new([]),
        },
    ))
}

pub(crate) struct BinaryOperatorOwner {
    operator: OperatorFunction,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}

struct ResolvedTypes {
    pattern: &'static str,
    arguments: [FunctionValueType; 2],
    result: FunctionValueType,
}

impl BinaryOperatorOwner {
    fn operands<'a>(
        &self,
        request: FunctionBindingRequest<'a>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<[&'a FunctionValueType; 2], FunctionBindingError> {
        request_types_with_constants(request, work)?;
        if let [
            FunctionArgument::Value {
                value_type: left, ..
            },
            FunctionArgument::Value {
                value_type: right, ..
            },
        ] = request.arguments
            && request.logical_argument_count == 2
        {
            Ok([left, right])
        } else {
            Err(FunctionBindingError::NoMatchingOverload)
        }
    }

    fn declared_overload(
        &self,
        pattern: &str,
    ) -> Result<&FunctionOverloadId, FunctionBindingError> {
        self.declaration
            .overloads()
            .iter()
            .find(|candidate| candidate.argument_pattern.as_ref() == pattern)
            .map(|candidate| &candidate.identity)
            .ok_or_else(|| {
                FunctionBindingError::InvalidBinding(
                    "Binary operator overload has no declaration".into(),
                )
            })
    }

    fn select(
        &self,
        request: FunctionBindingRequest<'_>,
        exact: Option<&FunctionOverloadId>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let inputs = self.operands(request, work)?;
        let nullable =
            |value: &FunctionValueType| value.nullable() || *value.data_type() == DataType::Null;
        let [left_null, right_null] = inputs.map(nullable);
        let types = if let Some(operation) = self.operator.arithmetic() {
            let types = arithmetic_types(operation, inputs)?;
            let [left, right] = types.arguments;
            ResolvedTypes {
                pattern: types.signature.pattern(),
                arguments: [
                    FunctionValueType::new(left, left_null),
                    FunctionValueType::new(right, right_null),
                ],
                result: FunctionValueType::new(types.result, left_null || right_null),
            }
        } else {
            let [left, right] = inputs;
            work.flush()?;
            let common = wider_comparison_value_type(left, right)
                .map_err(|_| FunctionBindingError::NoMatchingOverload)?;
            let args = [
                common.with_nullable(left_null)?,
                common.with_nullable(right_null)?,
            ];
            BinaryOperatorKernel::validate_comparison(
                self.operator,
                [&args[0], &args[1]],
                work.control(),
            )?;
            ResolvedTypes {
                pattern: "(comparable T,comparable T)",
                arguments: args,
                result: FunctionValueType::new(
                    DataType::Boolean,
                    !self.operator.is_null_safe_comparison() && (left_null || right_null),
                ),
            }
        };
        let overload = self.declared_overload(types.pattern)?;
        if let Some(expected) = exact {
            if expected != overload {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            for (expected, actual) in types.arguments.iter().zip(inputs) {
                if !binding_control::exact_type(expected, actual, work)? {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
            }
        }
        work.step()?;
        Ok(FunctionBindingSelection {
            overload: overload.clone(),
            argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                Box::new(types.arguments.map(FunctionArgumentType::Value)),
            ),
            result_type: FunctionResultType::Scalar(types.result),
            aggregate: None,
        })
    }

    fn effects(&self, input: CallEffectInput<'_>) -> Result<CallEffects, FunctionBindingError> {
        if input.function_id != self.declaration.function_id() || input.kind != FunctionKind::Scalar
        {
            return Err(FunctionBindingError::UnknownFunction);
        }
        if !input.environment.is_empty()
            || !matches!(input.proof_scope, CallProofScope::Unconditional)
                && input.proof_scope != input.context.proof_scope()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "Binary operator preparation requires its exact proof scope and no environment"
                    .into(),
            ));
        }
        let base = self
            .declaration
            .effect_declaration(&input.selected.overload)?;
        Ok(CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: input.proof_scope,
        })
    }

    fn refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
            other => FunctionEffectOwnerError::Owner(other),
        })?;
        let effects = self.effects(input)?;
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(effects)
    }

    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        let work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        if contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || novarocks_type_contract::CallEffectContext::from(contract.context()) != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects()
                != &self
                    .effects(input)
                    .map_err(|_| invalid("Invalid binary operator effects"))?
        {
            return Err(invalid(
                "Binary operator differs from its checked invocation",
            ));
        }
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("Binary operator has a stale selected binding"),
        })?;
        let prepared = BinaryOperatorKernel::prepare(self.operator, contract, control)?;
        work.finish().map_err(compile_failure)?;
        Ok(Arc::new(prepared))
    }
}

impl FunctionBindingResolver for BinaryOperatorOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| self.select(request, None, work))
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| self.select(request, Some(overload), work))
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            let inputs = self.operands(request, work)?;
            let [
                FunctionArgumentType::Value(left),
                FunctionArgumentType::Value(right),
            ] = &selected.argument_types[..]
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            for (expected, actual) in inputs.into_iter().zip([left, right]) {
                if !binding_control::exact_type(expected, actual, work)? {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
            }
            let (pattern, carrier, nullable) = if let Some(operation) = self.operator.arithmetic() {
                let types = arithmetic_types(operation, inputs)?;
                if inputs
                    .iter()
                    .zip(&types.arguments)
                    .any(|(input, target)| input.data_type() != target)
                {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
                (
                    types.signature.pattern(),
                    types.result,
                    left.nullable() || right.nullable(),
                )
            } else {
                work.flush()?;
                BinaryOperatorKernel::validate_comparison(self.operator, inputs, work.control())?;
                (
                    "(comparable T,comparable T)",
                    DataType::Boolean,
                    !self.operator.is_null_safe_comparison()
                        && (left.nullable()
                            || right.nullable()
                            || *left.data_type() == DataType::Null),
                )
            };
            work.step()?;
            if &selected.overload == self.declared_overload(pattern)?
                && selected.aggregate.is_none()
                && matches!(&selected.result_type, FunctionResultType::Scalar(result)
                    if result.logical_type() == ValueLogicalType::Physical
                        && result.data_type() == &carrier && result.nullable() == nullable)
            {
                Ok(())
            } else {
                Err(FunctionBindingError::InvalidBinding(
                    "Selected binary operator result differs from its declared overload".into(),
                ))
            }
        })
    }
}

impl PureFunctionMetadataOwner for BinaryOperatorOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> FunctionBindingResolverOwner {
        FunctionBindingResolverOwner::borrowed_binary_operator(self)
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for BinaryOperatorOwner {
    type Error = FunctionBindingError;

    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }

    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.refine(input, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }

    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, crate::CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>>
    {
        self.refine(input, control, || {
            checked.require_binding_author(self, input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl PureScalarImplementation for BinaryOperatorOwner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        self.prepare(input, contract, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }

    fn prepare_scalar_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        self.prepare(input, contract, control, || {
            checked.require_binding_author(self, input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}
