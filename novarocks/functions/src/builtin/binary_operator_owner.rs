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
    ArgumentControl, AssignmentCommonTypeError, CallEffects, CallProofScope, CompileCheckpoints,
    CompilePhase, DecimalOverflowPolicy, FunctionEffectDeclaration, FunctionInstanceState,
    FunctionNullBehavior, ObservableEffects, OperatorFunction, PureCompileControl,
    ValueLogicalType, arithmetic_result_value_shape_with_op, assignment_common_value_type,
};

use super::binding_control;
use super::catalogue::request_types_with_constants;
use super::operator_arithmetic::{ArithmeticSignature, arithmetic_types};
use super::operator_kernel::{
    BinaryOperatorKernel, composite_comparison_recipe, preserves_integer_array_operands,
};
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionArgument, FunctionArgumentType, FunctionBindingDeclaration,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionBindingResolverOwner, FunctionBindingSelection, FunctionCatalogError,
    FunctionDefinition, FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior,
    FunctionId, FunctionIntrinsicRowError, FunctionKind, FunctionOverloadDeclaration,
    FunctionOverloadId, FunctionPublicSignature, FunctionResultType, FunctionValueType,
    FunctionVisibility, FunctionVolatility, KernelFailure, PreparedScalarKernel,
    PureFunctionMetadataOwner, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    PureScalarImplementation, ScalarCallContract,
};

pub(super) fn definition(
    operator: OperatorFunction,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    operator_definition(
        operator,
        operator.catalog_name(),
        operator.function_id(),
        FunctionVisibility::Hidden,
    )
}

pub(super) fn modulus_definition() -> Result<FunctionDefinition, FunctionCatalogError> {
    operator_definition(
        OperatorFunction::Modulo,
        "mod",
        FunctionId::try_new("builtin.scalar/mod/v1").map_err(|error| {
            FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin mod function",
                value: error.to_string().into(),
            }
        })?,
        FunctionVisibility::Public,
    )
}

fn operator_definition(
    operator: OperatorFunction,
    name: &str,
    id: FunctionId,
    visibility: FunctionVisibility,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let declaration_error =
        |error: FunctionBindingError| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin binary operator declaration",
            value: error.to_string().into(),
        };
    let internal_wide = visibility == FunctionVisibility::Hidden;
    let overloads = if operator.arithmetic().is_some() {
        let mut overloads = ArithmeticSignature::ALL
            .iter()
            .map(|signature| {
                overload(
                    operator,
                    name,
                    signature.name(),
                    signature.pattern(),
                    signature.result_pattern(),
                    signature.row_error(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        if internal_wide {
            overloads.push(overload(
                operator,
                name,
                "internal-wide",
                WIDE_ARITHMETIC_PATTERN,
                "internal numeric result",
                FunctionIntrinsicRowError::MayRaise,
            )?);
        }
        overloads
    } else {
        vec![overload(
            operator,
            name,
            "comparison",
            "(comparable T,comparable T)",
            "boolean",
            if matches!(
                operator,
                OperatorFunction::Equal | OperatorFunction::NotEqual
            ) {
                FunctionIntrinsicRowError::MayRaise
            } else {
                FunctionIntrinsicRowError::NoRowError
            },
        )?]
    };
    let declaration = FunctionBindingDeclaration::try_new(id, FunctionKind::Scalar, overloads)
        .map_err(declaration_error)?;
    let declaration = if visibility == FunctionVisibility::Public {
        let forms = declaration
            .overloads()
            .iter()
            .map(|overload| {
                let arguments = overload
                    .argument_pattern
                    .strip_prefix('(')
                    .and_then(|value| value.strip_suffix(')'))
                    .expect("the remainder author declares parenthesized arguments");
                (
                    overload.identity.clone(),
                    vec![FunctionPublicSignature::new(
                        arguments,
                        overload.result_pattern.as_ref(),
                        "",
                    )],
                )
            })
            .collect::<Vec<_>>();
        declaration
            .with_public_signatures(forms)
            .map_err(declaration_error)?
    } else {
        declaration
    };
    let implementation =
        PureImplementationId::try_new(format!("builtin.scalar/{name}/selected-v1"))?;
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
        internal_wide,
        declaration,
        implementations,
    };
    FunctionDefinition::try_new_pure_scalar(name, visibility, Arc::new(owner)).map_err(|error| {
        FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin binary operator owner",
            value: error.to_string().into(),
        }
    })
}

fn overload(
    operator: OperatorFunction,
    catalogue_name: &str,
    name: &str,
    arguments: &str,
    result: &str,
    own_row_error: FunctionIntrinsicRowError,
) -> Result<FunctionOverloadDeclaration, FunctionCatalogError> {
    let identity = match operator.comparison_overload_text() {
        Some(identity) => FunctionOverloadId::try_new(identity),
        None => FunctionOverloadId::try_new(format!("builtin.scalar/{catalogue_name}/{name}-v1")),
    }
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
            argument_control: ArgumentControl::OrderedNull,
            instance_state: FunctionInstanceState::None,
            observable_effects: ObservableEffects::NONE,
            environment_dependencies: Box::new([]),
        },
    ))
}

pub(crate) struct BinaryOperatorOwner {
    operator: OperatorFunction,
    internal_wide: bool,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}

const WIDE_ARITHMETIC_PATTERN: &str = "(internal largeint or numeric,internal largeint or numeric)";

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

    fn wide_result_shape(
        &self,
        [left, right]: [&FunctionValueType; 2],
    ) -> Result<Option<(DataType, ValueLogicalType)>, FunctionBindingError> {
        let Some(operation) = self.operator.arithmetic() else {
            return Ok(None);
        };
        let is_largeint =
            |value: &FunctionValueType| value.logical_type() == ValueLogicalType::LargeInt;
        if !self.internal_wide || ![left, right].into_iter().any(is_largeint) {
            return Ok(None);
        }
        let signed_operand = |value: &FunctionValueType| {
            is_largeint(value) || crate::arithmetic::SignedWidth::from_type(value).is_some()
        };
        arithmetic_result_value_shape_with_op(left, right, operation)
            .filter(|(carrier, _)| {
                *carrier != DataType::Float64 || [left, right].into_iter().all(signed_operand)
            })
            .map(Some)
            .ok_or(FunctionBindingError::NoMatchingOverload)
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
        let types = if let Some((carrier, logical)) = self.wide_result_shape(inputs)? {
            let [left, right] = inputs;
            ResolvedTypes {
                pattern: WIDE_ARITHMETIC_PATTERN,
                arguments: [left.clone(), right.clone()],
                result: FunctionValueType::try_with_logical_type(carrier, true, logical)?,
            }
        } else if let Some(operation) = self.operator.arithmetic() {
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
            let args = if preserves_integer_array_operands(self.operator, inputs) {
                // The equality recipe widens only the actual signed elements.
                // The two independently authored ARRAY layouts stay intact.
                [left.clone(), right.clone()]
            } else {
                let common =
                    assignment_common_value_type(left, right, work).map_err(
                        |error| match error {
                            AssignmentCommonTypeError::Control(error) => {
                                FunctionBindingError::Control(error)
                            }
                            AssignmentCommonTypeError::Construction(error) => {
                                FunctionBindingError::ValueType(error.into())
                            }
                            AssignmentCommonTypeError::IncompatibleShape(_) => {
                                FunctionBindingError::NoMatchingOverload
                            }
                        },
                    )?;
                [
                    common.with_nullable(left_null)?,
                    common.with_nullable(right_null)?,
                ]
            };
            work.flush()?;
            let nullable = BinaryOperatorKernel::validate_comparison(
                self.operator,
                [&args[0], &args[1]],
                work.control(),
            )?;
            ResolvedTypes {
                pattern: "(comparable T,comparable T)",
                arguments: args,
                result: FunctionValueType::new(
                    DataType::Boolean,
                    !self.operator.is_null_safe_comparison() && nullable,
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

    fn effects(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionBindingError> {
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
        let own_row_error = if self.operator.arithmetic().is_some()
            && self.internal_wide
            && &input.selected.overload == self.declared_overload(WIDE_ARITHMETIC_PATTERN)?
        {
            if matches!(&input.selected.result_type, FunctionResultType::Scalar(result)
                if matches!(result.data_type(), DataType::Decimal256(..)))
                && input.decimal_overflow_policy == DecimalOverflowPolicy::ReportError
            {
                FunctionIntrinsicRowError::MayRaise
            } else {
                FunctionIntrinsicRowError::NoRowError
            }
        } else if matches!(
            self.operator,
            OperatorFunction::Equal | OperatorFunction::NotEqual
        ) {
            let [
                FunctionArgumentType::Value(left),
                FunctionArgumentType::Value(right),
            ] = input.selected.argument_types.as_ref()
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            match composite_comparison_recipe(self.operator, [left, right], control)? {
                Some(recipe) if recipe.may_raise_row_error() => FunctionIntrinsicRowError::MayRaise,
                _ => FunctionIntrinsicRowError::NoRowError,
            }
        } else {
            base.own_row_error
        };
        Ok(CallEffects {
            value_stability: base.value_stability,
            own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior.clone(),
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
        let effects = self.effects(input, control).map_err(|error| match error {
            FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
            other => FunctionEffectOwnerError::Owner(other),
        })?;
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
                != &self.effects(input, control).map_err(|error| match error {
                    FunctionBindingError::Control(error) => compile_failure(error),
                    _ => invalid("Invalid binary operator effects"),
                })?
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
            let (pattern, carrier, logical, nullable) =
                if let Some((carrier, logical)) = self.wide_result_shape(inputs)? {
                    (WIDE_ARITHMETIC_PATTERN, carrier, logical, true)
                } else if let Some(operation) = self.operator.arithmetic() {
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
                        ValueLogicalType::Physical,
                        left.nullable() || right.nullable(),
                    )
                } else {
                    work.flush()?;
                    let nullable = BinaryOperatorKernel::validate_comparison(
                        self.operator,
                        inputs,
                        work.control(),
                    )?;
                    (
                        "(comparable T,comparable T)",
                        DataType::Boolean,
                        ValueLogicalType::Physical,
                        !self.operator.is_null_safe_comparison() && nullable,
                    )
                };
            work.step()?;
            if &selected.overload == self.declared_overload(pattern)?
                && selected.aggregate.is_none()
                && matches!(&selected.result_type, FunctionResultType::Scalar(result)
                    if result.logical_type() == logical
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
