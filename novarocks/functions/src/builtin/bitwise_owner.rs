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

//! The installed bitwise bindings, effects and preparation share each exact owner.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinScalarResolver;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionVisibility, FunctionVolatility,
    KernelEvaluationControl, KernelFailure, PreparedScalarKernel, PureFunctionMetadataOwner,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, PureScalarImplementation,
    ScalarCallContract, ScalarCallInput, ScalarKernelInstance, SelectedValues,
};

use super::bitwise::BitwiseOp;

/// Startup selection covers the four installed bitwise operations.
/// Evaluation retains the private operation, never resolving a function name.
pub(super) fn operation(name: &str) -> Option<BitwiseOp> {
    Some(match name {
        "bitand" => BitwiseOp::And,
        "bitor" => BitwiseOp::Or,
        "bitxor" => BitwiseOp::Xor,
        "bitnot" => BitwiseOp::Not,
        _ => return None,
    })
}

/// Each operation has five profiles preserving signed integer width or the
/// declared LargeInt domain. NOT is unary; binary operations take two values
/// in the same complete domain, with independent nullability. No mutable state
/// or environment authority is retained.
pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(BitwiseOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin bitwise pure owner",
            value: error.to_string().into(),
        },
    )
}

struct BitwiseOwner {
    resolver: BuiltinScalarResolver,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    operation: BitwiseOp,
}
impl BitwiseOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let op = operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin bitwise name",
            value: name.into(),
        })?;
        let function = format!("builtin.scalar/{name}/v1");
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 5
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin bitwise pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
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
        Ok(Self {
            resolver,
            declaration,
            implementations,
            operation: op,
        })
    }

    fn call_effects(&self, scope: CallProofScope) -> CallEffects {
        let base = effects();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: scope,
        }
    }
}

impl FunctionBindingResolver for BitwiseOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver.resolve(request, control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &crate::FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver
            .select_at_overload_observed(overload, request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.resolver.validate_selected(selected, request, control)
    }
}

impl PureFunctionMetadataOwner for BitwiseOwner {
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for BitwiseOwner {
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
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        work.step().map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != self.declaration.function_id() || input.kind != FunctionKind::Scalar
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        if !input.environment.is_empty()
            || !matches!(input.proof_scope, CallProofScope::Unconditional)
                && input.proof_scope != CallProofScope::Domain(input.context.domain)
        {
            return Err(FunctionBindingError::InvalidBinding(
                "bitwise has no environment dependencies and requires an exact proof scope".into(),
            )
            .into());
        }
        work.flush().map_err(FunctionEffectOwnerError::Control)?;
        self.validate_selected(input.selected, input.request, control)
            .map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
        let result = self.call_effects(input.proof_scope);
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(result)
    }
}

impl PureScalarImplementation for BitwiseOwner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        work.step().map_err(compile_failure)?;
        if input.function_id != self.declaration.function_id()
            || input.kind != FunctionKind::Scalar
            || contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || contract.context() != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &self.call_effects(input.proof_scope)
            || !input.environment.is_empty()
        {
            return Err(invalid(
                "bitwise preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        self.validate_selected(input.selected, input.request, control)
            .map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("bitwise preparation has a stale selected binding"),
            })?;
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedBitwise {
            contract,
            operation: self.operation,
        }))
    }
}

#[derive(Debug)]
struct PreparedBitwise {
    contract: Arc<ScalarCallContract>,
    operation: BitwiseOp,
}
impl PreparedScalarKernel for PreparedBitwise {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn invocation_resource_profile(&self) -> Option<crate::ScalarInvocationResourceProfile> {
        use crate::scalar_resources::BitwiseWidth;
        use arrow_schema::DataType;
        use novarocks_type_contract::ValueLogicalType;

        let mut arguments = self.contract.value_argument_types();
        if arguments.len() != self.operation.arity() {
            return None;
        }
        let source = arguments.next()?;
        let result = self.contract.result_type();
        if source.logical_type != ValueLogicalType::Physical
            || result.logical_type != ValueLogicalType::Physical
            || result.data_type != source.data_type
            || !result.nullable
            || arguments.any(|argument| {
                argument.logical_type != source.logical_type
                    || argument.data_type != source.data_type
            })
        {
            return None;
        }
        let width = match source.data_type {
            DataType::Int8 => BitwiseWidth::Int8,
            DataType::Int16 => BitwiseWidth::Int16,
            DataType::Int32 => BitwiseWidth::Int32,
            DataType::Int64 => BitwiseWidth::Int64,
            _ => return None,
        };
        Some(crate::ScalarInvocationResourceProfile::bitwise(
            self.operation,
            width,
        ))
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<BitwiseInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(BitwiseInstance {
            operation: self.operation,
        }))
    }
}

struct BitwiseInstance {
    operation: BitwiseOp,
}
impl ScalarKernelInstance for BitwiseInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::bitwise::evaluate_bitwise(self.operation, input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
fn names() -> &'static [&'static str] {
    &["bitand", "bitor", "bitxor", "bitnot"]
}

#[cfg(test)]
pub(super) fn prepared_for_test(
    name: &str,
    sources: &[crate::FunctionValueType],
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    tests::prepared_for_test(name, sources)
}

#[cfg(test)]
pub(super) fn prepared_for_test_with_policy(
    name: &str,
    sources: &[crate::FunctionValueType],
    policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    tests::prepared_for_test_with_policy(name, sources, policy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FunctionArgument, FunctionArgumentType, FunctionResultType, FunctionSpecializationFailure,
        FunctionValueType, ScopedExpressionEffects, specialize_frozen_scalar, specialize_scalar,
    };
    use arrow_schema::DataType;
    use novarocks_type_contract::{
        CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
        ExpressionEffectContext, ExpressionUseId, SemanticParameterId, SemanticParameterKey,
        SemanticParameterRef, SemanticParameters, ValueLogicalType,
    };

    fn owner(name: &str) -> BitwiseOwner {
        let (_, signatures) = super::super::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(candidate, _)| candidate == name)
            .expect("the actual bit bitwise registry entry");
        let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
            name,
            &signatures,
            FunctionKind::Scalar,
        )
        .unwrap();
        BitwiseOwner::new(name, declaration, resolver).unwrap()
    }
    fn context() -> ExpressionEffectContext {
        ExpressionEffectContext {
            use_id: ExpressionUseId::new(41),
            domain: EvaluationDomainId::new(7),
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
        owner: &'a BitwiseOwner,
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

    fn sources(nullable: bool) -> [FunctionValueType; 5] {
        [
            FunctionValueType::new(DataType::Int8, nullable),
            FunctionValueType::new(DataType::Int16, nullable),
            FunctionValueType::new(DataType::Int32, nullable),
            FunctionValueType::new(DataType::Int64, nullable),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                nullable,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        ]
    }

    pub(super) fn prepared_for_test(
        name: &str,
        sources: &[FunctionValueType],
    ) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
        prepared_for_test_with_policy(name, sources, DecimalOverflowPolicy::ReportError)
    }
    pub(super) fn prepared_for_test_with_policy(
        name: &str,
        sources: &[FunctionValueType],
        policy: DecimalOverflowPolicy,
    ) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
        if operation(name).is_none() {
            return Err(FunctionSpecializationFailure::Binding(
                FunctionBindingError::UnknownFunction,
            ));
        }
        let owner = owner(name);
        let arguments = sources.iter().cloned().map(argument).collect::<Vec<_>>();
        let selected = Arc::new(owner.resolve(request(&arguments), crate::binding_test_control())?);
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = (0..arguments.len())
            .map(|index| Some(ExpressionUseId::new(42 + index as u32)))
            .collect::<Vec<_>>();
        let mut exact = input(&owner, &selected, &arguments, &parameters, &uses);
        exact.decimal_overflow_policy = policy;
        specialize_scalar(
            &owner,
            exact,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .map(|call| call.into_prepared())
    }

    #[test]
    fn startup_mapping_retains_four_exact_names_and_operations() {
        assert_eq!(names(), &["bitand", "bitor", "bitxor", "bitnot"]);
        for (name, op) in [
            ("bitand", BitwiseOp::And),
            ("bitor", BitwiseOp::Or),
            ("bitxor", BitwiseOp::Xor),
            ("bitnot", BitwiseOp::Not),
        ] {
            assert_eq!(operation(name), Some(op));
            assert_eq!(
                owner(name).declaration.function_id().as_str(),
                format!("builtin.scalar/{name}/v1")
            );
        }
        for outside in [
            "bit_shift_left",
            "bit_shift_right",
            "bit_shift_right_logical",
            "xx_hash3_128",
            "mod",
            "bit_and",
        ] {
            assert_eq!(operation(outside), None);
        }
    }

    #[test]
    fn whole_production_catalogue_attaches_bitwise_owners_and_20_exact_records() {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let mut total = 0;
        for name in names() {
            let owner = owner(name);
            let definition = catalog
                .definition_by_id(owner.declaration.function_id())
                .unwrap();
            assert!(definition.binding.as_ref().unwrap().pure.is_some());
            let declaration = definition.binding_declaration().unwrap();
            assert_eq!(declaration, owner.binding_declaration());
            assert_eq!(declaration.overloads().len(), 5);
            assert_eq!(owner.implementation_declarations().len(), 5);
            for (overload, implementation) in declaration
                .overloads()
                .iter()
                .zip(owner.implementation_declarations())
            {
                assert_eq!(implementation.overload, overload.identity);
                assert_eq!(
                    implementation.implementation.as_str(),
                    format!("builtin.scalar/{name}/selected-v1")
                );
                assert_eq!(implementation.abi, PureKernelAbi::ScalarV1);
                assert_eq!(overload.effects.as_ref(), Some(&effects()));
            }
            total += owner.implementation_declarations().len();
        }
        assert_eq!(total, 20);
    }

    #[test]
    fn bitwise_resources_cover_16_physical_profiles_and_leave_largeint_uncovered() {
        let mut covered = 0;
        let mut uncovered = 0;
        for name in names() {
            let operation = operation(name).unwrap();
            for source in sources(false) {
                let physical = source.logical_type == ValueLogicalType::Physical;
                for mask in 0..1usize << operation.arity() {
                    let arguments = (0..operation.arity())
                        .map(|index| {
                            let mut value = source.clone();
                            value.nullable = mask & (1 << index) != 0;
                            value
                        })
                        .collect::<Vec<_>>();
                    for policy in [
                        DecimalOverflowPolicy::OutputNull,
                        DecimalOverflowPolicy::ReportError,
                    ] {
                        let prepared =
                            prepared_for_test_with_policy(name, &arguments, policy).unwrap();
                        let profile = prepared.invocation_resource_profile();
                        assert_eq!(profile.is_some(), physical);
                        let instance =
                            crate::ScalarEvaluationInstance::instantiate(prepared).unwrap();
                        assert_eq!(instance.invocation_resource_profile(), profile);
                        if let Some(profile) = profile {
                            assert_eq!(profile.requests(0).unwrap().requests(), 6);
                            assert_eq!(
                                profile.requests(513).unwrap().requests(),
                                if source.data_type == DataType::Int64 {
                                    7
                                } else {
                                    12
                                }
                            );
                        }
                    }
                }
                if physical {
                    covered += 1;
                } else {
                    uncovered += 1;
                }
            }
        }
        assert_eq!(covered, 16);
        assert_eq!(uncovered, 4);
    }

    #[test]
    fn all_20_profiles_and_nullable_combinations_preserve_exact_fresh_frozen_contracts() {
        let parameters = SemanticParameters::try_new([]).unwrap();
        for name in names() {
            let owner = owner(name);
            let arity = if *name == "bitnot" { 1 } else { 2 };
            let uses = (0..arity)
                .map(|index| Some(ExpressionUseId::new(42 + index as u32)))
                .collect::<Vec<_>>();
            let mut selected_overloads = std::collections::BTreeSet::new();
            for source in sources(false) {
                for mask in 0..1usize << arity {
                    let full_sources = (0..arity)
                        .map(|index| {
                            let mut value = source.clone();
                            value.nullable = mask & (1 << index) != 0;
                            value
                        })
                        .collect::<Vec<_>>();
                    let arguments = full_sources
                        .iter()
                        .cloned()
                        .map(argument)
                        .collect::<Vec<_>>();
                    let selected = Arc::new(
                        owner
                            .resolve(request(&arguments), crate::binding_test_control())
                            .unwrap(),
                    );
                    selected_overloads.insert(selected.overload.clone());
                    assert_eq!(
                        selected.argument_types.as_ref(),
                        full_sources
                            .iter()
                            .cloned()
                            .map(FunctionArgumentType::Value)
                            .collect::<Vec<_>>()
                            .as_slice()
                    );
                    let mut expected = source.clone();
                    expected.nullable = true;
                    assert_eq!(selected.result_type, FunctionResultType::Scalar(expected));
                    for policy in [
                        DecimalOverflowPolicy::OutputNull,
                        DecimalOverflowPolicy::ReportError,
                    ] {
                        let mut input = input(&owner, &selected, &arguments, &parameters, &uses);
                        input.decimal_overflow_policy = policy;
                        let fresh = specialize_scalar(
                            &owner,
                            input,
                            selected.clone(),
                            ScopedExpressionEffects::pure_value(context()),
                            crate::binding_test_control(),
                        )
                        .unwrap();
                        let canonical = fresh.prepared().contract().clone();
                        let direct = owner
                            .prepare_scalar(input, canonical.clone(), crate::binding_test_control())
                            .unwrap();
                        assert!(Arc::ptr_eq(direct.contract(), &canonical));
                        assert!(std::ptr::eq(canonical.selected(), selected.as_ref()));
                        let frozen = canonical.effects().clone();
                        assert_eq!(frozen.own_row_error, FunctionIntrinsicRowError::NoRowError);
                        assert_eq!(frozen.instance_state, FunctionInstanceState::None);
                        assert_eq!(frozen.argument_control, ArgumentControl::Eager);
                        assert_eq!(frozen.null_behavior, FunctionNullBehavior::Strict);
                        assert!(frozen.environment.is_empty());
                        let be = specialize_frozen_scalar(
                            &owner,
                            input,
                            selected.clone(),
                            &frozen,
                            ScopedExpressionEffects::pure_value(context()),
                            crate::binding_test_control(),
                        )
                        .unwrap();
                        let canonical = be.prepared().contract().clone();
                        let direct = owner
                            .prepare_scalar(input, canonical.clone(), crate::binding_test_control())
                            .unwrap();
                        assert!(Arc::ptr_eq(direct.contract(), &canonical));
                        assert!(std::ptr::eq(canonical.selected(), selected.as_ref()));
                        assert_eq!(canonical.effects(), &frozen);
                        let instance = be.prepared().create_instance().unwrap();
                        assert_eq!(
                            instance.retained_bytes(),
                            std::mem::size_of::<BitwiseInstance>()
                        );
                        assert_eq!(
                            instance.retained_bytes(),
                            be.prepared().instance_retained_upper_bound()
                        );
                    }
                }
            }
            assert_eq!(selected_overloads.len(), 5);
            assert_eq!(
                selected_overloads,
                owner
                    .declaration
                    .overloads()
                    .iter()
                    .map(|overload| overload.identity.clone())
                    .collect()
            );
        }
    }

    #[test]
    fn exact_bitwise_owner_rejects_stale_output_overload_and_foreign_frozen_effects() {
        let owner = owner("bitand");
        let arguments = [
            argument(FunctionValueType::new(DataType::Int8, false)),
            argument(FunctionValueType::new(DataType::Int8, true)),
        ];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let mut stale = (*selected).clone();
        stale.result_type =
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Int8, false));
        assert!(
            owner
                .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                .is_err()
        );
        stale = (*selected).clone();
        stale.overload = self::owner("bitor").declaration.overloads()[0]
            .identity
            .clone();
        assert!(
            owner
                .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                .is_err()
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [
            Some(ExpressionUseId::new(42)),
            Some(ExpressionUseId::new(43)),
        ];
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        let mut foreign = owner
            .validate_and_refine(input, crate::binding_test_control())
            .unwrap();
        foreign.own_row_error = FunctionIntrinsicRowError::MayRaise;
        assert!(matches!(
            specialize_frozen_scalar(
                &owner,
                input,
                selected.clone(),
                &foreign,
                ScopedExpressionEffects::pure_value(context()),
                crate::binding_test_control()
            ),
            Err(FunctionSpecializationFailure::InvalidInput(
                "frozen call effects differ from exact local refinement"
            ))
        ));
    }

    #[test]
    fn bitwise_refinement_requires_exact_environment_domain_and_function_identity() {
        let owner = owner("bitor");
        let arguments = [
            argument(FunctionValueType::new(DataType::Int64, false)),
            argument(FunctionValueType::new(DataType::Int64, false)),
        ];
        let selected = owner
            .resolve(request(&arguments), crate::binding_test_control())
            .unwrap();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [
            Some(ExpressionUseId::new(42)),
            Some(ExpressionUseId::new(43)),
        ];
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        let mut foreign = input;
        foreign.proof_scope = CallProofScope::Domain(EvaluationDomainId::new(99));
        assert!(matches!(
            owner.validate_and_refine(foreign, crate::binding_test_control()),
            Err(FunctionEffectOwnerError::Owner(
                FunctionBindingError::InvalidBinding(_)
            ))
        ));
        let environment = [SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::StatementStartUtc,
        }];
        foreign = input;
        foreign.environment = &environment;
        assert!(matches!(
            owner.validate_and_refine(foreign, crate::binding_test_control()),
            Err(FunctionEffectOwnerError::Owner(
                FunctionBindingError::InvalidBinding(_)
            ))
        ));
        let other = FunctionId::try_new("builtin.scalar/bitand/v1").unwrap();
        foreign = input;
        foreign.function_id = &other;
        assert!(matches!(
            owner.validate_and_refine(foreign, crate::binding_test_control()),
            Err(FunctionEffectOwnerError::Owner(
                FunctionBindingError::UnknownFunction
            ))
        ));
        let mut exact = input;
        exact.proof_scope = CallProofScope::Domain(context().domain);
        assert_eq!(
            owner
                .validate_and_refine(exact, crate::binding_test_control())
                .unwrap()
                .proof_scope,
            CallProofScope::Domain(context().domain)
        );
    }

    #[test]
    fn bitwise_preparation_rejects_foreign_context_policy_pointer_and_uncoerced_types() {
        let owner = owner("bitor");
        let arguments = [
            argument(FunctionValueType::new(DataType::Int64, false)),
            argument(FunctionValueType::new(DataType::Int64, true)),
        ];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [
            Some(ExpressionUseId::new(42)),
            Some(ExpressionUseId::new(43)),
        ];
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        let fresh = specialize_scalar(
            &owner,
            input,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .unwrap();
        let canonical = fresh.prepared().contract().clone();
        let mut foreign = input;
        foreign.context.use_id = ExpressionUseId::new(100);
        assert!(
            owner
                .prepare_scalar(foreign, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        foreign = input;
        foreign.decimal_overflow_policy = DecimalOverflowPolicy::OutputNull;
        assert!(
            owner
                .prepare_scalar(foreign, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        let equal_foreign_selection = (*selected).clone();
        foreign = input;
        foreign.selected = &equal_foreign_selection;
        assert!(
            owner
                .prepare_scalar(foreign, canonical, crate::binding_test_control())
                .is_err()
        );
        for source in [
            FunctionValueType::new(DataType::Float64, false),
            FunctionValueType::new(DataType::Decimal128(18, 3), false),
            FunctionValueType::new(DataType::UInt64, false),
            FunctionValueType {
                data_type: DataType::FixedSizeBinary(16),
                nullable: false,
                logical_type: ValueLogicalType::Uuid,
            },
            FunctionValueType {
                data_type: DataType::FixedSizeBinary(16),
                nullable: false,
                logical_type: ValueLogicalType::LargeInt,
            },
        ] {
            for index in 0..2 {
                let mut wrong = arguments.clone();
                wrong[index] = argument(source.clone());
                assert!(matches!(
                    specialize_scalar(
                        &owner,
                        self::input(&owner, &selected, &wrong, &parameters, &uses),
                        selected.clone(),
                        ScopedExpressionEffects::pure_value(context()),
                        crate::binding_test_control()
                    ),
                    Err(FunctionSpecializationFailure::InvalidInput(_))
                ));
            }
        }
    }

    struct FailCompile(CompileControlError);
    impl PureCompileControl for FailCompile {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0)
        }
    }
    #[test]
    fn bitwise_owner_preserves_original_three_compile_failures() {
        let owner = owner("bitor");
        let arguments = [
            argument(FunctionValueType::new(DataType::Int64, false)),
            argument(FunctionValueType::new(DataType::Int64, false)),
        ];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [
            Some(ExpressionUseId::new(42)),
            Some(ExpressionUseId::new(43)),
        ];
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        let facts = owner
            .validate_and_refine(input, crate::binding_test_control())
            .unwrap();
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert!(
                matches!(owner.resolve(request(&arguments), &FailCompile(error)), Err(FunctionBindingError::Control(actual)) if actual == error)
            );
            assert!(
                matches!(owner.validate_and_refine(input, &FailCompile(error)), Err(FunctionEffectOwnerError::Control(actual)) if actual == error)
            );
            assert!(
                matches!(specialize_scalar(&owner, input, selected.clone(), ScopedExpressionEffects::pure_value(context()), &FailCompile(error)),
                Err(FunctionSpecializationFailure::Control(actual)) if actual == error)
            );
            assert!(matches!(
                specialize_frozen_scalar(&owner, input, selected.clone(), &facts, ScopedExpressionEffects::pure_value(context()), &FailCompile(error)),
                Err(FunctionSpecializationFailure::Control(actual)) if actual == error
            ));
        }
    }
    #[test]
    fn nominal_fixed16_cannot_bind_as_largeint_and_registered_arity_is_exact() {
        for name in names() {
            let owner = owner(name);
            let arity = if *name == "bitnot" { 1 } else { 2 };
            for source in [
                FunctionValueType::new(DataType::FixedSizeBinary(16), false),
                FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    false,
                    ValueLogicalType::Uuid,
                )
                .unwrap(),
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    false,
                    ValueLogicalType::Json,
                )
                .unwrap(),
            ] {
                let arguments = (0..arity)
                    .map(|_| argument(source.clone()))
                    .collect::<Vec<_>>();
                assert!(
                    owner
                        .resolve(request(&arguments), crate::binding_test_control())
                        .is_err()
                );
            }
            let value = argument(FunctionValueType::new(DataType::Int64, false));
            for count in [0, 1, 2, 3] {
                if count == arity {
                    continue;
                }
                let args = vec![value.clone(); count];
                assert!(
                    owner
                        .resolve(request(&args), crate::binding_test_control())
                        .is_err()
                );
            }
            let sources = vec![self::sources(false)[4].clone(); arity];
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let prepared = prepared_for_test_with_policy(name, &sources, policy).unwrap();
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                assert_eq!(
                    prepared.contract().effects().own_row_error,
                    FunctionIntrinsicRowError::NoRowError
                );
            }
        }
    }

    #[test]
    fn original_control_refuses_type_metadata_work_at_256_and_exit() {
        use arrow_schema::Field;
        use std::sync::Mutex;
        struct TraceControl {
            calls: Mutex<Vec<u32>>,
            refusal: Option<(usize, CompileControlError)>,
        }
        impl PureCompileControl for TraceControl {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                assert_eq!(phase, CompilePhase::FunctionSpecialization);
                assert!(units <= 256);
                let mut calls = self.calls.lock().unwrap();
                let at = calls.len();
                calls.push(units);
                if let Some((refused_at, error)) = self.refusal
                    && at == refused_at
                {
                    return Err(error);
                }
                Ok(())
            }
        }
        let owner = owner("bitxor");
        let args = [
            argument(FunctionValueType::new(DataType::Int64, false)),
            argument(FunctionValueType::new(DataType::Int64, false)),
        ];
        let expected = FunctionValueType::new(
            DataType::Struct(
                (0..128)
                    .map(|index| {
                        Arc::new(Field::new(format!("field{index}"), DataType::Int64, true))
                    })
                    .collect::<Vec<_>>()
                    .into(),
            ),
            true,
        );
        let request = FunctionBindingRequest {
            expected_result_type: Some(&expected),
            ..request(&args)
        };
        let baseline = TraceControl {
            calls: Mutex::new(Vec::new()),
            refusal: None,
        };
        let selected = owner.resolve(request, &baseline).unwrap();
        assert_eq!(
            selected.result_type,
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, true))
        );
        let calls = baseline.calls.lock().unwrap().clone();
        assert!(calls.contains(&256));
        assert!(*calls.last().unwrap() < 256);
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..calls.len() {
                let control = TraceControl {
                    calls: Mutex::new(Vec::new()),
                    refusal: Some((at, error)),
                };
                assert!(
                    matches!(owner.resolve(request, &control), Err(FunctionBindingError::Control(actual)) if actual == error)
                );
                assert_eq!(control.calls.lock().unwrap().len(), at + 1);
            }
        }
    }
    #[test]
    fn two_argument_bitnot_is_rejected_by_fresh_and_frozen_actual_owner() {
        let owner = owner("bitnot");
        let value = FunctionValueType::new(DataType::Int64, false);
        let arguments = [argument(value.clone())];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        assert_eq!(selected.argument_types.len(), 1);
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
        let exact = input(&owner, &selected, &arguments, &parameters, &uses);
        let fresh = specialize_scalar(
            &owner,
            exact,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .unwrap();
        let facts = fresh.prepared().contract().effects();
        let extra_arguments = [argument(value.clone()), argument(value.clone())];
        let extra_uses = [
            Some(ExpressionUseId::new(42)),
            Some(ExpressionUseId::new(43)),
        ];
        assert!(
            owner
                .resolve(request(&extra_arguments), crate::binding_test_control())
                .is_err()
        );
        let wrong = input(
            &owner,
            &selected,
            &extra_arguments,
            &parameters,
            &extra_uses,
        );
        assert!(
            specialize_scalar(
                &owner,
                wrong,
                selected.clone(),
                ScopedExpressionEffects::pure_value(context()),
                crate::binding_test_control()
            )
            .is_err()
        );
        assert!(
            specialize_frozen_scalar(
                &owner,
                wrong,
                selected.clone(),
                facts,
                ScopedExpressionEffects::pure_value(context()),
                crate::binding_test_control()
            )
            .is_err()
        );
        let mut forged = (*selected).clone();
        forged.argument_types = vec![
            FunctionArgumentType::Value(value.clone()),
            FunctionArgumentType::Value(value),
        ]
        .into_boxed_slice();
        assert!(
            owner
                .validate_selected(
                    &forged,
                    request(&extra_arguments),
                    crate::binding_test_control()
                )
                .is_err()
        );
        assert!(
            prepared_for_test(
                "bitnot",
                &[sources(false)[0].clone(), sources(false)[0].clone()]
            )
            .is_err()
        );
    }
}
