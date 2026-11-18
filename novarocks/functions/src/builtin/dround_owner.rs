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

//! The installed DROUND bindings, effects and preparation share each exact owner.

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

use super::dround::DroundOp;

const FUNCTION: &str = "builtin.scalar/dround/v1";
const IMPLEMENTATION: &str = "builtin.scalar/dround/selected-v1";

/// The seven unary profiles use floating rounding; the sole Float64/Int32
/// binary profile preserves its installed truncate-digits algorithm. Decimal
/// unary input is decoded using its exact scale and returns Float64. Domain
/// and non-finite results are successful NULL, without mutable state or env.
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
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(DroundOwner::new(declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar("dround", FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin dround pure owner",
            value: error.to_string().into(),
        },
    )
}

struct DroundOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl DroundOwner {
    fn new(
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let expected = effects();
        if declaration.function_id().as_str() != FUNCTION
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 8
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin dround pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation = PureImplementationId::try_new(IMPLEMENTATION)?;
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
            resolver: Arc::new(resolver),
            declaration,
            implementations,
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

impl FunctionBindingResolver for DroundOwner {
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

impl PureFunctionMetadataOwner for DroundOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::fixed_scalar(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for DroundOwner {
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
        self.refine_selected_core(input, control, || {
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
        self.refine_selected_core(input, control, || {
            checked.require_fixed_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl PureScalarImplementation for DroundOwner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        self.prepare_selected_core(input, contract, control, || {
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
        self.prepare_selected_core(input, contract, control, || {
            checked.require_fixed_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

#[derive(Debug)]
struct PreparedDround {
    contract: Arc<ScalarCallContract>,
    operation: DroundOp,
}
impl PreparedScalarKernel for PreparedDround {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<DroundInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(DroundInstance {
            operation: self.operation,
        }))
    }
}

struct DroundInstance {
    operation: DroundOp,
}
impl ScalarKernelInstance for DroundInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::dround::evaluate_dround(self.operation, input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
pub(super) fn prepared_for_test(
    sources: &[crate::FunctionValueType],
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    tests::prepared_for_test(sources)
}

impl DroundOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        work.step().map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != self.declaration.function_id() || input.kind != FunctionKind::Scalar
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        if !input.environment.is_empty()
            || !matches!(input.proof_scope, CallProofScope::Unconditional)
                && input.proof_scope != input.context.proof_scope()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "dround has no environment dependencies and requires an exact proof scope".into(),
            )
            .into());
        }
        work.flush().map_err(FunctionEffectOwnerError::Control)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
            other => FunctionEffectOwnerError::Owner(other),
        })?;
        let result = self.call_effects(input.proof_scope);
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(result)
    }
    fn prepare_selected_core(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        work.step().map_err(compile_failure)?;
        if input.function_id != self.declaration.function_id()
            || input.kind != FunctionKind::Scalar
            || contract.function_id() != input.function_id
            || !std::ptr::eq(contract.selected(), input.selected)
            || novarocks_type_contract::CallEffectContext::from(contract.context()) != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &self.call_effects(input.proof_scope)
            || !input.environment.is_empty()
        {
            return Err(invalid(
                "dround preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("dround preparation has a stale selected binding"),
        })?;
        let operation = match input.selected.argument_types.len() {
            1 => DroundOp::Round,
            2 => DroundOp::TruncateDigits,
            _ => return Err(invalid("DROUND preparation has an invalid selected arity")),
        };
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedDround {
            contract,
            operation,
        }))
    }
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
        CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
        ExpressionEffectContext, ExpressionUseId, SemanticParameterId, SemanticParameterKey,
        SemanticParameterRef, SemanticParameters, ValueLogicalType,
    };

    fn owner() -> DroundOwner {
        let (_, signatures) = super::super::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(name, _)| name == "dround")
            .expect("the actual DROUND registry entry");
        let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
            "dround",
            &signatures,
            FunctionKind::Scalar,
        )
        .unwrap();
        DroundOwner::new(declaration, resolver).unwrap()
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
        owner: &'a DroundOwner,
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

    fn profiles() -> Vec<Vec<DataType>> {
        let mut profiles = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(38, -3),
        ]
        .into_iter()
        .map(|source| vec![source])
        .collect::<Vec<_>>();
        profiles.push(vec![DataType::Float64, DataType::Int32]);
        profiles
    }
    fn uses(count: usize) -> Vec<Option<ExpressionUseId>> {
        (0..count)
            .map(|index| Some(ExpressionUseId::new(42 + index as u32)))
            .collect()
    }
    pub(super) fn prepared_for_test(
        sources: &[FunctionValueType],
    ) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
        let owner = owner();
        let arguments = sources.iter().cloned().map(argument).collect::<Vec<_>>();
        let selected = Arc::new(owner.resolve(request(&arguments), crate::binding_test_control())?);
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(arguments.len());
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
    fn actual_whole_catalogue_installs_one_dround_owner_and_eight_exact_records() {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let owner = owner();
        let definition = catalog
            .definition_by_id(owner.declaration.function_id())
            .unwrap();
        assert!(definition.binding.as_ref().unwrap().pure.is_some());
        let declaration = definition.binding_declaration().unwrap();
        assert_eq!(declaration, owner.binding_declaration());
        assert_eq!(declaration.function_id().as_str(), FUNCTION);
        assert_eq!(declaration.overloads().len(), 8);
        assert_eq!(owner.implementation_declarations().len(), 8);
        for (overload, implementation) in declaration
            .overloads()
            .iter()
            .zip(owner.implementation_declarations())
        {
            assert_eq!(implementation.overload, overload.identity);
            assert_eq!(implementation.implementation.as_str(), IMPLEMENTATION);
            assert_eq!(implementation.abi, PureKernelAbi::ScalarV1);
            assert_eq!(overload.effects.as_ref(), Some(&effects()));
        }
    }

    #[test]
    fn all_eight_dround_profiles_and_nullable_combinations_prepare_exact_fresh_frozen() {
        let owner = owner();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let profiles = profiles();
        assert_eq!(profiles.len(), 8);
        for profile in profiles {
            let uses = uses(profile.len());
            for mask in 0..(1usize << profile.len()) {
                let full_sources = profile
                    .iter()
                    .enumerate()
                    .map(|(index, source)| {
                        FunctionValueType::from_legacy_declared_projection(
                            source.clone(),
                            mask & (1 << index) != 0,
                        )
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
                let expected_arguments = full_sources
                    .iter()
                    .cloned()
                    .map(FunctionArgumentType::Value)
                    .collect::<Vec<_>>();
                assert_eq!(
                    selected.argument_types.as_ref(),
                    expected_arguments.as_slice()
                );
                let FunctionResultType::Scalar(result) = &selected.result_type else {
                    panic!("scalar result required")
                };
                assert_eq!(result.data_type().clone(), DataType::Float64);
                assert_eq!(result.logical_type(), ValueLogicalType::Physical);
                assert!(result.nullable());
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
                let direct = owner
                    .prepare_scalar(input, canonical.clone(), crate::binding_test_control())
                    .unwrap();
                assert!(Arc::ptr_eq(direct.contract(), &canonical));
                assert!(std::ptr::eq(
                    fresh.prepared().contract().selected(),
                    selected.as_ref()
                ));
                let frozen = canonical.effects().clone();
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
                assert!(std::ptr::eq(
                    be.prepared().contract().selected(),
                    selected.as_ref()
                ));
                assert_eq!(canonical.effects(), &frozen);
                let instance = be.prepared().create_instance().unwrap();
                assert_eq!(
                    instance.retained_bytes(),
                    std::mem::size_of::<DroundInstance>()
                );
                assert_eq!(
                    instance.retained_bytes(),
                    be.prepared().instance_retained_upper_bound()
                );
            }
        }
    }

    #[test]
    fn dround_selected_output_arity_and_overload_cannot_be_reauthored() {
        let owner = owner();
        let arguments = [argument(FunctionValueType::new(DataType::Float64, false))];
        let selected = owner
            .resolve(request(&arguments), crate::binding_test_control())
            .unwrap();
        let mut stale = selected.clone();
        stale.result_type =
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Float64, false));
        assert!(
            owner
                .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                .is_err()
        );
        stale = selected.clone();
        stale.argument_types =
            novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::new([]));
        assert!(
            owner
                .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                .is_err()
        );
        stale = selected;
        stale.overload =
            crate::FunctionOverloadId::try_new("builtin.scalar/round/dynamic-v1").unwrap();
        assert!(
            owner
                .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                .is_err()
        );
        let source = FunctionValueType::new(DataType::Float64, false);
        assert!(prepared_for_test(&[]).is_err());
        assert!(prepared_for_test(&[source.clone(), source.clone(), source]).is_err());
    }

    #[test]
    fn dround_refines_no_environment_and_rejects_foreign_frozen_facts() {
        let owner = owner();
        let arguments = [argument(FunctionValueType::new(DataType::Float64, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(arguments.len());
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        let facts = owner
            .validate_and_refine(input, crate::binding_test_control())
            .unwrap();
        assert_eq!(facts, owner.call_effects(CallProofScope::Unconditional));
        let mut foreign = facts;
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
        let mut wrong = input;
        wrong.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99));
        assert!(matches!(
            owner.validate_and_refine(wrong, crate::binding_test_control()),
            Err(FunctionEffectOwnerError::Owner(
                FunctionBindingError::InvalidBinding(_)
            ))
        ));
        let environment = [SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::StatementStartUtc,
        }];
        wrong = input;
        wrong.environment = &environment;
        assert!(matches!(
            owner.validate_and_refine(wrong, crate::binding_test_control()),
            Err(FunctionEffectOwnerError::Owner(
                FunctionBindingError::InvalidBinding(_)
            ))
        ));
        let other = FunctionId::try_new("builtin.scalar/round/v1").unwrap();
        wrong = input;
        wrong.function_id = &other;
        assert!(matches!(
            owner.validate_and_refine(wrong, crate::binding_test_control()),
            Err(FunctionEffectOwnerError::Owner(
                FunctionBindingError::UnknownFunction
            ))
        ));
        let mut exact = input;
        exact.proof_scope = CallProofScope::Region(context().region);
        assert_eq!(
            owner
                .validate_and_refine(exact, crate::binding_test_control())
                .unwrap()
                .proof_scope,
            CallProofScope::Region(context().region)
        );
    }

    #[test]
    fn dround_preparation_keeps_exact_context_policy_and_canonical_selected_pointer() {
        let owner = owner();
        let arguments = [argument(FunctionValueType::new(DataType::Float64, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(arguments.len());
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
        foreign.context = novarocks_type_contract::ExpressionEffectContext {
            use_id: ExpressionUseId::new(100),
            ..foreign.context.expression().unwrap()
        }
        .into();
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
    }

    #[test]
    fn dround_binary_requires_actual_coerced_f64_i32_and_unary_rejects_opaque_source() {
        let owner = owner();
        let arguments = [
            argument(FunctionValueType::new(DataType::Float64, false)),
            argument(FunctionValueType::new(DataType::Int32, false)),
        ];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(arguments.len());
        for (index, ty) in [
            (1, DataType::Int64),
            (1, DataType::Decimal128(18, 3)),
            (0, DataType::Decimal128(18, 3)),
        ] {
            let mut wrong = arguments.clone();
            wrong[index] = argument(FunctionValueType::from_legacy_declared_projection(
                ty, false,
            ));
            assert!(matches!(
                specialize_scalar(
                    &owner,
                    input(&owner, &selected, &wrong, &parameters, &uses),
                    selected.clone(),
                    ScopedExpressionEffects::pure_value(context()),
                    crate::binding_test_control()
                ),
                Err(FunctionSpecializationFailure::InvalidInput(_))
            ));
        }
        let arguments = [argument(FunctionValueType::new(DataType::Float64, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let uses = self::uses(arguments.len());
        for logical_type in [
            ValueLogicalType::Physical,
            ValueLogicalType::Uuid,
            ValueLogicalType::LargeInt,
        ] {
            let wrong = [argument(
                FunctionValueType::try_from_legacy_declared_projection(
                    DataType::FixedSizeBinary(16),
                    false,
                    logical_type,
                )
                .expect("valid temporary legacy function value declaration"),
            )];
            assert!(matches!(
                specialize_scalar(
                    &owner,
                    input(&owner, &selected, &wrong, &parameters, &uses),
                    selected.clone(),
                    ScopedExpressionEffects::pure_value(context()),
                    crate::binding_test_control()
                ),
                Err(FunctionSpecializationFailure::InvalidInput(_))
            ));
        }
    }

    struct FailCompile(CompileControlError);
    impl PureCompileControl for FailCompile {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    #[test]
    fn dround_owner_preserves_three_original_compile_refusals() {
        let owner = owner();
        let arguments = [argument(FunctionValueType::new(DataType::Float64, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(arguments.len());
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert!(
                matches!(owner.resolve(request(&arguments), &FailCompile(error.clone())), Err(FunctionBindingError::Control(actual)) if actual == error)
            );
            assert!(
                matches!(owner.validate_and_refine(input, &FailCompile(error.clone())), Err(FunctionEffectOwnerError::Control(actual)) if actual == error)
            );
            assert!(
                matches!(specialize_scalar(&owner, input, selected.clone(), ScopedExpressionEffects::pure_value(context()), &FailCompile(error.clone())),
                Err(FunctionSpecializationFailure::Control(actual)) if actual == error)
            );
        }
    }
}
