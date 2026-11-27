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

//! The installed elementary numeric bindings, effects and preparation share each exact owner.

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

use super::numeric_elementary::NumericElementaryOp;

#[cfg(test)]
pub(super) fn names() -> &'static [&'static str] {
    &["log", "e", "pi"]
}

pub(super) fn is_installed(name: &str) -> bool {
    Family::from_name(name).is_some()
}

#[derive(Clone, Copy)]
enum Family {
    Log,
    E,
    Pi,
}
impl Family {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "log" => Self::Log,
            "e" => Self::E,
            "pi" => Self::Pi,
            _ => return None,
        })
    }
    const fn overload_count(self) -> usize {
        match self {
            Self::Log => 1,
            Self::E | Self::Pi => 1,
        }
    }
    /// Called only after the exact resolver has validated the selected binding.
    /// The returned private operation is the runtime recipe; arity is not
    /// reinterpreted from an evaluated array or a runtime function name.
    const fn operation(self, selected_arity: usize) -> Option<NumericElementaryOp> {
        Some(match (self, selected_arity) {
            (Self::Log, 2) => NumericElementaryOp::LogBase,
            (Self::E, 0) => NumericElementaryOp::E,
            (Self::Pi, 0) => NumericElementaryOp::Pi,
            _ => return None,
        })
    }
}

/// Binary logarithm and mathematical constants use immutable eager evaluation.
pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::OrderedNull,
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
    let owner = Arc::new(NumericElementaryOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin numeric elementary pure owner",
            value: error.to_string().into(),
        },
    )
}

struct NumericElementaryOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    family: Family,
}
impl NumericElementaryOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let family =
            Family::from_name(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin numeric elementary name",
                value: name.into(),
            })?;
        let function = format!("builtin.scalar/{name}/v1");
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != family.overload_count()
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin numeric elementary pure declaration",
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
            resolver: Arc::new(resolver),
            declaration,
            implementations,
            family,
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

impl FunctionBindingResolver for NumericElementaryOwner {
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

impl PureFunctionMetadataOwner for NumericElementaryOwner {
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

impl FunctionEffectOwner for NumericElementaryOwner {
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

impl PureScalarImplementation for NumericElementaryOwner {
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
struct PreparedNumericElementary {
    contract: Arc<ScalarCallContract>,
    operation: NumericElementaryOp,
}
impl PreparedScalarKernel for PreparedNumericElementary {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<NumericElementaryInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(NumericElementaryInstance {
            operation: self.operation,
        }))
    }
}

struct NumericElementaryInstance {
    operation: NumericElementaryOp,
}
impl ScalarKernelInstance for NumericElementaryInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::numeric_elementary::evaluate_numeric_elementary(self.operation, input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
pub(super) fn prepared_for_test(
    name: &str,
    sources: &[crate::FunctionValueType],
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    tests::prepared_for_test(name, sources)
}

impl NumericElementaryOwner {
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
                "numeric elementary has no environment dependencies and requires an exact proof scope"
                    .into(),
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
                "numeric elementary preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("numeric elementary preparation has a stale selected binding"),
        })?;
        let operation = self
            .family
            .operation(input.selected.argument_types.len())
            .ok_or_else(|| invalid("elementary preparation has an invalid selected arity"))?;
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedNumericElementary {
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

    fn owner(name: &str) -> NumericElementaryOwner {
        let (_, signatures) = super::super::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(candidate, _)| candidate == name)
            .expect("the actual elementary numeric registry entry");
        let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
            name,
            &signatures,
            FunctionKind::Scalar,
        )
        .unwrap();
        NumericElementaryOwner::new(name, declaration, resolver).unwrap()
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
        owner: &'a NumericElementaryOwner,
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

    fn source_profiles(name: &str) -> Vec<Vec<DataType>> {
        vec![if name == "log" {
            vec![DataType::Float64; 2]
        } else {
            Vec::new()
        }]
    }

    fn uses(count: usize) -> Vec<Option<ExpressionUseId>> {
        (0..count)
            .map(|index| Some(ExpressionUseId::new(42 + index as u32)))
            .collect()
    }

    pub(super) fn prepared_for_test(
        name: &str,
        sources: &[FunctionValueType],
    ) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
        if !is_installed(name) {
            return Err(FunctionSpecializationFailure::Binding(
                FunctionBindingError::UnknownFunction,
            ));
        }
        let owner = owner(name);
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
    fn actual_whole_catalogue_attaches_elementary_owners() {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        for name in names() {
            let owner = owner(name);
            let definition = catalog
                .definition_by_id(owner.declaration.function_id())
                .unwrap();
            assert!(definition.binding.as_ref().unwrap().pure.is_some());
            let declaration = definition.binding_declaration().unwrap();
            assert_eq!(declaration, owner.binding_declaration());
            assert_eq!(declaration.overloads().len(), 1);
            assert_eq!(owner.implementation_declarations().len(), 1);
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
        }
    }

    #[test]
    fn elementary_profiles_and_nullable_combinations_prepare_exact_fresh_frozen() {
        let parameters = SemanticParameters::try_new([]).unwrap();
        let mut count = 0;
        for name in names() {
            let owner = owner(name);
            for profile in source_profiles(name) {
                count += 1;
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
                    assert_eq!(
                        result.nullable(),
                        full_sources.iter().any(FunctionValueType::nullable)
                    );
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
                        std::mem::size_of::<NumericElementaryInstance>()
                    );
                    assert_eq!(
                        instance.retained_bytes(),
                        be.prepared().instance_retained_upper_bound()
                    );
                }
            }
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn elementary_exact_owner_rejects_stale_output_arity_overload_and_frozen_effects() {
        let parameters = SemanticParameters::try_new([]).unwrap();
        for name in names() {
            let owner = owner(name);
            let profile = source_profiles(name).remove(0);
            let arguments = profile
                .iter()
                .map(|ty| {
                    argument(FunctionValueType::from_legacy_declared_projection(
                        ty.clone(),
                        false,
                    ))
                })
                .collect::<Vec<_>>();
            let selected = Arc::new(
                owner
                    .resolve(request(&arguments), crate::binding_test_control())
                    .unwrap(),
            );
            let mut stale = (*selected).clone();
            stale.result_type =
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, true));
            assert!(
                owner
                    .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                    .is_err()
            );
            stale = (*selected).clone();
            stale.overload = self::owner(if *name == "pi" { "e" } else { "pi" })
                .declaration
                .overloads()[0]
                .identity
                .clone();
            assert!(
                owner
                    .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                    .is_err()
            );
            stale = (*selected).clone();
            stale.argument_types = if arguments.is_empty() {
                novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::new([
                    FunctionArgumentType::Value(FunctionValueType::new(DataType::Int8, false)),
                ]))
            } else {
                novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::new([]))
            };
            assert!(
                owner
                    .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                    .is_err()
            );
            let uses = uses(arguments.len());
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
    }

    #[test]
    fn elementary_refinement_requires_exact_environment_domain_function_and_preparation() {
        let owner = owner("log");
        let arguments = [
            argument(FunctionValueType::new(DataType::Float64, false)),
            argument(FunctionValueType::new(DataType::Float64, false)),
        ];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(arguments.len());
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        let mut foreign = input;
        foreign.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99));
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
        let other = FunctionId::try_new("builtin.scalar/sign/v1").unwrap();
        foreign = input;
        foreign.function_id = &other;
        assert!(matches!(
            owner.validate_and_refine(foreign, crate::binding_test_control()),
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
        let fresh = specialize_scalar(
            &owner,
            input,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .unwrap();
        let canonical = fresh.prepared().contract().clone();
        foreign = input;
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
    fn elementary_preparation_rejects_wrong_arity_and_nonprofile_already_coerced_inputs() {
        let source = FunctionValueType::new(DataType::Float64, false);
        for (name, inputs) in [
            ("log", vec![]),
            ("log", vec![source.clone(); 3]),
            ("sign", vec![]),
            ("sign", vec![source.clone(); 2]),
            ("e", vec![source.clone()]),
            ("pi", vec![source.clone()]),
        ] {
            assert!(prepared_for_test(name, &inputs).is_err());
        }
        let owner = owner("log");
        let arguments = [argument(source.clone()), argument(source)];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(arguments.len());
        for source in [
            FunctionValueType::new(DataType::Decimal128(18, 3), false),
            FunctionValueType::new(DataType::UInt64, false),
            FunctionValueType::try_from_legacy_declared_projection(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::Uuid,
            )
            .expect("valid temporary legacy function value declaration"),
            FunctionValueType::try_from_legacy_declared_projection(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .expect("valid temporary legacy function value declaration"),
        ] {
            for index in 0..2 {
                let mut wrong = arguments.clone();
                wrong[index] = argument(source.clone());
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
    }

    struct FailCompile(CompileControlError);
    impl PureCompileControl for FailCompile {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    #[test]
    fn elementary_owner_preserves_original_three_compile_failures() {
        let parameters = SemanticParameters::try_new([]).unwrap();
        for name in names() {
            let owner = owner(name);
            let profile = source_profiles(name).remove(0);
            let arguments = profile
                .iter()
                .map(|ty| {
                    argument(FunctionValueType::from_legacy_declared_projection(
                        ty.clone(),
                        false,
                    ))
                })
                .collect::<Vec<_>>();
            let selected = Arc::new(
                owner
                    .resolve(request(&arguments), crate::binding_test_control())
                    .unwrap(),
            );
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
}
