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

//! Each installed string measurement shares its binding, effects and CPU preparation owner.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinScalarResolver;
use super::string_measure::StringMeasureOp;
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

/// Only the three actually installed Utf8 measurement profiles have owners.
/// This mapping is consumed at registration; instances retain the exact operation.
pub(super) fn operation(name: &str) -> Option<StringMeasureOp> {
    match name {
        "ascii" => Some(StringMeasureOp::Ascii),
        "length" => Some(StringMeasureOp::Bytes),
        "char_length" => Some(StringMeasureOp::Characters),
        _ => None,
    }
}

/// Measurements are strict and total for the bounded Utf8 offset domain.
/// The immutable operation discriminator is not mutable instance state.
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
    let owner = Arc::new(StringMeasureOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin string measurement pure owner",
            value: error.to_string().into(),
        },
    )
}

struct StringMeasureOwner {
    operation: StringMeasureOp,
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl StringMeasureOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let operation =
            operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                subject: "uninstalled string measurement",
                value: name.into(),
            })?;
        let function = format!("builtin.scalar/{name}/v1");
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 1
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin string measurement pure declaration",
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
            operation,
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

impl FunctionBindingResolver for StringMeasureOwner {
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

impl PureFunctionMetadataOwner for StringMeasureOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        crate::FunctionBindingResolverOwner::fixed_scalar(Arc::clone(&self.resolver))
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
    fn admit_selected_profile_observed(
        &self,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        self.declaration.effect_declaration(&selected.overload)?;
        let unavailable =
            || FunctionBindingError::UnavailableImplementation(selected.overload.clone());
        let result = (|| {
            let source = super::string_measure::check_source(
                &selected.argument_types,
                logical_argument_count,
            )
            .map_err(|_| unavailable())?;
            work.step()?;
            let crate::FunctionResultType::Scalar(target) = &selected.result_type else {
                return Err(unavailable());
            };
            super::string_measure::check_types(self.operation, source, target)
                .map_err(|_| unavailable())?;
            Ok(())
        })();
        if matches!(&result, Err(FunctionBindingError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
}

impl FunctionEffectOwner for StringMeasureOwner {
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

impl PureScalarImplementation for StringMeasureOwner {
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
struct PreparedStringMeasure {
    operation: StringMeasureOp,
    contract: Arc<ScalarCallContract>,
}
impl PreparedScalarKernel for PreparedStringMeasure {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<StringMeasureInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(StringMeasureInstance {
            operation: self.operation,
        }))
    }
}

struct StringMeasureInstance {
    operation: StringMeasureOp,
}
impl ScalarKernelInstance for StringMeasureInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::string_measure::evaluate_string_measure(self.operation, input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
pub(super) fn prepared_for_test(
    name: &str,
    source: &crate::FunctionValueType,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    prepared_for_test_with_policy(
        name,
        source,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    )
}

#[cfg(test)]
pub(super) fn prepared_for_test_with_policy(
    name: &str,
    source: &crate::FunctionValueType,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    tests::prepared_for_test_with_policy(name, source, policy)
}

impl StringMeasureOwner {
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
                "string measurement has no environment dependencies and requires an exact proof scope".into(),
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
                "string measurement preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("string measurement preparation has a stale selected binding"),
        })?;
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedStringMeasure {
            contract,
            operation: self.operation,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EngineFunctionCatalogBuilder, FunctionArgument, FunctionArgumentType, FunctionResultType,
        FunctionSpecializationFailure, FunctionValueType, PureCatalogError,
        ScopedExpressionEffects, specialize_frozen_scalar, specialize_scalar,
    };
    use arrow_schema::DataType;
    use novarocks_type_contract::{
        CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
        ExpressionEffectContext, ExpressionUseId, SemanticParameterId, SemanticParameterKey,
        SemanticParameterRef, SemanticParameters, ValueLogicalType,
    };

    const NAMES: [&str; 3] = ["ascii", "char_length", "length"];

    fn owner(name: &str) -> StringMeasureOwner {
        let (_, signatures) = super::super::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(candidate, _)| candidate == name)
            .unwrap();
        let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
            name,
            &signatures,
            FunctionKind::Scalar,
        )
        .unwrap();
        StringMeasureOwner::new(name, declaration, resolver).unwrap()
    }
    fn context() -> ExpressionEffectContext {
        ExpressionEffectContext {
            use_id: ExpressionUseId::new(61),
            region: EvaluationRegionId::new(9),
            demand: EvaluationDemand::Value,
        }
    }
    fn argument(source: FunctionValueType) -> FunctionArgument {
        FunctionArgument::Value {
            value_type: source,
            constant: None,
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
        owner: &'a StringMeasureOwner,
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
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            proof_scope: CallProofScope::Unconditional,
        }
    }
    pub(super) fn prepared_for_test_with_policy(
        name: &str,
        source: &FunctionValueType,
        policy: DecimalOverflowPolicy,
    ) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
        let owner = owner(name);
        let arguments = [argument(source.clone())];
        let selected = Arc::new(owner.resolve(request(&arguments), crate::binding_test_control())?);
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(62))];
        let mut exact = input(&owner, &selected, &arguments, &parameters, &uses);
        exact.decimal_overflow_policy = policy;
        specialize_scalar(
            &owner,
            exact,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .map(|specialization| specialization.into_prepared())
    }

    #[test]
    fn whole_catalogue_attaches_three_exact_measurements_without_sealing_other_families() {
        let mut builder = EngineFunctionCatalogBuilder::new();
        super::super::catalogue::contribute_builtin_functions(&mut builder).unwrap();
        for name in NAMES {
            let owner = owner(name);
            let definition = builder.definition(name, FunctionKind::Scalar).unwrap();
            assert!(definition.binding.as_ref().unwrap().pure.is_some());
            assert_eq!(
                definition.binding_declaration().unwrap(),
                owner.binding_declaration()
            );
            let [implementation] = owner.implementation_declarations() else {
                panic!("one actual measurement profile");
            };
            assert_eq!(
                implementation.overload,
                owner.declaration.overloads()[0].identity
            );
            assert_eq!(
                implementation.implementation.as_str(),
                format!("builtin.scalar/{name}/selected-v1")
            );
            assert_eq!(implementation.abi, PureKernelAbi::ScalarV1);
            assert_eq!(
                owner.declaration.overloads()[0].effects.as_ref(),
                Some(&effects())
            );
        }
        for name in [
            "character_length",
            "octet_length",
            "bit_length",
            "ord",
            "crc32",
        ] {
            assert!(operation(name).is_none());
        }
        assert!(matches!(
            builder.seal_pure(std::iter::empty()),
            Err(PureCatalogError::MissingOwner(_)
                | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
        ));
    }

    #[test]
    fn measurements_preserve_authored_nullable_and_fresh_frozen_canonical_contracts() {
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(62))];
        for name in NAMES {
            let owner = owner(name);
            for nullable in [false, true] {
                let source = FunctionValueType::new(DataType::Utf8, nullable);
                let arguments = [argument(source.clone())];
                let selected = Arc::new(
                    owner
                        .resolve(request(&arguments), crate::binding_test_control())
                        .unwrap(),
                );
                assert_eq!(
                    selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(source)]
                );
                assert_eq!(
                    selected.result_type,
                    FunctionResultType::Scalar(FunctionValueType::new(
                        DataType::Int32,
                        name == "ascii" || nullable
                    ))
                );
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    let mut exact = input(&owner, &selected, &arguments, &parameters, &uses);
                    exact.decimal_overflow_policy = policy;
                    let fresh = specialize_scalar(
                        &owner,
                        exact,
                        selected.clone(),
                        ScopedExpressionEffects::pure_value(context()),
                        crate::binding_test_control(),
                    )
                    .unwrap();
                    let canonical = fresh.prepared().contract().clone();
                    let direct = owner
                        .prepare_scalar(exact, canonical.clone(), crate::binding_test_control())
                        .unwrap();
                    assert!(Arc::ptr_eq(direct.contract(), &canonical));
                    assert!(std::ptr::eq(canonical.selected(), selected.as_ref()));
                    let facts = canonical.effects().clone();
                    let frozen = specialize_frozen_scalar(
                        &owner,
                        exact,
                        selected.clone(),
                        &facts,
                        ScopedExpressionEffects::pure_value(context()),
                        crate::binding_test_control(),
                    )
                    .unwrap();
                    assert_eq!(frozen.prepared().contract().effects(), &facts);
                    assert_eq!(
                        frozen.prepared().contract().decimal_overflow_policy(),
                        policy
                    );
                    assert!(std::ptr::eq(
                        frozen.prepared().contract().selected(),
                        selected.as_ref()
                    ));
                    assert_eq!(
                        frozen.prepared().instance_retained_upper_bound(),
                        std::mem::size_of::<StringMeasureInstance>()
                    );
                    assert_eq!(
                        frozen
                            .prepared()
                            .create_instance()
                            .unwrap()
                            .retained_bytes(),
                        std::mem::size_of::<StringMeasureInstance>()
                    );
                    assert_eq!(facts.instance_state, FunctionInstanceState::None);
                }
            }
        }
    }

    #[test]
    fn measurement_binding_requires_actual_coercion_to_the_single_utf8_profile() {
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(62))];
        for name in NAMES {
            let owner = owner(name);
            for source in [
                FunctionValueType::new(DataType::LargeUtf8, true),
                FunctionValueType::new(DataType::Null, true),
            ] {
                let raw = [argument(source)];
                let selected = Arc::new(
                    owner
                        .resolve(request(&raw), crate::binding_test_control())
                        .unwrap(),
                );
                let canonical_source = FunctionValueType::new(DataType::Utf8, true);
                assert_eq!(
                    selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(canonical_source.clone())]
                );
                owner
                    .validate_selected(&selected, request(&raw), crate::binding_test_control())
                    .unwrap();
                assert!(matches!(
                    specialize_scalar(
                        &owner,
                        input(&owner, &selected, &raw, &parameters, &uses),
                        selected.clone(),
                        ScopedExpressionEffects::pure_value(context()),
                        crate::binding_test_control()
                    ),
                    Err(FunctionSpecializationFailure::InvalidInput(_))
                ));
                assert!(prepared_for_test(name, &canonical_source).is_ok());
            }
            for source in [
                FunctionValueType::new(DataType::Binary, true),
                FunctionValueType::new(DataType::LargeBinary, true),
                FunctionValueType::new(DataType::Int64, true),
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    true,
                    ValueLogicalType::Json,
                )
                .unwrap(),
                FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    true,
                    ValueLogicalType::Uuid,
                )
                .unwrap(),
            ] {
                assert!(
                    owner
                        .resolve(request(&[argument(source)]), crate::binding_test_control())
                        .is_err()
                );
            }
            assert!(
                owner
                    .resolve(request(&[]), crate::binding_test_control())
                    .is_err()
            );
            let two = [
                argument(FunctionValueType::new(DataType::Utf8, true)),
                argument(FunctionValueType::new(DataType::Utf8, true)),
            ];
            assert!(
                owner
                    .resolve(request(&two), crate::binding_test_control())
                    .is_err()
            );
        }
    }

    #[test]
    fn measurement_owner_refuses_stale_result_foreign_identity_and_effects() {
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(62))];
        for name in NAMES {
            let owner = owner(name);
            let arguments = [argument(FunctionValueType::new(DataType::Utf8, false))];
            let selected = Arc::new(
                owner
                    .resolve(request(&arguments), crate::binding_test_control())
                    .unwrap(),
            );
            for result in [
                FunctionValueType::new(DataType::Int64, true),
                FunctionValueType::new(DataType::Int32, name != "ascii"),
            ] {
                let mut wrong = (*selected).clone();
                wrong.result_type = FunctionResultType::Scalar(result);
                assert!(
                    owner
                        .validate_selected(
                            &wrong,
                            request(&arguments),
                            crate::binding_test_control()
                        )
                        .is_err()
                );
            }
            let exact = input(&owner, &selected, &arguments, &parameters, &uses);
            let facts = owner
                .validate_and_refine(exact, crate::binding_test_control())
                .unwrap();
            for field in 0..4 {
                let mut wrong = facts.clone();
                match field {
                    0 => wrong.own_row_error = FunctionIntrinsicRowError::MayRaise,
                    1 => wrong.null_behavior = FunctionNullBehavior::CalledOnNull,
                    2 => wrong.instance_state = FunctionInstanceState::ScalarInstance,
                    _ => wrong.observable_effects.warnings = true,
                }
                assert!(
                    specialize_frozen_scalar(
                        &owner,
                        exact,
                        selected.clone(),
                        &wrong,
                        ScopedExpressionEffects::pure_value(context()),
                        crate::binding_test_control()
                    )
                    .is_err()
                );
            }
            let foreign_owner = owner_for_foreign_name(name);
            let mut foreign = exact;
            foreign.function_id = foreign_owner.declaration.function_id();
            assert!(matches!(
                owner.validate_and_refine(foreign, crate::binding_test_control()),
                Err(FunctionEffectOwnerError::Owner(
                    FunctionBindingError::UnknownFunction
                ))
            ));
        }
    }
    fn owner_for_foreign_name(name: &str) -> StringMeasureOwner {
        owner(if name == "ascii" { "length" } else { "ascii" })
    }

    #[test]
    fn measurement_preparation_refuses_foreign_pointer_context_policy_environment_and_scope() {
        let owner = owner("length");
        let arguments = [argument(FunctionValueType::new(DataType::Utf8, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(62))];
        let exact = input(&owner, &selected, &arguments, &parameters, &uses);
        let fresh = specialize_scalar(
            &owner,
            exact,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .unwrap();
        let canonical = fresh.prepared().contract().clone();
        let mut wrong = exact;
        wrong.context = novarocks_type_contract::ExpressionEffectContext {
            use_id: ExpressionUseId::new(99),
            ..wrong.context.expression().unwrap()
        }
        .into();
        assert!(
            owner
                .prepare_scalar(wrong, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        wrong = exact;
        wrong.decimal_overflow_policy = DecimalOverflowPolicy::ReportError;
        assert!(
            owner
                .prepare_scalar(wrong, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        let foreign = (*selected).clone();
        wrong = exact;
        wrong.selected = &foreign;
        assert!(
            owner
                .prepare_scalar(wrong, canonical, crate::binding_test_control())
                .is_err()
        );
        wrong = exact;
        wrong.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99));
        assert!(
            owner
                .validate_and_refine(wrong, crate::binding_test_control())
                .is_err()
        );
        let environment = [SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::StatementStartUtc,
        }];
        wrong = exact;
        wrong.environment = &environment;
        assert!(
            owner
                .validate_and_refine(wrong, crate::binding_test_control())
                .is_err()
        );
        wrong = exact;
        wrong.proof_scope = CallProofScope::Region(context().region);
        assert_eq!(
            owner
                .validate_and_refine(wrong, crate::binding_test_control())
                .unwrap()
                .proof_scope,
            wrong.proof_scope
        );
    }

    struct FailCompile {
        error: CompileControlError,
        positive: bool,
    }
    impl PureCompileControl for FailCompile {
        fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
            if !self.positive || work > 0 {
                Err(self.error.clone())
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn measurement_owner_preserves_three_typed_entry_and_actual_work_refusals() {
        let owner = owner("ascii");
        let arguments = [argument(FunctionValueType::new(DataType::Utf8, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(62))];
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
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for positive in [false, true] {
                let control = FailCompile {
                    error: error.clone(),
                    positive,
                };
                assert!(
                    matches!(owner.resolve(request(&arguments), &control), Err(FunctionBindingError::Control(actual)) if actual == error)
                );
                assert!(
                    matches!(owner.validate_and_refine(exact, &control), Err(FunctionEffectOwnerError::Control(actual)) if actual == error)
                );
                assert!(
                    matches!(specialize_scalar(&owner, exact, selected.clone(), ScopedExpressionEffects::pure_value(context()), &control), Err(FunctionSpecializationFailure::Control(actual)) if actual == error)
                );
                assert!(
                    matches!(specialize_frozen_scalar(&owner, exact, selected.clone(), facts, ScopedExpressionEffects::pure_value(context()), &control), Err(FunctionSpecializationFailure::Control(actual)) if actual == error)
                );
                let expected = KernelFailure::CompileControl(error.clone());
                assert!(
                    matches!(owner.prepare_scalar(exact, fresh.prepared().contract().clone(), &control), Err(actual) if std::mem::discriminant(&actual) == std::mem::discriminant(&expected))
                );
            }
        }
    }
}
