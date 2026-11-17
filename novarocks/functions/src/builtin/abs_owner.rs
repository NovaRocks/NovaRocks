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

//! The real ABS binding, effects and pure preparation have one immutable owner.

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

const FUNCTION: &str = "builtin.scalar/abs/v1";
const IMPLEMENTATION: &str = "builtin.scalar/abs/selected-v1";

/// These facts describe the eight actual selected signatures. Integer inputs
/// widen before ABS; valid decimal values fit their declared precision;
/// LARGEINT intentionally preserves its existing wrapping minimum contract.
/// Stale bindings and malformed carriers are outer preparation/input failures.
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
    let owner = Arc::new(AbsOwner::new(declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar("abs", FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin ABS pure owner",
            value: error.to_string().into(),
        },
    )
}

struct AbsOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl AbsOwner {
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
                subject: "builtin ABS pure declaration",
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

impl FunctionBindingResolver for AbsOwner {
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

impl PureFunctionMetadataOwner for AbsOwner {
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

impl FunctionEffectOwner for AbsOwner {
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
        self.refine_core(input, control, || {
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
        self.refine_core(input, control, || {
            checked.require_fixed_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl PureScalarImplementation for AbsOwner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        self.prepare_core(input, contract, control, || {
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
        self.prepare_core(input, contract, control, || {
            checked.require_fixed_author(self.resolver.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl AbsOwner {
    fn refine_core(
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
                && input.proof_scope != CallProofScope::Region(input.context.region)
        {
            return Err(FunctionBindingError::InvalidBinding(
                "ABS has no environment dependencies and requires an exact proof scope".into(),
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
    fn prepare_core(
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
            || contract.context() != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &self.call_effects(input.proof_scope)
            || !input.environment.is_empty()
        {
            return Err(invalid(
                "ABS preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("ABS preparation has a stale selected binding"),
        })?;
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedAbs { contract }))
    }
}

#[derive(Debug)]
struct PreparedAbs {
    contract: Arc<ScalarCallContract>,
}
impl PreparedScalarKernel for PreparedAbs {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        0
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(AbsInstance))
    }
}

struct AbsInstance;
impl ScalarKernelInstance for AbsInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::abs::evaluate_abs(input, control)
    }

    fn retained_bytes(&self) -> usize {
        0
    }
}

#[cfg(test)]
pub(super) fn prepared_for_test(source: crate::FunctionValueType) -> Arc<dyn PreparedScalarKernel> {
    tests::prepared_for_test(source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int8Array, Int16Array};
    use arrow_schema::DataType;
    use novarocks_type_contract::{
        CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
        ExpressionEffectContext, ExpressionUseId, SemanticParameters, ValueLogicalType,
    };
    use std::time::Duration;

    use crate::{
        EvaluatedArgument, FunctionArgument, FunctionArgumentType, FunctionResultType,
        FunctionSpecializationFailure, FunctionValueType, ScalarEvaluationInstance,
        ScopedExpressionEffects, Selection, specialize_frozen_scalar, specialize_scalar,
    };

    fn owner() -> AbsOwner {
        let (_, signatures) = super::super::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(name, _)| name == "abs")
            .expect("the actual ABS registry entry");
        let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
            "abs",
            &signatures,
            FunctionKind::Scalar,
        )
        .unwrap();
        AbsOwner::new(declaration, resolver).unwrap()
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
            logical_argument_count: 1,
        }
    }

    fn input<'a>(
        owner: &'a AbsOwner,
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

    #[test]
    fn actual_abs_owner_manifest_matches_the_production_binding_author() {
        let owner = owner();
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let definition = catalog
            .definition_by_id(owner.declaration.function_id())
            .unwrap();
        assert!(
            definition.binding.as_ref().unwrap().pure.is_some(),
            "the whole production catalogue must attach the actual ABS CPU owner"
        );
        let declaration = definition.binding_declaration().unwrap();
        assert_eq!(declaration, owner.binding_declaration());
        assert_eq!(owner.implementation_declarations().len(), 8);
        for (overload, implementation) in declaration
            .overloads()
            .iter()
            .zip(owner.implementation_declarations())
        {
            assert_eq!(implementation.overload, overload.identity);
            assert_eq!(implementation.abi, PureKernelAbi::ScalarV1);
            assert_eq!(implementation.implementation.as_str(), IMPLEMENTATION);
            assert_eq!(overload.effects.as_ref(), Some(&effects()));
        }
    }

    #[test]
    fn all_actual_abs_selected_profiles_prepare_fresh_and_frozen_without_reselection() {
        let owner = owner();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
        for (source, target) in [
            (DataType::Int8, DataType::Int16),
            (DataType::Int16, DataType::Int32),
            (DataType::Int32, DataType::Int64),
            (DataType::Int64, DataType::FixedSizeBinary(16)),
            (DataType::FixedSizeBinary(16), DataType::FixedSizeBinary(16)),
            (DataType::Float32, DataType::Float32),
            (DataType::Float64, DataType::Float64),
            (DataType::Decimal128(18, 3), DataType::Decimal128(18, 3)),
        ] {
            for nullable in [false, true] {
                // The LARGEINT fixture originates in the actual LARGEINT
                // profile, not in an arbitrary Fixed16 carrier.
                let source_logical = if source == DataType::FixedSizeBinary(16) {
                    ValueLogicalType::LargeInt
                } else {
                    ValueLogicalType::Physical
                };
                let arguments = [argument(
                    FunctionValueType::try_from_legacy_declared_projection(
                        source.clone(),
                        nullable,
                        source_logical,
                    )
                    .expect("valid temporary legacy function value declaration"),
                )];
                let selected = Arc::new(
                    owner
                        .resolve(request(&arguments), crate::binding_test_control())
                        .unwrap(),
                );
                assert_eq!(
                    selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(
                        FunctionValueType::try_from_legacy_declared_projection(
                            source.clone(),
                            nullable,
                            source_logical
                        )
                        .expect("valid temporary legacy function value declaration")
                    )]
                );
                let FunctionResultType::Scalar(result) = &selected.result_type else {
                    panic!("ABS must have a scalar result");
                };
                assert_eq!(result.data_type().clone(), target);
                assert_eq!(result.nullable(), nullable);
                assert_eq!(
                    result.logical_type(),
                    if target == DataType::FixedSizeBinary(16) {
                        ValueLogicalType::LargeInt
                    } else {
                        ValueLogicalType::Physical
                    }
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
                assert!(std::ptr::eq(
                    fresh.prepared().contract().selected(),
                    selected.as_ref()
                ));
                let frozen = fresh.prepared().contract().effects().clone();
                let be = specialize_frozen_scalar(
                    &owner,
                    input,
                    selected.clone(),
                    &frozen,
                    ScopedExpressionEffects::pure_value(context()),
                    crate::binding_test_control(),
                )
                .unwrap();
                assert_eq!(be.prepared().contract().effects(), &frozen);
                assert!(std::ptr::eq(
                    be.prepared().contract().selected(),
                    selected.as_ref()
                ));
                assert_eq!(be.prepared().instance_retained_upper_bound(), 0);
            }
        }
    }

    #[test]
    fn actual_abs_owner_rejects_opaque_fixed16_and_stale_output_profiles() {
        let owner = owner();
        for logical_type in [ValueLogicalType::Physical, ValueLogicalType::Uuid] {
            let arguments = [argument(
                FunctionValueType::try_from_legacy_declared_projection(
                    DataType::FixedSizeBinary(16),
                    false,
                    logical_type,
                )
                .expect("valid temporary legacy function value declaration"),
            )];
            assert!(
                owner
                    .resolve(request(&arguments), crate::binding_test_control())
                    .is_err()
            );
        }
        let arguments = [argument(FunctionValueType::new(DataType::Int8, false))];
        let mut selected = owner
            .resolve(request(&arguments), crate::binding_test_control())
            .unwrap();
        selected.result_type =
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Int8, false));
        assert!(
            owner
                .validate_selected(
                    &selected,
                    request(&arguments),
                    crate::binding_test_control()
                )
                .is_err()
        );
    }

    #[test]
    fn actual_abs_owner_refines_no_environment_and_rejects_foreign_frozen_facts() {
        let owner = owner();
        let arguments = [argument(FunctionValueType::new(DataType::Int8, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
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
        let mut foreign_input = input;
        foreign_input.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99));
        assert!(matches!(
            owner.validate_and_refine(foreign_input, crate::binding_test_control()),
            Err(FunctionEffectOwnerError::Owner(
                FunctionBindingError::InvalidBinding(_)
            ))
        ));
    }

    struct FailCompile(CompileControlError);
    impl PureCompileControl for FailCompile {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }

    #[test]
    fn actual_abs_owner_keeps_original_compile_refusal_typed() {
        let owner = owner();
        let arguments = [argument(FunctionValueType::new(DataType::Int8, false))];
        let selected = owner
            .resolve(request(&arguments), crate::binding_test_control())
            .unwrap();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert!(
                matches!(owner.validate_and_refine(input, &FailCompile(error.clone())),
                Err(FunctionEffectOwnerError::Control(actual)) if actual == error)
            );
            assert!(
                matches!(owner.resolve(request(&arguments), &FailCompile(error.clone())),
                Err(FunctionBindingError::Control(actual)) if actual == error)
            );
        }
    }

    struct EvaluationControl;
    impl KernelEvaluationControl for EvaluationControl {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("ABS must not invoke controlled wait");
        }
    }

    pub(super) fn prepared_for_test(source: FunctionValueType) -> Arc<dyn PreparedScalarKernel> {
        let owner = owner();
        let arguments = [argument(source)];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
        specialize_scalar(
            &owner,
            input(&owner, &selected, &arguments, &parameters, &uses),
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .unwrap()
        .into_prepared()
    }

    #[test]
    fn actual_prepared_abs_executes_sparse_and_independent_full_instances() {
        let prepared = prepared_for_test(FunctionValueType::new(DataType::Int8, false));
        let mut fe = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
        let mut be = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let column: ArrayRef = Arc::new(Int8Array::from(vec![i8::MIN, 5, -7]));
        let evaluated = [EvaluatedArgument::Column(&column)];
        let rows = [0, 2];
        let sparse = fe
            .evaluate(
                Selection::try_sparse(3, &rows).unwrap(),
                &evaluated,
                &EvaluationControl,
            )
            .unwrap();
        assert!(sparse.errors().is_empty());
        let values = sparse
            .values()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[128, 7]);
        let full = be
            .evaluate(Selection::all(3), &evaluated, &EvaluationControl)
            .unwrap();
        let values = full.values().as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(values.values().as_ref(), &[128, 5, 7]);
        assert_eq!(
            fe.contract().result_type().data_type().clone(),
            DataType::Int16
        );
        assert_eq!(fe.retained_bytes().unwrap(), fe.retained_upper_bound());
    }
}
