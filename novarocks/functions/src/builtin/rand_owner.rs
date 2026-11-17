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

//! Actual RAND/RANDOM binding, recipe, effects and preparation share one owner.

use std::sync::Arc;

use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl, ValueLogicalType,
};

use super::catalogue::BuiltinScalarResolver;
use super::rand::{RandInstance, SeedRecipe};
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionArgument, FunctionBindingDeclaration, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingResolver, FunctionBindingSelection,
    FunctionCatalogError, FunctionDefinition, FunctionEffectOwner, FunctionEffectOwnerError,
    FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError, FunctionKind,
    FunctionVisibility, FunctionVolatility, KernelFailure, PreparedScalarKernel,
    PureFunctionMetadataOwner, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    PureScalarImplementation, ScalarCallContract, ScalarKernelInstance,
};

pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Volatile,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::ScalarInstance,
        observable_effects: ObservableEffects {
            rng_sampling: true,
            warnings: false,
            controlled_wait: false,
        },
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(RandOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin RAND/RANDOM pure owner",
            value: error.to_string().into(),
        },
    )
}

struct RandOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl RandOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let expected = effects();
        if !matches!(name, "rand" | "random")
            || declaration.function_id().as_str() != format!("builtin.scalar/{name}/v1")
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 2
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin RAND/RANDOM pure declaration",
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
        })
    }

    fn call_effects(&self, recipe: SeedRecipe, scope: CallProofScope) -> CallEffects {
        let base = effects();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: if recipe == SeedRecipe::PerRow {
                FunctionInstanceState::None
            } else {
                base.instance_state
            },
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: scope,
        }
    }
}

/// Only the exact bound compile-time facts select a recipe. Runtime array
/// shape or broadcasting category never promotes a nonconstant seed.
fn seed_recipe(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SeedRecipe, FunctionBindingError> {
    match (request.logical_argument_count, request.arguments) {
        (0, []) => Ok(SeedRecipe::Unseeded),
        (1, [argument @ FunctionArgument::Value { value_type, .. }]) => {
            let Some(value) = super::catalogue::constant_source(Some(argument), work)? else {
                return Ok(SeedRecipe::PerRow);
            };
            if value_type.logical_type() != ValueLogicalType::Physical
                || value_type.data_type() != &DataType::Int64
            {
                return Err(FunctionBindingError::InvalidBinding(
                    "RAND/RANDOM constant seed differs from its exact signed BIGINT source".into(),
                ));
            }
            work.flush()?;
            if value.is_null_observed(CompilePhase::FunctionSpecialization, work.control())? {
                return Ok(SeedRecipe::Constant(0));
            }
            work.flush()?;
            let seed = value
                .int64_observed(CompilePhase::FunctionSpecialization, work.control())?
                .ok_or_else(|| {
                    FunctionBindingError::InvalidBinding(
                        "RAND/RANDOM non-NULL constant seed is not exact BIGINT".into(),
                    )
                })?;
            Ok(SeedRecipe::Constant(seed as u64))
        }
        _ => Err(FunctionBindingError::InvalidBinding(
            "RAND/RANDOM requires its exact zero-argument or one-value signature".into(),
        )),
    }
}

impl FunctionBindingResolver for RandOwner {
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
impl PureFunctionMetadataOwner for RandOwner {
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
impl FunctionEffectOwner for RandOwner {
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
impl PureScalarImplementation for RandOwner {
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
struct PreparedRand {
    contract: Arc<ScalarCallContract>,
    recipe: SeedRecipe,
}
impl PreparedScalarKernel for PreparedRand {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn instance_retained_upper_bound(&self) -> usize {
        RandInstance::retained_upper_bound()
    }
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(RandInstance::new(self.recipe)))
    }
}

#[cfg(test)]
fn test_owner(name: &str) -> Result<RandOwner, FunctionCatalogError> {
    let (_, signatures) = super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(candidate, _)| candidate == name)
        .expect("the actual RAND/RANDOM registry entry");
    let (declaration, resolver) =
        super::catalogue::scalar_definition_parts(name, &signatures, FunctionKind::Scalar)?;
    RandOwner::new(name, declaration, resolver)
}

/// Body tests use the real binding/refinement/preparation owner, with explicitly
/// authored source facts. This helper does not seal a subset catalogue or
/// certify the production compiler/evaluator migration.
#[cfg(test)]
pub(super) fn prepared_for_test(
    name: &str,
    source: Option<crate::FunctionValueType>,
    constant: Option<crate::ConstantValue>,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    let owner = test_owner(name).expect("actual RAND/RANDOM owner registration");
    let arguments = source
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant,
        })
        .into_iter()
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        logical_argument_count: arguments.len(),
        arguments: &arguments,
    };
    let selected = Arc::new(owner.resolve(request, crate::binding_test_control())?);
    let parameters = novarocks_type_contract::SemanticParameters::try_new([]).unwrap();
    let context = test_context();
    let uses = arguments
        .iter()
        .map(|_| Some(novarocks_type_contract::ExpressionUseId::new(2)))
        .collect::<Vec<_>>();
    let input = CallEffectInput {
        context,
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    };
    crate::specialize_scalar(
        &owner,
        input,
        selected.clone(),
        crate::ScopedExpressionEffects::pure_value(context),
        crate::binding_test_control(),
    )
    .map(crate::ScalarSpecialization::into_prepared)
}

#[cfg(test)]
fn test_context() -> novarocks_type_contract::ExpressionEffectContext {
    novarocks_type_contract::ExpressionEffectContext {
        use_id: novarocks_type_contract::ExpressionUseId::new(1),
        region: novarocks_type_contract::EvaluationRegionId::new(7),
        demand: novarocks_type_contract::EvaluationDemand::Value,
    }
}

impl RandOwner {
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
                && input.proof_scope != CallProofScope::Region(input.context.region)
        {
            return Err(FunctionBindingError::InvalidBinding(
                "RAND/RANDOM has no environment dependencies and requires an exact proof scope"
                    .into(),
            )
            .into());
        }
        work.flush().map_err(FunctionEffectOwnerError::Control)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
            other => FunctionEffectOwnerError::Owner(other),
        })?;
        work.step().map_err(FunctionEffectOwnerError::Control)?;
        let recipe = seed_recipe(input.request, &mut work)?;
        let result = self.call_effects(recipe, input.proof_scope);
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
            || contract.context() != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || !input.environment.is_empty()
        {
            return Err(invalid(
                "RAND/RANDOM preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("RAND/RANDOM preparation has a stale selected binding"),
        })?;
        work.step().map_err(compile_failure)?;
        work.flush().map_err(compile_failure)?;
        let recipe = seed_recipe(input.request, &mut work).map_err(|error| {
            if let Some(cause) = error.control_error() {
                compile_failure(cause)
            } else {
                invalid("RAND/RANDOM preparation has stale constant seed facts")
            }
        })?;
        if contract.effects() != &self.call_effects(recipe, input.proof_scope) {
            return Err(invalid(
                "RAND/RANDOM preparation has different exact recipe effects",
            ));
        }
        work.finish().map_err(compile_failure)?;
        Ok(Arc::new(PreparedRand { contract, recipe }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::catalogue::constant_binding_tests as cv;
    use super::*;
    use crate::ConstantValue;
    use crate::{
        FunctionArgumentType, FunctionResultType, FunctionSpecializationFailure, FunctionValueType,
        ScopedExpressionEffects, specialize_frozen_scalar, specialize_scalar,
    };
    use novarocks_type_contract::{
        CompileControlError, DecimalOverflowPolicy, EvaluationRegionId, ExpressionUseId,
        SemanticParameterKey, SemanticParameterRef, SemanticParameters,
    };

    fn args(constant: Option<ConstantValue>, nullable: bool) -> [FunctionArgument; 1] {
        [FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int64, nullable),
            constant,
        }]
    }
    fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            expected_result_type: None,
            arguments,
            logical_argument_count: arguments.len(),
        }
    }
    fn input<'a>(
        owner: &'a RandOwner,
        selected: &'a FunctionBindingSelection,
        arguments: &'a [FunctionArgument],
        parameters: &'a SemanticParameters,
        uses: &'a [Option<ExpressionUseId>],
    ) -> CallEffectInput<'a> {
        CallEffectInput {
            context: test_context(),
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

    #[test]
    fn actual_rand_random_four_records_match_whole_catalogue_attachments() {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let mut total = 0;
        for name in ["rand", "random"] {
            let owner = test_owner(name).unwrap();
            let definition = catalog
                .definition_by_id(owner.declaration.function_id())
                .unwrap();
            assert!(definition.binding.as_ref().unwrap().pure.is_some());
            assert_eq!(
                definition.binding_declaration(),
                Some(owner.binding_declaration())
            );
            assert_eq!(owner.implementation_declarations().len(), 2);
            for (overload, implementation) in owner
                .declaration
                .overloads()
                .iter()
                .zip(owner.implementation_declarations())
            {
                assert_eq!(implementation.overload, overload.identity);
                assert_eq!(implementation.abi, PureKernelAbi::ScalarV1);
                assert_eq!(
                    implementation.implementation.as_str(),
                    format!("builtin.scalar/{name}/selected-v1")
                );
                assert_eq!(overload.effects.as_ref(), Some(&effects()));
                total += 1;
            }
        }
        assert_eq!(total, 4);
    }

    #[test]
    fn exact_seed_recipes_keep_rng_observable_and_only_per_row_removes_state() {
        for name in ["rand", "random"] {
            for (arguments, expected) in [
                (Vec::new(), SeedRecipe::Unseeded),
                (
                    args(Some(cv::i64(-1, false)), false).to_vec(),
                    SeedRecipe::Constant(u64::MAX),
                ),
                (
                    args(Some(cv::null(DataType::Int64)), true).to_vec(),
                    SeedRecipe::Constant(0),
                ),
                (args(None, true).to_vec(), SeedRecipe::PerRow),
            ] {
                let owner = test_owner(name).unwrap();
                let selected = Arc::new(
                    owner
                        .resolve(request(&arguments), crate::binding_test_control())
                        .unwrap(),
                );
                let parameters = SemanticParameters::try_new([]).unwrap();
                let uses = vec![Some(ExpressionUseId::new(2)); arguments.len()];
                let input = input(&owner, &selected, &arguments, &parameters, &uses);
                let mut recipe_work = CompileCheckpoints::try_new(
                    crate::binding_test_control(),
                    CompilePhase::FunctionSpecialization,
                )
                .unwrap();
                assert_eq!(
                    seed_recipe(input.request, &mut recipe_work).unwrap(),
                    expected
                );
                let fresh = specialize_scalar(
                    &owner,
                    input,
                    selected.clone(),
                    ScopedExpressionEffects::pure_value(test_context()),
                    crate::binding_test_control(),
                )
                .unwrap();
                let frozen = fresh.prepared().contract().effects().clone();
                assert_eq!(
                    frozen,
                    owner.call_effects(expected, CallProofScope::Unconditional)
                );
                assert_eq!(frozen.value_stability, FunctionVolatility::Volatile);
                assert!(frozen.observable_effects.rng_sampling);
                assert!(frozen.environment.is_empty());
                assert_eq!(
                    frozen.instance_state,
                    if expected == SeedRecipe::PerRow {
                        FunctionInstanceState::None
                    } else {
                        FunctionInstanceState::ScalarInstance
                    }
                );
                let FunctionResultType::Scalar(result) = &selected.result_type else {
                    panic!("scalar result");
                };
                assert_eq!(result.data_type().clone(), DataType::Float64);
                assert_eq!(fresh.prepared().contract().result_type(), result);
                let be = specialize_frozen_scalar(
                    &owner,
                    input,
                    selected.clone(),
                    &frozen,
                    ScopedExpressionEffects::pure_value(test_context()),
                    crate::binding_test_control(),
                )
                .unwrap();
                assert_eq!(be.prepared().contract().effects(), &frozen);
                assert!(std::ptr::eq(
                    be.prepared().contract().selected(),
                    selected.as_ref()
                ));
                assert_eq!(
                    be.prepared().instance_retained_upper_bound(),
                    RandInstance::retained_upper_bound()
                );
            }
        }
    }

    #[test]
    fn private_preparation_copies_seed_facts_independently_of_effects() {
        use crate::{
            EvaluatedArgument, KernelEvaluationControl, ScalarEvaluationInstance, Selection,
        };
        use arrow_array::{ArrayRef, Float64Array, Int64Array};
        struct Evaluation;
        impl KernelEvaluationControl for Evaluation {
            fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
                Ok(())
            }
            fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
                panic!("RAND must not wait");
            }
        }
        // Independently captured from official rand 0.8.5 StdRng, not from
        // this body or its private recipe implementation.
        for (seed, expected_bits) in [
            (0, 0x3fe76547f659a58du64),
            (-1, 0x3faf4f30905c7ab0),
            (42, 0x3fe0d98eec6444e4),
        ] {
            let prepared = prepared_for_test(
                "rand",
                Some(FunctionValueType::new(DataType::Int64, false)),
                Some(cv::i64(seed, false)),
            )
            .unwrap();
            assert_eq!(
                prepared.contract().effects().instance_state,
                FunctionInstanceState::ScalarInstance
            );
            // All three seeds have the same effects. Their different retained
            // private values are established by actual computation instead.
            let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            let array: ArrayRef = Arc::new(Int64Array::from(vec![seed]));
            let arguments = [EvaluatedArgument::Scalar(&array)];
            let output = instance
                .evaluate(Selection::all(1), &arguments, &Evaluation)
                .unwrap();
            let values = output
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(values.value(0).to_bits(), expected_bits);
        }
    }

    #[test]
    fn wrong_frozen_recipe_state_and_stale_result_fail_before_a_runtime_instance() {
        let owner = test_owner("rand").unwrap();
        let arguments = args(Some(cv::i64(7, false)), false);
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(2))];
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        let foreign = owner.call_effects(SeedRecipe::PerRow, CallProofScope::Unconditional);
        assert!(matches!(
            specialize_frozen_scalar(
                &owner,
                input,
                selected.clone(),
                &foreign,
                ScopedExpressionEffects::pure_value(test_context()),
                crate::binding_test_control()
            ),
            Err(FunctionSpecializationFailure::InvalidInput(
                "frozen call effects differ from exact local refinement"
            ))
        ));
        let mut stale = (*selected).clone();
        stale.result_type =
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, false));
        assert!(
            owner
                .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                .is_err()
        );
    }

    #[test]
    fn wrong_typed_constant_seed_and_nullability_are_explicit_refusals() {
        for constant in [
            cv::boolean(true),
            cv::u64(7),
            cv::f64(1.0),
            cv::utf8("7", false),
        ] {
            assert!(matches!(
                prepared_for_test(
                    "rand",
                    Some(FunctionValueType::new(DataType::Int64, false)),
                    Some(constant)
                ),
                Err(FunctionSpecializationFailure::Binding(
                    FunctionBindingError::InvalidBinding(_)
                ))
            ));
        }
        assert!(matches!(
            prepared_for_test(
                "rand",
                Some(FunctionValueType::new(DataType::Int64, false)),
                Some(cv::null(DataType::Int64))
            ),
            Err(FunctionSpecializationFailure::Binding(
                FunctionBindingError::InvalidBinding(_)
            ))
        ));
        assert!(
            prepared_for_test(
                "random",
                Some(
                    FunctionValueType::try_with_logical_type(
                        DataType::FixedSizeBinary(16),
                        false,
                        ValueLogicalType::Uuid
                    )
                    .unwrap()
                ),
                None
            )
            .is_err()
        );
    }

    #[test]
    fn seeded_binding_coercion_remains_but_preparation_requires_the_actual_coerced_value() {
        let owner = test_owner("rand").unwrap();
        let narrow = [FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int32, false),
            constant: Some(cv::i32(7, false)),
        }];
        let selected = Arc::new(
            owner
                .resolve(request(&narrow), crate::binding_test_control())
                .unwrap(),
        );
        assert_eq!(
            selected.argument_types.as_ref(),
            &[FunctionArgumentType::Value(FunctionValueType::new(
                DataType::Int64,
                false
            ))]
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(2))];
        let raw = input(&owner, &selected, &narrow, &parameters, &uses);
        assert!(matches!(
            specialize_scalar(
                &owner,
                raw,
                selected.clone(),
                ScopedExpressionEffects::pure_value(test_context()),
                crate::binding_test_control()
            ),
            Err(FunctionSpecializationFailure::InvalidInput(
                "call value argument differs from its already-coerced selected domain"
            ))
        ));
        let coerced = args(Some(cv::i64(7, false)), false);
        owner
            .validate_selected(&selected, request(&coerced), crate::binding_test_control())
            .unwrap();
        let checked = input(&owner, &selected, &coerced, &parameters, &uses);
        assert!(
            specialize_scalar(
                &owner,
                checked,
                selected.clone(),
                ScopedExpressionEffects::pure_value(test_context()),
                crate::binding_test_control()
            )
            .is_ok()
        );
    }

    #[test]
    fn foreign_domain_environment_and_function_identity_are_not_authorized() {
        let owner = test_owner("rand").unwrap();
        let arguments = args(None, true);
        let selected = owner
            .resolve(request(&arguments), crate::binding_test_control())
            .unwrap();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(2))];
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
            id: novarocks_type_contract::SemanticParameterId::new(0),
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
        let other = FunctionId::try_new("builtin.scalar/random/v1").unwrap();
        foreign = input;
        foreign.function_id = &other;
        assert!(matches!(
            owner.validate_and_refine(foreign, crate::binding_test_control()),
            Err(FunctionEffectOwnerError::Owner(
                FunctionBindingError::UnknownFunction
            ))
        ));
    }

    struct Fail(CompileControlError);
    impl PureCompileControl for Fail {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    #[test]
    fn exact_rand_owner_keeps_all_original_control_categories() {
        let owner = test_owner("rand").unwrap();
        let arguments = args(None, true);
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(2))];
        let input = input(&owner, &selected, &arguments, &parameters, &uses);
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert!(
                matches!(owner.resolve(request(&arguments), &Fail(error.clone())), Err(FunctionBindingError::Control(actual)) if actual == error)
            );
            assert!(
                matches!(owner.validate_and_refine(input, &Fail(error.clone())), Err(FunctionEffectOwnerError::Control(actual)) if actual == error)
            );
            assert!(
                matches!(specialize_scalar(&owner, input, selected.clone(), ScopedExpressionEffects::pure_value(test_context()), &Fail(error.clone())), Err(FunctionSpecializationFailure::Control(actual)) if actual == error)
            );
        }
    }
}
