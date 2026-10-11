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

//! The installed TRUNCATE bindings, effects and preparation share each exact owner.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinDynamicScalarResolver;
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

#[cfg(test)]
use novarocks_type_contract::DecimalOverflowPolicy;

use super::truncate::TruncateOp;

const FUNCTION: &str = "builtin.scalar/truncate/v1";
const OVERLOAD: &str = "builtin.scalar/truncate/dynamic-v1";
const IMPLEMENTATION: &str = "builtin.scalar/truncate/selected-v1";

/// The original safe output cast returns successful NULL on overflow.
/// Frozen overflow policy remains part of the exact call contract.
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
    resolver: BuiltinDynamicScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(TruncateOwner::new(declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar("truncate", FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin truncate pure owner",
            value: error.to_string().into(),
        },
    )
}

struct TruncateOwner {
    resolver: BuiltinDynamicScalarResolver,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl TruncateOwner {
    fn new(
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinDynamicScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let expected = effects();
        if declaration.function_id().as_str() != FUNCTION
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 1
            || declaration.overloads().iter().any(|overload| {
                overload.identity.as_str() != OVERLOAD
                    || overload.aggregate.is_some()
                    || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin truncate pure declaration",
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
            resolver,
            declaration,
            implementations,
        })
    }

    fn call_effects(&self, input: CallEffectInput<'_>) -> CallEffects {
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
            proof_scope: input.proof_scope,
        }
    }
}

impl FunctionBindingResolver for TruncateOwner {
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

impl PureFunctionMetadataOwner for TruncateOwner {
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for TruncateOwner {
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
                "truncate has no environment dependencies and requires an exact proof scope".into(),
            )
            .into());
        }
        work.flush().map_err(FunctionEffectOwnerError::Control)?;
        self.validate_selected(input.selected, input.request, control)
            .map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
        let result = self.call_effects(input);
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(result)
    }
}

impl PureScalarImplementation for TruncateOwner {
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
            || contract.effects() != &self.call_effects(input)
            || !input.environment.is_empty()
        {
            return Err(invalid(
                "truncate preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        self.validate_selected(input.selected, input.request, control)
            .map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("truncate preparation has a stale selected binding"),
            })?;
        let operation = match input.selected.argument_types.len() {
            1 => TruncateOp::Unary,
            2 => TruncateOp::Binary,
            _ => {
                return Err(invalid(
                    "TRUNCATE preparation has an invalid selected arity",
                ));
            }
        };
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedTruncate {
            contract,
            operation,
        }))
    }
}

#[derive(Debug)]
struct PreparedTruncate {
    contract: Arc<ScalarCallContract>,
    operation: TruncateOp,
}
impl PreparedScalarKernel for PreparedTruncate {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<TruncateInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(TruncateInstance {
            operation: self.operation,
        }))
    }
}

struct TruncateInstance {
    operation: TruncateOp,
}
impl ScalarKernelInstance for TruncateInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::truncate::evaluate_truncate(self.operation, input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
pub(super) fn prepared_for_test(
    sources: &[crate::FunctionValueType],
    literals: &[Option<crate::ConstantValue>],
    policy: DecimalOverflowPolicy,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    tests::prepared_for_test(sources, literals, policy)
}
#[cfg(test)]
mod tests {
    use super::super::catalogue::constant_binding_tests as cv;
    use super::*;
    use crate::{
        ConstantValue, FunctionArgument, FunctionArgumentType, FunctionResultType,
        FunctionSpecializationFailure, FunctionValueType, ScopedExpressionEffects,
        specialize_frozen_scalar, specialize_scalar,
    };
    use arrow_schema::{DataType, Field};
    use novarocks_type_contract::{
        CompileControlError, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
        ExpressionUseId, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
        SemanticParameters, ValueLogicalType,
    };
    use std::sync::Mutex;
    fn owner() -> TruncateOwner {
        let (declaration, resolver) =
            super::super::catalogue::dynamic_definition_parts("truncate").unwrap();
        TruncateOwner::new(declaration, resolver).unwrap()
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
        owner: &'a TruncateOwner,
        selected: &'a FunctionBindingSelection,
        arguments: &'a [FunctionArgument],
        parameters: &'a SemanticParameters,
        uses: &'a [Option<ExpressionUseId>],
        policy: DecimalOverflowPolicy,
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
            decimal_overflow_policy: policy,
            proof_scope: CallProofScope::Unconditional,
        }
    }

    fn argument(ty: FunctionValueType) -> FunctionArgument {
        FunctionArgument::Value {
            value_type: ty,
            constant: None,
        }
    }

    fn sources() -> [DataType; 8] {
        [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(38, -3),
            DataType::Null,
        ]
    }
    fn uses(count: usize) -> Vec<Option<ExpressionUseId>> {
        (0..count)
            .map(|index| Some(ExpressionUseId::new(42 + index as u32)))
            .collect()
    }
    pub(super) fn prepared_for_test(
        sources: &[FunctionValueType],
        literals: &[Option<ConstantValue>],
        policy: DecimalOverflowPolicy,
    ) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
        if sources.len() != literals.len() {
            return Err(FunctionSpecializationFailure::InvalidInput(
                "truncate fixture literals do not match sources",
            ));
        }
        let owner = owner();
        let arguments = sources
            .iter()
            .zip(literals)
            .map(|(ty, literal)| FunctionArgument::Value {
                value_type: ty.clone(),
                constant: literal.clone(),
            })
            .collect::<Vec<_>>();
        let selected = Arc::new(owner.resolve(request(&arguments), crate::binding_test_control())?);
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(arguments.len());
        specialize_scalar(
            &owner,
            input(&owner, &selected, &arguments, &parameters, &uses, policy),
            selected.clone(),
            ScopedExpressionEffects::pure_value(context()),
            crate::binding_test_control(),
        )
        .map(|call| call.into_prepared())
    }
    #[test]
    fn actual_whole_catalogue_has_one_dynamic_truncate_overload_and_real_cpu_owner() {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let owner = owner();
        let definition = catalog
            .definition_by_id(owner.declaration.function_id())
            .unwrap();
        assert!(definition.binding.as_ref().unwrap().pure.is_some());
        assert_eq!(
            definition.binding_declaration().unwrap(),
            owner.binding_declaration()
        );
        assert_eq!(owner.declaration.overloads().len(), 1);
        assert_eq!(owner.implementations.len(), 1);
        let overload = &owner.declaration.overloads()[0];
        assert_eq!(overload.identity.as_str(), OVERLOAD);
        assert_eq!(overload.effects.as_ref(), Some(&effects()));
        let implementation = &owner.implementations[0];
        assert_eq!(implementation.overload, overload.identity);
        assert_eq!(implementation.implementation.as_str(), IMPLEMENTATION);
        assert_eq!(implementation.abi, PureKernelAbi::ScalarV1);
    }
    #[test]
    fn all_eight_sources_and_64_pairs_preserve_full_types_fresh_frozen_and_nullable() {
        let owner = owner();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let mut profiles = sources().into_iter().map(|ty| vec![ty]).collect::<Vec<_>>();
        for source in sources() {
            for digits in sources() {
                profiles.push(vec![source.clone(), digits]);
            }
        }
        assert_eq!(profiles.len(), 72);
        for profile in profiles {
            for mask in 0..1usize << profile.len() {
                // Physical Null is necessarily nullable; other inputs cover both flags.
                if profile
                    .iter()
                    .enumerate()
                    .any(|(index, ty)| matches!(ty, DataType::Null) && mask & (1 << index) == 0)
                {
                    continue;
                }
                let types = profile
                    .iter()
                    .enumerate()
                    .map(|(index, ty)| FunctionValueType::new(ty.clone(), mask & (1 << index) != 0))
                    .collect::<Vec<_>>();
                let arguments = types.iter().cloned().map(argument).collect::<Vec<_>>();
                let uses = uses(arguments.len());
                let selected = Arc::new(
                    owner
                        .resolve(request(&arguments), crate::binding_test_control())
                        .unwrap(),
                );
                assert_eq!(selected.overload.as_str(), OVERLOAD);
                assert_eq!(
                    selected.argument_types.as_ref(),
                    types
                        .iter()
                        .cloned()
                        .map(FunctionArgumentType::Value)
                        .collect::<Vec<_>>()
                        .as_slice()
                );
                let expected = match &profile[0] {
                    DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
                    _ if profile.len() == 2 => DataType::Float64,
                    _ => DataType::Int64,
                };
                assert_eq!(
                    selected.result_type,
                    FunctionResultType::Scalar(FunctionValueType::new(expected, true))
                );
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    let input = input(&owner, &selected, &arguments, &parameters, &uses, policy);
                    let fresh = specialize_scalar(
                        &owner,
                        input,
                        selected.clone(),
                        ScopedExpressionEffects::pure_value(context()),
                        crate::binding_test_control(),
                    )
                    .unwrap();
                    let contract = fresh.prepared().contract().clone();
                    let direct = owner
                        .prepare_scalar(input, contract.clone(), crate::binding_test_control())
                        .unwrap();
                    assert!(Arc::ptr_eq(direct.contract(), &contract));
                    assert!(std::ptr::eq(contract.selected(), selected.as_ref()));
                    let frozen = contract.effects().clone();
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
                    let instance = be.prepared().create_instance().unwrap();
                    assert_eq!(
                        instance.retained_bytes(),
                        std::mem::size_of::<TruncateInstance>()
                    );
                    assert_eq!(
                        instance.retained_bytes(),
                        be.prepared().instance_retained_upper_bound()
                    );
                }
            }
        }
    }
    #[test]
    fn literal_dependent_scale_stays_with_dynamic_binding_author_and_exact_validation() {
        let owner = owner();
        for (digits, scale) in [(2, 2), (257, 1), (128, 0), (-1, 0)] {
            let arguments = [
                argument(FunctionValueType::new(DataType::Decimal128(12, 5), false)),
                FunctionArgument::Value {
                    value_type: FunctionValueType::new(DataType::Int64, false),
                    constant: Some(cv::i64(digits, false)),
                },
            ];
            let selected = owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap();
            assert_eq!(
                selected.result_type,
                FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Decimal128(38, scale),
                    true
                ))
            );
            owner
                .validate_selected(
                    &selected,
                    request(&arguments),
                    crate::binding_test_control(),
                )
                .unwrap();
            let mut stale = selected.clone();
            stale.result_type = FunctionResultType::Scalar(FunctionValueType::new(
                DataType::Decimal128(38, 5),
                true,
            ));
            assert!(
                owner
                    .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                    .is_err()
            );
            let prepared = prepared_for_test(
                &[
                    FunctionValueType::new(DataType::Decimal128(12, 5), false),
                    FunctionValueType::new(DataType::Int64, false),
                ],
                &[None, Some(cv::i64(digits, false))],
                DecimalOverflowPolicy::OutputNull,
            )
            .unwrap();
            assert_eq!(prepared.contract().selected(), &selected);
        }
    }
    #[test]
    fn safe_output_effects_preserve_no_row_error_for_each_frozen_policy() {
        let owner = owner();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(1);
        for source in [DataType::Float64, DataType::Decimal128(38, 3)] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let arguments = [argument(FunctionValueType::new(source.clone(), true))];
                let selected = Arc::new(
                    owner
                        .resolve(request(&arguments), crate::binding_test_control())
                        .unwrap(),
                );
                let input = input(&owner, &selected, &arguments, &parameters, &uses, policy);
                let facts = owner
                    .validate_and_refine(input, crate::binding_test_control())
                    .unwrap();
                let expected = FunctionIntrinsicRowError::NoRowError;
                assert_eq!(facts.own_row_error, expected);
                assert_eq!(facts.null_behavior, FunctionNullBehavior::Strict);
                assert!(facts.environment.is_empty());
                let mut foreign = facts.clone();
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
                    Err(FunctionSpecializationFailure::InvalidInput(_))
                ));
                let mut foreign = facts;
                foreign.null_behavior = FunctionNullBehavior::CalledOnNull;
                assert!(
                    specialize_frozen_scalar(
                        &owner,
                        input,
                        selected.clone(),
                        &foreign,
                        ScopedExpressionEffects::pure_value(context()),
                        crate::binding_test_control()
                    )
                    .is_err()
                );
            }
        }
    }
    #[test]
    fn foreign_env_proof_context_policy_and_selected_pointer_do_not_reuse_receipt() {
        let owner = owner();
        let arguments = [argument(FunctionValueType::new(
            DataType::Decimal128(38, 3),
            true,
        ))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(1);
        let exact = input(
            &owner,
            &selected,
            &arguments,
            &parameters,
            &uses,
            DecimalOverflowPolicy::ReportError,
        );
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
        wrong.proof_scope = CallProofScope::Domain(EvaluationDomainId::new(99));
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
        wrong.context.use_id = ExpressionUseId::new(99);
        assert!(
            owner
                .prepare_scalar(wrong, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        wrong = exact;
        wrong.decimal_overflow_policy = DecimalOverflowPolicy::OutputNull;
        assert!(
            owner
                .prepare_scalar(wrong, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        let equal_foreign = (*selected).clone();
        wrong = exact;
        wrong.selected = &equal_foreign;
        assert!(
            owner
                .prepare_scalar(wrong, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        let mut domain = exact;
        domain.proof_scope = CallProofScope::Domain(context().domain);
        assert_eq!(
            owner
                .validate_and_refine(domain, crate::binding_test_control())
                .unwrap()
                .proof_scope,
            domain.proof_scope
        );
        let mut frozen = canonical.effects().clone();
        frozen.environment = environment.to_vec().into_boxed_slice();
        assert!(
            specialize_frozen_scalar(
                &owner,
                exact,
                selected.clone(),
                &frozen,
                ScopedExpressionEffects::pure_value(context()),
                crate::binding_test_control()
            )
            .is_err()
        );
    }
    #[test]
    fn stale_selected_domains_arity_and_wrong_dynamic_overload_are_rejected() {
        let owner = owner();
        let args = [argument(FunctionValueType::new(DataType::Float64, true))];
        let selected = owner
            .resolve(request(&args), crate::binding_test_control())
            .unwrap();
        for wrong in [
            FunctionBindingSelection {
                argument_types: Box::default(),
                ..selected.clone()
            },
            FunctionBindingSelection {
                overload: crate::FunctionOverloadId::try_new("builtin.scalar/round/dynamic-v1")
                    .unwrap(),
                ..selected.clone()
            },
            FunctionBindingSelection {
                result_type: FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Float64,
                    true,
                )),
                ..selected
            },
        ] {
            assert!(
                owner
                    .validate_selected(&wrong, request(&args), crate::binding_test_control())
                    .is_err()
            );
        }
        for ty in [
            FunctionValueType::new(DataType::FixedSizeBinary(16), true),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        ] {
            assert!(prepared_for_test(&[ty], &[None], DecimalOverflowPolicy::OutputNull).is_err());
        }
        assert!(prepared_for_test(&[], &[], DecimalOverflowPolicy::OutputNull).is_err());
        assert!(
            prepared_for_test(
                &[FunctionValueType::new(DataType::Float64, true)],
                &[],
                DecimalOverflowPolicy::OutputNull
            )
            .is_err()
        );
    }
    struct FailCompile(CompileControlError);
    impl PureCompileControl for FailCompile {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0)
        }
    }
    #[test]
    fn real_fresh_frozen_specializations_preserve_original_three_control_reasons() {
        let owner = owner();
        let args = [argument(FunctionValueType::new(DataType::Float64, true))];
        let selected = Arc::new(
            owner
                .resolve(request(&args), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = uses(1);
        let input = input(
            &owner,
            &selected,
            &args,
            &parameters,
            &uses,
            DecimalOverflowPolicy::OutputNull,
        );
        let facts = owner
            .validate_and_refine(input, crate::binding_test_control())
            .unwrap();
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert!(
                matches!(owner.resolve(request(&args),&FailCompile(error)),Err(FunctionBindingError::Control(actual)) if actual==error)
            );
            assert!(
                matches!(owner.validate_and_refine(input,&FailCompile(error)),Err(FunctionEffectOwnerError::Control(actual)) if actual==error)
            );
            assert!(
                matches!(specialize_scalar(&owner,input,selected.clone(),ScopedExpressionEffects::pure_value(context()),&FailCompile(error)),Err(FunctionSpecializationFailure::Control(actual)) if actual==error)
            );
            assert!(
                matches!(specialize_frozen_scalar(&owner,input,selected.clone(),&facts,ScopedExpressionEffects::pure_value(context()),&FailCompile(error)),Err(FunctionSpecializationFailure::Control(actual)) if actual==error)
            );
        }
    }
    #[derive(Default)]
    struct TraceControl {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for TraceControl {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::FunctionSpecialization);
            assert!(units <= 256);
            let mut calls = self.calls.lock().unwrap();
            let index = calls.len();
            calls.push(units);
            if let Some((at, error)) = self.refusal
                && at == index
            {
                return Err(error);
            }
            Ok(())
        }
    }
    #[test]
    fn actual_dynamic_resolver_observes_256_and_tail_of_complete_expected_type() {
        let owner = owner();
        let args = [argument(FunctionValueType::new(DataType::Float64, true))];
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
        let baseline = TraceControl::default();
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
                    matches!(owner.resolve(request,&control),Err(FunctionBindingError::Control(actual)) if actual==error)
                );
                assert_eq!(control.calls.lock().unwrap().len(), at + 1);
            }
        }
    }
}
