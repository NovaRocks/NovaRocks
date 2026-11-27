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

//! The installed bit shift bindings, effects and preparation share each exact owner.

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

use super::bit_shift::ShiftOp;

/// Startup selection covers the three installed shift operations.
/// Evaluation retains the private operation, never resolving a function name.
pub(super) fn operation(name: &str) -> Option<ShiftOp> {
    Some(match name {
        "bitwise_left_shift" => ShiftOp::Left,
        "bitwise_right_shift_arithmetic" => ShiftOp::Right,
        "bitwise_right_shift" => ShiftOp::RightLogical,
        _ => return None,
    })
}

/// The four signed profiles preserve their width and take an INTEGER count.
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
    let owner = Arc::new(BitShiftOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin bit shift pure owner",
            value: error.to_string().into(),
        },
    )
}

struct BitShiftOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    operation: ShiftOp,
}
impl BitShiftOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let op = operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin bit shift name",
            value: name.into(),
        })?;
        let function = format!("builtin.scalar/{name}/v1");
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 4
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin bit shift pure declaration",
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

impl FunctionBindingResolver for BitShiftOwner {
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

impl PureFunctionMetadataOwner for BitShiftOwner {
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

impl FunctionEffectOwner for BitShiftOwner {
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

impl PureScalarImplementation for BitShiftOwner {
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
struct PreparedBitShift {
    contract: Arc<ScalarCallContract>,
    operation: ShiftOp,
}
impl PreparedScalarKernel for PreparedBitShift {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn invocation_resource_profile(&self) -> Option<crate::ScalarInvocationResourceProfile> {
        use crate::scalar_resources::NarrowShiftWidth;
        use arrow_schema::DataType;
        use novarocks_type_contract::ValueLogicalType;

        let mut arguments = self.contract.value_argument_types();
        if arguments.len() != 2 {
            return None;
        }
        let source = arguments.next()?;
        let count = arguments.next()?;
        let result = self.contract.result_type();
        if count.logical_type() != ValueLogicalType::Physical
            || count.data_type() != &DataType::Int32
            || source.logical_type() != ValueLogicalType::Physical
            || result.logical_type() != ValueLogicalType::Physical
            || result.data_type() != source.data_type()
            || !result.nullable()
        {
            return None;
        }
        Some(match source.data_type() {
            DataType::Int8 => {
                crate::ScalarInvocationResourceProfile::narrow_shift(NarrowShiftWidth::Int8)
            }
            DataType::Int16 => {
                crate::ScalarInvocationResourceProfile::narrow_shift(NarrowShiftWidth::Int16)
            }
            DataType::Int32 => {
                crate::ScalarInvocationResourceProfile::narrow_shift(NarrowShiftWidth::Int32)
            }
            DataType::Int64 => crate::ScalarInvocationResourceProfile::int64_shift(),
            _ => return None,
        })
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<BitShiftInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(BitShiftInstance {
            operation: self.operation,
        }))
    }
}

struct BitShiftInstance {
    operation: ShiftOp,
}
impl ScalarKernelInstance for BitShiftInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::bit_shift::evaluate_shift(self.operation, input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl BitShiftOwner {
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
                "bit shift has no environment dependencies and requires an exact proof scope"
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
                "bit shift preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("bit shift preparation has a stale selected binding"),
        })?;
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedBitShift {
            contract,
            operation: self.operation,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FunctionArgument, FunctionResultType, FunctionSpecializationFailure, FunctionValueType,
        ScopedExpressionEffects, specialize_frozen_scalar, specialize_scalar,
    };
    use arrow_schema::DataType;
    use novarocks_type_contract::{
        CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
        ExpressionEffectContext, ExpressionUseId, SemanticParameterId, SemanticParameterKey,
        SemanticParameterRef, SemanticParameters, ValueLogicalType,
    };

    fn owner(name: &str) -> BitShiftOwner {
        let (_, signatures) = super::super::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(candidate, _)| candidate == name)
            .expect("the actual bit shift registry entry");
        let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
            name,
            &signatures,
            FunctionKind::Scalar,
        )
        .unwrap();
        BitShiftOwner::new(name, declaration, resolver).unwrap()
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
        owner: &'a BitShiftOwner,
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

    #[test]
    fn exact_shift_owner_rejects_stale_output_overload_and_foreign_frozen_effects() {
        let owner = owner("bitwise_left_shift");
        let arguments = [
            argument(FunctionValueType::new(DataType::Int8, false)),
            argument(FunctionValueType::new(DataType::Int32, true)),
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
        stale.overload = self::owner("bitwise_right_shift_arithmetic")
            .declaration
            .overloads()[0]
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
    fn shift_refinement_requires_exact_environment_domain_and_function_identity() {
        let owner = owner("bitwise_right_shift_arithmetic");
        let arguments = [
            argument(FunctionValueType::new(DataType::Int64, false)),
            argument(FunctionValueType::new(DataType::Int32, false)),
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
        let other = FunctionId::try_new("builtin.scalar/bitwise_left_shift/v1").unwrap();
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
    }

    #[test]
    fn shift_preparation_rejects_foreign_context_policy_pointer_and_uncoerced_types() {
        let owner = owner("bitwise_right_shift_arithmetic");
        let arguments = [
            argument(FunctionValueType::new(DataType::Int64, false)),
            argument(FunctionValueType::new(DataType::Int32, true)),
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
        for source in [
            FunctionValueType::new(DataType::Float64, false),
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
            Err(self.0.clone())
        }
    }
    #[test]
    fn shift_owner_preserves_original_three_compile_failures() {
        let owner = owner("bitwise_right_shift_arithmetic");
        let arguments = [
            argument(FunctionValueType::new(DataType::Int64, false)),
            argument(FunctionValueType::new(DataType::Int32, false)),
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
                matches!(owner.resolve(request(&arguments), &FailCompile(error.clone())), Err(FunctionBindingError::Control(actual)) if actual == error)
            );
            assert!(
                matches!(owner.validate_and_refine(input, &FailCompile(error.clone())), Err(FunctionEffectOwnerError::Control(actual)) if actual == error)
            );
            assert!(
                matches!(specialize_scalar(&owner, input, selected.clone(), ScopedExpressionEffects::pure_value(context()), &FailCompile(error.clone())),
                Err(FunctionSpecializationFailure::Control(actual)) if actual == error)
            );
            assert!(matches!(
                specialize_frozen_scalar(&owner, input, selected.clone(), &facts, ScopedExpressionEffects::pure_value(context()), &FailCompile(error.clone())),
                Err(FunctionSpecializationFailure::Control(actual)) if actual == error
            ));
        }
    }
}
