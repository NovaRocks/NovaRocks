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

//! The installed unary numeric bindings, effects and preparation share each exact owner.

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

use super::numeric_unary::NumericUnaryOp;

#[cfg(test)]
pub(super) fn names() -> &'static [&'static str] {
    &[
        "acos", "asin", "atan", "cbrt", "ceil", "ceiling", "dceil", "cos", "cot", "degress",
        "dlog1", "exp", "dexp", "floor", "dfloor", "ln", "log10", "dlog10", "log2", "radians",
        "sin", "sqrt", "dsqrt", "square", "tan", "positive",
    ]
}

/// This startup mapping covers only the names installed by unary_ops. The
/// private prepared operation avoids function-name resolution at evaluation.
pub(super) fn operation(name: &str) -> Option<NumericUnaryOp> {
    use NumericUnaryOp::*;
    Some(match name {
        "acos" => Acos,
        "asin" => Asin,
        "atan" => Atan,
        "cbrt" => Cbrt,
        "ceil" | "ceiling" | "dceil" => Ceil,
        "cos" => Cos,
        "cot" => Cot,
        "degress" => Degrees,
        "dlog1" => Dlog1,
        "exp" | "dexp" => Exp,
        "floor" | "dfloor" => Floor,
        "ln" => Ln,
        "log10" | "dlog10" => Log10,
        "log2" => Log2,
        "radians" => Radians,
        "sin" => Sin,
        "sqrt" | "dsqrt" => Sqrt,
        "square" => Square,
        "tan" => Tan,
        "positive" => Positive,
        _ => return None,
    })
}

/// Valid inputs have the seven existing Physical numeric profiles. Domain and
/// finite-range failures return successful NULL; malformed calls remain outer
/// failures. No mutable state or environment authority is retained.
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
    let owner = Arc::new(NumericUnaryOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin numeric unary pure owner",
            value: error.to_string().into(),
        },
    )
}

struct NumericUnaryOwner {
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    operation: NumericUnaryOp,
}
impl NumericUnaryOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let op = operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin numeric unary name",
            value: name.into(),
        })?;
        let function = format!("builtin.scalar/{name}/v1");
        let expected = effects();
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 7
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin numeric unary pure declaration",
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

impl FunctionBindingResolver for NumericUnaryOwner {
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

impl PureFunctionMetadataOwner for NumericUnaryOwner {
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

impl FunctionEffectOwner for NumericUnaryOwner {
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

impl PureScalarImplementation for NumericUnaryOwner {
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
struct PreparedNumericUnary {
    contract: Arc<ScalarCallContract>,
    operation: NumericUnaryOp,
}
impl PreparedScalarKernel for PreparedNumericUnary {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<NumericUnaryInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(NumericUnaryInstance {
            operation: self.operation,
        }))
    }
}

struct NumericUnaryInstance {
    operation: NumericUnaryOp,
}
impl ScalarKernelInstance for NumericUnaryInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::numeric_unary::evaluate_numeric_unary(self.operation, input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
pub(super) fn prepared_for_test(
    name: &str,
    source: crate::FunctionValueType,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    tests::prepared_for_test(name, source)
}

impl NumericUnaryOwner {
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
                "numeric unary has no environment dependencies and requires an exact proof scope"
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
            || contract.context() != input.context
            || contract.decimal_overflow_policy() != input.decimal_overflow_policy
            || contract.effects() != &self.call_effects(input.proof_scope)
            || !input.environment.is_empty()
        {
            return Err(invalid(
                "numeric unary preparation differs from its exact checked call",
            ));
        }
        work.flush().map_err(compile_failure)?;
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("numeric unary preparation has a stale selected binding"),
        })?;
        work.finish().map_err(compile_failure)?;
        // The prepared object retains the same canonical contract. Its body is
        // a static pure implementation and needs no live resolver or authority.
        Ok(Arc::new(PreparedNumericUnary {
            contract,
            operation: self.operation,
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

    fn owner(name: &str) -> NumericUnaryOwner {
        let (_, signatures) = super::super::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(candidate, _)| candidate == name)
            .expect("the actual unary numeric registry entry");
        let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
            name,
            &signatures,
            FunctionKind::Scalar,
        )
        .unwrap();
        NumericUnaryOwner::new(name, declaration, resolver).unwrap()
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
        owner: &'a NumericUnaryOwner,
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

    pub(super) fn prepared_for_test(
        name: &str,
        source: FunctionValueType,
    ) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
        if operation(name).is_none() {
            return Err(FunctionSpecializationFailure::Binding(
                FunctionBindingError::UnknownFunction,
            ));
        }
        let owner = owner(name);
        let arguments = [argument(source)];
        let selected = Arc::new(owner.resolve(request(&arguments), crate::binding_test_control())?);
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
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
    fn startup_operation_mapping_covers_exact_installed_names_and_aliases() {
        assert_eq!(names().len(), 26);
        let mut operations = Vec::new();
        for name in names() {
            let op = operation(name).unwrap();
            if !operations.contains(&op) {
                operations.push(op);
            }
        }
        assert_eq!(operations.len(), 20);
        for (alias, original) in [
            ("ceiling", "ceil"),
            ("dceil", "ceil"),
            ("dfloor", "floor"),
            ("dexp", "exp"),
            ("dlog10", "log10"),
            ("dsqrt", "sqrt"),
        ] {
            assert_eq!(operation(alias), operation(original));
            assert_ne!(
                owner(alias).declaration.function_id(),
                owner(original).declaration.function_id()
            );
        }
        for unavailable in [
            "degrees", "sinh", "cosh", "tanh", "log", "dround", "sign", "negative",
        ] {
            assert_eq!(operation(unavailable), None);
        }
    }

    #[test]
    fn whole_production_catalogue_attaches_all_unary_owners_and_182_exact_records() {
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
            assert_eq!(declaration.overloads().len(), 7);
            assert_eq!(owner.implementation_declarations().len(), 7);
            for (overload, implementation) in declaration
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
            }
            total += owner.implementation_declarations().len();
        }
        assert_eq!(total, 182);
    }

    #[test]
    fn all_26_names_and_seven_profiles_preserve_full_types_fresh_and_frozen() {
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
        for name in names() {
            let owner = owner(name);
            for source in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Float64,
                DataType::Decimal128(38, -3),
            ] {
                for nullable in [false, true] {
                    let source_type = FunctionValueType::from_legacy_declared_projection(
                        source.clone(),
                        nullable,
                    );
                    let arguments = [argument(source_type.clone())];
                    let selected = Arc::new(
                        owner
                            .resolve(request(&arguments), crate::binding_test_control())
                            .unwrap(),
                    );
                    assert_eq!(
                        selected.argument_types.as_ref(),
                        &[FunctionArgumentType::Value(source_type)]
                    );
                    let FunctionResultType::Scalar(result) = &selected.result_type else {
                        panic!("scalar result required")
                    };
                    let target = if matches!(
                        owner.operation,
                        NumericUnaryOp::Ceil | NumericUnaryOp::Floor
                    ) {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    };
                    assert_eq!(result.data_type().clone(), target);
                    assert_eq!(result.logical_type(), ValueLogicalType::Physical);
                    assert_eq!(
                        result.nullable(),
                        if *name == "positive"
                            && !matches!(source, DataType::Float32 | DataType::Float64)
                        {
                            nullable
                        } else {
                            true
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
                    let canonical = Arc::clone(fresh.prepared().contract());
                    let direct = owner
                        .prepare_scalar(input, canonical.clone(), crate::binding_test_control())
                        .unwrap();
                    assert!(Arc::ptr_eq(direct.contract(), &canonical));
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
                    let canonical = Arc::clone(be.prepared().contract());
                    let direct = owner
                        .prepare_scalar(input, canonical.clone(), crate::binding_test_control())
                        .unwrap();
                    assert!(Arc::ptr_eq(direct.contract(), &canonical));
                    assert!(std::ptr::eq(
                        be.prepared().contract().selected(),
                        selected.as_ref()
                    ));
                    assert_eq!(be.prepared().contract().effects(), &frozen);
                    let instance = be.prepared().create_instance().unwrap();
                    assert_eq!(
                        instance.retained_bytes(),
                        std::mem::size_of::<NumericUnaryInstance>()
                    );
                    assert_eq!(
                        be.prepared().instance_retained_upper_bound(),
                        instance.retained_bytes()
                    );
                }
            }
        }
    }

    #[test]
    fn exact_owner_rejects_stale_output_overload_and_foreign_frozen_effects() {
        let owner = owner("sqrt");
        let arguments = [argument(FunctionValueType::new(DataType::Int8, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let mut stale = (*selected).clone();
        stale.result_type =
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Float64, false));
        assert!(
            owner
                .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                .is_err()
        );
        stale = (*selected).clone();
        stale.overload = self::owner("acos").declaration.overloads()[0]
            .identity
            .clone();
        assert!(
            owner
                .validate_selected(&stale, request(&arguments), crate::binding_test_control())
                .is_err()
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
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
    fn refinement_requires_exact_environment_domain_and_function_identity() {
        let owner = owner("cos");
        let arguments = [argument(FunctionValueType::new(DataType::Float64, false))];
        let selected = owner
            .resolve(request(&arguments), crate::binding_test_control())
            .unwrap();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
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
        let foreign_id = FunctionId::try_new("builtin.scalar/sin/v1").unwrap();
        foreign = input;
        foreign.function_id = &foreign_id;
        assert!(matches!(
            owner.validate_and_refine(foreign, crate::binding_test_control()),
            Err(FunctionEffectOwnerError::Owner(
                FunctionBindingError::UnknownFunction
            ))
        ));
        let mut exact_domain = input;
        exact_domain.proof_scope = CallProofScope::Region(context().region);
        assert_eq!(
            owner
                .validate_and_refine(exact_domain, crate::binding_test_control())
                .unwrap()
                .proof_scope,
            CallProofScope::Region(context().region)
        );
    }

    #[test]
    fn preparation_rejects_foreign_context_policy_selected_pointer_and_opaque_sources() {
        let owner = owner("positive");
        let arguments = [argument(FunctionValueType::new(DataType::Float64, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
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
        for logical_type in [
            ValueLogicalType::Physical,
            ValueLogicalType::Uuid,
            ValueLogicalType::LargeInt,
        ] {
            let arguments = [argument(
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
                    self::input(&owner, &selected, &arguments, &parameters, &uses),
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
    fn original_three_compile_failures_survive_resolve_refine_and_specialize() {
        let owner = owner("ceil");
        let arguments = [argument(FunctionValueType::new(DataType::Float64, false))];
        let selected = Arc::new(
            owner
                .resolve(request(&arguments), crate::binding_test_control())
                .unwrap(),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(42))];
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
