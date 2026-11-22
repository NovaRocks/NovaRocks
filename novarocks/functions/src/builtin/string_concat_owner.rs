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

//! Strict builtin string and array concatenation bindings, effects and preparation.

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

#[derive(Clone, Copy, Debug)]
pub(super) enum ConcatKind {
    String,
    TrinoScalar,
    Array,
    Append,
    Prepend,
}

impl ConcatKind {
    fn element(self) -> Option<usize> {
        match self {
            Self::Append => Some(1),
            Self::Prepend => Some(0),
            _ => None,
        }
    }

    fn scalar(self) -> bool {
        matches!(self, Self::String | Self::TrinoScalar)
    }
}

pub(super) fn overload_kind(name: &str, signature: &str) -> Option<ConcatKind> {
    match name {
        "concat" if signature.starts_with("(list<any<T>>,any<T>)") => Some(ConcatKind::Append),
        "concat" if signature.starts_with("(any<T>,list<any<T>>)") => Some(ConcatKind::Prepend),
        "concat" if signature.starts_with("(list<") => Some(ConcatKind::Array),
        "concat"
            if ["(varchar(", "(char(", "(binary"]
                .into_iter()
                .any(|prefix| signature.starts_with(prefix)) =>
        {
            Some(ConcatKind::TrinoScalar)
        }
        _ => operation(name),
    }
}

pub(super) fn operation(name: &str) -> Option<ConcatKind> {
    match name {
        "concat" => Some(ConcatKind::String),
        _ => None,
    }
}

/// Concatenation is strict and stateless. Array arity and the Trino scalar
/// output boundary can report row errors.
pub(super) fn effects(kind: ConcatKind) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: match kind {
            ConcatKind::Array | ConcatKind::TrinoScalar => FunctionIntrinsicRowError::MayRaise,
            _ => FunctionIntrinsicRowError::NoRowError,
        },
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
    let owner = Arc::new(ConcatOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin concatenation pure owner",
            value: error.to_string().into(),
        },
    )
}

pub(super) struct ConcatOwner {
    kinds: Box<[ConcatKind]>,
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl ConcatOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let function = format!("builtin.scalar/{name}/v1");
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin concatenation pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let mut kinds = Vec::with_capacity(declaration.overloads().len());
        let mut implementations = Vec::with_capacity(declaration.overloads().len());
        for overload in declaration.overloads() {
            let kind = overload_kind(name, &overload.argument_pattern)
                .filter(|kind| {
                    overload.effects.as_ref() == Some(&effects(*kind))
                        && overload.aggregate.is_none()
                })
                .ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                    subject: "builtin concatenation overload",
                    value: overload.identity.as_str().into(),
                })?;
            let implementation = match (name, kind) {
                ("concat", ConcatKind::Array) => {
                    "builtin.scalar/concat/array-selected-v1".to_string()
                }
                ("concat", ConcatKind::Append) => {
                    "builtin.scalar/concat/append-selected-v1".to_string()
                }
                ("concat", ConcatKind::Prepend) => {
                    "builtin.scalar/concat/prepend-selected-v1".to_string()
                }
                _ => format!("builtin.scalar/{name}/selected-v1"),
            };
            kinds.push(kind);
            implementations.push(PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: PureImplementationId::try_new(implementation)?,
                abi: PureKernelAbi::ScalarV1,
            });
        }
        Ok(Self {
            kinds: kinds.into_boxed_slice(),
            resolver: Arc::new(resolver),
            declaration,
            implementations: implementations.into_boxed_slice(),
        })
    }

    fn kind(
        &self,
        selected: &FunctionBindingSelection,
    ) -> Result<ConcatKind, FunctionBindingError> {
        self.declaration
            .overloads()
            .iter()
            .zip(&self.kinds)
            .find_map(|(overload, kind)| (overload.identity == selected.overload).then_some(*kind))
            .ok_or_else(|| FunctionBindingError::UnknownOverload(selected.overload.clone()))
    }

    fn call_effects(kind: ConcatKind, scope: CallProofScope, arguments: usize) -> CallEffects {
        let base = effects(kind);
        let own_row_error = if matches!(kind, ConcatKind::TrinoScalar) || arguments == 1 {
            base.own_row_error
        } else {
            FunctionIntrinsicRowError::NoRowError
        };
        CallEffects {
            value_stability: base.value_stability,
            own_row_error,
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

impl FunctionBindingResolver for ConcatOwner {
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

impl PureFunctionMetadataOwner for ConcatOwner {
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

impl FunctionEffectOwner for ConcatOwner {
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

impl PureScalarImplementation for ConcatOwner {
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
struct PreparedConcat {
    kind: ConcatKind,
    contract: Arc<ScalarCallContract>,
}
impl PreparedScalarKernel for PreparedConcat {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        match self.kind {
            ConcatKind::String | ConcatKind::TrinoScalar => {
                std::mem::size_of::<StringConcatInstance>()
            }
            _ => std::mem::size_of::<ArrayConcatInstance>(),
        }
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Ok(match self.kind {
            ConcatKind::String | ConcatKind::TrinoScalar => Box::new(StringConcatInstance),
            kind => Box::new(ArrayConcatInstance(kind)),
        })
    }
}

struct StringConcatInstance;
impl ScalarKernelInstance for StringConcatInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::string_concat::evaluate_string_concat(input, control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

struct ArrayConcatInstance(ConcatKind);
impl ScalarKernelInstance for ArrayConcatInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        super::array_concat::evaluate(input, self.0.element(), control)
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
pub(super) fn owner_for_test(name: &str) -> ConcatOwner {
    let (_, signatures) = super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(candidate, _)| candidate == name)
        .unwrap();
    let (declaration, resolver) =
        super::catalogue::scalar_definition_parts(name, &signatures, FunctionKind::Scalar).unwrap();
    ConcatOwner::new(name, declaration, resolver).unwrap()
}
#[cfg(test)]
pub(super) fn prepared_for_test_with_policy(
    name: &str,
    sources: &[crate::FunctionValueType],
    policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    prepared_for_test_with_control(name, sources, policy, crate::binding_test_control())
}
#[cfg(test)]
pub(super) fn prepared_for_test_with_control(
    name: &str,
    sources: &[crate::FunctionValueType],
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, crate::FunctionSpecializationFailure> {
    use crate::{FunctionArgument, ScopedExpressionEffects, specialize_scalar};
    use novarocks_type_contract::{
        EvaluationDemand, EvaluationRegionId, ExpressionEffectContext, ExpressionUseId,
        SemanticParameters,
    };
    let owner = owner_for_test(name);
    let arguments = sources
        .iter()
        .map(|source| FunctionArgument::Value {
            value_type: source.clone(),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: arguments.len(),
    };
    let selected = Arc::new(owner.resolve(request, control)?);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = (0..arguments.len())
        .map(|ordinal| Some(ExpressionUseId::new(42 + ordinal as u32)))
        .collect::<Vec<_>>();
    let input = CallEffectInput {
        context: context.into(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: policy,
        proof_scope: CallProofScope::Unconditional,
    };
    specialize_scalar(
        &owner,
        input,
        selected.clone(),
        ScopedExpressionEffects::pure_value(context),
        control,
    )
    .map(|specialization| specialization.into_prepared())
}

impl ConcatOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
            {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            if !input.environment.is_empty()
                || !matches!(input.proof_scope, CallProofScope::Unconditional)
                    && input.proof_scope != input.context.proof_scope()
            {
                return Err(FunctionBindingError::InvalidBinding(
                "concatenation has no environment dependencies and requires an exact proof scope".into(),
            )
            .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
            Ok(Self::call_effects(
                self.kind(input.selected)?,
                input.proof_scope,
                input.selected.argument_types.len(),
            ))
        })();
        if matches!(&result, Err(FunctionEffectOwnerError::Control(_))) {
            return result;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        result
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
        let result: Result<Arc<dyn PreparedScalarKernel>, KernelFailure> = (|| {
            work.step().map_err(compile_failure)?;
            let kind = self
                .kind(input.selected)
                .map_err(|_| invalid("unknown concatenation overload"))?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
                || contract.function_id() != input.function_id
                || !std::ptr::eq(contract.selected(), input.selected)
                || novarocks_type_contract::CallEffectContext::from(contract.context())
                    != input.context
                || contract.decimal_overflow_policy() != input.decimal_overflow_policy
                || contract.effects()
                    != &Self::call_effects(
                        kind,
                        input.proof_scope,
                        input.selected.argument_types.len(),
                    )
                || !input.environment.is_empty()
            {
                return Err(invalid(
                    "concatenation preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("concatenation preparation has a stale selected binding"),
            })?;
            // The prepared object retains the same canonical contract. Its body is
            // a static pure implementation and needs no live resolver or authority.
            if !kind.scalar() {
                super::array_concat::validate_profile(&contract, kind.element(), || {
                    work.step().map_err(compile_failure)
                })?;
            }
            Ok(Arc::new(PreparedConcat { kind, contract }) as Arc<dyn PreparedScalarKernel>)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
}
