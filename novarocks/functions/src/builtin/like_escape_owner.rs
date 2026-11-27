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

//! Binding, demand and selected evaluation for Boolean NOT and LIKE.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::catalogue::BuiltinScalarResolver;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::{compile_failure, invalid};
use crate::native_like::LikePatternStorage;
use crate::{
    AggregateStateAllocator, CallEffectInput, FunctionBindingDeclaration, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingResolver, FunctionBindingSelection,
    FunctionCatalogError, FunctionDefinition, FunctionEffectOwner, FunctionEffectOwnerError,
    FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError, FunctionKind,
    FunctionVisibility, FunctionVolatility, KernelEvaluationControl, KernelFailure,
    PreparedScalarKernel, PureFunctionMetadataOwner, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, PureScalarImplementation, ScalarCallContract,
    ScalarCallInput, ScalarKernelInstance, SelectedValues,
};

pub(super) const BOOLEAN_NOT: &str = "__operator_not";
pub(super) const LIKE: &str = "__operator_like";

pub(super) fn effects(name: &str, signature: &str) -> FunctionEffectDeclaration {
    let may_raise = name == LIKE && signature.starts_with("(utf8,utf8,utf8)");
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: if may_raise {
            FunctionIntrinsicRowError::MayRaise
        } else {
            FunctionIntrinsicRowError::NoRowError
        },
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
    let owner = Arc::new(PredicateOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Hidden, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin predicate pure owner",
            value: error.to_string().into(),
        },
    )
}

#[derive(Clone, Copy, Debug)]
enum Predicate {
    BooleanNot,
    Like,
}

struct PredicateOwner {
    predicate: Predicate,
    resolver: Arc<BuiltinScalarResolver>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl PredicateOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: BuiltinScalarResolver,
    ) -> Result<Self, FunctionCatalogError> {
        let (predicate, count) = match name {
            BOOLEAN_NOT => (Predicate::BooleanNot, 1),
            LIKE => (Predicate::Like, 2),
            _ => {
                return Err(FunctionCatalogError::InvalidStableIdentity {
                    subject: "uninstalled predicate",
                    value: name.into(),
                });
            }
        };
        let function = format!("builtin.scalar/{name}/v1");
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != count
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some()
                    || overload.effects.as_ref() != Some(&effects(name, &overload.argument_pattern))
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin predicate pure declaration",
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
            predicate,
            resolver: Arc::new(resolver),
            declaration,
            implementations,
        })
    }

    fn call_effects(
        &self,
        selected: &FunctionBindingSelection,
        scope: CallProofScope,
    ) -> Result<CallEffects, FunctionBindingError> {
        let base = self.declaration.effect_declaration(&selected.overload)?;
        Ok(CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior.clone(),
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: scope,
        })
    }
}

impl FunctionBindingResolver for PredicateOwner {
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

impl PureFunctionMetadataOwner for PredicateOwner {
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

impl FunctionEffectOwner for PredicateOwner {
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

impl PureScalarImplementation for PredicateOwner {
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
struct PreparedPredicate {
    predicate: Predicate,
    contract: Arc<ScalarCallContract>,
}
impl PreparedScalarKernel for PreparedPredicate {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        std::mem::size_of::<PredicateInstance>()
            + if matches!(self.predicate, Predicate::Like) {
                HostAggregateAllocator::metadata_allocation_bytes()
            } else {
                0
            }
    }

    fn invocation_resource_profile(&self) -> Option<crate::ScalarInvocationResourceProfile> {
        Some(crate::ScalarInvocationResourceProfile::primitive_builder(
            crate::scalar_resources::PrimitiveBuilderOutput::Boolean,
            self.contract.effects().own_row_error == FunctionIntrinsicRowError::MayRaise,
        ))
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        self.create_instance_with_allocator(None)
    }

    fn create_instance_with_allocator(
        &self,
        host: Option<Arc<dyn AggregateStateAllocator>>,
    ) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        let pattern_storage = match self.predicate {
            Predicate::BooleanNot => None,
            Predicate::Like => host.map(LikePatternStorage::new),
        };
        Ok(Box::new(PredicateInstance {
            predicate: self.predicate,
            pattern_storage,
        }))
    }
}

struct PredicateInstance {
    predicate: Predicate,
    pattern_storage: Option<LikePatternStorage>,
}
impl ScalarKernelInstance for PredicateInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        match self.predicate {
            Predicate::BooleanNot => evaluate_not(input, control),
            Predicate::Like => evaluate_like(input, self.pattern_storage.as_mut(), control),
        }
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .pattern_storage
                .as_ref()
                .map_or(0, LikePatternStorage::retained_bytes)
    }
}

impl PredicateOwner {
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
                    "predicates have no environment dependencies and require an exact proof scope"
                        .into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
            Ok(self.call_effects(input.selected, input.proof_scope)?)
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
            let exact_effects = self
                .call_effects(input.selected, input.proof_scope)
                .map_err(|_| invalid("predicate preparation has a foreign overload"))?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
                || contract.function_id() != input.function_id
                || !std::ptr::eq(contract.selected(), input.selected)
                || novarocks_type_contract::CallEffectContext::from(contract.context())
                    != input.context
                || contract.decimal_overflow_policy() != input.decimal_overflow_policy
                || contract.effects() != &exact_effects
                || !input.environment.is_empty()
            {
                return Err(invalid(
                    "predicate preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("predicate preparation has a stale selected binding"),
            })?;
            // The prepared object retains the same canonical contract. Its body is
            // a static pure implementation and needs no live resolver or authority.
            Ok(Arc::new(PreparedPredicate {
                contract,
                predicate: self.predicate,
            }) as Arc<dyn PreparedScalarKernel>)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
}

fn evaluate_not<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    use arrow_array::{Array, BooleanArray, builder::BooleanBuilder};
    use arrow_schema::DataType;

    let [argument] = input.arguments() else {
        return Err(invalid("NOT requires one evaluated argument"));
    };
    let source = argument
        .array()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| invalid("NOT requires an exact Boolean argument"))?;
    let selection = input.selection();
    let mut work = crate::EvaluationCheckpoints::new(control);
    let outcome = (|| {
        work.flush()?;
        let mut values = BooleanBuilder::with_capacity(selection.len());
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let row = argument.value_row(ordinal, row);
            values.append_option((!source.is_null(row)).then(|| !source.value(row)));
        }
        work.flush()?;
        SelectedValues::try_new_observed(
            selection,
            &DataType::Boolean,
            Arc::new(values.finish()),
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(outcome)
}

fn evaluated_utf8<'a>(
    argument: crate::EvaluatedArgument<'a>,
) -> Result<&'a arrow_array::StringArray, KernelFailure> {
    argument
        .array()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| invalid("LIKE requires exact UTF-8 inputs"))
}

fn evaluate_like<'a>(
    input: ScalarCallInput<'_, 'a>,
    mut pattern_storage: Option<&mut LikePatternStorage>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    use crate::kernel_input::EvaluationCheckpoints;
    use crate::native_like::{LikeError, LikeObservation, like_in_observed};
    use arrow_array::{Array, builder::BooleanBuilder};
    use arrow_schema::DataType;

    let mut work = EvaluationCheckpoints::new(control);
    let output = (|| {
        let (text, pattern, escape) = match input.arguments() {
            [text, pattern] => (*text, *pattern, None),
            [text, pattern, escape] => (*text, *pattern, Some(*escape)),
            _ => return Err(invalid("LIKE requires two or three evaluated inputs")),
        };
        let strings = evaluated_utf8(text)?;
        let patterns = evaluated_utf8(pattern)?;
        let escapes = escape.map(evaluated_utf8).transpose()?;
        let selection = input.selection();
        let mut errors = Vec::new();
        work.flush()?;
        let mut values = BooleanBuilder::with_capacity(selection.len());
        work.flush()?;
        for (ordinal, row) in selection.iter().enumerate() {
            work.step()?;
            let text_row = text.value_row(ordinal, row);
            let pattern_row = pattern.value_row(ordinal, row);
            if strings.is_null(text_row) || patterns.is_null(pattern_row) {
                values.append_null();
                continue;
            }
            let text = strings.value(text_row);
            let pattern = patterns.value(pattern_row);
            let mut observe = |event| match event {
                LikeObservation::Step => work.step(),
                LikeObservation::OpaqueBoundary => work.flush(),
            };
            let escape = if let Some((escape, escapes)) = escape.zip(escapes) {
                let escape_row = escape.value_row(ordinal, row);
                if escapes.is_null(escape_row) {
                    values.append_null();
                    continue;
                }
                Some(escapes.value(escape_row))
            } else {
                None
            };
            match like_in_observed(
                text,
                pattern,
                escape,
                pattern_storage.as_deref_mut(),
                &mut observe,
            ) {
                Ok(value) => values.append_value(value),
                Err(LikeError::Operation(cause) | LikeError::Host(cause)) => return Err(cause),
                Err(LikeError::Pattern(cause)) => {
                    work.flush()?;
                    errors
                        .try_reserve(1)
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    errors.push(crate::RowDataError::from_domain(ordinal, cause)?);
                    values.append_null();
                    work.flush()?;
                }
            }
        }
        work.flush()?;
        let values = Arc::new(values.finish());
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &DataType::Boolean,
            values,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(output)
}
