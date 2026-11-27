// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One frozen writer preparation body shared by both native carriers.
//! Its contexts are original source facts; there is no synthetic graph,
//! fresh-effect fallback, name resolution, query owner, or allocation grant.
use crate::pure_catalogue::CheckedCallPreparationFailure;
use crate::{
    AggregateKernelPhase, AggregatePreparationOptions, CallArgumentUses, CallEffectInput,
    EngineFunctionCatalog, FunctionArgumentType, FunctionBindingRequest, FunctionBindingSelection,
    FunctionId, FunctionKind, FunctionSpecializationFailure, PureCallPreparation,
    PureCallSpecialization, ScopedRelationalEffects,
};
use novarocks_type_contract::{
    CallEffects, CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    FunctionValueType, PureCompileControl, RelationEffectContext, SemanticParameters,
};
use std::sync::Arc;

pub struct FrozenWriterAggregatePreparation<'a> {
    pub function_id: &'a FunctionId,
    pub selected: &'a Arc<FunctionBindingSelection>,
    pub original_request: FunctionBindingRequest<'a>,
    pub call_context: RelationEffectContext,
    pub materialized_input_context: RelationEffectContext,
    pub frozen_effects: &'a CallEffects,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub parameters: &'a SemanticParameters,
    pub actual_input_type: &'a FunctionValueType,
    pub phase: AggregateKernelPhase,
}

pub fn prepare_frozen_writer_aggregate(
    catalog: &EngineFunctionCatalog,
    source: FrozenWriterAggregatePreparation<'_>,
    control: &dyn PureCompileControl,
) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
    prepare_frozen_writer_aggregate_core::<std::convert::Infallible>(
        catalog, source, None, control, None,
    )
    .map_err(pure_writer_failure)
}

/// Same frozen preparation body with the actual caller-created immutable empty
/// order owner. The BE author observes that header before constructing it.
pub fn prepare_frozen_writer_aggregate_with_empty_order_keys(
    catalog: &EngineFunctionCatalog,
    source: FrozenWriterAggregatePreparation<'_>,
    order_keys: Arc<[crate::AggregateOrderKey]>,
    control: &dyn PureCompileControl,
) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
    if !order_keys.is_empty() {
        return Err(FunctionSpecializationFailure::InvalidInput(
            "writer source has order keys",
        ));
    }
    prepare_frozen_writer_aggregate_core::<std::convert::Infallible>(
        catalog,
        source,
        Some(order_keys),
        control,
        None,
    )
    .map_err(pure_writer_failure)
}
/// Prepare the same frozen writer source using its actual construction scope.
pub fn prepare_frozen_writer_aggregate_with_creator_in<H>(
    catalog: &EngineFunctionCatalog,
    source: FrozenWriterAggregatePreparation<'_>,
    creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
    control: &dyn PureCompileControl,
) -> Result<PureCallSpecialization, CheckedCallPreparationFailure<H>> {
    prepare_frozen_writer_aggregate_core(catalog, source, None, control, Some(creation))
}

/// Borrow the original caller-created empty order owner on the checked route.
pub fn prepare_frozen_writer_aggregate_with_empty_order_keys_and_creator_in<H>(
    catalog: &EngineFunctionCatalog,
    source: FrozenWriterAggregatePreparation<'_>,
    order_keys: Arc<[crate::AggregateOrderKey]>,
    creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
    control: &dyn PureCompileControl,
) -> Result<PureCallSpecialization, CheckedCallPreparationFailure<H>> {
    if !order_keys.is_empty() {
        return Err(
            FunctionSpecializationFailure::InvalidInput("writer source has order keys").into(),
        );
    }
    prepare_frozen_writer_aggregate_core(catalog, source, Some(order_keys), control, Some(creation))
}

fn pure_writer_failure(
    error: CheckedCallPreparationFailure<std::convert::Infallible>,
) -> FunctionSpecializationFailure {
    use crate::{CheckedOwnerFailure as O, FunctionBindingCreationFailure as B};
    match error {
        CheckedCallPreparationFailure::Preparation(O::Original(error)) => error,
        CheckedCallPreparationFailure::Binding(B::Binding(error)) => error.into(),
        CheckedCallPreparationFailure::Binding(B::Creator(never)) => match never {},
        CheckedCallPreparationFailure::Binding(B::Resource(_) | B::UnconvertedResolverOwner)
        | CheckedCallPreparationFailure::Preparation(O::UnconvertedOwner) => {
            unreachable!("pure writer preparation never enters a staged checked owner")
        }
    }
}

fn prepare_frozen_writer_aggregate_core<H>(
    catalog: &EngineFunctionCatalog,
    source: FrozenWriterAggregatePreparation<'_>,
    order_keys: Option<Arc<[crate::AggregateOrderKey]>>,
    control: &dyn PureCompileControl,
    creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Result<PureCallSpecialization, CheckedCallPreparationFailure<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result: Result<PureCallSpecialization, CheckedCallPreparationFailure<H>> = (|| {
        let input = source.materialized_input_context;
        if !matches!(
            source.phase,
            AggregateKernelPhase::Partial | AggregateKernelPhase::Final
        ) || source.call_context.demand != EvaluationDemand::Value
            || input.demand != EvaluationDemand::Value
            || !input.is_materialized_input_of(source.call_context)
            || source.original_request.logical_argument_count != 1
            || source.original_request.arguments.len() != 1
            || source.selected.argument_types.len() != 1
        {
            return Err(FunctionSpecializationFailure::InvalidInput(
                "writer preparation changes its original phase, scalar channel or Value scope",
            )
            .into());
        }
        work.step()?;
        let [FunctionArgumentType::Value(argument)] = source.selected.argument_types.as_slice()
        else {
            return Err(FunctionSpecializationFailure::InvalidInput(
                "writer preparation cannot consume a lambda",
            )
            .into());
        };
        if source.selected.aggregate.is_none() {
            return Err(FunctionSpecializationFailure::InvalidInput(
                "writer preparation has no frozen aggregate selection",
            )
            .into());
        }
        if source.phase == AggregateKernelPhase::Partial {
            let equal = argument.same_physical_binding_ignoring_root_nullability_observed(
                source.actual_input_type,
                || work.step(),
            )?;
            if !equal || (source.actual_input_type.nullable() && !argument.nullable()) {
                return Err(FunctionSpecializationFailure::InvalidInput(
                    "writer materialized input differs from its complete logical channel",
                )
                .into());
            }
        }
        let children = ScopedRelationalEffects::pure_value(input)
            .for_use(input)
            .map_err(FunctionSpecializationFailure::Effects)?;
        let order_keys = order_keys.unwrap_or_else(|| Arc::from([]));
        let (argument_uses, options) = if source.phase.consumes_logical_arguments() {
            (
                CallArgumentUses::MaterializedInput {
                    context: input,
                    input_type: source.actual_input_type,
                },
                AggregatePreparationOptions {
                    state_interpretation: None,
                    phase: source.phase,
                    distinct: false,
                    order_keys,
                    state_input_type: None,
                },
            )
        } else {
            (
                CallArgumentUses::AggregateMerge {
                    phase: source.phase,
                    state_context: input.into(),
                    state_input_type: source.actual_input_type,
                },
                AggregatePreparationOptions {
                    state_interpretation: None,
                    phase: source.phase,
                    distinct: false,
                    order_keys,
                    state_input_type: Some(source.actual_input_type.clone()),
                },
            )
        };
        work.flush()?;
        let effect_input = CallEffectInput {
            context: source.call_context.into(),
            argument_uses,
            function_id: source.function_id,
            kind: FunctionKind::Aggregate,
            selected: source.selected.as_ref(),
            request: source.original_request,
            environment: &source.frozen_effects.environment,
            parameters: source.parameters,
            decimal_overflow_policy: source.decimal_overflow_policy,
            proof_scope: source.frozen_effects.proof_scope,
        };
        let selected = Arc::clone(source.selected);
        let preparation = PureCallPreparation::Aggregate {
            arguments: ScopedRelationalEffects::primitive(source.call_context, children),
            options,
        };
        let token = match creation {
            Some(creation) => catalog.prepare_selected_checked_with_creator_in(
                effect_input,
                selected,
                Some(source.frozen_effects),
                preparation,
                creation,
                control,
            )?,
            None => catalog.prepare_frozen_selected(
                effect_input,
                selected,
                source.frozen_effects,
                preparation,
                control,
            )?,
        };
        work.flush()?;
        Ok(token)
    })();
    if result
        .as_ref()
        .is_err_and(CheckedCallPreparationFailure::is_interruption)
    {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
#[path = "frozen_writer_preparation_tests.rs"]
mod tests;
