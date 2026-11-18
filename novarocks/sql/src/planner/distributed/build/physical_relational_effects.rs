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

//! Checked actual relation identities and independent node-unit root effects.
use super::contract_lowering::NodeExpressionEffects;
use super::expression_occurrences::{AuthoredPhysicalOccurrences, WriterOccurrenceContexts};
use novarocks_expression_contract::{BoundExprSet, RootOrdinal};
use novarocks_functions::ScopedRelationalEffects;
use novarocks_physical_plan::{Fragment, PhysicalCallSite, ValueDef};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, EffectContractError, EvaluationDemand,
    ExpressionEffects, ExpressionUseId, RelationEffectContext,
};
#[derive(Debug)]
pub(crate) enum PhysicalRelationalEffectsError {
    Control(CompileControlError),
    Effects(EffectContractError),
    MissingChildEffects(ExpressionUseId),
    InvalidSource(&'static str),
}
impl From<CompileControlError> for PhysicalRelationalEffectsError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<EffectContractError> for PhysicalRelationalEffectsError {
    fn from(cause: EffectContractError) -> Self {
        Self::Effects(cause)
    }
}
/// The actual field names one root of its own node unit. Only the neutral
/// summary crosses into a relation; scalar region/caching rights do not.
/// The caller validates actual node/unit/source pairing and owns admission.
pub(crate) fn relational_root_effects_observed(
    unit: &BoundExprSet,
    root: RootOrdinal,
    child_effects: &NodeExpressionEffects,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ExpressionUseId, ExpressionEffects), PhysicalRelationalEffectsError> {
    let same_unit = std::ptr::eq(unit, child_effects.unit());
    work.step()?;
    if !same_unit {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "relational parameter summaries belong to another unit",
        ));
    }
    let declaration = unit.root(root);
    work.step()?;
    let declaration = declaration
        .filter(|root| root.demand == EvaluationDemand::Value)
        .ok_or(PhysicalRelationalEffectsError::InvalidSource(
            "ordered relational parameter is not an actual Value root",
        ))?;
    let use_id = unit.root_use_ids().get(root.get() as usize).copied();
    work.step()?;
    let use_id = use_id.ok_or(PhysicalRelationalEffectsError::InvalidSource(
        "ordered relational parameter has no actual use",
    ))?;
    let invocation = unit.control_flow().uses().get(&use_id);
    work.step()?;
    let matching = invocation.is_some_and(|invocation| {
        invocation.definition == declaration.definition
            && invocation.context.use_id == use_id
            && invocation.context.demand == EvaluationDemand::Value
    });
    work.step()?;
    if !matching {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "relational parameter invocation differs from its original unit root",
        ));
    }
    let invocation = invocation.expect("checked original root invocation");
    let region = unit
        .control_flow()
        .regions()
        .get(&invocation.context.region);
    work.step()?;
    if !region.is_some_and(|region| region.parent.is_none() && region.guard.is_none()) {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "relational parameter root has a guarded or absent region",
        ));
    }
    let summary = child_effects.summaries().get(&use_id).copied();
    work.step()?;
    let summary = summary.ok_or(PhysicalRelationalEffectsError::MissingChildEffects(use_id))?;
    let effects = summary.for_use(invocation.context);
    work.step()?;
    Ok((use_id, effects?))
}

pub(crate) fn relational_context_observed(
    occurrences: &AuthoredPhysicalOccurrences<'_>,
    site: PhysicalCallSite,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RelationEffectContext, PhysicalRelationalEffectsError> {
    relational_writer_context_observed(&occurrences.writer_contexts(), site, work)
}
pub(crate) fn relational_writer_context_observed(
    occurrences: &WriterOccurrenceContexts<'_, '_>,
    site: PhysicalCallSite,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RelationEffectContext, PhysicalRelationalEffectsError> {
    let mut found = None;
    for &(actual_site, context) in occurrences.relational_contexts {
        let matching = actual_site == site;
        work.step()?;
        if matching {
            let repeated = found.replace(context).is_some();
            work.step()?;
            if repeated {
                return Err(PhysicalRelationalEffectsError::InvalidSource(
                    "relational site has repeated relation contexts",
                ));
            }
        }
    }
    let context = found.ok_or(PhysicalRelationalEffectsError::InvalidSource(
        "relational site has no original relation context",
    ))?;
    if !context.is_call() {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "relation call has a materialized-input role or non-Value demand",
        ));
    }
    for &(other_site, other) in occurrences.relational_contexts {
        let colliding = other_site != site && other.identity.call == context.identity.call;
        work.step()?;
        if colliding {
            return Err(PhysicalRelationalEffectsError::InvalidSource(
                "relation call shares another call's identity",
            ));
        }
    }
    for (input_site, _, input) in occurrences.writer_value_uses() {
        let matching = if input_site == site {
            input.is_materialized_input_of(context)
        } else {
            input.identity.call != context.identity.call
        };
        work.step()?;
        if !matching {
            return Err(PhysicalRelationalEffectsError::InvalidSource(
                "materialized Writer input has a different actual relation identity or role",
            ));
        }
    }
    Ok(context)
}

/// A materialized Writer channel is an actual Value, never a re-evaluated
/// origin expression. Its conservative effects are pure in its own input role.
pub(crate) fn writer_value_context_effects_observed(
    occurrences: &WriterOccurrenceContexts<'_, '_>,
    fragment: &Fragment,
    site: PhysicalCallSite,
    actual_value: &ValueDef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(RelationEffectContext, ExpressionEffects), PhysicalRelationalEffectsError> {
    let same_fragment = std::ptr::eq(occurrences.fragment(), fragment);
    work.step()?;
    let same_value = fragment
        .values()
        .get(&actual_value.id)
        .is_some_and(|value| std::ptr::eq(value, actual_value));
    work.step()?;
    if !same_fragment || !same_value {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "Writer Value occurrence loans a foreign fragment or Value definition",
        ));
    }
    let mut found = None;
    for (actual_site, value, context) in occurrences.writer_value_uses() {
        let same_site = actual_site == site;
        work.step()?;
        if same_site {
            let matching = std::ptr::eq(value, actual_value) && found.is_none();
            work.step()?;
            if !matching {
                return Err(PhysicalRelationalEffectsError::InvalidSource(
                    "Writer channel differs from its original unique Value occurrence",
                ));
            }
            found = Some(context);
        }
    }
    let context = found.ok_or(PhysicalRelationalEffectsError::InvalidSource(
        "Writer channel has no original materialized Value occurrence",
    ))?;
    let call = relational_writer_context_observed(occurrences, site, work)?;
    let matching = context.is_materialized_input_of(call);
    work.step()?;
    if !matching {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "Writer channel differs from its original materialized-input role",
        ));
    }
    let effects = ScopedRelationalEffects::pure_value(context).for_use(context);
    work.step()?;
    Ok((context, effects?))
}
