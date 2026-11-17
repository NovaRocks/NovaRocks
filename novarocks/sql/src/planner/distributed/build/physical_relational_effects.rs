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

//! Checked original relational contexts and independent argument-root effects.

use super::expression_occurrences::{AuthoredPhysicalOccurrences, WriterOccurrenceContexts};
use novarocks_functions::ScopedExpressionEffects;
use novarocks_physical_plan::{
    ExprId, ExpressionRootSite, Fragment, PhysicalCallSite, PhysicalRootUses, ValueDef,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, EffectContractError, EvaluationDemand,
    ExpressionEffectContext, ExpressionEffects, ExpressionUseId,
};
use std::collections::BTreeMap;

#[derive(Debug)]
pub(crate) enum PhysicalRelationalEffectsError {
    Control(CompileControlError),
    Effects(EffectContractError),
    MissingChildEffects(ExpressionUseId),
    InvalidSource(&'static str),
}
impl From<CompileControlError> for PhysicalRelationalEffectsError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<EffectContractError> for PhysicalRelationalEffectsError {
    fn from(error: EffectContractError) -> Self {
        Self::Effects(error)
    }
}

/// Borrow the actual root and preserve its own context. This returns only a
/// neutral conservative summary for the operator; it never transfers the
/// child's region or grants movement/caching permission. The caller supplies
/// the role/ordinal and definition from its exact immutable source call, after
/// same-source static/topology admission. Original scope/resource obligations
/// and the caller's entry/ordinary footer remain outside this narrow loan.
pub(crate) fn relational_root_effects_observed(
    roots: &PhysicalRootUses,
    root_site: ExpressionRootSite,
    definition: ExprId,
    child_effects: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ExpressionUseId, ExpressionEffects), PhysicalRelationalEffectsError> {
    let root = roots.roots().sites().get(&root_site);
    work.step()?;
    let matching =
        root.is_some_and(|root| root.expr == definition && root.demand == EvaluationDemand::Value);
    work.step()?;
    if !matching {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "ordered relational argument root differs from actual source",
        ));
    }
    let use_id = roots.bindings().get(&root_site).copied();
    work.step()?;
    let use_id = use_id.ok_or(PhysicalRelationalEffectsError::InvalidSource(
        "ordered relational argument root has no actual use",
    ))?;
    let invocation = roots.flow().uses().get(&use_id);
    work.step()?;
    let matching = invocation.is_some_and(|invocation| {
        invocation.definition == definition
            && invocation.context.use_id == use_id
            && invocation.context.demand == EvaluationDemand::Value
    });
    work.step()?;
    if !matching {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "relational argument invocation differs from its original root",
        ));
    }
    let invocation = invocation.expect("checked original invocation");
    let region = roots.flow().regions().get(&invocation.context.region);
    work.step()?;
    let unguarded = region.is_some_and(|region| region.parent.is_none() && region.guard.is_none());
    work.step()?;
    if !unguarded {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "relational argument root has a guarded or absent region",
        ));
    }
    let summary = child_effects.get(&use_id).copied();
    work.step()?;
    let summary = summary.ok_or(PhysicalRelationalEffectsError::MissingChildEffects(use_id))?;
    // for_use retains the child's exact context. Only the neutral result
    // of this check is joined; no scoped cross-region join is permitted.
    let actual_effects = summary.for_use(invocation.context);
    work.step()?;
    let actual_effects = actual_effects?;
    Ok((use_id, actual_effects))
}

/// Relational contexts are not flow invocations. Keep their original authored
/// disjoint use namespace and unguarded Value region, with exactly one context
/// for this actual relational call site. The complete topology remains caller-validated.
pub(crate) fn relational_context_observed(
    occurrences: &AuthoredPhysicalOccurrences,
    site: PhysicalCallSite,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ExpressionEffectContext, PhysicalRelationalEffectsError> {
    relational_writer_context_observed(&occurrences.writer_contexts(), site, work)
}

pub(crate) fn relational_writer_context_observed(
    occurrences: &WriterOccurrenceContexts<'_, '_>,
    site: PhysicalCallSite,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ExpressionEffectContext, PhysicalRelationalEffectsError> {
    let mut found = None;
    for &(actual_site, context) in occurrences.relational_contexts {
        let matching = actual_site == site;
        work.step()?;
        if matching {
            let repeated = found.replace(context).is_some();
            work.step()?;
            if repeated {
                return Err(PhysicalRelationalEffectsError::InvalidSource(
                    "relational site has repeated relational contexts",
                ));
            }
        }
    }
    let context = found.ok_or(PhysicalRelationalEffectsError::InvalidSource(
        "relational site has no original relational context",
    ))?;
    for &(other_site, other) in occurrences.relational_contexts {
        let colliding = other_site != site
            && (other.use_id == context.use_id || other.region == context.region);
        work.step()?;
        if colliding {
            return Err(PhysicalRelationalEffectsError::InvalidSource(
                "relational context shares another relational use identity or region",
            ));
        }
    }
    for (_, _, other) in occurrences.writer_value_uses() {
        let colliding = other.use_id == context.use_id || other.region == context.region;
        work.step()?;
        if colliding {
            return Err(PhysicalRelationalEffectsError::InvalidSource(
                "relational context borrows a materialized Writer channel use or region",
            ));
        }
    }
    for invocation in occurrences.expression_invocations() {
        let shares_region = invocation.context.region == context.region;
        work.step()?;
        if shares_region {
            return Err(PhysicalRelationalEffectsError::InvalidSource(
                "relational context borrows an expression invocation region",
            ));
        }
    }
    let expression_collision = occurrences.contains_expression_use(context.use_id);
    work.step()?;
    let region = occurrences.region(context.region);
    work.step()?;
    let matching = !expression_collision
        && context.demand == EvaluationDemand::Value
        && region.is_some_and(|region| region.parent.is_none() && region.guard.is_none());
    work.step()?;
    if !matching {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "relational context differs from original disjoint Value region",
        ));
    }
    Ok(context)
}

/// Authenticate an actual materialized Writer channel from the same source
/// loan. Both host roles reuse this Value-only law; neither re-evaluates an
/// origin expression or transfers an upstream expression's effects.
pub(crate) fn writer_value_context_effects_observed(
    occurrences: &WriterOccurrenceContexts<'_, '_>,
    fragment: &Fragment,
    site: PhysicalCallSite,
    actual_value: &ValueDef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ExpressionEffectContext, ExpressionEffects), PhysicalRelationalEffectsError> {
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
            let same = std::ptr::eq(value, actual_value) && found.is_none();
            work.step()?;
            if !same {
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
    let region = occurrences.region(context.region);
    work.step()?;
    let actual = !occurrences.contains_expression_use(context.use_id)
        && context.demand == EvaluationDemand::Value
        && region.is_some_and(|region| region.parent.is_none() && region.guard.is_none());
    work.step()?;
    if !actual {
        return Err(PhysicalRelationalEffectsError::InvalidSource(
            "Writer channel differs from its original unguarded materialized Value region",
        ));
    }
    for (_, other) in occurrences.relational_contexts {
        let colliding = other.use_id == context.use_id || other.region == context.region;
        work.step()?;
        if colliding {
            return Err(PhysicalRelationalEffectsError::InvalidSource(
                "Writer channel borrows a relational operator use or region",
            ));
        }
    }
    for invocation in occurrences.expression_invocations() {
        let shares = invocation.context.region == context.region;
        work.step()?;
        if shares {
            return Err(PhysicalRelationalEffectsError::InvalidSource(
                "Writer channel borrows an expression invocation region",
            ));
        }
    }
    let scoped = super::physical_expression_effects::materialized_value_effects(context);
    let effects = scoped.for_use(context);
    work.step()?;
    Ok((context, effects?))
}
