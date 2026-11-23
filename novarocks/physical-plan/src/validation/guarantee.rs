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

//! A final semantic gate for guarantees lacking their own function proof.
//! The closed comparison proof uses the guaranteed root's own exact inputs
//! and claim. Other scalar calls still lack a provider proof.

use super::{FragmentPropertyError, ValidationContext, ValidationError, ValidationErrors};
use crate::{
    ExprId, ExprKind, ExprNode, Fragment, NodeId, NodeKind, PlanLimits,
    PropertyProofProjectionFacts, PropertyProofProjectionLimits, RootOrdinal,
    StaticFunctionArgument, ValueOrigin, supports_provider_scan_comparison,
};
use arrow_schema::DataType;
use novarocks_expression_contract::{BoundExprSet, RootScope};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileCheckpoints, CompileControlError, ControlShape,
    EvaluationDemand, FunctionArgumentType, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionKind, FunctionNullBehavior, FunctionVolatility,
    OperatorFunction, ValueLogicalType, owned_resources::btree,
};
use std::alloc::Layout;

struct Definition<'a> {
    id: (crate::NodeId, ExprId),
    node: &'a ExprNode,
    queued: bool,
}

fn resource(limits: PlanLimits) -> FragmentPropertyError {
    let mut errors = ValidationContext::with_limits(limits);
    errors.push(ValidationError::resource_limit(
        "fragment.guarantees.resources",
        "guarantee proof projection exceeds its explicit envelope",
    ));
    FragmentPropertyError::Structure(ValidationErrors::from_collector(errors))
}

fn add(a: usize, b: usize, limits: PlanLimits) -> Result<usize, FragmentPropertyError> {
    a.checked_add(b).ok_or_else(|| resource(limits))
}

fn mul(a: usize, b: usize, limits: PlanLimits) -> Result<usize, FragmentPropertyError> {
    a.checked_mul(b).ok_or_else(|| resource(limits))
}

// The existing structural validator has already checked references and cycles.
// This walk visits actual arena keys, never an ID-sized vector. A definition is
// queued once across all guarantees, including shared intrinsic descendants.
// The two scratch requests coexist with the original occurrence proof; these
// numerical ceilings authorize neither the allocator nor an installed owner.
pub(super) fn validate_guarantees_observed(
    fragment: &Fragment,
    original: PropertyProofProjectionFacts,
    limits: PlanLimits,
    ceilings: PropertyProofProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PropertyProofProjectionFacts, FragmentPropertyError> {
    let mut guarantees = 0usize;
    let mut comparison_work = 0usize;
    let value_lookup = btree::lookup_work(fragment.values().len()).map_err(|_| resource(limits))?;
    for node in fragment.nodes().values() {
        if let NodeKind::Scan { relation, .. } = &node.kind {
            let unit = node.expressions.as_deref();
            for guarantee in relation.predicate_guarantees() {
                let next = guarantees.checked_add(1);
                if let Some(unit) = unit
                    && unit
                        .root(guarantee.predicate)
                        .and_then(|root| unit.definition(root.definition))
                        .is_some_and(|definition| {
                            matches!(definition.kind, ExprKind::FunctionCall { .. })
                        })
                {
                    // Meter each map by its own retained cardinality. The
                    // shape checks operate on two flat type bindings and the
                    // bounded built-in identity, request and claim fields.
                    let mut own_work = add(value_lookup, 32, limits)?;
                    for count in [
                        unit.requests().len(),
                        unit.control_flow().uses().len(),
                        unit.claims().len(),
                    ] {
                        let lookup = btree::lookup_work(count).map_err(|_| resource(limits))?;
                        own_work = add(own_work, lookup, limits)?;
                        work.step()?;
                    }
                    comparison_work = add(comparison_work, own_work, limits)?;
                }
                work.step()?;
                guarantees = next.ok_or_else(|| resource(limits))?;
            }
        }
        work.step()?;
    }
    let mut edges = 0usize;
    if guarantees != 0 {
        for definition in fragment
            .nodes()
            .values()
            .filter_map(|n| n.expressions.as_ref())
            .flat_map(|u| u.definitions())
        {
            definition.kind.expression_references_observed(|_| {
                let next = edges.checked_add(1);
                work.step()?;
                edges = next.ok_or_else(|| resource(limits))?;
                Ok::<_, FragmentPropertyError>(())
            })?;
            work.step()?;
        }
    }
    let count = if guarantees == 0 {
        0
    } else {
        fragment
            .nodes()
            .values()
            .filter_map(|n| n.expressions.as_ref())
            .map(|u| u.definitions().len())
            .sum::<usize>()
    };
    let index_bytes = Layout::array::<Definition<'_>>(count)
        .map_err(|_| resource(limits))?
        .size();
    let queue_bytes = Layout::array::<usize>(count)
        .map_err(|_| resource(limits))?
        .size();
    let scratch = add(index_bytes, queue_bytes, limits)?;
    let comparisons = usize::BITS as usize - count.leading_zeros() as usize;
    // Two source-node passes, one arena count/publication/pop/kind visit per
    // definition, actual reference counting, sparse binary comparisons and
    // mark/enqueue visits. Twelve covers fixed arithmetic/admission work.
    let new_work = add(12, mul(fragment.nodes().len(), 2, limits)?, limits)?;
    let new_work = add(new_work, mul(count, 4, limits)?, limits)?;
    let new_work = add(
        new_work,
        mul(add(guarantees, edges, limits)?, comparisons + 4, limits)?,
        limits,
    )?;
    let new_work = add(new_work, comparison_work, limits)?;
    let facts = PropertyProofProjectionFacts {
        request_bytes: add(original.request_bytes, scratch, limits)?,
        coexisting_bytes: add(original.coexisting_bytes, scratch, limits)?,
        projection_work: add(original.projection_work, new_work, limits)?,
    };
    work.step()?;
    for (amount, maximum) in [
        (facts.request_bytes, ceilings.max_request_bytes),
        (facts.coexisting_bytes, ceilings.max_coexisting_bytes),
        (facts.projection_work, ceilings.max_projection_work),
    ] {
        let allowed = amount <= maximum;
        work.step()?;
        if !allowed {
            return Err(resource(limits));
        }
    }
    if guarantees == 0 {
        return Ok(facts);
    }
    work.flush()?;
    let mut definitions = Vec::new();
    if definitions.try_reserve_exact(count).is_err() {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    work.flush()?;
    let mut pending = Vec::new();
    if pending.try_reserve_exact(count).is_err() {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    work.flush()?;
    for owner in fragment.nodes().values() {
        if let Some(unit) = &owner.expressions {
            for node in unit.definitions() {
                definitions.push(Definition {
                    id: (owner.id, node.id),
                    node,
                    queued: false,
                });
                work.step()?;
            }
        }
    }
    for node in fragment.nodes().values() {
        work.step()?;
        let NodeKind::Scan { relation, .. } = &node.kind else {
            continue;
        };
        for guarantee in relation.predicate_guarantees() {
            let unit = node
                .expressions
                .as_deref()
                .ok_or_else(|| resource(limits))?;
            let root = unit
                .root(guarantee.predicate)
                .ok_or_else(|| resource(limits))?;
            let definition = unit
                .definition(root.definition)
                .ok_or_else(|| resource(limits))?;
            if matches!(definition.kind, ExprKind::FunctionCall { .. }) {
                if !prove_provider_comparison(
                    fragment,
                    node.id,
                    unit,
                    guarantee.predicate,
                    definition,
                    work,
                )? {
                    return Err(missing_function_proof(node.id, limits));
                }
                continue;
            }
            enqueue(
                &mut definitions,
                &mut pending,
                (node.id, root.definition),
                work,
                limits,
            )?;
            work.step()?;
            while let Some(ordinal) = pending.pop() {
                work.step()?;
                let definition = definitions[ordinal].node;
                let needs_proof = matches!(definition.kind, ExprKind::FunctionCall { .. });
                work.step()?;
                if needs_proof {
                    return Err(missing_function_proof(node.id, limits));
                }
                definition.kind.expression_references_observed(|id| {
                    enqueue(&mut definitions, &mut pending, (node.id, id), work, limits)?;
                    work.step()?;
                    Ok::<_, FragmentPropertyError>(())
                })?;
            }
        }
    }
    Ok(facts)
}

fn missing_function_proof(node: NodeId, limits: PlanLimits) -> FragmentPropertyError {
    let mut errors = ValidationContext::with_limits(limits);
    errors.push(ValidationError::new(
        format!("nodes[{}].relation.predicate_guarantees", node.get()),
        "predicate guarantee has no exact function proof",
    ));
    FragmentPropertyError::Structure(ValidationErrors::from_collector(errors))
}

fn prove_provider_comparison(
    fragment: &Fragment,
    scan: NodeId,
    unit: &BoundExprSet,
    ordinal: RootOrdinal,
    definition: &ExprNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FragmentPropertyError> {
    work.step()?;
    let ExprKind::FunctionCall { function, args } = &definition.kind else {
        return Ok(false);
    };
    let Some(operator) = OperatorFunction::from_function_id(&function.function_id) else {
        return Ok(false);
    };
    let (Some(_), Some(overload), FunctionKind::Scalar, [left, right]) = (
        operator.comparison(),
        operator.comparison_overload_text(),
        function.kind,
        args.as_ref(),
    ) else {
        return Ok(false);
    };
    if function.overload.as_str() != overload {
        return Ok(false);
    }
    let (Some(left), Some(right)) = (unit.definition(*left), unit.definition(*right)) else {
        return Ok(false);
    };
    let (column, constant, reference) = match (&left.kind, &right.kind) {
        (ExprKind::Value(column), ExprKind::Constant(reference)) => (column, right, reference),
        (ExprKind::Constant(reference), ExprKind::Value(column)) => (column, left, reference),
        _ => return Ok(false),
    };
    let provider_value = fragment.values().get(column);
    work.step()?;
    if !matches!(provider_value.map(|value| &value.origin),
        Some(ValueOrigin::ProviderField { scan_node, .. }) if *scan_node == scan)
        || constant.ty.nullable()
        || !supports_provider_scan_comparison(&left.ty)
        || !supports_provider_scan_comparison(&right.ty)
        || left.ty.data_type() != right.ty.data_type()
        || left.ty.value_type().temporal() != right.ty.value_type().temporal()
    {
        return Ok(false);
    }
    let [
        FunctionArgumentType::Value(left_type),
        FunctionArgumentType::Value(right_type),
    ] = &function.argument_types[..]
    else {
        return Ok(false);
    };
    if left_type != &left.ty
        || right_type != &right.ty
        || function.result_type != definition.ty
        || definition.ty.logical_type() != ValueLogicalType::Physical
        || definition.ty.data_type() != &DataType::Boolean
        || definition.ty.nullable() != (left.ty.nullable() || right.ty.nullable())
    {
        return Ok(false);
    }
    work.step()?;

    // The original call request ties the constant argument to its checked
    // pool address. Package validation checks the immutable pool separately.
    let request = unit.requests().get(&definition.id);
    work.step()?;
    let Some(request) = request else {
        return Ok(false);
    };
    if request.logical_argument_count != 2 || request.arguments.len() != 2 {
        return Ok(false);
    }
    for (argument, source) in request.arguments.iter().zip([left, right]) {
        let StaticFunctionArgument::Value {
            value_type,
            constant,
        } = argument
        else {
            return Ok(false);
        };
        if value_type != &source.ty
            || *constant != matches!(source.kind, ExprKind::Constant(_)).then_some(*reference)
        {
            return Ok(false);
        }
        work.step()?;
    }

    let (Some(root), Some(use_id)) = (unit.root(ordinal), unit.root_use(ordinal)) else {
        return Ok(false);
    };
    let invocation = unit.control_flow().uses().get(&use_id);
    work.step()?;
    let claim = unit.claims().get(&use_id);
    work.step()?;
    let (Some(invocation), Some(claim)) = (invocation, claim) else {
        return Ok(false);
    };
    let effects = &claim.effects;
    Ok(root.scope == RootScope::Port(0)
        && root.demand == EvaluationDemand::TruthOnly
        && invocation.definition == definition.id
        && invocation.control == ControlShape::Eager
        && invocation.context == claim.context
        && effects.proof_scope == CallProofScope::Region(invocation.context.region)
        && effects.value_stability == FunctionVolatility::Immutable
        && effects.failure_behavior == FunctionFailureBehavior::Propagate
        && effects.own_row_error == FunctionIntrinsicRowError::NoRowError
        && effects.null_behavior == FunctionNullBehavior::Strict
        && effects.argument_control == ArgumentControl::Eager
        && effects.instance_state == FunctionInstanceState::None
        && effects.observable_effects.is_empty()
        && effects.environment.is_empty()
        && claim.temporal_source.is_none()
        && claim.regexp_count_pattern_source.is_none()
        && claim.to_base64_byte_source.is_none())
}

fn enqueue(
    definitions: &mut [Definition<'_>],
    pending: &mut Vec<usize>,
    id: (crate::NodeId, ExprId),
    work: &mut CompileCheckpoints<'_>,
    limits: PlanLimits,
) -> Result<(), FragmentPropertyError> {
    let mut low = 0;
    let mut high = definitions.len();
    while low < high {
        let mid = low + (high - low) / 2;
        let order = definitions[mid].id.cmp(&id);
        work.step()?;
        match order {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => {
                let queued = definitions[mid].queued;
                work.step()?;
                if !queued {
                    definitions[mid].queued = true;
                    pending.push(mid);
                    work.step()?;
                }
                return Ok(());
            }
        }
    }
    let mut errors = ValidationContext::with_limits(limits);
    errors.push(ValidationError::new(
        "fragment.guarantees.expression",
        "guarantee references an absent definition after structural validation",
    ));
    Err(FragmentPropertyError::Structure(
        ValidationErrors::from_collector(errors),
    ))
}
