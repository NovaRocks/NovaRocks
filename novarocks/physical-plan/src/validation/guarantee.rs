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
//! Actual row uses and legacy binding bits do not certify a scan guarantee.

use super::{FragmentPropertyError, ValidationContext, ValidationError, ValidationErrors};
use crate::{
    ExprId, ExprKind, ExprNode, Fragment, NodeKind, PlanLimits, PropertyProofProjectionFacts,
    PropertyProofProjectionLimits,
};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
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
    for node in fragment.nodes().values() {
        if let NodeKind::Scan { relation, .. } = &node.kind {
            for _ in relation.predicate_guarantees() {
                let next = guarantees.checked_add(1);
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
            enqueue(
                &mut definitions,
                &mut pending,
                (
                    node.id,
                    fragment
                        .unit(node.id)
                        .and_then(|u| u.root(guarantee.predicate))
                        .ok_or_else(|| resource(limits))?
                        .definition,
                ),
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
                    let mut errors = ValidationContext::with_limits(limits);
                    errors.push(ValidationError::new(
                        format!("nodes[{}].relation.predicate_guarantees", node.id.get()),
                        "predicate guarantee has no exact function proof",
                    ));
                    return Err(FragmentPropertyError::Structure(
                        ValidationErrors::from_collector(errors),
                    ));
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
