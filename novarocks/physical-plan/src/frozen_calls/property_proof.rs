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

use super::*;
use crate::resource::CutResourcePreflight;
use crate::validation::ValidationContext;
use crate::{ExprId, ExprNode, PhysicalNode, PlanLimits, ValueDef, ValueId};
use std::alloc::Layout;

/// Explicit projection ceilings supplied by the complete property caller.
/// These admit the new index only, not opaque source validators or host MEM.
#[derive(Clone, Copy, Debug)]
pub struct PropertyProofProjectionLimits {
    pub max_request_bytes: usize,
    pub max_coexisting_bytes: usize,
    pub max_projection_work: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct PropertyProofProjectionFacts {
    pub request_bytes: usize,
    pub coexisting_bytes: usize,
    pub projection_work: usize,
}

pub(super) fn projection_facts(
    nodes: usize,
    uses: usize,
    calls: usize,
    source_retained_bytes: usize,
) -> Result<PropertyProofProjectionFacts, FrozenCallError> {
    let request_bytes = Layout::array::<NodeProof>(nodes)
        .map_err(|_| FrozenCallError::TooManyItems)?
        .size();
    let comparisons = usize::BITS as usize - nodes.leading_zeros() as usize;
    // Prelude: seven cardinality checks, the numerical model, source scan,
    // five source additions, source validation, three envelope comparisons.
    // Index: N publications. Sole visit_calls: U invocation visits, N node
    // visits, at most C relational visits. Callback: two owner lookups, one
    // entry lookup, one criterion/publication, and a bounded binary search.
    let projection_work = 7usize
        .checked_add(1)
        .and_then(|n| n.checked_add(1))
        .and_then(|n| n.checked_add(5))
        .and_then(|n| n.checked_add(1))
        .and_then(|n| n.checked_add(3))
        .and_then(|n| n.checked_add(nodes))
        .and_then(|n| n.checked_add(uses))
        .and_then(|n| n.checked_add(nodes))
        .and_then(|n| {
            calls
                .checked_mul(comparisons + 5)
                .and_then(|c| n.checked_add(c))
        })
        .ok_or(FrozenCallError::TooManyItems)?;
    let coexisting_bytes = source_retained_bytes
        .checked_add(request_bytes)
        .ok_or(FrozenCallError::TooManyItems)?;
    Ok(PropertyProofProjectionFacts {
        request_bytes,
        coexisting_bytes,
        projection_work,
    })
}

// Only an independently checkable lower floor. The caller must invoice ALL
// structural source backing; deeper BTree/string owners are not guessed.
// Arc-held system-relation data is a separate resource contribution.
pub(super) fn source_floor(
    fragment: &Fragment,
    uses: &PhysicalRootUses,
    calls: &FrozenFragmentCalls,
) -> Result<usize, FrozenCallError> {
    let mut bytes = std::mem::size_of_val(fragment)
        .checked_add(std::mem::size_of_val(uses))
        .and_then(|n| n.checked_add(std::mem::size_of_val(calls)))
        .ok_or(FrozenCallError::TooManyItems)?;
    for (count, width) in [
        (
            fragment.nodes().len(),
            std::mem::size_of::<(NodeId, PhysicalNode)>(),
        ),
        (
            fragment.values().len(),
            std::mem::size_of::<(ValueId, ValueDef)>(),
        ),
        (
            fragment.expressions().len(),
            std::mem::size_of::<(ExprId, ExprNode)>(),
        ),
        (
            calls.entries.len(),
            std::mem::size_of::<(PhysicalCallSite, FrozenPhysicalCall)>(),
        ),
    ] {
        bytes = count
            .checked_mul(width)
            .and_then(|n| bytes.checked_add(n))
            .ok_or(FrozenCallError::TooManyItems)?;
    }
    Ok(bytes)
}

#[derive(Clone, Copy)]
struct NodeProof {
    node: NodeId,
    first_unsafe: Option<PhysicalCallSite>,
    declared_broadcast: bool,
}

/// A private projection of complete occurrence claims for exactly one borrowed
/// snapshot. It authenticates no installed owner and grants no allocation.
/// The immutable loans prevent replacement of any source while it is used.
pub(crate) struct OccurrencePropertyProof<'a> {
    fragment: &'a Fragment,
    _uses: &'a PhysicalRootUses,
    _calls: &'a FrozenFragmentCalls,
    nodes: Vec<NodeProof>,
    facts: PropertyProofProjectionFacts,
    first_broadcast_unsafe: Option<PhysicalCallSite>,
}
impl OccurrencePropertyProof<'_> {
    pub(crate) fn facts(&self) -> PropertyProofProjectionFacts {
        self.facts
    }

    pub(crate) fn require_declared_broadcast_equivalence(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FrozenCallError> {
        let first = self.first_broadcast_unsafe;
        work.step()?;
        match first {
            Some(site) => Err(FrozenCallError::ReplicaEquivalence(site)),
            None => Ok(()),
        }
    }

    pub(crate) fn require_fragment(
        &self,
        fragment: &Fragment,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FrozenCallError> {
        let same = std::ptr::eq(self.fragment, fragment);
        work.step()?;
        if same {
            Ok(())
        } else {
            Err(FrozenCallError::WrongFragment)
        }
    }

    pub(crate) fn replica_safe(
        &self,
        node: NodeId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, FrozenCallError> {
        let index = find_node(&self.nodes, node, work)?;
        let safe = self.nodes[index].first_unsafe.is_none();
        work.step()?;
        Ok(safe)
    }
}

// Every actual comparison is completed before observation. The sparse source
// ID is never used as a vector length or substituted by a first expression use.
fn find_node(
    nodes: &[NodeProof],
    node: NodeId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, FrozenCallError> {
    let mut low = 0usize;
    let mut high = nodes.len();
    while low < high {
        let mid = low + (high - low) / 2;
        let ordering = nodes[mid].node.cmp(&node);
        work.step()?;
        match ordering {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => return Ok(mid),
        }
    }
    Err(FrozenCallError::InvalidSite)
}

impl FrozenFragmentCalls {
    pub(crate) fn property_proof<'a>(
        &'a self,
        fragment: &'a Fragment,
        uses: &'a PhysicalRootUses,
        limits: &PlanLimits,
        source_retained_bytes: usize,
        projection_limits: PropertyProofProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<OccurrencePropertyProof<'a>, FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = self.property_proof_core(
            fragment,
            uses,
            limits,
            source_retained_bytes,
            projection_limits,
            None,
            &mut work,
        );
        finish_frozen_calls(work, result)
    }

    /// Project the same source on the caller's meter without entry or finish.
    /// This scope port does not invoice the opaque structural scratch owners.
    pub(crate) fn property_proof_in<'a>(
        &'a self,
        fragment: &'a Fragment,
        uses: &'a PhysicalRootUses,
        limits: &PlanLimits,
        source_retained_bytes: usize,
        projection_limits: PropertyProofProjectionLimits,
        admit: &mut dyn FnMut(
            &novarocks_type_contract::ControlOwnedResourceFacts,
        ) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<OccurrencePropertyProof<'a>, FrozenCallError> {
        self.property_proof_core(
            fragment,
            uses,
            limits,
            source_retained_bytes,
            projection_limits,
            Some(admit),
            work,
        )
    }

    fn property_proof_core<'a>(
        &'a self,
        fragment: &'a Fragment,
        uses: &'a PhysicalRootUses,
        limits: &PlanLimits,
        source_retained_bytes: usize,
        projection_limits: PropertyProofProjectionLimits,
        mut admit: Option<&mut ResourceAdmission<'_>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<OccurrencePropertyProof<'a>, FrozenCallError> {
        (|| {
            // Bound source cardinalities before a source validator or the new
            // canonical node index allocates. Limits count definitions, not IDs.
            for (count, maximum) in [
                (fragment.nodes().len(), limits.fragment_nodes),
                (fragment.values().len(), limits.fragment_values),
                (fragment.expressions().len(), limits.fragment_expressions),
                (self.entries.len(), MAX_CONTROL_USE_REFERENCES),
                (
                    uses.flow().use_reference_count(),
                    MAX_CONTROL_USE_REFERENCES,
                ),
                (uses.bindings().len(), MAX_CONTROL_USE_REFERENCES),
                (uses.roots().sites().len(), MAX_CONTROL_USE_REFERENCES),
            ] {
                let admitted = count <= maximum;
                work.step()?;
                if !admitted {
                    return Err(FrozenCallError::TooManyItems);
                }
            }
            let projected = (|| {
                if source_retained_bytes < source_floor(fragment, uses, self)? {
                    return Err(FrozenCallError::TooManyItems);
                }
                projection_facts(
                    fragment.nodes().len(),
                    uses.flow().uses().len(),
                    self.entries.len(),
                    source_retained_bytes,
                )
            })();
            work.step()?;
            let facts = projected?;
            for (amount, maximum) in [
                (facts.request_bytes, projection_limits.max_request_bytes),
                (
                    facts.coexisting_bytes,
                    projection_limits.max_coexisting_bytes,
                ),
                (facts.projection_work, projection_limits.max_projection_work),
            ] {
                let admitted = amount <= maximum;
                work.step()?;
                if !admitted {
                    return Err(FrozenCallError::TooManyItems);
                }
            }
            work.flush()?;
            let call_items = if admit.is_some() {
                self.dynamic_items_in(work)?
            } else {
                self.dynamic_items_observed(work.control())?
            };
            // Reuse the source's numerical vocabulary. This is a structural
            // envelope, not a model of source/validator allocations or MEM.
            let mut errors = ValidationContext::with_limits(*limits);
            let mut source = CutResourcePreflight::new();
            work.flush()?;
            source.add_fragment(fragment, &mut errors);
            work.step()?;
            work.flush()?;
            for count in [
                call_items,
                uses.flow().domains().len(),
                uses.flow().use_reference_count(),
                uses.bindings().len(),
                uses.roots().sites().len(),
            ] {
                source.add_items(count);
                work.step()?;
            }
            source.validate("fragment.property_proof.resources", &mut errors);
            work.step()?;
            if !errors.is_empty() {
                return Err(FrozenCallError::TooManyItems);
            }
            work.flush()?;
            if let Some(admit) = admit.as_deref_mut() {
                self.validate_fragment_in(fragment, uses, &mut |facts| admit(facts), work)?;
            } else {
                self.validate_fragment(fragment, uses, work.control())?;
            }
            work.flush()?;
            let mut nodes = Vec::new();
            let reserved = nodes.try_reserve_exact(fragment.nodes().len());
            if reserved.is_err() {
                // This is an actual request refusal, not a guessed budget.
                return Err(FrozenCallError::Control(
                    CompileControlError::ResourceExhausted,
                ));
            }
            work.flush()?;
            debug_assert_eq!(
                facts.request_bytes,
                std::mem::size_of::<NodeProof>() * fragment.nodes().len()
            );
            for (id, node) in fragment.nodes() {
                nodes.push(NodeProof {
                    node: *id,
                    first_unsafe: None,
                    declared_broadcast: node.output_properties.distribution
                        == crate::Distribution::Broadcast,
                });
                work.step()?;
            }
            let mut first_broadcast_unsafe = None;
            visit_calls::<FrozenCallError>(fragment, uses, work, |site, binding, work| {
                let owner = match site {
                    PhysicalCallSite::Expression(id) => {
                        let invocation = uses.flow().uses().get(&id);
                        work.step()?;
                        let invocation = invocation.ok_or(FrozenCallError::InvalidSite)?;
                        let expression = fragment.expressions().get(invocation.definition);
                        work.step()?;
                        expression.ok_or(FrozenCallError::InvalidSite)?.owner
                    }
                    PhysicalCallSite::Aggregate { node, .. }
                    | PhysicalCallSite::TopNState { node, .. }
                    | PhysicalCallSite::WriterPartial { node, .. }
                    | PhysicalCallSite::WriterFinal { node, .. }
                    | PhysicalCallSite::Table { node } => node,
                };
                let index = find_node(&nodes, owner, work)?;
                let call = self.entries.get(&site);
                work.step()?;
                let call = call.ok_or(FrozenCallError::MissingSite(site))?;
                let safe =
                    super::replica::call_is_replica_equivalent(binding.kind(), &call.effects);
                if !safe {
                    if nodes[index].first_unsafe.is_none() {
                        nodes[index].first_unsafe = Some(site);
                    }
                    if nodes[index].declared_broadcast && first_broadcast_unsafe.is_none() {
                        first_broadcast_unsafe = Some(site);
                    }
                }
                work.step()?;
                Ok(())
            })?;
            Ok(OccurrencePropertyProof {
                fragment,
                _uses: uses,
                _calls: self,
                nodes,
                facts,
                first_broadcast_unsafe,
            })
        })()
    }
}
