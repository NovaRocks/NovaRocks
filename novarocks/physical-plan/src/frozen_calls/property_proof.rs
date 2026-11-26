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
use crate::{ExprId, ExprKind, ExprNode, PhysicalNode, PlanLimits, ValueDef, ValueId};
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
// Fixed header facts are available before any source traversal. Fragment
// already contains the relation-table header; count its backing entries below.
fn source_header_floor(
    fragment: &Fragment,
    calls: &FrozenFragmentCalls,
) -> Result<usize, FrozenCallError> {
    let mut bytes = std::mem::size_of_val(fragment);
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

// The production owner borrows the caller's original work scope. No entry or
// footer is created while walking the original unit/port source.
fn source_floor_in(
    fragment: &Fragment,
    calls: &FrozenFragmentCalls,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, FrozenCallError> {
    let mut bytes = source_header_floor(fragment, calls)?;
    // A unit Arc can be shared by several nodes. Without allocating a second
    // owner index before admission, the largest actual unit is an independent
    // shallow lower bound. The caller still invoices ALL source backing; this
    // floor never substitutes for that invoice or grants construction capacity.
    let mut largest_unit = 0usize;
    for node in fragment.nodes().values() {
        let current = node.expressions.as_ref();
        work.step()?;
        let Some(unit) = current else {
            continue;
        };
        let mut unit_bytes = std::mem::size_of_val(unit.as_ref());
        for (count, width) in [
            (unit.definitions().len(), std::mem::size_of::<ExprNode>()),
            (unit.ports().len(), std::mem::size_of::<crate::ExprPort>()),
            (unit.roots().len(), std::mem::size_of::<crate::ExprRoot>()),
            (
                unit.root_use_ids().len(),
                std::mem::size_of::<novarocks_type_contract::ExpressionUseId>(),
            ),
            (
                unit.control_flow().uses().len(),
                std::mem::size_of::<(
                    novarocks_type_contract::ExpressionUseId,
                    novarocks_type_contract::ExpressionInvocation<ExprId>,
                )>(),
            ),
            (
                unit.control_flow().regions().len(),
                std::mem::size_of::<(
                    novarocks_type_contract::EvaluationRegionId,
                    novarocks_type_contract::ExpressionEvaluationRegion,
                )>(),
            ),
            (
                unit.claims().len(),
                std::mem::size_of::<(
                    novarocks_type_contract::ExpressionUseId,
                    novarocks_expression_contract::ExpressionCallClaim,
                )>(),
            ),
            (
                unit.requests().len(),
                std::mem::size_of::<(ExprId, crate::PhysicalCallRequest)>(),
            ),
        ] {
            unit_bytes = count
                .checked_mul(width)
                .and_then(|n| unit_bytes.checked_add(n))
                .ok_or(FrozenCallError::TooManyItems)?;
        }
        for port in unit.ports() {
            unit_bytes = port
                .values
                .len()
                .checked_mul(std::mem::size_of::<(ValueId, crate::ValueType)>())
                .and_then(|n| unit_bytes.checked_add(n))
                .ok_or(FrozenCallError::TooManyItems)?;
            work.step()?;
        }
        largest_unit = largest_unit.max(unit_bytes);
    }
    bytes = bytes
        .checked_add(largest_unit)
        .ok_or(FrozenCallError::TooManyItems)?;
    Ok(bytes)
}

// Existing direct numerical tests use the same core through a test-only owner.
#[cfg(test)]
pub(super) fn source_floor(
    fragment: &Fragment,
    calls: &FrozenFragmentCalls,
) -> Result<usize, FrozenCallError> {
    struct FloorControl;
    impl PureCompileControl for FloorControl {
        fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    let control = FloorControl;
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)?;
    let result = source_floor_in(fragment, calls, &mut work);
    finish_frozen_calls(work, result)
}

#[derive(Clone, Copy, Debug)]
pub(super) enum OccurrenceSite {
    Expression {
        node: NodeId,
        invocation: novarocks_type_contract::ExpressionUseId,
    },
    Relation(PhysicalCallSite),
}
impl OccurrenceSite {
    pub(super) fn node(self) -> NodeId {
        match self {
            Self::Expression { node, .. } => node,
            Self::Relation(site) => site.node(),
        }
    }
    pub(super) fn failure(self) -> FrozenCallError {
        match self {
            Self::Expression { node, invocation } => {
                FrozenCallError::ReplicaExpression { node, invocation }
            }
            Self::Relation(site) => FrozenCallError::ReplicaEquivalence(site),
        }
    }
}
/// Deterministic actual occurrence order: node/local use, then original
/// relational node/call order. Local IDs from different nodes never collide.
pub(super) fn visit_occurrence_calls<E: From<FrozenCallError>>(
    fragment: &Fragment,
    calls: &FrozenFragmentCalls,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(
        OccurrenceSite,
        FunctionKind,
        &CallEffects,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>,
) -> Result<(), E> {
    for node in fragment.nodes().values() {
        if let Some(unit) = &node.expressions {
            for (id, invocation) in unit.control_flow().uses() {
                let definition = unit
                    .definition(invocation.definition)
                    .ok_or(FrozenCallError::InvalidSite)?;
                if let ExprKind::FunctionCall { function, .. } = &definition.kind {
                    let claim = unit.claims().get(id).ok_or(FrozenCallError::WrongControl)?;
                    visit(
                        OccurrenceSite::Expression {
                            node: node.id,
                            invocation: *id,
                        },
                        function.kind,
                        &claim.effects,
                        work,
                    )?;
                }
                work.step().map_err(FrozenCallError::Control)?;
            }
        }
        work.step().map_err(FrozenCallError::Control)?;
    }
    visit_relational_calls_observed(fragment, work, |site, binding, work| {
        let call = calls
            .entries
            .get(&site)
            .ok_or(FrozenCallError::MissingSite(site))?;
        visit(
            OccurrenceSite::Relation(site),
            binding.kind(),
            &call.effects,
            work,
        )
    })
}
#[derive(Default)]
struct UnitCounts {
    definitions: usize,
    uses: usize,
    references: usize,
    roots: usize,
    regions: usize,
    claims: usize,
    ports: usize,
}
impl UnitCounts {
    fn read(
        fragment: &Fragment,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FrozenCallError> {
        let mut counts = Self::default();
        for node in fragment.nodes().values() {
            if let Some(unit) = &node.expressions {
                for (count, amount) in [
                    (&mut counts.definitions, unit.definitions().len()),
                    (&mut counts.uses, unit.control_flow().uses().len()),
                    (
                        &mut counts.references,
                        unit.control_flow().use_reference_count(),
                    ),
                    (&mut counts.roots, unit.roots().len()),
                    (&mut counts.regions, unit.control_flow().regions().len()),
                    (&mut counts.claims, unit.claims().len()),
                    (&mut counts.ports, unit.ports().len()),
                ] {
                    *count = count
                        .checked_add(amount)
                        .ok_or(FrozenCallError::TooManyItems)?;
                }
            }
            work.step()?;
        }
        Ok(counts)
    }
}

#[derive(Clone, Copy)]
struct NodeProof {
    node: NodeId,
    first_unsafe: Option<OccurrenceSite>,
    declared_broadcast: bool,
}

/// A private projection of complete occurrence claims for exactly one borrowed
/// snapshot. It authenticates no installed owner and grants no allocation.
/// The immutable loans prevent replacement of any source while it is used.
pub(crate) struct OccurrencePropertyProof<'a> {
    fragment: &'a Fragment,
    _calls: &'a FrozenFragmentCalls,
    nodes: Vec<NodeProof>,
    facts: PropertyProofProjectionFacts,
    first_broadcast_unsafe: Option<OccurrenceSite>,
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
            Some(site) => Err(site.failure()),
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
        limits: &PlanLimits,
        source_retained_bytes: usize,
        projection_limits: PropertyProofProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<OccurrencePropertyProof<'a>, FrozenCallError> {
        self.property_proof_observed(
            fragment,
            limits,
            Some((source_retained_bytes, projection_limits)),
            control,
        )
    }

    pub(crate) fn property_proof_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        limits: &PlanLimits,
        admission: Option<(usize, PropertyProofProjectionLimits)>,
        control: &dyn PureCompileControl,
    ) -> Result<OccurrencePropertyProof<'a>, FrozenCallError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = self.property_proof_core(fragment, limits, admission, None, &mut work);
        finish_frozen_calls(work, result)
    }

    /// Project the same source on the caller's meter without entry or finish.
    /// This scope port does not invoice the opaque structural scratch owners.
    #[cfg(test)]
    pub(crate) fn property_proof_in<'a>(
        &'a self,
        fragment: &'a Fragment,
        limits: &PlanLimits,
        source_retained_bytes: usize,
        projection_limits: PropertyProofProjectionLimits,
        admit: &mut dyn FnMut(
            &novarocks_type_contract::ControlOwnedResourceFacts,
        ) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<OccurrencePropertyProof<'a>, FrozenCallError> {
        self.property_proof_with_admission_in(
            fragment,
            limits,
            Some((source_retained_bytes, projection_limits)),
            admit,
            work,
        )
    }

    pub(crate) fn property_proof_with_admission_in<'a>(
        &'a self,
        fragment: &'a Fragment,
        limits: &PlanLimits,
        admission: Option<(usize, PropertyProofProjectionLimits)>,
        admit: &mut ResourceAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<OccurrencePropertyProof<'a>, FrozenCallError> {
        self.property_proof_core(fragment, limits, admission, Some(admit), work)
    }

    fn property_proof_core<'a>(
        &'a self,
        fragment: &'a Fragment,
        limits: &PlanLimits,
        admission: Option<(usize, PropertyProofProjectionLimits)>,
        mut admit: Option<&mut ResourceAdmission<'_>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<OccurrencePropertyProof<'a>, FrozenCallError> {
        (|| {
            if !std::ptr::eq(self, fragment.calls()) {
                return Err(FrozenCallError::WrongFragment);
            }
            // These bounded header laws require no source scan. Keep their
            // ordinary refusal ahead of later controller observations.
            if fragment.nodes().len() > limits.fragment_nodes
                || fragment.values().len() > limits.fragment_values
                || self.entries.len() > MAX_CONTROL_USE_REFERENCES
            {
                return Err(FrozenCallError::TooManyItems);
            }
            if let Some((source, _)) = admission
                && source < source_header_floor(fragment, self)?
            {
                return Err(FrozenCallError::TooManyItems);
            }
            let counts = UnitCounts::read(fragment, work)?;
            // Bound source cardinalities before a source validator or the new
            // canonical node index allocates. Limits count definitions, not IDs.
            for (count, maximum) in [
                (fragment.nodes().len(), limits.fragment_nodes),
                (fragment.values().len(), limits.fragment_values),
                (counts.definitions, limits.fragment_expressions),
                (self.entries.len(), MAX_CONTROL_USE_REFERENCES),
                (counts.references, MAX_CONTROL_USE_REFERENCES),
                (counts.roots, MAX_CONTROL_USE_REFERENCES),
                (counts.roots, MAX_CONTROL_USE_REFERENCES),
            ] {
                let admitted = count <= maximum;
                work.step()?;
                if !admitted {
                    return Err(FrozenCallError::TooManyItems);
                }
            }
            let projected = (|| {
                if let Some((source, _)) = admission
                    && source < source_floor_in(fragment, self, work)?
                {
                    return Err(FrozenCallError::TooManyItems);
                }
                projection_facts(
                    fragment.nodes().len(),
                    counts.uses,
                    self.entries
                        .len()
                        .checked_add(counts.claims)
                        .ok_or(FrozenCallError::TooManyItems)?,
                    admission.map_or(0, |(source, _)| source),
                )
            })();
            work.step()?;
            let mut facts = projected?;
            let source_lookups = novarocks_type_contract::ControlResourceCounter::lookup_work(
                fragment.nodes().len(),
            )
            .map_err(resource_error)?;
            facts.projection_work = facts
                .projection_work
                .checked_add(
                    fragment
                        .nodes()
                        .len()
                        .checked_mul(
                            source_lookups
                                .checked_add(11)
                                .ok_or(FrozenCallError::TooManyItems)?,
                        )
                        .ok_or(FrozenCallError::TooManyItems)?,
                )
                .ok_or(FrozenCallError::TooManyItems)?;
            if let Some((_, limits)) = admission {
                facts.projection_work = facts
                    .projection_work
                    .checked_add(fragment.nodes().len())
                    .and_then(|n| n.checked_add(counts.ports))
                    .ok_or(FrozenCallError::TooManyItems)?;
                for (amount, maximum) in [
                    (facts.request_bytes, limits.max_request_bytes),
                    (facts.coexisting_bytes, limits.max_coexisting_bytes),
                    (facts.projection_work, limits.max_projection_work),
                ] {
                    let admitted = amount <= maximum;
                    work.step()?;
                    if !admitted {
                        return Err(FrozenCallError::TooManyItems);
                    }
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
            // Unit-owned control and scalar claim collections were counted by
            // add_fragment. Only the separate relation table remains here.
            source.add_items(call_items);
            work.step()?;
            source.validate("fragment.property_proof.resources", &mut errors);
            work.step()?;
            if !errors.is_empty() {
                return Err(FrozenCallError::TooManyItems);
            }
            work.flush()?;
            if let Some(admit) = admit.as_deref_mut() {
                let mut child = novarocks_type_contract::ControlOwnedResourceFacts::default();
                self.validate_fragment_in(
                    fragment,
                    &mut |facts| {
                        child = *facts;
                        admit(facts)
                    },
                    work,
                )?;
                if admission.is_none() {
                    let mut resources = novarocks_type_contract::ControlResourceCounter::default();
                    resources.merge(child).map_err(resource_error)?;
                    resources
                        .buffer::<NodeProof>(fragment.nodes().len(), 1)
                        .map_err(resource_error)?;
                    admit(&resources.facts())?;
                }
            } else {
                self.validate_fragment(fragment, work.control())?;
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
            visit_occurrence_calls::<FrozenCallError>(
                fragment,
                self,
                work,
                |site, kind, effects, work| {
                    let owner = site.node();
                    let index = find_node(&nodes, owner, work)?;
                    let safe = super::replica::call_is_replica_equivalent(kind, effects);
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
                },
            )?;
            Ok(OccurrencePropertyProof {
                fragment,
                _calls: self,
                nodes,
                facts,
                first_broadcast_unsafe,
            })
        })()
    }
}
