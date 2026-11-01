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

//! Instance-local NDV sizing observations over already-computed full hashes.
//! Equality, exact counts, complete coverage and capacity decisions remain with
//! their original owners. This module owns no threads, queue, quota or codec.

use super::{KeySpec, LocalHashBatch, LocalHashPolicy, invariant, shared};
use crate::exec::pipeline::operator::InputConstructionOwner;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelEvaluationControl,
    builtin::aggregate_ds_hll_state::hll_heap_bytes,
    datasketches_hll::{HllHandle, HllTargetType},
    datasketches_hll_failure::{
        ClassifiedHllFailure, HllFailureDetail, HllFailureRecipe, HllFailureSink,
        HllInvariantRecipe, HllObservation,
    },
    opaque_memory::{OpaqueReservation, OpaqueRetainedCharge},
};
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_value_arrays::ValueViolation;
use std::sync::Arc;

const LOG_K: u8 = 12;
const TARGET: HllTargetType = HllTargetType::Hll8;

/// A private build/projection/partition name. Equal values do not authorize
/// merging: independently created domains remain distinct actual instances.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SizingDomainKind {
    WholeInput,
    Partition(usize),
}

struct Domain {
    spec: KeySpec,
    policy: LocalHashPolicy,
    kind: SizingDomainKind,
    _storage: ValueTypeStorageLifetime,
}

/// One actual, construction-funded domain shared by a build's partial sketches.
/// This identity neither asserts complete coverage nor grants resource capacity.
/// The original task graph creates it for the frozen input/projection and clones
/// this same instance into its partials. Exact completion remains with that graph.
#[derive(Clone)]
pub(super) struct SizingDomain(Arc<Domain>);
impl SizingDomain {
    pub(super) fn new(
        spec: &KeySpec,
        policy: &LocalHashPolicy,
        kind: SizingDomainKind,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        if !policy.accepts(spec) {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(Self(shared(
            owner,
            Domain {
                spec: spec.clone(),
                policy: policy.clone(),
                kind,
                _storage: owner.storage_lifetime(),
            },
        )?))
    }
    pub(super) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(super) fn spec(&self) -> &KeySpec {
        &self.0.spec
    }
    pub(super) fn policy(&self) -> &LocalHashPolicy {
        &self.0.policy
    }
    pub(super) fn kind(&self) -> SizingDomainKind {
        self.0.kind
    }
}

/// The caller funds the actual containing holder/container through its original
/// construction owner. Opaque retained owns only the library heap, so inline
/// HllHandle storage is not charged twice.
/// Field order destroys the live library handle before returning its heap charge.
pub(super) struct SizingSketch {
    handle: Option<HllHandle>,
    charge: OpaqueRetainedCharge,
    host: Arc<dyn AggregateStateAllocator>,
    domain: SizingDomain,
    failed: Option<ExecutionFailure>,
}

/// Move this entire value between task owners. Returning a bare Vec would lose
/// its real retained authority. The byte vector drops before its capacity charge.
pub(super) struct SizingPayload {
    bytes: Vec<u8>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Owns original serialized HLL capacity until bytes drop; field order is required."
        )
    )]
    charge: OpaqueRetainedCharge,
    domain: SizingDomain,
}
impl SizingPayload {
    #[cfg(test)]
    pub(super) fn retained_upper_bytes(&self) -> usize {
        self.charge.bytes()
    }
    pub(super) fn domain(&self) -> &SizingDomain {
        &self.domain
    }
}

// Actual decoder failure backing can outlive the operation. Retain only its
// already-reserved error headroom with the original source, independently of
// the destination's temporary union/decode headroom. The generic
// ExecutionFailure::from_domain capture keeps its existing Arc/BoundedDetail
// author; this source-string lease does not invoice those different metadata.
struct LibraryFailure {
    source: ClassifiedHllFailure,
    _lease: OpaqueReservation,
}
impl std::fmt::Debug for LibraryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LibraryFailure")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for LibraryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.source, f)
    }
}
impl std::error::Error for LibraryFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for LibraryFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.source)
    }
}
struct Sink<'a, 'control> {
    work: &'a mut EvaluationCheckpoints<'control>,
    library_failure_lease: Option<OpaqueReservation>,
}
impl HllFailureSink for Sink<'_, '_> {
    type Error = ExecutionFailure;
    fn failure(&mut self, recipe: HllFailureRecipe) -> ExecutionFailure {
        let carries_library_source = matches!(
            &recipe.detail,
            HllFailureDetail::Deserialize(_) | HllFailureDetail::Create(_)
        );
        let source = recipe.classify();
        if carries_library_source && let Some(lease) = self.library_failure_lease.take() {
            return ExecutionFailure::from_domain(LibraryFailure {
                source,
                _lease: lease,
            });
        }
        source.into()
    }
    fn invariant(&mut self, recipe: HllInvariantRecipe) -> ExecutionFailure {
        ClassifiedHllFailure::from(recipe).into()
    }
    fn observe(&mut self, event: HllObservation) -> ExecutionResult<()> {
        match event {
            HllObservation::Step => self.work.step(),
            HllObservation::OpaqueBoundary => self.work.flush(),
        }
        .map_err(ExecutionFailure::from)
    }
}

impl SizingSketch {
    pub(super) fn new(
        domain: &SizingDomain,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        let mut work = EvaluationCheckpoints::new(control);
        let mut sink = Sink {
            work: &mut work,
            library_failure_lease: None,
        };
        sink.observe(HllObservation::OpaqueBoundary)?;
        let mut charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let preflight = HllHandle::new_allocation_preflight_with_failure(LOG_K, TARGET, &mut sink)?;
        let mut lease = charge.reserve_operation(preflight.bounds().operation_peak_bytes)?;
        // Local declaration order also drops this handle before lease/charge if
        // creation reconciliation or its next control observation refuses.
        let (handle, outcome) =
            HllHandle::new_under_reservation_with_failure(&preflight, &lease, &mut sink)?;
        charge.reconcile_under_reservation(hll_heap_bytes(outcome.current_bytes), &mut lease)?;
        sink.observe(HllObservation::OpaqueBoundary)?;
        Ok(Self {
            handle: Some(handle),
            charge,
            host,
            domain: domain.clone(),
            failed: None,
        })
    }
    fn check(&self) -> ExecutionResult<()> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        if self.handle.is_none() {
            return Err(invariant(ValueViolation::Layout));
        }
        Ok(())
    }
    fn latch(&mut self, cause: ExecutionFailure) {
        if self.failed.is_none() {
            self.failed = Some(cause);
        }
        // Actual backing is destroyed while any local operation lease remains
        // live; only then can retained and unused headroom be returned.
        self.handle = None;
        self.charge.release_retained();
    }
    #[cfg(test)]
    pub(super) fn retained_upper_bytes(&self) -> usize {
        self.charge.bytes()
    }

    /// The private LocalHashBatch carries the original policy/input provenance.
    /// The iterator selects actual caller occurrences; it does not assert that
    /// the full domain is complete or grant a new range ownership token.
    /// hash=0 is a normal member. Only the original participation fact excludes
    /// an occurrence from indexable NDV; exact B/NULL/min/max live elsewhere.
    pub(super) fn update_occurrences(
        &mut self,
        hashes: &LocalHashBatch,
        occurrences: impl IntoIterator<Item = usize>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        self.check()?;
        let mut work = EvaluationCheckpoints::new(control);
        let mut sink = Sink {
            work: &mut work,
            library_failure_lease: None,
        };
        let result = (|| {
            sink.observe(HllObservation::OpaqueBoundary)?;
            hashes.validate(hashes.input(), &self.domain.0.policy)?;
            for occurrence in occurrences {
                sink.observe(HllObservation::Step)?;
                if hashes.participates(occurrence)? {
                    self.update_one(hashes.hash(occurrence)?, &mut sink)?;
                }
            }
            sink.observe(HllObservation::OpaqueBoundary)
        })();
        if let Err(cause) = &result {
            self.latch(cause.clone());
        }
        result
    }
    fn update_one(&mut self, hash: u64, sink: &mut Sink<'_, '_>) -> ExecutionResult<()> {
        let preflight = self
            .handle
            .as_ref()
            .ok_or_else(|| invariant(ValueViolation::Layout))?
            .update_hash_allocation_preflight_with_failure(sink)?;
        let mut lease = self
            .charge
            .reserve_operation(preflight.bounds().additional_headroom_bytes())?;
        let result = self
            .handle
            .as_mut()
            .ok_or_else(|| invariant(ValueViolation::Layout))?
            .update_hash_under_reservation_with_failure(hash, &preflight, &lease, sink);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(cause) => {
                self.latch(cause.clone());
                return Err(cause);
            }
        };
        // Every path after potential mutation destroys failed backing before
        // dropping lease. Do not let `?` release headroom ahead of that backing.
        if let Err(cause) = self
            .charge
            .reconcile_under_reservation(hll_heap_bytes(outcome.current_bytes), &mut lease)
        {
            let cause = ExecutionFailure::from(cause);
            self.latch(cause.clone());
            return Err(cause);
        }
        if let Err(cause) = sink.observe(HllObservation::OpaqueBoundary) {
            self.latch(cause.clone());
            return Err(cause);
        }
        Ok(())
    }

    /// Serializing is read-only on the live sketch. On failure the original
    /// method destroys any unpublished payload before this operation lease.
    pub(super) fn serialize_observed(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<SizingPayload> {
        self.check()?;
        let mut work = EvaluationCheckpoints::new(control);
        let mut sink = Sink {
            work: &mut work,
            library_failure_lease: None,
        };
        sink.observe(HllObservation::OpaqueBoundary)?;
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| invariant(ValueViolation::Layout))?;
        let preflight = handle.serialization_allocation_preflight_observed(&mut || {
            sink.observe(HllObservation::Step)
        })?;
        sink.observe(HllObservation::OpaqueBoundary)?;
        let mut charge = OpaqueRetainedCharge::try_new(Arc::clone(&self.host))?;
        let mut lease = charge.reserve_operation(preflight.bounds().additional_headroom_bytes)?;
        let bytes =
            handle.serialize_under_reservation_with_failure(&preflight, &lease, &mut sink)?;
        charge.reconcile_under_reservation(bytes.capacity(), &mut lease)?;
        sink.observe(HllObservation::OpaqueBoundary)?;
        Ok(SizingPayload {
            bytes,
            charge,
            domain: self.domain.clone(),
        })
    }
    /// Prefer this ownership handoff when a route partial no longer needs its
    /// sketch. The source handle is destroyed before its retained charge and
    /// the returned payload keeps only its own real capacity reservation.
    pub(super) fn seal(
        self,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<SizingPayload> {
        self.serialize_observed(control)
    }

    pub(super) fn merge_payload(
        &mut self,
        payload: SizingPayload,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<()> {
        self.check()?;
        let mut work = EvaluationCheckpoints::new(control);
        let mut sink = Sink {
            work: &mut work,
            library_failure_lease: None,
        };
        let result = (|| {
            sink.observe(HllObservation::OpaqueBoundary)?;
            if !self.domain.same_instance(&payload.domain) {
                return Err(invariant(ValueViolation::Layout));
            }
            let preflight = self
                .handle
                .as_ref()
                .ok_or_else(|| invariant(ValueViolation::Layout))?
                .merge_payload_allocation_preflight_with_failure(&payload.bytes, &mut sink)?;
            // Separate the retained original-error source from operation-only
            // temporary backing. The original author supplies error headroom
            // in addition to the success-operation bounds. Reserve both while
            // leaving the input payload with its existing source charge.
            let mut lease = self
                .charge
                .reserve_operation(preflight.bounds().additional_headroom_bytes())?;
            let error_headroom = preflight.library_error_headroom_bytes();
            sink.library_failure_lease = if error_headroom == 0 {
                None
            } else {
                Some(self.charge.reserve_operation(error_headroom)?)
            };
            let result = self
                .handle
                .as_mut()
                .ok_or_else(|| invariant(ValueViolation::Layout))?
                .merge_payload_under_reservation_with_failure(
                    &payload.bytes,
                    &preflight,
                    &lease,
                    &mut sink,
                );
            let outcome = match result {
                Ok(outcome) => outcome,
                Err(cause) => {
                    self.latch(cause.clone());
                    return Err(cause);
                }
            };
            if let Err(cause) = self
                .charge
                .reconcile_under_reservation(hll_heap_bytes(outcome.current_bytes), &mut lease)
            {
                let cause = ExecutionFailure::from(cause);
                self.latch(cause.clone());
                return Err(cause);
            }
            if let Err(cause) = sink.observe(HllObservation::OpaqueBoundary) {
                self.latch(cause.clone());
                return Err(cause);
            }
            Ok(())
        })();
        if let Err(cause) = &result {
            self.latch(cause.clone());
        }
        // payload stays alive across the original deserialize/union operation;
        // its capacity was excluded from destination preflight, not forgotten.
        result
    }

    /// A rounded library observation, not a complete-coverage NdvHint. The
    /// original task graph must prove completion before exposing such a hint.
    pub(super) fn estimate_observed(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<u64> {
        self.check()?;
        let mut work = EvaluationCheckpoints::new(control);
        let mut sink = Sink {
            work: &mut work,
            library_failure_lease: None,
        };
        sink.observe(HllObservation::OpaqueBoundary)?;
        let estimate = self
            .handle
            .as_ref()
            .ok_or_else(|| invariant(ValueViolation::Layout))?
            .estimate_with_failure(&mut sink)?;
        u64::try_from(estimate).map_err(|_| invariant(ValueViolation::Bounds))
    }
}
