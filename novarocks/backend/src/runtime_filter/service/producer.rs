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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::ThreadId;

use crate::runtime_filter::core::channel::{FinalDomainRejection, RuntimeFilterChannel};
use novarocks::runtime_filter_transition::codec::contribution::{
    ContributionCodecError, RuntimeFilterContribution, encode_contribution,
    encoded_contribution_len, semantic_contribution_bytes, validate_contribution_contract,
};
use novarocks::runtime_filter_transition::codec::producer::encode_producer_failure;
use novarocks::runtime_filter_transition::model::contract::{BindingId, ChannelId};
use novarocks::runtime_filter_transition::port::events::{
    RuntimeFilterEventIdentity, TransportRouteEventIdentity,
};
#[cfg(test)]
use novarocks::runtime_filter_transition::port::final_domain::CompletionFenceAuthority;
use novarocks::runtime_filter_transition::port::final_domain::FinalDomainShard;
use novarocks::runtime_filter_transition::port::identity::{
    DeploymentEpoch, PartitionId, ProducerSequence, ProducerStreamId, RuntimeFilterParticipantId,
};
use novarocks::runtime_filter_transition::port::ordered_bound::OrderedBoundUpdate;
use novarocks::runtime_filter_transition::port::producer::{
    FinalDomainProducerAdapter, OrderedBoundProducerAdapter, ProducerAdapter,
    ProducerFailureReason, RuntimeContractViolation, RuntimeContractViolationKind, SubmitOutcome,
    TopKSummaryProducerAdapter,
};
use novarocks::runtime_filter_transition::port::routing::RuntimeFilterRemoteRoute;
use novarocks::runtime_filter_transition::port::support::{
    RuntimeFilterMemoryAccount, TemporaryContributionLease,
};
use novarocks::runtime_filter_transition::port::topk_summary::TopKSummary;
use novarocks::runtime_filter_transition::port::transport::{
    ContributionRouteIdentity, ProducerInstanceRouteIdentity, ProducerOpenMetadata,
    RuntimeFilterEnvelope, RuntimeFilterEnvelopeKind, RuntimeFilterRouteIdentity,
};
use novarocks::runtime_filter_transition::port::value_domain::ValueDomainDelta;
use novarocks_types::UniqueId;

use super::ActionDispatcher;
use super::registry::InboundProducerContract;
use super::reliable_transport::{
    ReliableEnvelopeTransport, ReliableSendError, ReliableSendOutcome,
};

pub(super) struct RemoteProducerState {
    epoch: DeploymentEpoch,
    route: RuntimeFilterRemoteRoute,
    kind: novarocks::runtime_filter_transition::port::producer::ProducerPortKind,
    local_partition_count: u32,
    lifecycle: Mutex<RemoteProducerLifecycle>,
    lifecycle_wake: Condvar,
}

struct RemoteProducerLifecycle {
    failed: bool,
    terminal_sequences: Vec<Option<ProducerSequence>>,
    in_flight_owner: Option<ThreadId>,
    deferred_failure: Option<DeferredProducerFailure>,
}

#[derive(Clone, Copy)]
struct DeferredProducerFailure {
    reason: ProducerFailureReason,
    adapter_fail_open: bool,
}

impl DeferredProducerFailure {
    const fn adapter(reason: ProducerFailureReason) -> Self {
        Self {
            reason,
            adapter_fail_open: true,
        }
    }

    const fn state_only(reason: ProducerFailureReason) -> Self {
        Self {
            reason,
            adapter_fail_open: false,
        }
    }

    fn merge_flags(&mut self, later: Self) {
        // First failure wins the reason; later observations may only strengthen the
        // requirement that the adapter emit the fail-open side effects.
        self.adapter_fail_open |= later.adapter_fail_open;
    }
}

enum CloseOperationAdmission {
    TerminalNoop,
    Duplicate,
    Permit(RemoteProducerOperationPermit),
}

struct RemoteProducerOperationPermit {
    state: Arc<RemoteProducerState>,
    owner: ThreadId,
    initial_failure: Option<DeferredProducerFailure>,
    active: bool,
}

impl RemoteProducerState {
    pub(super) fn new(
        epoch: DeploymentEpoch,
        route: RuntimeFilterRemoteRoute,
        kind: novarocks::runtime_filter_transition::port::producer::ProducerPortKind,
        local_partition_count: u32,
    ) -> Self {
        Self {
            epoch,
            route,
            kind,
            local_partition_count,
            lifecycle: Mutex::new(RemoteProducerLifecycle {
                failed: false,
                terminal_sequences: vec![None; local_partition_count as usize],
                in_flight_owner: None,
                deferred_failure: None,
            }),
            lifecycle_wake: Condvar::new(),
        }
    }

    pub(super) fn validate_open(
        &self,
        epoch: DeploymentEpoch,
        route: &RuntimeFilterRemoteRoute,
        kind: novarocks::runtime_filter_transition::port::producer::ProducerPortKind,
        local_partition_count: u32,
    ) -> Result<(), RuntimeContractViolation> {
        if self.kind != kind {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ProducerPortMismatch,
                "remote producer kind differs from the frozen open contract",
            ));
        }
        if self.local_partition_count != local_partition_count {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::PartitionCountConflict,
                "remote producer partition count differs from the frozen open contract",
            ));
        }
        if self.epoch != epoch || self.route != *route {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ServiceUnavailable,
                "remote producer epoch or route differs from the frozen open contract",
            ));
        }
        Ok(())
    }

    fn wait_for_operation<'a>(
        &'a self,
        mut lifecycle: MutexGuard<'a, RemoteProducerLifecycle>,
        owner: ThreadId,
        reentrant_failure: DeferredProducerFailure,
    ) -> Option<MutexGuard<'a, RemoteProducerLifecycle>> {
        loop {
            if lifecycle.failed {
                return None;
            }
            match lifecycle.in_flight_owner {
                Some(in_flight) if in_flight == owner => {
                    if let Some(existing) = lifecycle.deferred_failure.as_mut() {
                        existing.merge_flags(reentrant_failure);
                    } else {
                        lifecycle.deferred_failure = Some(reentrant_failure);
                    }
                    return None;
                }
                Some(_) => {
                    lifecycle = self
                        .lifecycle_wake
                        .wait(lifecycle)
                        .unwrap_or_else(|error| error.into_inner());
                }
                None => return Some(lifecycle),
            }
        }
    }

    fn acquire_submit(
        self: &Arc<Self>,
        partition_id: PartitionId,
        sequence: ProducerSequence,
    ) -> Result<Option<RemoteProducerOperationPermit>, RuntimeContractViolation> {
        let owner = std::thread::current().id();
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(mut lifecycle) = self.wait_for_operation(
            lifecycle,
            owner,
            DeferredProducerFailure::adapter(ProducerFailureReason::UpstreamUnavailable),
        ) else {
            return Ok(None);
        };
        if lifecycle.terminal_sequences[partition_id.get() as usize]
            .is_some_and(|terminal| sequence >= terminal)
        {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "delta sequence is outside the exclusive terminal range",
            ));
        }
        lifecycle.in_flight_owner = Some(owner);
        drop(lifecycle);
        Ok(Some(RemoteProducerOperationPermit::new(
            Arc::clone(self),
            owner,
            None,
        )))
    }

    fn acquire_close(
        self: &Arc<Self>,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<CloseOperationAdmission, RuntimeContractViolation> {
        let owner = std::thread::current().id();
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(mut lifecycle) = self.wait_for_operation(
            lifecycle,
            owner,
            DeferredProducerFailure::adapter(ProducerFailureReason::UpstreamUnavailable),
        ) else {
            return Ok(CloseOperationAdmission::TerminalNoop);
        };
        if let Some(previous) = lifecycle.terminal_sequences[partition_id.get() as usize] {
            return if previous == terminal_sequence {
                Ok(CloseOperationAdmission::Duplicate)
            } else {
                Err(RuntimeContractViolation::new(
                    RuntimeContractViolationKind::ConflictingTerminalSequence,
                    "partition close replay changed terminal sequence",
                ))
            };
        }
        lifecycle.in_flight_owner = Some(owner);
        drop(lifecycle);
        Ok(CloseOperationAdmission::Permit(
            RemoteProducerOperationPermit::new(Arc::clone(self), owner, None),
        ))
    }

    fn acquire_fail(
        self: &Arc<Self>,
        reason: ProducerFailureReason,
    ) -> Option<RemoteProducerOperationPermit> {
        let owner = std::thread::current().id();
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let failure = DeferredProducerFailure::adapter(reason);
        let mut lifecycle = self.wait_for_operation(lifecycle, owner, failure)?;
        lifecycle.in_flight_owner = Some(owner);
        drop(lifecycle);
        Some(RemoteProducerOperationPermit::new(
            Arc::clone(self),
            owner,
            Some(failure),
        ))
    }

    pub(super) fn mark_failed(&self) {
        let owner = std::thread::current().id();
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(mut lifecycle) = self.wait_for_operation(
            lifecycle,
            owner,
            DeferredProducerFailure::state_only(ProducerFailureReason::UpstreamUnavailable),
        ) else {
            return;
        };
        lifecycle.failed = true;
    }
}

impl RemoteProducerOperationPermit {
    fn new(
        state: Arc<RemoteProducerState>,
        owner: ThreadId,
        initial_failure: Option<DeferredProducerFailure>,
    ) -> Self {
        Self {
            state,
            owner,
            initial_failure,
            active: true,
        }
    }

    fn finish(
        mut self,
        terminal: Option<(PartitionId, ProducerSequence)>,
        later_failure: Option<DeferredProducerFailure>,
    ) -> Option<DeferredProducerFailure> {
        let mut lifecycle = self
            .state
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        debug_assert_eq!(lifecycle.in_flight_owner, Some(self.owner));
        let deferred = lifecycle.deferred_failure.take();
        let mut failure = self.initial_failure.take();
        for observed in [deferred, later_failure].into_iter().flatten() {
            if let Some(existing) = failure.as_mut() {
                existing.merge_flags(observed);
            } else {
                failure = Some(observed);
            }
        }
        if failure.is_some() {
            lifecycle.failed = true;
        } else if let Some((partition_id, terminal_sequence)) = terminal {
            lifecycle.terminal_sequences[partition_id.get() as usize] = Some(terminal_sequence);
        }
        lifecycle.in_flight_owner = None;
        self.active = false;
        drop(lifecycle);
        self.state.lifecycle_wake.notify_all();
        failure
    }
}

impl Drop for RemoteProducerOperationPermit {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut lifecycle = self
            .state
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if lifecycle.in_flight_owner == Some(self.owner) {
            lifecycle.failed = true;
            lifecycle.deferred_failure = None;
            lifecycle.in_flight_owner = None;
        }
        drop(lifecycle);
        self.state.lifecycle_wake.notify_all();
        self.active = false;
    }
}

pub(super) struct RemoteProducerAdapter {
    query_id: UniqueId,
    participant_id: RuntimeFilterParticipantId,
    channel_id: ChannelId,
    epoch: DeploymentEpoch,
    route: RuntimeFilterRemoteRoute,
    channel: Arc<RuntimeFilterChannel>,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    local_partition_count: u32,
    contract: InboundProducerContract,
    transport: Arc<ReliableEnvelopeTransport>,
    dispatcher: Arc<ActionDispatcher>,
    state: Arc<RemoteProducerState>,
    failed_open: AtomicBool,
}

impl RemoteProducerAdapter {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        channel_id: ChannelId,
        epoch: DeploymentEpoch,
        route: RuntimeFilterRemoteRoute,
        channel: Arc<RuntimeFilterChannel>,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        local_partition_count: u32,
        contract: InboundProducerContract,
        transport: Arc<ReliableEnvelopeTransport>,
        dispatcher: Arc<ActionDispatcher>,
        state: Arc<RemoteProducerState>,
    ) -> Self {
        Self {
            query_id,
            participant_id,
            channel_id,
            epoch,
            route,
            channel,
            binding_id,
            fragment_instance_id,
            local_partition_count,
            contract,
            transport,
            dispatcher,
            state,
            failed_open: AtomicBool::new(false),
        }
    }

    fn event_identity(&self) -> TransportRouteEventIdentity {
        TransportRouteEventIdentity::new(
            RuntimeFilterEventIdentity::new(
                self.query_id,
                self.participant_id,
                self.channel_id,
                self.epoch,
            ),
            self.route.route_edge_id(),
        )
    }

    fn preflight(&self, partition_id: PartitionId) -> Result<(), RuntimeContractViolation> {
        self.channel.preflight_remote_open(
            self.binding_id,
            self.fragment_instance_id,
            self.local_partition_count,
            partition_id,
        )
    }

    fn dispatch_failure_once(&self, reason: ProducerFailureReason) {
        if self.failed_open.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Ok(action) =
            self.channel
                .fail_instance(self.binding_id, self.fragment_instance_id, reason)
        {
            let _ = self.dispatcher.dispatch(self.channel_id, action);
        }
    }

    fn complete_operation_failure(
        &self,
        failure: DeferredProducerFailure,
        unavailable_already_sent: bool,
    ) {
        if !failure.adapter_fail_open {
            return;
        }
        if !unavailable_already_sent && let Ok(envelope) = self.failure_envelope(failure.reason) {
            let _ = self
                .transport
                .send_envelope(&self.route, envelope, self.event_identity());
        }
        self.dispatch_failure_once(failure.reason);
    }

    fn fail_operation(
        &self,
        permit: RemoteProducerOperationPermit,
        reason: ProducerFailureReason,
    ) -> SubmitOutcome {
        let failure = permit
            .finish(None, Some(DeferredProducerFailure::adapter(reason)))
            .expect("an explicitly failed operation records a failure");
        self.complete_operation_failure(failure, false);
        SubmitOutcome::TerminalNoop
    }

    fn finish_operation_error(
        &self,
        permit: RemoteProducerOperationPermit,
        violation: RuntimeContractViolation,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        if let Some(failure) = permit.finish(None, None) {
            self.complete_operation_failure(failure, false);
            Ok(SubmitOutcome::TerminalNoop)
        } else {
            Err(violation)
        }
    }

    fn codec_is_resource(error: ContributionCodecError) -> bool {
        matches!(
            error,
            ContributionCodecError::LengthOverflow
                | ContributionCodecError::EncodedSizeExceeded
                | ContributionCodecError::ResourceLimit
        )
    }

    fn codec_violation(error: ContributionCodecError) -> RuntimeContractViolation {
        RuntimeContractViolation::new(
            RuntimeContractViolationKind::TypeMismatch,
            error.to_string(),
        )
    }

    fn conflicting_replay() -> RuntimeContractViolation {
        RuntimeContractViolation::new(
            RuntimeContractViolationKind::ConflictingReplay,
            "producer message identity was reused with different content",
        )
    }

    fn send_result_requires_fail_open(
        outcome: &Result<ReliableSendOutcome, ReliableSendError>,
    ) -> bool {
        matches!(
            outcome,
            Ok(ReliableSendOutcome::ResourceLimit(_) | ReliableSendOutcome::Shutdown)
                | Err(ReliableSendError::RetiredIdentity)
        )
    }

    fn send_contribution(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        contribution: RuntimeFilterContribution,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.preflight(partition_id)?;
        let stream =
            ProducerStreamId::new(self.binding_id, self.fragment_instance_id, partition_id);
        validate_contribution_contract(
            &contribution,
            self.contract.codec_expectation(stream, sequence),
        )
        .map_err(Self::codec_violation)?;
        let Some(permit) = self.state.acquire_submit(partition_id, sequence)? else {
            return Ok(SubmitOutcome::TerminalNoop);
        };
        let encoded_bytes = match encoded_contribution_len(
            &contribution,
            self.contract.codec_expectation(stream, sequence),
        ) {
            Ok(bytes) => bytes,
            Err(error) if Self::codec_is_resource(error) => {
                return Ok(self.fail_operation(permit, ProducerFailureReason::UpstreamUnavailable));
            }
            Err(error) => {
                let failure = permit.finish(None, None);
                if let Some(failure) = failure {
                    self.complete_operation_failure(failure, false);
                    return Ok(SubmitOutcome::TerminalNoop);
                }
                return Err(Self::codec_violation(error));
            }
        };
        let semantic_bytes = match semantic_contribution_bytes(&contribution) {
            Ok(bytes) => bytes,
            Err(error) if Self::codec_is_resource(error) => {
                return Ok(self.fail_operation(permit, ProducerFailureReason::UpstreamUnavailable));
            }
            Err(error) => {
                let failure = permit.finish(None, None);
                if let Some(failure) = failure {
                    self.complete_operation_failure(failure, false);
                    return Ok(SubmitOutcome::TerminalNoop);
                }
                return Err(Self::codec_violation(error));
            }
        };
        if semantic_bytes > self.contract.limits().max_contribution_bytes()
            || encoded_bytes > self.contract.limits().max_encoded_bytes()
        {
            return Ok(self.fail_operation(permit, ProducerFailureReason::UpstreamUnavailable));
        }
        let encoded = match encode_contribution(
            &contribution,
            self.contract.codec_expectation(stream, sequence),
            self.contract.limits().max_encoded_bytes(),
        ) {
            Ok(encoded) => encoded,
            Err(error) if Self::codec_is_resource(error) => {
                return Ok(self.fail_operation(permit, ProducerFailureReason::UpstreamUnavailable));
            }
            Err(error) => {
                let failure = permit.finish(None, None);
                if let Some(failure) = failure {
                    self.complete_operation_failure(failure, false);
                    return Ok(SubmitOutcome::TerminalNoop);
                }
                return Err(Self::codec_violation(error));
            }
        };
        let (schema_digest, payload) = encoded.into_parts();
        let route_identity = match ContributionRouteIdentity::try_new(
            self.binding_id,
            self.fragment_instance_id,
            partition_id,
            sequence,
        ) {
            Ok(identity) => RuntimeFilterRouteIdentity::contribution(identity),
            Err(error) => {
                return self.finish_operation_error(
                    permit,
                    RuntimeContractViolation::new(
                        RuntimeContractViolationKind::InvalidPartition,
                        error.to_string(),
                    ),
                );
            }
        };
        let producer_open = match ProducerOpenMetadata::try_new(self.local_partition_count) {
            Ok(open) => open,
            Err(error) => {
                return self.finish_operation_error(
                    permit,
                    RuntimeContractViolation::new(
                        RuntimeContractViolationKind::InvalidPartitionCount,
                        error.to_string(),
                    ),
                );
            }
        };
        let envelope = match RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::Contribution,
            self.query_id,
            self.channel_id,
            self.epoch,
            route_identity,
            Some(producer_open),
            None,
            &schema_digest,
            payload,
        ) {
            Ok(envelope) => envelope,
            Err(error) => {
                return self.finish_operation_error(
                    permit,
                    RuntimeContractViolation::new(
                        RuntimeContractViolationKind::TypeMismatch,
                        error.to_string(),
                    ),
                );
            }
        };
        let outcome =
            self.transport
                .send_envelope(&self.route, Arc::new(envelope), self.event_identity());
        if Self::send_result_requires_fail_open(&outcome) {
            return Ok(self.fail_operation(permit, ProducerFailureReason::UpstreamUnavailable));
        }
        match outcome {
            Ok(ReliableSendOutcome::Buffered(_)) => {
                if let Some(failure) = permit.finish(None, None) {
                    self.complete_operation_failure(failure, false);
                    Ok(SubmitOutcome::TerminalNoop)
                } else {
                    Ok(SubmitOutcome::Applied)
                }
            }
            Err(ReliableSendError::IdentityConflict) => {
                if let Some(failure) = permit.finish(None, None) {
                    self.complete_operation_failure(failure, false);
                    Ok(SubmitOutcome::TerminalNoop)
                } else {
                    Err(Self::conflicting_replay())
                }
            }
            Ok(ReliableSendOutcome::ResourceLimit(_) | ReliableSendOutcome::Shutdown)
            | Err(ReliableSendError::RetiredIdentity) => unreachable!("handled above"),
        }
    }

    fn send_close(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.preflight(partition_id)?;
        let route_identity = RuntimeFilterRouteIdentity::contribution(
            ContributionRouteIdentity::try_new(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                terminal_sequence,
            )
            .map_err(|error| {
                RuntimeContractViolation::new(
                    RuntimeContractViolationKind::InvalidPartition,
                    error.to_string(),
                )
            })?,
        );
        let envelope = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            self.query_id,
            self.channel_id,
            self.epoch,
            route_identity,
            Some(
                ProducerOpenMetadata::try_new(self.local_partition_count).map_err(|error| {
                    RuntimeContractViolation::new(
                        RuntimeContractViolationKind::InvalidPartitionCount,
                        error.to_string(),
                    )
                })?,
            ),
            None,
            &self.contract.schema_digest(),
            Vec::new(),
        )
        .map_err(|error| {
            RuntimeContractViolation::new(
                RuntimeContractViolationKind::TypeMismatch,
                error.to_string(),
            )
        })?;
        let permit = match self.state.acquire_close(partition_id, terminal_sequence)? {
            CloseOperationAdmission::TerminalNoop => return Ok(SubmitOutcome::TerminalNoop),
            CloseOperationAdmission::Duplicate => return Ok(SubmitOutcome::Duplicate),
            CloseOperationAdmission::Permit(permit) => permit,
        };
        let outcome =
            self.transport
                .send_envelope(&self.route, Arc::new(envelope), self.event_identity());
        if Self::send_result_requires_fail_open(&outcome) {
            return Ok(self.fail_operation(permit, ProducerFailureReason::UpstreamUnavailable));
        }
        match outcome {
            Ok(ReliableSendOutcome::Buffered(_)) => {
                if let Some(failure) = permit.finish(Some((partition_id, terminal_sequence)), None)
                {
                    self.complete_operation_failure(failure, false);
                    Ok(SubmitOutcome::TerminalNoop)
                } else {
                    Ok(SubmitOutcome::Applied)
                }
            }
            Err(ReliableSendError::IdentityConflict) => {
                if let Some(failure) = permit.finish(None, None) {
                    self.complete_operation_failure(failure, false);
                    Ok(SubmitOutcome::TerminalNoop)
                } else {
                    Err(Self::conflicting_replay())
                }
            }
            Ok(ReliableSendOutcome::ResourceLimit(_) | ReliableSendOutcome::Shutdown)
            | Err(ReliableSendError::RetiredIdentity) => unreachable!("handled above"),
        }
    }

    fn failure_envelope(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<Arc<RuntimeFilterEnvelope>, RuntimeContractViolation> {
        let route_identity = RuntimeFilterRouteIdentity::producer_instance(
            ProducerInstanceRouteIdentity::try_new(self.binding_id, self.fragment_instance_id)
                .map_err(|error| {
                    RuntimeContractViolation::new(
                        RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                        error.to_string(),
                    )
                })?,
        );
        let envelope = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::ProducerUnavailable,
            self.query_id,
            self.channel_id,
            self.epoch,
            route_identity,
            None,
            None,
            &self.contract.schema_digest(),
            encode_producer_failure(reason),
        )
        .map_err(|error| {
            RuntimeContractViolation::new(
                RuntimeContractViolationKind::TypeMismatch,
                error.to_string(),
            )
        })?;
        Ok(Arc::new(envelope))
    }

    fn send_failure(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let envelope = self.failure_envelope(reason)?;
        let Some(permit) = self.state.acquire_fail(reason) else {
            return Ok(SubmitOutcome::TerminalNoop);
        };
        let outcome = self
            .transport
            .send_envelope(&self.route, envelope, self.event_identity());
        let failure = permit
            .finish(None, None)
            .expect("an explicit fail operation carries its initial failure");
        self.complete_operation_failure(failure, true);
        match outcome {
            Ok(ReliableSendOutcome::Buffered(_)) => Ok(SubmitOutcome::Applied),
            Ok(ReliableSendOutcome::ResourceLimit(_) | ReliableSendOutcome::Shutdown)
            | Err(ReliableSendError::RetiredIdentity) => Ok(SubmitOutcome::TerminalNoop),
            Err(ReliableSendError::IdentityConflict) => Err(Self::conflicting_replay()),
        }
    }
}

pub(super) struct ServiceProducerAdapter {
    channel_id: ChannelId,
    channel: Arc<RuntimeFilterChannel>,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    dispatcher: Arc<ActionDispatcher>,
    final_domain_authorized: bool,
    #[cfg(test)]
    final_domain_authority: Mutex<Option<CompletionFenceAuthority>>,
    #[cfg(test)]
    before_dispatch: std::sync::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    final_domain_submit_failure: Mutex<Option<(PartitionId, ProducerSequence)>>,
}

impl ServiceProducerAdapter {
    pub(super) fn new(
        channel_id: ChannelId,
        channel: Arc<RuntimeFilterChannel>,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
        dispatcher: Arc<ActionDispatcher>,
        final_domain_authorized: bool,
        #[cfg(test)] final_domain_authority: Option<CompletionFenceAuthority>,
    ) -> Self {
        Self {
            channel_id,
            channel,
            binding_id,
            fragment_instance_id,
            memory_account,
            dispatcher,
            final_domain_authorized,
            #[cfg(test)]
            final_domain_authority: Mutex::new(final_domain_authority),
            #[cfg(test)]
            before_dispatch: std::sync::Mutex::new(None),
            #[cfg(test)]
            final_domain_submit_failure: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(super) fn set_before_dispatch(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.before_dispatch.lock().unwrap() = Some(hook);
    }

    #[cfg(test)]
    pub(super) fn inject_final_domain_submit_failure(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
    ) {
        *self
            .final_domain_submit_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((partition_id, sequence));
    }

    #[cfg(test)]
    fn take_final_domain_submit_failure(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
    ) -> bool {
        let mut failure = self
            .final_domain_submit_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if *failure == Some((partition_id, sequence)) {
            *failure = None;
            true
        } else {
            false
        }
    }

    #[cfg(test)]
    pub(super) fn final_domain_test_issuer(
        &self,
        open_drivers: u32,
    ) -> Option<
        novarocks::runtime_filter_transition::port::final_domain::CollectingFinalDomainTestIssuer,
    > {
        self.final_domain_authority
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .map(|authority| {
                novarocks::runtime_filter_transition::port::final_domain::CollectingFinalDomainTestIssuer::new(
                    authority,
                    open_drivers,
                )
            })
    }

    fn finish(
        &self,
        action: crate::runtime_filter::core::channel::ChannelAction,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let outcome = action.outcome();
        #[cfg(test)]
        self.dispatcher
            .reserve_core_before_hook(self.channel_id, &action);
        #[cfg(test)]
        let hook = self.before_dispatch.lock().unwrap().take();
        #[cfg(test)]
        if let Some(hook) = hook {
            hook();
        }
        self.dispatcher.dispatch(self.channel_id, action)?;
        Ok(outcome)
    }

    fn finish_ordered(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        result: Result<
            crate::runtime_filter::core::channel::ChannelAction,
            RuntimeContractViolation,
        >,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        match result {
            Ok(action) => self.finish(action),
            Err(error) => {
                let identity = self.channel.contribution_identity(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                );
                let action = self
                    .channel
                    .ordered_rejection_action(identity, error.kind());
                self.dispatcher
                    .dispatch(self.channel_id, action)
                    .expect("ordered rejection-only dispatch cannot materialize or route");
                Err(error)
            }
        }
    }

    fn finish_topk(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        result: Result<
            crate::runtime_filter::core::channel::ChannelAction,
            RuntimeContractViolation,
        >,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        match result {
            Ok(action) => self.finish(action),
            Err(error) => {
                let identity = self.channel.contribution_identity(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                );
                let action = self.channel.topk_rejection_action(identity, error.kind());
                self.dispatcher
                    .dispatch(self.channel_id, action)
                    .expect("top-k rejection-only dispatch cannot materialize or route");
                Err(error)
            }
        }
    }

    fn finish_final(
        &self,
        result: Result<crate::runtime_filter::core::channel::ChannelAction, FinalDomainRejection>,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        match result {
            Ok(action) => self.finish(action),
            Err(rejection) => {
                let (error, action) = rejection.into_parts();
                self.dispatcher
                    .dispatch(self.channel_id, action)
                    .expect("final-domain rejection-only dispatch cannot materialize or route");
                Err(error)
            }
        }
    }
}

impl ProducerAdapter for ServiceProducerAdapter {
    fn submit(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        delta: ValueDomainDelta,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .authorize_submit(self.binding_id, self.fragment_instance_id, partition_id)?;
        let Ok(bytes) = delta.estimated_contribution_bytes() else {
            return self
                .channel
                .reject_submit_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &delta,
                )
                .and_then(|action| self.finish(action));
        };
        let Ok(lease) = TemporaryContributionLease::try_new(self.memory_account.clone(), bytes)
        else {
            return self
                .channel
                .reject_submit_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &delta,
                )
                .and_then(|action| self.finish(action));
        };
        self.channel
            .submit(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                sequence,
                delta,
                lease,
            )
            .and_then(|action| self.finish(action))
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .close_partition(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                terminal_sequence,
            )
            .and_then(|action| self.finish(action))
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .fail_instance(self.binding_id, self.fragment_instance_id, reason)
            .and_then(|action| self.finish(action))
    }
}

impl FinalDomainProducerAdapter for ServiceProducerAdapter {
    fn complete(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        shard: FinalDomainShard,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        #[cfg(test)]
        if self.take_final_domain_submit_failure(partition_id, sequence) {
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ServiceUnavailable,
                "injected selected final-domain submit failure",
            ));
        }
        let result = (|| {
            self.channel.authorize_final(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                sequence,
                self.final_domain_authorized,
                &shard,
            )?;
            let Some(bytes) = shard.canonical_contribution_bytes() else {
                return self.channel.reject_final_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &shard,
                );
            };
            let Ok(lease) = TemporaryContributionLease::try_new(self.memory_account.clone(), bytes)
            else {
                return self.channel.reject_final_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &shard,
                );
            };
            self.channel.complete_final(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                sequence,
                shard,
                lease,
            )
        })();
        self.finish_final(result)
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .close_final_partition(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                terminal_sequence,
            )
            .and_then(|action| self.finish(action))
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .fail_instance(self.binding_id, self.fragment_instance_id, reason)
            .and_then(|action| self.finish(action))
    }
}

impl OrderedBoundProducerAdapter for ServiceProducerAdapter {
    fn submit_bound(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        update: OrderedBoundUpdate,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let result = (|| {
            self.channel.authorize_submit(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
            )?;
            let Some(bytes) = update.canonical_contribution_bytes() else {
                return self.channel.reject_ordered_submit_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &update,
                );
            };
            let Ok(lease) = TemporaryContributionLease::try_new(self.memory_account.clone(), bytes)
            else {
                return self.channel.reject_ordered_submit_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &update,
                );
            };
            self.channel.submit_ordered(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                sequence,
                update,
                lease,
            )
        })();
        self.finish_ordered(partition_id, sequence, result)
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .close_ordered_partition(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                terminal_sequence,
            )
            .and_then(|action| self.finish(action))
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .fail_instance(self.binding_id, self.fragment_instance_id, reason)
            .and_then(|action| self.finish(action))
    }
}

impl TopKSummaryProducerAdapter for ServiceProducerAdapter {
    fn submit_summary(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        summary: TopKSummary,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let result = (|| {
            self.channel.authorize_submit(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
            )?;
            let bytes = summary.canonical_contribution_bytes().ok_or_else(|| {
                RuntimeContractViolation::new(
                    RuntimeContractViolationKind::InvalidContributionLease,
                    "top-k summary canonical size overflowed",
                )
            })?;
            let Ok(lease) = TemporaryContributionLease::try_new(self.memory_account.clone(), bytes)
            else {
                return self.channel.reject_topk_submit_resource_exhausted(
                    self.binding_id,
                    self.fragment_instance_id,
                    partition_id,
                    sequence,
                    &summary,
                );
            };
            self.channel.submit_topk_summary(
                self.binding_id,
                self.fragment_instance_id,
                partition_id,
                sequence,
                summary,
                lease,
            )
        })();
        self.finish_topk(partition_id, sequence, result)
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let result = self.channel.close_topk_partition(
            self.binding_id,
            self.fragment_instance_id,
            partition_id,
            terminal,
        );
        self.finish_topk(partition_id, terminal, result)
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.channel
            .fail_instance(self.binding_id, self.fragment_instance_id, reason)
            .and_then(|action| self.finish(action))
    }
}

impl ProducerAdapter for RemoteProducerAdapter {
    fn submit(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        delta: ValueDomainDelta,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_contribution(
            partition_id,
            sequence,
            RuntimeFilterContribution::Membership(delta),
        )
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_close(partition_id, terminal_sequence)
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_failure(reason)
    }
}

impl OrderedBoundProducerAdapter for RemoteProducerAdapter {
    fn submit_bound(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        update: OrderedBoundUpdate,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_contribution(
            partition_id,
            sequence,
            RuntimeFilterContribution::OrderedBound(update),
        )
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_close(partition_id, terminal_sequence)
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_failure(reason)
    }
}

impl TopKSummaryProducerAdapter for RemoteProducerAdapter {
    fn submit_summary(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        summary: TopKSummary,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_contribution(
            partition_id,
            sequence,
            RuntimeFilterContribution::TopKSummary(summary),
        )
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_close(partition_id, terminal)
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_failure(reason)
    }
}

impl FinalDomainProducerAdapter for RemoteProducerAdapter {
    fn complete(
        &self,
        partition_id: PartitionId,
        sequence: ProducerSequence,
        shard: FinalDomainShard,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_contribution(
            partition_id,
            sequence,
            RuntimeFilterContribution::FinalDomain(shard),
        )
    }

    fn close_partition(
        &self,
        partition_id: PartitionId,
        terminal_sequence: ProducerSequence,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_close(partition_id, terminal_sequence)
    }

    fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.send_failure(reason)
    }
}
