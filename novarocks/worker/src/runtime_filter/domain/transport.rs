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

//! Worker-owned runtime-filter envelope and bounded retry primitives.
//!
//! These values retain the native envelope's route and byte semantics but do
//! not own RPC encoding.  The native adapter is responsible for translating a
//! validated [`BackendRuntimeFilterEnvelope`] to the unchanged wire DTO.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_execution::runtime_filter::RuntimeFilterBindingId;
use novarocks_types::UniqueId;

use super::{
    BackendChannelIdentity, BackendProducerStreamIdentity, BackendRouteEdgeId,
    BackendTransportSequence,
};
use crate::{
    ReliableTransportAckOutcome, ReliableTransportFailOpenReason, ReliableTransportPolicy,
    ReliableTransportResourceLimit, ReliableTransportSendOutcome, ReliableTransportState,
    ReliableTransportStateError,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BackendEnvelopeKind {
    Contribution,
    Artifact,
    ProducerClosed,
    ProducerUnavailable,
    Unavailable,
    CompletedWithoutArtifact,
    DegradedLogical,
    FinalArtifact,
    Ack,
}

impl BackendEnvelopeKind {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    const fn requires_producer_open(self) -> bool {
        matches!(self, Self::Contribution | Self::ProducerClosed)
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    const fn requires_accept_status(self) -> bool {
        matches!(self, Self::Ack)
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    const fn requires_payload(self) -> bool {
        matches!(
            self,
            Self::Contribution
                | Self::Artifact
                | Self::FinalArtifact
                | Self::ProducerUnavailable
                | Self::Unavailable
                | Self::DegradedLogical
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendAcceptStatus {
    Accepted,
    Duplicate,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackendContributionRouteIdentity {
    stream: BackendProducerStreamIdentity,
    sequence: BackendTransportSequence,
}

impl BackendContributionRouteIdentity {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn new(
        stream: BackendProducerStreamIdentity,
        sequence: BackendTransportSequence,
    ) -> Self {
        Self { stream, sequence }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn stream(self) -> BackendProducerStreamIdentity {
        self.stream
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn sequence(self) -> BackendTransportSequence {
        self.sequence
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackendDeliveryRouteIdentity {
    channel: BackendChannelIdentity,
    route_edge_id: BackendRouteEdgeId,
    sequence: BackendTransportSequence,
}

impl BackendDeliveryRouteIdentity {
    pub const fn new(
        channel: BackendChannelIdentity,
        route_edge_id: BackendRouteEdgeId,
        sequence: BackendTransportSequence,
    ) -> Self {
        Self {
            channel,
            route_edge_id,
            sequence,
        }
    }

    pub const fn channel(self) -> BackendChannelIdentity {
        self.channel
    }

    pub const fn route_edge_id(self) -> BackendRouteEdgeId {
        self.route_edge_id
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn sequence(self) -> BackendTransportSequence {
        self.sequence
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub struct BackendProducerInstanceRouteIdentity {
    channel: BackendChannelIdentity,
    producer_binding_id: RuntimeFilterBindingId,
    task_key: UniqueId,
}

impl BackendProducerInstanceRouteIdentity {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn new(
        channel: BackendChannelIdentity,
        producer_binding_id: RuntimeFilterBindingId,
        task_key: UniqueId,
    ) -> Self {
        Self {
            channel,
            producer_binding_id,
            task_key,
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn channel(self) -> BackendChannelIdentity {
        self.channel
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn producer_binding_id(self) -> RuntimeFilterBindingId {
        self.producer_binding_id
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn task_key(self) -> UniqueId {
        self.task_key
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub enum BackendRouteIdentity {
    Contribution(BackendContributionRouteIdentity),
    Delivery(BackendDeliveryRouteIdentity),
    ProducerInstance(BackendProducerInstanceRouteIdentity),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackendProducerOpenMetadata {
    local_partition_count: NonZeroU32,
}

impl BackendProducerOpenMetadata {
    pub fn try_new(local_partition_count: u32) -> Result<Self, BackendTransportError> {
        NonZeroU32::new(local_partition_count)
            .map(|local_partition_count| Self {
                local_partition_count,
            })
            .ok_or(BackendTransportError::ZeroLocalPartitionCount)
    }

    pub const fn local_partition_count(self) -> NonZeroU32 {
        self.local_partition_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendTransportError {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    ZeroRouteIdentity,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    IdentityKindMismatch(BackendEnvelopeKind),
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    PayloadRequired(BackendEnvelopeKind),
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    PayloadForbidden(BackendEnvelopeKind),
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    ProducerOpenRequired(BackendEnvelopeKind),
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    ProducerOpenForbidden(BackendEnvelopeKind),
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    AcceptStatusRequired(BackendEnvelopeKind),
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    AcceptStatusForbidden(BackendEnvelopeKind),
    ZeroLocalPartitionCount,
    EmptyRejectionReason,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    ZeroRpcDeadline,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    IdentityConflict,
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    RetiredIdentity,
}

impl fmt::Display for BackendTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter transport: {self:?}"
        )
    }
}

impl std::error::Error for BackendTransportError {}
impl novarocks_execution_contract::failure::FailureProjection for BackendTransportError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::*;
        let reason = match self {
            Self::ZeroRouteIdentity => R::FilterTransportZeroRouteIdentity,
            Self::IdentityKindMismatch(_) => R::FilterTransportIdentityKindMismatch,
            Self::PayloadRequired(_) => R::FilterTransportPayloadRequired,
            Self::PayloadForbidden(_) => R::FilterTransportPayloadForbidden,
            Self::ProducerOpenRequired(_) => R::FilterTransportProducerOpenRequired,
            Self::ProducerOpenForbidden(_) => R::FilterTransportProducerOpenForbidden,
            Self::AcceptStatusRequired(_) => R::FilterTransportAcceptStatusRequired,
            Self::AcceptStatusForbidden(_) => R::FilterTransportAcceptStatusForbidden,
            Self::ZeroLocalPartitionCount => R::FilterTransportZeroLocalPartitionCount,
            Self::EmptyRejectionReason => R::FilterTransportEmptyRejectionReason,
            Self::ZeroRpcDeadline => R::FilterTransportZeroRpcDeadline,
            Self::IdentityConflict => R::FilterTransportIdentityConflict,
            Self::RetiredIdentity => R::FilterTransportRetiredIdentity,
        };
        if matches!(self, Self::RetiredIdentity) {
            FailureFact::stop_observation(reason.reason())
        } else {
            FailureFact::new(FailureClass::Invariant, reason.reason())
        }
    }
}

/// Immutable Backend envelope. Payload bytes remain opaque here so the
/// transport cannot interpret canonical contribution or artifact content.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub struct BackendRuntimeFilterEnvelope {
    kind: BackendEnvelopeKind,
    channel: BackendChannelIdentity,
    route_identity: BackendRouteIdentity,
    producer_open: Option<BackendProducerOpenMetadata>,
    accept_status: Option<BackendAcceptStatus>,
    schema_digest: [u8; 32],
    payload: Arc<[u8]>,
}

impl BackendRuntimeFilterEnvelope {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn new(
        kind: BackendEnvelopeKind,
        channel: BackendChannelIdentity,
        route_identity: BackendRouteIdentity,
        producer_open: Option<BackendProducerOpenMetadata>,
        accept_status: Option<BackendAcceptStatus>,
        schema_digest: [u8; 32],
        payload: impl Into<Arc<[u8]>>,
    ) -> Result<Self, BackendTransportError> {
        validate_route(kind, route_identity)?;
        validate_presence(
            kind.requires_producer_open(),
            producer_open.is_some(),
            kind,
            true,
        )?;
        validate_presence(
            kind.requires_accept_status(),
            accept_status.is_some(),
            kind,
            false,
        )?;
        let payload = payload.into();
        if kind.requires_payload() && payload.is_empty() {
            return Err(BackendTransportError::PayloadRequired(kind));
        }
        if !kind.requires_payload() && !payload.is_empty() {
            return Err(BackendTransportError::PayloadForbidden(kind));
        }
        Ok(Self {
            kind,
            channel,
            route_identity,
            producer_open,
            accept_status,
            schema_digest,
            payload,
        })
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn kind(&self) -> BackendEnvelopeKind {
        self.kind
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn channel(&self) -> BackendChannelIdentity {
        self.channel
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn route_identity(&self) -> BackendRouteIdentity {
        self.route_identity
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn producer_open(&self) -> Option<BackendProducerOpenMetadata> {
        self.producer_open
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn accept_status(&self) -> Option<BackendAcceptStatus> {
        self.accept_status
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn payload(&self) -> &Arc<[u8]> {
        &self.payload
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.payload.len())
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn validate_route(
    kind: BackendEnvelopeKind,
    route: BackendRouteIdentity,
) -> Result<(), BackendTransportError> {
    let valid = match kind {
        BackendEnvelopeKind::Contribution | BackendEnvelopeKind::ProducerClosed => {
            matches!(route, BackendRouteIdentity::Contribution(route) if route.sequence().get() != 0)
        }
        BackendEnvelopeKind::ProducerUnavailable => {
            matches!(route, BackendRouteIdentity::ProducerInstance(_))
        }
        BackendEnvelopeKind::Artifact
        | BackendEnvelopeKind::FinalArtifact
        | BackendEnvelopeKind::Unavailable
        | BackendEnvelopeKind::CompletedWithoutArtifact
        | BackendEnvelopeKind::DegradedLogical => {
            matches!(route, BackendRouteIdentity::Delivery(route) if route.route_edge_id().get() != 0 && route.sequence().get() != 0)
        }
        BackendEnvelopeKind::Ack => true,
    };
    valid
        .then_some(())
        .ok_or(BackendTransportError::IdentityKindMismatch(kind))
}

#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
fn validate_presence(
    required: bool,
    present: bool,
    kind: BackendEnvelopeKind,
    producer_open: bool,
) -> Result<(), BackendTransportError> {
    match (required, present, producer_open) {
        (true, false, true) => Err(BackendTransportError::ProducerOpenRequired(kind)),
        (false, true, true) => Err(BackendTransportError::ProducerOpenForbidden(kind)),
        (true, false, false) => Err(BackendTransportError::AcceptStatusRequired(kind)),
        (false, true, false) => Err(BackendTransportError::AcceptStatusForbidden(kind)),
        _ => Ok(()),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendIngressResult {
    status: BackendAcceptStatus,
    rejection_reason: Option<Arc<str>>,
}

impl BackendIngressResult {
    pub const fn accepted() -> Self {
        Self {
            status: BackendAcceptStatus::Accepted,
            rejection_reason: None,
        }
    }

    pub const fn duplicate() -> Self {
        Self {
            status: BackendAcceptStatus::Duplicate,
            rejection_reason: None,
        }
    }

    pub fn rejected(reason: impl Into<Arc<str>>) -> Result<Self, BackendTransportError> {
        let reason = reason.into();
        if reason.is_empty() {
            return Err(BackendTransportError::EmptyRejectionReason);
        }
        Ok(Self {
            status: BackendAcceptStatus::Rejected,
            rejection_reason: Some(reason),
        })
    }

    pub const fn status(&self) -> BackendAcceptStatus {
        self.status
    }

    pub fn rejection_reason(&self) -> Option<&str> {
        self.rejection_reason.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub struct BackendTransportEnvelope {
    envelope: Arc<BackendRuntimeFilterEnvelope>,
    rpc_deadline: Duration,
}

impl BackendTransportEnvelope {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn new(
        envelope: Arc<BackendRuntimeFilterEnvelope>,
        rpc_deadline: Duration,
    ) -> Result<Self, BackendTransportError> {
        if rpc_deadline.is_zero() {
            return Err(BackendTransportError::ZeroRpcDeadline);
        }
        Ok(Self {
            envelope,
            rpc_deadline,
        })
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn envelope(&self) -> &Arc<BackendRuntimeFilterEnvelope> {
        &self.envelope
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn rpc_deadline(&self) -> Duration {
        self.rpc_deadline
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub struct BackendRetryPolicy {
    retry_interval: Duration,
    max_attempts: u32,
    deadline: Duration,
    max_pending_entries: usize,
    max_pending_bytes: usize,
}

impl BackendRetryPolicy {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn new(
        retry_interval: Duration,
        max_attempts: u32,
        deadline: Duration,
        max_pending_entries: usize,
        max_pending_bytes: usize,
    ) -> Result<Self, BackendTransportError> {
        if max_attempts == 0 || max_pending_entries == 0 || deadline.is_zero() {
            return Err(BackendTransportError::ZeroRouteIdentity);
        }
        Ok(Self {
            retry_interval,
            max_attempts,
            deadline,
            max_pending_entries,
            max_pending_bytes,
        })
    }
}

impl ReliableTransportPolicy for BackendRetryPolicy {
    fn retry_interval(self) -> Duration {
        self.retry_interval
    }

    fn max_attempts(self) -> u32 {
        self.max_attempts
    }

    fn deadline(self) -> Duration {
        self.deadline
    }

    fn max_pending_entries(self) -> usize {
        self.max_pending_entries
    }

    fn max_pending_bytes(self) -> usize {
        self.max_pending_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub enum BackendTransportResourceLimit {
    PendingEntries,
    PendingBytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub enum BackendRetrySendOutcome {
    Buffered,
    ResourceLimit(BackendTransportResourceLimit),
    Duplicate,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub enum BackendAckOutcome {
    Released,
    ReleasedOnDuplicate,
    Rejected,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendTransportFailOpenReason {
    Deadline,
    AttemptsExhausted,
    ContractRejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub struct BackendRetryTick {
    retried: Vec<Arc<BackendTransportEnvelope>>,
    failed_open: Vec<(BackendRouteIdentity, BackendTransportFailOpenReason)>,
}

impl BackendRetryTick {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn retried(&self) -> &[Arc<BackendTransportEnvelope>] {
        &self.retried
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn failed_open(&self) -> &[(BackendRouteIdentity, BackendTransportFailOpenReason)] {
        &self.failed_open
    }
}

/// Bounded sender-side retry state. It is deliberately driven by the Service's
/// query tick; this type starts no background task and does not perform I/O.
#[allow(
    dead_code,
    reason = "Retained for staged backend runtime-filter domain and materialization integration."
)]
pub struct BackendReliableTransport {
    state: ReliableTransportState<
        BackendRouteIdentity,
        Arc<BackendTransportEnvelope>,
        BackendRetryPolicy,
    >,
}

impl BackendReliableTransport {
    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn new(policy: BackendRetryPolicy) -> Self {
        Self {
            state: ReliableTransportState::new(policy),
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn send(
        &mut self,
        frame: Arc<BackendTransportEnvelope>,
        now: Instant,
    ) -> Result<BackendRetrySendOutcome, BackendTransportError> {
        let key = frame.envelope().route_identity();
        let bytes = frame.envelope().retained_bytes();
        match self.state.send(key, frame, bytes, now) {
            Ok(ReliableTransportSendOutcome::Buffered) => Ok(BackendRetrySendOutcome::Buffered),
            Ok(ReliableTransportSendOutcome::ResourceLimit(
                ReliableTransportResourceLimit::PendingEntries,
            )) => Ok(BackendRetrySendOutcome::ResourceLimit(
                BackendTransportResourceLimit::PendingEntries,
            )),
            Ok(ReliableTransportSendOutcome::ResourceLimit(
                ReliableTransportResourceLimit::PendingBytes,
            )) => Ok(BackendRetrySendOutcome::ResourceLimit(
                BackendTransportResourceLimit::PendingBytes,
            )),
            Ok(ReliableTransportSendOutcome::Duplicate) => Ok(BackendRetrySendOutcome::Duplicate),
            Ok(ReliableTransportSendOutcome::Shutdown) => Ok(BackendRetrySendOutcome::Shutdown),
            Err(ReliableTransportStateError::IdentityConflict) => {
                Err(BackendTransportError::IdentityConflict)
            }
            Err(ReliableTransportStateError::RetiredIdentity) => {
                Err(BackendTransportError::RetiredIdentity)
            }
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn acknowledge(
        &mut self,
        key: BackendRouteIdentity,
        status: BackendAcceptStatus,
    ) -> BackendAckOutcome {
        match self.state.acknowledge(key) {
            ReliableTransportAckOutcome::Unknown => BackendAckOutcome::Unknown,
            ReliableTransportAckOutcome::Released(_) => match status {
                BackendAcceptStatus::Accepted => BackendAckOutcome::Released,
                BackendAcceptStatus::Duplicate => BackendAckOutcome::ReleasedOnDuplicate,
                BackendAcceptStatus::Rejected => BackendAckOutcome::Rejected,
            },
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn drive(&mut self, now: Instant) -> BackendRetryTick {
        // The legacy domain owner has no per-attempt I/O callback. Its query
        // tick therefore treats every unacknowledged frame as retry-eligible;
        // native callers instead mark only observed transport failures.
        self.state.schedule_all_pending_retries();
        let tick = self.state.drive(now);
        let retried = tick
            .retried()
            .iter()
            .map(|(_, frame)| Arc::clone(frame))
            .collect();
        let failed_open = tick
            .failed_open()
            .iter()
            .map(|(key, reason)| {
                let reason = match reason {
                    ReliableTransportFailOpenReason::Deadline => {
                        BackendTransportFailOpenReason::Deadline
                    }
                    ReliableTransportFailOpenReason::AttemptsExhausted => {
                        BackendTransportFailOpenReason::AttemptsExhausted
                    }
                };
                (*key, reason)
            })
            .collect();
        BackendRetryTick {
            retried,
            failed_open,
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn shutdown(&mut self) {
        self.state.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use novarocks_execution::runtime_filter::{
        PartitionId, RuntimeFilterBindingId, RuntimeFilterChannelId,
    };

    use super::*;
    use crate::runtime_filter::domain::BackendParticipantIdentity;

    fn delivery_frame_with_payload(
        sequence: u64,
        payload: impl Into<Arc<[u8]>>,
    ) -> Arc<BackendTransportEnvelope> {
        let participant = BackendParticipantIdentity::new(UniqueId::new(1, 2), 3);
        let channel = BackendChannelIdentity::new(
            participant,
            RuntimeFilterBindingId::new(4),
            RuntimeFilterChannelId::new(5),
        );
        let route = BackendRouteIdentity::Delivery(BackendDeliveryRouteIdentity::new(
            channel,
            BackendRouteEdgeId::new(6),
            BackendTransportSequence::new(sequence),
        ));
        Arc::new(
            BackendTransportEnvelope::new(
                Arc::new(
                    BackendRuntimeFilterEnvelope::new(
                        BackendEnvelopeKind::Artifact,
                        channel,
                        route,
                        None,
                        None,
                        [7; 32],
                        payload,
                    )
                    .unwrap(),
                ),
                Duration::from_secs(1),
            )
            .unwrap(),
        )
    }

    fn delivery_frame(sequence: u64) -> Arc<BackendTransportEnvelope> {
        delivery_frame_with_payload(sequence, Arc::<[u8]>::from([8, 9]))
    }

    #[test]
    fn envelope_rejects_route_and_payload_shape_drift() {
        let frame = delivery_frame(1);
        let bad = BackendRuntimeFilterEnvelope::new(
            BackendEnvelopeKind::Contribution,
            frame.envelope().channel(),
            frame.envelope().route_identity(),
            None,
            None,
            [0; 32],
            Arc::<[u8]>::from([1]),
        );
        assert_eq!(
            bad.unwrap_err(),
            BackendTransportError::IdentityKindMismatch(BackendEnvelopeKind::Contribution)
        );
    }

    #[test]
    fn retry_ack_dedupe_and_deadline_preserve_route_identity() {
        let policy = BackendRetryPolicy::new(
            Duration::from_millis(5),
            2,
            Duration::from_millis(10),
            2,
            4096,
        )
        .unwrap();
        let start = Instant::now();
        let frame = delivery_frame(1);
        let key = frame.envelope().route_identity();
        let mut transport = BackendReliableTransport::new(policy);
        assert_eq!(
            transport.send(Arc::clone(&frame), start).unwrap(),
            BackendRetrySendOutcome::Buffered
        );
        assert_eq!(
            transport.send(Arc::clone(&frame), start).unwrap(),
            BackendRetrySendOutcome::Duplicate
        );
        assert_eq!(
            transport
                .drive(start + Duration::from_millis(5))
                .retried()
                .len(),
            1
        );
        assert_eq!(
            transport.acknowledge(key, BackendAcceptStatus::Duplicate),
            BackendAckOutcome::ReleasedOnDuplicate
        );
        assert_eq!(
            transport.acknowledge(key, BackendAcceptStatus::Accepted),
            BackendAckOutcome::Unknown
        );

        let deadline_frame = delivery_frame(2);
        let deadline_key = deadline_frame.envelope().route_identity();
        assert_eq!(
            transport.send(deadline_frame, start).unwrap(),
            BackendRetrySendOutcome::Buffered
        );
        assert_eq!(
            transport
                .drive(start + Duration::from_millis(10))
                .failed_open(),
            &[(deadline_key, BackendTransportFailOpenReason::Deadline)]
        );
    }

    #[test]
    fn contribution_identity_keeps_partition_and_sequence_coordinates() {
        let participant = BackendParticipantIdentity::new(UniqueId::new(1, 2), 3);
        let channel = BackendChannelIdentity::new(
            participant,
            RuntimeFilterBindingId::new(4),
            RuntimeFilterChannelId::new(5),
        );
        let identity = BackendContributionRouteIdentity::new(
            BackendProducerStreamIdentity::new(channel, UniqueId::new(6, 7), PartitionId::new(8)),
            BackendTransportSequence::new(9),
        );
        assert_eq!(identity.stream().partition_id(), PartitionId::new(8));
        assert_eq!(identity.sequence().get(), 9);
    }

    #[test]
    fn reliable_transport_capacity_entries_preserve_identity_and_release_on_ack() {
        let policy = BackendRetryPolicy::new(
            Duration::from_millis(1),
            1,
            Duration::from_millis(10),
            1,
            usize::MAX,
        )
        .unwrap();
        let start = Instant::now();
        let first = delivery_frame(1);
        let first_key = first.envelope().route_identity();
        let second = delivery_frame(2);
        let conflicting_first = delivery_frame_with_payload(1, Arc::<[u8]>::from([9, 8]));
        let mut transport = BackendReliableTransport::new(policy);

        assert_eq!(
            transport.send(Arc::clone(&first), start).unwrap(),
            BackendRetrySendOutcome::Buffered
        );
        assert_eq!(
            transport.send(Arc::clone(&first), start).unwrap(),
            BackendRetrySendOutcome::Duplicate
        );
        assert_eq!(
            transport.send(conflicting_first, start),
            Err(BackendTransportError::IdentityConflict)
        );
        assert_eq!(
            transport.send(Arc::clone(&second), start).unwrap(),
            BackendRetrySendOutcome::ResourceLimit(BackendTransportResourceLimit::PendingEntries)
        );
        assert_eq!(
            transport.acknowledge(first_key, BackendAcceptStatus::Accepted),
            BackendAckOutcome::Released
        );
        assert_eq!(
            transport.send(second, start).unwrap(),
            BackendRetrySendOutcome::Buffered
        );
    }

    #[test]
    fn reliable_transport_capacity_bytes_do_not_charge_duplicates_and_release_on_deadline() {
        let start = Instant::now();
        let first = delivery_frame(1);
        let first_bytes = first.envelope().retained_bytes();
        let first_key = first.envelope().route_identity();
        let second = delivery_frame(2);
        let policy = BackendRetryPolicy::new(
            Duration::from_millis(1),
            1,
            Duration::from_millis(10),
            2,
            first_bytes,
        )
        .unwrap();
        let mut transport = BackendReliableTransport::new(policy);

        assert_eq!(
            transport.send(Arc::clone(&first), start).unwrap(),
            BackendRetrySendOutcome::Buffered
        );
        assert_eq!(
            transport.send(first, start).unwrap(),
            BackendRetrySendOutcome::Duplicate
        );
        assert_eq!(
            transport.send(Arc::clone(&second), start).unwrap(),
            BackendRetrySendOutcome::ResourceLimit(BackendTransportResourceLimit::PendingBytes)
        );
        assert_eq!(
            transport
                .drive(start + Duration::from_millis(10))
                .failed_open(),
            &[(first_key, BackendTransportFailOpenReason::Deadline)]
        );
        assert_eq!(
            transport.send(second, start).unwrap(),
            BackendRetrySendOutcome::Buffered
        );
    }

    #[test]
    fn reliable_transport_capacity_overflow_returns_typed_byte_limit() {
        let policy =
            BackendRetryPolicy::new(Duration::from_millis(1), 1, Duration::from_millis(10), 2, 0)
                .unwrap();
        let mut transport = BackendReliableTransport::new(policy);

        assert_eq!(
            transport.send(delivery_frame(1), Instant::now()).unwrap(),
            BackendRetrySendOutcome::ResourceLimit(BackendTransportResourceLimit::PendingBytes)
        );
    }

    #[test]
    fn reliable_transport_capacity_shutdown_is_terminal() {
        let policy = BackendRetryPolicy::new(
            Duration::from_millis(1),
            1,
            Duration::from_millis(10),
            2,
            usize::MAX,
        )
        .unwrap();
        let start = Instant::now();
        let first = delivery_frame(1);
        let first_key = first.envelope().route_identity();
        let mut transport = BackendReliableTransport::new(policy);

        assert_eq!(
            transport.send(first, start).unwrap(),
            BackendRetrySendOutcome::Buffered
        );
        transport.shutdown();
        assert_eq!(
            transport.acknowledge(first_key, BackendAcceptStatus::Accepted),
            BackendAckOutcome::Unknown
        );
        assert_eq!(
            transport.send(delivery_frame(2), start).unwrap(),
            BackendRetrySendOutcome::Shutdown
        );
    }
}

#[cfg(test)]
mod failure_projection_tests {
    use super::*;
    use novarocks_execution_contract::failure::{FailureClass, FailureEvidence, FailureProjection};
    #[test]
    fn explicit_transport_retirement_is_an_observation_but_conflict_is_originating() {
        let retired = BackendTransportError::RetiredIdentity.failure_fact();
        assert_eq!(retired.class(), FailureClass::Control);
        assert_eq!(retired.evidence(), FailureEvidence::StopObservation);
        let conflict = BackendTransportError::IdentityConflict.failure_fact();
        assert_eq!(conflict.class(), FailureClass::Invariant);
        assert_eq!(conflict.evidence(), FailureEvidence::Originating);
        assert_ne!(retired.reason(), conflict.reason());
        assert_eq!(
            BackendProducerOpenMetadata::try_new(0)
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
    }
}
