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

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, PartitionId, ProducerSequence, RouteEdgeId,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum RuntimeFilterEnvelopeKind {
    Contribution,
    Artifact,
    ProducerClosed,
    ProducerUnavailable,
    Unavailable,
    CompletedWithoutArtifact,
    DegradedLogical,
    FinalArtifact,
    /// Acknowledges either a `Contribution`-kind or `Delivery`-kind route identity.
    /// The acked route identity is the envelope's own top-level `route_identity`
    /// (see `RuntimeFilterEnvelope::route_identity`); an `Ack` envelope does not
    /// carry a second, separate identity for what it is acknowledging. The accept
    /// status of that acknowledgement is carried by `RuntimeFilterEnvelope::accept_status`.
    Ack,
}

impl RuntimeFilterEnvelopeKind {
    const fn requires_producer_open(self) -> bool {
        matches!(self, Self::Contribution | Self::ProducerClosed)
    }

    const fn requires_accept_status(self) -> bool {
        matches!(self, Self::Ack)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTransportError {
    kind: RuntimeFilterTransportErrorKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RuntimeFilterTransportErrorKind {
    ZeroIdentity(&'static str),
    InvalidSchemaDigestLength(usize),
    IdentityKindMismatch(RuntimeFilterEnvelopeKind),
    PayloadRequired(RuntimeFilterEnvelopeKind),
    PayloadForbidden(RuntimeFilterEnvelopeKind),
    ZeroLocalPartitionCount,
    ProducerOpenRequired(RuntimeFilterEnvelopeKind),
    ProducerOpenForbidden(RuntimeFilterEnvelopeKind),
    AcceptStatusRequired(RuntimeFilterEnvelopeKind),
    AcceptStatusForbidden(RuntimeFilterEnvelopeKind),
    EmptyRejectionReason,
}

impl RuntimeFilterTransportError {
    fn zero_identity(identity: &'static str) -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::ZeroIdentity(identity),
        }
    }

    fn invalid_schema_digest_length(actual: usize) -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::InvalidSchemaDigestLength(actual),
        }
    }

    fn identity_kind_mismatch(kind: RuntimeFilterEnvelopeKind) -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::IdentityKindMismatch(kind),
        }
    }

    fn payload_required(kind: RuntimeFilterEnvelopeKind) -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::PayloadRequired(kind),
        }
    }

    fn payload_forbidden(kind: RuntimeFilterEnvelopeKind) -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::PayloadForbidden(kind),
        }
    }

    fn zero_local_partition_count() -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::ZeroLocalPartitionCount,
        }
    }

    fn producer_open_required(kind: RuntimeFilterEnvelopeKind) -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::ProducerOpenRequired(kind),
        }
    }

    fn producer_open_forbidden(kind: RuntimeFilterEnvelopeKind) -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::ProducerOpenForbidden(kind),
        }
    }

    fn accept_status_required(kind: RuntimeFilterEnvelopeKind) -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::AcceptStatusRequired(kind),
        }
    }

    fn accept_status_forbidden(kind: RuntimeFilterEnvelopeKind) -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::AcceptStatusForbidden(kind),
        }
    }

    fn empty_rejection_reason() -> Self {
        Self {
            kind: RuntimeFilterTransportErrorKind::EmptyRejectionReason,
        }
    }
}

impl fmt::Display for RuntimeFilterTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            RuntimeFilterTransportErrorKind::ZeroIdentity(identity) => {
                write!(formatter, "runtime filter {identity} must not be zero")
            }
            RuntimeFilterTransportErrorKind::InvalidSchemaDigestLength(actual) => write!(
                formatter,
                "runtime filter schema digest must contain exactly 32 bytes, got {actual}"
            ),
            RuntimeFilterTransportErrorKind::IdentityKindMismatch(kind) => write!(
                formatter,
                "runtime filter envelope kind {kind:?} has an incompatible route identity"
            ),
            RuntimeFilterTransportErrorKind::PayloadRequired(kind) => write!(
                formatter,
                "runtime filter envelope kind {kind:?} requires a non-empty payload"
            ),
            RuntimeFilterTransportErrorKind::PayloadForbidden(kind) => write!(
                formatter,
                "runtime filter envelope kind {kind:?} forbids a payload"
            ),
            RuntimeFilterTransportErrorKind::ZeroLocalPartitionCount => {
                formatter.write_str("runtime filter local partition count must not be zero")
            }
            RuntimeFilterTransportErrorKind::ProducerOpenRequired(kind) => write!(
                formatter,
                "runtime filter envelope kind {kind:?} requires producer-open metadata"
            ),
            RuntimeFilterTransportErrorKind::ProducerOpenForbidden(kind) => write!(
                formatter,
                "runtime filter envelope kind {kind:?} forbids producer-open metadata"
            ),
            RuntimeFilterTransportErrorKind::AcceptStatusRequired(kind) => write!(
                formatter,
                "runtime filter envelope kind {kind:?} requires an accept status"
            ),
            RuntimeFilterTransportErrorKind::AcceptStatusForbidden(kind) => write!(
                formatter,
                "runtime filter envelope kind {kind:?} forbids an accept status"
            ),
            RuntimeFilterTransportErrorKind::EmptyRejectionReason => {
                formatter.write_str("runtime filter rejection reason must not be empty")
            }
        }
    }
}

impl Error for RuntimeFilterTransportError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ContributionRouteIdentity {
    producer_binding_id: BindingId,
    fragment_instance_id: UniqueId,
    partition_id: PartitionId,
    sequence: ProducerSequence,
}

impl ContributionRouteIdentity {
    pub(crate) fn try_new(
        producer_binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: ProducerSequence,
    ) -> Result<Self, RuntimeFilterTransportError> {
        if producer_binding_id.get() == 0 {
            return Err(RuntimeFilterTransportError::zero_identity(
                "producer binding id",
            ));
        }
        if fragment_instance_id.high() == 0 && fragment_instance_id.low() == 0 {
            return Err(RuntimeFilterTransportError::zero_identity(
                "fragment instance id",
            ));
        }
        Ok(Self {
            producer_binding_id,
            fragment_instance_id,
            partition_id,
            sequence,
        })
    }

    pub(crate) const fn producer_binding_id(&self) -> BindingId {
        self.producer_binding_id
    }

    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) const fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    pub(crate) const fn sequence(&self) -> ProducerSequence {
        self.sequence
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeliveryRouteIdentity {
    route_edge_id: RouteEdgeId,
    sequence: ProducerSequence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProducerInstanceRouteIdentity {
    producer_binding_id: BindingId,
    fragment_instance_id: UniqueId,
}

impl ProducerInstanceRouteIdentity {
    pub(crate) fn try_new(
        producer_binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Result<Self, RuntimeFilterTransportError> {
        if producer_binding_id.get() == 0 {
            return Err(RuntimeFilterTransportError::zero_identity(
                "producer binding id",
            ));
        }
        if fragment_instance_id.high() == 0 && fragment_instance_id.low() == 0 {
            return Err(RuntimeFilterTransportError::zero_identity(
                "fragment instance id",
            ));
        }
        Ok(Self {
            producer_binding_id,
            fragment_instance_id,
        })
    }

    pub(crate) const fn producer_binding_id(&self) -> BindingId {
        self.producer_binding_id
    }

    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }
}

impl DeliveryRouteIdentity {
    pub(crate) fn try_new(
        route_edge_id: RouteEdgeId,
        sequence: ProducerSequence,
    ) -> Result<Self, RuntimeFilterTransportError> {
        if route_edge_id.get() == 0 {
            return Err(RuntimeFilterTransportError::zero_identity("route edge id"));
        }
        if sequence.get() == 0 {
            return Err(RuntimeFilterTransportError::zero_identity(
                "delivery sequence",
            ));
        }
        Ok(Self {
            route_edge_id,
            sequence,
        })
    }

    pub(crate) const fn route_edge_id(&self) -> RouteEdgeId {
        self.route_edge_id
    }

    pub(crate) const fn sequence(&self) -> ProducerSequence {
        self.sequence
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterRouteIdentity {
    kind: RuntimeFilterRouteIdentityKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RuntimeFilterRouteIdentityKind {
    Contribution(ContributionRouteIdentity),
    Delivery(DeliveryRouteIdentity),
    ProducerInstance(ProducerInstanceRouteIdentity),
}

impl RuntimeFilterRouteIdentity {
    pub(crate) const fn contribution(identity: ContributionRouteIdentity) -> Self {
        Self {
            kind: RuntimeFilterRouteIdentityKind::Contribution(identity),
        }
    }

    pub(crate) const fn delivery(identity: DeliveryRouteIdentity) -> Self {
        Self {
            kind: RuntimeFilterRouteIdentityKind::Delivery(identity),
        }
    }

    pub(crate) const fn producer_instance(identity: ProducerInstanceRouteIdentity) -> Self {
        Self {
            kind: RuntimeFilterRouteIdentityKind::ProducerInstance(identity),
        }
    }

    pub(crate) const fn as_contribution(&self) -> Option<&ContributionRouteIdentity> {
        match &self.kind {
            RuntimeFilterRouteIdentityKind::Contribution(identity) => Some(identity),
            RuntimeFilterRouteIdentityKind::Delivery(_)
            | RuntimeFilterRouteIdentityKind::ProducerInstance(_) => None,
        }
    }

    pub(crate) const fn as_delivery(&self) -> Option<&DeliveryRouteIdentity> {
        match &self.kind {
            RuntimeFilterRouteIdentityKind::Delivery(identity) => Some(identity),
            RuntimeFilterRouteIdentityKind::Contribution(_)
            | RuntimeFilterRouteIdentityKind::ProducerInstance(_) => None,
        }
    }

    pub(crate) const fn as_producer_instance(&self) -> Option<&ProducerInstanceRouteIdentity> {
        match &self.kind {
            RuntimeFilterRouteIdentityKind::ProducerInstance(identity) => Some(identity),
            RuntimeFilterRouteIdentityKind::Contribution(_)
            | RuntimeFilterRouteIdentityKind::Delivery(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerOpenMetadata {
    local_partition_count: NonZeroU32,
}

impl ProducerOpenMetadata {
    pub(crate) fn try_new(local_partition_count: u32) -> Result<Self, RuntimeFilterTransportError> {
        let local_partition_count = NonZeroU32::new(local_partition_count)
            .ok_or_else(RuntimeFilterTransportError::zero_local_partition_count)?;
        Ok(Self {
            local_partition_count,
        })
    }

    pub(crate) fn try_from_raw_for_kind(
        kind: RuntimeFilterEnvelopeKind,
        local_partition_count: Option<u32>,
    ) -> Result<Option<Self>, RuntimeFilterTransportError> {
        validate_producer_open_presence(kind, local_partition_count.is_some())?;
        local_partition_count.map(Self::try_new).transpose()
    }

    pub(crate) const fn local_partition_count(self) -> NonZeroU32 {
        self.local_partition_count
    }
}

fn validate_producer_open_presence(
    kind: RuntimeFilterEnvelopeKind,
    is_present: bool,
) -> Result<(), RuntimeFilterTransportError> {
    if kind.requires_producer_open() && !is_present {
        return Err(RuntimeFilterTransportError::producer_open_required(kind));
    }
    if !kind.requires_producer_open() && is_present {
        return Err(RuntimeFilterTransportError::producer_open_forbidden(kind));
    }
    Ok(())
}

fn validate_accept_status_presence(
    kind: RuntimeFilterEnvelopeKind,
    is_present: bool,
) -> Result<(), RuntimeFilterTransportError> {
    if kind.requires_accept_status() && !is_present {
        return Err(RuntimeFilterTransportError::accept_status_required(kind));
    }
    if !kind.requires_accept_status() && is_present {
        return Err(RuntimeFilterTransportError::accept_status_forbidden(kind));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterEnvelope {
    kind: RuntimeFilterEnvelopeKind,
    query_id: UniqueId,
    channel_id: ChannelId,
    deployment_epoch: DeploymentEpoch,
    route_identity: RuntimeFilterRouteIdentity,
    producer_open: Option<ProducerOpenMetadata>,
    /// Present only for `Ack` envelopes: the accept status of the route
    /// acknowledged by this envelope's top-level `route_identity`.
    accept_status: Option<RuntimeFilterAcceptStatus>,
    schema_digest: [u8; 32],
    payload: Vec<u8>,
}

impl RuntimeFilterEnvelope {
    pub(crate) fn try_new(
        kind: RuntimeFilterEnvelopeKind,
        query_id: UniqueId,
        channel_id: ChannelId,
        deployment_epoch: DeploymentEpoch,
        route_identity: RuntimeFilterRouteIdentity,
        producer_open: Option<ProducerOpenMetadata>,
        accept_status: Option<RuntimeFilterAcceptStatus>,
        schema_digest: &[u8],
        payload: Vec<u8>,
    ) -> Result<Self, RuntimeFilterTransportError> {
        if query_id.high() == 0 && query_id.low() == 0 {
            return Err(RuntimeFilterTransportError::zero_identity("query id"));
        }
        if channel_id.get() == 0 {
            return Err(RuntimeFilterTransportError::zero_identity("channel id"));
        }
        if deployment_epoch.get() == 0 {
            return Err(RuntimeFilterTransportError::zero_identity(
                "deployment epoch",
            ));
        }
        if schema_digest.len() != 32 {
            return Err(RuntimeFilterTransportError::invalid_schema_digest_length(
                schema_digest.len(),
            ));
        }
        validate_producer_open_presence(kind, producer_open.is_some())?;
        validate_accept_status_presence(kind, accept_status.is_some())?;
        let identity_matches = match kind {
            RuntimeFilterEnvelopeKind::Contribution | RuntimeFilterEnvelopeKind::ProducerClosed => {
                route_identity.as_contribution().is_some()
            }
            RuntimeFilterEnvelopeKind::ProducerUnavailable => {
                route_identity.as_producer_instance().is_some()
            }
            RuntimeFilterEnvelopeKind::Artifact
            | RuntimeFilterEnvelopeKind::FinalArtifact
            | RuntimeFilterEnvelopeKind::Unavailable
            | RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
            | RuntimeFilterEnvelopeKind::DegradedLogical => route_identity.as_delivery().is_some(),
            RuntimeFilterEnvelopeKind::Ack => true,
        };
        if !identity_matches {
            return Err(RuntimeFilterTransportError::identity_kind_mismatch(kind));
        }
        let payload_required = matches!(
            kind,
            RuntimeFilterEnvelopeKind::Contribution
                | RuntimeFilterEnvelopeKind::Artifact
                | RuntimeFilterEnvelopeKind::FinalArtifact
                | RuntimeFilterEnvelopeKind::ProducerUnavailable
                | RuntimeFilterEnvelopeKind::Unavailable
                | RuntimeFilterEnvelopeKind::DegradedLogical
        );
        if payload_required && payload.is_empty() {
            return Err(RuntimeFilterTransportError::payload_required(kind));
        }
        if !payload_required && !payload.is_empty() {
            return Err(RuntimeFilterTransportError::payload_forbidden(kind));
        }

        let mut fixed_schema_digest = [0; 32];
        fixed_schema_digest.copy_from_slice(schema_digest);
        Ok(Self {
            kind,
            query_id,
            channel_id,
            deployment_epoch,
            route_identity,
            producer_open,
            accept_status,
            schema_digest: fixed_schema_digest,
            payload,
        })
    }

    pub(crate) const fn kind(&self) -> RuntimeFilterEnvelopeKind {
        self.kind
    }

    pub(crate) const fn query_id(&self) -> UniqueId {
        self.query_id
    }

    pub(crate) const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    pub(crate) const fn deployment_epoch(&self) -> DeploymentEpoch {
        self.deployment_epoch
    }

    pub(crate) const fn route_identity(&self) -> &RuntimeFilterRouteIdentity {
        &self.route_identity
    }

    pub(crate) const fn producer_open(&self) -> Option<ProducerOpenMetadata> {
        self.producer_open
    }

    /// The accept status acknowledged by an `Ack` envelope. `None` for every
    /// other kind. The identity being acknowledged is `route_identity`, not a
    /// separate field.
    pub(crate) const fn accept_status(&self) -> Option<RuntimeFilterAcceptStatus> {
        self.accept_status
    }

    pub(crate) const fn schema_digest(&self) -> &[u8; 32] {
        &self.schema_digest
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Deterministic retained-memory charge owned directly by this envelope. The
    /// variable heap component uses `Vec::capacity`, not its logical wire length.
    /// Arc/control-block and transport-entry fixed overhead are bounded separately by
    /// the reliable transport's pending-entry ceiling.
    pub(crate) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.payload.capacity())
    }
}

/// A complete domain envelope plus the unary deadline installed for this query.
///
/// The reliable transport owns the envelope identity and retry lifetime. The sink
/// receives this immutable value and is responsible only for wire transmission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTransportEnvelope {
    envelope: Arc<RuntimeFilterEnvelope>,
    rpc_deadline: Duration,
}

impl RuntimeFilterTransportEnvelope {
    pub(crate) fn new(envelope: Arc<RuntimeFilterEnvelope>, rpc_deadline: Duration) -> Self {
        assert!(
            !rpc_deadline.is_zero(),
            "runtime filter envelope RPC deadline must be nonzero"
        );
        Self {
            envelope,
            rpc_deadline,
        }
    }

    pub(crate) fn envelope(&self) -> &RuntimeFilterEnvelope {
        self.envelope.as_ref()
    }

    pub(crate) const fn envelope_arc(&self) -> &Arc<RuntimeFilterEnvelope> {
        &self.envelope
    }

    pub(crate) fn into_parts(self) -> (Arc<RuntimeFilterEnvelope>, Duration) {
        (self.envelope, self.rpc_deadline)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterAcceptStatus {
    Accepted,
    Duplicate,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterIngressResult {
    accept_status: RuntimeFilterAcceptStatus,
    rejection_reason: Option<String>,
}

impl RuntimeFilterIngressResult {
    pub(crate) const fn accepted() -> Self {
        Self {
            accept_status: RuntimeFilterAcceptStatus::Accepted,
            rejection_reason: None,
        }
    }

    pub(crate) const fn duplicate() -> Self {
        Self {
            accept_status: RuntimeFilterAcceptStatus::Duplicate,
            rejection_reason: None,
        }
    }

    pub(crate) fn rejected(reason: impl Into<String>) -> Result<Self, RuntimeFilterTransportError> {
        let reason = reason.into();
        if reason.is_empty() {
            return Err(RuntimeFilterTransportError::empty_rejection_reason());
        }
        Ok(Self {
            accept_status: RuntimeFilterAcceptStatus::Rejected,
            rejection_reason: Some(reason),
        })
    }

    pub(crate) const fn accept_status(&self) -> RuntimeFilterAcceptStatus {
        self.accept_status
    }

    pub(crate) fn rejection_reason(&self) -> Option<&str> {
        self.rejection_reason.as_deref()
    }
}

pub(crate) trait RuntimeFilterEnvelopeIngress: Send + Sync + 'static {
    fn accept(&self, envelope: RuntimeFilterEnvelope) -> RuntimeFilterIngressResult;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, PartitionId, ProducerSequence, RouteEdgeId,
    };

    use super::*;

    fn contribution_route() -> RuntimeFilterRouteIdentity {
        RuntimeFilterRouteIdentity::contribution(
            ContributionRouteIdentity::try_new(
                BindingId::new(4),
                UniqueId::new(5, 6),
                PartitionId::new(7),
                ProducerSequence::new(8),
            )
            .unwrap(),
        )
    }

    fn delivery_route() -> RuntimeFilterRouteIdentity {
        RuntimeFilterRouteIdentity::delivery(
            DeliveryRouteIdentity::try_new(RouteEdgeId::new(9), ProducerSequence::new(10)).unwrap(),
        )
    }

    fn envelope(
        kind: RuntimeFilterEnvelopeKind,
        route_identity: RuntimeFilterRouteIdentity,
        payload: &[u8],
    ) -> RuntimeFilterEnvelope {
        let producer_open = matches!(
            kind,
            RuntimeFilterEnvelopeKind::Contribution | RuntimeFilterEnvelopeKind::ProducerClosed
        )
        .then(|| ProducerOpenMetadata::try_new(24).unwrap());
        let accept_status = matches!(kind, RuntimeFilterEnvelopeKind::Ack)
            .then_some(RuntimeFilterAcceptStatus::Accepted);
        RuntimeFilterEnvelope::try_new(
            kind,
            UniqueId::new(1, 2),
            ChannelId::new(3),
            DeploymentEpoch::new(4),
            route_identity,
            producer_open,
            accept_status,
            &[11; 32],
            payload.to_vec(),
        )
        .unwrap()
    }

    #[test]
    fn valid_envelopes_preserve_all_domain_coordinates() {
        let cases = [
            (
                RuntimeFilterEnvelopeKind::Contribution,
                contribution_route(),
                &b"contribution"[..],
            ),
            (
                RuntimeFilterEnvelopeKind::Artifact,
                delivery_route(),
                &b"artifact"[..],
            ),
            (
                RuntimeFilterEnvelopeKind::FinalArtifact,
                delivery_route(),
                &b"final-artifact"[..],
            ),
            (
                RuntimeFilterEnvelopeKind::ProducerClosed,
                contribution_route(),
                &b""[..],
            ),
            (
                RuntimeFilterEnvelopeKind::Unavailable,
                delivery_route(),
                &b"reason"[..],
            ),
            (
                RuntimeFilterEnvelopeKind::Ack,
                contribution_route(),
                &b""[..],
            ),
        ];

        for (kind, route_identity, payload) in cases {
            let envelope = envelope(kind, route_identity.clone(), payload);
            assert_eq!(envelope.kind(), kind);
            assert_eq!(envelope.query_id(), UniqueId::new(1, 2));
            assert_eq!(envelope.channel_id(), ChannelId::new(3));
            assert_eq!(envelope.deployment_epoch(), DeploymentEpoch::new(4));
            assert_eq!(envelope.route_identity(), &route_identity);
            assert_eq!(envelope.schema_digest(), &[11; 32]);
            assert_eq!(envelope.payload(), payload);
        }

        let contribution = contribution_route();
        let contribution = contribution.as_contribution().unwrap();
        assert_eq!(contribution.producer_binding_id(), BindingId::new(4));
        assert_eq!(contribution.fragment_instance_id(), UniqueId::new(5, 6));
        assert_eq!(contribution.partition_id(), PartitionId::new(7));
        assert_eq!(contribution.sequence(), ProducerSequence::new(8));

        let delivery = delivery_route();
        let delivery = delivery.as_delivery().unwrap();
        assert_eq!(delivery.route_edge_id(), RouteEdgeId::new(9));
        assert_eq!(delivery.sequence(), ProducerSequence::new(10));
    }

    #[test]
    fn unique_ids_reject_only_the_all_zero_value() {
        for query_id in [UniqueId::new(0, 29), UniqueId::new(31, 0)] {
            let envelope = RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::Contribution,
                query_id,
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                contribution_route(),
                Some(ProducerOpenMetadata::try_new(24).unwrap()),
                None,
                &[11; 32],
                b"payload".to_vec(),
            )
            .unwrap();
            assert_eq!(envelope.query_id(), query_id);
        }

        for fragment_instance_id in [UniqueId::new(0, 37), UniqueId::new(41, 0)] {
            let identity = ContributionRouteIdentity::try_new(
                BindingId::new(4),
                fragment_instance_id,
                PartitionId::new(7),
                ProducerSequence::new(8),
            )
            .unwrap();
            assert_eq!(identity.fragment_instance_id(), fragment_instance_id);
        }
    }

    #[test]
    fn contribution_identity_preserves_zero_based_partition_and_sequence() {
        let route_identity = RuntimeFilterRouteIdentity::contribution(
            ContributionRouteIdentity::try_new(
                BindingId::new(4),
                UniqueId::new(5, 6),
                PartitionId::new(0),
                ProducerSequence::new(0),
            )
            .unwrap(),
        );
        let contribution = envelope(
            RuntimeFilterEnvelopeKind::Contribution,
            route_identity.clone(),
            b"payload",
        );
        let producer_closed = envelope(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            route_identity.clone(),
            b"",
        );

        for accepted in [contribution, producer_closed] {
            let identity = accepted
                .route_identity()
                .as_contribution()
                .expect("contribution route identity");
            assert_eq!(identity.partition_id(), PartitionId::new(0));
            assert_eq!(identity.sequence(), ProducerSequence::new(0));
        }
    }

    #[test]
    fn route_identities_reject_only_invalid_non_ordinal_coordinates() {
        for result in [
            ContributionRouteIdentity::try_new(
                BindingId::new(0),
                UniqueId::new(5, 6),
                PartitionId::new(7),
                ProducerSequence::new(8),
            ),
            ContributionRouteIdentity::try_new(
                BindingId::new(4),
                UniqueId::new(0, 0),
                PartitionId::new(7),
                ProducerSequence::new(8),
            ),
        ] {
            assert!(result.is_err());
        }

        assert!(
            DeliveryRouteIdentity::try_new(RouteEdgeId::new(0), ProducerSequence::new(10)).is_err()
        );
        assert!(
            DeliveryRouteIdentity::try_new(RouteEdgeId::new(9), ProducerSequence::new(0)).is_err()
        );
    }

    #[test]
    fn envelope_rejects_zero_coordinates_and_invalid_digest_lengths() {
        let common = |query_id, channel_id, deployment_epoch, schema_digest: &[u8]| {
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::Contribution,
                query_id,
                channel_id,
                deployment_epoch,
                contribution_route(),
                Some(ProducerOpenMetadata::try_new(24).unwrap()),
                None,
                schema_digest,
                b"payload".to_vec(),
            )
        };

        assert!(
            common(
                UniqueId::new(0, 0),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                &[11; 32],
            )
            .is_err()
        );
        assert!(
            common(
                UniqueId::new(1, 2),
                ChannelId::new(0),
                DeploymentEpoch::new(4),
                &[11; 32],
            )
            .is_err()
        );
        assert!(
            common(
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(0),
                &[11; 32],
            )
            .is_err()
        );
        assert!(
            common(
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                &[11; 31],
            )
            .is_err()
        );
        assert!(
            common(
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                &[11; 33],
            )
            .is_err()
        );
    }

    #[test]
    fn envelope_rejects_kind_identity_mismatches() {
        for (kind, route_identity, producer_open, payload) in [
            (
                RuntimeFilterEnvelopeKind::Contribution,
                delivery_route(),
                Some(ProducerOpenMetadata::try_new(24).unwrap()),
                &b"payload"[..],
            ),
            (
                RuntimeFilterEnvelopeKind::Artifact,
                contribution_route(),
                None,
                &b"payload"[..],
            ),
            (
                RuntimeFilterEnvelopeKind::FinalArtifact,
                contribution_route(),
                None,
                &b"payload"[..],
            ),
            (
                RuntimeFilterEnvelopeKind::ProducerClosed,
                delivery_route(),
                Some(ProducerOpenMetadata::try_new(24).unwrap()),
                &b""[..],
            ),
            (
                RuntimeFilterEnvelopeKind::Unavailable,
                contribution_route(),
                None,
                &b"reason"[..],
            ),
        ] {
            assert!(
                RuntimeFilterEnvelope::try_new(
                    kind,
                    UniqueId::new(1, 2),
                    ChannelId::new(3),
                    DeploymentEpoch::new(4),
                    route_identity,
                    producer_open,
                    None,
                    &[11; 32],
                    payload.to_vec(),
                )
                .is_err()
            );
        }

        assert!(
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::Ack,
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                delivery_route(),
                None,
                Some(RuntimeFilterAcceptStatus::Accepted),
                &[11; 32],
                Vec::new(),
            )
            .is_ok()
        );
    }

    #[test]
    fn envelope_rejects_required_and_forbidden_payload_mismatches() {
        for (kind, route_identity, producer_open, payload) in [
            (
                RuntimeFilterEnvelopeKind::Contribution,
                contribution_route(),
                Some(ProducerOpenMetadata::try_new(24).unwrap()),
                &b""[..],
            ),
            (
                RuntimeFilterEnvelopeKind::Artifact,
                delivery_route(),
                None,
                &b""[..],
            ),
            (
                RuntimeFilterEnvelopeKind::FinalArtifact,
                delivery_route(),
                None,
                &b""[..],
            ),
            (
                RuntimeFilterEnvelopeKind::ProducerClosed,
                contribution_route(),
                Some(ProducerOpenMetadata::try_new(24).unwrap()),
                &b"payload"[..],
            ),
            (
                RuntimeFilterEnvelopeKind::Unavailable,
                delivery_route(),
                None,
                &b""[..],
            ),
            (
                RuntimeFilterEnvelopeKind::Ack,
                delivery_route(),
                None,
                &b"payload"[..],
            ),
        ] {
            // Ack additionally needs a valid accept status here so the assertion below
            // exercises only the payload-presence axis under test, not the separate
            // accept-status presence requirement.
            let accept_status = matches!(kind, RuntimeFilterEnvelopeKind::Ack)
                .then_some(RuntimeFilterAcceptStatus::Accepted);
            assert!(
                RuntimeFilterEnvelope::try_new(
                    kind,
                    UniqueId::new(1, 2),
                    ChannelId::new(3),
                    DeploymentEpoch::new(4),
                    route_identity,
                    producer_open,
                    accept_status,
                    &[11; 32],
                    payload.to_vec(),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn contribution_and_closed_require_nonzero_producer_open_metadata() {
        assert!(ProducerOpenMetadata::try_new(0).is_err());
        let producer_open = ProducerOpenMetadata::try_new(24);
        assert!(
            producer_open.is_ok(),
            "positive local partition count must construct metadata"
        );
        let producer_open = producer_open.unwrap();
        assert_eq!(
            producer_open.local_partition_count(),
            NonZeroU32::new(24).unwrap()
        );

        for (kind, payload) in [
            (RuntimeFilterEnvelopeKind::Contribution, b"payload".to_vec()),
            (RuntimeFilterEnvelopeKind::ProducerClosed, Vec::new()),
        ] {
            let build = |producer_open| {
                RuntimeFilterEnvelope::try_new(
                    kind,
                    UniqueId::new(1, 2),
                    ChannelId::new(3),
                    DeploymentEpoch::new(4),
                    contribution_route(),
                    producer_open,
                    None,
                    &[11; 32],
                    payload.clone(),
                )
            };

            assert!(build(None).is_err(), "{kind:?} must require producer-open");
            let envelope = build(Some(producer_open)).expect("valid producer-open metadata");
            assert_eq!(envelope.producer_open(), Some(producer_open));
        }
    }

    #[test]
    fn delivery_and_ack_kinds_forbid_producer_open_metadata() {
        let producer_open = ProducerOpenMetadata::try_new(24).unwrap();
        for (kind, route_identity, payload) in [
            (
                RuntimeFilterEnvelopeKind::Artifact,
                delivery_route(),
                b"artifact".to_vec(),
            ),
            (
                RuntimeFilterEnvelopeKind::FinalArtifact,
                delivery_route(),
                b"final-artifact".to_vec(),
            ),
            (
                RuntimeFilterEnvelopeKind::Unavailable,
                delivery_route(),
                b"reason".to_vec(),
            ),
            (
                RuntimeFilterEnvelopeKind::Ack,
                contribution_route(),
                Vec::new(),
            ),
        ] {
            // Ack additionally needs a valid accept status here so the assertion below
            // exercises only the producer-open axis under test.
            let accept_status = matches!(kind, RuntimeFilterEnvelopeKind::Ack)
                .then_some(RuntimeFilterAcceptStatus::Accepted);
            assert!(
                RuntimeFilterEnvelope::try_new(
                    kind,
                    UniqueId::new(1, 2),
                    ChannelId::new(3),
                    DeploymentEpoch::new(4),
                    route_identity,
                    Some(producer_open),
                    accept_status,
                    &[11; 32],
                    payload,
                )
                .is_err(),
                "{kind:?} must forbid producer-open metadata"
            );
        }
    }

    #[test]
    fn producer_closed_keeps_payload_empty_while_carrying_open_metadata() {
        let producer_open = ProducerOpenMetadata::try_new(24).unwrap();
        let build = |payload| {
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::ProducerClosed,
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                contribution_route(),
                Some(producer_open),
                None,
                &[11; 32],
                payload,
            )
        };

        let envelope = build(Vec::new()).expect("empty producer-close payload");
        assert_eq!(envelope.producer_open(), Some(producer_open));
        assert!(envelope.payload().is_empty());
        assert!(build(b"unexpected".to_vec()).is_err());
    }

    #[test]
    fn delivery_terminal_kinds_enforce_identity_and_payload_contracts() {
        let completed = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
            UniqueId::new(1, 2),
            ChannelId::new(3),
            DeploymentEpoch::new(4),
            delivery_route(),
            None,
            None,
            &[11; 32],
            Vec::new(),
        )
        .expect("completed-without-artifact terminal");
        assert!(completed.payload().is_empty());

        assert!(
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                delivery_route(),
                None,
                None,
                &[11; 32],
                b"unexpected".to_vec(),
            )
            .is_err()
        );
        assert!(
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::DegradedLogical,
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                delivery_route(),
                None,
                None,
                &[11; 32],
                Vec::new(),
            )
            .is_err()
        );
        assert!(
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::DegradedLogical,
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                contribution_route(),
                None,
                None,
                &[11; 32],
                b"reason".to_vec(),
            )
            .is_err()
        );
    }

    #[test]
    fn ack_payload_round_trips_each_accept_status_with_acked_route_identity() {
        for accept_status in [
            RuntimeFilterAcceptStatus::Accepted,
            RuntimeFilterAcceptStatus::Duplicate,
            RuntimeFilterAcceptStatus::Rejected,
        ] {
            for route_identity in [contribution_route(), delivery_route()] {
                let ack = RuntimeFilterEnvelope::try_new(
                    RuntimeFilterEnvelopeKind::Ack,
                    UniqueId::new(1, 2),
                    ChannelId::new(3),
                    DeploymentEpoch::new(4),
                    route_identity.clone(),
                    None,
                    Some(accept_status),
                    &[11; 32],
                    Vec::new(),
                )
                .expect("ack envelope with a present accept status is valid");

                assert_eq!(ack.accept_status(), Some(accept_status));
                assert_eq!(ack.route_identity(), &route_identity);
            }
        }
    }

    #[test]
    fn ack_payload_is_required_for_ack_kind() {
        for route_identity in [contribution_route(), delivery_route()] {
            let error = RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::Ack,
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                route_identity,
                None,
                None,
                &[11; 32],
                Vec::new(),
            )
            .expect_err("ack envelope without an accept status must be rejected");
            assert_eq!(
                error.to_string(),
                "runtime filter envelope kind Ack requires an accept status"
            );
        }
    }

    #[test]
    fn ack_payload_is_forbidden_for_non_ack_kinds() {
        for (kind, route_identity, producer_open, payload) in [
            (
                RuntimeFilterEnvelopeKind::Contribution,
                contribution_route(),
                Some(ProducerOpenMetadata::try_new(24).unwrap()),
                b"payload".to_vec(),
            ),
            (
                RuntimeFilterEnvelopeKind::Artifact,
                delivery_route(),
                None,
                b"artifact".to_vec(),
            ),
            (
                RuntimeFilterEnvelopeKind::ProducerClosed,
                contribution_route(),
                Some(ProducerOpenMetadata::try_new(24).unwrap()),
                Vec::new(),
            ),
            (
                RuntimeFilterEnvelopeKind::Unavailable,
                delivery_route(),
                None,
                b"reason".to_vec(),
            ),
        ] {
            let error = RuntimeFilterEnvelope::try_new(
                kind,
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                route_identity,
                producer_open,
                Some(RuntimeFilterAcceptStatus::Accepted),
                &[11; 32],
                payload,
            )
            .expect_err("non-ack envelope carrying an accept status must be rejected");
            assert_eq!(
                error.to_string(),
                format!("runtime filter envelope kind {kind:?} forbids an accept status")
            );
        }
    }

    #[test]
    fn ingress_result_preserves_exact_accept_taxonomy() {
        let accepted = RuntimeFilterIngressResult::accepted();
        assert_eq!(
            accepted.accept_status(),
            RuntimeFilterAcceptStatus::Accepted
        );
        assert_eq!(accepted.rejection_reason(), None);

        let duplicate = RuntimeFilterIngressResult::duplicate();
        assert_eq!(
            duplicate.accept_status(),
            RuntimeFilterAcceptStatus::Duplicate
        );
        assert_eq!(duplicate.rejection_reason(), None);

        let rejected = RuntimeFilterIngressResult::rejected("stale epoch").unwrap();
        assert_eq!(
            rejected.accept_status(),
            RuntimeFilterAcceptStatus::Rejected
        );
        assert_eq!(rejected.rejection_reason(), Some("stale epoch"));
        assert!(RuntimeFilterIngressResult::rejected("").is_err());
    }

    #[derive(Default)]
    struct RecordingIngress {
        kinds: Mutex<Vec<RuntimeFilterEnvelopeKind>>,
    }

    impl RuntimeFilterEnvelopeIngress for RecordingIngress {
        fn accept(&self, envelope: RuntimeFilterEnvelope) -> RuntimeFilterIngressResult {
            self.kinds.lock().unwrap().push(envelope.kind());
            RuntimeFilterIngressResult::accepted()
        }
    }

    #[test]
    fn trait_object_dispatch_preserves_all_envelope_kinds_in_order() {
        let recording = Arc::new(RecordingIngress::default());
        let ingress: Arc<dyn RuntimeFilterEnvelopeIngress> = recording.clone();
        for envelope in [
            envelope(
                RuntimeFilterEnvelopeKind::Contribution,
                contribution_route(),
                b"contribution",
            ),
            envelope(
                RuntimeFilterEnvelopeKind::Artifact,
                delivery_route(),
                b"artifact",
            ),
            envelope(
                RuntimeFilterEnvelopeKind::FinalArtifact,
                delivery_route(),
                b"final-artifact",
            ),
            envelope(
                RuntimeFilterEnvelopeKind::ProducerClosed,
                contribution_route(),
                b"",
            ),
            envelope(
                RuntimeFilterEnvelopeKind::Unavailable,
                delivery_route(),
                b"reason",
            ),
            envelope(
                RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                delivery_route(),
                b"",
            ),
            envelope(
                RuntimeFilterEnvelopeKind::DegradedLogical,
                delivery_route(),
                b"reason",
            ),
            envelope(RuntimeFilterEnvelopeKind::Ack, delivery_route(), b""),
        ] {
            assert_eq!(
                ingress.accept(envelope).accept_status(),
                RuntimeFilterAcceptStatus::Accepted
            );
        }

        assert_eq!(
            *recording.kinds.lock().unwrap(),
            [
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::FinalArtifact,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::Unavailable,
                RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                RuntimeFilterEnvelopeKind::DegradedLogical,
                RuntimeFilterEnvelopeKind::Ack,
            ]
        );
    }

    #[test]
    fn producer_unavailable_uses_producer_instance_identity() {
        let identity =
            ProducerInstanceRouteIdentity::try_new(BindingId::new(7), UniqueId::new(8, 9)).unwrap();
        let envelope = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::ProducerUnavailable,
            UniqueId::new(1, 2),
            ChannelId::new(3),
            DeploymentEpoch::new(4),
            RuntimeFilterRouteIdentity::producer_instance(identity.clone()),
            None,
            None,
            &[5; 32],
            vec![1, 2],
        )
        .unwrap();

        assert_eq!(
            envelope.route_identity().as_producer_instance(),
            Some(&identity)
        );
        assert!(envelope.route_identity().as_delivery().is_none());
        assert!(
            RuntimeFilterEnvelope::try_new(
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
                UniqueId::new(1, 2),
                ChannelId::new(3),
                DeploymentEpoch::new(4),
                delivery_route(),
                None,
                None,
                &[5; 32],
                vec![1, 2],
            )
            .is_err()
        );
    }
}
