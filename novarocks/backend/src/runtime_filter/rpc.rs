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

use std::sync::Arc;

use novarocks_execution::runtime_filter::{
    PartitionId, RuntimeFilterBindingId, RuntimeFilterChannelId,
};
use novarocks_proto as proto;
#[cfg(debug_assertions)]
use novarocks_proto::lifecycle::{AttemptId, QueryExecutionId};
#[cfg(debug_assertions)]
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

use crate::runtime_filter::domain::{
    BackendAcceptStatus, BackendEnvelopeKind, BackendIngressResult, BackendParticipantIdentity,
    BackendProducerOpenMetadata, BackendRouteEdgeId, BackendTransportSequence,
};

/// The RPC adapter's typed, Backend-owned ingress boundary. The query
/// lifecycle registry owns the later install/routing authorization; this port
/// only receives a wire-valid envelope and reports its exact ACK disposition.
pub(crate) trait BackendRuntimeFilterEnvelopeIngress: Send + Sync {
    fn accept(&self, envelope: BackendNativeRuntimeFilterEnvelope) -> BackendIngressResult;
}

/// Runtime-filter coordinates as they exist on the native wire before a
/// participant resolves them against an installed route graph.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum BackendNativeRouteIdentity {
    Contribution(BackendNativeContributionRouteIdentity),
    Delivery(BackendNativeDeliveryRouteIdentity),
    ProducerInstance(BackendNativeProducerInstanceRouteIdentity),
}

impl BackendNativeRouteIdentity {
    pub(crate) const fn contribution(identity: BackendNativeContributionRouteIdentity) -> Self {
        Self::Contribution(identity)
    }

    pub(crate) const fn delivery(identity: BackendNativeDeliveryRouteIdentity) -> Self {
        Self::Delivery(identity)
    }

    pub(crate) const fn producer_instance(
        identity: BackendNativeProducerInstanceRouteIdentity,
    ) -> Self {
        Self::ProducerInstance(identity)
    }

    pub(crate) const fn as_contribution(&self) -> Option<BackendNativeContributionRouteIdentity> {
        match self {
            Self::Contribution(identity) => Some(*identity),
            _ => None,
        }
    }

    pub(crate) const fn as_delivery(&self) -> Option<BackendNativeDeliveryRouteIdentity> {
        match self {
            Self::Delivery(identity) => Some(*identity),
            _ => None,
        }
    }

    pub(crate) const fn as_producer_instance(
        &self,
    ) -> Option<BackendNativeProducerInstanceRouteIdentity> {
        match self {
            Self::ProducerInstance(identity) => Some(*identity),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendNativeContributionRouteIdentity {
    producer_binding_id: RuntimeFilterBindingId,
    fragment_instance_id: UniqueId,
    partition_id: PartitionId,
    sequence: BackendTransportSequence,
}

impl BackendNativeContributionRouteIdentity {
    pub(crate) const fn new(
        producer_binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
        partition_id: PartitionId,
        sequence: BackendTransportSequence,
    ) -> Self {
        Self {
            producer_binding_id,
            fragment_instance_id,
            partition_id,
            sequence,
        }
    }

    pub(crate) const fn producer_binding_id(self) -> RuntimeFilterBindingId {
        self.producer_binding_id
    }

    pub(crate) const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) const fn partition_id(self) -> PartitionId {
        self.partition_id
    }

    pub(crate) const fn sequence(self) -> BackendTransportSequence {
        self.sequence
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendNativeDeliveryRouteIdentity {
    route_edge_id: BackendRouteEdgeId,
    sequence: BackendTransportSequence,
}

impl BackendNativeDeliveryRouteIdentity {
    pub(crate) const fn new(
        route_edge_id: BackendRouteEdgeId,
        sequence: BackendTransportSequence,
    ) -> Self {
        Self {
            route_edge_id,
            sequence,
        }
    }

    pub(crate) const fn route_edge_id(self) -> BackendRouteEdgeId {
        self.route_edge_id
    }

    pub(crate) const fn sequence(self) -> BackendTransportSequence {
        self.sequence
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct BackendNativeProducerInstanceRouteIdentity {
    producer_binding_id: RuntimeFilterBindingId,
    fragment_instance_id: UniqueId,
}

impl BackendNativeProducerInstanceRouteIdentity {
    pub(crate) const fn new(
        producer_binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
    ) -> Self {
        Self {
            producer_binding_id,
            fragment_instance_id,
        }
    }

    pub(crate) const fn producer_binding_id(self) -> RuntimeFilterBindingId {
        self.producer_binding_id
    }

    pub(crate) const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }
}

/// Strictly decoded native envelope. It intentionally precedes route-graph
/// authorization, so delivery routes do not invent a consumer binding absent
/// from the frozen protobuf shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendNativeRuntimeFilterEnvelope {
    kind: crate::runtime_filter::domain::BackendEnvelopeKind,
    participant: BackendParticipantIdentity,
    channel_id: RuntimeFilterChannelId,
    route_identity: BackendNativeRouteIdentity,
    producer_open: Option<BackendProducerOpenMetadata>,
    accept_status: Option<BackendAcceptStatus>,
    schema_digest: [u8; 32],
    payload: Arc<[u8]>,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
impl BackendNativeRuntimeFilterEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: crate::runtime_filter::domain::BackendEnvelopeKind,
        participant: BackendParticipantIdentity,
        channel_id: RuntimeFilterChannelId,
        route_identity: BackendNativeRouteIdentity,
        producer_open: Option<BackendProducerOpenMetadata>,
        accept_status: Option<BackendAcceptStatus>,
        schema_digest: [u8; 32],
        payload: impl Into<Arc<[u8]>>,
    ) -> Result<Self, String> {
        validate_native_route(kind, route_identity)?;
        validate_native_presence(kind, producer_open.is_some(), accept_status.is_some())?;
        let payload = payload.into();
        if native_kind_requires_payload(kind) && payload.is_empty() {
            return Err(format!(
                "runtime filter envelope kind {kind:?} requires a payload"
            ));
        }
        if !native_kind_requires_payload(kind) && !payload.is_empty() {
            return Err(format!(
                "runtime filter envelope kind {kind:?} forbids a payload"
            ));
        }
        Ok(Self {
            kind,
            participant,
            channel_id,
            route_identity,
            producer_open,
            accept_status,
            schema_digest,
            payload,
        })
    }

    pub(crate) const fn kind(&self) -> crate::runtime_filter::domain::BackendEnvelopeKind {
        self.kind
    }

    pub(crate) const fn participant(&self) -> BackendParticipantIdentity {
        self.participant
    }

    pub(crate) const fn query_id(&self) -> UniqueId {
        self.participant.query_id()
    }

    pub(crate) const fn deployment_epoch(&self) -> u64 {
        self.participant.deployment_epoch()
    }

    pub(crate) const fn channel_id(&self) -> RuntimeFilterChannelId {
        self.channel_id
    }

    pub(crate) const fn route_identity(&self) -> &BackendNativeRouteIdentity {
        &self.route_identity
    }

    pub(crate) const fn producer_open(&self) -> Option<BackendProducerOpenMetadata> {
        self.producer_open
    }

    pub(crate) const fn accept_status(&self) -> Option<BackendAcceptStatus> {
        self.accept_status
    }

    pub(crate) const fn schema_digest(&self) -> &[u8; 32] {
        &self.schema_digest
    }

    pub(crate) fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }
}

fn native_kind_requires_payload(kind: crate::runtime_filter::domain::BackendEnvelopeKind) -> bool {
    use crate::runtime_filter::domain::BackendEnvelopeKind;

    matches!(
        kind,
        BackendEnvelopeKind::Contribution
            | BackendEnvelopeKind::Artifact
            | BackendEnvelopeKind::FinalArtifact
            | BackendEnvelopeKind::ProducerUnavailable
            | BackendEnvelopeKind::Unavailable
            | BackendEnvelopeKind::DegradedLogical
    )
}

fn validate_native_route(
    kind: crate::runtime_filter::domain::BackendEnvelopeKind,
    route: BackendNativeRouteIdentity,
) -> Result<(), String> {
    use crate::runtime_filter::domain::BackendEnvelopeKind;

    let valid = match kind {
        BackendEnvelopeKind::Contribution | BackendEnvelopeKind::ProducerClosed => {
            matches!(route, BackendNativeRouteIdentity::Contribution(_))
        }
        BackendEnvelopeKind::ProducerUnavailable => {
            matches!(route, BackendNativeRouteIdentity::ProducerInstance(_))
        }
        BackendEnvelopeKind::Artifact
        | BackendEnvelopeKind::FinalArtifact
        | BackendEnvelopeKind::Unavailable
        | BackendEnvelopeKind::CompletedWithoutArtifact
        | BackendEnvelopeKind::DegradedLogical => {
            matches!(route, BackendNativeRouteIdentity::Delivery(_))
        }
        BackendEnvelopeKind::Ack => matches!(route, BackendNativeRouteIdentity::Contribution(_)),
    };
    valid.then_some(()).ok_or_else(|| {
        format!("runtime filter envelope kind {kind:?} has an invalid route identity")
    })
}

fn validate_native_presence(
    kind: crate::runtime_filter::domain::BackendEnvelopeKind,
    has_producer_open: bool,
    has_accept_status: bool,
) -> Result<(), String> {
    use crate::runtime_filter::domain::BackendEnvelopeKind;

    let producer_open_required = matches!(
        kind,
        BackendEnvelopeKind::Contribution | BackendEnvelopeKind::ProducerClosed
    );
    if producer_open_required && !has_producer_open {
        return Err(format!(
            "runtime filter envelope kind {kind:?} requires producer-open metadata"
        ));
    }
    if !producer_open_required && has_producer_open {
        return Err(format!(
            "runtime filter envelope kind {kind:?} forbids producer-open metadata"
        ));
    }
    let accept_status_required = kind == BackendEnvelopeKind::Ack;
    if accept_status_required && !has_accept_status {
        return Err(format!(
            "runtime filter envelope kind {kind:?} requires an accept status"
        ));
    }
    if !accept_status_required && has_accept_status {
        return Err(format!(
            "runtime filter envelope kind {kind:?} forbids an accept status"
        ));
    }
    Ok(())
}

pub(crate) fn encode_runtime_filter_envelope(
    envelope: &BackendNativeRuntimeFilterEnvelope,
) -> proto::filter::RuntimeFilterEnvelope {
    proto::filter::RuntimeFilterEnvelope {
        kind: encode_kind(envelope.kind()) as i32,
        query_id: Some(proto::common::UniqueId {
            hi: envelope.query_id().high(),
            lo: envelope.query_id().low(),
        }),
        channel_id: envelope.channel_id().get(),
        deployment_epoch: envelope.deployment_epoch(),
        route_identity: Some(encode_route_identity(envelope.route_identity())),
        schema_digest: envelope.schema_digest().to_vec(),
        payload: envelope.payload().to_vec(),
        producer_open: envelope.producer_open().map(|metadata| {
            proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count: metadata.local_partition_count().get(),
            }
        }),
    }
}

pub(crate) fn decode_runtime_filter_envelope_response(
    response: proto::filter::RuntimeFilterEnvelopeResponse,
) -> Result<(BackendNativeRouteIdentity, BackendAcceptStatus), String> {
    let identity = response
        .acked_route_identity
        .as_ref()
        .ok_or_else(|| "runtime filter ACK route identity is missing".to_string())
        .and_then(|identity| decode_route_identity(identity).map_err(|error| error.to_string()))?;
    let status = match proto::filter::RuntimeFilterAcceptStatus::try_from(response.accept_status) {
        Ok(proto::filter::RuntimeFilterAcceptStatus::Accepted) => BackendAcceptStatus::Accepted,
        Ok(proto::filter::RuntimeFilterAcceptStatus::Duplicate) => BackendAcceptStatus::Duplicate,
        Ok(proto::filter::RuntimeFilterAcceptStatus::Rejected) => BackendAcceptStatus::Rejected,
        Ok(proto::filter::RuntimeFilterAcceptStatus::Unspecified) => {
            return Err("runtime filter ACK accept status must be specified".to_string());
        }
        Err(_) => return Err("runtime filter ACK accept status is unknown".to_string()),
    };
    match status {
        BackendAcceptStatus::Accepted | BackendAcceptStatus::Duplicate
            if !response.rejection_reason.is_empty() =>
        {
            return Err("runtime filter successful ACK carried a rejection reason".to_string());
        }
        BackendAcceptStatus::Rejected if response.rejection_reason.trim().is_empty() => {
            return Err("runtime filter rejected ACK omitted its rejection reason".to_string());
        }
        _ => {}
    }
    Ok((identity, status))
}

#[expect(
    clippy::result_large_err,
    reason = "The native transport adapter returns tonic status directly."
)]
pub(crate) fn handle_runtime_filter_envelope(
    ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
    request: proto::filter::RuntimeFilterEnvelope,
) -> Result<proto::filter::RuntimeFilterEnvelopeResponse, tonic::Status> {
    let proto::filter::RuntimeFilterEnvelope {
        kind,
        query_id,
        channel_id,
        deployment_epoch,
        route_identity,
        schema_digest,
        payload,
        producer_open,
    } = request;

    let kind = decode_kind(kind)?;
    let query_id =
        query_id.ok_or_else(|| invalid_argument("runtime filter query id is missing"))?;
    let query_id = UniqueId::new(query_id.hi, query_id.lo);
    if query_id == UniqueId::new(0, 0) {
        return Err(invalid_argument("runtime filter query id must be non-zero"));
    }
    if channel_id == 0 {
        return Err(invalid_argument(
            "runtime filter channel id must be non-zero",
        ));
    }
    if deployment_epoch == 0 {
        return Err(invalid_argument(
            "runtime filter deployment epoch must be non-zero",
        ));
    }
    let route_identity = route_identity
        .ok_or_else(|| invalid_argument("runtime filter route identity is missing"))?;
    let domain_route_identity = decode_route_identity(&route_identity)?;
    // Presence is a kind-level wire invariant. Validate it before parsing the
    // metadata body so a forbidden field is never reported as a malformed
    // producer-open value.
    validate_native_presence(kind, producer_open.is_some(), false).map_err(invalid_argument)?;
    let producer_open = producer_open
        .map(|metadata| BackendProducerOpenMetadata::try_new(metadata.local_partition_count))
        .transpose()
        .map_err(transport_error)?;
    // `proto::filter::RuntimeFilterEnvelope` has no wire field for an Ack accept
    // status yet (RFD-4/M3 introduces the domain-level requirement; wiring a wire
    // representation for it is a later task), so this generic decode path can never
    // supply one. That is a no-op for every other kind, which forbids the field.
    let envelope = BackendNativeRuntimeFilterEnvelope::new(
        kind,
        BackendParticipantIdentity::new(query_id, deployment_epoch),
        RuntimeFilterChannelId::new(channel_id),
        domain_route_identity,
        producer_open,
        None,
        schema_digest.as_slice().try_into().map_err(|_| {
            invalid_argument("runtime filter schema digest must be exactly 32 bytes")
        })?,
        payload,
    )
    .map_err(transport_error)?;

    let acked_route_identity = Some(route_identity);
    let result = ingress.accept(envelope);
    if drop_accepted_contribution_response(&result, kind, query_id, deployment_epoch)? {
        return Err(tonic::Status::deadline_exceeded(
            "runner-owned runtime-filter contribution response dropped after Accepted",
        ));
    }
    let (accept_status, rejection_reason) = match result.status() {
        BackendAcceptStatus::Accepted => (
            proto::filter::RuntimeFilterAcceptStatus::Accepted,
            String::new(),
        ),
        BackendAcceptStatus::Duplicate => (
            proto::filter::RuntimeFilterAcceptStatus::Duplicate,
            String::new(),
        ),
        BackendAcceptStatus::Rejected => (
            proto::filter::RuntimeFilterAcceptStatus::Rejected,
            result
                .rejection_reason()
                .expect("rejected ingress result has a non-empty reason")
                .to_string(),
        ),
    };

    Ok(proto::filter::RuntimeFilterEnvelopeResponse {
        acked_route_identity,
        accept_status: accept_status as i32,
        rejection_reason,
    })
}

/// The test fault is deliberately claimed only after the domain owner has
/// admitted a Contribution.  The token is consumed before the error is
/// returned, so the exact retry reaches the ordinary domain dedupe owner and
/// receives its Duplicate acknowledgement.
#[cfg(debug_assertions)]
#[expect(
    clippy::result_large_err,
    reason = "The native transport adapter returns tonic status directly."
)]
fn drop_accepted_contribution_response(
    result: &BackendIngressResult,
    kind: BackendEnvelopeKind,
    query_id: UniqueId,
    deployment_epoch: u64,
) -> Result<bool, tonic::Status> {
    if kind != BackendEnvelopeKind::Contribution || result.status() != BackendAcceptStatus::Accepted
    {
        return Ok(false);
    }
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(false);
    };
    let execution_id = runtime_filter_fault_execution_id(query_id, deployment_epoch)?;
    let scope = novarocks_failpoint::claim_matching_receiver_agnostic_fault(
        &root,
        novarocks_failpoint::QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
        execution_id,
    )
    .map_err(tonic::Status::failed_precondition)?;
    Ok(scope.is_some())
}

#[cfg(debug_assertions)]
#[expect(
    clippy::result_large_err,
    reason = "The native transport adapter returns tonic status directly."
)]
fn runtime_filter_fault_execution_id(
    query_id: UniqueId,
    deployment_epoch: u64,
) -> Result<QueryExecutionId, tonic::Status> {
    QueryExecutionId::new(
        QueryId::new(query_id.high(), query_id.low()),
        AttemptId::new(deployment_epoch).map_err(|error| {
            tonic::Status::failed_precondition(format!(
                "runtime filter fault deployment epoch is not a valid attempt: {error}"
            ))
        })?,
    )
    .map_err(|error| {
        tonic::Status::failed_precondition(format!(
            "runtime filter fault execution identity is invalid: {error}"
        ))
    })
}

#[cfg(not(debug_assertions))]
fn drop_accepted_contribution_response(
    _result: &BackendIngressResult,
    _kind: BackendEnvelopeKind,
    _query_id: UniqueId,
    _deployment_epoch: u64,
) -> Result<bool, tonic::Status> {
    Ok(false)
}

#[expect(
    clippy::result_large_err,
    reason = "The native transport adapter returns tonic status directly."
)]
fn decode_kind(
    kind: i32,
) -> Result<crate::runtime_filter::domain::BackendEnvelopeKind, tonic::Status> {
    let kind = proto::filter::RuntimeFilterEnvelopeKind::try_from(kind)
        .map_err(|_| invalid_argument("runtime filter envelope kind is unknown"))?;
    match kind {
        proto::filter::RuntimeFilterEnvelopeKind::Unspecified => Err(invalid_argument(
            "runtime filter envelope kind must be specified",
        )),
        proto::filter::RuntimeFilterEnvelopeKind::Contribution => {
            Ok(crate::runtime_filter::domain::BackendEnvelopeKind::Contribution)
        }
        proto::filter::RuntimeFilterEnvelopeKind::Artifact => {
            Ok(crate::runtime_filter::domain::BackendEnvelopeKind::Artifact)
        }
        proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed => {
            Ok(crate::runtime_filter::domain::BackendEnvelopeKind::ProducerClosed)
        }
        proto::filter::RuntimeFilterEnvelopeKind::ProducerUnavailable => {
            Ok(crate::runtime_filter::domain::BackendEnvelopeKind::ProducerUnavailable)
        }
        proto::filter::RuntimeFilterEnvelopeKind::Unavailable => {
            Ok(crate::runtime_filter::domain::BackendEnvelopeKind::Unavailable)
        }
        proto::filter::RuntimeFilterEnvelopeKind::Ack => {
            Ok(crate::runtime_filter::domain::BackendEnvelopeKind::Ack)
        }
        proto::filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact => {
            Ok(crate::runtime_filter::domain::BackendEnvelopeKind::CompletedWithoutArtifact)
        }
        proto::filter::RuntimeFilterEnvelopeKind::DegradedLogical => {
            Ok(crate::runtime_filter::domain::BackendEnvelopeKind::DegradedLogical)
        }
        proto::filter::RuntimeFilterEnvelopeKind::FinalArtifact => {
            Ok(crate::runtime_filter::domain::BackendEnvelopeKind::FinalArtifact)
        }
    }
}

fn encode_kind(
    kind: crate::runtime_filter::domain::BackendEnvelopeKind,
) -> proto::filter::RuntimeFilterEnvelopeKind {
    use crate::runtime_filter::domain::BackendEnvelopeKind;

    match kind {
        BackendEnvelopeKind::Contribution => proto::filter::RuntimeFilterEnvelopeKind::Contribution,
        BackendEnvelopeKind::Artifact => proto::filter::RuntimeFilterEnvelopeKind::Artifact,
        BackendEnvelopeKind::ProducerClosed => {
            proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed
        }
        BackendEnvelopeKind::ProducerUnavailable => {
            proto::filter::RuntimeFilterEnvelopeKind::ProducerUnavailable
        }
        BackendEnvelopeKind::Unavailable => proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
        BackendEnvelopeKind::Ack => proto::filter::RuntimeFilterEnvelopeKind::Ack,
        BackendEnvelopeKind::CompletedWithoutArtifact => {
            proto::filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
        }
        BackendEnvelopeKind::DegradedLogical => {
            proto::filter::RuntimeFilterEnvelopeKind::DegradedLogical
        }
        BackendEnvelopeKind::FinalArtifact => {
            proto::filter::RuntimeFilterEnvelopeKind::FinalArtifact
        }
    }
}

fn encode_route_identity(
    identity: &BackendNativeRouteIdentity,
) -> proto::filter::RuntimeFilterRouteIdentity {
    use proto::filter::runtime_filter_route_identity::Value;

    let value = match identity {
        BackendNativeRouteIdentity::Contribution(identity) => {
            Value::Contribution(proto::filter::RuntimeFilterContributionRouteIdentity {
                producer_binding_id: identity.producer_binding_id().get(),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: identity.fragment_instance_id().high(),
                    lo: identity.fragment_instance_id().low(),
                }),
                partition_id: identity.partition_id().get(),
                sequence: identity.sequence().get(),
            })
        }
        BackendNativeRouteIdentity::Delivery(identity) => {
            Value::Delivery(proto::filter::RuntimeFilterDeliveryRouteIdentity {
                route_edge_id: identity
                    .route_edge_id()
                    .get()
                    .try_into()
                    .expect("native wire route-edge id is u32"),
                sequence: identity.sequence().get(),
            })
        }
        BackendNativeRouteIdentity::ProducerInstance(identity) => {
            Value::ProducerInstance(proto::filter::RuntimeFilterProducerInstanceRouteIdentity {
                producer_binding_id: identity.producer_binding_id().get(),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: identity.fragment_instance_id().high(),
                    lo: identity.fragment_instance_id().low(),
                }),
            })
        }
    };
    proto::filter::RuntimeFilterRouteIdentity { value: Some(value) }
}

#[expect(
    clippy::result_large_err,
    reason = "The native transport adapter returns tonic status directly."
)]
fn decode_route_identity(
    route_identity: &proto::filter::RuntimeFilterRouteIdentity,
) -> Result<BackendNativeRouteIdentity, tonic::Status> {
    use proto::filter::runtime_filter_route_identity::Value;

    match route_identity.value.as_ref() {
        Some(Value::Contribution(identity)) => {
            let fragment_instance_id = identity.fragment_instance_id.ok_or_else(|| {
                invalid_argument("runtime filter fragment instance id is missing")
            })?;
            let fragment_instance_id =
                UniqueId::new(fragment_instance_id.hi, fragment_instance_id.lo);
            if identity.producer_binding_id == 0 {
                return Err(invalid_argument(
                    "runtime filter producer binding id must be non-zero",
                ));
            }
            if fragment_instance_id == UniqueId::new(0, 0) {
                return Err(invalid_argument(
                    "runtime filter fragment instance id must be non-zero",
                ));
            }
            Ok(BackendNativeRouteIdentity::contribution(
                BackendNativeContributionRouteIdentity::new(
                    RuntimeFilterBindingId::new(identity.producer_binding_id),
                    fragment_instance_id,
                    PartitionId::new(identity.partition_id),
                    BackendTransportSequence::new(identity.sequence),
                ),
            ))
        }
        Some(Value::Delivery(identity)) => {
            if identity.route_edge_id == 0 {
                return Err(invalid_argument(
                    "runtime filter route edge id must be non-zero",
                ));
            }
            if identity.sequence == 0 {
                return Err(invalid_argument(
                    "runtime filter delivery sequence must be non-zero",
                ));
            }
            Ok(BackendNativeRouteIdentity::delivery(
                BackendNativeDeliveryRouteIdentity::new(
                    BackendRouteEdgeId::new(u64::from(identity.route_edge_id)),
                    BackendTransportSequence::new(identity.sequence),
                ),
            ))
        }
        Some(Value::ProducerInstance(identity)) => {
            let fragment_instance_id = identity.fragment_instance_id.ok_or_else(|| {
                invalid_argument("runtime filter fragment instance id is missing")
            })?;
            let fragment_instance_id =
                UniqueId::new(fragment_instance_id.hi, fragment_instance_id.lo);
            if identity.producer_binding_id == 0 {
                return Err(invalid_argument(
                    "runtime filter producer binding id must be non-zero",
                ));
            }
            if fragment_instance_id == UniqueId::new(0, 0) {
                return Err(invalid_argument(
                    "runtime filter fragment instance id must be non-zero",
                ));
            }
            Ok(BackendNativeRouteIdentity::producer_instance(
                BackendNativeProducerInstanceRouteIdentity::new(
                    RuntimeFilterBindingId::new(identity.producer_binding_id),
                    fragment_instance_id,
                ),
            ))
        }
        None => Err(invalid_argument(
            "runtime filter route identity value is missing",
        )),
    }
}

fn transport_error(error: impl std::fmt::Display) -> tonic::Status {
    invalid_argument(error.to_string())
}

fn invalid_argument(message: impl Into<String>) -> tonic::Status {
    tonic::Status::invalid_argument(message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tonic::Code;

    use novarocks_execution::runtime_filter::{
        PartitionId, RuntimeFilterBindingId as BindingId, RuntimeFilterChannelId as ChannelId,
    };
    use novarocks_proto as proto;
    use novarocks_types::UniqueId;

    use crate::runtime_filter::domain::{
        BackendAcceptStatus as RuntimeFilterAcceptStatus,
        BackendEnvelopeKind as RuntimeFilterEnvelopeKind,
        BackendIngressResult as RuntimeFilterIngressResult, BackendParticipantIdentity,
        BackendProducerOpenMetadata, BackendRouteEdgeId as RouteEdgeId,
        BackendTransportSequence as ProducerSequence,
    };

    #[cfg(debug_assertions)]
    use super::runtime_filter_fault_execution_id;
    use super::{
        BackendNativeContributionRouteIdentity, BackendNativeRouteIdentity,
        BackendNativeRuntimeFilterEnvelope as RuntimeFilterEnvelope,
        BackendRuntimeFilterEnvelopeIngress as RuntimeFilterEnvelopeIngress,
        decode_runtime_filter_envelope_response, drop_accepted_contribution_response,
        encode_runtime_filter_envelope, handle_runtime_filter_envelope,
    };

    #[derive(Debug)]
    struct RecordingIngress {
        envelopes: Mutex<Vec<RuntimeFilterEnvelope>>,
        result: RuntimeFilterIngressResult,
    }

    impl RecordingIngress {
        fn new(result: RuntimeFilterIngressResult) -> Self {
            Self {
                envelopes: Mutex::new(Vec::new()),
                result,
            }
        }

        fn take(&self) -> Vec<RuntimeFilterEnvelope> {
            std::mem::take(&mut *self.envelopes.lock().unwrap())
        }

        fn is_empty(&self) -> bool {
            self.envelopes.lock().unwrap().is_empty()
        }
    }

    impl RuntimeFilterEnvelopeIngress for RecordingIngress {
        fn accept(&self, envelope: RuntimeFilterEnvelope) -> RuntimeFilterIngressResult {
            self.envelopes.lock().unwrap().push(envelope);
            self.result.clone()
        }
    }

    fn contribution_route() -> proto::filter::RuntimeFilterRouteIdentity {
        proto::filter::RuntimeFilterRouteIdentity {
            value: Some(
                proto::filter::runtime_filter_route_identity::Value::Contribution(
                    proto::filter::RuntimeFilterContributionRouteIdentity {
                        producer_binding_id: 17,
                        fragment_instance_id: Some(proto::common::UniqueId { hi: 18, lo: 19 }),
                        partition_id: 20,
                        sequence: 21,
                    },
                ),
            ),
        }
    }

    fn delivery_route() -> proto::filter::RuntimeFilterRouteIdentity {
        proto::filter::RuntimeFilterRouteIdentity {
            value: Some(
                proto::filter::runtime_filter_route_identity::Value::Delivery(
                    proto::filter::RuntimeFilterDeliveryRouteIdentity {
                        route_edge_id: 22,
                        sequence: 23,
                    },
                ),
            ),
        }
    }

    fn producer_instance_route() -> proto::filter::RuntimeFilterRouteIdentity {
        proto::filter::RuntimeFilterRouteIdentity {
            value: Some(
                proto::filter::runtime_filter_route_identity::Value::ProducerInstance(
                    proto::filter::RuntimeFilterProducerInstanceRouteIdentity {
                        producer_binding_id: 17,
                        fragment_instance_id: Some(proto::common::UniqueId { hi: 18, lo: 19 }),
                    },
                ),
            ),
        }
    }

    fn valid_wire_envelope(
        kind: proto::filter::RuntimeFilterEnvelopeKind,
    ) -> proto::filter::RuntimeFilterEnvelope {
        let (route_identity, payload, producer_open) = match kind {
            proto::filter::RuntimeFilterEnvelopeKind::Contribution => (
                contribution_route(),
                b"contribution".to_vec(),
                Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                    local_partition_count: 24,
                }),
            ),
            proto::filter::RuntimeFilterEnvelopeKind::Artifact => {
                (delivery_route(), b"artifact".to_vec(), None)
            }
            proto::filter::RuntimeFilterEnvelopeKind::FinalArtifact => {
                (delivery_route(), b"final-artifact".to_vec(), None)
            }
            proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed => (
                contribution_route(),
                Vec::new(),
                Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                    local_partition_count: 24,
                }),
            ),
            proto::filter::RuntimeFilterEnvelopeKind::ProducerUnavailable => (
                producer_instance_route(),
                b"producer-unavailable".to_vec(),
                None,
            ),
            proto::filter::RuntimeFilterEnvelopeKind::Unavailable => {
                (delivery_route(), b"unavailable".to_vec(), None)
            }
            proto::filter::RuntimeFilterEnvelopeKind::Ack => {
                (contribution_route(), Vec::new(), None)
            }
            proto::filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact => {
                (delivery_route(), Vec::new(), None)
            }
            proto::filter::RuntimeFilterEnvelopeKind::DegradedLogical => {
                (delivery_route(), b"degraded-logical".to_vec(), None)
            }
            proto::filter::RuntimeFilterEnvelopeKind::Unspecified => {
                panic!("unspecified kind is not a valid fixture")
            }
        };
        proto::filter::RuntimeFilterEnvelope {
            kind: kind as i32,
            query_id: Some(proto::common::UniqueId { hi: 11, lo: 12 }),
            channel_id: 13,
            deployment_epoch: 14,
            route_identity: Some(route_identity),
            schema_digest: vec![15; 32],
            payload,
            producer_open,
        }
    }

    #[test]
    fn all_valid_kinds_reach_ingress_with_exact_domain_values() {
        let cases = [
            (
                proto::filter::RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::Contribution,
                b"contribution".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::Artifact,
                b"artifact".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::FinalArtifact,
                RuntimeFilterEnvelopeKind::FinalArtifact,
                b"final-artifact".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                b"".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::ProducerUnavailable,
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
                b"producer-unavailable".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
                RuntimeFilterEnvelopeKind::Unavailable,
                b"unavailable".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                b"".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::DegradedLogical,
                RuntimeFilterEnvelopeKind::DegradedLogical,
                b"degraded-logical".as_slice(),
            ),
            // Ack is intentionally excluded: RFD-4/M3 requires an Ack envelope to
            // carry an accept status (`RuntimeFilterEnvelope::accept_status`), and
            // this wire message has no field to source one from, so it can no
            // longer reach ingress as "valid" through this generic decode path.
            // See `ack_kind_is_rejected_for_missing_wire_accept_status` below.
        ];

        for (wire_kind, domain_kind, expected_payload) in cases {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            handle_runtime_filter_envelope(ingress.clone(), valid_wire_envelope(wire_kind))
                .unwrap();

            let envelopes = ingress.take();
            assert_eq!(envelopes.len(), 1);
            let envelope = &envelopes[0];
            assert_eq!(envelope.kind(), domain_kind);
            assert_eq!(envelope.query_id(), UniqueId::new(11, 12));
            assert_eq!(envelope.channel_id(), ChannelId::new(13));
            assert_eq!(envelope.deployment_epoch(), 14);
            assert_eq!(envelope.schema_digest(), &[15; 32]);
            assert_eq!(envelope.payload(), expected_payload);

            match domain_kind {
                RuntimeFilterEnvelopeKind::Contribution
                | RuntimeFilterEnvelopeKind::ProducerClosed
                | RuntimeFilterEnvelopeKind::Ack => {
                    let identity = envelope
                        .route_identity()
                        .as_contribution()
                        .expect("contribution identity");
                    assert_eq!(identity.producer_binding_id(), BindingId::new(17));
                    assert_eq!(identity.fragment_instance_id(), UniqueId::new(18, 19));
                    assert_eq!(identity.partition_id(), PartitionId::new(20));
                    assert_eq!(identity.sequence(), ProducerSequence::new(21));
                }
                RuntimeFilterEnvelopeKind::ProducerUnavailable => {
                    let identity = envelope
                        .route_identity()
                        .as_producer_instance()
                        .expect("producer-instance identity");
                    assert_eq!(identity.producer_binding_id(), BindingId::new(17));
                    assert_eq!(identity.fragment_instance_id(), UniqueId::new(18, 19));
                }
                RuntimeFilterEnvelopeKind::Artifact
                | RuntimeFilterEnvelopeKind::FinalArtifact
                | RuntimeFilterEnvelopeKind::Unavailable
                | RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
                | RuntimeFilterEnvelopeKind::DegradedLogical => {
                    let identity = envelope
                        .route_identity()
                        .as_delivery()
                        .expect("delivery identity");
                    assert_eq!(identity.route_edge_id(), RouteEdgeId::new(22));
                    assert_eq!(identity.sequence(), ProducerSequence::new(23));
                }
            }
        }
    }

    #[test]
    fn ack_kind_is_rejected_for_missing_wire_accept_status() {
        // `valid_wire_envelope` builds an otherwise well-formed Ack wire envelope, but
        // this wire message has no field to carry the accept status that RFD-4/M3
        // requires domain-side (`RuntimeFilterEnvelope::accept_status`). The adapter
        // always decodes a bare Ack with no accept status, so it is now unconditionally
        // rejected before it ever reaches ingress.
        let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
        let error = handle_runtime_filter_envelope(
            ingress.clone(),
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Ack),
        )
        .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "runtime filter envelope kind Ack requires an accept status"
        );
        assert!(ingress.is_empty());
    }

    #[test]
    fn unknown_proto_envelope_kind_is_rejected_before_ingress() {
        let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.kind = i32::MAX;

        let error = handle_runtime_filter_envelope(ingress.clone(), request).unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(error.message(), "runtime filter envelope kind is unknown");
        assert!(ingress.is_empty());
    }

    #[test]
    fn partial_unique_ids_reach_ingress_as_exact_domain_values() {
        let cases = [
            (UniqueId::new(0, 29), UniqueId::new(18, 19)),
            (UniqueId::new(31, 0), UniqueId::new(18, 19)),
            (UniqueId::new(11, 12), UniqueId::new(0, 37)),
            (UniqueId::new(11, 12), UniqueId::new(41, 0)),
        ];

        for (query_id, fragment_instance_id) in cases {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let mut request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
            request.query_id = Some(proto::common::UniqueId {
                hi: query_id.high(),
                lo: query_id.low(),
            });
            let Some(proto::filter::runtime_filter_route_identity::Value::Contribution(identity)) =
                request.route_identity.as_mut().unwrap().value.as_mut()
            else {
                unreachable!()
            };
            identity.fragment_instance_id = Some(proto::common::UniqueId {
                hi: fragment_instance_id.high(),
                lo: fragment_instance_id.low(),
            });

            let response = handle_runtime_filter_envelope(ingress.clone(), request).unwrap();
            assert_eq!(
                response.accept_status,
                proto::filter::RuntimeFilterAcceptStatus::Accepted as i32
            );

            let envelopes = ingress.take();
            assert_eq!(envelopes.len(), 1);
            let envelope = &envelopes[0];
            assert_eq!(envelope.query_id(), query_id);
            let identity = envelope
                .route_identity()
                .as_contribution()
                .expect("contribution identity");
            assert_eq!(identity.fragment_instance_id(), fragment_instance_id);
        }
    }

    #[test]
    fn zero_based_contribution_coordinates_reach_ingress_unchanged() {
        for kind in [
            proto::filter::RuntimeFilterEnvelopeKind::Contribution,
            proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
        ] {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let mut request = valid_wire_envelope(kind);
            let Some(proto::filter::runtime_filter_route_identity::Value::Contribution(identity)) =
                request.route_identity.as_mut().unwrap().value.as_mut()
            else {
                unreachable!()
            };
            identity.partition_id = 0;
            identity.sequence = 0;

            handle_runtime_filter_envelope(ingress.clone(), request).unwrap();

            let envelopes = ingress.take();
            assert_eq!(envelopes.len(), 1);
            let identity = envelopes[0]
                .route_identity()
                .as_contribution()
                .expect("contribution identity");
            assert_eq!(identity.partition_id(), PartitionId::new(0));
            assert_eq!(identity.sequence(), ProducerSequence::new(0));
        }
    }

    #[test]
    fn adapter_rejects_missing_zero_and_forbidden_open_metadata_before_ingress() {
        let mut malformed = Vec::new();
        for kind in [
            proto::filter::RuntimeFilterEnvelopeKind::Contribution,
            proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
        ] {
            let mut missing = valid_wire_envelope(kind);
            missing.producer_open = None;
            malformed.push(missing);

            let mut zero = valid_wire_envelope(kind);
            zero.producer_open = Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count: 0,
            });
            malformed.push(zero);
        }
        for kind in [
            proto::filter::RuntimeFilterEnvelopeKind::Artifact,
            proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
            proto::filter::RuntimeFilterEnvelopeKind::Ack,
        ] {
            let mut forbidden = valid_wire_envelope(kind);
            forbidden.producer_open = Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count: 24,
            });
            malformed.push(forbidden);
        }

        for request in malformed {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let result = handle_runtime_filter_envelope(ingress.clone(), request);
            assert!(
                result.is_err(),
                "invalid producer-open metadata must be rejected"
            );
            assert_eq!(result.unwrap_err().code(), Code::InvalidArgument);
            assert!(ingress.is_empty());
        }
    }

    #[test]
    fn forbidden_producer_open_presence_precedes_zero_count_validation() {
        for kind in [
            proto::filter::RuntimeFilterEnvelopeKind::Artifact,
            proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
            proto::filter::RuntimeFilterEnvelopeKind::Ack,
        ] {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let mut request = valid_wire_envelope(kind);
            request.producer_open = Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count: 0,
            });

            let error = handle_runtime_filter_envelope(ingress.clone(), request).unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument);
            assert_eq!(
                error.message(),
                format!(
                    "runtime filter envelope kind {:?} forbids producer-open metadata",
                    match kind {
                        proto::filter::RuntimeFilterEnvelopeKind::Artifact =>
                            RuntimeFilterEnvelopeKind::Artifact,
                        proto::filter::RuntimeFilterEnvelopeKind::Unavailable =>
                            RuntimeFilterEnvelopeKind::Unavailable,
                        proto::filter::RuntimeFilterEnvelopeKind::Ack =>
                            RuntimeFilterEnvelopeKind::Ack,
                        _ => unreachable!(),
                    }
                )
            );
            assert!(ingress.is_empty());
        }
    }

    #[test]
    fn adapter_preserves_exact_count_for_contribution_and_closed() {
        for (kind, local_partition_count) in [
            (proto::filter::RuntimeFilterEnvelopeKind::Contribution, 37),
            (
                proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
                u32::MAX,
            ),
        ] {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let mut request = valid_wire_envelope(kind);
            request.producer_open = Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count,
            });

            handle_runtime_filter_envelope(ingress.clone(), request).unwrap();
            let envelopes = ingress.take();
            assert_eq!(envelopes.len(), 1);
            assert_eq!(
                envelopes[0]
                    .producer_open()
                    .map(|metadata| metadata.local_partition_count().get()),
                Some(local_partition_count)
            );
        }
    }

    #[test]
    fn ingress_results_map_exactly_and_echo_validated_route() {
        let cases = [
            (
                RuntimeFilterIngressResult::accepted(),
                proto::filter::RuntimeFilterAcceptStatus::Accepted,
                "",
            ),
            (
                RuntimeFilterIngressResult::duplicate(),
                proto::filter::RuntimeFilterAcceptStatus::Duplicate,
                "",
            ),
            (
                RuntimeFilterIngressResult::rejected("not authorized").unwrap(),
                proto::filter::RuntimeFilterAcceptStatus::Rejected,
                "not authorized",
            ),
        ];

        for (result, expected_status, expected_reason) in cases {
            let ingress = Arc::new(RecordingIngress::new(result));
            let request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
            let expected_route = request.route_identity;
            let response = handle_runtime_filter_envelope(ingress.clone(), request).unwrap();

            assert_eq!(response.accept_status, expected_status as i32);
            assert_eq!(response.rejection_reason, expected_reason);
            assert_eq!(response.acked_route_identity, expected_route);
            assert_eq!(ingress.take().len(), 1);
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    fn contribution_ack_drop_fault_never_claims_non_accepted_or_non_contribution_results() {
        let query_id = UniqueId::new(11, 12);
        for (result, kind) in [
            (
                RuntimeFilterIngressResult::duplicate(),
                RuntimeFilterEnvelopeKind::Contribution,
            ),
            (
                RuntimeFilterIngressResult::rejected("rejected").unwrap(),
                RuntimeFilterEnvelopeKind::Contribution,
            ),
            (
                RuntimeFilterIngressResult::accepted(),
                RuntimeFilterEnvelopeKind::Artifact,
            ),
        ] {
            assert!(
                !drop_accepted_contribution_response(&result, kind, query_id, 14)
                    .expect("ineligible runtime-filter response cannot claim a fault"),
                "only accepted Contribution responses may consume the fault token"
            );
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    fn contribution_ack_drop_fault_uses_the_native_query_and_attempt_identity() {
        let execution_id = runtime_filter_fault_execution_id(UniqueId::new(11, 12), 14)
            .expect("positive deployment epoch is an attempt id");
        assert_eq!(execution_id.query_id().high(), 11);
        assert_eq!(execution_id.query_id().low(), 12);
        assert_eq!(execution_id.attempt_id().get(), 14);
    }

    #[test]
    fn backend_native_envelope_preserves_the_frozen_wire_fields() {
        let envelope = RuntimeFilterEnvelope::new(
            RuntimeFilterEnvelopeKind::Contribution,
            BackendParticipantIdentity::new(UniqueId::new(11, 12), 14),
            ChannelId::new(13),
            BackendNativeRouteIdentity::contribution(BackendNativeContributionRouteIdentity::new(
                BindingId::new(17),
                UniqueId::new(18, 19),
                PartitionId::new(20),
                ProducerSequence::new(21),
            )),
            Some(BackendProducerOpenMetadata::try_new(24).unwrap()),
            None,
            [15; 32],
            b"contribution".as_slice(),
        )
        .unwrap();

        assert_eq!(
            encode_runtime_filter_envelope(&envelope),
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution)
        );
    }

    #[test]
    fn ack_response_decode_preserves_route_and_accept_disposition() {
        for (status, reason, expected) in [
            (
                proto::filter::RuntimeFilterAcceptStatus::Accepted,
                "",
                RuntimeFilterAcceptStatus::Accepted,
            ),
            (
                proto::filter::RuntimeFilterAcceptStatus::Duplicate,
                "",
                RuntimeFilterAcceptStatus::Duplicate,
            ),
            (
                proto::filter::RuntimeFilterAcceptStatus::Rejected,
                "rejected by route authority",
                RuntimeFilterAcceptStatus::Rejected,
            ),
        ] {
            let (route, decoded) = decode_runtime_filter_envelope_response(
                proto::filter::RuntimeFilterEnvelopeResponse {
                    acked_route_identity: Some(contribution_route()),
                    accept_status: status as i32,
                    rejection_reason: reason.to_string(),
                },
            )
            .unwrap();
            assert_eq!(decoded, expected);
            assert!(route.as_contribution().is_some());
        }
    }

    #[test]
    fn malformed_wire_is_invalid_argument_and_never_reaches_ingress() {
        let mut malformed = Vec::new();

        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.kind = proto::filter::RuntimeFilterEnvelopeKind::Unspecified as i32;
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.kind = 99;
        malformed.push(request);

        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.query_id = None;
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.query_id = Some(proto::common::UniqueId { hi: 0, lo: 0 });
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.channel_id = 0;
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.deployment_epoch = 0;
        malformed.push(request);

        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.route_identity = None;
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.route_identity = Some(proto::filter::RuntimeFilterRouteIdentity { value: None });
        malformed.push(request);

        for mutate in [
            |identity: &mut proto::filter::RuntimeFilterContributionRouteIdentity| {
                identity.producer_binding_id = 0
            },
            |identity: &mut proto::filter::RuntimeFilterContributionRouteIdentity| {
                identity.fragment_instance_id = None
            },
            |identity: &mut proto::filter::RuntimeFilterContributionRouteIdentity| {
                identity.fragment_instance_id = Some(proto::common::UniqueId { hi: 0, lo: 0 })
            },
        ] {
            let mut request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
            let Some(proto::filter::runtime_filter_route_identity::Value::Contribution(identity)) =
                request.route_identity.as_mut().unwrap().value.as_mut()
            else {
                unreachable!()
            };
            mutate(identity);
            malformed.push(request);
        }

        for mutate in [
            |identity: &mut proto::filter::RuntimeFilterDeliveryRouteIdentity| {
                identity.route_edge_id = 0
            },
            |identity: &mut proto::filter::RuntimeFilterDeliveryRouteIdentity| {
                identity.sequence = 0
            },
        ] {
            let mut request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Artifact);
            let Some(proto::filter::runtime_filter_route_identity::Value::Delivery(identity)) =
                request.route_identity.as_mut().unwrap().value.as_mut()
            else {
                unreachable!()
            };
            mutate(identity);
            malformed.push(request);
        }

        for (kind, wrong_route) in [
            (
                proto::filter::RuntimeFilterEnvelopeKind::Contribution,
                delivery_route(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Artifact,
                contribution_route(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
                delivery_route(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
                contribution_route(),
            ),
        ] {
            let mut request = valid_wire_envelope(kind);
            request.route_identity = Some(wrong_route);
            malformed.push(request);
        }

        for digest_len in [0, 31, 33] {
            let mut request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
            request.schema_digest = vec![15; digest_len];
            malformed.push(request);
        }

        for (kind, payload) in [
            (
                proto::filter::RuntimeFilterEnvelopeKind::Contribution,
                Vec::new(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Artifact,
                Vec::new(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
                b"unexpected".to_vec(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
                Vec::new(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Ack,
                b"unexpected".to_vec(),
            ),
        ] {
            let mut request = valid_wire_envelope(kind);
            request.payload = payload;
            malformed.push(request);
        }

        assert_eq!(malformed.len(), 25);
        for request in malformed {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let error = handle_runtime_filter_envelope(ingress.clone(), request).unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument, "{error}");
            assert!(ingress.is_empty());
        }
    }

    #[test]
    fn domain_rejection_is_not_a_tonic_error() {
        let ingress = Arc::new(RecordingIngress::new(
            RuntimeFilterIngressResult::rejected("semantic rejection").unwrap(),
        ));
        // Any kind that reaches ingress exercises this mapping; Contribution is used
        // here (rather than Ack) because it does not require a wire-sourced accept
        // status to be well-formed.
        let response = handle_runtime_filter_envelope(
            ingress,
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution),
        )
        .expect("domain rejection must produce a response");

        assert_eq!(
            response.accept_status,
            proto::filter::RuntimeFilterAcceptStatus::Rejected as i32
        );
        assert_eq!(response.rejection_reason, "semantic rejection");
    }
}
