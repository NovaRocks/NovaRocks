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

use novarocks::proto;
use novarocks::runtime_filter_transition::model::contract::{BindingId, ChannelId};
use novarocks::runtime_filter_transition::port::identity::{
    DeploymentEpoch, PartitionId, ProducerSequence, RouteEdgeId,
};
use novarocks::runtime_filter_transition::port::transport::{
    ContributionRouteIdentity, DeliveryRouteIdentity, ProducerInstanceRouteIdentity,
    ProducerOpenMetadata, RuntimeFilterAcceptStatus, RuntimeFilterEnvelope,
    RuntimeFilterEnvelopeIngress, RuntimeFilterEnvelopeKind, RuntimeFilterRouteIdentity,
    RuntimeFilterTransportError,
};
use novarocks_types::UniqueId;

pub(crate) fn encode_runtime_filter_envelope(
    envelope: &RuntimeFilterEnvelope,
) -> proto::filter::RuntimeFilterEnvelope {
    proto::filter::RuntimeFilterEnvelope {
        kind: encode_kind(envelope.kind()) as i32,
        query_id: Some(proto::common::UniqueId {
            hi: envelope.query_id().high(),
            lo: envelope.query_id().low(),
        }),
        channel_id: envelope.channel_id().get(),
        deployment_epoch: envelope.deployment_epoch().get(),
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
) -> Result<(RuntimeFilterRouteIdentity, RuntimeFilterAcceptStatus), String> {
    let identity = response
        .acked_route_identity
        .as_ref()
        .ok_or_else(|| "runtime filter ACK route identity is missing".to_string())
        .and_then(|identity| decode_route_identity(identity).map_err(|error| error.to_string()))?;
    let status = match proto::filter::RuntimeFilterAcceptStatus::try_from(response.accept_status) {
        Ok(proto::filter::RuntimeFilterAcceptStatus::Accepted) => {
            RuntimeFilterAcceptStatus::Accepted
        }
        Ok(proto::filter::RuntimeFilterAcceptStatus::Duplicate) => {
            RuntimeFilterAcceptStatus::Duplicate
        }
        Ok(proto::filter::RuntimeFilterAcceptStatus::Rejected) => {
            RuntimeFilterAcceptStatus::Rejected
        }
        Ok(proto::filter::RuntimeFilterAcceptStatus::Unspecified) => {
            return Err("runtime filter ACK accept status must be specified".to_string());
        }
        Err(_) => return Err("runtime filter ACK accept status is unknown".to_string()),
    };
    match status {
        RuntimeFilterAcceptStatus::Accepted | RuntimeFilterAcceptStatus::Duplicate
            if !response.rejection_reason.is_empty() =>
        {
            return Err("runtime filter successful ACK carried a rejection reason".to_string());
        }
        RuntimeFilterAcceptStatus::Rejected if response.rejection_reason.trim().is_empty() => {
            return Err("runtime filter rejected ACK omitted its rejection reason".to_string());
        }
        _ => {}
    }
    Ok((identity, status))
}

pub(crate) fn handle_runtime_filter_envelope(
    ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
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
    let route_identity = route_identity
        .ok_or_else(|| invalid_argument("runtime filter route identity is missing"))?;
    let domain_route_identity = decode_route_identity(&route_identity)?;
    let producer_open = ProducerOpenMetadata::try_from_raw_for_kind(
        kind,
        producer_open.map(|metadata| metadata.local_partition_count),
    )
    .map_err(transport_error)?;
    // `proto::filter::RuntimeFilterEnvelope` has no wire field for an Ack accept
    // status yet (RFD-4/M3 introduces the domain-level requirement; wiring a wire
    // representation for it is a later task), so this generic decode path can never
    // supply one. That is a no-op for every other kind, which forbids the field.
    let envelope = RuntimeFilterEnvelope::try_new(
        kind,
        query_id,
        ChannelId::new(channel_id),
        DeploymentEpoch::new(deployment_epoch),
        domain_route_identity,
        producer_open,
        None,
        &schema_digest,
        payload,
    )
    .map_err(transport_error)?;

    let acked_route_identity = Some(route_identity.clone());
    let result = ingress.accept(envelope);
    let (accept_status, rejection_reason) = match result.accept_status() {
        RuntimeFilterAcceptStatus::Accepted => (
            proto::filter::RuntimeFilterAcceptStatus::Accepted,
            String::new(),
        ),
        RuntimeFilterAcceptStatus::Duplicate => (
            proto::filter::RuntimeFilterAcceptStatus::Duplicate,
            String::new(),
        ),
        RuntimeFilterAcceptStatus::Rejected => (
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

fn decode_kind(kind: i32) -> Result<RuntimeFilterEnvelopeKind, tonic::Status> {
    let kind = proto::filter::RuntimeFilterEnvelopeKind::try_from(kind)
        .map_err(|_| invalid_argument("runtime filter envelope kind is unknown"))?;
    match kind {
        proto::filter::RuntimeFilterEnvelopeKind::Unspecified => Err(invalid_argument(
            "runtime filter envelope kind must be specified",
        )),
        proto::filter::RuntimeFilterEnvelopeKind::Contribution => {
            Ok(RuntimeFilterEnvelopeKind::Contribution)
        }
        proto::filter::RuntimeFilterEnvelopeKind::Artifact => {
            Ok(RuntimeFilterEnvelopeKind::Artifact)
        }
        proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed => {
            Ok(RuntimeFilterEnvelopeKind::ProducerClosed)
        }
        proto::filter::RuntimeFilterEnvelopeKind::ProducerUnavailable => {
            Ok(RuntimeFilterEnvelopeKind::ProducerUnavailable)
        }
        proto::filter::RuntimeFilterEnvelopeKind::Unavailable => {
            Ok(RuntimeFilterEnvelopeKind::Unavailable)
        }
        proto::filter::RuntimeFilterEnvelopeKind::Ack => Ok(RuntimeFilterEnvelopeKind::Ack),
        proto::filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact => {
            Ok(RuntimeFilterEnvelopeKind::CompletedWithoutArtifact)
        }
        proto::filter::RuntimeFilterEnvelopeKind::DegradedLogical => {
            Ok(RuntimeFilterEnvelopeKind::DegradedLogical)
        }
        proto::filter::RuntimeFilterEnvelopeKind::FinalArtifact => {
            Ok(RuntimeFilterEnvelopeKind::FinalArtifact)
        }
    }
}

fn encode_kind(kind: RuntimeFilterEnvelopeKind) -> proto::filter::RuntimeFilterEnvelopeKind {
    match kind {
        RuntimeFilterEnvelopeKind::Contribution => {
            proto::filter::RuntimeFilterEnvelopeKind::Contribution
        }
        RuntimeFilterEnvelopeKind::Artifact => proto::filter::RuntimeFilterEnvelopeKind::Artifact,
        RuntimeFilterEnvelopeKind::ProducerClosed => {
            proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed
        }
        RuntimeFilterEnvelopeKind::ProducerUnavailable => {
            proto::filter::RuntimeFilterEnvelopeKind::ProducerUnavailable
        }
        RuntimeFilterEnvelopeKind::Unavailable => {
            proto::filter::RuntimeFilterEnvelopeKind::Unavailable
        }
        RuntimeFilterEnvelopeKind::Ack => proto::filter::RuntimeFilterEnvelopeKind::Ack,
        RuntimeFilterEnvelopeKind::CompletedWithoutArtifact => {
            proto::filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
        }
        RuntimeFilterEnvelopeKind::DegradedLogical => {
            proto::filter::RuntimeFilterEnvelopeKind::DegradedLogical
        }
        RuntimeFilterEnvelopeKind::FinalArtifact => {
            proto::filter::RuntimeFilterEnvelopeKind::FinalArtifact
        }
    }
}

fn encode_route_identity(
    identity: &RuntimeFilterRouteIdentity,
) -> proto::filter::RuntimeFilterRouteIdentity {
    use proto::filter::runtime_filter_route_identity::Value;

    let value = if let Some(identity) = identity.as_contribution() {
        Value::Contribution(proto::filter::RuntimeFilterContributionRouteIdentity {
            producer_binding_id: identity.producer_binding_id().get(),
            fragment_instance_id: Some(proto::common::UniqueId {
                hi: identity.fragment_instance_id().high(),
                lo: identity.fragment_instance_id().low(),
            }),
            partition_id: identity.partition_id().get(),
            sequence: identity.sequence().get(),
        })
    } else if let Some(identity) = identity.as_delivery() {
        Value::Delivery(proto::filter::RuntimeFilterDeliveryRouteIdentity {
            route_edge_id: identity.route_edge_id().get(),
            sequence: identity.sequence().get(),
        })
    } else {
        let identity = identity
            .as_producer_instance()
            .expect("runtime filter route identity is typed");
        Value::ProducerInstance(proto::filter::RuntimeFilterProducerInstanceRouteIdentity {
            producer_binding_id: identity.producer_binding_id().get(),
            fragment_instance_id: Some(proto::common::UniqueId {
                hi: identity.fragment_instance_id().high(),
                lo: identity.fragment_instance_id().low(),
            }),
        })
    };
    proto::filter::RuntimeFilterRouteIdentity { value: Some(value) }
}

fn decode_route_identity(
    route_identity: &proto::filter::RuntimeFilterRouteIdentity,
) -> Result<RuntimeFilterRouteIdentity, tonic::Status> {
    use proto::filter::runtime_filter_route_identity::Value;

    match route_identity.value.as_ref() {
        Some(Value::Contribution(identity)) => {
            let fragment_instance_id = identity.fragment_instance_id.ok_or_else(|| {
                invalid_argument("runtime filter fragment instance id is missing")
            })?;
            let identity = ContributionRouteIdentity::try_new(
                BindingId::new(identity.producer_binding_id),
                UniqueId::new(fragment_instance_id.hi, fragment_instance_id.lo),
                PartitionId::new(identity.partition_id),
                ProducerSequence::new(identity.sequence),
            )
            .map_err(transport_error)?;
            Ok(RuntimeFilterRouteIdentity::contribution(identity))
        }
        Some(Value::Delivery(identity)) => {
            let identity = DeliveryRouteIdentity::try_new(
                RouteEdgeId::new(identity.route_edge_id),
                ProducerSequence::new(identity.sequence),
            )
            .map_err(transport_error)?;
            Ok(RuntimeFilterRouteIdentity::delivery(identity))
        }
        Some(Value::ProducerInstance(identity)) => {
            let fragment_instance_id = identity.fragment_instance_id.ok_or_else(|| {
                invalid_argument("runtime filter fragment instance id is missing")
            })?;
            let identity = ProducerInstanceRouteIdentity::try_new(
                BindingId::new(identity.producer_binding_id),
                UniqueId::new(fragment_instance_id.hi, fragment_instance_id.lo),
            )
            .map_err(transport_error)?;
            Ok(RuntimeFilterRouteIdentity::producer_instance(identity))
        }
        None => Err(invalid_argument(
            "runtime filter route identity value is missing",
        )),
    }
}

fn transport_error(error: RuntimeFilterTransportError) -> tonic::Status {
    invalid_argument(error.to_string())
}

fn invalid_argument(message: impl Into<String>) -> tonic::Status {
    tonic::Status::invalid_argument(message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tonic::Code;

    use novarocks::proto;
    use novarocks::runtime_filter_transition::model::contract::{BindingId, ChannelId};
    use novarocks::runtime_filter_transition::port::identity::{
        DeploymentEpoch, PartitionId, ProducerSequence, RouteEdgeId,
    };
    use novarocks::runtime_filter_transition::port::transport::{
        RuntimeFilterEnvelope, RuntimeFilterEnvelopeIngress, RuntimeFilterEnvelopeKind,
        RuntimeFilterIngressResult,
    };
    use novarocks_types::UniqueId;

    use super::handle_runtime_filter_envelope;

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
            assert_eq!(envelope.deployment_epoch(), DeploymentEpoch::new(14));
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
            let expected_route = request.route_identity.clone();
            let response = handle_runtime_filter_envelope(ingress.clone(), request).unwrap();

            assert_eq!(response.accept_status, expected_status as i32);
            assert_eq!(response.rejection_reason, expected_reason);
            assert_eq!(response.acked_route_identity, expected_route);
            assert_eq!(ingress.take().len(), 1);
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
