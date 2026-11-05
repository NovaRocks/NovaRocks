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
//! Native Exchange Unit wire projection and Worker-owned route settlement.

use novarocks_execution::runtime::task::io::{ExchangeReceiverKey, ExchangeReceiverPort};
use novarocks_execution_contract::ExchangeStreamIdentity;
use novarocks_proto_models as proto;
use novarocks_worker::{InboundFrameClaim, NormalClosedInbound, TaskInboundCapabilities};
use std::sync::{Arc, OnceLock};

#[cfg(test)]
pub(crate) struct ExchangeRouteQuery {
    pub stream: ExchangeStreamIdentity,
    pub sender_count: u32,
}

/// Native exchange ingress composition.
///
/// This owns only wire-facing ports. Each route authority and the receiver it
/// delegates to remain owned by their respective runtime domains.
#[derive(Clone)]
pub struct NativeExchangeDataPlane {
    receiver_port: Arc<dyn ExchangeReceiverPort>,
    unit_capabilities: Arc<TaskInboundCapabilities>,
    unit_backend: novarocks_types::BackendProcessId,
    unit_publisher: Arc<
        OnceLock<
            Result<
                Arc<crate::exchange_grant_subscription::ExchangeGrantPublisher>,
                novarocks_execution_contract::failure::FailureFact,
            >,
        >,
    >,
}

impl NativeExchangeDataPlane {
    pub fn new(
        receiver_port: Arc<dyn ExchangeReceiverPort>,
        capabilities: Arc<TaskInboundCapabilities>,
        backend: novarocks_types::BackendProcessId,
    ) -> Self {
        Self {
            receiver_port,
            unit_capabilities: capabilities,
            unit_backend: backend,
            unit_publisher: Arc::new(OnceLock::new()),
        }
    }

    /// Installs the single change feed before any subscription enumerates the
    /// authoritative state. No alternate route owner or transport is consulted.
    pub(crate) fn initialize_unit_publications(
        &self,
    ) -> Result<
        Arc<crate::exchange_grant_subscription::ExchangeGrantPublisher>,
        novarocks_execution_contract::failure::FailureFact,
    > {
        self.unit_publisher
            .get_or_init(|| {
                crate::exchange_grant_subscription::ExchangeGrantPublisher::install(Arc::clone(
                    &self.receiver_port,
                ))
            })
            .clone()
    }

    pub(crate) fn deliver_unit(
        &self,
        raw: proto::novarocks::DeliverExchangeUnitRequest,
        caller: novarocks_types::BackendProcessId,
    ) -> Result<proto::novarocks::DeliverExchangeUnitResponse, tonic::Status> {
        use novarocks_execution::runtime::task::io::exchange_unit::{
            ExchangeDemandEntry, ExchangeDemandOperation, ExchangeUnitReceiverFrame,
        };
        use novarocks_task_codec::exchange_unit_codec as codec;
        let frame = codec::decode_delivery_request(
            raw,
            novarocks_proto_codec::FieldPath::root("exchange_unit_request"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let stream = frame.stream;
        self.check_unit_pair(codec::ExchangeProcessPair::for_stream(stream), caller)?;
        let sequence = frame.item.sequence();
        let sender_count = frame.sender_count;
        let outcome = {
            let capabilities = &self.unit_capabilities;
            let key = ExchangeReceiverKey {
                task_key: stream.destination_task_key(),
                node_id: stream.destination_node().get(),
            };
            let next = frame.next_demand;
            let delivery = capabilities.with_claimed_stream(stream, |admission| {
                if admission.sender_count() != frame.sender_count {
                    return Err(unit_route_failure(460));
                }
                let mut receipt = self.receiver_port.receive_unit(
                    key,
                    ExchangeUnitReceiverFrame {
                        stream,
                        sender_count: frame.sender_count,
                        sender_id: frame.sender_id,
                        backend_number: frame.backend_number,
                        item: frame.item,
                        payload: frame.payload,
                        next_demand: None,
                    },
                )?;
                if let Some(demand) = next {
                    let observation = self.receiver_port.register_unit_demand(
                        key,
                        ExchangeDemandEntry {
                            stream,
                            operation: ExchangeDemandOperation::Register(demand),
                        },
                    )?;
                    if observation.stream != stream {
                        return Err(unit_route_failure(462));
                    }
                    receipt.facts = observation.facts;
                }
                validate_unit_facts(stream, receipt.facts)?;
                Ok(receipt)
            });
            match delivery {
                Ok(Ok(receipt)) => codec::DecodedExchangeDeliveryOutcome::Observed {
                    verdict: receipt.verdict,
                    facts: receipt.facts,
                },
                Ok(Err(fact)) => codec::DecodedExchangeDeliveryOutcome::Refused(fact),
                Err(InboundFrameClaim::NormallyClosed(proof)) => {
                    match unit_normal_close_facts(proof, stream, Some(sender_count)) {
                            Ok(facts) => codec::DecodedExchangeDeliveryOutcome::Observed {
                                verdict: novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeAdmissionVerdict::Idempotent,
                                facts,
                            },
                            Err(fact) => codec::DecodedExchangeDeliveryOutcome::Refused(fact),
                        }
                }
                Err(claim) => {
                    codec::DecodedExchangeDeliveryOutcome::Refused(unit_claim_failure(claim))
                }
            }
        };
        Ok(codec::encode_delivery_response(stream, sequence, outcome))
    }

    pub(crate) fn register_unit_demands(
        &self,
        raw: proto::novarocks::RegisterExchangeDemandRequest,
        caller: novarocks_types::BackendProcessId,
    ) -> Result<proto::novarocks::RegisterExchangeDemandResponse, tonic::Status> {
        use novarocks_execution::runtime::task::io::exchange_unit::{
            ExchangeDemandEntry, ExchangeDemandOperation,
        };
        use novarocks_task_codec::exchange_unit_codec as codec;
        let batch = codec::decode_demand_request(
            &raw,
            novarocks_proto_codec::FieldPath::root("exchange_demand_request"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        self.check_unit_pair(batch.pair, caller)?;
        let mut outcomes = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            let stream = entry.stream;
            let outcome = match self.unit_capabilities.with_claimed_stream(stream, |_| {
                let observation = self.receiver_port.register_unit_demand(
                    ExchangeReceiverKey {
                        task_key: stream.destination_task_key(),
                        node_id: stream.destination_node().get(),
                    },
                    ExchangeDemandEntry {
                        stream,
                        operation: match entry.operation {
                            codec::ExchangeDemandOperation::Demand(demand) => {
                                ExchangeDemandOperation::Register(demand)
                            }
                            codec::ExchangeDemandOperation::StopSender => {
                                ExchangeDemandOperation::StopSender
                            }
                        },
                    },
                )?;
                if observation.stream != stream {
                    return Err(unit_route_failure(462));
                }
                validate_unit_facts(stream, observation.facts)?;
                Ok(observation.facts)
            }) {
                Ok(outcome) => outcome,
                Err(InboundFrameClaim::NormallyClosed(proof)) => {
                    unit_normal_close_facts(proof, stream, None)
                }
                Err(claim) => Err(unit_claim_failure(claim)),
            };
            outcomes.push(codec::DecodedExchangeDemandResult { stream, outcome });
        }
        Ok(codec::encode_demand_response(&outcomes))
    }

    pub(crate) fn subscribe_unit_grants(
        &self,
        raw: proto::novarocks::SubscribeExchangeGrantsRequest,
        caller: novarocks_types::BackendProcessId,
        ownership: Option<Arc<crate::native_ingress::NativeIngressOwnership>>,
    ) -> Result<crate::exchange_grant_subscription::ExchangeGrantStream, tonic::Status> {
        use novarocks_task_codec::exchange_unit_codec as codec;
        let subscription = codec::decode_grant_subscription(
            &raw,
            novarocks_proto_codec::FieldPath::root("exchange_grant_subscription"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        self.check_unit_pair(subscription.pair, caller)?;
        let publisher = self
            .initialize_unit_publications()
            .map_err(crate::exchange_grant_subscription::failure_status)?;
        publisher.subscribe(subscription, ownership)
    }

    fn check_unit_pair(
        &self,
        pair: novarocks_task_codec::exchange_unit_codec::ExchangeProcessPair,
        caller: novarocks_types::BackendProcessId,
    ) -> Result<(), tonic::Status> {
        if caller != pair.sender {
            return Err(tonic::Status::permission_denied(
                "exchange sender differs from authenticated backend process",
            ));
        }
        let backend = self.unit_backend;
        if backend != pair.receiver {
            return Err(tonic::Status::failed_precondition(
                "exchange destination differs from this backend process",
            ));
        }
        Ok(())
    }
}

pub(crate) fn authenticated_unit_sender<T>(
    request: &tonic::Request<T>,
) -> Result<novarocks_types::BackendProcessId, tonic::Status> {
    match request
        .extensions()
        .get::<novarocks_native_trust::AuthenticatedNativeCaller>()
        .and_then(|caller| caller.process_identity())
    {
        Some(novarocks_native_trust::NativeProcessIdentity::Backend(sender)) => Ok(sender),
        _ => Err(tonic::Status::unauthenticated(
            "exchange requires an authenticated backend process identity",
        )),
    }
}

fn unit_route_failure(code: u16) -> novarocks_execution_contract::failure::FailureFact {
    crate::exchange_grant_subscription::exchange_failure(
        novarocks_execution_contract::failure::FailureClass::Invariant,
        code,
    )
}

/// Project only the Worker's retained, source-authored close fact. A close
/// answers replay; it grants no authority to decode or accept another payload.
fn unit_normal_close_facts(
    proof: NormalClosedInbound,
    stream: ExchangeStreamIdentity,
    sender_count: Option<u32>,
) -> Result<
    novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeStreamFacts,
    novarocks_execution_contract::failure::FailureFact,
> {
    use novarocks_execution_contract::ordered_retained_stream::exchange::{
        ExchangeStreamFacts, ExchangeUnitCloseDisposition,
    };
    let close = proof.close();
    if close.stream != stream
        || proof.sender_count() == 0
        || stream.source().sender_ordinal() >= proof.sender_count()
        || sender_count.is_some_and(|count| count != proof.sender_count())
    {
        return Err(unit_route_failure(460));
    }
    let mut facts = ExchangeStreamFacts {
        receiver_closed: Some(close.disposition),
        ..Default::default()
    };
    if let ExchangeUnitCloseDisposition::Consumed { final_sequence } = close.disposition {
        facts.custody_through = final_sequence.get();
        facts.granted_through = final_sequence
            .get()
            .checked_sub(1)
            .ok_or_else(|| unit_route_failure(462))?;
    }
    validate_unit_facts(stream, facts)?;
    Ok(facts)
}

fn unit_claim_failure(
    claim: InboundFrameClaim,
) -> novarocks_execution_contract::failure::FailureFact {
    match claim {
        InboundFrameClaim::NormallyClosed(_)
        | InboundFrameClaim::NotHeld
        | InboundFrameClaim::Refused(_)
        | InboundFrameClaim::Authorized => unit_route_failure(460),
    }
}

fn validate_unit_facts(
    stream: ExchangeStreamIdentity,
    facts: novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeStreamFacts,
) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
    use novarocks_task_codec::exchange_unit_codec as codec;
    codec::decode_stream_state(
        &codec::encode_stream_state(codec::DecodedExchangeStreamState { stream, facts }),
        novarocks_proto_codec::FieldPath::root("exchange_receiver_facts"),
    )
    .map(|_| ())
    .map_err(|_| unit_route_failure(462))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution::runtime::task::io::UnavailableExchangeReceiverPort;
    use novarocks_proto_codec::FieldPath;
    use novarocks_task_codec::exchange_unit_codec as codec;

    fn request() -> codec::DecodedExchangeUnitFrame {
        let frame = crate::exchange_transmitter::tests::unit_frame().receiver;
        codec::DecodedExchangeUnitFrame {
            stream: frame.stream,
            sender_count: frame.sender_count,
            sender_id: frame.sender_id,
            backend_number: frame.backend_number,
            item: frame.item,
            payload: frame.payload,
            next_demand: None,
        }
    }
    fn plane(frame: &codec::DecodedExchangeUnitFrame) -> NativeExchangeDataPlane {
        NativeExchangeDataPlane::new(
            Arc::new(UnavailableExchangeReceiverPort),
            TaskInboundCapabilities::new(),
            frame.stream.destination().backend_process_id(),
        )
    }
    #[test]
    fn an_unheld_exact_unit_route_is_refused_before_receiver_delivery() {
        let frame = request();
        let plane = plane(&frame);
        let reply = plane
            .deliver_unit(
                codec::encode_delivery_request(frame.clone()),
                frame.stream.source().task().backend_process_id(),
            )
            .unwrap();
        let outcome = codec::decode_delivery_response(
            &reply,
            frame.stream,
            frame.item.sequence(),
            FieldPath::root("test"),
        )
        .unwrap();
        assert!(
            matches!(outcome,codec::DecodedExchangeDeliveryOutcome::Refused(fact) if fact.reason().code()==460)
        );
    }
    #[test]
    fn malformed_unit_envelope_is_refused_before_worker_or_receiver_authorization() {
        let frame = request();
        let plane = plane(&frame);
        for mutation in 0..7 {
            let mut raw = codec::encode_delivery_request(frame.clone());
            match mutation {
                0 => raw.stream = None,
                1 => raw.stream.as_mut().unwrap().destination_task_key_lo += 1,
                2 => raw.stream.as_mut().unwrap().sender_ordinal += 1,
                3 => raw.sender_count = 0,
                4 => raw.sender_count = raw.sender_ordinal,
                5 => raw.node_id = -1,
                _ => {
                    raw.item = Some(proto::novarocks::deliver_exchange_unit_request::Item::End(
                        proto::novarocks::ExchangeUnitEnd { sequence: 0 },
                    ))
                }
            }
            assert_eq!(
                plane
                    .deliver_unit(raw, frame.stream.source().task().backend_process_id())
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
    }
    #[test]
    fn authenticated_pair_cannot_substitute_another_backend_or_reused_destination() {
        let frame = request();
        let raw = codec::encode_delivery_request(frame.clone());
        assert_eq!(
            plane(&frame)
                .deliver_unit(raw.clone(), novarocks_types::BackendProcessId::new_v7())
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let wrong = NativeExchangeDataPlane::new(
            Arc::new(UnavailableExchangeReceiverPort),
            TaskInboundCapabilities::new(),
            novarocks_types::BackendProcessId::new_v7(),
        );
        assert_eq!(
            wrong
                .deliver_unit(raw, frame.stream.source().task().backend_process_id())
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
