use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::runtime::fragment::io::ExchangeFrameTransmitter;

use novarocks_proto_codec::FieldPath;
#[cfg(test)]
use novarocks_proto_models::novarocks as proto;
use prost::Message;
use tokio_util::sync::CancellationToken;

use crate::{BackendDataRuntime, NativeRpcClient};

pub fn grpc_exchange_transmitter(
    runtime: BackendDataRuntime,
    send_timeout: Duration,
    #[cfg(novarocks_dev_controls)] observations: Option<
        Arc<crate::test_observation_controls::NativeObservationControls>,
    >,
) -> Arc<dyn ExchangeFrameTransmitter> {
    let grants = crate::exchange_grant_subscription::ExchangeGrantClients::new(
        runtime.clone(),
        send_timeout,
    );
    Arc::new(GrpcExchangeFrameTransmitter {
        runtime,
        send_timeout,
        grants,
        #[cfg(novarocks_dev_controls)]
        observations,
    })
}

struct GrpcExchangeFrameTransmitter {
    #[cfg(novarocks_dev_controls)]
    observations: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
    runtime: BackendDataRuntime,
    send_timeout: Duration,
    grants: Arc<crate::exchange_grant_subscription::ExchangeGrantClients>,
}

impl ExchangeFrameTransmitter for GrpcExchangeFrameTransmitter {
    #[cfg(novarocks_dev_controls)]
    fn normal_close_accepted(
        &self,
        notice: novarocks_execution::runtime::fragment::io::exchange::ExchangeNormalCloseAccepted,
    ) {
        let stream = notice.stream;
        if notice.facts.receiver_closed.is_none()
            || notice.sender_count == 0
            || stream.source().sender_ordinal() >= notice.sender_count
            || self.runtime.native_trust().local_process_identity()
                != Some(novarocks_native_trust::NativeProcessIdentity::Backend(
                    stream.source().task().backend_process_id(),
                ))
        {
            return;
        }
        let Some(controls) = self.observations.as_deref() else {
            return;
        };
        // The original sender has already accepted these facts. Observation
        // authorization can suppress diagnostics, but cannot roll back that
        // acceptance or turn it into a new protocol failure.
        let Ok(Some(observer)) = controls.observation_for(
            "exchange-snapshot",
            stream.source().task().query_execution_id(),
        ) else {
            return;
        };
        observer.observe(&format!(
            "NOVAROCKS_EXCHANGE_SNAPSHOT event=normal_close_validated destination_task={} attempt={} dest_finst={} node_id={} sender_finst={} sender_ordinal={} sender_count={} custody_through={} granted_through={} close={:?}",
            stream.destination(), stream.destination().query_execution_id().attempt_id().get(),
            stream.destination_fragment_instance_id(), stream.destination_node().get(),
            stream.source().fragment_instance_id(), stream.source().sender_ordinal(),
            notice.sender_count, notice.facts.custody_through, notice.facts.granted_through,
            notice.facts.receiver_closed,
        ));
    }

    fn deliver_unit(
        &self,
        frame: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeUnitFrame,
        stop: CancellationToken,
        activity: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeCallActivity,
        done: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeDeliveryCallback,
    ) {
        use novarocks_execution::runtime::fragment::io::exchange_unit::{
            ExchangeCallOutcome, ExchangeUnitReceipt,
        };
        use novarocks_task_codec::exchange_unit_codec as codec;
        let stream = frame.receiver.stream;
        let sequence = frame.receiver.item.sequence();
        // Keep the original pre-dispatch binding check: a conflicting
        // observation scope still refuses this request before any I/O.
        #[cfg(novarocks_dev_controls)]
        let _observer = match self
            .observations
            .as_deref()
            .map(|controls| {
                controls.observation_for(
                    "exchange-snapshot",
                    stream.source().task().query_execution_id(),
                )
            })
            .transpose()
        {
            Ok(observer) => observer.flatten(),
            Err(_) => {
                done(ExchangeCallOutcome::Refused(unit_failure(
                    novarocks_execution_contract::failure::FailureClass::Invariant,
                    464,
                )));
                drop(activity);
                return;
            }
        };
        let local = self.runtime.native_trust().local_process_identity();
        if local
            != Some(novarocks_native_trust::NativeProcessIdentity::Backend(
                stream.source().task().backend_process_id(),
            ))
        {
            done(ExchangeCallOutcome::Refused(unit_failure(
                novarocks_execution_contract::failure::FailureClass::Data,
                451,
            )));
            drop(activity);
            return;
        }
        let request = codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
            stream,
            sender_count: frame.receiver.sender_count,
            sender_id: frame.receiver.sender_id,
            backend_number: frame.receiver.backend_number,
            item: frame.receiver.item,
            payload: frame.receiver.payload,
            next_demand: frame.receiver.next_demand,
        });
        if let Err(fact) = checked_envelope(&request) {
            done(ExchangeCallOutcome::Refused(fact));
            drop(activity);
            return;
        }
        if codec::decode_delivery_request(
            request.clone(),
            FieldPath::root("outgoing_exchange_unit"),
        )
        .is_err()
        {
            done(ExchangeCallOutcome::Refused(unit_failure(
                novarocks_execution_contract::failure::FailureClass::Invariant,
                464,
            )));
            drop(activity);
            return;
        }
        let runtime = self.runtime.clone();
        let timeout = self.send_timeout;
        self.runtime.handle().spawn(async move {
            #[cfg(novarocks_dev_controls)]
            let _observation = _observer;
            // This guard follows the real local request future, including its
            // owned DTO/payload aliases. Unknown is only a result notification;
            // the caller cannot release this guard from that callback.
            let _activity = activity;
            let client = NativeRpcClient::new_backend_endpoint(
                runtime,
                frame.destination.native_endpoint().clone(),
                stream.destination().backend_process_id(),
            );
            let outcome = match client
                .deliver_exchange_unit_async(request, timeout, stop)
                .await
            {
                Ok(response) => match codec::decode_delivery_response(
                    &response,
                    stream,
                    sequence,
                    FieldPath::root("exchange_unit_response"),
                ) {
                    Ok(codec::DecodedExchangeDeliveryOutcome::Observed { verdict, facts }) => {
                        ExchangeCallOutcome::Observed(ExchangeUnitReceipt { verdict, facts })
                    }
                    Ok(codec::DecodedExchangeDeliveryOutcome::Refused(fact)) => {
                        ExchangeCallOutcome::Refused(fact)
                    }
                    Err(_) => ExchangeCallOutcome::Unknown(
                        "exchange unit response cannot be fully validated".into(),
                    ),
                },
                Err(crate::native_client::ExchangeRpcFailure::NotProcessed(detail)) => {
                    ExchangeCallOutcome::NotProcessed(detail)
                }
                Err(crate::native_client::ExchangeRpcFailure::Unknown(detail)) => {
                    ExchangeCallOutcome::Unknown(detail)
                }
            };
            done(outcome);
        });
    }

    fn register_unit_demands(
        &self,
        destination: novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        entries: Vec<
            novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeDemandEntry,
        >,
        stop: CancellationToken,
        done: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeDemandCallback,
    ) {
        use novarocks_execution::runtime::fragment::io::exchange_unit::{
            ExchangeDemandCallOutcome, ExchangeDemandOperation,
        };
        use novarocks_task_codec::exchange_unit_codec as codec;
        let streams = entries.iter().map(|entry| entry.stream).collect::<Vec<_>>();
        let Some(first) = streams.first().copied() else {
            done(ExchangeDemandCallOutcome::Refused(unit_failure(
                novarocks_execution_contract::failure::FailureClass::Invariant,
                464,
            )));
            return;
        };
        let local = self.runtime.native_trust().local_process_identity();
        if streams.iter().any(|stream| {
            local
                != Some(novarocks_native_trust::NativeProcessIdentity::Backend(
                    stream.source().task().backend_process_id(),
                ))
        }) {
            done(ExchangeDemandCallOutcome::Refused(unit_failure(
                novarocks_execution_contract::failure::FailureClass::Data,
                451,
            )));
            return;
        }
        let entries = entries
            .into_iter()
            .map(|entry| codec::DecodedExchangeDemandEntry {
                stream: entry.stream,
                operation: match entry.operation {
                    ExchangeDemandOperation::Register(demand) => {
                        codec::ExchangeDemandOperation::Demand(demand)
                    }
                    ExchangeDemandOperation::StopSender => {
                        codec::ExchangeDemandOperation::StopSender
                    }
                },
            })
            .collect::<Vec<_>>();
        let request = match codec::encode_demand_request(
            &entries,
            FieldPath::root("outgoing_exchange_demands"),
        ) {
            Ok(request) => request,
            Err(_) => {
                done(ExchangeDemandCallOutcome::Refused(unit_failure(
                    novarocks_execution_contract::failure::FailureClass::Invariant,
                    464,
                )));
                return;
            }
        };
        if let Err(fact) = checked_envelope(&request) {
            done(ExchangeDemandCallOutcome::Refused(fact));
            return;
        }
        let runtime = self.runtime.clone();
        let timeout = self.send_timeout;
        self.runtime.handle().spawn(async move {
            let client = NativeRpcClient::new_backend_endpoint(
                runtime,
                destination.native_endpoint().clone(),
                first.destination().backend_process_id(),
            );
            let outcome = match client
                .register_exchange_demand_async(request, timeout, stop)
                .await
            {
                Ok(response) => match codec::decode_demand_response(
                    &response,
                    &streams,
                    FieldPath::root("exchange_demand_response"),
                ) {
                    Ok(results) => ExchangeDemandCallOutcome::Observed(
                        results
                            .into_iter()
                            .map(|result| (result.stream, result.outcome))
                            .collect(),
                    ),
                    Err(_) => ExchangeDemandCallOutcome::Unknown(
                        "exchange demand response cannot be fully validated".into(),
                    ),
                },
                Err(crate::native_client::ExchangeRpcFailure::NotProcessed(detail)) => {
                    ExchangeDemandCallOutcome::NotProcessed(detail)
                }
                Err(crate::native_client::ExchangeRpcFailure::Unknown(detail)) => {
                    ExchangeDemandCallOutcome::Unknown(detail)
                }
            };
            done(outcome);
        });
    }

    fn watch_unit_facts(
        &self,
        destination: novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        stream: novarocks_execution_contract::ExchangeStreamIdentity,
        wake: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeGrantWake,
    ) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
        self.grants.watch(destination, stream, wake)
    }

    fn forget_unit_facts(&self, stream: novarocks_execution_contract::ExchangeStreamIdentity) {
        self.grants.forget(stream);
    }

    fn schedule_unit_retry(
        &self,
        delay: Duration,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
        self.runtime.handle().spawn(async move {
            tokio::time::sleep(delay).await;
            task();
        });
        Ok(())
    }
}

fn unit_failure(
    class: novarocks_execution_contract::failure::FailureClass,
    code: u16,
) -> novarocks_execution_contract::failure::FailureFact {
    crate::exchange_grant_subscription::exchange_failure(class, code)
}

fn checked_envelope(
    value: &impl Message,
) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
    if value.encoded_len() > novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES
    {
        Err(unit_failure(
            novarocks_execution_contract::failure::FailureClass::Resource,
            463,
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_execution::runtime::fragment::io::exchange_unit::{
        ExchangeCallActivity, ExchangeCallOutcome, ExchangeUnitFrame, ExchangeUnitReceiverFrame,
    };
    use novarocks_execution_contract::ordered_retained_stream::exchange::{
        ExchangeAdmissionVerdict, ExchangeStreamFacts, ExchangeUnitCloseDisposition,
        ExchangeUnitItem, ExchangeUnitSequence,
    };
    use novarocks_execution_contract::{
        ExchangeSource, ExchangeStreamIdentity, FragmentNodeId, TaskIdentity,
    };
    use novarocks_task_codec::exchange_unit_codec as codec;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId, UniqueId,
    };

    fn task() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    pub(crate) fn unit_frame() -> ExchangeUnitFrame {
        let destination = task();
        let source = TaskIdentity::new(
            destination.query_execution_id(),
            StageId::new(2).unwrap(),
            TaskId::new(2).unwrap(),
            destination.backend_process_id(),
        );
        let stream = ExchangeStreamIdentity::try_new(
            ExchangeSource::new(source, UniqueId::new(21, 22), 1),
            destination,
            UniqueId::new(11, 12),
            FragmentNodeId::new(7),
        )
        .unwrap();
        ExchangeUnitFrame {
            destination: novarocks_execution::runtime::endpoint::RuntimeEndpoint::new(
                "127.0.0.1",
                9090,
            )
            .unwrap(),
            receiver: ExchangeUnitReceiverFrame {
                stream,
                sender_count: 2,
                sender_id: 1,
                backend_number: 0,
                item: ExchangeUnitItem::End(ExchangeUnitSequence::try_new(5).unwrap()),
                payload: Bytes::new(),
                next_demand: None,
            },
        }
    }
    fn response(
        frame: &ExchangeUnitFrame,
        closed: ExchangeUnitCloseDisposition,
    ) -> proto::DeliverExchangeUnitResponse {
        let (custody_through, granted_through) = match closed {
            ExchangeUnitCloseDisposition::Withdrawn => (0, 0),
            ExchangeUnitCloseDisposition::Consumed { final_sequence } => {
                (final_sequence.get(), final_sequence.get() - 1)
            }
        };
        codec::encode_delivery_response(
            frame.receiver.stream,
            frame.receiver.item.sequence(),
            codec::DecodedExchangeDeliveryOutcome::Observed {
                verdict: ExchangeAdmissionVerdict::Idempotent,
                facts: ExchangeStreamFacts {
                    custody_through,
                    granted_through,
                    sender_stopped: false,
                    receiver_closed: Some(closed),
                },
            },
        )
    }
    fn state(reply: &mut proto::DeliverExchangeUnitResponse) -> &mut proto::ExchangeStreamState {
        let proto::deliver_exchange_unit_response::Outcome::Observed(observed) =
            reply.outcome.as_mut().unwrap()
        else {
            panic!("observed reply")
        };
        observed.state.as_mut().unwrap()
    }
    fn decode(
        frame: &ExchangeUnitFrame,
        reply: &proto::DeliverExchangeUnitResponse,
    ) -> Result<codec::DecodedExchangeDeliveryOutcome, novarocks_proto_codec::ProtocolError> {
        codec::decode_delivery_response(
            reply,
            frame.receiver.stream,
            frame.receiver.item.sequence(),
            FieldPath::root("test"),
        )
    }
    #[test]
    fn missing_frozen_sender_process_is_refused_before_tcp_dial() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut frame = unit_frame();
        frame.destination =
            novarocks_execution::runtime::endpoint::RuntimeEndpoint::from_socket_addr(
                listener.local_addr().unwrap(),
            );
        let transmitter = grpc_exchange_transmitter(
            crate::backend_test_support::test_backend_data_runtime(),
            Duration::from_secs(1),
            #[cfg(novarocks_dev_controls)]
            None,
        );
        let (tx, rx) = std::sync::mpsc::channel();
        let exited = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let exit = exited.clone();
        transmitter.deliver_unit(
            frame,
            CancellationToken::new(),
            ExchangeCallActivity::new(move || {
                exit.store(true, std::sync::atomic::Ordering::Release);
            }),
            Box::new(move |outcome| tx.send(outcome).unwrap()),
        );
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            ExchangeCallOutcome::Refused(_)
        ));
        assert!(exited.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    #[test]
    fn exact_normal_close_names_only_one_destination_stream() {
        let frame = unit_frame();
        let reply = response(&frame, ExchangeUnitCloseDisposition::Withdrawn);
        assert!(
            matches!(decode(&frame,&reply).unwrap(),codec::DecodedExchangeDeliveryOutcome::Observed {facts,..} if facts.receiver_closed==Some(ExchangeUnitCloseDisposition::Withdrawn))
        );
        let mut other = frame.clone();
        other.receiver.stream = ExchangeStreamIdentity::try_new(
            frame.receiver.stream.source(),
            task(),
            UniqueId::new(31, 32),
            frame.receiver.stream.destination_node(),
        )
        .unwrap();
        assert!(decode(&other, &reply).is_err());
    }
    #[test]
    fn stale_process_wrong_sender_and_wrong_request_sequence_never_become_normal_close() {
        let frame = unit_frame();
        for mutation in 0..4 {
            let mut reply = response(&frame, ExchangeUnitCloseDisposition::Withdrawn);
            match mutation {
                0 => {
                    state(&mut reply).stream.as_mut().unwrap().destination_task =
                        Some(novarocks_task_codec::identity::encode_task_identity(task()))
                }
                1 => state(&mut reply).stream.as_mut().unwrap().sender_ordinal += 1,
                2 => {
                    state(&mut reply)
                        .stream
                        .as_mut()
                        .unwrap()
                        .source_finst_id_lo += 1
                }
                _ => reply.item_sequence += 1,
            }
            assert!(decode(&frame, &reply).is_err());
        }
    }
    #[test]
    fn missing_or_unfrozen_destination_proof_fails_closed() {
        let frame = unit_frame();
        for missing in 0..3 {
            let mut reply = response(&frame, ExchangeUnitCloseDisposition::Withdrawn);
            match missing {
                0 => state(&mut reply).stream.as_mut().unwrap().destination_task = None,
                1 => state(&mut reply).stream = None,
                _ => reply.outcome = None,
            }
            assert!(decode(&frame, &reply).is_err());
        }
    }
    #[test]
    fn normal_close_requires_disposition_and_preserves_consumed_final_sequence() {
        let frame = unit_frame();
        let final_sequence = ExchangeUnitSequence::try_new(5).unwrap();
        let valid = response(
            &frame,
            ExchangeUnitCloseDisposition::Consumed { final_sequence },
        );
        assert!(
            matches!(decode(&frame,&valid).unwrap(),codec::DecodedExchangeDeliveryOutcome::Observed{facts,..} if facts.receiver_closed==Some(ExchangeUnitCloseDisposition::Consumed{final_sequence}))
        );
        let mut missing = valid.clone();
        state(&mut missing)
            .receiver_closed
            .as_mut()
            .unwrap()
            .disposition = None;
        assert!(decode(&frame, &missing).is_err());
        let mut zero = valid;
        state(&mut zero)
            .receiver_closed
            .as_mut()
            .unwrap()
            .disposition = Some(proto::exchange_unit_receiver_closed::Disposition::Consumed(
            proto::ExchangeUnitConsumed { final_sequence: 0 },
        ));
        assert!(decode(&frame, &zero).is_err());
    }
}
