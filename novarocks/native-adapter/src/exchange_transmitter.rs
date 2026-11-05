use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::runtime::task::io::ExchangeFrameTransmitter;

use novarocks_proto_codec::FieldPath;
#[cfg(test)]
use novarocks_proto_models::novarocks as proto;
use prost::Message;
use tokio_util::sync::CancellationToken;

use crate::{BackendDataRuntime, NativeRpcClient};

pub fn grpc_exchange_transmitter(
    runtime: BackendDataRuntime,
    send_timeout: Duration,
    capabilities: Arc<novarocks_worker::TaskInboundCapabilities>,
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
        capabilities,
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
    capabilities: Arc<novarocks_worker::TaskInboundCapabilities>,
    grants: Arc<crate::exchange_grant_subscription::ExchangeGrantClients>,
}

impl ExchangeFrameTransmitter for GrpcExchangeFrameTransmitter {
    #[cfg(novarocks_dev_controls)]
    fn normal_close_accepted(
        &self,
        notice: novarocks_execution::runtime::task::io::exchange::ExchangeNormalCloseAccepted,
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
            novarocks_failpoint::observation_marker::ObservationAction::ExchangeSnapshot.as_str(),
            stream.source().task().query_execution_id(),
        ) else {
            return;
        };
        observer.observe(&format!(
            "{} event=normal_close_validated destination_task={} attempt={} destination_task_key={} node_id={} sender_task_key={} sender_ordinal={} sender_count={} custody_through={} granted_through={} close={:?}", novarocks_failpoint::observation_marker::EXCHANGE_SNAPSHOT,
            stream.destination(), stream.destination().query_execution_id().attempt_id().get(),
            stream.destination_task_key(), stream.destination_node().get(),
            stream.source().task_key(), stream.source().sender_ordinal(),
            notice.sender_count, notice.facts.custody_through, notice.facts.granted_through,
            notice.facts.receiver_closed,
        ));
    }

    fn deliver_unit(
        &self,
        frame: novarocks_execution::runtime::task::io::exchange_unit::ExchangeUnitFrame,
        stop: CancellationToken,
        activity: novarocks_execution::runtime::task::io::exchange_unit::ExchangeCallActivity,
        done: novarocks_execution::runtime::task::io::exchange_unit::ExchangeDeliveryCallback,
    ) {
        use novarocks_execution::runtime::task::io::exchange_unit::{
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
                    novarocks_failpoint::observation_marker::ObservationAction::ExchangeSnapshot
                        .as_str(),
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
        entries: Vec<novarocks_execution::runtime::task::io::exchange_unit::ExchangeDemandEntry>,
        stop: CancellationToken,
        activity: novarocks_execution::runtime::task::io::exchange_unit::ExchangeCallActivity,
        done: novarocks_execution::runtime::task::io::exchange_unit::ExchangeDemandCallback,
    ) {
        use novarocks_execution::runtime::task::io::exchange_unit::{
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
        let stopping_one =
            entries.len() == 1 && entries[0].operation == ExchangeDemandOperation::StopSender;
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
        // Encoding is complete before the asynchronous request takes over.
        drop(entries);
        // Read only the original destination Context's positive abort fact.
        // Same-process routing still uses Native RPC for data and ordinary
        // control. This fact discharges only an already-stopped stream's debt;
        // it neither fabricates receiver facts nor releases request activity.
        if stopping_one
            && first.source().task().query_execution_id()
                == first.destination().query_execution_id()
            && local
                == Some(novarocks_native_trust::NativeProcessIdentity::Backend(
                    first.destination().backend_process_id(),
                ))
            && self
                .capabilities
                .participation_admission_aborted(first.destination().query_execution_id())
        {
            done(ExchangeDemandCallOutcome::RouteRetired(first));
            return;
        }
        let runtime = self.runtime.clone();
        let timeout = self.send_timeout;
        self.runtime.handle().spawn(async move {
            // The inner future owns all request/client captures. The original
            // activity remains outside it and outside the outcome callback.
            let outcome = async move {
                let client = NativeRpcClient::new_backend_endpoint(
                    runtime,
                    destination.native_endpoint().clone(),
                    first.destination().backend_process_id(),
                );
                match client
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
                }
            }
            .await;
            done(outcome);
            drop(activity);
        });
    }

    fn watch_unit_facts(
        &self,
        destination: novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        stream: novarocks_execution_contract::ExchangeStreamIdentity,
        wake: novarocks_execution::runtime::task::io::exchange_unit::ExchangeGrantWake,
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
    use novarocks_execution::runtime::task::io::exchange_unit::{
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
            novarocks_worker::TaskInboundCapabilities::new(),
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
    fn stalled_control_cancellation_keeps_activity_until_callback_and_capture_exit() {
        use novarocks_execution::runtime::task::io::exchange_unit::{
            ExchangeDemandCallOutcome, ExchangeDemandEntry, ExchangeDemandOperation,
        };
        use std::io::{Read, Write};
        use std::sync::mpsc;
        use std::time::Instant;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let frame = unit_frame();
        let stream = frame.receiver.stream;
        let runtime = crate::backend_test_support::test_backend_data_runtime();
        runtime
            .native_trust()
            .bind_process_identity(novarocks_native_trust::NativeProcessIdentity::Backend(
                stream.source().task().backend_process_id(),
            ))
            .unwrap();
        let transmitter = grpc_exchange_transmitter(
            runtime,
            Duration::from_secs(3),
            novarocks_worker::TaskInboundCapabilities::new(),
            #[cfg(novarocks_dev_controls)]
            None,
        );
        let (outcome_tx, outcome_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let (exit_tx, exit_rx) = mpsc::sync_channel(1);
        let stop = CancellationToken::new();
        transmitter.register_unit_demands(
            novarocks_execution::runtime::endpoint::RuntimeEndpoint::from_socket_addr(
                listener.local_addr().unwrap(),
            ),
            vec![ExchangeDemandEntry {
                stream,
                operation: ExchangeDemandOperation::StopSender,
            }],
            stop.clone(),
            ExchangeCallActivity::new(move || {
                exit_tx.send(()).unwrap();
            }),
            Box::new(move |outcome| {
                outcome_tx.send(outcome).unwrap();
                // Keep this actual callback/capture alive after reporting its
                // outcome. The activity must still belong to this same call.
                release_rx.recv().unwrap();
            }),
        );
        let accept_deadline = Instant::now() + Duration::from_secs(1);
        let mut accepted = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < accept_deadline, "control did not connect");
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("control blackhole accept failed: {error}"),
            }
        };
        accepted
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut preface = [0; 24];
        accepted.read_exact(&mut preface).unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        accepted.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).unwrap();
        // Use the existing Native client's H2 request barrier: a connected
        // socket alone would still permit pre-dispatch cancellation.
        loop {
            let mut header = [0; 9];
            accepted.read_exact(&mut header).unwrap();
            let length = u32::from_be_bytes([0, header[0], header[1], header[2]]);
            let mut payload = vec![0; length as usize];
            accepted.read_exact(&mut payload).unwrap();
            let stream_id = u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff;
            if header[3] == 1 && stream_id != 0 {
                break;
            }
        }
        stop.cancel();
        let outcome = outcome_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            matches!(outcome, ExchangeDemandCallOutcome::Unknown(_)),
            "{outcome:?}"
        );
        assert!(matches!(exit_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        release_tx.send(()).unwrap();
        exit_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(accepted);
    }

    #[test]
    fn only_the_local_destination_abort_fence_retires_stop_without_dialing() {
        use novarocks_execution::runtime::task::io::exchange_unit::{
            ExchangeDemandCallOutcome, ExchangeDemandEntry, ExchangeDemandOperation,
        };
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut frame = unit_frame();
        frame.destination =
            novarocks_execution::runtime::endpoint::RuntimeEndpoint::from_socket_addr(
                listener.local_addr().unwrap(),
            );
        let stream = frame.receiver.stream;
        let runtime = crate::backend_test_support::test_backend_data_runtime();
        runtime
            .native_trust()
            .bind_process_identity(novarocks_native_trust::NativeProcessIdentity::Backend(
                stream.source().task().backend_process_id(),
            ))
            .unwrap();
        let capabilities = novarocks_worker::TaskInboundCapabilities::new();
        let transmitter = grpc_exchange_transmitter(
            runtime,
            Duration::from_secs(1),
            capabilities.clone(),
            #[cfg(novarocks_dev_controls)]
            None,
        );
        let call = |stream, operation| {
            let stop = CancellationToken::new();
            stop.cancel();
            let (tx, rx) = std::sync::mpsc::channel();
            transmitter.register_unit_demands(
                frame.destination.clone(),
                vec![ExchangeDemandEntry { stream, operation }],
                stop,
                ExchangeCallActivity::new(|| {}),
                Box::new(move |outcome| tx.send(outcome).unwrap()),
            );
            rx.recv_timeout(Duration::from_secs(2)).unwrap()
        };
        let participation = novarocks_execution_contract::ParticipationRef::new(
            stream.destination().query_execution_id(),
            novarocks_types::identity::FrontendProcessId::new_v7(),
            stream.destination().backend_process_id(),
        );
        // Missing metadata and normal quiesce provide no retirement proof.
        assert!(matches!(
            call(stream, ExchangeDemandOperation::StopSender),
            ExchangeDemandCallOutcome::NotProcessed(_)
        ));
        capabilities.close_participation(participation);
        assert!(matches!(
            call(stream, ExchangeDemandOperation::StopSender),
            ExchangeDemandCallOutcome::NotProcessed(_)
        ));
        let other_execution = QueryExecutionId::new(
            participation.query_execution_id().query_id(),
            AttemptId::new(2).unwrap(),
        )
        .unwrap();
        capabilities.abort_participation(novarocks_execution_contract::ParticipationRef::new(
            other_execution,
            participation.frontend_process_id(),
            participation.backend_process_id(),
        ));
        assert!(matches!(
            call(stream, ExchangeDemandOperation::StopSender),
            ExchangeDemandCallOutcome::NotProcessed(_)
        ));
        // The original positive abort fences all routes and late installation
        // even after their receiver descriptors have actually been removed.
        capabilities.abort_participation(participation);
        assert!(
            matches!(call(stream, ExchangeDemandOperation::StopSender), ExchangeDemandCallOutcome::RouteRetired(actual) if actual == stream)
        );
        let remote = TaskIdentity::new(
            stream.destination().query_execution_id(),
            stream.destination().stage_id(),
            stream.destination().task_id(),
            BackendProcessId::new_v7(),
        );
        let remote_stream = ExchangeStreamIdentity::try_new(
            stream.source(),
            remote,
            stream.destination_task_key(),
            stream.destination_node(),
        )
        .unwrap();
        assert!(matches!(
            call(remote_stream, ExchangeDemandOperation::StopSender),
            ExchangeDemandCallOutcome::NotProcessed(_)
        ));
        let demand = novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitDemand {
            sequence: novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitSequence::try_new(1).unwrap(), wire_bytes: 1,
        };
        assert!(matches!(
            call(stream, ExchangeDemandOperation::Register(demand)),
            ExchangeDemandCallOutcome::NotProcessed(_)
        ));
        capabilities.forget_participation(participation);
        assert!(matches!(
            call(stream, ExchangeDemandOperation::StopSender),
            ExchangeDemandCallOutcome::NotProcessed(_)
        ));
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
                        .source_task_key_lo += 1
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
