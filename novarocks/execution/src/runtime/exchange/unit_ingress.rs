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

//! The original receiver queue composed with its structural slot owner.
//! Admission, decode commit, transfer and cleanup all take instance then queue.
//! Out-of-order chunks stay in their already reserved unit until its prefix is
//! admitted; boundaries describe that same queue and never store another copy.

use super::*;
use crate::runtime::exchange_slots::{
    ExchangeSlotAdmission, ExchangeSlotInstance, ExchangeSlotRegistration,
};
use crate::runtime::fragment::io::exchange_receiver::ExchangeReceiverKey;
use crate::runtime::fragment::io::exchange_unit::{
    ExchangeDemandEntry, ExchangeDemandOperation, ExchangeStreamObservation, ExchangeUnitReceipt,
    ExchangeUnitReceiverFrame, ExchangeUnitStateListener,
};
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_execution_contract::ordered_retained_stream::exchange::{
    ExchangeDecodeCommit, ExchangeStreamFacts, ExchangeUnitItem, ExchangeUnitSequence,
};
use std::collections::BTreeMap;

#[derive(Clone)]
pub(super) struct UnitReceiverOwner {
    pub(super) slots: ExchangeSlotInstance,
    control: Arc<RuntimeErrorState>,
    streams: Vec<ExchangeStreamIdentity>,
}

#[cfg(test)]
type EndProjectionPause = Box<dyn FnOnce(&Receiver, ExchangeUnitReceipt) + Send>;

#[derive(Default)]
pub(super) struct UnitQueue {
    pending: BTreeMap<(ExchangeStreamIdentity, u64), Vec<Chunk>>,
    enqueued: BTreeMap<ExchangeStreamIdentity, u64>,
    ends: BTreeMap<ExchangeStreamIdentity, ExchangeUnitSequence>,
    boundaries: VecDeque<UnitBoundary>,
    #[cfg(test)]
    before_end_projection: Option<EndProjectionPause>,
}

impl UnitQueue {
    pub(super) fn has_pending_transfers(&self) -> bool {
        !self.boundaries.is_empty()
    }
}

struct UnitBoundary {
    stream: ExchangeStreamIdentity,
    sequence: ExchangeUnitSequence,
    remaining: usize,
}

fn receiver_key(key: ExchangeKey) -> ExchangeReceiverKey {
    ExchangeReceiverKey {
        fragment_instance_id: UniqueId::new(key.finst_id_hi, key.finst_id_lo),
        node_id: key.node_id,
    }
}

impl ReceiverState {
    fn expose_unit_prefix(&mut self, stream: ExchangeStreamIdentity, facts: ExchangeStreamFacts) {
        let mut through = self.units.enqueued.get(&stream).copied().unwrap_or(0);
        while let Some(next) = through.checked_add(1) {
            if next > facts.custody_through {
                break;
            }
            let Some(chunks) = self.units.pending.remove(&(stream, next)) else {
                break;
            };
            self.units.boundaries.push_back(UnitBoundary {
                stream,
                sequence: ExchangeUnitSequence::try_new(next).expect("an admitted unit sequence"),
                remaining: chunks.len(),
            });
            self.chunks
                .extend(chunks.into_iter().map(|chunk| SourcedExchangeChunk {
                    source: Some(stream.source()),
                    chunk,
                }));
            through = next;
        }
        self.units.enqueued.insert(stream, through);
        if self
            .units
            .ends
            .get(&stream)
            .is_some_and(|end| end.get() <= facts.custody_through)
        {
            self.finished.insert(ExchangeSenderIdentity::native(
                stream.source().fragment_instance_id(),
                stream.source().sender_ordinal(),
            ));
        }
    }
}

impl UnitReceiverOwner {
    pub(super) fn report_failure(&self, error: ExchangeFailure) {
        self.control.set_error(error.into_execution());
    }
}

impl Receiver {
    #[cfg(novarocks_dev_controls)]
    fn observe_unit_decode(&self, event: &str, sequence: ExchangeUnitSequence) {
        // Observers may synchronize a bounded test. Never invoke one while
        // holding the queue or slot lock needed by an independent replay.
        let observer = self
            .mu
            .lock()
            .expect("exchange receiver lock")
            .test_observer
            .clone();
        emit_exchange_snapshot_marker(observer.as_deref(), || {
            format!("event={event} sequence={}", sequence.get())
        });
    }

    pub(super) fn unit_owner(&self) -> Option<UnitReceiverOwner> {
        self.unit_owner
            .lock()
            .expect("exchange unit owner lock")
            .clone()
    }
    pub(super) fn retire_units(&self) -> Result<(), ExchangeFailure> {
        let owner = self.unit_owner();
        let Some(owner) = owner else {
            return Ok(());
        };
        let closes = owner
            .streams
            .iter()
            .map(|stream| {
                let snapshot = owner
                    .slots
                    .stream_snapshot(*stream)
                    .map_err(ExchangeFailure::from)?;
                let close = match snapshot.observed_end {
                    Some(end) if snapshot.handed_off_through == end.get() => {
                        ExchangeCloseDisposition::Consumed {
                            final_sequence: end,
                        }
                    }
                    _ => ExchangeCloseDisposition::Withdrawn,
                };
                Ok((*stream, close))
            })
            .collect::<Result<Vec<_>, ExchangeFailure>>()?;
        owner
            .slots
            .retire_with(&closes, || {
                let mut st = self.mu.lock().expect("exchange receiver lock");
                st.canceled = true;
                st.chunks.clear();
                st.units.pending.clear();
                st.units.boundaries.clear();
            })
            .map_err(ExchangeFailure::from)?;
        self.observable.defer_notify().arm();
        Ok(())
    }
}

impl ExecutionExchangeRegistry {
    /// The host supplies the exact frozen roster before Installed allows any
    /// production request. Count-only registration never grants admission.
    pub fn bind_unit_receiver(
        &self,
        key: ExchangeKey,
        streams: Vec<ExchangeStreamIdentity>,
        control: Arc<RuntimeErrorState>,
        grantable_unit_bytes: u64,
    ) -> Result<(), ExchangeFailure> {
        let receiver = self
            .receivers
            .lock()
            .expect("exchange lock")
            .get(&key)
            .cloned()
            .ok_or_else(|| {
                exchange_contract("exchange unit binding requires its registered queue")
            })?;
        let task = streams
            .first()
            .ok_or_else(|| exchange_contract("exchange unit roster is empty"))?
            .destination();
        if control
            .task_identity()
            .is_some_and(|identity| identity != task)
        {
            return Err(exchange_contract(
                "exchange decode control names another Task",
            ));
        }
        let mut binding = receiver
            .unit_owner
            .lock()
            .expect("exchange unit owner lock");
        if binding.is_some() {
            return Err(exchange_contract(
                "exchange unit owner is already installed",
            ));
        }
        {
            let st = receiver.mu.lock().expect("exchange receiver lock");
            if st.canceled
                || st.expected.is_none()
                || st.expected_senders != streams.len()
                || !st.chunks.is_empty()
            {
                return Err(exchange_contract(
                    "exchange unit roster differs from its registered queue",
                ));
            }
        }
        let slots = self
            .unit_slots
            .register(ExchangeSlotRegistration {
                task,
                fragment_instance_id: UniqueId::new(key.finst_id_hi, key.finst_id_lo),
                node: novarocks_execution_contract::FragmentNodeId::new(key.node_id),
                streams: streams.clone(),
                grantable_unit_bytes,
            })
            .map_err(ExchangeFailure::from)?;
        *binding = Some(UnitReceiverOwner {
            slots,
            control,
            streams,
        });
        Ok(())
    }

    pub fn set_unit_state_listener(
        &self,
        listener: Arc<dyn ExchangeUnitStateListener>,
    ) -> Result<(), ExchangeFailure> {
        self.unit_slots
            .set_listener(listener)
            .map_err(ExchangeFailure::from)
    }

    pub fn unit_sender_states(
        &self,
        sender: novarocks_types::BackendProcessId,
        after: Option<ExchangeStreamIdentity>,
        limit: usize,
    ) -> Vec<ExchangeStreamObservation> {
        self.unit_slots.sender_states(sender, after, limit)
    }

    pub fn register_unit_demand(
        &self,
        key: ExchangeKey,
        entry: ExchangeDemandEntry,
    ) -> Result<ExchangeStreamObservation, ExchangeFailure> {
        let slots = self
            .unit_slots
            .get(receiver_key(key))
            .ok_or_else(|| exchange_contract("exchange unit receiver is not installed"))?;
        let facts = match entry.operation {
            ExchangeDemandOperation::Register(demand) => {
                slots.register_demand(entry.stream, demand)
            }
            ExchangeDemandOperation::StopSender => slots.stop_sender(entry.stream),
        }
        .map_err(ExchangeFailure::from)?;
        Ok(ExchangeStreamObservation {
            stream: entry.stream,
            facts,
        })
    }

    pub fn receive_unit(
        &self,
        key: ExchangeKey,
        frame: ExchangeUnitReceiverFrame,
    ) -> Result<ExchangeUnitReceipt, ExchangeFailure> {
        self.receive_unit_with(key, frame, |sender, bytes| {
            self.decode_chunks_for_sender(key, sender, bytes)
        })
    }

    fn receive_unit_with(
        &self,
        key: ExchangeKey,
        frame: ExchangeUnitReceiverFrame,
        decode: impl FnOnce(ExchangeSenderIdentity, &[u8]) -> Result<Vec<Chunk>, ExchangeFailure>,
    ) -> Result<ExchangeUnitReceipt, ExchangeFailure> {
        let stream = frame.stream;
        if stream.destination_fragment_instance_id()
            != UniqueId::new(key.finst_id_hi, key.finst_id_lo)
            || stream.destination_node().get() != key.node_id
            || frame.sender_count == 0
            || stream.source().sender_ordinal() >= frame.sender_count
        {
            return Err(exchange_contract(
                "exchange unit differs from its exact frozen stream",
            ));
        }
        let receiver = self
            .receivers
            .lock()
            .expect("exchange lock")
            .get(&key)
            .cloned()
            .ok_or_else(|| exchange_contract("exchange receiver is not registered"))?;
        let owner = receiver
            .unit_owner()
            .ok_or_else(|| exchange_contract("exchange unit receiver is not installed"))?;
        if owner.streams.len() != frame.sender_count as usize {
            return Err(exchange_contract(
                "exchange sender count differs from its frozen receiver",
            ));
        }
        match frame.item {
            ExchangeUnitItem::Unit { demand, .. }
                if demand.wire_bytes != frame.payload.len() as u64 =>
            {
                return Err(exchange_contract(
                    "exchange unit length differs from its registered demand",
                ));
            }
            ExchangeUnitItem::End(_) if !frame.payload.is_empty() => {
                return Err(exchange_contract("exchange End cannot carry a payload"));
            }
            _ => {}
        }
        let admission = owner
            .slots
            .begin_admission(stream, frame.item)
            .map_err(ExchangeFailure::from)?;
        #[cfg(test)]
        if matches!(frame.item, ExchangeUnitItem::End(_))
            && let ExchangeSlotAdmission::Observed(receipt) = &admission
        {
            let pause = receiver
                .mu
                .lock()
                .expect("exchange receiver lock")
                .units
                .before_end_projection
                .take();
            if let Some(pause) = pause {
                // The receiver-local test rendezvous owns no slot/queue lock.
                pause(&receiver, *receipt);
            }
        }
        let observed_end = owner
            .slots
            .stream_snapshot(stream)
            .map_err(ExchangeFailure::from)?
            .observed_end;
        if let Some(end) = observed_end {
            receiver
                .mu
                .lock()
                .expect("exchange receiver lock")
                .units
                .ends
                .insert(stream, end);
        }
        match admission {
            ExchangeSlotAdmission::Observed(receipt) => {
                // End is already visible in the queue before this fresh read.
                // A decode finishing later sees that End; one finishing earlier
                // is covered here. Admission facts can predate both transitions.
                let facts = owner.slots.facts(stream).map_err(ExchangeFailure::from)?;
                let mut state = receiver.mu.lock().expect("exchange receiver lock");
                if matches!(frame.item, ExchangeUnitItem::End(_))
                    && receipt.verdict
                        == crate::runtime::fragment::io::exchange_unit::ExchangeAdmissionVerdict::Applied
                {
                    state.recv_request_count = state.recv_request_count.saturating_add(1);
                }
                state.expose_unit_prefix(stream, facts);
                drop(state);
                receiver.observable.defer_notify().arm();
                Ok(receipt)
            }
            ExchangeSlotAdmission::Decode(lease) => {
                let sequence = lease.sequence();
                #[cfg(novarocks_dev_controls)]
                receiver.observe_unit_decode("unit_decode_begin", sequence);
                let sender = ExchangeSenderIdentity::native(
                    stream.source().fragment_instance_id(),
                    stream.source().sender_ordinal(),
                );
                let start = Instant::now();
                let mut chunks = match decode(sender, &frame.payload) {
                    Ok(chunks) => chunks,
                    Err(error) => {
                        // Publish to the original Task owner even when the RPC
                        // reply has already been lost. Lease release is only
                        // physical cleanup and never substitutes for this cause.
                        let error = error.into_execution();
                        owner.control.set_error(error.clone());
                        lease.abandon();
                        #[cfg(novarocks_dev_controls)]
                        receiver.observe_unit_decode("unit_decode_exited", sequence);
                        receiver.observable.defer_notify().arm();
                        return Err(ExchangeFailure::Execution(error));
                    }
                };
                let receipt = lease
                    .finish_with(|result| {
                        let mut st = receiver.mu.lock().expect("exchange receiver lock");
                        if result.disposition != ExchangeDecodeCommit::Discarded {
                            if let Some(tracker) = st.mem_tracker.as_ref() {
                                for chunk in &mut chunks {
                                    chunk.transfer_to(tracker);
                                }
                            }
                            st.recv_request_count = st.recv_request_count.saturating_add(1);
                            st.recv_payload_bytes = st
                                .recv_payload_bytes
                                .saturating_add(frame.payload.len() as u128);
                            st.recv_deserialize_ns = st
                                .recv_deserialize_ns
                                .saturating_add(start.elapsed().as_nanos());
                            st.recv_chunks = st.recv_chunks.saturating_add(chunks.len() as u128);
                            st.recv_rows = st.recv_rows.saturating_add(
                                chunks.iter().map(|chunk| chunk.len() as u128).sum::<u128>(),
                            );
                            st.units.pending.insert((stream, sequence.get()), chunks);
                            st.expose_unit_prefix(stream, result.receipt.facts);
                        }
                        result.receipt
                    })
                    .map_err(ExchangeFailure::from)?;
                #[cfg(novarocks_dev_controls)]
                receiver.observe_unit_decode("unit_decode_exited", sequence);
                receiver.observable.defer_notify().arm();
                Ok(receipt)
            }
        }
    }
}

impl ExchangeReceiverHandle {
    pub(super) fn try_pop_unit(
        &self,
        owner: &UnitReceiverOwner,
        expected_senders: usize,
    ) -> Result<Option<SourcedExchangePopResult>, ExchangeFailure> {
        loop {
            let item = owner
                .slots
                .consume_with(
                    || {
                        let state = self.receiver.mu.lock().expect("exchange receiver lock");
                        state
                            .units
                            .boundaries
                            .front()
                            .filter(|unit| unit.remaining <= 1)
                            .map(|unit| (unit.stream, unit.sequence))
                    },
                    || {
                        let mut st = self.receiver.mu.lock().expect("exchange receiver lock");
                        if st.canceled {
                            return Err(exchange_stopped());
                        }
                        if let Some(boundary) = st.units.boundaries.front_mut() {
                            if boundary.remaining == 0 {
                                st.units.boundaries.pop_front();
                                return Ok(None);
                            }
                            boundary.remaining -= 1;
                            if boundary.remaining == 0 {
                                st.units.boundaries.pop_front();
                            }
                            let chunk = st
                                .chunks
                                .pop_front()
                                .expect("unit boundary owns its decoded queue prefix");
                            return Ok(Some(Some(SourcedExchangePopResult::Chunk(chunk))));
                        }
                        let expected = st.expected_senders.max(expected_senders);
                        let item = (st.finished.len() >= expected).then(|| {
                            SourcedExchangePopResult::Finished(ExchangeRecvStats {
                                request_received: st.recv_request_count,
                                bytes_received: st.recv_payload_bytes,
                                deserialize_ns: st.recv_deserialize_ns,
                                chunks_received: st.recv_chunks,
                                rows_received: st.recv_rows,
                            })
                        });
                        Ok(Some(item))
                    },
                )
                .map_err(ExchangeFailure::from)??;
            if let Some(item) = item {
                return Ok(item);
            }
        }
    }

    pub(super) fn complete_unit_input(
        &self,
        owner: &UnitReceiverOwner,
    ) -> Result<(), ExchangeFailure> {
        let completed = owner
            .slots
            .complete_all_input_consumption_with(|| {
                let st = self.receiver.mu.lock().expect("exchange receiver lock");
                !st.canceled
                    && st.chunks.is_empty()
                    && st.units.boundaries.is_empty()
                    && st.units.pending.is_empty()
            })
            .map_err(ExchangeFailure::from)?;
        if !completed {
            return Err(exchange_contract(
                "exchange input has not consumed its exact End prefix",
            ));
        }
        self.receiver.observable.defer_notify().arm();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::execution_runtime::ExchangeFlowLimits;
    use crate::runtime::task_control::test_support::Owner;
    use arrow::array::Int32Array;
    use arrow::datatypes::Field;
    use novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitDemand;
    use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase, TaskIdentity};
    use novarocks_types::{StageId, TaskId};
    use std::sync::Barrier;

    struct Fixture {
        registry: Arc<ExecutionExchangeRegistry>,
        key: ExchangeKey,
        stream: ExchangeStreamIdentity,
        schema: ChunkSchemaRef,
        task: Arc<Owner>,
    }

    impl Fixture {
        fn new(slots: usize) -> Self {
            let limits =
                ExchangeFlowLimits::try_new(slots, slots, 0, 2, Duration::from_secs(30)).unwrap();
            Self::with_registry(Arc::new(ExecutionExchangeRegistry::with_flow_limits(
                limits,
            )))
        }

        fn with_registry(registry: Arc<ExecutionExchangeRegistry>) -> Self {
            let task = Owner::new();
            let destination = task.identity();
            let key = ExchangeKey {
                finst_id_hi: destination.query_execution_id().query_id().high(),
                finst_id_lo: 991,
                node_id: 7,
            };
            let source = TaskIdentity::new(
                destination.query_execution_id(),
                StageId::new(2).unwrap(),
                TaskId::new(2).unwrap(),
                destination.backend_process_id(),
            );
            let stream = ExchangeStreamIdentity::try_new(
                ExchangeSource::new(source, UniqueId::new(992, 1), 0),
                destination,
                UniqueId::new(key.finst_id_hi, key.finst_id_lo),
                novarocks_execution_contract::FragmentNodeId::new(7),
            )
            .unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
            let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                &schema,
                &[SlotId::new(1)],
            )
            .unwrap();
            let schema = super::super::tests::plain_declaration(&schema);
            registry
                .try_register_expected_chunk_schema_with_binding(
                    key,
                    1,
                    (schema.clone()).clone(),
                    schema.clone(),
                    ExchangeColumnBinding::Positional,
                    crate::runtime::fragment::exchange::test_receiver_creation_factory(),
                )
                .unwrap();
            registry
                .bind_unit_receiver(
                    key,
                    vec![stream],
                    Arc::new(RuntimeErrorState::for_task(Arc::new(
                        task.view(TaskFailurePhase::Execution),
                    ))),
                    64 * 1024 * 1024,
                )
                .unwrap();
            Self {
                registry,
                key,
                stream,
                schema,
                task,
            }
        }

        fn frame(&self, n: u64, values: &[i32], end: bool) -> ExchangeUnitReceiverFrame {
            let chunk = Chunk::try_new_with_columns(
                self.schema.clone(),
                vec![Arc::new(Int32Array::from(values.to_vec()))],
            )
            .unwrap();
            let payload = Bytes::from(super::super::tests::online_unit(&[chunk], true).unwrap());
            self.unit_frame(n, payload, end)
        }

        fn unit_frame(&self, n: u64, payload: Bytes, end: bool) -> ExchangeUnitReceiverFrame {
            ExchangeUnitReceiverFrame {
                stream: self.stream,
                sender_count: 1,
                sender_id: 0,
                backend_number: 0,
                item: ExchangeUnitItem::Unit {
                    demand: ExchangeUnitDemand {
                        sequence: ExchangeUnitSequence::try_new(n).unwrap(),
                        wire_bytes: payload.len() as u64,
                    },
                    end_after_unit: end,
                },
                payload,
                next_demand: None,
            }
        }

        fn register(&self, frame: &ExchangeUnitReceiverFrame) -> ExchangeStreamObservation {
            let ExchangeUnitItem::Unit { demand, .. } = frame.item else {
                panic!("data unit");
            };
            self.registry
                .register_unit_demand(
                    self.key,
                    ExchangeDemandEntry {
                        stream: self.stream,
                        operation: ExchangeDemandOperation::Register(demand),
                    },
                )
                .unwrap()
        }

        fn receiver(&self) -> ExchangeReceiverHandle {
            self.registry.get_receiver_handle(self.key, 1).unwrap()
        }

        fn values(&self, receiver: &ExchangeReceiverHandle) -> Vec<i32> {
            let Some(ExchangePopResult::Chunk(chunk)) =
                receiver.try_pop_next_with_stats(1).unwrap()
            else {
                panic!("data chunk");
            };
            chunk
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        }
    }

    #[test]
    fn end_admitted_during_decode_finishes_real_source_without_a_future_producer_wake() {
        use crate::exec::operators::ExchangeSourceFactory;
        use crate::exec::pipeline::{
            binding::ExchangeBinding,
            operator_factory::OperatorFactory,
            step::NextStep,
            step_context::{DEFAULT_TURN_WORK_UNITS, StepContext, TurnWork},
        };
        use crate::runtime::execution_runtime::{ExecutionRuntime, test_execution_runtime};
        use crate::runtime::fragment::io::ExecutionRuntimeExchangeReceiverPort;
        use crate::runtime::runtime_state::RuntimeState;

        let base = test_execution_runtime();
        let runtime = Arc::new(
            ExecutionRuntime::new_with_exchange_flow_limits(
                base.config().clone(),
                base.function_set().clone(),
                base.memory_authority().clone(),
                ExchangeFlowLimits::try_new(1, 1, 0, 2, Duration::from_secs(30)).unwrap(),
            )
            .unwrap(),
        );
        let f = Fixture::with_registry(runtime.exchange_registry());
        let control = Arc::new(RuntimeErrorState::for_task(Arc::new(
            f.task.view(TaskFailurePhase::Execution),
        )));
        let state = RuntimeState::new(
            None,
            None,
            Some(f.task.identity().query_execution_id().query_id()),
            Some(UniqueId::new(f.key.finst_id_hi, f.key.finst_id_lo)),
            Some(0),
            Some(MemTracker::new_root("late End real Source")),
            Some(runtime.clone()),
            control,
        );
        let factory = ExchangeSourceFactory::new_compiled(
            f.key.node_id,
            ExchangeBinding {
                key: f.key,
                expected_senders: 1,
                receiver_port: Arc::new(ExecutionRuntimeExchangeReceiverPort::new(runtime)),
            },
        );
        let mut source = factory.create(1, 0);
        source.bind_runtime_state(&state).unwrap();
        source.prepare().unwrap();
        let mut drive = || {
            let errors = state.error_state();
            let mut work = TurnWork::new(std::num::NonZeroU64::MIN, DEFAULT_TURN_WORK_UNITS);
            let mut values = novarocks_value_arrays::decode::ValueConversionContext::default();
            source
                .drive(&mut StepContext::new(&errors, &mut work, &mut values))
                .unwrap()
        };
        let NextStep::Wait(before) = drive().next else {
            panic!("the original empty Source must really wait");
        };
        assert!(!before.is_ready());

        let data = f.frame(1, &[41, 42], false);
        f.register(&data);
        let mut end = data.clone();
        end.item = ExchangeUnitItem::End(ExchangeUnitSequence::try_new(2).unwrap());
        end.payload = Bytes::new();
        let (entered, decoding) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let registry = f.registry.clone();
        let key = f.key;
        let decoder = std::thread::spawn(move || {
            registry.receive_unit_with(key, data, |sender, payload| {
                entered.send(()).unwrap();
                released.recv().unwrap();
                registry.decode_chunks_for_sender(key, sender, payload)
            })
        });
        decoding.recv().unwrap();
        let receiver = f
            .registry
            .receivers
            .lock()
            .unwrap()
            .get(&f.key)
            .unwrap()
            .clone();
        let stream = f.stream;
        receiver.mu.lock().unwrap().units.before_end_projection =
            Some(Box::new(move |receiver, admission| {
                let released = release.send(());
                let decoded = decoder.join();
                released.unwrap();
                let decoded = decoded.unwrap().unwrap();
                assert_eq!(admission.facts.custody_through, 0);
                assert_eq!(
                    admission.verdict,
                    crate::runtime::fragment::io::exchange_unit::ExchangeAdmissionVerdict::Applied
                );
                assert_eq!(decoded.facts.custody_through, 2);
                let snapshot = receiver
                    .unit_owner()
                    .unwrap()
                    .slots
                    .stream_snapshot(stream)
                    .unwrap();
                assert_eq!(
                    snapshot.observed_end,
                    Some(ExchangeUnitSequence::try_new(2).unwrap())
                );
                assert_eq!(snapshot.facts.custody_through, 2);
                assert_eq!(snapshot.decoding_positions, 0);
                let queue = receiver.mu.lock().unwrap();
                assert!(queue.units.ends.is_empty());
                assert!(queue.finished.is_empty());
                assert_eq!(queue.chunks.len(), 1);
            }));
        let admitted = f.registry.receive_unit(f.key, end).unwrap();
        assert_eq!(
            admitted.facts.custody_through, 0,
            "preserve the actual admission ACK facts"
        );
        assert!(
            before.is_ready(),
            "the original Source receives real ingress notifications"
        );
        assert_eq!(
            f.registry.stream_close_facts(f.key, &[stream]).unwrap()[0].disposition,
            ExchangeCloseDisposition::Withdrawn,
            "custody ACK does not author Consumed"
        );

        // Both ingress calls and the original decoder have actually returned.
        // No producer, test wake or forced kernel poll can rescue a stale End.
        let data = drive();
        assert!(matches!(data.next, NextStep::Runnable));
        let chunk = data.output.expect("the original source hands off Data1");
        let values = chunk
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[41, 42]);
        drop(chunk);
        let end = drive();
        assert!(end.output.is_none());
        assert!(
            matches!(end.next, NextStep::Finished),
            "an already admitted exact End must finish, not freeze an unready Wait: {:?}",
            end.next
        );
        assert_eq!(
            f.registry.stream_close_facts(f.key, &[stream]).unwrap()[0].disposition,
            ExchangeCloseDisposition::Consumed {
                final_sequence: ExchangeUnitSequence::try_new(2).unwrap()
            }
        );
        assert!(state.error().is_none());
        assert!(f.task.accepted().is_none());
        let slots = f.registry.unit_slots.get(receiver_key(f.key)).unwrap();
        assert_eq!(slots.snapshot().in_use, 0);
        assert_eq!(slots.snapshot().decoding, 0);
        f.registry.remove_exchange_key(f.key);
        assert_eq!(f.registry.unit_slots.snapshot().registered_instances, 0);
    }

    #[test]
    fn receiver_queue_reorders_units_and_replays_without_duplicating_rows_or_consumption() {
        let f = Fixture::new(2);
        let first = f.frame(1, &[11, 12], false);
        let second = f.frame(2, &[21], true);
        f.register(&first);
        f.register(&second);
        assert_eq!(
            f.registry
                .receive_unit(f.key, second.clone())
                .unwrap()
                .facts
                .custody_through,
            0
        );
        assert!(!f.receiver().has_output_or_finished(1));
        assert_eq!(
            f.registry
                .receive_unit(f.key, first.clone())
                .unwrap()
                .facts
                .custody_through,
            3
        );
        let replay = f.registry.receive_unit(f.key, second).unwrap();
        assert_eq!(
            replay.verdict,
            crate::runtime::fragment::io::exchange_unit::ExchangeAdmissionVerdict::Idempotent
        );
        assert_eq!(replay.facts.custody_through, 3);
        let receiver = f.receiver();
        assert_eq!(f.values(&receiver), [11, 12]);
        assert_eq!(f.values(&receiver), [21]);
        assert!(matches!(
            receiver.try_pop_next_with_stats(1).unwrap(),
            Some(ExchangePopResult::Finished(_))
        ));
        assert_eq!(
            f.registry.stream_close_facts(f.key, &[f.stream]).unwrap()[0].disposition,
            ExchangeCloseDisposition::Withdrawn
        );
        receiver.complete_input_consumption(1).unwrap();
        assert_eq!(
            f.registry.stream_close_facts(f.key, &[f.stream]).unwrap()[0].disposition,
            ExchangeCloseDisposition::Consumed {
                final_sequence: ExchangeUnitSequence::try_new(3).unwrap()
            }
        );
        f.registry.remove_exchange_key(f.key);
        assert_eq!(f.registry.unit_slots.snapshot().registered_instances, 0);
    }

    #[test]
    fn native_unit_limit_rejection_keeps_the_receipts_resource_failure_identity() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let fixture = Fixture::new(1);
        let error = fixture.registry.register_unit_demand(fixture.key, ExchangeDemandEntry {
            stream: fixture.stream,
            operation: ExchangeDemandOperation::Register(ExchangeUnitDemand {
                sequence: ExchangeUnitSequence::FIRST,
                wire_bytes: novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64 + 1,
            }),
        }).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact, novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeReceiptError::UnitTooLarge {
                requested: novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64 + 1,
                limit: novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64,
            }.failure_fact());
        fixture.registry.cancel_exchange_key(fixture.key);
    }

    #[test]
    fn receiver_empty_unit_is_ready_for_real_handoff_and_unblocks_the_next_demand() {
        let f = Fixture::new(1);
        // Even a zero-row Unit carries exactly one complete RecordBatch.
        let empty = f.frame(1, &[], false);
        f.register(&empty);
        f.registry.receive_unit(f.key, empty).unwrap();
        let next = f.frame(2, &[7], true);
        assert_eq!(f.register(&next).facts.granted_through, 1);
        let receiver = f.receiver();
        assert!(receiver.has_output_or_finished(1));
        assert!(
            matches!(receiver.try_pop_next_with_stats(1).unwrap(), Some(ExchangePopResult::Chunk(chunk)) if chunk.is_empty())
        );
        assert_eq!(
            f.registry
                .unit_slots
                .get(receiver_key(f.key))
                .unwrap()
                .facts(f.stream)
                .unwrap()
                .granted_through,
            2
        );
        f.registry.receive_unit(f.key, next).unwrap();
        assert_eq!(f.values(&receiver), [7]);
        receiver.complete_input_consumption(1).unwrap();
        f.registry.remove_exchange_key(f.key);
    }

    #[test]
    fn original_decode_failure_reaches_task_owner_after_unknown_reply_and_decoding_replay() {
        let f = Fixture::new(1);
        let frame = f.frame(1, &[4], false);
        f.register(&frame);
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let registry = f.registry.clone();
        let key = f.key;
        let first = frame.clone();
        let child_entered = entered.clone();
        let child_release = release.clone();
        let work = std::thread::spawn(move || {
            registry.receive_unit_with(key, first, |_, _| {
                child_entered.wait();
                child_release.wait();
                Err(exchange_payload(
                    "original decoder rejected its Arrow input",
                ))
            })
        });
        entered.wait();
        // The original transport may already be unknown. The duplicate must
        // observe Decoding immediately, never run a second decoder or wait.
        let duplicate = f
            .registry
            .receive_unit_with(f.key, frame, |_, _| panic!("duplicate decoder"))
            .unwrap();
        assert_eq!(duplicate.facts.custody_through, 0);
        assert!(f.task.accepted().is_none());
        release.wait();
        let error = work.join().unwrap().unwrap_err().into_execution();
        let accepted = f
            .task
            .accepted()
            .expect("originating failure is independent of the lost reply");
        assert!(
            accepted
                .originating_failure()
                .unwrap()
                .same_occurrence(&error)
        );
        assert_eq!(
            f.registry
                .unit_slots
                .get(receiver_key(f.key))
                .unwrap()
                .snapshot()
                .decoding,
            0
        );
        f.registry.cancel_exchange_key(f.key);
    }

    #[test]
    fn receiver_retirement_keeps_decoding_responsibility_until_actual_work_exit() {
        let f = Fixture::new(1);
        let frame = f.frame(1, &[4], true);
        f.register(&frame);
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let registry = f.registry.clone();
        let key = f.key;
        let child_entered = entered.clone();
        let child_release = release.clone();
        let work = std::thread::spawn(move || {
            registry.receive_unit_with(key, frame, |_, _| {
                child_entered.wait();
                child_release.wait();
                Ok(Vec::new())
            })
        });
        entered.wait();
        let owner = f.registry.unit_slots.get(receiver_key(f.key)).unwrap();
        f.registry.cancel_exchange_key(f.key);
        assert_eq!(owner.snapshot().decoding, 1);
        assert_eq!(owner.snapshot().in_use, 1);
        assert!(matches!(
            owner.facts(f.stream).unwrap().receiver_closed,
            Some(ExchangeCloseDisposition::Withdrawn)
        ));
        release.wait();
        work.join().unwrap().unwrap();
        assert_eq!(owner.snapshot().in_use, 0);
        assert_eq!(owner.snapshot().decoding, 0);
        assert!(
            f.task.accepted().is_none(),
            "normal cleanup does not author a decode failure"
        );
    }
}
