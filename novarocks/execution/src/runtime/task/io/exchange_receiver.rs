//! Application-hosted ingress for one exchange receiver.

use crate::exec::chunk::ChunkSchemaRef;
use crate::runtime::exchange::{
    ExchangeColumnBinding, ExchangeKey, ExchangeReceiverCloseObserver, ExchangeReceiverPin,
};
use crate::runtime::execution_runtime::ExecutionRuntime;
use crate::runtime::mem_tracker::MemTracker;
use novarocks_types::UniqueId;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExchangeReceiverKey {
    pub task_key: UniqueId,
    pub node_id: i32,
}

#[derive(Clone)]
pub struct ExchangeReceiverRegistration {
    pub key: ExchangeReceiverKey,
    pub expected_senders: usize,
    /// Complete source declaration for the prepared wire recipe.
    pub wire_chunk_schema: ChunkSchemaRef,
    /// Local output declaration consumed after positional decoding.
    pub expected_chunk_schema: ChunkSchemaRef,
    /// How decoded wire columns bind to the complete frozen declaration.
    pub column_binding: ExchangeColumnBinding,
    pub creation_factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
}

/// Application-hosted receiver registry and ingress boundary.
///
/// Execution uses this port instead of a process-global exchange map. The
/// role installs one shared implementation for native ingress and operators.
pub trait ExchangeReceiverPort: Send + Sync + 'static {
    /// Freeze the exact roster and original Task failure owner before Installed.
    fn bind_unit_receiver(
        &self,
        key: ExchangeReceiverKey,
        streams: Vec<novarocks_execution_contract::ExchangeStreamIdentity>,
        control: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
        grantable_unit_bytes: u64,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure>;
    fn register_unit_demand(
        &self,
        key: ExchangeReceiverKey,
        entry: super::exchange_unit::ExchangeDemandEntry,
    ) -> Result<
        super::exchange_unit::ExchangeStreamObservation,
        novarocks_failure_contract::FailureFact,
    >;
    fn receive_unit(
        &self,
        key: ExchangeReceiverKey,
        frame: super::exchange_unit::ExchangeUnitReceiverFrame,
    ) -> Result<super::exchange_unit::ExchangeUnitReceipt, novarocks_failure_contract::FailureFact>;
    /// A page from the real owner, with its change feed installed first.
    fn unit_sender_states(
        &self,
        sender: novarocks_types::BackendProcessId,
        after: Option<novarocks_execution_contract::ExchangeStreamIdentity>,
        limit: usize,
    ) -> Result<
        Vec<super::exchange_unit::ExchangeStreamObservation>,
        novarocks_failure_contract::FailureFact,
    >;
    fn set_unit_state_listener(
        &self,
        listener: Arc<dyn super::exchange_unit::ExchangeUnitStateListener>,
    ) -> Result<(), novarocks_failure_contract::FailureFact>;
    fn register(
        &self,
        registration: ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure>;
    /// Lifecycle preparation must capture its exact registered receiver in
    /// the registration operation, not re-look it up after cancellation.
    /// An error must not leave a newly registered receiver without its owner.
    fn register_with_close_observer(
        &self,
        _registration: ExchangeReceiverRegistration,
    ) -> Result<ExchangeReceiverCloseObserver, crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }
    /// A one-time registration-stage observation; never creates a receiver.
    fn close_observer(
        &self,
        _key: ExchangeReceiverKey,
    ) -> Result<ExchangeReceiverCloseObserver, crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }
    fn cancel(&self, key: ExchangeReceiverKey);
    fn remove(&self, key: ExchangeReceiverKey);
    fn cancel_task(&self, task_key: UniqueId);
    fn receiver_pin(
        &self,
        key: ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverPin, crate::runtime::exchange::ExchangeFailure>;
    /// Read only facts authored by the real input source; never infer them
    /// from Task status, accepted End, readiness, or a missing receiver.
    fn stream_close_facts(
        &self,
        key: ExchangeReceiverKey,
        expected: &[novarocks_execution_contract::ExchangeStreamIdentity],
    ) -> Result<
        Vec<novarocks_execution_contract::ExchangeStreamClose>,
        crate::runtime::exchange::ExchangeFailure,
    >;
    fn ensure_mem_tracker(
        &self,
        key: ExchangeReceiverKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, crate::runtime::exchange::ExchangeFailure>;
    fn snapshot(
        &self,
        key: ExchangeReceiverKey,
    ) -> Option<crate::runtime::exchange::ExchangeReceiverSnapshot>;
}

/// Runtime-owned bridge from process ingress frames to one execution runtime's
/// exchange registry.
///
/// The adapter has no role, listener, or Worker lifecycle state. A host gives
/// the same instance to native ingress and fragment operators so they share
/// the one execution runtime registry for the process.
#[derive(Clone, Debug)]
pub struct ExecutionRuntimeExchangeReceiverPort {
    runtime: Arc<ExecutionRuntime>,
    #[cfg(novarocks_dev_controls)]
    test_observer: Option<Arc<dyn crate::runtime::exchange::TestEventObserver>>,
}

impl ExecutionRuntimeExchangeReceiverPort {
    pub fn new(runtime: Arc<ExecutionRuntime>) -> Self {
        Self {
            runtime,
            #[cfg(novarocks_dev_controls)]
            test_observer: None,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_test_observer(
        mut self,
        observer: Arc<dyn crate::runtime::exchange::TestEventObserver>,
    ) -> Self {
        self.test_observer = Some(observer);
        self
    }

    fn key(key: ExchangeReceiverKey) -> ExchangeKey {
        ExchangeKey {
            task_key_hi: key.task_key.high(),
            task_key_lo: key.task_key.low(),
            node_id: key.node_id,
        }
    }
}

impl ExchangeReceiverPort for ExecutionRuntimeExchangeReceiverPort {
    fn bind_unit_receiver(
        &self,
        key: ExchangeReceiverKey,
        streams: Vec<novarocks_execution_contract::ExchangeStreamIdentity>,
        control: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
        grantable_unit_bytes: u64,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        self.runtime.exchange_registry().bind_unit_receiver(
            Self::key(key),
            streams,
            control,
            grantable_unit_bytes,
        )
    }

    fn register_unit_demand(
        &self,
        key: ExchangeReceiverKey,
        entry: super::exchange_unit::ExchangeDemandEntry,
    ) -> Result<
        super::exchange_unit::ExchangeStreamObservation,
        novarocks_failure_contract::FailureFact,
    > {
        use novarocks_failure_contract::FailureProjection;
        self.runtime
            .exchange_registry()
            .register_unit_demand(Self::key(key), entry)
            .map_err(|error| error.failure_fact())
    }

    fn receive_unit(
        &self,
        key: ExchangeReceiverKey,
        frame: super::exchange_unit::ExchangeUnitReceiverFrame,
    ) -> Result<super::exchange_unit::ExchangeUnitReceipt, novarocks_failure_contract::FailureFact>
    {
        use novarocks_failure_contract::FailureProjection;
        self.runtime
            .exchange_registry()
            .receive_unit(Self::key(key), frame)
            .map_err(|error| error.failure_fact())
    }

    fn unit_sender_states(
        &self,
        sender: novarocks_types::BackendProcessId,
        after: Option<novarocks_execution_contract::ExchangeStreamIdentity>,
        limit: usize,
    ) -> Result<
        Vec<super::exchange_unit::ExchangeStreamObservation>,
        novarocks_failure_contract::FailureFact,
    > {
        Ok(self
            .runtime
            .exchange_registry()
            .unit_sender_states(sender, after, limit))
    }

    fn set_unit_state_listener(
        &self,
        listener: Arc<dyn super::exchange_unit::ExchangeUnitStateListener>,
    ) -> Result<(), novarocks_failure_contract::FailureFact> {
        use novarocks_failure_contract::FailureProjection;
        self.runtime
            .exchange_registry()
            .set_unit_state_listener(listener)
            .map_err(|error| error.failure_fact())
    }

    fn register(
        &self,
        registration: ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        self.register_with_close_observer(registration).map(|_| ())
    }
    fn register_with_close_observer(
        &self,
        registration: ExchangeReceiverRegistration,
    ) -> Result<ExchangeReceiverCloseObserver, crate::runtime::exchange::ExchangeFailure> {
        let key = Self::key(registration.key);
        let original = self
            .runtime
            .exchange_registry()
            .try_register_expected_chunk_schema_with_close_observer(
                key,
                registration.expected_senders,
                registration.wire_chunk_schema,
                registration.expected_chunk_schema,
                registration.column_binding,
                registration.creation_factory,
            )?;
        #[cfg(novarocks_dev_controls)]
        if let Some(observer) = self.test_observer.clone()
            && let Err(error) = original.install_test_observer(observer)
        {
            // This operation actually registered the receiver. Its own
            // failure retains the original loan until cancellation ends.
            self.runtime.exchange_registry().cancel_exchange_key(key);
            return Err(error);
        }
        Ok(original)
    }
    fn close_observer(
        &self,
        key: ExchangeReceiverKey,
    ) -> Result<ExchangeReceiverCloseObserver, crate::runtime::exchange::ExchangeFailure> {
        self.runtime
            .exchange_registry()
            .close_observer(Self::key(key))
    }

    fn cancel(&self, key: ExchangeReceiverKey) {
        self.runtime
            .exchange_registry()
            .cancel_exchange_key(Self::key(key));
    }

    fn remove(&self, key: ExchangeReceiverKey) {
        self.runtime
            .exchange_registry()
            .remove_exchange_key(Self::key(key));
    }

    fn cancel_task(&self, task_key: UniqueId) {
        self.runtime
            .exchange_registry()
            .cancel_task(task_key.high(), task_key.low());
    }

    fn receiver_pin(
        &self,
        key: ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverPin, crate::runtime::exchange::ExchangeFailure> {
        self.runtime
            .exchange_registry()
            .get_receiver_pin(Self::key(key), expected_senders)
    }

    fn stream_close_facts(
        &self,
        key: ExchangeReceiverKey,
        expected: &[novarocks_execution_contract::ExchangeStreamIdentity],
    ) -> Result<
        Vec<novarocks_execution_contract::ExchangeStreamClose>,
        crate::runtime::exchange::ExchangeFailure,
    > {
        self.runtime
            .exchange_registry()
            .stream_close_facts(Self::key(key), expected)
    }

    fn ensure_mem_tracker(
        &self,
        key: ExchangeReceiverKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, crate::runtime::exchange::ExchangeFailure> {
        self.runtime
            .exchange_registry()
            .ensure_receiver_mem_tracker(Self::key(key), root)
    }

    fn snapshot(
        &self,
        key: ExchangeReceiverKey,
    ) -> Option<crate::runtime::exchange::ExchangeReceiverSnapshot> {
        self.runtime
            .exchange_registry()
            .snapshot_receiver_state(Self::key(key))
    }
}

#[derive(Debug, Default)]
pub struct UnavailableExchangeReceiverPort;

impl ExchangeReceiverPort for UnavailableExchangeReceiverPort {
    fn bind_unit_receiver(
        &self,
        _key: ExchangeReceiverKey,
        _streams: Vec<novarocks_execution_contract::ExchangeStreamIdentity>,
        _control: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
        _grantable_unit_bytes: u64,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }
    fn register_unit_demand(
        &self,
        _key: ExchangeReceiverKey,
        _entry: super::exchange_unit::ExchangeDemandEntry,
    ) -> Result<
        super::exchange_unit::ExchangeStreamObservation,
        novarocks_failure_contract::FailureFact,
    > {
        use novarocks_failure_contract::FailureProjection;
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable().failure_fact())
    }
    fn receive_unit(
        &self,
        _key: ExchangeReceiverKey,
        _frame: super::exchange_unit::ExchangeUnitReceiverFrame,
    ) -> Result<super::exchange_unit::ExchangeUnitReceipt, novarocks_failure_contract::FailureFact>
    {
        use novarocks_failure_contract::FailureProjection;
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable().failure_fact())
    }
    fn unit_sender_states(
        &self,
        _sender: novarocks_types::BackendProcessId,
        _after: Option<novarocks_execution_contract::ExchangeStreamIdentity>,
        _limit: usize,
    ) -> Result<
        Vec<super::exchange_unit::ExchangeStreamObservation>,
        novarocks_failure_contract::FailureFact,
    > {
        use novarocks_failure_contract::FailureProjection;
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable().failure_fact())
    }
    fn set_unit_state_listener(
        &self,
        _listener: Arc<dyn super::exchange_unit::ExchangeUnitStateListener>,
    ) -> Result<(), novarocks_failure_contract::FailureFact> {
        use novarocks_failure_contract::FailureProjection;
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable().failure_fact())
    }

    fn register(
        &self,
        _registration: ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn cancel(&self, _key: ExchangeReceiverKey) {}

    fn remove(&self, _key: ExchangeReceiverKey) {}

    fn cancel_task(&self, _task_key: UniqueId) {}

    fn receiver_pin(
        &self,
        _key: ExchangeReceiverKey,
        _expected_senders: usize,
    ) -> Result<ExchangeReceiverPin, crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn stream_close_facts(
        &self,
        _key: ExchangeReceiverKey,
        _expected: &[novarocks_execution_contract::ExchangeStreamIdentity],
    ) -> Result<
        Vec<novarocks_execution_contract::ExchangeStreamClose>,
        crate::runtime::exchange::ExchangeFailure,
    > {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn ensure_mem_tracker(
        &self,
        _key: ExchangeReceiverKey,
        _root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn snapshot(
        &self,
        _key: ExchangeReceiverKey,
    ) -> Option<crate::runtime::exchange::ExchangeReceiverSnapshot> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::execution_runtime::test_execution_runtime;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use crate::runtime::task_control::test_support::Owner;
    use novarocks_execution_contract::ordered_retained_stream::exchange::{
        ExchangeAdmissionVerdict, ExchangeUnitItem, ExchangeUnitSequence,
    };
    use novarocks_execution_contract::{
        ExchangeCloseDisposition, ExchangeSource, ExchangeStreamIdentity, FragmentNodeId,
        TaskConclusionPort, TaskFailurePhase, TaskIdentity,
    };
    use novarocks_types::{StageId, TaskId};

    #[test]
    fn runtime_port_uses_exact_native_source_identity_for_eos() {
        let runtime = test_execution_runtime();
        let port = ExecutionRuntimeExchangeReceiverPort::new(runtime);
        let task = Owner::new();
        let control = Arc::new(RuntimeErrorState::for_task(Arc::new(
            task.view(TaskFailurePhase::Execution),
        )));
        let key = ExchangeReceiverKey {
            task_key: UniqueId::new(50, 60),
            node_id: 70,
        };
        let tracker = MemTracker::new_root("exact native EOS fixture");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(), control.clone(),
        ).unwrap();
        let layout = crate::exec::chunk::typed_fixture::layout(
            Arc::new(arrow::datatypes::Schema::empty()),
            &[],
            &[],
        );
        let expected = factory.create_compiled_chunk_schema(&layout).unwrap();
        let close_observer = port
            .register_with_close_observer(ExchangeReceiverRegistration {
                key,
                expected_senders: 2,
                wire_chunk_schema: expected.clone(),
                expected_chunk_schema: expected,
                column_binding: crate::runtime::exchange::ExchangeColumnBinding::Positional,
                creation_factory: factory.clone(),
            })
            .expect("actual native receiver installation");
        let streams = [0_u32, 1].map(|ordinal| {
            ExchangeStreamIdentity::try_new(
                ExchangeSource::new(
                    TaskIdentity::new(
                        task.identity().query_execution_id(),
                        StageId::new(2).unwrap(),
                        TaskId::new(ordinal + 100).unwrap(),
                        task.identity().backend_process_id(),
                    ),
                    UniqueId::new(i64::from(ordinal) + 1, i64::from(ordinal) + 1),
                    ordinal,
                ),
                task.identity(),
                key.task_key,
                FragmentNodeId::new(key.node_id),
            )
            .unwrap()
        });
        port.bind_unit_receiver(key, streams.to_vec(), control,
            novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64,
        ).unwrap();
        let frame = |stream| super::super::exchange_unit::ExchangeUnitReceiverFrame {
            stream,
            sender_count: 2,
            // Auxiliary wire fields intentionally collide. They grant no
            // source identity or completion authority.
            sender_id: 7,
            backend_number: 11,
            item: ExchangeUnitItem::End(ExchangeUnitSequence::FIRST),
            payload: bytes::Bytes::new(),
            next_demand: None,
        };
        port.receive_unit(key, frame(streams[0])).unwrap();
        assert_eq!(port.snapshot(key).unwrap().finished_senders, 1);
        assert_eq!(
            port.receive_unit(key, frame(streams[0])).unwrap().verdict,
            ExchangeAdmissionVerdict::Idempotent
        );
        assert_eq!(port.snapshot(key).unwrap().finished_senders, 1);
        port.receive_unit(key, frame(streams[1])).unwrap();
        assert_eq!(port.snapshot(key).unwrap().finished_senders, 2);
        let receiver = port.receiver_pin(key, 2).unwrap();
        receiver.complete_input_consumption(2).unwrap();
        let closes = port.stream_close_facts(key, &streams).unwrap();
        assert!(closes.iter().all(|close| close.disposition
            == ExchangeCloseDisposition::Consumed {
                final_sequence: ExchangeUnitSequence::FIRST
            }));
        drop(receiver);
        port.remove(key);
        assert!(port.snapshot(key).is_none());
        assert_eq!(
            close_observer.stream_close_facts(&streams).unwrap(),
            closes,
            "the original Unit handoff facts survive removal of routing"
        );
        drop(close_observer);
        drop(factory);
        assert_eq!(
            tracker.current(),
            0,
            "removed receiver releases its finite creation"
        );
    }
}
