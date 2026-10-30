//! Application-hosted ingress for one exchange receiver.

use crate::exec::chunk::ChunkSchemaRef;
use crate::runtime::exchange::{
    ExchangeColumnBinding, ExchangeKey, ExchangeReceiverHandle, ExchangeSenderIdentity,
};
use crate::runtime::execution_runtime::ExecutionRuntime;
use crate::runtime::mem_tracker::MemTracker;
use novarocks_types::UniqueId;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExchangeReceiverKey {
    pub fragment_instance_id: UniqueId,
    pub node_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExchangeReceiverFrame {
    pub stream: novarocks_execution_contract::ExchangeStreamIdentity,
    pub source_fragment_instance_id: UniqueId,
    pub sender_ordinal: u32,
    pub sender_count: u32,
    pub sender_id: i32,
    pub backend_number: i32,
    pub sequence: i64,
    pub eos: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone)]
pub struct ExchangeReceiverRegistration {
    pub key: ExchangeReceiverKey,
    pub expected_senders: usize,
    pub expected_chunk_schema: ChunkSchemaRef,
    /// How decoded wire columns bind to `expected_chunk_schema`. Legacy
    /// fragments bind by slot id; compiled fragments bind by position.
    pub column_binding: ExchangeColumnBinding,
    pub creation_factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
}

/// Application-hosted receiver registry and ingress boundary.
///
/// Execution uses this port instead of a process-global exchange map. The
/// role installs one shared implementation for native ingress and operators.
pub trait ExchangeReceiverPort: Send + Sync + 'static {
    fn register(
        &self,
        registration: ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure>;
    fn push(
        &self,
        key: ExchangeReceiverKey,
        frame: ExchangeReceiverFrame,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure>;
    fn cancel(&self, key: ExchangeReceiverKey);
    fn remove(&self, key: ExchangeReceiverKey);
    fn cancel_fragment(&self, fragment_instance_id: UniqueId);
    fn receiver_handle(
        &self,
        key: ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, crate::runtime::exchange::ExchangeFailure>;
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
    fn push_local(
        &self,
        key: ExchangeReceiverKey,
        sender_id: i32,
        backend_number: i32,
        chunks: Vec<crate::exec::chunk::Chunk>,
        eos: bool,
    );
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
            finst_id_hi: key.fragment_instance_id.high(),
            finst_id_lo: key.fragment_instance_id.low(),
            node_id: key.node_id,
        }
    }
}

impl ExchangeReceiverPort for ExecutionRuntimeExchangeReceiverPort {
    fn register(
        &self,
        registration: ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        let key = Self::key(registration.key);
        self.runtime
            .exchange_registry()
            .try_register_expected_chunk_schema_with_binding(
                key,
                registration.expected_senders,
                registration.expected_chunk_schema,
                registration.column_binding,
                registration.creation_factory,
            )?;
        #[cfg(novarocks_dev_controls)]
        if let Some(observer) = self.test_observer.clone() {
            self.runtime
                .exchange_registry()
                .install_test_observer(key, observer)?;
        }
        Ok(())
    }

    fn push(
        &self,
        key: ExchangeReceiverKey,
        frame: ExchangeReceiverFrame,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        self.runtime
            .exchange_registry()
            .admit_native_frame(Self::key(key), frame)
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

    fn cancel_fragment(&self, fragment_instance_id: UniqueId) {
        self.runtime
            .exchange_registry()
            .cancel_fragment(fragment_instance_id.high(), fragment_instance_id.low());
    }

    fn receiver_handle(
        &self,
        key: ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, crate::runtime::exchange::ExchangeFailure> {
        self.runtime
            .exchange_registry()
            .get_receiver_handle(Self::key(key), expected_senders)
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

    fn push_local(
        &self,
        key: ExchangeReceiverKey,
        sender_id: i32,
        backend_number: i32,
        chunks: Vec<crate::exec::chunk::Chunk>,
        eos: bool,
    ) {
        self.runtime.exchange_registry().push_chunks(
            Self::key(key),
            ExchangeSenderIdentity::local(sender_id, backend_number),
            chunks,
            eos,
        );
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
    fn register(
        &self,
        _registration: ExchangeReceiverRegistration,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn push(
        &self,
        _key: ExchangeReceiverKey,
        _frame: ExchangeReceiverFrame,
    ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
        Err(crate::runtime::exchange::ExchangeFailure::receiver_unavailable())
    }

    fn cancel(&self, _key: ExchangeReceiverKey) {}

    fn remove(&self, _key: ExchangeReceiverKey) {}

    fn cancel_fragment(&self, _fragment_instance_id: UniqueId) {}

    fn receiver_handle(
        &self,
        _key: ExchangeReceiverKey,
        _expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, crate::runtime::exchange::ExchangeFailure> {
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

    fn push_local(
        &self,
        _key: ExchangeReceiverKey,
        _sender_id: i32,
        _backend_number: i32,
        _chunks: Vec<crate::exec::chunk::Chunk>,
        _eos: bool,
    ) {
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
    use std::sync::Arc;

    use super::{
        ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverPort,
        ExecutionRuntimeExchangeReceiverPort,
    };
    use crate::runtime::execution_runtime::test_execution_runtime;
    use novarocks_types::UniqueId;

    #[test]
    fn runtime_port_uses_exact_native_source_identity_for_eos() {
        let port = ExecutionRuntimeExchangeReceiverPort::new(Arc::clone(&test_execution_runtime()));
        let key = ExchangeReceiverKey {
            fragment_instance_id: UniqueId::new(50, 60),
            node_id: 70,
        };
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("exact native EOS fixture");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(), std::sync::Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let layout = crate::exec::chunk::typed_fixture::layout(
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            &[],
            &[],
        );
        let expected = factory.create_compiled_chunk_schema(&layout).unwrap();
        port.register(crate::runtime::fragment::io::ExchangeReceiverRegistration {
            key,
            expected_senders: 2,
            expected_chunk_schema: expected,
            column_binding: crate::runtime::exchange::ExchangeColumnBinding::Positional,
            creation_factory: factory.clone(),
        })
        .expect("actual native receiver installation");
        let frame = |source_fragment_instance_id, sender_ordinal| ExchangeReceiverFrame {
            stream: crate::runtime::fragment::io::exchange::fixture_stream(
                source_fragment_instance_id,
                key.fragment_instance_id,
                key.node_id,
                sender_ordinal,
            ),
            source_fragment_instance_id,
            sender_ordinal,
            sender_count: 2,
            // These legacy fields intentionally collide. Native completion
            // identity must not use either of them.
            sender_id: 7,
            backend_number: 11,
            sequence: 0,
            eos: true,
            payload: Vec::new(),
        };

        port.push(key, frame(UniqueId::new(1, 1), 0))
            .expect("first native eos");
        port.push(key, frame(UniqueId::new(2, 2), 1))
            .expect("second native eos");

        let snapshot = port.snapshot(key).expect("receiver snapshot");
        assert_eq!(snapshot.finished_senders, 2);
        port.remove(key);
        assert!(port.snapshot(key).is_none());
        drop(factory);
        assert_eq!(
            tracker.current(),
            0,
            "removed receiver releases its finite creation"
        );
    }
}
