use std::sync::Arc;
use std::time::Instant;

use novarocks_execution::runtime::exchange::ExchangeKey;
use novarocks_execution::runtime::execution_runtime::ExecutionRuntime;
use novarocks_execution::runtime::fragment::io::{
    ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverPort, ExchangeReceiverRegistration,
};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_types::UniqueId;

/// Backend-owned adapter that composes native ingress with the execution
/// receiver registry shared by fragment operators.
#[derive(Clone, Debug)]
pub(crate) struct BackendExchangeReceiverPort {
    runtime: Arc<ExecutionRuntime>,
}

impl BackendExchangeReceiverPort {
    pub(crate) fn new(runtime: Arc<ExecutionRuntime>) -> Self {
        Self { runtime }
    }

    fn key(key: ExchangeReceiverKey) -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: key.fragment_instance_id.high(),
            finst_id_lo: key.fragment_instance_id.low(),
            node_id: key.node_id,
        }
    }
}

impl ExchangeReceiverPort for BackendExchangeReceiverPort {
    fn register(&self, registration: ExchangeReceiverRegistration) -> Result<(), String> {
        self.runtime
            .exchange_registry()
            .try_register_expected_chunk_schema(
                Self::key(registration.key),
                registration.expected_senders,
                registration.expected_chunk_schema,
            )
    }

    fn push(&self, key: ExchangeReceiverKey, frame: ExchangeReceiverFrame) -> Result<(), String> {
        let exchange_key = Self::key(key);
        let decode_start = Instant::now();
        let registry = self.runtime.exchange_registry();
        let chunks = registry.decode_chunks_for_sender(
            exchange_key,
            frame.sender_id,
            frame.backend_number,
            &frame.payload,
        )?;
        registry.push_chunks_with_stats(
            exchange_key,
            frame.sender_id,
            frame.backend_number,
            chunks,
            frame.eos,
            frame.payload.len(),
            decode_start.elapsed().as_nanos(),
        );
        Ok(())
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
    ) -> Result<novarocks_execution::runtime::exchange::ExchangeReceiverHandle, String> {
        self.runtime
            .exchange_registry()
            .get_receiver_handle(Self::key(key), expected_senders)
    }

    fn ensure_mem_tracker(
        &self,
        key: ExchangeReceiverKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, String> {
        self.runtime
            .exchange_registry()
            .ensure_receiver_mem_tracker(Self::key(key), root)
    }

    fn push_local(
        &self,
        key: ExchangeReceiverKey,
        sender_id: i32,
        backend_number: i32,
        chunks: Vec<novarocks_execution::exec::chunk::Chunk>,
        eos: bool,
    ) {
        self.runtime.exchange_registry().push_chunks(
            Self::key(key),
            sender_id,
            backend_number,
            chunks,
            eos,
        );
    }

    fn snapshot(
        &self,
        key: ExchangeReceiverKey,
    ) -> Option<novarocks_execution::runtime::exchange::ExchangeReceiverSnapshot> {
        self.runtime
            .exchange_registry()
            .snapshot_receiver_state(Self::key(key))
    }
}
