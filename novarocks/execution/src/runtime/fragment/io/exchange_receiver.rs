//! Application-hosted ingress for one exchange receiver.

use crate::exec::chunk::ChunkSchemaRef;
use crate::runtime::exchange::ExchangeReceiverHandle;
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
}

/// Backend-owned receiver registry and ingress boundary.
///
/// Execution uses this port instead of a process-global exchange map. The
/// backend installs one shared implementation for gRPC ingress and operators.
pub trait ExchangeReceiverPort: Send + Sync + 'static {
    fn register(&self, registration: ExchangeReceiverRegistration) -> Result<(), String>;
    fn push(&self, key: ExchangeReceiverKey, frame: ExchangeReceiverFrame) -> Result<(), String>;
    fn cancel(&self, key: ExchangeReceiverKey);
    fn remove(&self, key: ExchangeReceiverKey);
    fn cancel_fragment(&self, fragment_instance_id: UniqueId);
    fn receiver_handle(
        &self,
        key: ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, String>;
    fn ensure_mem_tracker(
        &self,
        key: ExchangeReceiverKey,
        root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, String>;
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

#[derive(Debug, Default)]
pub struct UnavailableExchangeReceiverPort;

impl ExchangeReceiverPort for UnavailableExchangeReceiverPort {
    fn register(&self, _registration: ExchangeReceiverRegistration) -> Result<(), String> {
        Err("exchange receiver port is unavailable".to_string())
    }

    fn push(&self, _key: ExchangeReceiverKey, _frame: ExchangeReceiverFrame) -> Result<(), String> {
        Err("exchange receiver port is unavailable".to_string())
    }

    fn cancel(&self, _key: ExchangeReceiverKey) {}

    fn remove(&self, _key: ExchangeReceiverKey) {}

    fn cancel_fragment(&self, _fragment_instance_id: UniqueId) {}

    fn receiver_handle(
        &self,
        _key: ExchangeReceiverKey,
        _expected_senders: usize,
    ) -> Result<ExchangeReceiverHandle, String> {
        Err("exchange receiver port is unavailable".to_string())
    }

    fn ensure_mem_tracker(
        &self,
        _key: ExchangeReceiverKey,
        _root: &Arc<MemTracker>,
    ) -> Result<Arc<MemTracker>, String> {
        Err("exchange receiver port is unavailable".to_string())
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
