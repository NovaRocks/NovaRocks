use novarocks_types::UniqueId;

use crate::runtime::endpoint::RuntimeEndpoint;

use super::FragmentIoError;

/// Encoded exchange payload prepared by the execution kernel for a host-owned
/// transport adapter.
#[derive(Clone, Debug)]
pub struct ExchangeFrame {
    pub destination: RuntimeEndpoint,
    pub destination_fragment_instance_id: UniqueId,
    pub sender_fragment_instance_id: UniqueId,
    pub destination_node_id: i32,
    pub sender_id: i32,
    pub backend_number: i32,
    pub sequence: i64,
    pub eos: bool,
    pub payload: Vec<u8>,
}

/// Host-owned transport boundary for exchange frames.
pub trait ExchangeFrameTransmitter: Send + Sync + 'static {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), FragmentIoError>;
}

#[cfg(test)]
pub(crate) fn discard_exchange_transmitter() -> std::sync::Arc<dyn ExchangeFrameTransmitter> {
    std::sync::Arc::new(DiscardExchangeFrameTransmitter)
}

#[cfg(test)]
struct DiscardExchangeFrameTransmitter;

#[cfg(test)]
impl ExchangeFrameTransmitter for DiscardExchangeFrameTransmitter {
    fn transmit(&self, _frame: ExchangeFrame) -> Result<(), FragmentIoError> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn in_process_test_exchange_receiver_port()
-> std::sync::Arc<dyn super::ExchangeReceiverPort> {
    std::sync::Arc::new(TestExchangeReceiverPort::default())
}

#[cfg(test)]
#[derive(Default)]
struct TestExchangeReceiverPort {
    registry: std::sync::Arc<crate::runtime::exchange::ExecutionExchangeRegistry>,
}

#[cfg(test)]
impl TestExchangeReceiverPort {
    fn key(key: super::ExchangeReceiverKey) -> crate::runtime::exchange::ExchangeKey {
        crate::runtime::exchange::ExchangeKey {
            finst_id_hi: key.fragment_instance_id.high(),
            finst_id_lo: key.fragment_instance_id.low(),
            node_id: key.node_id,
        }
    }
}

#[cfg(test)]
impl super::ExchangeReceiverPort for TestExchangeReceiverPort {
    fn register(&self, registration: super::ExchangeReceiverRegistration) -> Result<(), String> {
        self.registry.try_register_expected_chunk_schema(
            Self::key(registration.key),
            registration.expected_senders,
            registration.expected_chunk_schema,
        )
    }

    fn push(
        &self,
        key: super::ExchangeReceiverKey,
        frame: super::ExchangeReceiverFrame,
    ) -> Result<(), String> {
        let key = Self::key(key);
        let chunks = self.registry.decode_chunks_for_sender(
            key,
            frame.sender_id,
            frame.backend_number,
            &frame.payload,
        )?;
        self.registry.push_chunks(
            key,
            frame.sender_id,
            frame.backend_number,
            chunks,
            frame.eos,
        );
        Ok(())
    }

    fn cancel(&self, key: super::ExchangeReceiverKey) {
        self.registry.cancel_exchange_key(Self::key(key));
    }
    fn remove(&self, key: super::ExchangeReceiverKey) {
        self.registry.remove_exchange_key(Self::key(key));
    }
    fn cancel_fragment(&self, fragment_instance_id: UniqueId) {
        self.registry
            .cancel_fragment(fragment_instance_id.high(), fragment_instance_id.low());
    }
    fn receiver_handle(
        &self,
        key: super::ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<crate::runtime::exchange::ExchangeReceiverHandle, String> {
        self.registry
            .get_receiver_handle(Self::key(key), expected_senders)
    }
    fn ensure_mem_tracker(
        &self,
        key: super::ExchangeReceiverKey,
        root: &std::sync::Arc<crate::runtime::mem_tracker::MemTracker>,
    ) -> Result<std::sync::Arc<crate::runtime::mem_tracker::MemTracker>, String> {
        self.registry
            .ensure_receiver_mem_tracker(Self::key(key), root)
    }
    fn push_local(
        &self,
        key: super::ExchangeReceiverKey,
        sender_id: i32,
        backend_number: i32,
        chunks: Vec<crate::exec::chunk::Chunk>,
        eos: bool,
    ) {
        self.registry
            .push_chunks(Self::key(key), sender_id, backend_number, chunks, eos);
    }
    fn snapshot(
        &self,
        key: super::ExchangeReceiverKey,
    ) -> Option<crate::runtime::exchange::ExchangeReceiverSnapshot> {
        self.registry.snapshot_receiver_state(Self::key(key))
    }
}
