use novarocks_types::UniqueId;

use novarocks_execution::runtime::fragment::io::{ExchangeFrame, ExchangeFrameTransmitter};
use novarocks_execution::runtime::fragment::io::{
    FragmentIoError, FragmentIoErrorKind, FragmentIoOperation,
};

#[cfg(test)]
pub(crate) fn discard_exchange_transmitter() -> std::sync::Arc<dyn ExchangeFrameTransmitter> {
    std::sync::Arc::new(DiscardExchangeFrameTransmitter)
}

/// Test-only transport for core semantic tests.
///
/// Production backends still own their gRPC transmitter.  This adapter keeps
/// core unit tests on the encoded exchange-frame boundary while delivering
/// the frame to the same receiver handler in-process.
#[cfg(test)]
pub(crate) fn in_process_test_exchange_transmitter() -> std::sync::Arc<dyn ExchangeFrameTransmitter>
{
    std::sync::Arc::new(InProcessTestExchangeFrameTransmitter {
        receiver_port: InProcessTestExchangeReceiverPort::default(),
    })
}

#[cfg(test)]
pub(crate) fn in_process_test_exchange_receiver_port()
-> std::sync::Arc<dyn novarocks_execution::runtime::fragment::io::ExchangeReceiverPort> {
    test_exchange_receiver_port()
}

#[cfg(test)]
fn test_exchange_receiver_port() -> std::sync::Arc<InProcessTestExchangeReceiverPort> {
    static PORT: std::sync::OnceLock<std::sync::Arc<InProcessTestExchangeReceiverPort>> =
        std::sync::OnceLock::new();
    std::sync::Arc::clone(
        PORT.get_or_init(|| std::sync::Arc::new(InProcessTestExchangeReceiverPort::default())),
    )
}

#[cfg(test)]
pub(crate) fn test_exchange_snapshot(
    key: novarocks_execution::runtime::exchange::ExchangeKey,
) -> Option<novarocks_execution::runtime::exchange::ExchangeReceiverSnapshot> {
    test_exchange_receiver_port()
        .registry
        .snapshot_receiver_state(key)
}

#[cfg(test)]
pub(crate) fn test_register_exchange_schema(
    key: novarocks_execution::runtime::exchange::ExchangeKey,
    expected_senders: usize,
    schema: novarocks_execution::exec::chunk::ChunkSchemaRef,
) -> Result<(), String> {
    test_exchange_receiver_port()
        .registry
        .register_expected_chunk_schema(key, expected_senders, schema)
}

#[cfg(test)]
pub(crate) fn test_cancel_exchange_key(key: novarocks_execution::runtime::exchange::ExchangeKey) {
    test_exchange_receiver_port()
        .registry
        .cancel_exchange_key(key);
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
struct InProcessTestExchangeFrameTransmitter {
    receiver_port: InProcessTestExchangeReceiverPort,
}

#[cfg(test)]
#[derive(Clone, Default)]
struct InProcessTestExchangeReceiverPort {
    registry: std::sync::Arc<novarocks_execution::runtime::exchange::ExecutionExchangeRegistry>,
}

#[cfg(test)]
impl InProcessTestExchangeReceiverPort {
    fn exchange_key(
        key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
    ) -> novarocks_execution::runtime::exchange::ExchangeKey {
        novarocks_execution::runtime::exchange::ExchangeKey {
            finst_id_hi: key.fragment_instance_id.high(),
            finst_id_lo: key.fragment_instance_id.low(),
            node_id: key.node_id,
        }
    }
}

#[cfg(test)]
impl novarocks_execution::runtime::fragment::io::ExchangeReceiverPort
    for InProcessTestExchangeReceiverPort
{
    fn register(
        &self,
        registration: novarocks_execution::runtime::fragment::io::ExchangeReceiverRegistration,
    ) -> Result<(), String> {
        self.registry.try_register_expected_chunk_schema(
            Self::exchange_key(registration.key),
            registration.expected_senders,
            registration.expected_chunk_schema,
        )
    }

    fn push(
        &self,
        key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
        frame: novarocks_execution::runtime::fragment::io::ExchangeReceiverFrame,
    ) -> Result<(), String> {
        let key = Self::exchange_key(key);
        let decoded = self.registry.decode_chunks_for_sender(
            key,
            frame.sender_id,
            frame.backend_number,
            &frame.payload,
        )?;
        self.registry.push_chunks_with_stats(
            key,
            frame.sender_id,
            frame.backend_number,
            decoded,
            frame.eos,
            frame.payload.len(),
            0,
        );
        Ok(())
    }

    fn cancel(&self, key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey) {
        self.registry.cancel_exchange_key(Self::exchange_key(key));
    }

    fn remove(&self, key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey) {
        self.registry.remove_exchange_key(Self::exchange_key(key));
    }

    fn cancel_fragment(&self, fragment_instance_id: UniqueId) {
        self.registry
            .cancel_fragment(fragment_instance_id.high(), fragment_instance_id.low());
    }

    fn receiver_handle(
        &self,
        key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
        expected_senders: usize,
    ) -> Result<novarocks_execution::runtime::exchange::ExchangeReceiverHandle, String> {
        self.registry
            .get_receiver_handle(Self::exchange_key(key), expected_senders)
    }

    fn ensure_mem_tracker(
        &self,
        key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
        root: &std::sync::Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    ) -> Result<std::sync::Arc<novarocks_execution::runtime::mem_tracker::MemTracker>, String> {
        self.registry
            .ensure_receiver_mem_tracker(Self::exchange_key(key), root)
    }

    fn push_local(
        &self,
        key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
        sender_id: i32,
        backend_number: i32,
        chunks: Vec<novarocks_execution::exec::chunk::Chunk>,
        eos: bool,
    ) {
        self.registry.push_chunks(
            Self::exchange_key(key),
            sender_id,
            backend_number,
            chunks,
            eos,
        );
    }

    fn snapshot(
        &self,
        key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
    ) -> Option<novarocks_execution::runtime::exchange::ExchangeReceiverSnapshot> {
        self.registry
            .snapshot_receiver_state(Self::exchange_key(key))
    }
}

#[cfg(test)]
impl ExchangeFrameTransmitter for InProcessTestExchangeFrameTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), FragmentIoError> {
        let response = crate::service::internal_rpc::handle_transmit_chunk(
            &self.receiver_port,
            novarocks_protocol::novarocks::ExchangeRequest {
                finst_id_hi: frame.destination_fragment_instance_id.high(),
                finst_id_lo: frame.destination_fragment_instance_id.low(),
                node_id: frame.destination_node_id,
                sender_id: frame.sender_id,
                be_number: frame.backend_number,
                sequence: frame.sequence,
                eos: frame.eos,
                payload: frame.payload,
            },
        );
        let status = response.status.unwrap_or_default();
        if status.code == 0 {
            Ok(())
        } else {
            Err(FragmentIoError::new(
                FragmentIoOperation::ExchangeTransmit,
                FragmentIoErrorKind::RemoteRejected,
                status.message,
            ))
        }
    }
}
