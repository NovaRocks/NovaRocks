use crate::common::types::UniqueId;
use crate::runtime::endpoint::RuntimeEndpoint;

use super::FragmentIoError;

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

pub trait ExchangeFrameTransmitter: Send + Sync + 'static {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), FragmentIoError>;
}

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
    std::sync::Arc::new(InProcessTestExchangeFrameTransmitter)
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
struct InProcessTestExchangeFrameTransmitter;

#[cfg(test)]
impl ExchangeFrameTransmitter for InProcessTestExchangeFrameTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), FragmentIoError> {
        let response = crate::service::internal_rpc::handle_transmit_chunk(
            crate::proto::novarocks::ExchangeRequest {
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
                super::FragmentIoOperation::ExchangeTransmit,
                super::FragmentIoErrorKind::RemoteRejected,
                status.message,
            ))
        }
    }
}
