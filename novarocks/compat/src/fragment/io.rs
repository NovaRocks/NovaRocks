use std::sync::Arc;

use novarocks::runtime::fragment::{
    ExchangeFrame, ExchangeFrameTransmitter, FragmentIoError, FragmentIoErrorKind,
    FragmentIoOperation,
};

pub(crate) fn brpc_exchange_transmitter() -> Arc<dyn ExchangeFrameTransmitter> {
    Arc::new(BrpcExchangeFrameTransmitter)
}

struct BrpcExchangeFrameTransmitter;

impl ExchangeFrameTransmitter for BrpcExchangeFrameTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), FragmentIoError> {
        let port = u16::try_from(frame.destination.port()).map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ExchangeTransmit,
                FragmentIoErrorKind::InvalidResponse,
                format!("invalid BRPC exchange destination port: {error}"),
            )
        })?;
        crate::internal_rpc_client::send_chunks(
            frame.destination.host(),
            port,
            frame.destination_fragment_instance_id,
            frame.destination_node_id,
            frame.sender_id,
            frame.backend_number,
            frame.eos,
            frame.sequence,
            frame.payload,
        )
        .map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ExchangeTransmit,
                FragmentIoErrorKind::Unavailable,
                error,
            )
        })
    }
}
