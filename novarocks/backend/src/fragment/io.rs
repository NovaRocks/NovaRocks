use std::sync::Arc;

use novarocks::runtime::fragment::{
    ExchangeFrame, ExchangeFrameTransmitter, FragmentIoError, FragmentIoErrorKind,
    FragmentIoOperation,
};

pub(crate) fn grpc_exchange_transmitter() -> Arc<dyn ExchangeFrameTransmitter> {
    Arc::new(GrpcExchangeFrameTransmitter)
}

struct GrpcExchangeFrameTransmitter;

impl ExchangeFrameTransmitter for GrpcExchangeFrameTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), FragmentIoError> {
        let port = u16::try_from(frame.destination.port()).map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ExchangeTransmit,
                FragmentIoErrorKind::InvalidResponse,
                format!("invalid gRPC exchange destination port: {error}"),
            )
        })?;
        novarocks::service::grpc_client::send_chunks(
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
