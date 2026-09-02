use std::sync::Arc;

use novarocks_execution::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, ExchangeTransmitRejection, FragmentIoError,
    FragmentIoErrorKind, FragmentIoOperation,
};

use crate::BackendDataRuntime;
use crate::rpc::client::BackendRpcClient;

pub(crate) fn grpc_exchange_transmitter(
    runtime: BackendDataRuntime,
) -> Arc<dyn ExchangeFrameTransmitter> {
    Arc::new(GrpcExchangeFrameTransmitter { runtime })
}

/// Wraps a transport-level cause as the producer's own failure.
fn failed(kind: FragmentIoErrorKind, detail: impl Into<String>) -> ExchangeTransmitRejection {
    ExchangeTransmitRejection::Failed(FragmentIoError::new(
        FragmentIoOperation::ExchangeTransmit,
        kind,
        detail,
    ))
}

struct GrpcExchangeFrameTransmitter {
    runtime: BackendDataRuntime,
}

impl ExchangeFrameTransmitter for GrpcExchangeFrameTransmitter {
    /// Every outcome here is the producer's own failure.
    ///
    /// A destination cannot yet report a normal departure: the exchange
    /// response carries no typed rejection, so there is nothing to map onto
    /// `DestinationCanceled`. Reporting one anyway would let a real failure
    /// pass as a success-compatible cancellation, so everything stays
    /// `Failed` until the response can actually say otherwise.
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
        let port = u16::try_from(frame.destination.port()).map_err(|error| {
            failed(
                FragmentIoErrorKind::InvalidResponse,
                format!("invalid gRPC exchange destination port: {error}"),
            )
        })?;
        let client = BackendRpcClient::new_host_port(
            self.runtime.clone(),
            frame.destination.host().to_string(),
            port,
        )
        .map_err(|error| failed(FragmentIoErrorKind::Unavailable, error))?;
        client
            .exchange_unary(
                frame.destination_fragment_instance_id,
                frame.destination_node_id,
                frame.sender_fragment_instance_id,
                frame.sender_ordinal,
                frame.sender_count,
                frame.sender_id,
                frame.backend_number,
                frame.eos,
                frame.sequence,
                frame.payload,
            )
            .map_err(|error| failed(FragmentIoErrorKind::Unavailable, error))
    }
}
