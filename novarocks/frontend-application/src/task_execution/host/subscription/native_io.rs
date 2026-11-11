// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One authenticated Native covered status stream for an admitted generation.

use super::{
    CoveredSubscriptionIo, CoveredSubscriptionStream, StreamKey, SubscriptionEnd,
    SubscriptionFailureCustody, SubscriptionGeneration, SubscriptionIoFuture, SubscriptionRunError,
};
use crate::native::{
    task_transport::wire::subscription_rejection_is_fatal,
    transport::{ChannelAcquisitionError, Client, is_observed_connection_loss},
};
use novarocks_attempt_protocol::{
    failure::HostProjectedFailure,
    input::{ObservationFailure, ObservationFailureKind},
};
use novarocks_control_runtime::actor::Actor;
use novarocks_proto_codec::ProtocolError;
use novarocks_proto_models::novarocks::TaskStatusStreamEvent;
use novarocks_task_codec::operation::{
    DecodedCoveredSubscription, encode_covered_subscribe_task_status,
};
use novarocks_types::BackendProcessId;
use std::{alloc::Layout, future::Future, sync::Arc};

/// The original failure object and the phase that observed it.
#[derive(Debug)]
pub(crate) enum NativeSubscriptionFailure {
    WrongClient {
        expected: BackendProcessId,
        actual: BackendProcessId,
    },
    Encode(ProtocolError),
    Channel(ChannelAcquisitionError),
    Open(tonic::Status),
    Read(tonic::Status),
}

impl NativeSubscriptionFailure {
    pub(super) fn kind(&self) -> ObservationFailureKind {
        match self {
            Self::WrongClient { .. } | Self::Encode(_) => ObservationFailureKind::ProtocolViolation,
            Self::Channel(error) => ObservationFailureKind::RecoverableTransport {
                observed_connection_loss: error.observed_connection_loss(),
            },
            Self::Open(status) | Self::Read(status) if subscription_rejection_is_fatal(status) => {
                ObservationFailureKind::RpcRejected
            }
            Self::Open(status) | Self::Read(status) => {
                ObservationFailureKind::RecoverableTransport {
                    observed_connection_loss: is_observed_connection_loss(status),
                }
            }
        }
    }
}

/// The same admitted Source retains the raw failure in its original canonical
/// record, including the opening/reading site, and returns its passive projection.
pub(crate) trait NativeSubscriptionFailureCustody: SubscriptionFailureCustody {
    fn retain_native_failure(
        &self,
        key: StreamKey,
        failure: NativeSubscriptionFailure,
    ) -> HostProjectedFailure;
}

struct NativeCoveredSubscriptionIo {
    key: StreamKey,
    client: Client,
    failures: Arc<dyn NativeSubscriptionFailureCustody>,
}

impl NativeCoveredSubscriptionIo {
    async fn open_original(
        &self,
        request: DecodedCoveredSubscription,
    ) -> Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure> {
        assert_eq!(
            (request.participation, request.generation),
            (self.key.participation, self.key.generation),
            "the admitted generation supplies its own immutable covered request",
        );
        let expected = self.key.participation.backend_process_id();
        let actual = self.client.backend_process_id();
        if actual != expected {
            return Err(retain(
                self.failures.as_ref(),
                self.key,
                NativeSubscriptionFailure::WrongClient { expected, actual },
            ));
        }
        let wire = encode_covered_subscribe_task_status(&request).map_err(|error| {
            retain(
                self.failures.as_ref(),
                self.key,
                NativeSubscriptionFailure::Encode(error),
            )
        })?;
        let mut grpc = self
            .client
            .grpc_for_covered_subscription()
            .await
            .map_err(|error| {
                retain(
                    self.failures.as_ref(),
                    self.key,
                    NativeSubscriptionFailure::Channel(error),
                )
            })?;
        let stream = grpc
            .subscribe_task_status(tonic::Request::new(wire))
            .await
            .map_err(|error| {
                retain(
                    self.failures.as_ref(),
                    self.key,
                    NativeSubscriptionFailure::Open(error),
                )
            })?
            .into_inner();
        Ok(Box::new(NativeCoveredSubscriptionStream {
            stream,
            key: self.key,
            failures: Arc::clone(&self.failures),
        }))
    }
}

impl CoveredSubscriptionIo for NativeCoveredSubscriptionIo {
    fn open(
        &self,
        request: DecodedCoveredSubscription,
    ) -> SubscriptionIoFuture<'_, Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>>
    {
        Box::pin(self.open_original(request))
    }
}

struct NativeCoveredSubscriptionStream {
    // The original Tonic response body holds its Native lane stream position
    // until EOF or actual drop inside the credentialed generation body.
    stream: tonic::Streaming<TaskStatusStreamEvent>,
    key: StreamKey,
    failures: Arc<dyn NativeSubscriptionFailureCustody>,
}

impl NativeCoveredSubscriptionStream {
    async fn next_original(&mut self) -> Result<Option<TaskStatusStreamEvent>, ObservationFailure> {
        self.stream.message().await.map_err(|error| {
            retain(
                self.failures.as_ref(),
                self.key,
                NativeSubscriptionFailure::Read(error),
            )
        })
    }
}

impl CoveredSubscriptionStream for NativeCoveredSubscriptionStream {
    fn next(
        &mut self,
    ) -> SubscriptionIoFuture<'_, Result<Option<TaskStatusStreamEvent>, ObservationFailure>> {
        Box::pin(self.next_original())
    }
}

fn retain(
    failures: &dyn NativeSubscriptionFailureCustody,
    key: StreamKey,
    original: NativeSubscriptionFailure,
) -> ObservationFailure {
    let kind = original.kind();
    let cause = failures.retain_native_failure(key, original);
    ObservationFailure { kind, cause }
}

impl<A: Actor> SubscriptionGeneration<A> {
    /// Consume the actual prepared generation in the original credentialed
    /// carrier. Its original Work and ExitCustody wrap this same future.
    pub(crate) fn run_native(
        self,
        client: Client,
        failures: Arc<dyn NativeSubscriptionFailureCustody>,
    ) -> impl Future<Output = Result<SubscriptionEnd, SubscriptionRunError>> + Send {
        let key = self
            .guard
            .as_ref()
            .expect("original prepared generation")
            .key;
        let io = Arc::new(NativeCoveredSubscriptionIo {
            key,
            client,
            failures: Arc::clone(&failures),
        });
        self.run(io, failures)
    }
}

/// First-party allocations introduced by this adapter. The original Source
/// also prices its own generation carrier and the existing Native allocations
/// through their respective owners.
pub(crate) fn native_subscription_io_layouts()
-> Result<[Layout; 4], novarocks_type_contract::owned_resources::layout::LayoutResourceError> {
    fn open_layout<F: Future>(
        _: fn(&'static NativeCoveredSubscriptionIo, DecodedCoveredSubscription) -> F,
    ) -> Layout {
        Layout::new::<F>()
    }
    fn next_layout<F: Future>(_: fn(&'static mut NativeCoveredSubscriptionStream) -> F) -> Layout {
        Layout::new::<F>()
    }

    Ok([
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
            NativeCoveredSubscriptionIo,
        >())?,
        open_layout(NativeCoveredSubscriptionIo::open_original),
        Layout::new::<NativeCoveredSubscriptionStream>(),
        next_layout(NativeCoveredSubscriptionStream::next_original),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_covered_failures_preserve_original_retry_and_connection_evidence() {
        use ObservationFailureKind::{RecoverableTransport, RpcRejected};

        let open = NativeSubscriptionFailure::Open(tonic::Status::failed_precondition(
            "the covered subscription was rejected",
        ));
        let read = NativeSubscriptionFailure::Read(tonic::Status::unavailable(
            "connection reset is only status text",
        ));
        let typed_loss = NativeSubscriptionFailure::Read(tonic::Status::from_error(Box::new(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        )));
        let local_channel = NativeSubscriptionFailure::Channel(ChannelAcquisitionError::fatal(
            "channel acquisition failed before a connection was observed",
        ));

        assert_eq!(open.kind(), RpcRejected);
        assert_eq!(
            read.kind(),
            RecoverableTransport {
                observed_connection_loss: false,
            },
        );
        assert_eq!(
            typed_loss.kind(),
            RecoverableTransport {
                observed_connection_loss: true,
            },
        );
        assert_eq!(
            local_channel.kind(),
            RecoverableTransport {
                observed_connection_loss: false,
            },
        );
    }
}
