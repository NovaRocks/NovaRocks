// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Buf;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::resource_preflight;
use prost::Message;
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

/// The original uncompressed unary operation encoder's requested backing.
/// This is a pure source fact, not a resource grant. The same plan sizes the
/// actual Tonic buffer before it exists and rejects a different message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeUnaryEncodingPlan {
    message_bytes: usize,
    buffer_bytes: usize,
}
impl NativeUnaryEncodingPlan {
    pub fn for_original_operation(message_bytes: usize) -> Option<Self> {
        if message_bytes == 0 {
            return None;
        }
        Some(Self {
            message_bytes,
            buffer_bytes: message_bytes.checked_add(5)?,
        })
    }
    /// Exact protobuf frame content; the process encoding wallet counts this
    /// independently of the payload backing's five-byte gRPC prefix.
    pub fn protobuf_bytes(self) -> usize {
        self.message_bytes
    }
    pub fn retained_bytes(self) -> usize {
        self.buffer_bytes
    }
}

/// Native protobuf codec used by generated NovaRocks clients and servers.
#[derive(Debug, Clone)]
pub struct NativeProstCodec<T, U> {
    operation_reply_plan: Option<resource_preflight::OperationReplyDecodePlan>,
    operation_encoding_plan: Option<NativeUnaryEncodingPlan>,
    marker: PhantomData<(T, U)>,
}

impl<T, U> Default for NativeProstCodec<T, U> {
    fn default() -> Self {
        Self {
            operation_reply_plan: None,
            operation_encoding_plan: None,
            marker: PhantomData,
        }
    }
}
impl<T, U> NativeProstCodec<T, U> {
    /// Caller has already installed this exact plan's original reply custody.
    pub fn with_operation_reply_plan(plan: resource_preflight::OperationReplyDecodePlan) -> Self {
        Self {
            operation_reply_plan: Some(plan),
            operation_encoding_plan: None,
            marker: PhantomData,
        }
    }
}

impl<T, U> NativeProstCodec<T, U> {
    /// The original Apply caller uses one uncompressed unary message. Keep its
    /// paid encoding plan on this same codec through actual frame exit.
    pub fn with_original_operation_plans(
        reply: resource_preflight::OperationReplyDecodePlan,
        encoding: NativeUnaryEncodingPlan,
    ) -> Self {
        Self {
            operation_reply_plan: Some(reply),
            operation_encoding_plan: Some(encoding),
            marker: PhantomData,
        }
    }
}

impl<T, U> Codec for NativeProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = NativeProstEncoder<T>;
    type Decoder = NativeProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        let mut encoder = NativeProstEncoder::default();
        if let Some(plan) = self.operation_encoding_plan {
            // Tonic's uncompressed unary stream owns one eagerly allocated
            // buffer. Exact preallocation includes its five-byte gRPC header;
            // the checked message fits without an old/new growth allocation.
            encoder.buffer_settings = BufferSettings::new(plan.buffer_bytes, plan.buffer_bytes);
            encoder.original_operation = Some(plan);
        }
        encoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        NativeProstDecoder {
            operation_reply_plan: self.operation_reply_plan,
            ..NativeProstDecoder::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct NativeProstEncoder<T> {
    marker: PhantomData<T>,
    buffer_settings: BufferSettings,
    original_operation: Option<NativeUnaryEncodingPlan>,
    original_encoded: bool,
}

/// This first-party header lives inside Tonic's encoded Body pointee,
/// separately from the original Apply future's inline codec value.
pub fn original_operation_encoder_layout() -> std::alloc::Layout {
    // Both Apply methods use this same encoder with only a zero-sized T marker.
    std::alloc::Layout::new::<NativeProstEncoder<proto::ApplyTaskOperationsRequest>>()
}

impl<T> Default for NativeProstEncoder<T> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
            buffer_settings: BufferSettings::default(),
            original_operation: None,
            original_encoded: false,
        }
    }
}

impl<T: Message> Encoder for NativeProstEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        destination: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        if let Some(plan) = self.original_operation {
            if self.original_encoded || item.encoded_len() != plan.message_bytes {
                return Err(Status::internal(
                    "original operation encoding invoice changed",
                ));
            }
            self.original_encoded = true;
        }
        item.encode(destination)
            .expect("Message only errors if not enough space");
        Ok(())
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

#[derive(Debug, Clone)]
pub struct NativeProstDecoder<U> {
    operation_reply_plan: Option<resource_preflight::OperationReplyDecodePlan>,
    marker: PhantomData<U>,
    buffer_settings: BufferSettings,
}

impl<U> Default for NativeProstDecoder<U> {
    fn default() -> Self {
        Self {
            operation_reply_plan: None,
            marker: PhantomData,
            buffer_settings: BufferSettings::default(),
        }
    }
}

impl<U> Decoder for NativeProstDecoder<U>
where
    U: Message + Default + Send + 'static,
{
    type Item = U;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        // Tonic has assembled one frame, but prost has not allocated its tree.
        // DecodeBuf is backed by a contiguous BytesMut. If that invariant ever
        // changes, fail closed instead of checking only a prefix of the frame.
        let raw = source.chunk();
        if raw.len() != source.remaining() {
            return Err(Status::internal("native codec received a split frame"));
        }
        check_resource_shape::<U>(raw)?;
        if TypeId::of::<U>() == TypeId::of::<proto::ApplyTaskOperationsResponse>()
            && let Some(plan) = self.operation_reply_plan
        {
            plan.check(raw).map_err(operation_reply_failure)?;
        }
        U::decode(source)
            .map(Some)
            .map_err(|error| Status::internal(error.to_string()))
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.buffer_settings
    }
}

/// A local reply preflight failure is not a remote ingress refusal and does
/// not prove that the original mutation was not applied.
#[derive(Debug)]
struct OperationReplyPreflightFailure(resource_preflight::ResourcePreflightError);
impl std::fmt::Display for OperationReplyPreflightFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}
impl std::error::Error for OperationReplyPreflightFailure {}

/// Checks the original local source, never a remote status message or marker.
pub fn is_operation_reply_preflight_failure(status: &Status) -> bool {
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        if error.is::<OperationReplyPreflightFailure>() {
            return true;
        }
        source = error.source();
    }
    false
}

fn operation_reply_failure(error: resource_preflight::ResourcePreflightError) -> Status {
    let mut status = Status::data_loss("Native operation reply exceeds its paid backing plan");
    status.set_source(Arc::new(OperationReplyPreflightFailure(error)));
    status
}

fn check_resource_shape<U: 'static>(raw: &[u8]) -> Result<(), Status> {
    if TypeId::of::<U>() == TypeId::of::<proto::ApplyTaskOperationsResponse>() {
        return resource_preflight::check_operation_reply(raw).map_err(|error| {
            let mut status =
                Status::data_loss("Native operation reply exceeds its resource grammar");
            status.set_source(Arc::new(OperationReplyPreflightFailure(error)));
            status
        });
    }
    let result = if TypeId::of::<U>() == TypeId::of::<proto::ApplyTaskOperationsRequest>() {
        resource_preflight::check_operation_batch(raw)
    } else if TypeId::of::<U>() == TypeId::of::<proto::ApplyTaskControlOperationsRequest>() {
        resource_preflight::check_control_operation_batch(raw)
    } else if TypeId::of::<U>() == TypeId::of::<proto::SubscribeTaskStatusRequest>() {
        resource_preflight::check_status_subscription(raw)
    } else {
        Ok(())
    };
    result.map_err(|error| Status::resource_exhausted(format!("native codec preflight: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn original_unary_plan_encodes_a_real_frame_and_keeps_its_backing_after_body_exit() {
        use hyper::body::Body;
        use tonic::codec::EncodeBody;
        let request = proto::ApplyTaskOperationsRequest {
            operations: vec![proto::TaskOperation {
                envelope: Some(proto::TaskOperationEnvelope {
                    operation_id: Some(proto::TaskOperationId {
                        value: vec![0x71; 16],
                    }),
                    max_wait_millis: 1000,
                }),
                operation: Some(proto::task_operation::Operation::CreateTask(
                    proto::CreateTaskRequest {
                        frozen_fragment: bytes::Bytes::from(vec![0x31; 9000]),
                        creation_metadata: bytes::Bytes::from(vec![0x42; 128]),
                    },
                )),
            }],
        };
        let expected = request.encode_to_vec();
        let plan = NativeUnaryEncodingPlan::for_original_operation(expected.len()).unwrap();
        assert_eq!(plan.retained_bytes(), expected.len() + 5);
        let reply = resource_preflight::OperationReplyDecodePlan::for_transport(
            novarocks_task_codec::TransportBudget::DEFAULT,
        );
        let mut codec = NativeProstCodec::<
            proto::ApplyTaskOperationsRequest,
            proto::ApplyTaskOperationsResponse,
        >::with_original_operation_plans(reply, plan);
        let encoder = codec.encoder();
        let source = tokio_stream::iter([Ok::<_, Status>(request)]);
        let mut body = Box::pin(EncodeBody::new_client(
            encoder,
            source,
            None,
            Some(expected.len()),
        ));
        let frame = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx))
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(frame.len(), plan.retained_bytes());
        assert_eq!(&frame[5..], expected.as_slice());
        let retained = frame.clone();
        let pointer = retained.as_ptr();
        drop(body);
        drop(frame);
        assert_eq!(retained.as_ptr(), pointer);
        assert_eq!(&retained[5..], expected.as_slice());
        assert_eq!(
            proto::ApplyTaskOperationsRequest::decode(retained.slice(5..))
                .unwrap()
                .operations
                .len(),
            1
        );
        drop(retained);
    }

    #[tokio::test]
    async fn original_unary_plan_rejects_a_different_message_before_any_encoding_growth() {
        use hyper::body::Body;
        use tonic::codec::EncodeBody;
        let plan = NativeUnaryEncodingPlan::for_original_operation(1).unwrap();
        let reply = resource_preflight::OperationReplyDecodePlan::for_transport(
            novarocks_task_codec::TransportBudget::DEFAULT,
        );
        let mut codec = NativeProstCodec::<
            proto::ApplyTaskOperationsRequest,
            proto::ApplyTaskOperationsResponse,
        >::with_original_operation_plans(reply, plan);
        let request = proto::ApplyTaskOperationsRequest {
            operations: vec![proto::TaskOperation::default(); 32],
        };
        let mut body = Box::pin(EncodeBody::new_client(
            codec.encoder(),
            tokio_stream::iter([Ok::<_, Status>(request)]),
            None,
            None,
        ));
        let error = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        // The original unary caller drops a failed body; it does not repoll
        // a failed encoder or publish any partial gRPC frame.
        drop(body);
    }

    #[test]
    fn decoder_dispatches_resource_checks_by_message_type() {
        let raw = proto::ApplyTaskOperationsRequest {
            operations: vec![proto::TaskOperation::default(); 33],
        }
        .encode_to_vec();
        assert_eq!(
            check_resource_shape::<proto::ApplyTaskOperationsRequest>(&raw)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert!(check_resource_shape::<proto::HeartbeatRequest>(&raw).is_ok());

        let control = proto::ApplyTaskControlOperationsRequest {
            operations: vec![proto::TaskControlOperation::default(); 33],
        }
        .encode_to_vec();
        assert_eq!(
            check_resource_shape::<proto::ApplyTaskControlOperationsRequest>(&control)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
    }
}

#[cfg(test)]
#[path = "native_codec/reply_tests.rs"]
mod reply_tests;
