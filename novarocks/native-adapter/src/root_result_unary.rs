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

//! Post-admission unary root encoding and physical DATA ownership.
//!
//! This helper installs no RPC. Its caller must own bounded pre-decode
//! lane/stream/request/header/future resources. Closed and refused routes use
//! that control capacity. Tonic allocates the encoding backing as transport
//! headroom. The response body and H2 DATA aliases keep the original Root
//! delivery and metadata owner until their actual exit.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes};
use hyper::body::{Body, Frame, SizeHint};
use novarocks_execution_contract::root_result::{
    RootReadOutcome, RootReadResponse, RootResultReply,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as wire;
use novarocks_result_contract::RootProfileV1;
use novarocks_task_codec::root_result::{decode_read, encode_reply, encode_response};
use prost::Message;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::http;
use tonic::server::UnaryService;
use tonic::{Code, Request, Response, Status};

use crate::root_result_reader::{
    NativeRootReadResponse, NativeRootResultReader, NativeRootSendOwnership,
};

const GRPC_PREFIX: usize = 5;
pub(crate) const ROOT_CONTROL_FRAME_MAX_BYTES: usize = RootProfileV1::ENVELOPE_BYTES;
pub(crate) const ROOT_DATA_FRAME_MAX_BYTES: usize =
    RootProfileV1::SEGMENT_BYTES + ROOT_CONTROL_FRAME_MAX_BYTES;

struct OwnedWireReply {
    // The DTO (including the original offered alias) exits before its owner.
    message: wire::FetchRootResultResponse,
    _ownership: Option<NativeRootSendOwnership>,
}

// Design: ADR-0276 (docs/adr/ADR-0276-third-party-encoding-buffers-use-transport-headroom.md)
#[derive(Clone, Copy)]
enum RootFrameEncoding {
    Ready {
        protobuf_bytes: usize,
        framed_bytes: usize,
    },
    Empty,
    Oversized,
}

impl RootFrameEncoding {
    fn for_reply(message: &wire::FetchRootResultResponse) -> Self {
        let limit = match &message.result {
            Some(wire::fetch_root_result_response::Result::Reply(wire::RootReadReceipt {
                outcome: Some(wire::root_read_receipt::Outcome::Data(_)),
                ..
            })) => ROOT_DATA_FRAME_MAX_BYTES,
            _ => ROOT_CONTROL_FRAME_MAX_BYTES,
        };
        let protobuf_bytes = message.encoded_len();
        if protobuf_bytes == 0 {
            return Self::Empty;
        }
        match protobuf_bytes.checked_add(GRPC_PREFIX) {
            Some(framed_bytes) if framed_bytes <= limit => Self::Ready {
                protobuf_bytes,
                framed_bytes,
            },
            _ => Self::Oversized,
        }
    }

    fn allocation_bytes(self) -> usize {
        match self {
            Self::Ready { framed_bytes, .. } => framed_bytes,
            Self::Empty | Self::Oversized => GRPC_PREFIX,
        }
    }

    fn protobuf_bytes(self) -> Result<usize, Status> {
        match self {
            Self::Ready { protobuf_bytes, .. } => Ok(protobuf_bytes),
            Self::Empty => Err(Status::internal("empty root reply protobuf")),
            Self::Oversized => Err(Status::resource_exhausted(
                "root reply exceeds fixed send envelope",
            )),
        }
    }
}

struct RootReplyHandoff {
    encoding: RootFrameEncoding,
    ownership: Option<NativeRootSendOwnership>,
}

/// Root-owned reply metadata covers the DATA wrapper and the DTO's backend UUID.
/// Tonic owns its encoding body, shared header and byte backing.
pub fn native_root_unary_metadata_bytes() -> usize {
    novarocks_worker::guarded_bytes::owner_wrapper_bytes::<Bytes, NativeRootSendOwnership>()
        .checked_add(16)
        .expect("fixed root unary metadata fits usize")
}

/// Actual Tonic unary decode/call/encode, followed in the same poll by a
/// concrete body handoff. No await can expose the intermediate BoxBody after
/// the admitted service returns. The finite lane owner is supplied by the
/// eventual listener, not manufactured from a late root admission here.
pub async fn root_result_unary<B>(
    reader: &NativeRootResultReader,
    request: http::Request<B>,
) -> http::Response<NativeRootUnaryBody>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send,
{
    let handoff = Mutex::new(None);
    // Pin the Darwin lazy mutex allocation before giving shared references
    // to codec/service. This pre-admission allocation belongs to the lane.
    drop(handoff.lock().expect("root unary handoff lock"));
    let codec = RootCodec { handoff: &handoff };
    let service = RootService {
        reader,
        handoff: &handoff,
    };
    // No compression is enabled: there is one independently allocated
    // encoder buffer, not an additional uncompression buffer or reencode.
    let mut grpc = tonic::server::Grpc::new(codec)
        .max_decoding_message_size(RootProfileV1::ENVELOPE_BYTES)
        .max_encoding_message_size(ROOT_DATA_FRAME_MAX_BYTES - GRPC_PREFIX);
    let response = grpc.unary(service, request).await;
    let ownership = handoff
        .lock()
        .expect("root unary handoff lock")
        .take()
        .and_then(|handoff| handoff.ownership);
    let (parts, inner) = response.into_parts();
    http::Response::from_parts(
        parts,
        NativeRootUnaryBody {
            inner,
            ownership,
            emitted_data: false,
        },
    )
}

struct RootService<'a> {
    reader: &'a NativeRootResultReader,
    handoff: &'a Mutex<Option<RootReplyHandoff>>,
}
impl<'a> UnaryService<wire::FetchRootResultRequest> for RootService<'a> {
    type Response = OwnedWireReply;
    // This finite future allocation precedes read admission and is part of
    // the caller's lane/stream inventory, not the root's later metadata.
    type Future =
        Pin<Box<dyn Future<Output = Result<Response<Self::Response>, Status>> + Send + 'a>>;

    fn call(&mut self, request: Request<wire::FetchRootResultRequest>) -> Self::Future {
        let reader = self.reader;
        let handoff = self.handoff;
        Box::pin(async move {
            let read = decode_read(request.get_ref(), FieldPath::root("root_read"))
                .map_err(|_| Status::new(Code::InvalidArgument, "invalid frozen root read"))?;
            let (message, ownership) = match reader
                .read_with_transport_metadata(&read, native_root_unary_metadata_bytes())
                .await
            {
                NativeRootReadResponse::Owned(reply) => {
                    let ownership = reply.ownership();
                    let message = encode_reply(reply.reply())
                        .map_err(|_| Status::new(Code::Internal, "invalid owned root reply"))?;
                    (message, Some(ownership))
                }
                NativeRootReadResponse::AwaitTerminalControl { accepted_consumed } => {
                    let message = encode_reply(&RootResultReply {
                        root_task: read.root_task(),
                        profile: read.profile(),
                        kind: read.kind(),
                        accepted_consumed,
                        outcome: RootReadOutcome::AwaitTerminalControl,
                    })
                    .map_err(|_| Status::new(Code::Internal, "invalid sealed root reply"))?;
                    (message, None)
                }
                NativeRootReadResponse::Deferred(reason) => {
                    let message = encode_response(&RootReadResponse::Deferred(reason))
                        .map_err(|_| Status::internal("invalid root deferral projection"))?;
                    (message, None)
                }
                NativeRootReadResponse::Refused(reason) => {
                    let message = encode_response(&RootReadResponse::Refused(reason))
                        .map_err(|_| Status::internal("invalid root refusal projection"))?;
                    (message, None)
                }
            };
            // Projection follows the same metadata pregrant. Publish the
            // strong handoff before returning Ready to Tonic's map_response,
            // which allocates its encoding buffer before the first body poll.
            *handoff.lock().expect("root unary handoff lock") = Some(RootReplyHandoff {
                encoding: RootFrameEncoding::for_reply(&message),
                ownership: ownership.clone(),
            });
            Ok(Response::new(OwnedWireReply {
                message,
                _ownership: ownership,
            }))
        })
    }
}

struct RootCodec<'a> {
    handoff: &'a Mutex<Option<RootReplyHandoff>>,
}
impl Codec for RootCodec<'_> {
    type Encode = OwnedWireReply;
    type Decode = wire::FetchRootResultRequest;
    type Encoder = RootEncoder;
    type Decoder = RootDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        // Tonic 0.12.3 calls codec.encoder() before EncodedBytes::new.
        let handoff = self.handoff.lock().expect("root unary handoff lock");
        let reply = handoff
            .as_ref()
            .expect("successful root unary reply handoff");
        RootEncoder {
            encoding: reply.encoding,
            _ownership: reply.ownership.clone(),
        }
    }
    fn decoder(&mut self) -> Self::Decoder {
        RootDecoder
    }
}
struct RootDecoder;
impl Decoder for RootDecoder {
    type Item = wire::FetchRootResultRequest;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Message::decode(src)
            .map(Some)
            .map_err(|_| Status::new(Code::InvalidArgument, "invalid root read protobuf"))
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(RootProfileV1::ENVELOPE_BYTES, RootProfileV1::ENVELOPE_BYTES)
    }
}
struct RootEncoder {
    encoding: RootFrameEncoding,
    _ownership: Option<NativeRootSendOwnership>,
}
impl Encoder for RootEncoder {
    type Item = OwnedWireReply;
    type Error = Status;
    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        let protobuf_bytes = self.encoding.protobuf_bytes()?;
        // remaining_mut reports the growable bound, not actual capacity.
        // The single unary item has already consumed the five-byte prefix;
        // only a nonempty reply can inspect the preallocated remainder.
        if dst.chunk_mut().len() < protobuf_bytes {
            return Err(Status::new(
                Code::Internal,
                "root encoder lost its preallocated backing",
            ));
        }
        item.message
            .encode(dst)
            .map_err(|_| Status::new(Code::Internal, "root protobuf encoding failed"))
    }
    fn buffer_settings(&self) -> BufferSettings {
        let framed_bytes = self.encoding.allocation_bytes();
        #[cfg(test)]
        tests::arm_prefix_probe(framed_bytes);
        BufferSettings::new(framed_bytes, framed_bytes)
    }
}

/// Keep this concrete value through the framework's body handoff. Field
/// order drops Tonic's inner Box/buffer before the independent send owner.
/// Every owned DATA frame retains that owner through all Bytes slices.
pub struct NativeRootUnaryBody {
    inner: tonic::body::BoxBody,
    ownership: Option<NativeRootSendOwnership>,
    emitted_data: bool,
}
impl Body for NativeRootUnaryBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => {
                    if this.emitted_data {
                        return Poll::Ready(Some(Err(Status::new(
                            Code::Internal,
                            "root unary emitted multiple DATA backings",
                        ))));
                    }
                    this.emitted_data = true;
                    let data = if let Some(owner) = &this.ownership {
                        novarocks_worker::guarded_bytes::bytes_with_exit_guard(data, owner.clone())
                    } else {
                        data
                    };
                    Poll::Ready(Some(Ok(Frame::data(data))))
                }
                Err(frame) => Poll::Ready(Some(Ok(frame))),
            },
            result => result,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::future::{Ready, poll_fn, ready};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum PrefixBacking {
        Disabled,
        WaitingForSettings,
        WaitingForBacking,
        Live(usize),
        Exited,
        Grew,
    }

    // Tonic creates the backing immediately after asking for BufferSettings.
    // Follow that address through polling even when error handling allocates
    // other values of the same size.
    thread_local! {
        static PREFIX_BACKING: Cell<PrefixBacking> = const { Cell::new(PrefixBacking::Disabled) };
    }

    pub(super) fn arm_prefix_probe(framed_bytes: usize) {
        PREFIX_BACKING.with(|backing| {
            if backing.get() == PrefixBacking::WaitingForSettings && framed_bytes == GRPC_PREFIX {
                backing.set(PrefixBacking::WaitingForBacking);
            }
        });
    }

    struct PrefixAllocationProbe;

    #[global_allocator]
    static ALLOCATOR: PrefixAllocationProbe = PrefixAllocationProbe;

    unsafe impl GlobalAlloc for PrefixAllocationProbe {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if layout.size() == GRPC_PREFIX && !pointer.is_null() {
                let _ = PREFIX_BACKING.try_with(|backing| {
                    if backing.get() == PrefixBacking::WaitingForBacking {
                        backing.set(PrefixBacking::Live(pointer as usize));
                    }
                });
            }
            pointer
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            let _ = PREFIX_BACKING.try_with(|backing| {
                if backing.get() == PrefixBacking::Live(pointer as usize) {
                    backing.set(PrefixBacking::Exited);
                }
            });
            unsafe { System.dealloc(pointer, layout) };
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let _ = PREFIX_BACKING.try_with(|backing| {
                if backing.get() == PrefixBacking::Live(pointer as usize) {
                    backing.set(PrefixBacking::Grew);
                }
            });
            unsafe { System.realloc(pointer, layout, size) }
        }
    }

    fn data_message(body_bytes: usize) -> wire::FetchRootResultResponse {
        wire::FetchRootResultResponse {
            result: Some(wire::fetch_root_result_response::Result::Reply(
                wire::RootReadReceipt {
                    outcome: Some(wire::root_read_receipt::Outcome::Data(
                        novarocks_proto_models::result::RootData {
                            body: Bytes::from(vec![0; body_bytes]),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    fn control_message(detail_bytes: usize) -> wire::FetchRootResultResponse {
        wire::FetchRootResultResponse {
            result: Some(wire::fetch_root_result_response::Result::Refused(
                wire::RootReadRefusal {
                    safe_detail: "x".repeat(detail_bytes),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    // The seed lengths straddle neither protobuf length-varint boundary.
    fn framed_message(
        build: fn(usize) -> wire::FetchRootResultResponse,
        framed_bytes: usize,
    ) -> wire::FetchRootResultResponse {
        let seed = framed_bytes - 32;
        let framed_seed = build(seed).encoded_len() + GRPC_PREFIX;
        let message = build(seed + (framed_bytes - framed_seed));
        assert_eq!(message.encoded_len() + GRPC_PREFIX, framed_bytes);
        message
    }

    struct SyntheticReply(Option<OwnedWireReply>);

    impl UnaryService<wire::FetchRootResultRequest> for SyntheticReply {
        type Response = OwnedWireReply;
        type Future = Ready<Result<Response<Self::Response>, Status>>;

        fn call(&mut self, _: Request<wire::FetchRootResultRequest>) -> Self::Future {
            ready(Ok(Response::new(self.0.take().unwrap())))
        }
    }

    async fn encode_synthetic(
        message: wire::FetchRootResultResponse,
        encoding: RootFrameEncoding,
    ) -> tonic::body::BoxBody {
        let handoff = Mutex::new(Some(RootReplyHandoff {
            encoding,
            ownership: None,
        }));
        let mut grpc = tonic::server::Grpc::new(RootCodec { handoff: &handoff })
            .max_encoding_message_size(ROOT_DATA_FRAME_MAX_BYTES - GRPC_PREFIX);
        // The synthetic service supplies the reply independent of the request.
        let request = http::Request::new(axum::body::Body::from(Bytes::from_static(
            &[0; GRPC_PREFIX],
        )));
        let reply = SyntheticReply(Some(OwnedWireReply {
            message,
            _ownership: None,
        }));
        grpc.unary(reply, request).await.into_body()
    }

    #[tokio::test]
    async fn exact_data_and_control_limits_encode_and_oversize_keeps_tonic_trailers() {
        for (build, max_bytes) in [
            (
                data_message as fn(usize) -> wire::FetchRootResultResponse,
                ROOT_DATA_FRAME_MAX_BYTES,
            ),
            (control_message, ROOT_CONTROL_FRAME_MAX_BYTES),
        ] {
            let message = framed_message(build, max_bytes);
            let encoding = RootFrameEncoding::for_reply(&message);
            assert_eq!(encoding.allocation_bytes(), max_bytes);
            let mut body = encode_synthetic(message, encoding).await;
            let data = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap();
            assert_eq!(data.len(), max_bytes);
            let trailers = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .unwrap()
                .into_trailers()
                .unwrap();
            assert_eq!(trailers["grpc-status"], "0");

            assert_tonic_rejection(
                framed_message(build, max_bytes + 1),
                Code::ResourceExhausted,
            )
            .await;
        }
    }

    async fn assert_tonic_rejection(message: wire::FetchRootResultResponse, local_code: Code) {
        let encoding = RootFrameEncoding::for_reply(&message);
        assert_eq!(encoding.protobuf_bytes().unwrap_err().code(), local_code);
        assert_eq!(encoding.allocation_bytes(), GRPC_PREFIX);
        PREFIX_BACKING.with(|backing| backing.set(PrefixBacking::WaitingForSettings));
        let mut body = encode_synthetic(message, encoding).await;
        assert!(matches!(
            PREFIX_BACKING.with(Cell::get),
            PrefixBacking::Live(_)
        ));
        let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        let final_backing = PREFIX_BACKING.with(|backing| backing.replace(PrefixBacking::Disabled));
        assert!(
            matches!(
                final_backing,
                PrefixBacking::Live(_) | PrefixBacking::Exited
            ),
            "prefix backing changed during encoding: {final_backing:?}"
        );
        let trailers = frame
            .unwrap()
            .unwrap()
            .into_trailers()
            .expect("no DATA on error");
        assert_eq!(trailers["grpc-status"], "13");
        assert!(body.is_end_stream());
    }

    #[tokio::test]
    async fn empty_protobuf_never_grows_the_prefix_only_backing() {
        assert_tonic_rejection(wire::FetchRootResultResponse::default(), Code::Internal).await;
    }
}
