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

//! Original admitted request/response/upload-frame physical tail.
use super::*;
use novarocks_resource_contract::ExistingOwnerRetentionLease;

/// Typed request extension from the already-installed original action claim.
/// It grants nothing and cannot be made from a WorkId or scalar byte limit.
#[derive(Debug)]
pub struct NativeRequestCustody {
    lease: ExistingOwnerRetentionLease,
}
impl NativeRequestCustody {
    pub fn from_retained_action(lease: ExistingOwnerRetentionLease) -> Self {
        Self { lease }
    }
}
impl Clone for NativeRequestCustody {
    fn clone(&self) -> Self {
        Self {
            lease: self
                .lease
                .try_clone()
                .expect("original Native action pin count remains representable"),
        }
    }
}
struct TailData {
    _permit: NativeLaneStreamPermit,
}
pub(super) struct TailHandle {
    // The real permit and Arc allocation leave before this value's action pin.
    data: Option<Arc<TailData>>,
    lease: Option<ExistingOwnerRetentionLease>,
}
impl Clone for TailHandle {
    fn clone(&self) -> Self {
        let lease = self
            .lease
            .as_ref()
            .unwrap()
            .try_clone()
            .expect("original Native action tail count remains representable");
        Self {
            data: Some(Arc::clone(self.data.as_ref().unwrap())),
            lease: Some(lease),
        }
    }
}
impl Drop for TailHandle {
    fn drop(&mut self) {
        let data = self.data.take();
        if let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(data))) {
            // A genuine foreign Drop unwind is not an actual-exit proof.
            std::panic::resume_unwind(Box::new(TailDropFailure {
                _cause: cause,
                _lease: self.lease.take(),
            }));
        }
        drop(self.lease.take());
    }
}
struct TailDropFailure {
    _cause: Box<dyn std::any::Any + Send>,
    _lease: Option<ExistingOwnerRetentionLease>,
}
pub(super) enum StreamCustody {
    // These original holders remain owned until this stream custody exits.
    Direct { _permit: NativeLaneStreamPermit },
    Paid { _tail: TailHandle },
}
impl StreamCustody {
    pub(super) fn install(
        permit: NativeLaneStreamPermit,
        request: &mut hyper::http::Request<BoxBody>,
    ) -> Self {
        match request.extensions_mut().remove::<NativeRequestCustody>() {
            None => Self::Direct { _permit: permit },
            Some(custody) => {
                let tail = TailHandle {
                    data: Some(Arc::new(TailData { _permit: permit })),
                    lease: Some(custody.lease),
                };
                let (parts, body) = std::mem::replace(
                    request,
                    hyper::http::Request::new(tonic::body::empty_body()),
                )
                .into_parts();
                *request = hyper::http::Request::from_parts(
                    parts,
                    tonic::body::boxed(RetainedRequestBody {
                        inner: Some(body),
                        tail: Some(tail.clone()),
                    }),
                );
                Self::Paid { _tail: tail }
            }
        }
    }
}
struct RetainedFrame {
    bytes: Bytes,
    _tail: TailHandle,
}
impl AsRef<[u8]> for RetainedFrame {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}
struct RetainedRequestBody {
    inner: Option<BoxBody>,
    tail: Option<TailHandle>,
}
impl HttpBody for RetainedRequestBody {
    type Data = Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, tonic::Status>>> {
        match Pin::new(self.inner.as_mut().expect("original request body")).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame.map_data(|bytes| {
                Bytes::from_owner(RetainedFrame {
                    bytes,
                    _tail: self.tail.as_ref().expect("original action tail").clone(),
                })
            })))),
            other => other,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(HttpBody::is_end_stream)
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner
            .as_ref()
            .map_or_else(Default::default, HttpBody::size_hint)
    }
}
impl Drop for RetainedRequestBody {
    fn drop(&mut self) {
        let inner = self.inner.take();
        if let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(inner))) {
            std::panic::resume_unwind(Box::new(RequestBodyDropFailure {
                _cause: cause,
                _tail: self.tail.take(),
            }));
        }
        drop(self.tail.take());
    }
}
struct RequestBodyDropFailure {
    _cause: Box<dyn std::any::Any + Send>,
    _tail: Option<TailHandle>,
}
/// Actual first-party requested holders. The one unary stream emits at most
/// one encoded message frame; Bytes aliases/slices share its owner allocation.
/// Shared protocol/runtime connection backing remains process-owned.
pub fn native_request_custody_layouts() -> Result<
    (std::alloc::Layout, std::alloc::Layout, std::alloc::Layout),
    novarocks_type_contract::owned_resources::layout::LayoutResourceError,
> {
    let tail =
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            TailData,
        >())?;
    let frame = std::alloc::Layout::new::<RetainedFrame>();
    Ok((
        tail,
        frame,
        std::alloc::Layout::new::<RetainedRequestBody>(),
    ))
}
