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

use std::convert::Infallible;
use std::future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::TryStreamExt;
use http::{Request, Response};
use http_body::{Frame, SizeHint};
use opendal::raw::{parse_content_encoding, parse_content_length};
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport};

/// Preserve the original reqwest 0.12 bridge at OpenDAL's public transport seam.
#[derive(Clone)]
pub(crate) struct Reqwest012Transport {
    client: reqwest::Client,
}

impl Reqwest012Transport {
    pub(crate) fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl HttpTransport for Reqwest012Transport {
    async fn fetch(&self, req: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let uri = req.uri().clone();
        let is_head = req.method() == http::Method::HEAD;
        let (parts, body) = req.into_parts();
        let url = reqwest::Url::parse(&uri.to_string()).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "request url is invalid")
                .with_operation("http_util::Client::send::fetch")
                .with_context("url", uri.to_string())
                .set_source(err)
        })?;
        let mut request = self
            .client
            .request(parts.method, url)
            .headers(parts.headers);
        #[cfg(not(target_arch = "wasm32"))]
        {
            request = request.version(parts.version);
        }
        if !body.is_empty() {
            #[cfg(not(target_arch = "wasm32"))]
            {
                request = request.body(reqwest::Body::wrap(HttpBufferBody::bridge(body)));
            }
            #[cfg(target_arch = "wasm32")]
            {
                request = request.body(reqwest::Body::from(body.to_bytes()));
            }
        }
        let mut response = request.send().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "send http request")
                .with_operation("http_util::Client::send")
                .with_context("url", uri.to_string())
                .with_temporary(is_temporary_error(&err))
                .set_source(err)
        })?;
        let content_length = if is_head || parse_content_encoding(response.headers())?.is_some() {
            None
        } else {
            parse_content_length(response.headers())?
        };
        let status = response.status();
        #[cfg(not(target_arch = "wasm32"))]
        let version = response.version();
        let headers = mem::take(response.headers_mut());
        let response_uri = uri.clone();
        let stream = response
            .bytes_stream()
            .try_filter(|chunk| future::ready(!chunk.is_empty()))
            .map_ok(Buffer::from)
            .map_err(move |err| {
                Error::new(ErrorKind::Unexpected, "read data from http response")
                    .with_operation("http_util::Client::send")
                    .with_context("url", uri.to_string())
                    .with_temporary(is_temporary_error(&err))
                    .set_source(err)
            });
        let mut response = Response::new(HttpBody::new(stream, content_length));
        *response.status_mut() = status;
        #[cfg(not(target_arch = "wasm32"))]
        {
            *response.version_mut() = version;
        }
        *response.headers_mut() = headers;
        response.extensions_mut().insert(response_uri);
        Ok(response)
    }
}

fn is_temporary_error(error: &reqwest::Error) -> bool {
    error.is_request() || error.is_body() || error.is_decode()
}

struct HttpBufferBody {
    data: Buffer,
}

impl HttpBufferBody {
    fn bridge(data: Buffer) -> Self {
        Self { data }
    }
}

impl http_body::Body for HttpBufferBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.data.next() {
            Some(bytes) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
            None => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.data.len() as u64)
    }
}
