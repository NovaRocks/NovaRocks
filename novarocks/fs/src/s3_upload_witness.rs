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

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use http::{Method, Request, Response, StatusCode};
use novarocks_spi::connector::ConnectorRequestContext;
use opendal::raw::oio::ReadStream;
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter, Operator};

const MAX_MULTIPART_RESPONSE_BYTES: usize = 64 * 1024;

/// One writer's exact upload exit evidence from the original signed S3 transport.
#[derive(Clone, Default)]
pub struct S3UploadWitness(Arc<Mutex<UploadState>>);

#[derive(Default)]
struct UploadState {
    possible_initiations: usize,
    uploads: BTreeSet<String>,
    abort_context: Option<ConnectorRequestContext>,
}

impl S3UploadWitness {
    pub(crate) fn install(&self, operator: &Operator) -> Operator {
        let base = operator.base_context();
        let context = base.with_http_transport(HttpTransporter::new(UploadTransport {
            inner: base.http_transport().clone(),
            witness: self.clone(),
        }));
        operator.clone().with_context(context)
    }

    pub fn install_abort_context(&self, cleanup: ConnectorRequestContext) {
        self.0.lock().unwrap().abort_context = Some(cleanup);
    }

    pub fn check_exit(&self) -> opendal::Result<()> {
        let state = self.0.lock().unwrap();
        if state.possible_initiations == 0 && state.uploads.is_empty() {
            return Ok(());
        }
        Err(Error::new(
            ErrorKind::Unexpected,
            "S3 multipart upload exit remains unproven",
        ))
    }
}

struct UploadTransport {
    inner: HttpTransporter,
    witness: S3UploadWitness,
}

enum MultipartRequest {
    Initiate,
    Complete(String),
    Abort(String),
}

impl MultipartRequest {
    fn from_http(request: &Request<Buffer>) -> Option<Self> {
        if !matches!(*request.method(), Method::POST | Method::DELETE) {
            return None;
        }
        let query = request.uri().query()?;
        let mut upload_id = None;
        let mut initiate = false;
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "uploads" if value.is_empty() => initiate = true,
                "uploadId" if !value.is_empty() => upload_id = Some(value.into_owned()),
                _ => {}
            }
        }
        match (request.method(), initiate, upload_id) {
            (&Method::POST, true, None) => Some(Self::Initiate),
            (&Method::POST, false, Some(id)) => Some(Self::Complete(id)),
            (&Method::DELETE, false, Some(id)) => Some(Self::Abort(id)),
            _ => None,
        }
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct MultipartResponse {
    #[serde(default)]
    upload_id: String,
    #[serde(default, rename = "ETag")]
    etag: String,
    #[serde(default)]
    code: String,
}

impl HttpTransport for UploadTransport {
    async fn fetch(&self, mut request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let Some(operation) = MultipartRequest::from_http(&request) else {
            return self.inner.fetch(request).await;
        };
        {
            let mut state = self.witness.0.lock().unwrap();
            match &operation {
                MultipartRequest::Initiate => {
                    state.possible_initiations =
                        state.possible_initiations.checked_add(1).ok_or_else(|| {
                            Error::new(ErrorKind::Unexpected, "S3 upload attempt overflow")
                        })?;
                }
                MultipartRequest::Abort(_) => {
                    if let Some(cleanup) = &state.abort_context {
                        request.extensions_mut().insert(cleanup.clone());
                    }
                }
                MultipartRequest::Complete(_) => {}
            }
        }
        let response = self.inner.fetch(request).await?;
        let status = response.status();
        if let MultipartRequest::Abort(id) = &operation
            && status == StatusCode::NO_CONTENT
        {
            self.witness.0.lock().unwrap().uploads.remove(id);
            return Ok(response);
        }
        // The SDK also parses these XML responses. Preserve the exact bounded
        // body, while retaining the distinction between a generic 404 and
        // NoSuchUpload for the requested upload ID.
        let inspect = matches!(
            (&operation, status),
            (MultipartRequest::Initiate, StatusCode::OK)
                | (
                    MultipartRequest::Complete(_),
                    StatusCode::OK | StatusCode::NOT_FOUND
                )
                | (MultipartRequest::Abort(_), StatusCode::NOT_FOUND)
        );
        if !inspect {
            return Ok(response);
        }
        let (parts, mut body) = response.into_parts();
        let mut bytes = Vec::new();
        loop {
            let next = body.read().await?;
            if next.is_empty() {
                break;
            }
            let total = bytes
                .len()
                .checked_add(next.len())
                .filter(|size| *size <= MAX_MULTIPART_RESPONSE_BYTES)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "S3 multipart response exceeds the byte limit",
                    )
                })?;
            bytes.try_reserve(total - bytes.len()).map_err(|cause| {
                Error::new(ErrorKind::Unexpected, "reserve S3 multipart response").set_source(cause)
            })?;
            for slice in next {
                bytes.extend_from_slice(&slice);
            }
        }
        if let Ok(value) = quick_xml::de::from_reader::<_, MultipartResponse>(bytes.as_slice()) {
            let mut state = self.witness.0.lock().unwrap();
            match operation {
                MultipartRequest::Initiate
                    if !value.upload_id.is_empty() && value.code.is_empty() =>
                {
                    state.possible_initiations -= 1;
                    state.uploads.insert(value.upload_id);
                }
                MultipartRequest::Complete(id)
                    if status == StatusCode::OK
                        && value.code.is_empty()
                        && !value.etag.is_empty() =>
                {
                    state.uploads.remove(&id);
                }
                MultipartRequest::Complete(id) | MultipartRequest::Abort(id)
                    if status == StatusCode::NOT_FOUND && value.code == "NoSuchUpload" =>
                {
                    state.uploads.remove(&id);
                }
                _ => {}
            }
        }
        let size = bytes.len() as u64;
        Ok(Response::from_parts(
            parts,
            HttpBody::new(futures::stream::iter([Ok(Buffer::from(bytes))]), Some(size)),
        ))
    }
}
