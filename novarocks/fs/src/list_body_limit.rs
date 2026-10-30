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

use futures::stream;
use http::{Request, Response};
use opendal::raw::{Operation, oio::ReadStream};
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter};

/// Per-response list body profile, independent of generic object reads.
/// XML decoding and transport allocations remain third-party growth.
pub(crate) const OBJECT_STORE_LIST_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// Typed cause survives OpenDAL layering without classifying by diagnostics.
#[derive(Debug)]
pub(crate) struct ListBodyLimitExceeded;

impl std::fmt::Display for ListBodyLimitExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("list response body byte limit exceeded")
    }
}

impl std::error::Error for ListBodyLimitExceeded {}

pub(crate) fn is_list_body_limit_exceeded(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if error.downcast_ref::<ListBodyLimitExceeded>().is_some() {
            return true;
        }
        cause = error.source();
    }
    false
}

/// Wrap OpenDAL's existing transport through its public HTTP seam.
/// Signing credential acquisition keeps its separate reqwest client.
pub(crate) struct ListBodyLimitFetch {
    inner: HttpTransporter,
    limit: usize,
}

impl ListBodyLimitFetch {
    pub(crate) fn new(inner: HttpTransporter, limit: usize) -> Self {
        Self { inner, limit }
    }
}

impl HttpTransport for ListBodyLimitFetch {
    async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let is_list = request.extensions().get::<Operation>() == Some(&Operation::List);
        let response = self.inner.fetch(request).await?;
        if !is_list {
            return Ok(response);
        }
        let (parts, body) = response.into_parts();
        let limit = self.limit;
        // Count actual bytes rather than trusting Content-Length. The inner
        // body retains OpenDAL's original content-length validation. Reject
        // the offending chunk before SDK read_all/XML can retain or decode it.
        let stream = stream::try_unfold((body, 0usize), move |(mut body, consumed)| async move {
            let chunk = body.read().await?;
            if chunk.is_empty() {
                return Ok(None);
            }
            let total = consumed
                .checked_add(chunk.len())
                .filter(|total| *total <= limit)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "list response body byte limit exceeded",
                    )
                    .with_operation("ListBodyLimitFetch::read")
                    .with_context("limit", limit.to_string())
                    .set_source(ListBodyLimitExceeded)
                })?;
            Ok(Some((chunk, (body, total))))
        });
        // No temporary flag: the RetryLayer above this fetcher must not
        // repeatedly request an oversized page.
        Ok(Response::from_parts(
            parts,
            HttpBody::new(Box::pin(stream), None),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::stream;
    use http::{Request, Response};
    use opendal::layers::{ConcurrentLimitLayer, RetryLayer, TimeoutLayer};
    use opendal::raw::Operation;
    use opendal::{Buffer, HttpBody, HttpTransport, HttpTransporter, Operator};

    use super::{ListBodyLimitFetch, is_list_body_limit_exceeded};

    const XML: &[u8] = b"<ListBucketResult><Name>bucket</Name><Prefix></Prefix><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>";

    #[derive(Clone)]
    struct RecordingFetch {
        body: &'static [u8],
        operations: Arc<Mutex<Vec<Option<Operation>>>>,
    }

    impl HttpTransport for RecordingFetch {
        async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            self.operations
                .lock()
                .unwrap()
                .push(request.extensions().get::<Operation>().copied());
            // Deliberately omit Content-Length: the bound must count bytes,
            // including a response delivered in several transport chunks.
            let chunks = self
                .body
                .chunks(13)
                .map(|chunk| Ok(Buffer::from(chunk)))
                .collect::<Vec<_>>();
            Ok(Response::builder()
                .status(200)
                .header("x-amz-request-id", "bounded-test")
                .body(HttpBody::new(stream::iter(chunks), None))
                .unwrap())
        }
    }

    fn operator(body: &'static [u8], cap: usize) -> (Operator, Arc<Mutex<Vec<Option<Operation>>>>) {
        let operations = Arc::new(Mutex::new(Vec::new()));
        let fetch = ListBodyLimitFetch::new(
            HttpTransporter::new(RecordingFetch {
                body,
                operations: Arc::clone(&operations),
            }),
            cap,
        );
        // Use a remote endpoint: replacing the public HttpTransport works for
        // every S3 endpoint and does not depend on localhost detection.
        let operator = Operator::new(
            opendal::services::S3::default()
                .endpoint("http://object-store.example.test")
                .bucket("bucket")
                .region("us-east-1")
                .access_key_id("key")
                .secret_access_key("secret"),
        )
        .unwrap();
        let context = operator
            .base_context()
            .with_http_transport(HttpTransporter::new(fetch));
        let operator = operator
            .with_context(context)
            .layer(TimeoutLayer::new().with_timeout(Duration::from_secs(5)))
            .layer(ConcurrentLimitLayer::new(1).with_http_concurrent_limit(1))
            .layer(
                RetryLayer::new()
                    .with_max_times(3)
                    .with_min_delay(Duration::from_millis(1)),
            );
        (operator, operations)
    }

    #[tokio::test]
    async fn list_body_limit_public_http_fetch_layer_operation_pin() {
        let (operator, operations) = operator(XML, XML.len());
        assert!(operator.list("").await.unwrap().is_empty());
        assert_eq!(*operations.lock().unwrap(), vec![Some(Operation::List)]);
    }

    #[tokio::test]
    async fn list_body_limit_overflow_is_not_temporary_and_retry_does_not_repeat() {
        let (operator, operations) = operator(XML, XML.len() - 1);
        let error = operator.list("").await.unwrap_err();
        assert!(!error.is_temporary(), "{error:?}");
        assert!(is_list_body_limit_exceeded(&error));
        assert!(
            error
                .to_string()
                .contains("list response body byte limit exceeded"),
            "{error:?}"
        );
        assert_eq!(*operations.lock().unwrap(), vec![Some(Operation::List)]);
    }

    #[tokio::test]
    async fn list_body_limit_does_not_limit_non_list_read() {
        let (operator, operations) = operator(XML, 1);
        assert_eq!(
            operator.read("payload").await.unwrap().to_bytes().as_ref(),
            XML
        );
        assert_eq!(*operations.lock().unwrap(), vec![Some(Operation::Read)]);
    }

    #[tokio::test]
    async fn list_body_limit_preserves_unknown_operation_response() {
        let operations = Arc::new(Mutex::new(Vec::new()));
        let fetch = ListBodyLimitFetch::new(
            HttpTransporter::new(RecordingFetch {
                body: XML,
                operations: Arc::clone(&operations),
            }),
            1,
        );
        let mut response = fetch
            .fetch(
                Request::builder()
                    .uri("http://example.test")
                    .body(Buffer::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()["x-amz-request-id"], "bounded-test");
        assert_eq!(
            response
                .body_mut()
                .to_buffer()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            XML
        );
        assert_eq!(*operations.lock().unwrap(), vec![None]);
    }
}
