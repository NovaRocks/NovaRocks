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
use axum::Json;
use axum::body::Bytes;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::service::stream_load::{self, HttpHeaders};

fn normalize_headers(headers: &HeaderMap) -> HttpHeaders {
    let mut output = HttpHeaders::new();
    for (name, value) in headers {
        if let Ok(value) = value.to_str() {
            output.insert(name.as_str().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    output
}

pub(crate) async fn handle_stream_load(
    Path((db, table)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Stream load execution is synchronous and can block for seconds; run it in
    // Tokio's blocking section so Starlet heartbeat RPCs stay responsive.
    let response = tokio::task::block_in_place(|| {
        stream_load::handle_stream_load(db, table, normalize_headers(&headers), body.to_vec())
    });
    (StatusCode::OK, Json(response))
}

pub(crate) async fn handle_transaction_load(headers: HeaderMap, body: Bytes) -> impl IntoResponse {
    let response = tokio::task::block_in_place(|| {
        stream_load::handle_transaction_load(normalize_headers(&headers), body.to_vec())
    });
    (StatusCode::OK, Json(response))
}

pub(crate) async fn handle_transaction_op(
    Path(txn_op): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let response = tokio::task::block_in_place(|| {
        stream_load::handle_transaction_op(txn_op, normalize_headers(&headers))
    });
    (StatusCode::OK, Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderName, HeaderValue};

    #[test]
    fn normalize_headers_lowercases_and_trims() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-test-header"),
            HeaderValue::from_static("  value  "),
        );
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Basic dGVzdDp0ZXN0"),
        );

        let normalized = normalize_headers(&headers);
        assert_eq!(
            normalized.get("x-test-header").map(String::as_str),
            Some("value")
        );
        assert_eq!(
            normalized.get("authorization").map(String::as_str),
            Some("Basic dGVzdDp0ZXN0")
        );
    }
}
