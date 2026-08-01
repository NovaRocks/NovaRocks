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
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post, put};
use serde_json::json;

use novarocks_types::QueryId;

use super::service::{
    CompatLoadService, HttpHeaders, handle_stream_load, handle_transaction_load,
    handle_transaction_op,
};
use super::tracking::LoadTrackingStore;

fn normalize_headers(headers: &HeaderMap) -> HttpHeaders {
    let mut output = HttpHeaders::new();
    for (name, value) in headers {
        if let Ok(value) = value.to_str() {
            output.insert(name.as_str().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    output
}

pub(crate) fn router(service: Arc<CompatLoadService>, tracking: Arc<LoadTrackingStore>) -> Router {
    Router::new()
        .route("/api/:db/:table/_stream_load", put(handle_stream_load_http))
        .route("/api/transaction/load", put(handle_transaction_load_http))
        .route(
            "/api/transaction/:txn_op",
            post(handle_transaction_op_http).put(handle_transaction_op_http),
        )
        .route("/api/_load_tracking/:hi/:lo", get(handle_load_tracking_log))
        .with_state((service, tracking))
}

pub(crate) async fn handle_stream_load_http(
    State((service, _tracking)): State<(Arc<CompatLoadService>, Arc<LoadTrackingStore>)>,
    Path((db, table)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !service.is_accepting() {
        return (
            StatusCode::OK,
            Json(json!({"Status": "Fail", "Message": "stream load service is shutting down"})),
        );
    }
    // Stream load execution is synchronous and can block for seconds; run it in
    // Tokio's blocking section so Starlet heartbeat RPCs stay responsive.
    let response = tokio::task::block_in_place(|| {
        handle_stream_load(
            service.as_ref(),
            db,
            table,
            normalize_headers(&headers),
            body.to_vec(),
        )
    });
    (StatusCode::OK, Json(response))
}

pub(crate) async fn handle_transaction_load_http(
    State((service, _tracking)): State<(Arc<CompatLoadService>, Arc<LoadTrackingStore>)>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if !service.is_accepting() {
        return (
            StatusCode::OK,
            Json(json!({"Status": "Fail", "Message": "stream load service is shutting down"})),
        );
    }
    let response = tokio::task::block_in_place(|| {
        handle_transaction_load(service.as_ref(), normalize_headers(&headers), body.to_vec())
    });
    (StatusCode::OK, Json(response))
}

pub(crate) async fn handle_transaction_op_http(
    State((service, _tracking)): State<(Arc<CompatLoadService>, Arc<LoadTrackingStore>)>,
    Path(txn_op): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !service.is_accepting() {
        return (
            StatusCode::OK,
            Json(json!({"Status": "Fail", "Message": "stream load service is shutting down"})),
        );
    }
    let response = tokio::task::block_in_place(|| {
        handle_transaction_op(service.as_ref(), txn_op, normalize_headers(&headers))
    });
    (StatusCode::OK, Json(response))
}

async fn handle_load_tracking_log(
    State((_service, tracking)): State<(Arc<CompatLoadService>, Arc<LoadTrackingStore>)>,
    Path((hi, lo)): Path<(i64, i64)>,
) -> impl IntoResponse {
    let query_id = QueryId::new(hi, lo);
    match tracking.get_tracking_log(query_id) {
        Some(log) => (StatusCode::OK, format!("{log}\n")).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!(
                "tracking log is not available for query_id={:016x}:{:016x}",
                query_id.high(),
                query_id.low()
            ),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct UnusedSyncExecutor;

    impl crate::fragment::SyncFragmentExecutor for UnusedSyncExecutor {
        fn execute_encoded(&self, _payload: &[u8]) -> Result<novarocks_types::UniqueId, String> {
            Err("unexpected fragment execution".to_string())
        }
    }

    fn test_service() -> Arc<CompatLoadService> {
        Arc::new(CompatLoadService::new(
            Arc::new(super::super::registry::CompatLoadRegistry::default()),
            Arc::new(UnusedSyncExecutor),
        ))
    }

    #[test]
    fn tracking_route_preserves_success_and_not_found_bodies() {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let service = test_service();
        let tracking = Arc::new(LoadTrackingStore::default());
        let query_id = QueryId::new(41, 42);
        tracking.append_logs(query_id, ["line-1".to_string(), "line-2".to_string()]);

        let response = runtime
            .block_on(handle_load_tracking_log(
                State((Arc::clone(&service), Arc::clone(&tracking))),
                Path((41, 42)),
            ))
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let body = runtime
            .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
            .expect("read tracking response");
        assert_eq!(&body[..], b"line-1\nline-2\n");

        let response = runtime
            .block_on(handle_load_tracking_log(
                State((service, tracking)),
                Path((99, 100)),
            ))
            .into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = runtime
            .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
            .expect("read missing tracking response");
        assert_eq!(
            &body[..],
            b"tracking log is not available for query_id=0000000000000063:0000000000000064"
        );
    }
}
