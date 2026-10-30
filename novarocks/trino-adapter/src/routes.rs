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

use crate::{body, host::base_uri, registry::TrinoQueryRegistry, request::ParsedStatementRequest};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header::HOST},
    response::{IntoResponse, Response},
    routing::post,
};
use std::sync::Arc;
use tokio::sync::Notify;

pub(crate) fn routes(registry: Arc<TrinoQueryRegistry>) -> Router {
    Router::new()
        .route(
            "/v1/statement",
            post(submit).head(|| async { StatusCode::OK }),
        )
        .route(
            "/v1/statement/executing/:id/:slug/:token",
            axum::routing::get(next).delete(cancel).head(heartbeat),
        )
        .layer(DefaultBodyLimit::disable())
        .with_state(registry)
}
async fn submit(
    State(registry): State<Arc<TrinoQueryRegistry>>,
    headers: HeaderMap,
    sql: Bytes,
) -> Response {
    let sql = match String::from_utf8(sql.to_vec()) {
        Ok(sql) => sql,
        Err(_) => return (StatusCode::BAD_REQUEST, "SQL body must be UTF-8").into_response(),
    };
    let request = match ParsedStatementRequest::parse(&headers, sql, &registry.user) {
        Ok(request) => request,
        Err(error) => return (error.status, error.message).into_response(),
    };
    // Global Host validation precedes this route, including UTF-8 and multiplicity checks.
    let Some(base) = headers
        .get(HOST)
        .and_then(|h| h.to_str().ok())
        .and_then(base_uri)
    else {
        return StatusCode::FORBIDDEN.into_response();
    };
    let page = registry.register(request, base);
    body::response(page, Arc::new(Notify::new()))
}
async fn next(
    State(registry): State<Arc<TrinoQueryRegistry>>,
    Path((id, slug, token)): Path<(String, String, String)>,
) -> Response {
    let Ok(token) = token.parse::<u64>() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(entry) = registry.lookup(&id, &slug) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match registry.get(&entry, token).await {
        Ok(page) => body::response(page, Arc::clone(&entry.progress)),
        Err(status) => status.into_response(),
    }
}
async fn cancel(
    State(registry): State<Arc<TrinoQueryRegistry>>,
    Path((id, slug, token)): Path<(String, String, String)>,
) -> StatusCode {
    if token.parse::<u64>().is_err() {
        return StatusCode::NOT_FOUND;
    }
    let Some(entry) = registry.lookup(&id, &slug) else {
        return StatusCode::NOT_FOUND;
    };
    registry.cancel(&entry).await;
    StatusCode::NO_CONTENT
}
async fn heartbeat(
    State(registry): State<Arc<TrinoQueryRegistry>>,
    Path((id, slug, token)): Path<(String, String, String)>,
) -> StatusCode {
    if token.parse::<u64>().is_err() {
        return StatusCode::NOT_FOUND;
    }
    if registry.lookup(&id, &slug).is_some() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}
