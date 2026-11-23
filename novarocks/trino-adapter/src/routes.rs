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

use crate::{
    body,
    host::base_uri,
    registry::TrinoQueryRegistry,
    request::{ParsedStatementRequest, QueryTextLimit, StatementBody},
};
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header::HOST},
    response::{IntoResponse, Response},
    routing::post,
};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Clone)]
struct StatementState {
    registry: Arc<TrinoQueryRegistry>,
    limit: QueryTextLimit,
}

pub(crate) fn routes(registry: Arc<TrinoQueryRegistry>, limit: QueryTextLimit) -> Router {
    Router::new()
        .route(
            "/v1/statement",
            post(submit).head(|| async { StatusCode::OK }),
        )
        .layer(DefaultBodyLimit::disable())
        .with_state(StatementState {
            registry: Arc::clone(&registry),
            limit,
        })
        .merge(
            Router::new()
                .route(
                    "/v1/statement/executing/:id/:slug/:token",
                    axum::routing::get(next).delete(cancel).head(heartbeat),
                )
                .with_state(registry),
        )
}

#[derive(Debug)]
enum BodyReadError {
    Transport(axum::Error),
    InvalidUtf8,
}
impl IntoResponse for BodyReadError {
    fn into_response(self) -> Response {
        match self {
            Self::Transport(error) => (
                StatusCode::BAD_REQUEST,
                format!("Failed to buffer the request body: {error}"),
            )
                .into_response(),
            Self::InvalidUtf8 => {
                (StatusCode::BAD_REQUEST, "SQL body must be UTF-8").into_response()
            }
        }
    }
}

async fn read_statement_body(
    body: Body,
    limit: QueryTextLimit,
) -> Result<StatementBody, BodyReadError> {
    let mut chunks = body.into_data_stream();
    let mut text = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(BodyReadError::Transport)?;
        if chunk.len() > limit.byte_limit() - text.len() {
            // Do not retain the offending chunk or poll another body frame.
            // Dropping this original stream closes its remaining read ownership.
            return Ok(StatementBody::ByteLimitExceeded);
        }
        text.extend_from_slice(&chunk);
    }
    String::from_utf8(text)
        .map(StatementBody::Complete)
        .map_err(|_| BodyReadError::InvalidUtf8)
}

async fn submit(State(state): State<StatementState>, headers: HeaderMap, body: Body) -> Response {
    let body = match read_statement_body(body, state.limit).await {
        Ok(body) => body,
        Err(error) => return error.into_response(),
    };
    let registry = state.registry;
    let request =
        match ParsedStatementRequest::parse_body(&headers, body, &registry.user, state.limit) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Bytes, to_bytes},
        http::HeaderValue,
    };
    use futures::Stream;
    use novarocks_query_application::{
        cancellation::QueryCancellationReason,
        session::{QuerySession, QuerySessionFactory, StatementSessionRequest},
        session_control::QueryControlService,
        session_error::{QueryServiceError, QueryServiceErrorKind},
        session_value::StatementSessionError,
    };
    use std::{
        collections::VecDeque,
        io,
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::{Context, Poll},
        time::Duration,
    };

    struct ObservedBody {
        chunks: VecDeque<Bytes>,
        polls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }
    impl Stream for ObservedBody {
        type Item = Result<Bytes, io::Error>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let count = self.polls.fetch_add(1, Ordering::SeqCst);
            assert!(
                count < 2,
                "body was polled after the chunk proving byte overflow"
            );
            Poll::Ready(self.chunks.pop_front().map(Ok))
        }
    }
    impl Drop for ObservedBody {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn byte_overflow_stops_polling_and_drops_the_original_body() {
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let body = Body::from_stream(ObservedBody {
            chunks: [
                Bytes::from_static(b"a"),
                Bytes::from_static(b"bcd"),
                Bytes::from_static(b"unread"),
            ]
            .into(),
            polls: Arc::clone(&polls),
            dropped: Arc::clone(&dropped),
        });
        assert!(matches!(
            read_statement_body(body, QueryTextLimit::new(1).unwrap())
                .await
                .unwrap(),
            StatementBody::ByteLimitExceeded
        ));
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(dropped.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn body_bound_accepts_equality_and_zero_is_finite() {
        let read = read_statement_body(Body::from("\u{0800}"), QueryTextLimit::new(1).unwrap())
            .await
            .unwrap();
        assert!(matches!(read, StatementBody::Complete(text) if text == "\u{0800}"));
        let zero = QueryTextLimit::new(0).unwrap();
        assert!(
            matches!(read_statement_body(Body::empty(), zero).await.unwrap(), StatementBody::Complete(text) if text.is_empty())
        );
        assert!(matches!(
            read_statement_body(Body::from("x"), zero).await.unwrap(),
            StatementBody::ByteLimitExceeded
        ));
    }
    #[tokio::test]
    async fn body_transport_and_utf8_failures_remain_http_rejections() {
        let limit = QueryTextLimit::new(8).unwrap();
        let invalid = read_statement_body(Body::from(vec![0xff]), limit)
            .await
            .unwrap_err();
        let response = invalid.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
            b"SQL body must be UTF-8"
        );
        let body = Body::from_stream(futures::stream::once(async {
            Err::<Bytes, _>(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "original body reset",
            ))
        }));
        let error = read_statement_body(body, limit).await.unwrap_err();
        assert!(matches!(&error, BodyReadError::Transport(_)));
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let text = String::from_utf8(to_bytes(response.into_body(), 1024).await.unwrap().to_vec())
            .unwrap();
        assert_eq!(
            text,
            "Failed to buffer the request body: original body reset"
        );
    }

    struct RefuseWorkFactory(Arc<AtomicUsize>);
    impl QuerySessionFactory for RefuseWorkFactory {
        fn open_statement_session(
            &self,
            _: StatementSessionRequest,
        ) -> Result<Arc<dyn QuerySession>, StatementSessionError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(StatementSessionError::Service(QueryServiceError::new(
                QueryServiceErrorKind::InvalidValue,
                "unexpected Session creation",
            )))
        }
        fn cancel_all(&self, _: QueryCancellationReason) {}
    }
    fn registry(opened: Arc<AtomicUsize>) -> Arc<TrinoQueryRegistry> {
        prepared_registry(Arc::new(RefuseWorkFactory(opened)), Duration::from_secs(60))
    }

    fn prepared_registry(
        factory: Arc<dyn QuerySessionFactory>,
        timeout: Duration,
    ) -> Arc<TrinoQueryRegistry> {
        let control = QueryControlService::new(Arc::new(
            novarocks_query_application::query_control::QueryApplicationControl::new(
                novarocks_types::FrontendProcessId::new_v7(),
            ),
        ));
        TrinoQueryRegistry::new(factory, control, "root".into(), timeout)
    }
    async fn json(response: Response) -> serde_json::Value {
        assert_eq!(response.status(), StatusCode::OK);
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap()
    }
    fn queued_sql(entry: &crate::entry::QueryEntry, name: &str) -> std::sync::Weak<str> {
        let request = entry.request.lock().unwrap();
        Arc::downgrade(request.as_ref().unwrap().prepared_sql(name).unwrap())
    }

    async fn submit_prepared(state: &StatementState, prepared: &'static str) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-user", HeaderValue::from_static("root"));
        headers.insert(HOST, HeaderValue::from_static("localhost:8080"));
        headers.insert(crate::prepared::HEADER, HeaderValue::from_static(prepared));
        submit(State(state.clone()), headers, Body::from("EXECUTE p")).await
    }

    fn endpoint(page: &serde_json::Value) -> (String, String, String) {
        let mut parts = page["nextUri"].as_str().unwrap().rsplit('/');
        let token = parts.next().unwrap().to_owned();
        let slug = parts.next().unwrap().to_owned();
        let id = parts.next().unwrap().to_owned();
        (id, slug, token)
    }

    #[tokio::test]
    async fn prepared_post_rejects_bad_syntax_and_cancellation_releases_queued_sql() {
        let opened = Arc::new(AtomicUsize::new(0));
        let registry = registry(Arc::clone(&opened));
        let state = StatementState {
            registry: Arc::clone(&registry),
            limit: QueryTextLimit::default(),
        };
        let malformed = submit_prepared(&state, "p=SELECT+)").await;
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

        let queued = json(submit_prepared(&state, "p=SELECT+1").await).await;
        let (id, slug, token) = endpoint(&queued);
        let entry = registry.lookup(&id, &slug).unwrap();
        let sql = queued_sql(&entry, "p");
        assert_eq!(sql.upgrade().unwrap().as_ref(), "SELECT 1");
        assert_eq!(opened.load(Ordering::SeqCst), 0);

        let status = cancel(State(Arc::clone(&registry)), Path((id, slug, token))).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(entry.request.lock().unwrap().is_none());
        assert!(sql.upgrade().is_none());
        assert_eq!(opened.load(Ordering::SeqCst), 0);
        registry.finalize().await;
    }

    #[derive(Default)]
    struct ObservePreparedFactory(
        std::sync::Mutex<
            Option<novarocks_query_application::session_value::PreparedStatementDefinition>,
        >,
    );

    impl QuerySessionFactory for ObservePreparedFactory {
        fn open_statement_session(
            &self,
            request: StatementSessionRequest,
        ) -> Result<Arc<dyn QuerySession>, StatementSessionError> {
            let (_, _, session) = request.into_parts();
            *self.0.lock().unwrap() = session.prepared_statements().get("p").cloned();
            Err(StatementSessionError::Service(QueryServiceError::new(
                QueryServiceErrorKind::InvalidValue,
                "Test session construction stops after borrowing its exact definition",
            )))
        }

        fn cancel_all(&self, _: QueryCancellationReason) {}
    }

    #[tokio::test]
    async fn prepared_first_get_preserves_the_original_sql_until_its_last_alias_drops() {
        let observer = Arc::new(ObservePreparedFactory::default());
        let registry = prepared_registry(observer.clone(), Duration::from_secs(60));
        let state = StatementState {
            registry: Arc::clone(&registry),
            limit: QueryTextLimit::default(),
        };
        let queued = json(submit_prepared(&state, "p=SELECT+1,other=SELECT+2").await).await;
        let (id, slug, token) = endpoint(&queued);
        let entry = registry.lookup(&id, &slug).unwrap();
        let sql = queued_sql(&entry, "p");
        let other_sql = queued_sql(&entry, "other");
        assert!(observer.0.lock().unwrap().is_none());
        let failed = json(next(State(Arc::clone(&registry)), Path((id, slug, token))).await).await;
        assert!(failed.get("error").is_some());
        registry.finalize().await;
        assert!(entry.request.lock().unwrap().is_none());
        assert!(other_sql.upgrade().is_none());
        let selected = observer.0.lock().unwrap().take().unwrap();
        assert!(Arc::ptr_eq(&sql.upgrade().unwrap(), selected.sql()));
        assert_eq!(selected.sql().as_ref(), "SELECT 1");
        drop(selected);
        assert!(sql.upgrade().is_none());
    }

    #[tokio::test]
    async fn prepared_post_expiry_releases_queued_sql_with_an_old_lookup() {
        let opened = Arc::new(AtomicUsize::new(0));
        let timeout = Duration::from_secs(60);
        let registry = prepared_registry(Arc::new(RefuseWorkFactory(Arc::clone(&opened))), timeout);
        let state = StatementState {
            registry: Arc::clone(&registry),
            limit: QueryTextLimit::default(),
        };
        let queued = json(submit_prepared(&state, "p=SELECT+1").await).await;
        let (id, slug, _) = endpoint(&queued);
        let entry = registry.lookup(&id, &slug).unwrap();
        let sql = queued_sql(&entry, "p");
        *entry.touched.lock().unwrap() = std::time::Instant::now() - 2 * timeout;
        entry.requests.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            while entry.request.lock().unwrap().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the original registry timer has not expired its queued request");
        assert!(sql.upgrade().is_none());
        assert_eq!(
            registry.get(&entry, 0).await.err(),
            Some(StatusCode::NOT_FOUND)
        );
        assert_eq!(opened.load(Ordering::SeqCst), 0);
        let timer = entry.timer.lock().unwrap().take();
        if let Some(timer) = timer {
            timer.await.unwrap();
        }
        registry.finalize().await;
    }

    #[tokio::test]
    async fn logical_and_byte_oversize_share_registered_error_35_without_opening_a_session() {
        for (text, limit) in [("xx", 1), ("xxxx", 1), ("😀x", 2), ("x", 0)] {
            let opened = Arc::new(AtomicUsize::new(0));
            let registry = registry(Arc::clone(&opened));
            let mut headers = HeaderMap::new();
            headers.insert("x-trino-user", HeaderValue::from_static("root"));
            headers.insert(HOST, HeaderValue::from_static("localhost:8080"));
            let first = json(
                submit(
                    State(StatementState {
                        registry: Arc::clone(&registry),
                        limit: QueryTextLimit::new(limit).unwrap(),
                    }),
                    headers,
                    Body::from(text),
                )
                .await,
            )
            .await;
            assert!(
                first.get("error").is_none(),
                "registration must precede deferred failure"
            );
            let uri = first["nextUri"].as_str().unwrap();
            let mut parts = uri.rsplit('/');
            let token = parts.next().unwrap().to_owned();
            let slug = parts.next().unwrap().to_owned();
            let id = parts.next().unwrap().to_owned();
            let failed = json(
                next(
                    State(Arc::clone(&registry)),
                    Path((id.clone(), slug, token)),
                )
                .await,
            )
            .await;
            registry.finalize().await;
            assert_eq!(failed["id"], id);
            assert_eq!(failed["error"]["errorCode"], 35, "{text:?}");
            assert_eq!(failed["error"]["errorName"], "QUERY_TEXT_TOO_LARGE");
            assert_eq!(failed["error"]["errorType"], "USER_ERROR");
            assert!(failed.get("nextUri").is_none());
            assert_eq!(opened.load(Ordering::SeqCst), 0);
        }
    }
}
