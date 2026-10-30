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

//! Component tests retain the real Query Application statement and workload window.
use crate::{
    body::{self, WriteDeadlines},
    entry::{Page, Phase, QueryEntry},
    paging::PagePipeline,
    registry::TrinoQueryRegistry,
    request::ParsedStatementRequest,
    settlement::Disposition,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use futures::future::poll_fn;
use hyper::body::Body as HttpBody;
use novarocks_execution_contract::SafeDetail;
use novarocks_execution_contract::failure::FailureProjection;
use novarocks_query_application::{
    api::{StatementExternalEffectDisposition, build_string_query_result},
    cancellation::{QueryCancellationReason, QueryCancellationView},
    client_connection::ClientConnectionToken,
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
    protocol_delivery::{
        GovernedCompletionStatementResult, GovernedErrorStatementResult,
        GovernedImmediateStatementResult, QuerySessionOutput,
    },
    query_control::QueryApplicationControl,
    session::{
        QuerySession, QuerySessionFactory, QuerySessionOpenRequest, QuerySessionStatement,
        SessionProtocolTerminal, StatementSessionRequest,
    },
    session_control::{QueryControlService, QuerySessionLease, SessionIdentity, SessionToken},
    session_effects::SessionEffects,
    session_error::{QueryServiceError, QueryServiceErrorKind},
    session_value::StatementSessionError,
    sql::session::SessionSqlState,
    statement_kind::StatementKind,
};
use novarocks_result_contract::BorrowedPayloadSpan;
use novarocks_workload_control::{
    ResultCapacityConfig, ResultCapacityHandle, ResultWindowClass, WorkClass, WorkError, WorkScope,
    WorkloadConfig, WorkloadControl,
};
use serde_json::Value;
use std::{
    num::NonZeroU32,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Barrier, Notify};

#[derive(Clone)]
struct Captured {
    scope: WorkScope,
    cancellation: QueryCancellationView,
    session: SessionToken,
    input_catalog: Option<String>,
    input_schema: String,
    selected_error: Option<QueryServiceError>,
}
#[derive(Clone, Copy)]
enum OutputMode {
    Rows,
    Completion,
    InvalidHeaders,
    Failure(QueryFailureReason),
}

struct FixtureState {
    host: WorkloadControl,
    capacity: ResultCapacityHandle,
    control: QueryControlService,
    captured: Mutex<Vec<Captured>>,
    opened: AtomicUsize,
    completed: AtomicUsize,
    terminal_drops: AtomicUsize,
    execution_started: Notify,
    release: Notify,
    held: AtomicBool,
    mode: OutputMode,
    hold_after_cancel: bool,
    deadline_ms: Option<u64>,
}
struct Fixture {
    state: Arc<FixtureState>,
    registry: Arc<TrinoQueryRegistry>,
    idle_timeout: Duration,
}
impl Fixture {
    fn new(held: bool, completion: bool, timeout: Duration) -> Self {
        Self::configured(
            held,
            false,
            if completion {
                OutputMode::Completion
            } else {
                OutputMode::Rows
            },
            timeout,
            None,
        )
    }
    fn configured(
        held: bool,
        hold_after_cancel: bool,
        mode: OutputMode,
        timeout: Duration,
        deadline_ms: Option<u64>,
    ) -> Self {
        let host = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        let capacity = host
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        host.mark_ready().unwrap();
        let control = QueryControlService::new(Arc::new(QueryApplicationControl::new(
            novarocks_types::FrontendProcessId::new_v7(),
        )));
        let state = Arc::new(FixtureState {
            host,
            capacity,
            control: control.clone(),
            captured: Mutex::new(Vec::new()),
            opened: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            terminal_drops: AtomicUsize::new(0),
            execution_started: Notify::new(),
            release: Notify::new(),
            held: AtomicBool::new(held),
            mode,
            hold_after_cancel,
            deadline_ms,
        });
        let sessions: Arc<dyn QuerySessionFactory> = Arc::new(Factory(Arc::clone(&state)));
        let registry = TrinoQueryRegistry::new(sessions, control, "root".into(), timeout);
        Self {
            state,
            registry,
            idle_timeout: timeout,
        }
    }
    fn standard() -> Self {
        Self::new(false, false, Duration::from_secs(60))
    }
    fn register(&self) -> (Arc<QueryEntry>, Value) {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-user", HeaderValue::from_static("root"));
        headers.insert("x-trino-catalog", HeaderValue::from_static("input_lake"));
        headers.insert("x-trino-schema", HeaderValue::from_static("input_schema"));
        let request =
            ParsedStatementRequest::parse(&headers, "SELECT fixture".into(), "root").unwrap();
        let first = self
            .registry
            .register(request, "http://localhost:8080".into());
        let json = json(&first);
        let (id, slug, _) = locator(json["nextUri"].as_str().unwrap());
        let entry = self.registry.lookup(&id, &slug).unwrap();
        (entry, json)
    }
    fn captured(&self) -> Captured {
        self.state.captured.lock().unwrap().last().unwrap().clone()
    }
    async fn started(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.state.execution_started.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if !self.state.captured.lock().unwrap().is_empty() {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("original statement must start");
    }
    async fn outcome(&self, entry: &Arc<QueryEntry>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = entry.progress.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if entry.production.lock().unwrap().outcome.is_some() {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("driver must publish original outcome");
    }
    fn assert_live_original(&self) {
        self.assert_retained_original();
        assert!(self.captured().scope.check().is_ok());
    }
    fn assert_retained_original(&self) {
        let snapshot = self.state.host.observation().snapshot();
        assert_eq!(snapshot.root_responsibilities, 1);
        assert_eq!(snapshot.businesses, 1);
        assert_eq!(self.state.capacity.snapshot().held_positions, [0, 1, 0, 0]);
        assert_eq!(self.state.completed.load(Ordering::Acquire), 0);
        assert_eq!(self.state.terminal_drops.load(Ordering::Acquire), 0);
    }
    async fn assert_released(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            self.state
                .host
                .observation()
                .wait_until_no_root_responsibilities()
                .await;
            while self.state.capacity.snapshot().held_positions != [0; 4] {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("original owner and window must actually exit");
        let snapshot = self.state.host.observation().snapshot();
        assert_eq!(snapshot.businesses, 0);
        assert_eq!(self.state.completed.load(Ordering::Acquire), 1);
        assert_eq!(self.state.terminal_drops.load(Ordering::Acquire), 1);
        assert_eq!(self.captured().scope.check(), Err(WorkError::Released));
    }
    fn release_execute(&self) {
        self.state.held.store(false, Ordering::Release);
        self.state.release.notify_waiters();
    }
    fn settlement_count(&self, outcome: &str) -> u64 {
        let registry = prometheus::Registry::new();
        self.registry.observation.register(&registry).unwrap();
        registry
            .gather()
            .iter()
            .filter(|family| family.get_name() == "novarocks_trino_query_settlements_total")
            .flat_map(|family| family.get_metric())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.get_name() == "outcome" && label.get_value() == outcome)
            })
            .map(|metric| metric.get_counter().get_value() as u64)
            .sum()
    }
    fn protocol_metrics(&self) -> (i64, u64) {
        let registry = prometheus::Registry::new();
        self.registry.observation.register(&registry).unwrap();
        let mut active = 0;
        let mut settled = 0;
        for family in registry.gather() {
            for metric in family.get_metric() {
                match family.get_name() {
                    "novarocks_trino_queries_active" => {
                        active += metric.get_gauge().get_value() as i64
                    }
                    "novarocks_trino_query_settlements_total" => {
                        settled += metric.get_counter().get_value() as u64
                    }
                    _ => {}
                }
            }
        }
        (active, settled)
    }
}
struct Factory(Arc<FixtureState>);
impl QuerySessionFactory for Factory {
    fn open_statement_session(
        &self,
        request: StatementSessionRequest,
    ) -> Result<Arc<dyn QuerySession>, StatementSessionError> {
        let (id, principal, value) = request.into_parts();
        let input = SessionSqlState::from_session_value(value)?;
        let lease = self
            .0
            .control
            .register_session(SessionIdentity::client_statement(id, principal))
            .expect("original statement session registration");
        let token = lease.token();
        self.0.opened.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::new(Session {
            state: Arc::clone(&self.0),
            lease: Mutex::new(Some(lease)),
            token,
            input,
        }))
    }
    fn open_session(
        &self,
        _: QuerySessionOpenRequest,
    ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
        Err(QueryServiceError::new(
            QueryServiceErrorKind::Internal,
            "connection sessions are outside this fixture",
        ))
    }
    fn cancel_all(&self, reason: QueryCancellationReason) {
        self.0.control.cancel_all(reason);
    }
}
struct Session {
    state: Arc<FixtureState>,
    lease: Mutex<Option<QuerySessionLease>>,
    token: SessionToken,
    input: SessionSqlState,
}
#[async_trait::async_trait]
impl QuerySession for Session {
    #[cfg(novarocks_dev_controls)]
    fn connection_session(&self, _: ClientConnectionToken) -> Option<SessionToken> {
        None
    }
    async fn init_database(&self, _: &str) -> Result<QuerySessionStatement, QueryServiceError> {
        Err(QueryServiceError::new(
            QueryServiceErrorKind::Internal,
            "COM_INIT_DB is outside this fixture",
        ))
    }
    async fn execute_statement(&self, _: &str) -> Result<QuerySessionStatement, QueryServiceError> {
        let statement = self
            .state
            .control
            .begin_governed_statement_with_result(
                self.token,
                &self.state.host.root_admission(),
                WorkClass::Management,
                self.state
                    .deadline_ms
                    .map(|ms| tokio::time::Instant::now() + Duration::from_millis(ms)),
                self.state.deadline_ms,
                None,
                ResultWindowClass::Local,
            )
            .unwrap();
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            self.state.deadline_ms,
        );
        // The executor selects this typed domain failure before a late protocol
        // command. A held return models the original action still exiting.
        let selected_error = match self.state.mode {
            OutputMode::Failure(reason) => Some(originating_error(reason)),
            OutputMode::Rows | OutputMode::Completion | OutputMode::InvalidHeaders => None,
        };
        self.state.captured.lock().unwrap().push(Captured {
            scope: statement.scope().clone(),
            cancellation: cancellation.clone(),
            session: self.token,
            input_catalog: self.input.current_catalog().map(str::to_owned),
            input_schema: self.input.current_database().to_owned(),
            selected_error: selected_error.clone(),
        });
        self.state.execution_started.notify_waiters();
        while self.state.held.load(Ordering::Acquire)
            && (self.state.hold_after_cancel || cancellation.reason().is_none())
        {
            let release = self.state.release.notified();
            if !self.state.held.load(Ordering::Acquire) {
                break;
            }
            if self.state.hold_after_cancel {
                release.await;
            } else {
                tokio::select! { _ = release => {}, _ = cancellation.cancelled() => {} }
            }
        }
        let (output, kind) = if let Some(error) = selected_error {
            (
                QuerySessionOutput::GovernedError(GovernedErrorStatementResult::new(
                    error, statement,
                )),
                StatementKind::Insert,
            )
        } else if matches!(
            self.state.mode,
            OutputMode::Completion | OutputMode::InvalidHeaders
        ) || cancellation.reason().is_some()
        {
            let effects = if matches!(
                self.state.mode,
                OutputMode::Completion | OutputMode::InvalidHeaders
            ) {
                SessionEffects::set_catalog_schema(
                    Some("resolved_lake".into()),
                    if matches!(self.state.mode, OutputMode::InvalidHeaders) {
                        "invalid\nschema".into()
                    } else {
                        "resolved_schema".into()
                    },
                )
            } else {
                SessionEffects::default()
            };
            (
                QuerySessionOutput::GovernedCompletion(
                    GovernedCompletionStatementResult::with_session_effects(statement, effects),
                ),
                StatementKind::Use,
            )
        } else {
            let result =
                build_string_query_result("value", vec!["row-one".into(), "row-two".into()])
                    .unwrap();
            let result = GovernedImmediateStatementResult::try_new(
                result,
                statement,
                *self.input.presentation(),
            )
            .unwrap_or_else(|(error, _)| panic!("{error}"));
            (
                QuerySessionOutput::GovernedQuery(result),
                StatementKind::Query,
            )
        };
        Ok(QuerySessionStatement::new(
            output,
            Box::new(Terminal(Arc::clone(&self.state))),
            kind,
        ))
    }
    async fn execute_batch(&self, sql: &str) -> Result<QuerySessionStatement, QueryServiceError> {
        self.execute_statement(sql).await
    }
    fn cancel_current(&self, reason: QueryCancellationReason) {
        self.state
            .control
            .cancel_session_statement(self.token, reason);
    }
    fn close(&self) {
        self.lease.lock().unwrap().take();
    }
}
fn originating_error(reason: QueryFailureReason) -> QueryServiceError {
    let error = QueryServiceError::from_execution_failure(ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating("original selected write outcome"),
        ExecutionFailureSite::Coordinator,
    ));
    if reason == QueryFailureReason::CommitUnknown {
        error.attach_external_effect_disposition(StatementExternalEffectDisposition::CommitUnknown)
    } else {
        error
    }
}

struct Terminal(Arc<FixtureState>);
impl SessionProtocolTerminal for Terminal {
    fn complete(self: Box<Self>) {
        self.0.completed.fetch_add(1, Ordering::AcqRel);
    }
}
impl Drop for Terminal {
    fn drop(&mut self) {
        self.0.terminal_drops.fetch_add(1, Ordering::AcqRel);
    }
}
fn bytes(page: &Page) -> Vec<u8> {
    page.chunks
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect()
}
fn json(page: &Page) -> Value {
    serde_json::from_slice(&bytes(page)).unwrap()
}
fn locator(uri: &str) -> (String, String, u64) {
    let mut components = uri.rsplit('/');
    let token = components.next().unwrap().parse().unwrap();
    let slug = components.next().unwrap().to_owned();
    let id = components.next().unwrap().to_owned();
    (id, slug, token)
}
fn next(page: &Page) -> u64 {
    locator(json(page)["nextUri"].as_str().unwrap()).2
}
async fn get(registry: &Arc<TrinoQueryRegistry>, entry: &Arc<QueryEntry>, token: u64) -> Arc<Page> {
    let page = tokio::time::timeout(Duration::from_secs(5), registry.get(entry, token))
        .await
        .expect("bounded registry request")
        .unwrap();
    let settled = matches!(entry.state.lock().await.phase, Phase::Settled { .. });
    if settled {
        let production = entry.production.lock().unwrap();
        assert!(entry.request.lock().unwrap().is_none());
        assert!(production.pipeline.is_none());
        assert!(production.columns.is_none());
        assert!(production.outcome.is_none());
        assert!(!production.retained);
    }
    page
}

#[tokio::test]
async fn final_data_page_retains_original_scope_until_ack_and_terminal_replays_exactly() {
    let fixture = Fixture::standard();
    let (entry, queued) = fixture.register();
    assert_eq!(queued["stats"]["state"], "QUEUED");
    assert_eq!(queued["stats"]["scheduled"], false);
    assert_eq!(fixture.state.opened.load(Ordering::Acquire), 0);
    let data = get(&fixture.registry, &entry, 0).await;
    assert_eq!(
        json(&data)["data"],
        serde_json::json!([["row-one"], ["row-two"]])
    );
    assert_eq!(json(&data)["stats"]["state"], "RUNNING");
    let token = next(&data);
    fixture.outcome(&entry).await;
    fixture.assert_live_original();
    assert_eq!(fixture.protocol_metrics(), (1, 0));
    let original = bytes(&data);
    let replay = get(&fixture.registry, &entry, 0).await;
    assert!(Arc::ptr_eq(&data, &replay));
    assert_eq!(bytes(&replay), original);
    assert!(
        fixture
            .registry
            .lookup(entry.id.as_str(), "wrong-slug")
            .is_none()
    );
    assert_eq!(
        fixture.registry.get(&entry, token + 1).await.err().unwrap(),
        StatusCode::NOT_FOUND
    );
    drop(replay);
    drop(data);
    let terminal = get(&fixture.registry, &entry, token).await;
    assert!(json(&terminal).get("nextUri").is_none());
    assert!(json(&terminal).get("data").is_none());
    assert!(json(&terminal).get("updateType").is_none());
    assert_eq!(json(&terminal)["stats"]["state"], "FINISHED");
    fixture.assert_released().await;
    assert_eq!(fixture.protocol_metrics(), (0, 1));
    let repeated = get(&fixture.registry, &entry, token).await;
    assert!(Arc::ptr_eq(&terminal, &repeated));
    assert_eq!(bytes(&repeated), bytes(&terminal));
    assert_eq!(
        fixture.registry.get(&entry, 0).await.err().unwrap(),
        StatusCode::GONE
    );
    fixture.registry.cancel(&entry).await;
    assert_eq!(fixture.protocol_metrics(), (0, 1));
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn partial_response_body_drop_does_not_ack_or_settle_the_original_statement() {
    let fixture = Fixture::standard();
    let (entry, _) = fixture.register();
    let page = get(&fixture.registry, &entry, 0).await;
    fixture.outcome(&entry).await;
    let expected = bytes(&page);
    let token = next(&page);
    let deadlines = WriteDeadlines::new(Duration::from_secs(30));
    let mut body = deadlines
        .wrap(body::response(page.clone(), entry.progress.clone()))
        .into_body();
    let first_frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    assert!(!first_frame.data_ref().unwrap().is_empty());
    drop(first_frame);
    drop(body);
    drop(page);
    assert!(deadlines.has_pending());
    fixture.assert_live_original();
    let replay = get(&fixture.registry, &entry, 0).await;
    assert_eq!(bytes(&replay), expected);
    assert_eq!(next(&replay), token);
    drop(replay);
    // A failed connection drops this transport's custody. The token is still retained.
    drop(deadlines);
    fixture.assert_live_original();
    let terminal = get(&fixture.registry, &entry, token).await;
    assert_eq!(json(&terminal)["stats"]["state"], "FINISHED");
    fixture.assert_released().await;
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn empty_long_poll_retains_its_token_and_does_not_fabricate_completion() {
    let fixture = Fixture::new(true, false, Duration::from_secs(60));
    let (entry, _) = fixture.register();
    let empty = get(&fixture.registry, &entry, 0).await;
    fixture.started().await;
    assert!(json(&empty).get("data").is_none());
    assert!(json(&empty).get("columns").is_none());
    assert_eq!(json(&empty)["stats"]["state"], "RUNNING");
    fixture.assert_live_original();
    assert_eq!(
        bytes(get(&fixture.registry, &entry, 0).await.as_ref()),
        bytes(&empty)
    );
    let token = next(&empty);
    drop(empty);
    fixture.state.held.store(false, Ordering::Release);
    fixture.state.release.notify_waiters();
    let data = get(&fixture.registry, &entry, token).await;
    assert_eq!(
        json(&data)["data"],
        serde_json::json!([["row-one"], ["row-two"]])
    );
    let final_token = next(&data);
    drop(data);
    drop(get(&fixture.registry, &entry, final_token).await);
    fixture.assert_released().await;
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn delete_before_success_seal_cancels_the_real_original_scope_once() {
    let fixture = Fixture::new(true, false, Duration::from_secs(60));
    let (entry, _) = fixture.register();
    let empty = get(&fixture.registry, &entry, 0).await;
    fixture.assert_live_original();
    let cancellation = fixture.captured().cancellation;
    assert_eq!(cancellation.reason(), None);
    let token = next(&empty);
    drop(empty);
    fixture.registry.cancel(&entry).await;
    assert_eq!(
        cancellation.reason(),
        Some(QueryCancellationReason::ClientCancelRequested)
    );
    let terminal = get(&fixture.registry, &entry, token).await;
    assert_eq!(json(&terminal)["error"]["errorName"], "USER_CANCELED");
    fixture.assert_released().await;
    fixture.registry.cancel(&entry).await;
    assert_eq!(fixture.protocol_metrics(), (0, 1));
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn delete_after_success_seal_fails_delivery_without_reclassifying_sealed_success() {
    let fixture = Fixture::standard();
    let (entry, _) = fixture.register();
    let data = get(&fixture.registry, &entry, 0).await;
    fixture.outcome(&entry).await;
    let cancellation = fixture.captured().cancellation;
    let token = next(&data);
    assert_eq!(cancellation.reason(), None);
    drop(data);
    fixture.registry.cancel(&entry).await;
    assert_eq!(
        cancellation.reason(),
        None,
        "success seal blocks later workload cancellation"
    );
    assert_eq!(
        json(get(&fixture.registry, &entry, token).await.as_ref())["error"]["errorName"],
        "USER_CANCELED"
    );
    fixture.assert_released().await;
    assert_eq!(fixture.protocol_metrics(), (0, 1));
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn abandonment_settles_unsealed_and_sealed_originals_and_terminal_tombstone_is_stable() {
    for held in [true, false] {
        let fixture = Fixture::new(held, false, Duration::from_millis(80));
        let (entry, _) = fixture.register();
        let page = get(&fixture.registry, &entry, 0).await;
        let old_token = next(&page);
        let cancellation = fixture.captured().cancellation;
        drop(page);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = entry.progress.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if matches!(entry.state.lock().await.phase, Phase::Settled { .. }) { break; }
                tokio::select! { _ = notified => {}, _ = tokio::time::sleep(Duration::from_millis(5)) => {} }
            }
        }).await.expect("abandon timer must settle the original");
        let terminal = get(&fixture.registry, &entry, old_token).await;
        assert_eq!(json(&terminal)["error"]["errorName"], "ABANDONED_QUERY");
        assert_eq!(
            cancellation.reason(),
            if held {
                Some(QueryCancellationReason::ClientDisconnected)
            } else {
                None
            }
        );
        let old_replay = get(&fixture.registry, &entry, 0).await;
        assert_eq!(bytes(&old_replay), bytes(&terminal));
        fixture.assert_released().await;
        assert_eq!(fixture.protocol_metrics(), (0, 1));
        fixture.registry.finalize().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn terminal_ack_delete_and_finalize_race_settles_original_owner_only_once() {
    let fixture = Fixture::standard();
    let (entry, _) = fixture.register();
    let page = get(&fixture.registry, &entry, 0).await;
    fixture.outcome(&entry).await;
    let token = next(&page);
    drop(page);
    let barrier = Arc::new(Barrier::new(3));
    let ack = {
        let registry = fixture.registry.clone();
        let entry = entry.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            registry.get(&entry, token).await.unwrap()
        })
    };
    let cancel = {
        let registry = fixture.registry.clone();
        let entry = entry.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            registry.cancel(&entry).await;
        })
    };
    let finalize = {
        let registry = fixture.registry.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            registry.finalize().await;
        })
    };
    let terminal = tokio::time::timeout(Duration::from_secs(5), ack)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), cancel)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), finalize)
        .await
        .unwrap()
        .unwrap();
    assert!(json(&terminal).get("nextUri").is_none());
    fixture.assert_released().await;
    assert_eq!(fixture.protocol_metrics(), (0, 1));
    assert_eq!(fixture.captured().cancellation.reason(), None);
}

#[tokio::test]
async fn draining_rejects_registered_and_new_submissions_without_creating_work() {
    let fixture = Fixture::standard();
    let (entry, _) = fixture.register();
    fixture.registry.begin_drain();
    let terminal = get(&fixture.registry, &entry, 0).await;
    assert_eq!(
        json(&terminal)["error"]["errorName"],
        "SERVER_SHUTTING_DOWN"
    );
    assert_eq!(json(&terminal)["stats"]["scheduled"], false);
    let headers = HeaderMap::from_iter([(
        axum::http::HeaderName::from_static("x-trino-user"),
        HeaderValue::from_static("root"),
    )]);
    let request = ParsedStatementRequest::parse(&headers, "SELECT 1".into(), "root").unwrap();
    let page = fixture
        .registry
        .register(request, "http://localhost:8080".into());
    assert_eq!(json(&page)["error"]["errorName"], "SERVER_SHUTTING_DOWN");
    assert_eq!(json(&page)["stats"]["scheduled"], false);
    assert!(json(&page).get("nextUri").is_none());
    assert_eq!(fixture.state.opened.load(Ordering::Acquire), 0);
    assert_eq!(
        fixture
            .state
            .host
            .observation()
            .snapshot()
            .root_responsibilities,
        0
    );
    assert_eq!(fixture.state.capacity.snapshot().held_positions, [0; 4]);
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn terminal_session_headers_come_from_executor_effects_not_input_name_differences() {
    let fixture = Fixture::new(false, true, Duration::from_secs(60));
    let (entry, _) = fixture.register();
    let terminal = get(&fixture.registry, &entry, 0).await;
    assert_eq!(terminal.headers["x-trino-set-catalog"], "resolved_lake");
    assert_eq!(terminal.headers["x-trino-set-schema"], "resolved_schema");
    assert!(!terminal.headers.contains_key("x-trino-set-session"));
    assert_eq!(json(&terminal)["updateType"], "USE");
    assert_eq!(json(&terminal)["stats"]["scheduled"], false);
    let captured = fixture.captured();
    assert_eq!(captured.input_catalog.as_deref(), Some("input_lake"));
    assert_eq!(captured.input_schema, "input_schema");
    assert_eq!(
        fixture
            .state
            .control
            .begin_statement(captured.session)
            .err(),
        Some(novarocks_query_application::session_control::QueryControlError::UnknownSession)
    );
    fixture.assert_released().await;
    assert_eq!(
        bytes(&terminal),
        bytes(get(&fixture.registry, &entry, 0).await.as_ref())
    );
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn page_bounds_are_derived_from_the_original_window_and_refuse_oversized_rows() {
    let fixture = Fixture::standard();
    let lease = fixture
        .state
        .control
        .register_session(SessionIdentity::new(
            ClientConnectionToken::new(71, 1).unwrap(),
            "root",
        ))
        .unwrap();
    let mut owner = fixture
        .state
        .control
        .begin_governed_statement_with_result(
            lease.token(),
            &fixture.state.host.root_admission(),
            WorkClass::Management,
            None,
            None,
            None,
            ResultWindowClass::Client,
        )
        .unwrap();
    let window = owner.result_window_alias().unwrap();
    let total = window.all_objects_bytes().unwrap() as usize;
    let mut pipeline = PagePipeline::new(
        window,
        1024,
        crate::paging::ResultSourceBacking::RootSegments,
    )
    .unwrap();
    assert!(pipeline.row_limit() < total);
    let row = NonZeroU32::new((pipeline.row_limit() + 1).try_into().unwrap()).unwrap();
    let error = pipeline
        .append(BorrowedPayloadSpan {
            starts_row: Some(row),
            bytes: b"",
            completes_row: false,
        })
        .unwrap_err();
    assert!(error.contains("exceeds the original result window"));
    assert!(!pipeline.has_rows());
    assert!(pipeline.at_row_boundary());
    drop(pipeline);
    assert!(matches!(owner.seal_success_visibility(), novarocks_query_application::session_control::GovernedStatementVisibilitySealOutcome::Sealed));
    owner.finish();
    drop(lease);
    assert_eq!(fixture.state.capacity.snapshot().held_positions, [0; 4]);
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn registered_touch_preserves_entry_and_expiry_never_instantiates_a_statement() {
    let timeout = Duration::from_millis(100);
    let fixture = Fixture::new(false, false, timeout);
    let (entry, _) = fixture.register();
    let id = entry.id.to_string();
    let slug = entry.slug.clone();
    // Use the same real monotonic clock as the production owner. Explicit
    // aging exercises expiry, while request notification reschedules its timer.
    *entry.touched.lock().unwrap() = std::time::Instant::now() - 2 * timeout;
    assert!(fixture.registry.lookup(&id, &slug).is_some());
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        fixture.registry.lookup(&id, &slug).is_some(),
        "a valid request refreshes the entry"
    );
    *entry.touched.lock().unwrap() = std::time::Instant::now() - 2 * timeout;
    entry.requests.notify_one();
    let weak = Arc::downgrade(&entry);
    drop(entry);
    tokio::time::timeout(Duration::from_secs(5), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("registered expiry releases its sole registry entry");
    assert!(fixture.registry.lookup(&id, &slug).is_none());
    assert_eq!(fixture.state.opened.load(Ordering::Acquire), 0);
    assert_eq!(
        fixture
            .state
            .host
            .observation()
            .snapshot()
            .root_responsibilities,
        0
    );
    assert_eq!(fixture.state.capacity.snapshot().held_positions, [0; 4]);
    fixture.registry.finalize().await;
}

#[derive(Clone, Copy, Debug)]
enum LateCommand {
    Delete,
    IdleAbandon,
}
async fn begin_late_command(
    fixture: &Fixture,
    entry: &Arc<QueryEntry>,
    command: LateCommand,
) -> Option<tokio::task::JoinHandle<()>> {
    let join = match command {
        LateCommand::Delete => {
            let registry = fixture.registry.clone();
            let entry = entry.clone();
            Some(tokio::spawn(async move {
                registry.cancel(&entry).await;
            }))
        }
        LateCommand::IdleAbandon => {
            // Wake the actual registry timer with an actually stale entry clock.
            *entry.touched.lock().unwrap() = std::time::Instant::now() - 2 * fixture.idle_timeout;
            entry.requests.notify_one();
            None
        }
    };
    let mut commands = entry.command.subscribe();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if commands.borrow().is_some() {
                break;
            }
            commands.changed().await.unwrap();
        }
    })
    .await
    .expect("late request must reach the same original entry");
    assert_eq!(
        *commands.borrow(),
        Some(match command {
            LateCommand::Delete => Disposition::ClientCancel,
            LateCommand::IdleAbandon => Disposition::Abandon,
        })
    );
    join
}
async fn wait_settled(entry: &Arc<QueryEntry>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let progress = entry.progress.notified();
            tokio::pin!(progress);
            progress.as_mut().enable();
            if matches!(entry.state.lock().await.phase, Phase::Settled { .. }) {
                return;
            }
            progress.await;
        }
    })
    .await
    .expect("the original protocol owner must settle");
}

#[tokio::test]
async fn original_deadline_or_shutdown_wins_before_late_delete_or_idle_abandon() {
    for shutdown in [false, true] {
        for command in [LateCommand::Delete, LateCommand::IdleAbandon] {
            let fixture = Fixture::configured(
                true,
                true,
                OutputMode::Rows,
                Duration::from_secs(3600),
                if shutdown { None } else { Some(60) },
            );
            let (entry, _) = fixture.register();
            let empty = get(&fixture.registry, &entry, 0).await;
            let token = next(&empty);
            drop(empty);
            fixture.assert_retained_original();
            let captured = fixture.captured();
            let (expected_reason, expected_name, expected_code) = if shutdown {
                assert_eq!(
                    fixture.state.host.cancel_active_roots(
                        novarocks_workload_control::CancellationReason::ServerShutdown
                    ),
                    1
                );
                (
                    QueryCancellationReason::ServerShutdown,
                    "SERVER_SHUTTING_DOWN",
                    65545,
                )
            } else {
                // Exercise the original owner's admitted deadline and real
                // expiration author, rather than injecting a timeout message.
                tokio::time::sleep(Duration::from_millis(80)).await;
                fixture.state.host.expire_deadlines();
                (
                    QueryCancellationReason::DeadlineExceeded { timeout_ms: 60 },
                    "EXCEEDED_TIME_LIMIT",
                    131075,
                )
            };
            assert_eq!(
                captured.cancellation.reason(),
                Some(expected_reason.clone())
            );
            let join = begin_late_command(&fixture, &entry, command).await;
            // Cancellation does not prove the original executor has exited.
            assert_eq!(
                fixture
                    .state
                    .host
                    .observation()
                    .snapshot()
                    .root_responsibilities,
                1
            );
            assert_eq!(fixture.state.host.observation().snapshot().businesses, 1);
            assert_eq!(
                fixture.state.capacity.snapshot().held_positions,
                [0, 1, 0, 0]
            );
            assert_eq!(fixture.state.completed.load(Ordering::Acquire), 0);
            assert_eq!(
                captured.cancellation.reason(),
                Some(expected_reason.clone())
            );
            fixture.release_execute();
            if let Some(join) = join {
                join.await.unwrap();
            }
            wait_settled(&entry).await;
            let terminal = get(&fixture.registry, &entry, token).await;
            let error = &json(&terminal)["error"];
            assert_eq!(error["errorName"], expected_name, "{command:?}");
            assert_eq!(error["errorCode"], expected_code, "{command:?}");
            assert_eq!(captured.cancellation.reason(), Some(expected_reason));
            assert!(
                !matches!(
                    entry.state.lock().await.phase,
                    Phase::Settled {
                        abandoned: true,
                        ..
                    }
                ),
                "a prior non-client cause is not an abandonment tombstone"
            );
            assert_eq!(fixture.settlement_count("failed"), 1);
            assert_eq!(fixture.settlement_count("client_cancel"), 0);
            assert_eq!(fixture.settlement_count("abandoned"), 0);
            assert_eq!(fixture.protocol_metrics(), (0, 1));
            assert_eq!(
                bytes(&terminal),
                bytes(get(&fixture.registry, &entry, token).await.as_ref())
            );
            fixture.assert_released().await;
            fixture.registry.finalize().await;
        }
    }
}

#[tokio::test]
async fn originating_governed_failure_survives_late_commands_before_and_after_driver_publication() {
    for (reason, expected_name, expected_code) in [
        (
            QueryFailureReason::CommitUnknown,
            "GENERIC_INTERNAL_ERROR",
            65536,
        ),
        (
            QueryFailureReason::TransportCapacity,
            "GENERIC_INSUFFICIENT_RESOURCES",
            131072,
        ),
        (QueryFailureReason::BackendLost, "REMOTE_HOST_GONE", 65558),
    ] {
        for published in [false, true] {
            for command in [LateCommand::Delete, LateCommand::IdleAbandon] {
                let fixture = Fixture::configured(
                    true,
                    true,
                    OutputMode::Failure(reason),
                    Duration::from_secs(3600),
                    None,
                );
                let (entry, _) = fixture.register();
                let empty = get(&fixture.registry, &entry, 0).await;
                let token = next(&empty);
                drop(empty);
                fixture.assert_live_original();
                let captured = fixture.captured();
                let selected = captured
                    .selected_error
                    .as_ref()
                    .expect("executor selected a typed originating failure");
                assert_eq!(
                    selected.execution_failure().unwrap().fact(),
                    reason.failure_fact()
                );
                if published {
                    fixture.release_execute();
                    fixture.outcome(&entry).await;
                    fixture.assert_live_original();
                }
                let join = begin_late_command(&fixture, &entry, command).await;
                if !published {
                    assert_eq!(
                        fixture
                            .state
                            .host
                            .observation()
                            .snapshot()
                            .root_responsibilities,
                        1
                    );
                    assert_eq!(fixture.state.completed.load(Ordering::Acquire), 0);
                    fixture.release_execute();
                }
                if let Some(join) = join {
                    join.await.unwrap();
                }
                wait_settled(&entry).await;
                let terminal = get(&fixture.registry, &entry, token).await;
                let wire = json(&terminal);
                assert_eq!(
                    wire["error"]["errorName"], expected_name,
                    "{reason:?} {command:?} published={published}"
                );
                assert_eq!(
                    wire["error"]["errorCode"], expected_code,
                    "{reason:?} {command:?} published={published}"
                );
                assert_eq!(wire["error"]["message"], selected.message());
                assert!(wire.get("updateType").is_none());
                assert!(terminal.headers.is_empty());
                if reason == QueryFailureReason::CommitUnknown {
                    let message = wire["error"]["message"].as_str().unwrap();
                    assert!(message.contains("[query.execution.commit_unknown]"));
                    assert!(message.contains("CommitUnknown"));
                    assert!(message.contains("do_not_retry=true"));
                    assert!(message.contains("do not retry statement"));
                }
                if published {
                    assert_eq!(
                        captured.cancellation.reason(),
                        None,
                        "a completed driver cannot invent a new scope cancellation"
                    );
                }
                assert!(!matches!(
                    entry.state.lock().await.phase,
                    Phase::Settled {
                        abandoned: true,
                        ..
                    }
                ));
                assert_eq!(fixture.settlement_count("failed"), 1);
                assert_eq!(fixture.settlement_count("client_cancel"), 0);
                assert_eq!(fixture.settlement_count("abandoned"), 0);
                assert_eq!(fixture.protocol_metrics(), (0, 1));
                assert_eq!(
                    bytes(&terminal),
                    bytes(get(&fixture.registry, &entry, token).await.as_ref())
                );
                fixture.assert_released().await;
                fixture.registry.finalize().await;
            }
        }
    }
}

#[tokio::test]
async fn partial_row_survives_page_cut_and_backing_reclaims_only_after_actual_replay_exit() {
    use novarocks_result_contract::{ClientRowProfile, ClientRowStreamCursor};
    let fixture = Fixture::standard();
    let lease = fixture
        .state
        .control
        .register_session(SessionIdentity::client_statement(
            fixture.state.control.mint_client_query_id(),
            "root",
        ))
        .unwrap();
    let owner = fixture
        .state
        .control
        .begin_governed_statement_with_result(
            lease.token(),
            &fixture.state.host.root_admission(),
            WorkClass::Management,
            None,
            None,
            None,
            ResultWindowClass::Client,
        )
        .unwrap();
    let mut pipeline = PagePipeline::new(
        owner.result_window_alias().unwrap(),
        0,
        crate::paging::ResultSourceBacking::RootSegments,
    )
    .unwrap();
    // The first native body ends inside the second JSON row. Expectations do
    // not use a JSON parser or the adapter's page serialization helper.
    let mut first = Vec::from(3u32.to_le_bytes());
    first.extend_from_slice(b"[1]");
    first.extend_from_slice(&5u32.to_le_bytes());
    first.push(b'[');
    let profile = ClientRowProfile::try_new(first.len(), 64).unwrap();
    let first_body = ClientRowStreamCursor::new()
        .validate_body(profile, &first)
        .unwrap();
    for span in first_body.payload_spans() {
        pipeline.append(span).unwrap();
    }
    let (page, count) = pipeline.take_rows().unwrap();
    assert_eq!(count, 1);
    assert_eq!(page.as_ref(), b"[1]");
    assert!(!pipeline.at_row_boundary());
    let in_flight_body = page.clone();
    drop(page); // successor token acknowledged the logical replay owner
    assert!(
        !pipeline.reclaim(),
        "a live HTTP body still owns the original backing"
    );
    drop(in_flight_body);
    assert!(
        pipeline.reclaim(),
        "actual last alias exit permits reuse without allocation"
    );
    let next_body = first_body.after().validate_body(profile, b"2,3]").unwrap();
    for span in next_body.payload_spans() {
        pipeline.append(span).unwrap();
    }
    next_body.after().validate_end().unwrap();
    let (next_page, count) = pipeline.take_rows().unwrap();
    assert_eq!(count, 1);
    assert_eq!(next_page.as_ref(), b"[2,3]");
    assert!(pipeline.at_row_boundary());
    drop(next_page);
    drop(pipeline);
    GovernedCompletionStatementResult::new(
        novarocks_query_application::session_control::StatementCommandCompletion::new(0),
        owner,
    )
    .into_parts()
    .1
    .complete();
    drop(lease);
    assert_eq!(fixture.state.capacity.snapshot().held_positions, [0; 4]);
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn invalid_session_effect_header_fails_delivery_and_never_counts_as_protocol_complete() {
    let fixture = Fixture::configured(
        false,
        false,
        OutputMode::InvalidHeaders,
        Duration::from_secs(60),
        None,
    );
    let (entry, _) = fixture.register();
    let terminal = get(&fixture.registry, &entry, 0).await;
    assert!(terminal.headers.is_empty());
    assert_eq!(
        json(&terminal)["error"]["errorName"],
        "GENERIC_INTERNAL_ERROR"
    );
    assert!(terminal.headers.get("x-trino-set-schema").is_none());
    assert_eq!(
        fixture.captured().cancellation.reason(),
        None,
        "sealed business success is not reclassified"
    );
    fixture.assert_released().await;
    assert_eq!(fixture.settlement_count("complete"), 0);
    assert_eq!(fixture.settlement_count("failed"), 1);
    fixture.registry.finalize().await;
}

#[tokio::test]
async fn malformed_or_overflowing_tokens_are_not_found_without_starting_the_statement() {
    use tower::ServiceExt;
    let fixture = Fixture::standard();
    let (entry, _) = fixture.register();
    for token in ["-1", "not-a-token", "18446744073709551616"] {
        for method in ["GET", "HEAD", "DELETE"] {
            let uri = format!(
                "/v1/statement/executing/{}/{}/{}",
                entry.id, entry.slug, token
            );
            let response = crate::routes::routes(fixture.registry.clone())
                .oneshot(
                    axum::http::Request::builder()
                        .method(method)
                        .uri(uri)
                        .header("host", "localhost")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {token}");
        }
    }
    assert_eq!(fixture.state.opened.load(Ordering::Acquire), 0);
    assert_eq!(fixture.protocol_metrics(), (0, 0));
    fixture.registry.finalize().await;
}
