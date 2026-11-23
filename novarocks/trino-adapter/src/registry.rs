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

//! Design: docs/adr/ADR-0203-client-result-presentation-and-trino-paged-delivery.md
//! Request serialization decides protocol ownership; socket writes never settle statements.
use crate::{
    entry::{EntryState, Outcome, Page, Phase, Production, QueryEntry, RegisteredQueryRequest},
    envelope::{envelope, page_chunks},
    error_mapping::{
        TrinoErrorSource, TrinoQueryError, query_error_for_query, query_error_for_query_with_effect,
    },
    request::{ParsedStatementRequest, session_effect_headers},
    settlement::Disposition,
    update_type::update_type,
};
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use novarocks_query_application::{
    cancellation::QueryCancellationReason,
    query_control::{
        ClientQueryCancellation, ClientQueryConclusion, ClientQueryStart, ClientQueryTime,
    },
    session::QuerySessionFactory,
    session_control::QueryControlService,
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use rand::RngCore;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, watch};

pub(crate) struct TrinoQueryRegistry {
    entries: Mutex<HashMap<String, Arc<QueryEntry>>>,
    sessions: Arc<dyn QuerySessionFactory>,
    control: QueryControlService,
    pub(crate) draining: AtomicBool,
    pub(crate) user: String,
    timeout: Duration,
    stopped: AtomicBool,
    shutdown: Notify,
    pub(crate) observation: crate::observation::TrinoProtocolObservation,
}
impl TrinoQueryRegistry {
    pub(crate) fn new(
        sessions: Arc<dyn QuerySessionFactory>,
        control: QueryControlService,
        user: String,
        timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(HashMap::new()),
            sessions,
            control,
            draining: AtomicBool::new(false),
            user,
            timeout,
            stopped: AtomicBool::new(false),
            shutdown: Notify::new(),
            observation: crate::observation::TrinoProtocolObservation::new()
                .expect("fixed protocol metric descriptors"),
        })
    }
    pub(crate) fn register(
        self: &Arc<Self>,
        request: ParsedStatementRequest,
        base: String,
    ) -> Arc<Page> {
        let id = self.control.mint_client_query_id();
        let mut random = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut random);
        let slug = random.iter().map(|v| format!("{v:02x}")).collect();
        let (command, _) = watch::channel(None);
        let mut entries = self.entries.lock().expect("query registry");
        let (registration, rejection) = if self.draining.load(Ordering::Acquire) {
            (
                None,
                Some(query_error_for_query(
                    id.as_str(),
                    TrinoErrorSource::FrontendDraining,
                )),
            )
        } else if entries.contains_key(id.as_str()) {
            (
                None,
                Some(registration_error(
                    id.as_str(),
                    "client query ID is already registered",
                )),
            )
        } else {
            match self.control.register_client_query(
                id.clone(),
                request.principal(),
                request.observation().source.as_deref().map(Arc::from),
                request.original_text(),
                ClientQueryTime::now(),
            ) {
                Ok(registration) => (Some(registration), None),
                Err(error) => (
                    None,
                    Some(registration_error(
                        id.as_str(),
                        &format!("register client query: {error:?}"),
                    )),
                ),
            }
        };
        if registration.is_some() {
            tracing::debug!(query_id = %id, observation = ?request.observation(), "Trino query registered");
        }
        let original_text = registration
            .as_ref()
            .and_then(|_| request.original_text().cloned());
        if let Some(text) = &original_text {
            self.observation.observe_original_text(text);
        }
        let request = registration.as_ref().map(|_| RegisteredQueryRequest {
            request,
            unclaimed: self.observation.unclaimed(),
        });
        let entry = Arc::new(QueryEntry {
            id,
            slug,
            base,
            request: Mutex::new(request),
            registration,
            original_text: Mutex::new(original_text),
            state: tokio::sync::Mutex::new(EntryState {
                phase: Phase::Registered,
                next_token: 0,
                retained: None,
            }),
            production: Mutex::new(Production {
                pipeline: None,
                columns: None,
                outcome: None,
                retained: false,
                admission: None,
                selected_count: None,
                update_count: None,
            }),
            external_effect: Mutex::new(None),
            #[cfg(test)]
            delivery_panic: Mutex::new(None),
            progress: Arc::new(Notify::new()),
            touched: Mutex::new(Instant::now()),
            created: Instant::now(),
            started: Mutex::new(None),
            command,
            driver: Mutex::new(None),
            observation: self.observation.clone(),
            requests: Notify::new(),
            timer: Mutex::new(None),
        });
        if let Some(error) = rejection {
            return terminal_page(&entry, None, Some(error), HeaderMap::new(), None);
        }
        let page = ordinary_page(&entry, Some(0), "QUEUED", None, None, None, None, None);
        let registry = Arc::downgrade(self);
        let timer_entry = Arc::downgrade(&entry);
        let timer = tokio::spawn(async move {
            let mut watching_cancellation = true;
            loop {
                let Some(registry) = registry.upgrade() else {
                    break;
                };
                let shutdown = registry.shutdown.notified();
                tokio::pin!(shutdown);
                shutdown.as_mut().enable();
                if registry.stopped.load(Ordering::Acquire) {
                    break;
                }
                let timeout = registry.timeout;
                let Some(entry) = timer_entry.upgrade() else {
                    break;
                };
                let deadline = *entry.touched.lock().expect("query clock") + timeout;
                let expired = tokio::select! {
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => true,
                    _ = entry.registration().cancellation_requested(), if watching_cancellation => {
                        watching_cancellation = false;
                        false
                    }
                    _ = entry.requests.notified() => continue,
                    _ = &mut shutdown => break,
                };
                let mut state = entry.state.lock().await;
                if !expired {
                    if matches!(state.phase, Phase::Registered)
                        && let Some(error) = original_cancellation_error(&registry.control, &entry)
                    {
                        registered_terminal(&registry.control, &entry, &mut state, error);
                    }
                    continue;
                }
                // Recheck under the same request lock: accepted client
                // activity wins if it reached the entry before the timer.
                if entry.touched.lock().expect("query clock").elapsed() < timeout {
                    continue;
                }
                match state.phase {
                    Phase::Registered => {
                        registry.control.request_unstarted_client_cancel(
                            entry.registration(),
                            QueryCancellationReason::ClientDisconnected,
                        );
                        let error = protocol_cancellation_error(
                            &entry,
                            QueryCancellationReason::ClientDisconnected,
                        );
                        // The same entry-state lock excludes first GET while
                        // settlement releases the original request, including
                        // its prepared snapshot.
                        registered_terminal(&registry.control, &entry, &mut state, error);
                        state.phase = Phase::Expired;
                    }
                    Phase::Settled { .. } => state.phase = Phase::Expired,
                    Phase::Executing => {
                        cancel_locked(&registry.control, &entry, &mut state, Disposition::Abandon)
                            .await;
                        entry.touch();
                        tracing::debug!(query_id = %entry.id, "Trino query abandoned");
                        continue;
                    }
                    Phase::Expired => break,
                }
                registry
                    .entries
                    .lock()
                    .expect("query registry")
                    .remove(entry.id.as_str());
                break;
            }
        });
        *entry.timer.lock().expect("query timer") = Some(timer);
        entries.insert(entry.id.to_string(), Arc::clone(&entry));
        page
    }
    pub(crate) fn lookup(&self, id: &str, slug: &str) -> Option<Arc<QueryEntry>> {
        let entry = self
            .entries
            .lock()
            .expect("query registry")
            .get(id)
            .cloned()?;
        if entry.slug != slug {
            return None;
        }
        Some(entry)
    }

    fn client_activity(&self, entry: &QueryEntry) {
        entry.touch();
        self.control
            .touch_client_query(entry.registration(), ClientQueryTime::now());
    }

    pub(crate) async fn heartbeat(&self, entry: &QueryEntry) -> bool {
        let state = entry.state.lock().await;
        if matches!(state.phase, Phase::Expired) {
            return false;
        }
        self.client_activity(entry);
        true
    }

    pub(crate) async fn get(
        self: &Arc<Self>,
        entry: &Arc<QueryEntry>,
        token: u64,
    ) -> Result<Arc<Page>, StatusCode> {
        let mut state = entry.state.lock().await;
        if matches!(state.phase, Phase::Expired) {
            return Err(StatusCode::NOT_FOUND);
        }
        if let Phase::Settled {
            token: terminal_token,
            page,
            abandoned,
        } = &state.phase
        {
            if token == *terminal_token || *abandoned {
                self.client_activity(entry);
                return Ok(Arc::clone(page));
            }
            return Err(if token < *terminal_token {
                StatusCode::GONE
            } else {
                StatusCode::NOT_FOUND
            });
        }
        if let Some((last, page)) = &state.retained {
            if token == *last {
                self.client_activity(entry);
                return Ok(Arc::clone(page));
            }
            if token < *last {
                return Err(StatusCode::GONE);
            }
        }
        if token != state.next_token {
            return Err(StatusCode::NOT_FOUND);
        }
        self.client_activity(entry);
        // ACK changes logical retention. In-flight bodies keep their own original backing/window.
        state.retained.take();
        entry.production.lock().expect("query pipeline").retained = false;
        entry.progress.notify_waiters();
        if matches!(state.phase, Phase::Registered) {
            if self.draining.load(Ordering::Acquire) {
                let error =
                    query_error_for_query(entry.id.as_str(), TrinoErrorSource::FrontendDraining);
                return Ok(registered_terminal(&self.control, entry, &mut state, error));
            }
            let claim = match self
                .control
                .claim_client_query(entry.registration(), ClientQueryTime::now())
            {
                ClientQueryStart::Claimed(claim) => claim,
                ClientQueryStart::Cancelled => {
                    let error =
                        original_cancellation_error(&self.control, entry).unwrap_or_else(|| {
                            registration_error(
                                entry.id.as_str(),
                                "client query lost its original cancellation",
                            )
                        });
                    return Ok(registered_terminal(&self.control, entry, &mut state, error));
                }
                ClientQueryStart::Unavailable => {
                    let error = registration_error(
                        entry.id.as_str(),
                        "registered client query could not be claimed",
                    );
                    return Ok(registered_terminal(&self.control, entry, &mut state, error));
                }
            };
            let RegisteredQueryRequest { request, unclaimed } = entry
                .request
                .lock()
                .expect("registered request")
                .take()
                .expect("first GET owns the original request");
            drop(unclaimed);
            state.phase = Phase::Executing;
            entry.observation.start();
            *entry.started.lock().expect("query start clock") = Some(Instant::now());
            let task_entry = Arc::clone(entry);
            let sessions = Arc::clone(&self.sessions);
            let join = tokio::spawn(async move {
                use futures::FutureExt;
                let guard_entry = Arc::clone(&task_entry);
                if std::panic::AssertUnwindSafe(crate::driver::run(
                    task_entry, sessions, request, claim,
                ))
                .catch_unwind()
                .await
                .is_err()
                {
                    tracing::error!(query_id = %guard_entry.id, "Trino query driver panicked");
                    crate::driver::publish_panic(&guard_entry);
                }
            });
            *entry.driver.lock().expect("query driver") = Some(join);
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let progress = entry.progress.notified();
            tokio::pin!(progress);
            progress.as_mut().enable();
            let should_wait = {
                let production = entry.production.lock().expect("query pipeline");
                production.outcome.is_none()
                    && !production.pipeline.as_ref().is_some_and(|p| p.ready())
            };
            if !should_wait || tokio::time::Instant::now() >= deadline {
                break;
            }
            if tokio::time::timeout_at(deadline, progress).await.is_err() {
                break;
            }
        }
        let mut production = entry.production.lock().expect("query pipeline");
        if let Some((rows, _count)) = production.pipeline.as_mut().and_then(|p| p.take_rows()) {
            if let Some(count) = production.selected_count.take() {
                production.update_count = Some(count);
            }
            let next = token.checked_add(1).ok_or(StatusCode::NOT_FOUND)?;
            let window = production.pipeline.as_ref().map(|p| p.window.clone());
            let page = ordinary_page(
                entry,
                Some(next),
                "RUNNING",
                production.columns.as_ref(),
                Some(rows),
                window,
                production.admission.and_then(command_update_type),
                production.update_count,
            );
            production.retained = true;
            state.next_token = next;
            state.retained = Some((token, Arc::clone(&page)));
            return Ok(page);
        }
        if let Some(outcome) = production.outcome.take() {
            let columns = production.columns.take();
            production.pipeline.take();
            drop(production);
            let page = settle_outcome(&self.control, entry, columns, outcome);
            state.phase = Phase::Settled {
                token,
                page: Arc::clone(&page),
                abandoned: false,
            };
            return Ok(page);
        }
        let next = token.checked_add(1).ok_or(StatusCode::NOT_FOUND)?;
        let window = production.pipeline.as_ref().map(|p| p.window.clone());
        let page = ordinary_page(
            entry,
            Some(next),
            "RUNNING",
            production.columns.as_ref(),
            None,
            window,
            production.admission.and_then(command_update_type),
            production.update_count,
        );
        production.retained = true;
        state.next_token = next;
        state.retained = Some((token, Arc::clone(&page)));
        Ok(page)
    }
    pub(crate) async fn cancel(&self, entry: &Arc<QueryEntry>) -> bool {
        let mut state = entry.state.lock().await;
        if matches!(state.phase, Phase::Expired) {
            return false;
        }
        self.client_activity(entry);
        if !matches!(state.phase, Phase::Settled { .. }) {
            cancel_locked(&self.control, entry, &mut state, Disposition::ClientCancel).await;
            tracing::debug!(query_id = %entry.id, "Trino query canceled by DELETE");
        }
        true
    }

    pub(crate) async fn expire_query_history(&self) {
        if let Some(mut expiry) = self.control.subscribe_client_query_history() {
            loop {
                let deadline = *expiry.borrow_and_update();
                tokio::select! {
                    _ = async {
                        match deadline {
                            Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
                            None => std::future::pending().await,
                        }
                    } => {
                        self.control.evict_client_query_history(Instant::now());
                    }
                    changed = expiry.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                }
            }
        }
        std::future::pending().await
    }

    pub(crate) fn begin_drain(&self) {
        let _entries = self.entries.lock().expect("query registry");
        self.draining.store(true, Ordering::Release);
    }
    pub(crate) async fn finalize(&self) {
        self.begin_drain();
        self.stopped.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
        let entries = self
            .entries
            .lock()
            .expect("query registry")
            .drain()
            .map(|(_, entry)| entry)
            .collect::<Vec<_>>();
        for entry in entries {
            {
                let mut state = entry.state.lock().await;
                if !matches!(state.phase, Phase::Settled { .. } | Phase::Expired) {
                    cancel_locked(&self.control, &entry, &mut state, Disposition::Abandon).await;
                }
            }
            let timer = entry.timer.lock().expect("query timer").take();
            if let Some(timer) = timer {
                let _ = timer.await;
            }
            let join = entry.driver.lock().expect("query driver").take();
            if let Some(join) = join
                && let Err(error) = join.await
            {
                tracing::error!(%error, "Trino query driver failed");
            }
        }
    }
}
fn settle_outcome(
    control: &QueryControlService,
    entry: &QueryEntry,
    columns: Option<Bytes>,
    mut outcome: Outcome,
) -> Arc<Page> {
    let headers =
        if outcome.error.is_none() {
            session_effect_headers(&outcome.effects).unwrap_or_else(|error| {
                outcome.disposition = Disposition::Fail;
                let effect = outcome
                    .settlement
                    .as_ref()
                    .and_then(|settlement| settlement.owner.external_effect_disposition());
                outcome.error = Some(crate::error_mapping::query_error_for_query_with_effect(
                entry.id.as_str(), TrinoErrorSource::Service {
                    error: &novarocks_query_application::session_error::QueryServiceError::new(
                        novarocks_query_application::session_error::QueryServiceErrorKind::Internal,
                        format!("invalid application session effect header: {error}")), sql: "",
                }, effect,
            ));
                HeaderMap::new()
            })
        } else {
            HeaderMap::new()
        };
    if let Some(settlement) = outcome.settlement.take() {
        settlement.finish(outcome.disposition);
    }
    let error = settle_client_result(control, entry, outcome.error);
    let abandoned = error.as_ref().is_some_and(|error| {
        error.failure() == novarocks_query_application::query_control::ABANDONED_QUERY
    });
    let disposition = if error.as_ref().is_some_and(|error| {
        error.failure() == novarocks_query_application::query_control::ADMINISTRATIVELY_KILLED
    }) {
        Disposition::Cancelled
    } else if abandoned {
        Disposition::Abandon
    } else {
        outcome.disposition
    };
    entry.observation.settle(true, disposition);
    let production = entry.production.lock().expect("query pipeline");
    let update = if error.is_none() {
        production.admission.and_then(command_update_type)
    } else {
        None
    };
    let count = if error.is_none() {
        production.update_count
    } else {
        None
    };
    drop(production);
    let empty = Bytes::from_static(b"[]");
    let columns = if error.is_some() {
        None
    } else if columns.is_none() && update.is_some() {
        Some(&empty)
    } else {
        columns.as_ref()
    };
    let headers = if error.is_some() {
        HeaderMap::new()
    } else {
        headers
    };
    terminal_page_with_count(entry, columns, error, headers, update, count)
}
async fn cancel_locked(
    control: &QueryControlService,
    entry: &QueryEntry,
    state: &mut EntryState,
    requested: Disposition,
) {
    let reason = if requested == Disposition::ClientCancel {
        QueryCancellationReason::ClientCancelRequested
    } else {
        QueryCancellationReason::ClientDisconnected
    };
    control.request_unstarted_client_cancel(entry.registration(), reason.clone());
    state.retained.take();
    entry.request.lock().expect("registered request").take();
    entry.production.lock().expect("query pipeline").retained = false;
    let executing = matches!(state.phase, Phase::Executing);
    let mut effective = requested;
    let mut selected_error = None;
    if executing {
        entry.command.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(requested);
                true
            } else {
                false
            }
        });
        entry.progress.notify_waiters();
        loop {
            let progress = entry.progress.notified();
            tokio::pin!(progress);
            progress.as_mut().enable();
            let outcome = entry
                .production
                .lock()
                .expect("query pipeline")
                .outcome
                .take();
            if let Some(mut outcome) = outcome {
                entry
                    .production
                    .lock()
                    .expect("query pipeline")
                    .pipeline
                    .take();
                entry
                    .production
                    .lock()
                    .expect("query pipeline")
                    .columns
                    .take();
                if outcome.error.is_some() {
                    // Keep the original typed failure or already reconciled command receipt.
                    effective = outcome.disposition;
                    selected_error = outcome.error.take();
                    if let Some(settlement) = outcome.settlement.take() {
                        settlement.finish(effective);
                    }
                } else if let Some(settlement) = outcome.settlement.take() {
                    let effect = settlement.owner.external_effect_disposition();
                    let (finish, reason) = settlement.finish(requested);
                    let (disposition, error) = crate::driver::command_receipt(
                        entry.id.as_str(),
                        requested,
                        finish,
                        reason,
                        effect,
                    );
                    effective = disposition;
                    selected_error = Some(error);
                }
                break;
            }
            progress.await;
        }
    }
    let error = selected_error.unwrap_or_else(|| protocol_cancellation_error(entry, reason));
    let error =
        settle_client_result(control, entry, Some(error)).expect("cancel settles as a failure");
    let failed = error.failure();
    if failed == novarocks_query_application::query_control::ADMINISTRATIVELY_KILLED {
        effective = Disposition::Cancelled;
    } else if failed == novarocks_query_application::query_control::ABANDONED_QUERY {
        effective = Disposition::Abandon;
    }
    let page = terminal_page(entry, None, Some(error), HeaderMap::new(), None);
    if executing {
        entry.observation.settle(true, effective);
    }
    state.phase = Phase::Settled {
        token: state.next_token,
        page,
        abandoned: effective == Disposition::Abandon,
    };
    entry.progress.notify_waiters();
}
fn registration_error(id: &str, message: &str) -> TrinoQueryError {
    query_error_for_query(
        id,
        TrinoErrorSource::Service {
            error: &QueryServiceError::new(QueryServiceErrorKind::Internal, message),
            sql: "",
        },
    )
}

fn protocol_cancellation_error(
    entry: &QueryEntry,
    reason: QueryCancellationReason,
) -> TrinoQueryError {
    query_error_for_query_with_effect(
        entry.id.as_str(),
        TrinoErrorSource::Cancellation {
            reason: &reason,
            message: if reason == QueryCancellationReason::ClientCancelRequested {
                "Query canceled by client"
            } else {
                "Query abandoned by client"
            },
        },
        entry.external_effect_disposition(),
    )
}

fn original_cancellation_error(
    control: &QueryControlService,
    entry: &QueryEntry,
) -> Option<TrinoQueryError> {
    match control.client_query_cancellation(entry.registration())? {
        ClientQueryCancellation::Management(receipt) => Some(query_error_for_query_with_effect(
            entry.id.as_str(),
            TrinoErrorSource::ManagementCancellation(&receipt),
            entry.external_effect_disposition(),
        )),
        ClientQueryCancellation::Protocol(reason) => {
            Some(protocol_cancellation_error(entry, reason))
        }
    }
}

fn settle_client_result(
    control: &QueryControlService,
    entry: &QueryEntry,
    original_error: Option<TrinoQueryError>,
) -> Option<TrinoQueryError> {
    let proposed = original_error
        .as_ref()
        .map_or(ClientQueryConclusion::Finished, |error| {
            ClientQueryConclusion::Failed(error.failure())
        });
    let settled =
        control.settle_client_query(entry.registration(), proposed, ClientQueryTime::now());
    entry
        .original_text
        .lock()
        .expect("client query text custody")
        .take();
    match settled {
        Some(ClientQueryConclusion::Finished) => None,
        Some(ClientQueryConclusion::Failed(identity))
            if original_error
                .as_ref()
                .is_some_and(|error| error.failure() == identity) =>
        {
            original_error
        }
        Some(ClientQueryConclusion::AdministrativelyKilled(receipt)) => {
            Some(query_error_for_query_with_effect(
                entry.id.as_str(),
                TrinoErrorSource::ManagementCancellation(&receipt),
                entry.external_effect_disposition(),
            ))
        }
        Some(ClientQueryConclusion::Failed(identity)) => {
            original_cancellation_error(control, entry)
                .filter(|error| error.failure() == identity)
                .or_else(|| {
                    Some(registration_error(
                        entry.id.as_str(),
                        "client query conclusion differs from its original protocol error",
                    ))
                })
        }
        None => Some(registration_error(
            entry.id.as_str(),
            "client query lost its original protocol registration",
        )),
    }
}

fn registered_terminal(
    control: &QueryControlService,
    entry: &QueryEntry,
    state: &mut EntryState,
    error: TrinoQueryError,
) -> Arc<Page> {
    entry.request.lock().expect("registered request").take();
    let error =
        settle_client_result(control, entry, Some(error)).expect("registered terminal failure");
    let abandoned = error.failure() == novarocks_query_application::query_control::ABANDONED_QUERY;
    let page = terminal_page(entry, None, Some(error), HeaderMap::new(), None);
    state.phase = Phase::Settled {
        token: state.next_token,
        page: Arc::clone(&page),
        abandoned,
    };
    page
}

fn timed_envelope<'a>(
    entry: &'a QueryEntry,
    next: Option<u64>,
    state: &'a str,
    error: Option<&'a crate::error_mapping::TrinoQueryError>,
    update: Option<&'a str>,
) -> crate::envelope::Envelope<'a> {
    let mut value = envelope(
        entry.id.as_str(),
        &entry.base,
        &entry.slug,
        next,
        state,
        error,
        update,
    );
    value.stats.elapsed_time_millis =
        entry.created.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let start = *entry.started.lock().expect("query start clock");
    let queued = start.map_or_else(
        || entry.created.elapsed(),
        |start| start.duration_since(entry.created),
    );
    value.stats.queued_time_millis = queued.as_millis().min(u64::MAX as u128) as u64;
    value
}
fn ordinary_page(
    entry: &QueryEntry,
    next: Option<u64>,
    state: &str,
    columns: Option<&Bytes>,
    rows: Option<Bytes>,
    window: Option<novarocks_workload_control::ResultWindowAlias>,
    update: Option<&str>,
    count: Option<u64>,
) -> Arc<Page> {
    let mut envelope = timed_envelope(entry, next, state, None, update);
    envelope.update_count = count;
    Arc::new(Page {
        chunks: page_chunks(envelope, columns, rows),
        headers: HeaderMap::new(),
        status: StatusCode::OK,
        _window: window,
    })
}
fn terminal_page(
    entry: &QueryEntry,
    columns: Option<&Bytes>,
    error: Option<crate::error_mapping::TrinoQueryError>,
    headers: HeaderMap,
    update: Option<&str>,
) -> Arc<Page> {
    terminal_page_with_count(entry, columns, error, headers, update, None)
}
fn terminal_page_with_count(
    entry: &QueryEntry,
    columns: Option<&Bytes>,
    error: Option<crate::error_mapping::TrinoQueryError>,
    headers: HeaderMap,
    update: Option<&str>,
    count: Option<u64>,
) -> Arc<Page> {
    let state = if error.is_some() {
        "FAILED"
    } else {
        "FINISHED"
    };
    let mut envelope = timed_envelope(entry, None, state, error.as_ref(), update);
    envelope.update_count = count;
    Arc::new(Page {
        chunks: page_chunks(envelope, columns, None),
        headers,
        status: StatusCode::OK,
        _window: None,
    })
}

fn command_update_type(
    admission: novarocks_query_application::session::StatementAdmission,
) -> Option<&'static str> {
    use novarocks_query_application::session::StatementOutputShape;
    use novarocks_query_application::statement_kind::StatementKind;
    match admission.shape {
        StatementOutputShape::Rows | StatementOutputShape::Empty => update_type(admission.kind),
        StatementOutputShape::ResultSet
            if matches!(
                admission.kind,
                StatementKind::AddFiles
                    | StatementKind::Optimize
                    | StatementKind::RewriteManifests
                    | StatementKind::ExpireSnapshots
                    | StatementKind::RemoveOrphanFiles
            ) =>
        {
            update_type(admission.kind)
        }
        StatementOutputShape::ResultSet | StatementOutputShape::UndeterminedCall => None,
    }
}

#[cfg(test)]
mod command_update_type_tests {
    use super::*;
    use novarocks_query_application::{
        session::{StatementAdmission, StatementOutputShape},
        statement_kind::StatementKind,
    };

    #[test]
    fn table_execute_metrics_keep_update_type_with_result_set() {
        for kind in [
            StatementKind::AddFiles,
            StatementKind::Optimize,
            StatementKind::RewriteManifests,
            StatementKind::ExpireSnapshots,
            StatementKind::RemoveOrphanFiles,
        ] {
            assert_eq!(
                command_update_type(StatementAdmission {
                    kind,
                    shape: StatementOutputShape::ResultSet
                }),
                Some("ALTER TABLE EXECUTE")
            );
        }
        assert_eq!(
            command_update_type(StatementAdmission {
                kind: StatementKind::CreateTable,
                shape: StatementOutputShape::ResultSet
            }),
            None
        );
    }
}
