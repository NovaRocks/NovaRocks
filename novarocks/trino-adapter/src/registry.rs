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
    entry::{EntryState, Outcome, Page, Phase, Production, QueryEntry},
    envelope::{envelope, page_chunks},
    error_mapping::{TrinoErrorSource, query_error_for_query},
    request::{ParsedStatementRequest, session_effect_headers},
    settlement::Disposition,
    update_type::update_type,
};
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use novarocks_query_application::{
    session::QuerySessionFactory, session_control::QueryControlService,
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
        tracing::debug!(query_id = %id, observation = ?request.observation(), "Trino query registered");
        let entry = Arc::new(QueryEntry {
            id,
            slug,
            base,
            request: Mutex::new(Some(request)),
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
            }),
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
        if self.draining.load(Ordering::Acquire) {
            return terminal_page(
                &entry,
                None,
                Some(query_error_for_query(
                    entry.id.as_str(),
                    TrinoErrorSource::FrontendDraining,
                )),
                HeaderMap::new(),
                None,
            );
        }
        let page = ordinary_page(&entry, Some(0), "QUEUED", None, None, None);
        self.entries
            .lock()
            .expect("query registry")
            .insert(entry.id.to_string(), Arc::clone(&entry));
        let registry = Arc::downgrade(self);
        let timer_entry = Arc::downgrade(&entry);
        let timer = tokio::spawn(async move {
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
                tokio::select! {
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {},
                    _ = entry.requests.notified() => continue,
                    _ = &mut shutdown => break,
                }
                if entry.touched.lock().expect("query clock").elapsed() < timeout {
                    continue;
                }
                let mut state = entry.state.lock().await;
                // Recheck under the same request lock: a concurrent request wins by touching first.
                if entry.touched.lock().expect("query clock").elapsed() < timeout {
                    continue;
                }
                match state.phase {
                    Phase::Registered | Phase::Settled { .. } => {
                        registry
                            .entries
                            .lock()
                            .expect("query registry")
                            .remove(entry.id.as_str());
                        break;
                    }
                    Phase::Executing => {
                        cancel_locked(&entry, &mut state, Disposition::Abandon).await;
                        entry.touch();
                        tracing::debug!(query_id = %entry.id, "Trino query abandoned");
                    }
                }
            }
        });
        *entry.timer.lock().expect("query timer") = Some(timer);
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
        entry.touch();
        Some(entry)
    }
    pub(crate) async fn get(
        self: &Arc<Self>,
        entry: &Arc<QueryEntry>,
        token: u64,
    ) -> Result<Arc<Page>, StatusCode> {
        let mut state = entry.state.lock().await;
        if let Phase::Settled {
            token: terminal_token,
            page,
            abandoned,
        } = &state.phase
        {
            if token == *terminal_token || *abandoned {
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
                return Ok(Arc::clone(page));
            }
            if token < *last {
                return Err(StatusCode::GONE);
            }
        }
        if token != state.next_token {
            return Err(StatusCode::NOT_FOUND);
        }
        // ACK changes logical retention. In-flight bodies keep their own original backing/window.
        state.retained.take();
        entry.production.lock().expect("query pipeline").retained = false;
        entry.progress.notify_waiters();
        if matches!(state.phase, Phase::Registered) {
            if self.draining.load(Ordering::Acquire) {
                let page = terminal_page(
                    entry,
                    None,
                    Some(query_error_for_query(
                        entry.id.as_str(),
                        TrinoErrorSource::FrontendDraining,
                    )),
                    HeaderMap::new(),
                    None,
                );
                entry.request.lock().expect("registered request").take();
                state.phase = Phase::Settled {
                    token,
                    page: Arc::clone(&page),
                    abandoned: false,
                };
                return Ok(page);
            }
            let request = entry
                .request
                .lock()
                .expect("registered request")
                .take()
                .expect("first GET owns the original request");
            state.phase = Phase::Executing;
            entry.observation.start();
            *entry.started.lock().expect("query start clock") = Some(Instant::now());
            let task_entry = Arc::clone(entry);
            let sessions = Arc::clone(&self.sessions);
            let join = tokio::spawn(async move {
                use futures::FutureExt;
                let guard_entry = Arc::clone(&task_entry);
                if std::panic::AssertUnwindSafe(crate::driver::run(task_entry, sessions, request))
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
            let next = token.checked_add(1).ok_or(StatusCode::NOT_FOUND)?;
            let window = production.pipeline.as_ref().map(|p| p.window.clone());
            let page = ordinary_page(
                entry,
                Some(next),
                "RUNNING",
                production.columns.as_ref(),
                Some(rows),
                window,
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
            let page = settle_outcome(entry, columns, outcome);
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
        );
        production.retained = true;
        state.next_token = next;
        state.retained = Some((token, Arc::clone(&page)));
        Ok(page)
    }
    pub(crate) async fn cancel(&self, entry: &Arc<QueryEntry>) {
        let mut state = entry.state.lock().await;
        if !matches!(state.phase, Phase::Settled { .. }) {
            cancel_locked(entry, &mut state, Disposition::ClientCancel).await;
            tracing::debug!(query_id = %entry.id, "Trino query canceled by DELETE");
        }
    }
    pub(crate) fn begin_drain(&self) {
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
                if !matches!(state.phase, Phase::Settled { .. }) {
                    cancel_locked(&entry, &mut state, Disposition::Abandon).await;
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
fn settle_outcome(entry: &QueryEntry, columns: Option<Bytes>, mut outcome: Outcome) -> Arc<Page> {
    let headers = if outcome.error.is_none() {
        session_effect_headers(&outcome.effects).unwrap_or_else(|error| {
            outcome.disposition = Disposition::Fail;
            outcome.error = Some(crate::driver::internal(
                entry.id.as_str(),
                &format!("invalid application session effect header: {error}"),
            ));
            HeaderMap::new()
        })
    } else {
        HeaderMap::new()
    };
    if let Some(settlement) = outcome.settlement.take() {
        settlement.finish(outcome.disposition);
    }
    entry.observation.settle(true, outcome.disposition);
    let update = if columns.is_none() && outcome.error.is_none() {
        update_type(outcome.kind)
    } else {
        None
    };
    terminal_page(entry, columns.as_ref(), outcome.error, headers, update)
}
async fn cancel_locked(entry: &QueryEntry, state: &mut EntryState, requested: Disposition) {
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
                    let (finish, reason) = settlement.finish(requested);
                    let (disposition, error) = crate::driver::command_receipt(
                        entry.id.as_str(),
                        requested,
                        finish,
                        reason,
                    );
                    effective = disposition;
                    selected_error = Some(error);
                }
                break;
            }
            progress.await;
        }
    }
    let error = selected_error.unwrap_or_else(|| {
        let reason = if requested == Disposition::ClientCancel {
            novarocks_query_application::cancellation::QueryCancellationReason::ClientCancelRequested
        } else { novarocks_query_application::cancellation::QueryCancellationReason::ClientDisconnected };
        query_error_for_query(entry.id.as_str(), TrinoErrorSource::Cancellation { reason: &reason,
            message: if requested == Disposition::ClientCancel { "Query canceled by client" } else { "Query abandoned by client" } })
    });
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
) -> Arc<Page> {
    Arc::new(Page {
        chunks: page_chunks(
            timed_envelope(entry, next, state, None, None),
            columns,
            rows,
        ),
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
    let state = if error.is_some() {
        "FAILED"
    } else {
        "FINISHED"
    };
    Arc::new(Page {
        chunks: page_chunks(
            timed_envelope(entry, None, state, error.as_ref(), update),
            columns,
            None,
        ),
        headers,
        status: StatusCode::OK,
        _window: None,
    })
}
