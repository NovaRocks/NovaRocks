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
    ClientSession, Column, SessionEffects, StatementFailure, StatementResult, StatementTrace,
    StatementTraceState, TrinoError, decode_value, session::insert, trace::TraceProgress,
};
use reqwest::{Method, Url, blocking::Client, header::HeaderMap};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub source: String,
}
#[derive(Clone)]
pub struct TrinoClient {
    client: Client,
    base: Url,
    config: ClientConfig,
}
impl TrinoClient {
    pub fn new(config: ClientConfig) -> Result<Self, StatementFailure> {
        let mut base = Url::parse("http://localhost")
            .map_err(|e| StatementFailure::protocol(e.to_string()))?;
        base.set_host(Some(&config.host))
            .map_err(|e| StatementFailure::protocol(e.to_string()))?;
        base.set_port(Some(config.port))
            .map_err(|_| StatementFailure::protocol("invalid Trino port"))?;
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(transport)?;
        Ok(Self {
            client,
            base,
            config,
        })
    }
    pub fn submit(
        &self,
        session: &ClientSession,
        sql: &str,
        deadline: Instant,
    ) -> Result<PendingStatement, StatementFailure> {
        self.submit_observed(session, sql, deadline, |_| {})
    }
    pub fn submit_observed(
        &self,
        session: &ClientSession,
        sql: &str,
        deadline: Instant,
        observe: impl FnOnce(StatementTrace<'_>),
    ) -> Result<PendingStatement, StatementFailure> {
        let mut progress = TraceProgress::default();
        let result = self.submit_inner(session, sql, deadline, &mut progress);
        observe(progress.observation(
            if result.is_ok() {
                StatementTraceState::Submitted
            } else {
                StatementTraceState::Failed
            },
            result.as_ref().err(),
        ));
        result
    }
    fn submit_inner(
        &self,
        session: &ClientSession,
        sql: &str,
        deadline: Instant,
        progress: &mut TraceProgress,
    ) -> Result<PendingStatement, StatementFailure> {
        let mut headers = session.request_headers()?;
        insert(&mut headers, "x-trino-user", &self.config.user)?;
        insert(&mut headers, "x-trino-source", &self.config.source)?;
        insert(&mut headers, "accept-encoding", "identity")?;
        let uri = self
            .base
            .join("/v1/statement")
            .map_err(|e| StatementFailure::protocol(e.to_string()))?;
        let response = self.request(Method::POST, uri, &headers, Some(sql), deadline)?;
        let page = parse_page(&response.body)?;
        *progress = TraceProgress {
            query_id: Some(page.id.clone()),
            initial_next_uri: page.next_uri.clone(),
            pages: 1,
            update_type: page.update_type.clone(),
        };
        if let Some(error) = page.error {
            return Err(StatementFailure::Trino(error));
        }
        let cancel = CancelHandle {
            client: self.clone(),
            headers: headers.clone(),
            next: Arc::new(Mutex::new(None)),
        };
        cancel.set_next(page.next_uri.as_deref())?;
        Ok(PendingStatement {
            query_id: page.id.clone(),
            initial_next_uri: page.next_uri.clone(),
            client: self.clone(),
            headers,
            initial: Some((page, response.headers)),
            progress: progress.clone(),
            cancel,
            deadline,
        })
    }
    pub fn execute(
        &self,
        session: &mut ClientSession,
        sql: &str,
        deadline: Instant,
    ) -> Result<StatementResult, StatementFailure> {
        let (result, effects) = self.submit(session, sql, deadline)?.run(deadline)?;
        session.apply(&effects);
        Ok(result)
    }
    fn continuation(&self, uri: &str) -> Result<Url, StatementFailure> {
        let target = Url::parse(uri)
            .map_err(|e| StatementFailure::protocol(format!("invalid continuation URI: {e}")))?;
        if target.scheme() != self.base.scheme()
            || target.host_str() != self.base.host_str()
            || target.port_or_known_default() != self.base.port_or_known_default()
            || !target.username().is_empty()
            || target.password().is_some()
            || target.fragment().is_some()
        {
            return Err(StatementFailure::protocol(format!(
                "refused foreign continuation URI: {uri}"
            )));
        }
        Ok(target)
    }
    fn request(
        &self,
        method: Method,
        uri: Url,
        headers: &HeaderMap,
        sql: Option<&str>,
        deadline: Instant,
    ) -> Result<Response, StatementFailure> {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or(StatementFailure::Deadline)?;
        let mut request = self
            .client
            .request(method, uri)
            .headers(headers.clone())
            .timeout(remaining);
        if let Some(sql) = sql {
            request = request.body(sql.to_owned());
        }
        let response = request.send().map_err(|e| request_error(e, deadline))?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .map_err(|e| request_error(e, deadline))?
            .to_vec();
        if status != 200 {
            return Err(StatementFailure::Http {
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        Ok(Response { headers, body })
    }
}
fn transport(error: reqwest::Error) -> StatementFailure {
    StatementFailure::Transport(error.to_string())
}
fn request_error(error: reqwest::Error, deadline: Instant) -> StatementFailure {
    if error.is_timeout() || Instant::now() >= deadline {
        StatementFailure::Deadline
    } else {
        transport(error)
    }
}
struct Response {
    headers: HeaderMap,
    body: Vec<u8>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Page {
    id: String,
    next_uri: Option<String>,
    columns: Option<Vec<Column>>,
    data: Option<Vec<Vec<Box<RawValue>>>>,
    error: Option<TrinoError>,
    update_type: Option<String>,
}

fn parse_page(bytes: &[u8]) -> Result<Page, StatementFailure> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    // A nested Trino type signature uses several JSON containers per SQL level.
    deserializer.disable_recursion_limit();
    let page = Page::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(page)
}

/// The statement is registered but not yet executed by NovaRocks. The caller
/// may arm observations using query_id before run sends the first GET.
pub struct PendingStatement {
    pub query_id: String,
    pub initial_next_uri: Option<String>,
    client: TrinoClient,
    headers: HeaderMap,
    initial: Option<(Page, HeaderMap)>,
    progress: TraceProgress,
    cancel: CancelHandle,
    deadline: Instant,
}
impl PendingStatement {
    /// Returns only the protocol facts already received during submission.
    pub fn submission_observation(&self) -> StatementTrace<'_> {
        self.progress
            .observation(StatementTraceState::Submitted, None)
    }
    pub fn cancel_handle(&self) -> CancelHandle {
        self.cancel.clone()
    }
    pub fn run(
        self,
        deadline: Instant,
    ) -> Result<(StatementResult, SessionEffects), StatementFailure> {
        self.run_observed(deadline, |_| {})
    }
    pub fn run_observed(
        mut self,
        deadline: Instant,
        observe: impl FnOnce(StatementTrace<'_>),
    ) -> Result<(StatementResult, SessionEffects), StatementFailure> {
        let deadline = deadline.min(self.deadline);
        let result = self.run_inner(deadline);
        match &result {
            Err(StatementFailure::Deadline) => {
                // The original request deadline has expired. A bounded cancellation
                // request is best effort and never extends the statement's verdict.
                let _ = self.cancel.delete(Instant::now() + Duration::from_secs(1));
            }
            Err(StatementFailure::UnsupportedSessionHeader(_) | StatementFailure::Protocol(_)) => {
                // A local refusal is not server completion. Retire the known
                // continuation while preserving the original error and deadline.
                let _ = self.cancel.delete(deadline);
            }
            _ => {}
        }
        observe(self.progress.observation(
            if result.is_ok() {
                StatementTraceState::Finished
            } else {
                StatementTraceState::Failed
            },
            result.as_ref().err(),
        ));
        result
    }
    fn run_inner(
        &mut self,
        deadline: Instant,
    ) -> Result<(StatementResult, SessionEffects), StatementFailure> {
        let mut effects = SessionEffects::default();
        let mut columns: Option<Vec<Column>> = None;
        let mut rows = Vec::new();
        let mut pages = 0;
        let mut update_type = None;
        let (mut page, mut headers) = self
            .initial
            .take()
            .ok_or_else(|| StatementFailure::protocol("statement already consumed"))?;
        loop {
            if Instant::now() >= deadline {
                return Err(StatementFailure::Deadline);
            }
            if page.id != self.query_id {
                return Err(StatementFailure::protocol(
                    "query identity changed between pages",
                ));
            }
            // Publish only a validated continuation before interpreting local
            // state. A decode/header refusal can then cancel this exact query;
            // a foreign URI leaves the previous known local URI in custody.
            self.cancel.set_next(page.next_uri.as_deref())?;
            effects.absorb(&headers)?;
            pages += 1;
            if let Some(new) = page.columns {
                for col in &new {
                    col.type_signature.validate()?;
                }
                if columns.as_ref().is_some_and(|old| old != &new) {
                    return Err(StatementFailure::protocol(
                        "columns changed between Trino pages",
                    ));
                }
                columns = Some(new);
            }
            if let Some(data) = page.data.filter(|data| !data.is_empty()) {
                let cols = columns.as_ref().ok_or_else(|| {
                    StatementFailure::protocol("Trino data arrived before columns")
                })?;
                for row in data {
                    if row.len() != cols.len() {
                        return Err(StatementFailure::protocol(
                            "Trino data width differs from columns",
                        ));
                    }
                    rows.push(
                        row.iter()
                            .zip(cols)
                            .map(|(cell, col)| decode_value(&col.type_signature, cell))
                            .collect::<Result<_, _>>()?,
                    );
                }
            }
            if let Some(error) = page.error {
                return Err(StatementFailure::Trino(error));
            }
            if let Some(update) = page.update_type {
                update_type = Some(update);
            }
            let Some(next) = page.next_uri else {
                return Ok((
                    StatementResult {
                        columns: columns.unwrap_or_default(),
                        rows,
                        query_id: self.query_id.clone(),
                        pages,
                        update_type,
                    },
                    effects,
                ));
            };
            let target = self.client.continuation(&next)?;
            let response =
                self.client
                    .request(Method::GET, target, &self.headers, None, deadline)?;
            page = parse_page(&response.body)?;
            self.progress.pages += 1;
            if page.update_type.is_some() {
                self.progress.update_type = page.update_type.clone();
            }
            headers = response.headers;
        }
    }
}
#[derive(Clone)]
pub struct CancelHandle {
    client: TrinoClient,
    headers: HeaderMap,
    next: Arc<Mutex<Option<Url>>>,
}
impl CancelHandle {
    fn set_next(&self, uri: Option<&str>) -> Result<(), StatementFailure> {
        let next = uri.map(|u| self.client.continuation(u)).transpose()?;
        *self
            .next
            .lock()
            .map_err(|_| StatementFailure::protocol("Trino cancel state poisoned"))? = next;
        Ok(())
    }
    /// Cancel the current continuation. Clones may be used on another thread.
    pub fn delete(&self, deadline: Instant) -> Result<(), StatementFailure> {
        let uri = self
            .next
            .lock()
            .map_err(|_| StatementFailure::protocol("Trino cancel state poisoned"))?
            .clone();
        let Some(uri) = uri else {
            return Ok(());
        };
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or(StatementFailure::Deadline)?;
        let response = self
            .client
            .client
            .delete(uri)
            .headers(self.headers.clone())
            .timeout(remaining)
            .send()
            .map_err(|e| request_error(e, deadline))?;
        let status = response.status().as_u16();
        let body = response.bytes().map_err(|e| request_error(e, deadline))?;
        if !matches!(status, 200 | 204) {
            return Err(StatementFailure::Http {
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        Ok(())
    }
}
