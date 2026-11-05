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

//! Raw Trino response actor for the native System protocol scenarios.
//!
//! The oracle consumes original HTTP status, repeated headers and body bytes.
//! It does not reuse adapter parsing, row framing or presentation helpers.
use anyhow::{Context, Result, ensure};
use reqwest::{
    Method,
    blocking::Client,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[derive(Clone)]
pub(crate) struct TrinoClient {
    client: Client,
    base: String,
    deadline: Instant,
    user: String,
    session: Vec<(String, String)>,
    root: Arc<PathBuf>,
    sequence: Arc<AtomicU64>,
}
#[derive(Clone, Debug)]
pub(crate) struct RawResponse {
    pub(crate) status: u16,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Vec<u8>,
}
impl RawResponse {
    pub(crate) fn json(&self) -> Result<Value> {
        serde_json::from_slice(&self.body).context("decode original Trino JSON body")
    }
    pub(crate) fn next_uri(&self) -> Result<Option<String>> {
        let value = self.json()?;
        match value.get("nextUri") {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(uri)) => Ok(Some(uri.clone())),
            _ => anyhow::bail!("Trino nextUri is not a string"),
        }
    }
    pub(crate) fn semantic_headers(&self) -> Vec<(String, Vec<u8>)> {
        let mut headers = self
            .headers
            .iter()
            .filter(|(name, _)| {
                name.as_str().starts_with("x-trino-") || name.as_str() == "content-type"
            })
            .map(|(name, value)| (name.to_string(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        headers.sort();
        headers
    }
    pub(crate) fn assert_success(&self) -> Result<Value> {
        ensure!(
            self.status == 200,
            "Trino response HTTP {}: {:?}",
            self.status,
            String::from_utf8_lossy(&self.body)
        );
        let json = self.json()?;
        ensure!(
            json.get("error").is_none(),
            "Trino query failed: {}",
            json["error"]
        );
        Ok(json)
    }
}
/// One original response reader, idle between explicit bounded read commands.
/// HTTP headers, first body bytes, EOF, response Drop and reader Join are
/// separate observations. Holding this owner never drains the response.
pub(crate) struct HeldHttpResponse {
    head: RawResponse,
    commands: std::sync::mpsc::Sender<BodyCommand>,
    reader: Option<std::thread::JoinHandle<ReaderExit>>,
    deadline: Instant,
    evidence: PathBuf,
    first_wait_failure: Option<String>,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct HeldBodyObservation {
    pub(crate) first_byte_observed: bool,
    pub(crate) eof_observed: bool,
    pub(crate) read_bytes: u64,
    pub(crate) read_calls: u64,
}
#[derive(Debug)]
pub(crate) struct BodyRead {
    pub(crate) bytes: Vec<u8>,
    pub(crate) eof: bool,
}
#[derive(Debug, Serialize)]
struct ReaderExit {
    observation: HeldBodyObservation,
    response_dropped: bool,
    failures: Vec<String>,
}
#[derive(Debug, Serialize)]
pub(crate) struct JoinedHttpResponse {
    pub(crate) observation: HeldBodyObservation,
    pub(crate) response_dropped: bool,
    pub(crate) reader_joined: bool,
    pub(crate) reader_panicked: bool,
    pub(crate) failures: Vec<String>,
}
impl JoinedHttpResponse {
    pub(crate) fn require_clean(&self) -> Result<()> {
        ensure!(
            self.reader_joined
                && !self.reader_panicked
                && self.response_dropped
                && self.failures.is_empty(),
            "original HTTP reader settlement failed: {self:?}"
        );
        Ok(())
    }
}
enum BodyCommand {
    Read(
        usize,
        std::sync::mpsc::SyncSender<std::result::Result<BodyRead, String>>,
    ),
    Observe(std::sync::mpsc::SyncSender<HeldBodyObservation>),
    Close,
}
fn body_remaining(deadline: Instant) -> std::result::Result<std::time::Duration, String> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|value| !value.is_zero())
        .ok_or_else(|| "Trino actor original response deadline expired".to_owned())
}
fn write_body_json(path: &Path, value: &impl Serialize) -> std::result::Result<(), String> {
    fs::write(
        path,
        serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}
fn original_body_reader(
    request: reqwest::blocking::RequestBuilder,
    commands: std::sync::mpsc::Receiver<BodyCommand>,
    header: std::sync::mpsc::SyncSender<std::result::Result<RawResponse, String>>,
    deadline: Instant,
    evidence: PathBuf,
) -> ReaderExit {
    let mut state = ReaderExit {
        observation: HeldBodyObservation {
            first_byte_observed: false,
            eof_observed: false,
            read_bytes: 0,
            read_calls: 0,
        },
        response_dropped: false,
        failures: Vec::new(),
    };
    let request = match body_remaining(deadline) {
        Ok(remaining) => request.timeout(remaining),
        Err(error) => {
            state.failures.push(error.clone());
            let _ = header.send(Err(error));
            return state;
        }
    };
    let mut response = match request.send() {
        Ok(response) => response,
        Err(error) => {
            let error = format!("open original held HTTP response: {error}");
            state.failures.push(error.clone());
            let _ = header.send(Err(error));
            return state;
        }
    };
    let head = RawResponse {
        status: response.status().as_u16(),
        headers: response.headers().clone(),
        body: Vec::new(),
    };
    let headers = head
        .headers
        .iter()
        .map(|(name, value)| (name.to_string(), value.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    if let Err(error) = write_body_json(
        &evidence.with_extension("headers.json"),
        &serde_json::json!({"status":head.status,"headers":headers,"body_read":false}),
    ) {
        state
            .failures
            .push(format!("record original response headers: {error}"));
        let _ = header.send(Err(state.failures[0].clone()));
    } else if header.send(Ok(head)).is_ok() {
        loop {
            let remaining = match body_remaining(deadline) {
                Ok(remaining) => remaining,
                Err(error) => {
                    state.failures.push(error);
                    break;
                }
            };
            let command = match commands.recv_timeout(remaining) {
                Ok(command) => command,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    state
                        .failures
                        .push("Trino actor original response deadline expired while held".into());
                    break;
                }
            };
            match command {
                BodyCommand::Close => break,
                BodyCommand::Observe(reply) => {
                    let _ = reply.send(state.observation.clone());
                }
                BodyCommand::Read(max_bytes, reply) => {
                    if let Some(error) = state.failures.first() {
                        let _ = reply.send(Err(error.clone()));
                        continue;
                    }
                    let read = (|| -> std::result::Result<BodyRead, String> {
                        body_remaining(deadline)?;
                        let mut bytes = vec![0; max_bytes];
                        state.observation.read_calls += 1;
                        let count = response.read(&mut bytes).map_err(|error| {
                            format!("read original held HTTP response: {error}")
                        })?;
                        bytes.truncate(count);
                        state.observation.eof_observed |= count == 0;
                        state.observation.first_byte_observed |= count > 0;
                        state.observation.read_bytes = state
                            .observation
                            .read_bytes
                            .checked_add(count as u64)
                            .ok_or("held HTTP byte observation overflow")?;
                        // Each original chunk is retained on disk; the reader
                        // owns only this bounded chunk, never the full result.
                        fs::write(
                            evidence.with_extension(format!(
                                "read-{:06}.body",
                                state.observation.read_calls
                            )),
                            &bytes,
                        )
                        .map_err(|error| format!("record original body chunk: {error}"))?;
                        Ok(BodyRead {
                            bytes,
                            eof: count == 0,
                        })
                    })();
                    if let Err(error) = &read {
                        state.failures.push(error.clone());
                    }
                    let _ = reply.send(read);
                }
            }
        }
    }
    // The same original thread drops the real Response before publishing its
    // exit. No local terminal flag or timeout substitutes for this Drop/Join.
    drop(response);
    state.response_dropped = true;
    state
}
impl HeldHttpResponse {
    pub(crate) fn status(&self) -> u16 {
        self.head.status
    }
    pub(crate) fn headers(&self) -> &HeaderMap {
        &self.head.headers
    }
    fn receive<T>(&mut self, response: std::sync::mpsc::Receiver<T>) -> Result<T> {
        let result = body_remaining(self.deadline).and_then(|remaining| {
            response
                .recv_timeout(remaining)
                .map_err(|error| format!("wait for original HTTP reader: {error}"))
        });
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                self.first_wait_failure.get_or_insert_with(|| error.clone());
                anyhow::bail!("{error}")
            }
        }
    }
    /// One actual bounded reader call. No body is read between commands.
    pub(crate) fn read_step(&mut self, max_bytes: usize) -> Result<BodyRead> {
        ensure!(max_bytes > 0, "held response read needs a nonempty bound");
        let (reply, response) = std::sync::mpsc::sync_channel(1);
        self.commands
            .send(BodyCommand::Read(max_bytes, reply))
            .context("original HTTP reader exited before read")?;
        self.receive(response)?.map_err(anyhow::Error::msg)
    }
    pub(crate) fn read_prefix(&mut self, bytes: usize) -> Result<Vec<u8>> {
        ensure!(bytes > 0, "held response prefix needs a nonempty bound");
        let mut prefix = Vec::with_capacity(bytes);
        while prefix.len() < bytes {
            let next = self.read_step(bytes - prefix.len())?;
            ensure!(
                !next.eof,
                "original HTTP response reached EOF before requested prefix"
            );
            prefix.extend_from_slice(&next.bytes);
        }
        Ok(prefix)
    }
    pub(crate) fn observe_held(&mut self) -> Result<HeldBodyObservation> {
        let (reply, response) = std::sync::mpsc::sync_channel(1);
        self.commands
            .send(BodyCommand::Observe(reply))
            .context("original HTTP reader exited before held observation")?;
        self.receive(response)
    }
    /// Interrupt without draining unread bytes, then join that same reader.
    pub(crate) fn interrupt(mut self) -> JoinedHttpResponse {
        self.finish("interrupted", false)
    }
    /// Settle only an EOF the caller actually read. This never drains the body.
    pub(crate) fn settle(mut self) -> JoinedHttpResponse {
        self.finish("eof", true)
    }
    fn finish(&mut self, reason: &str, require_eof: bool) -> JoinedHttpResponse {
        let _ = self.commands.send(BodyCommand::Close);
        let mut result = match self
            .reader
            .take()
            .expect("one original HTTP reader Join")
            .join()
        {
            Ok(exit) => JoinedHttpResponse {
                observation: exit.observation,
                response_dropped: exit.response_dropped,
                reader_joined: true,
                reader_panicked: false,
                failures: exit.failures,
            },
            Err(_) => JoinedHttpResponse {
                observation: HeldBodyObservation {
                    first_byte_observed: false,
                    eof_observed: false,
                    read_bytes: 0,
                    read_calls: 0,
                },
                response_dropped: false,
                reader_joined: true,
                reader_panicked: true,
                failures: vec!["original HTTP reader panicked; body Drop remains unproven".into()],
            },
        };
        if let Some(error) = self.first_wait_failure.take() {
            result.failures.insert(0, error);
        }
        if require_eof && !result.observation.eof_observed {
            result
                .failures
                .push("settle requires an originally observed response EOF".into());
        }
        if let Err(error) = write_body_json(
            &self.evidence.with_extension("settled.json"),
            &serde_json::json!({"reason":reason,"original_reader":result}),
        ) {
            result
                .failures
                .push(format!("record original HTTP reader settlement: {error}"));
        }
        result
    }
}
impl Drop for HeldHttpResponse {
    fn drop(&mut self) {
        if self.reader.is_some() {
            let _ = self.finish("owner-drop", false);
        }
    }
}

impl TrinoClient {
    pub(crate) fn new(port: u16, user: &str, deadline: Instant, root: &Path) -> Result<Self> {
        fs::create_dir_all(root).context("create Trino HTTP evidence directory")?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "Trino actor original deadline expired"
        );
        Ok(Self {
            client: Client::builder()
                .connect_timeout(remaining)
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()?,
            base: format!("http://127.0.0.1:{port}"),
            deadline,
            user: user.into(),
            session: Vec::new(),
            root: Arc::new(root.to_owned()),
            sequence: Arc::new(AtomicU64::new(0)),
        })
    }
    pub(crate) fn with_session(&self, headers: &[(&str, &str)]) -> Self {
        let mut client = self.clone();
        for (name, value) in headers {
            client
                .session
                .retain(|(old, _)| !old.eq_ignore_ascii_case(name));
            client.session.push(((*name).into(), (*value).into()));
        }
        client
    }
    pub(crate) fn base(&self) -> &str {
        &self.base
    }
    pub(crate) fn submit(&self, sql: &str) -> Result<RawResponse> {
        self.request(
            Method::POST,
            &format!("{}/v1/statement", self.base),
            Some(sql),
            &[],
        )
    }
    pub(crate) fn get(&self, uri: &str) -> Result<RawResponse> {
        self.request(Method::GET, uri, None, &[])
    }
    /// Receive the original response headers, retaining its unconsumed body on
    /// one original reader thread until explicit reads, interruption or EOF.
    pub(crate) fn open_get(&self, uri: &str) -> Result<HeldHttpResponse> {
        let request = self.build_request(Method::GET, uri, None, &[])?;
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let evidence = self.root.join(format!("held-response-{sequence:06}"));
        write_body_json(
            &evidence.with_extension("request.json"),
            &serde_json::json!({"method":"GET","uri":uri,"body_read":false}),
        )
        .map_err(anyhow::Error::msg)?;
        let (commands, command_receiver) = std::sync::mpsc::channel();
        let (header, headers) = std::sync::mpsc::sync_channel(1);
        let deadline = self.deadline;
        let original_evidence = evidence.clone();
        let reader = std::thread::Builder::new()
            .name("trino-original-body".into())
            .spawn(move || {
                original_body_reader(
                    request,
                    command_receiver,
                    header,
                    deadline,
                    original_evidence,
                )
            })?;
        let mut held = HeldHttpResponse {
            head: RawResponse {
                status: 0,
                headers: HeaderMap::new(),
                body: Vec::new(),
            },
            commands,
            reader: Some(reader),
            deadline,
            evidence,
            first_wait_failure: None,
        };
        match held.receive(headers) {
            Ok(Ok(head)) => {
                held.head = head;
                Ok(held)
            }
            result => {
                let primary = match result {
                    Ok(Err(error)) => error,
                    Err(error) => format!("{error:#}"),
                    Ok(Ok(_)) => unreachable!("successful original headers returned above"),
                };
                let settled = held.finish("headers-error", false);
                anyhow::bail!("{primary}; original HTTP reader joined: {settled:?}")
            }
        }
    }

    pub(crate) fn head(&self, uri: &str) -> Result<RawResponse> {
        self.request(Method::HEAD, uri, None, &[])
    }
    pub(crate) fn delete(&self, uri: &str) -> Result<RawResponse> {
        self.request(Method::DELETE, uri, None, &[])
    }
    pub(crate) fn request(
        &self,
        method: Method,
        uri: &str,
        sql: Option<&str>,
        extra: &[(&str, &str)],
    ) -> Result<RawResponse> {
        let request = self.build_request(method.clone(), uri, sql, extra)?;
        let response = request
            .send()
            .with_context(|| format!("send {method} {uri}"))?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .context("read original Trino body")?
            .to_vec();
        let raw = RawResponse {
            status,
            headers,
            body,
        };
        self.record(method.as_str(), uri, sql, &raw, false)?;
        Ok(raw)
    }
    fn build_request(
        &self,
        method: Method,
        uri: &str,
        sql: Option<&str>,
        extra: &[(&str, &str)],
    ) -> Result<reqwest::blocking::RequestBuilder> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "Trino actor original deadline expired before {method} {uri}"
        );
        let target = reqwest::Url::parse(uri).context("parse returned Trino URI")?;
        let expected = reqwest::Url::parse(&self.base)?;
        ensure!(
            target.scheme() == "http"
                && target.host_str() == expected.host_str()
                && target.port_or_known_default() == expected.port_or_known_default()
                && target.username().is_empty()
                && target.password().is_none()
                && target.fragment().is_none(),
            "Trino actor refuses a foreign continuation URI: {uri}"
        );
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-user", HeaderValue::from_str(&self.user)?);
        headers.insert(
            "x-trino-client-capabilities",
            HeaderValue::from_static("PARAMETRIC_DATETIME,VARIANT,VARIANT_BINARY"),
        );
        headers.insert("accept-encoding", HeaderValue::from_static("identity"));
        for (name, value) in &self.session {
            headers.append(
                HeaderName::from_bytes(name.as_bytes())?,
                HeaderValue::from_str(value)?,
            );
        }
        for (name, value) in extra {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes())?,
                HeaderValue::from_str(value)?,
            );
        }
        let mut request = self
            .client
            .request(method, target)
            .headers(headers)
            .timeout(remaining);
        if let Some(sql) = sql {
            request = request.body(sql.to_owned());
        }
        Ok(request)
    }
    /// Completion headers are explicit client state, never a server-side session diff.
    pub(crate) fn apply_effects(&mut self, terminal: &RawResponse) -> Result<()> {
        let mut assignments = BTreeMap::new();
        for (_, encoded) in self
            .session
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("x-trino-session"))
        {
            for item in encoded.split(',') {
                let (name, value) = item
                    .split_once('=')
                    .context("malformed stored session assignment")?;
                assignments.insert(name.to_owned(), value.to_owned());
            }
        }
        for value in terminal.headers.get_all("x-trino-set-session") {
            for item in value.to_str()?.split(',') {
                let (name, encoded) = item
                    .split_once('=')
                    .context("malformed Set-Session response")?;
                assignments.insert(name.trim().into(), encoded.into());
            }
        }
        for value in terminal.headers.get_all("x-trino-clear-session") {
            for name in value.to_str()?.split(',') {
                assignments.remove(name.trim());
            }
        }
        self.session
            .retain(|(name, _)| !name.eq_ignore_ascii_case("x-trino-session"));
        if !assignments.is_empty() {
            self.session.push((
                "x-trino-session".into(),
                assignments
                    .into_iter()
                    .map(|(n, v)| format!("{n}={v}"))
                    .collect::<Vec<_>>()
                    .join(","),
            ));
        }
        for (response, request) in [
            ("x-trino-set-catalog", "x-trino-catalog"),
            ("x-trino-set-schema", "x-trino-schema"),
        ] {
            if let Some(value) = terminal.headers.get(response) {
                self.session
                    .retain(|(name, _)| !name.eq_ignore_ascii_case(request));
                self.session.push((request.into(), value.to_str()?.into()));
            }
        }
        Ok(())
    }
    pub(crate) fn execute(&self, sql: &str) -> Result<Vec<RawResponse>> {
        let mut pages = vec![self.submit(sql)?];
        loop {
            let page = pages.last().expect("initial POST response");
            ensure!(
                page.status == 200,
                "Trino query returned HTTP {}",
                page.status
            );
            let Some(next) = page.next_uri()? else {
                return Ok(pages);
            };
            pages.push(self.get(&next)?);
        }
    }
    fn record(
        &self,
        method: &str,
        uri: &str,
        sql: Option<&str>,
        response: &RawResponse,
        interrupted: bool,
    ) -> Result<()> {
        #[derive(Serialize)]
        struct Record<'a> {
            method: &'a str,
            uri: &'a str,
            sql: Option<&'a str>,
            status: u16,
            headers: Vec<(String, String)>,
            interrupted: bool,
            body: String,
        }
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let stem = format!("response-{sequence:06}");
        let body = format!("{stem}.body");
        fs::write(self.root.join(&body), &response.body)?;
        let headers = response
            .headers
            .iter()
            .map(|(name, value)| Ok((name.to_string(), value.to_str()?.to_owned())))
            .collect::<Result<Vec<_>>>()?;
        fs::write(
            self.root.join(format!("{stem}.json")),
            serde_json::to_vec_pretty(&Record {
                method,
                uri,
                sql,
                status: response.status,
                headers,
                interrupted,
                body,
            })?,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn continuation_uri_must_stay_with_the_original_endpoint() {
        let root = tempfile::tempdir().unwrap();
        let actor = TrinoClient::new(
            8080,
            "root",
            Instant::now() + Duration::from_secs(10),
            root.path(),
        )
        .unwrap();
        assert!(
            actor
                .build_request(
                    Method::GET,
                    "http://127.0.0.1:8080/v1/statement/executing/id/slug/0",
                    None,
                    &[]
                )
                .is_ok()
        );
        for uri in [
            "http://evil.example:8080/v1/statement",
            "http://127.0.0.1:8081/v1/statement",
            "https://127.0.0.1:8080/v1/statement",
            "http://alice@127.0.0.1:8080/v1/statement",
            "http://alice:secret@127.0.0.1:8080/v1/statement",
            "http://127.0.0.1:8080/v1/statement#fragment",
        ] {
            assert!(
                actor.build_request(Method::GET, uri, None, &[]).is_err(),
                "{uri}"
            );
        }
    }
    #[test]
    fn raw_json_oracle_rejects_non_string_continuations() {
        let response = RawResponse {
            status: 200,
            headers: HeaderMap::new(),
            body: br#"{"id":"id","nextUri":42}"#.to_vec(),
        };
        assert!(response.next_uri().is_err());
        let terminal = RawResponse {
            body: br#"{"id":"id","stats":{"state":"FINISHED"}}"#.to_vec(),
            ..response
        };
        assert_eq!(terminal.next_uri().unwrap(), None);
    }
    #[test]
    fn explicit_effects_keep_url_encoded_values_and_replace_client_state() {
        let root = tempfile::tempdir().unwrap();
        let mut actor = TrinoClient::new(
            8080,
            "root",
            Instant::now() + Duration::from_secs(10),
            root.path(),
        )
        .unwrap()
        .with_session(&[
            ("x-trino-session", "pipeline_dop=2,query_timeout=90"),
            ("x-trino-catalog", "old"),
            ("x-trino-schema", "old"),
        ]);
        let mut headers = HeaderMap::new();
        headers.append(
            "x-trino-set-session",
            HeaderValue::from_static("pipeline_dop=4"),
        );
        headers.append(
            "x-trino-set-session",
            HeaderValue::from_static("sql_mode=A%2CB"),
        );
        headers.insert(
            "x-trino-clear-session",
            HeaderValue::from_static("query_timeout"),
        );
        headers.insert("x-trino-set-catalog", HeaderValue::from_static("new"));
        headers.insert("x-trino-set-schema", HeaderValue::from_static("ns"));
        actor
            .apply_effects(&RawResponse {
                status: 200,
                headers,
                body: Vec::new(),
            })
            .unwrap();
        assert_eq!(
            actor.session,
            vec![
                (
                    "x-trino-session".into(),
                    "pipeline_dop=4,sql_mode=A%2CB".into()
                ),
                ("x-trino-catalog".into(), "new".into()),
                ("x-trino-schema".into(), "ns".into())
            ]
        );
    }
    struct FixtureJoin(Option<std::thread::JoinHandle<()>>);
    impl FixtureJoin {
        fn spawn(run: impl FnOnce() + Send + 'static) -> Self {
            Self(Some(std::thread::spawn(run)))
        }
        fn join(mut self) -> std::thread::Result<()> {
            self.0.take().unwrap().join()
        }
    }
    impl Drop for FixtureJoin {
        fn drop(&mut self) {
            if let Some(original) = self.0.take() {
                let _ = original.join();
            }
        }
    }
    fn fixture_accept(listener: &std::net::TcpListener) -> std::net::TcpStream {
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    return stream;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::park_timeout(Duration::from_millis(1))
                }
                Err(error) => panic!("original local HTTP fixture accept failed: {error}"),
            }
        }
    }
    fn read_fixture_request(stream: &mut std::net::TcpStream) {
        use std::io::Read;
        let deadline = Instant::now() + Duration::from_secs(5);
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            stream
                .set_read_timeout(Some(
                    deadline
                        .checked_duration_since(Instant::now())
                        .filter(|value| !value.is_zero())
                        .expect("local fixture request deadline"),
                ))
                .unwrap();
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
            assert!(
                request.len() < 32768,
                "local fixture request headers exceeded bound"
            );
        }
        assert!(request.starts_with(b"GET /held "));
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
    }
    #[test]
    fn original_held_response_separates_headers_first_byte_eof_and_join() {
        use std::{io::Write, net::TcpListener, sync::mpsc};
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (release, tail) = mpsc::channel();
        let server = FixtureJoin::spawn(move || {
            let mut stream = fixture_accept(&listener);
            read_fixture_request(&mut stream);
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\nX-Trino-Set-Session: a=1\r\nX-Trino-Set-Session: b=2\r\n\r\nab").unwrap();
            stream.flush().unwrap();
            tail.recv_timeout(Duration::from_secs(5)).unwrap();
            stream.write_all(b"cdef").unwrap();
            stream.flush().unwrap();
        });
        let actor = TrinoClient::new(
            port,
            "root",
            Instant::now() + Duration::from_secs(5),
            root.path(),
        )
        .unwrap();
        let mut held = actor.open_get(&format!("{}/held", actor.base())).unwrap();
        assert_eq!(held.status(), 200);
        assert_eq!(
            held.headers().get_all("x-trino-set-session").iter().count(),
            2
        );
        let at_headers = held.observe_held().unwrap();
        assert!(!at_headers.first_byte_observed && !at_headers.eof_observed);
        assert_eq!((at_headers.read_calls, at_headers.read_bytes), (0, 0));
        assert_eq!(held.read_prefix(1).unwrap(), b"a");
        assert_eq!(held.read_prefix(1).unwrap(), b"b");
        let paused = held.observe_held().unwrap();
        assert!(paused.first_byte_observed && !paused.eof_observed);
        assert_eq!(paused.read_bytes, 2);
        // This synchronous round trip proves the original worker has no
        // implicit drain or queued read while held at the same barrier.
        assert_eq!(held.observe_held().unwrap().read_calls, paused.read_calls);
        release.send(()).unwrap();
        assert_eq!(held.read_prefix(4).unwrap(), b"cdef");
        let eof = held.read_step(1).unwrap();
        assert!(eof.eof && eof.bytes.is_empty());
        let settled = held.settle();
        settled.require_clean().unwrap();
        assert!(
            settled.observation.eof_observed && settled.response_dropped && settled.reader_joined
        );
        assert_eq!(settled.observation.read_bytes, 6);
        server.join().unwrap();
    }
    #[test]
    fn interruption_and_owner_drop_close_the_unread_original_body_and_join() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
            sync::mpsc,
        };
        for explicit in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let (closed, closure) = mpsc::channel();
            let server = FixtureJoin::spawn(move || {
                let mut stream = fixture_accept(&listener);
                read_fixture_request(&mut stream);
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nab",
                    )
                    .unwrap();
                stream.flush().unwrap();
                let mut byte = [0];
                let result = stream.read(&mut byte);
                assert!(
                    matches!(result, Ok(0))
                        || result.as_ref().is_err_and(|error| matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                        )),
                    "original response was not actually closed: {result:?}"
                );
                closed.send(()).unwrap();
            });
            let actor = TrinoClient::new(
                port,
                "root",
                Instant::now() + Duration::from_secs(5),
                root.path(),
            )
            .unwrap();
            let mut held = actor.open_get(&format!("{}/held", actor.base())).unwrap();
            assert_eq!(held.read_prefix(1).unwrap(), b"a");
            let evidence = held.evidence.with_extension("settled.json");
            if explicit {
                let settled = held.interrupt();
                settled.require_clean().unwrap();
                assert!(!settled.observation.eof_observed);
                assert_eq!(settled.observation.read_bytes, 1);
            } else {
                drop(held);
            }
            closure.recv_timeout(Duration::from_secs(5)).unwrap();
            server.join().unwrap();
            let record: Value = serde_json::from_slice(&fs::read(evidence).unwrap()).unwrap();
            assert_eq!(record["original_reader"]["reader_joined"], true);
            assert_eq!(record["original_reader"]["response_dropped"], true);
            assert_eq!(
                record["original_reader"]["observation"]["eof_observed"],
                false
            );
        }
    }
    #[test]
    fn original_response_deadline_failure_survives_actual_join() {
        use std::{io::Write, net::TcpListener, sync::mpsc};
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (release, gate) = mpsc::channel();
        let server = FixtureJoin::spawn(move || {
            let mut stream = fixture_accept(&listener);
            read_fixture_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n")
                .unwrap();
            stream.flush().unwrap();
            gate.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        let actor = TrinoClient::new(
            port,
            "root",
            Instant::now() + Duration::from_secs(1),
            root.path(),
        )
        .unwrap();
        let mut held = actor.open_get(&format!("{}/held", actor.base())).unwrap();
        let primary = format!("{:#}", held.read_step(1).unwrap_err());
        let settled = held.interrupt();
        assert!(settled.reader_joined && settled.response_dropped);
        assert!(!settled.observation.first_byte_observed && !settled.observation.eof_observed);
        assert!(!settled.failures.is_empty());
        assert!(settled.require_clean().is_err());
        assert!(
            settled
                .failures
                .iter()
                .any(|error| error.contains(&primary))
                || primary.contains("original HTTP reader exited"),
            "{primary}; {settled:?}"
        );
        release.send(()).unwrap();
        server.join().unwrap();
    }
}
