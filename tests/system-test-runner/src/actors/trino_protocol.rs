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
    /// Read a real response prefix then drop the original HTTP body before EOF.
    /// A separate complete request establishes the replay oracle for that token.
    pub(crate) fn interrupt(&self, uri: &str, prefix_bytes: usize) -> Result<RawResponse> {
        ensure!(
            prefix_bytes > 0,
            "interrupted response needs a nonempty prefix"
        );
        let mut response = self.build_request(Method::GET, uri, None, &[])?.send()?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let mut body = vec![0; prefix_bytes];
        response
            .read_exact(&mut body)
            .context("read interrupted body prefix")?;
        drop(response);
        let raw = RawResponse {
            status,
            headers,
            body,
        };
        self.record("GET", uri, None, &raw, true)?;
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
                && target.password().is_none(),
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
}
