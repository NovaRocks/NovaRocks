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

use anyhow::{Context, Result, ensure};
use reqwest::{Client, Method, Url};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use super::super::trino_protocol::require_error;
use crate::actors::trino_protocol::RawResponse;

pub(super) struct TierClient {
    client: Client,
    base: Url,
    user: String,
    timeout: Duration,
    deadline: Instant,
    root: PathBuf,
    records: Vec<(Value, Vec<u8>)>,
    live: Option<Url>,
}

impl TierClient {
    pub(super) fn new(
        client: Client,
        port: u16,
        user: String,
        timeout: Duration,
        deadline: Instant,
        root: PathBuf,
    ) -> Result<Self> {
        Ok(Self {
            client,
            base: Url::parse(&format!("http://127.0.0.1:{port}/v1/statement"))?,
            user,
            timeout,
            deadline,
            root,
            records: Vec::new(),
            live: None,
        })
    }

    pub(super) async fn prepare(&mut self, seconds: u64) -> Result<()> {
        let effect = format!("query_timeout={seconds}");
        let page = self
            .statement(&format!("SET query_timeout = {seconds}"), None)
            .await?;
        let result = page.assert_success()?;
        ensure!(
            result["stats"]["state"] == "FINISHED"
                && result["columns"] == json!([])
                && result["updateType"] == "SET SESSION"
                && result.get("updateCount").is_none()
                && page
                    .headers
                    .get_all("x-trino-set-session")
                    .iter()
                    .eq([effect.as_str()]),
            "Trino tier setup did not return the original query timeout effect"
        );
        Ok(())
    }

    pub(super) async fn run_timeout(&mut self, sql: &str, seconds: u64) -> Result<()> {
        let page = self
            .statement(sql, Some(&format!("query_timeout={seconds}")))
            .await?;
        let error = require_error(
            &page,
            (131075, "EXCEEDED_TIME_LIMIT", "INSUFFICIENT_RESOURCES"),
        )?;
        ensure!(
            error["stats"]["state"] == "FAILED",
            "unexpected Trino deadline state"
        );
        Ok(())
    }

    async fn statement(&mut self, sql: &str, effect: Option<&str>) -> Result<RawResponse> {
        let mut page = self
            .request(Method::POST, self.base.clone(), Some(sql), effect)
            .await?;
        self.live = self.continuation(&page)?;
        let submitted = page.assert_success()?;
        let id = submitted["id"]
            .as_str()
            .context("Trino POST omitted query identity")?;
        ensure!(
            self.live.is_some(),
            "Trino POST omitted its original first GET"
        );
        while let Some(uri) = self.live.clone() {
            page = self.request(Method::GET, uri, None, effect).await?;
            let value = page.json()?;
            ensure!(
                value["id"] == id,
                "Trino continuation changed query identity"
            );
            self.live = self.continuation(&page)?;
            ensure!(
                value.get("data").is_none(),
                "Trino tier query exposed unexpected rows"
            );
            if self.live.is_some() {
                page.assert_success()?;
            }
        }
        Ok(page)
    }

    fn continuation(&self, page: &RawResponse) -> Result<Option<Url>> {
        page.next_uri()?
            .map(|uri| {
                let target = Url::parse(&uri).context("parse Trino tier continuation")?;
                ensure!(
                    target.origin() == self.base.origin()
                        && target.username().is_empty()
                        && target.password().is_none()
                        && target.fragment().is_none(),
                    "Trino tier continuation changed the original endpoint"
                );
                Ok(target)
            })
            .transpose()
    }

    pub(super) async fn cancel(&mut self) -> Result<()> {
        if let Some(uri) = self.live.clone() {
            let response = self.request(Method::DELETE, uri, None, None).await?;
            ensure!(
                response.status == 204,
                "Trino tier cleanup DELETE returned {}",
                response.status
            );
            self.live = None;
        }
        Ok(())
    }

    pub(super) fn persist(&self) -> Result<()> {
        std::fs::create_dir_all(&self.root)?;
        for (sequence, (metadata, body)) in self.records.iter().enumerate() {
            let file = self.root.join(format!("response-{sequence:04}.json"));
            std::fs::write(file.with_extension("body"), body)?;
            std::fs::write(file, serde_json::to_vec(metadata)?)?;
        }
        Ok(())
    }

    async fn request(
        &mut self,
        method: Method,
        uri: Url,
        sql: Option<&str>,
        effect: Option<&str>,
    ) -> Result<RawResponse> {
        let remaining = self
            .deadline
            .checked_duration_since(Instant::now())
            .context("original Trino tier deadline expired")?;
        let mut request = self
            .client
            .request(method.clone(), uri.clone())
            .header("x-trino-user", &self.user)
            .header("accept-encoding", "identity")
            .timeout(remaining.min(self.timeout));
        if let Some(sql) = sql {
            request = request.body(sql.to_owned());
        }
        if let Some(effect) = effect {
            request = request.header("x-trino-session", effect);
        }
        let response = request.send().await.context("send Trino tier request")?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .context("read Trino tier response")?
            .to_vec();
        let headers_record = headers
            .iter()
            .map(|(key, value)| (key.as_str(), value.to_str().unwrap_or("<non-text>")))
            .collect::<Vec<_>>();
        self.records.push((
            json!({
                "method":method.as_str(),"uri":uri.as_str(),"sql":sql,"status":status,
                "headers":headers_record,"session_effect":effect,
            }),
            body.clone(),
        ));
        Ok(RawResponse {
            status,
            headers,
            body,
        })
    }
}
