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

use anyhow::{Context, Result, bail};
use reqwest::Url;
use reqwest::blocking::{Client, Response};
use serde::Deserialize;
use std::time::{Duration, Instant};

const MAX_CONTROL_RESPONSE_BYTES: usize = 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Typed access to the isolated UEA-7 publication fixture control plane.
///
/// This actor is deliberately restricted to loopback HTTP. It is a test
/// controller, not an Iceberg catalog implementation or production protocol.
#[derive(Clone)]
pub struct PublicationCatalogActor {
    control_uri: Url,
    client: Client,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PublicationArm {
    pub arm_id: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PublicationStatus {
    pub arm_id: String,
    pub table: String,
    pub phase: String,
    pub commit_id: String,
    pub thread: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PublicationTraceEvent {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub event: String,
    pub table: String,
    pub arm_id: String,
    pub commit_id: String,
    pub base_metadata: String,
    pub updated_metadata: String,
    pub thread: String,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PublicationMetrics {
    pub commit_attempts: u64,
    pub commit_successes: u64,
    pub commit_conflicts: u64,
    pub commit_failures: u64,
    pub input_files: u64,
    pub input_file_length_calls: u64,
    pub input_file_exists_calls: u64,
    pub input_streams: u64,
    pub input_bytes: u64,
    pub output_files: u64,
    pub output_streams: u64,
    pub output_bytes: u64,
    pub delete_successes: u64,
}

impl PublicationCatalogActor {
    pub fn connect(control_uri: &str, timeout: Duration) -> Result<Self> {
        let control_uri = Url::parse(control_uri).context("parse publication control URI")?;
        if control_uri.scheme() != "http"
            || !matches!(
                control_uri.host_str(),
                Some("127.0.0.1" | "localhost" | "::1")
            )
        {
            bail!("publication control URI must use loopback HTTP");
        }
        if control_uri.query().is_some() || control_uri.fragment().is_some() {
            bail!("publication control URI must not contain a query or fragment");
        }
        let client = Client::builder()
            .no_proxy()
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .context("build publication control client")?;
        let actor = Self {
            control_uri,
            client,
        };
        actor
            .get_json::<serde_json::Value>("health", &[])
            .context("publication fixture health check")?;
        Ok(actor)
    }

    pub fn arm(&self, table: &str) -> Result<PublicationArm> {
        self.post_json("arm", &[("table", table)])
            .with_context(|| format!("arm publication hold for {table}"))
    }

    pub fn status(&self, arm_id: &str) -> Result<PublicationStatus> {
        self.get_json("status", &[("arm_id", arm_id)])
            .with_context(|| format!("read publication hold status for {arm_id}"))
    }

    pub fn wait_for_phase(
        &self,
        arm_id: &str,
        expected: &str,
        deadline: Instant,
    ) -> Result<PublicationStatus> {
        loop {
            let status = self.status(arm_id)?;
            if status.phase == expected {
                return Ok(status);
            }
            if matches!(
                status.phase.as_str(),
                "succeeded" | "conflict" | "failed" | "timed-out"
            ) {
                bail!(
                    "publication hold {arm_id} reached terminal phase {} while waiting for {expected}",
                    status.phase
                );
            }
            if Instant::now() >= deadline {
                bail!(
                    "timed out waiting for publication hold {arm_id} to reach phase {expected}; last phase was {}",
                    status.phase
                );
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    pub fn release(&self, arm_id: &str) -> Result<PublicationStatus> {
        self.post_json("release", &[("arm_id", arm_id)])
            .with_context(|| format!("release publication hold {arm_id}"))
    }

    pub fn trace(&self) -> Result<Vec<PublicationTraceEvent>> {
        let body = self
            .bounded_body(self.client.get(self.url("trace", &[])?).send()?)
            .context("read publication trace")?;
        body.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).context("decode publication trace NDJSON event"))
            .collect()
    }

    pub fn metrics(&self) -> Result<PublicationMetrics> {
        self.get_json("metrics", &[])
            .context("read publication metrics")
    }

    fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        let response = self.client.get(self.url(path, query)?).send()?;
        let body = self.bounded_body(response)?;
        serde_json::from_str(&body).with_context(|| format!("decode publication {path} response"))
    }

    fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        let response = self.client.post(self.url(path, query)?).send()?;
        let body = self.bounded_body(response)?;
        serde_json::from_str(&body).with_context(|| format!("decode publication {path} response"))
    }

    fn url(&self, path: &str, query: &[(&str, &str)]) -> Result<Url> {
        let mut url = self.control_uri.join(path)?;
        url.query_pairs_mut().extend_pairs(query.iter().copied());
        Ok(url)
    }

    fn bounded_body(&self, response: Response) -> Result<String> {
        let response = response.error_for_status()?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_CONTROL_RESPONSE_BYTES as u64)
        {
            bail!("publication control response exceeds the fixture limit");
        }
        let bytes = response.bytes()?;
        if bytes.len() > MAX_CONTROL_RESPONSE_BYTES {
            bail!("publication control response exceeds the fixture limit");
        }
        String::from_utf8(bytes.to_vec()).context("publication control response is not UTF-8")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_loopback_control_uri() {
        let error = match PublicationCatalogActor::connect(
            "https://catalog.example.test:8182/",
            Duration::from_secs(1),
        ) {
            Ok(_) => panic!("remote control URI must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("loopback HTTP"));
    }

    #[test]
    #[ignore = "requires a live UEA-7 publication fixture"]
    fn reads_live_fixture_trace_and_metrics() -> Result<()> {
        // Keep the root from creation so startup, assertion or cleanup failure
        // cannot erase the original registration through TempDir's Drop.
        let scenario_root = tempfile::Builder::new()
            .prefix("uea7-publication-actor-")
            .tempdir()
            .context("create publication actor fixture root")?
            .keep();
        let (mut fixture, uri) = novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture::start_with_publication_hook(&scenario_root)
            .with_context(|| format!("start publication actor fixture; original root retained at {}", scenario_root.display()))?;
        let assertion = std::panic::catch_unwind(|| {
            let actor = PublicationCatalogActor::connect(&uri, Duration::from_secs(5)).unwrap();
            assert_eq!(actor.metrics().unwrap().commit_attempts, 0);
            assert_eq!(
                actor
                    .trace()
                    .unwrap()
                    .last()
                    .map(|event| event.event.as_str()),
                Some("control-started")
            );
        });
        // The same original fixture owner closes even if an assertion panics.
        // Its registration records cleanup independently of the test outcome.
        let shutdown = fixture.shutdown();
        if let Err(error) = &shutdown {
            eprintln!(
                "publication actor fixture cleanup failed; original root retained at {}: {error:#}",
                scenario_root.display()
            );
        }
        match assertion {
            Ok(()) => {
                shutdown.with_context(|| {
                    format!(
                        "publication actor fixture shutdown; original root retained at {}",
                        scenario_root.display()
                    )
                })?;
                std::fs::remove_dir_all(&scenario_root).with_context(|| {
                    format!(
                        "remove released publication actor fixture root {}",
                        scenario_root.display()
                    )
                })
            }
            Err(payload) => {
                if shutdown.is_ok() {
                    eprintln!(
                        "publication actor assertion failed after confirmed fixture cleanup; original root retained at {}",
                        scenario_root.display()
                    );
                }
                std::panic::resume_unwind(payload)
            }
        }
    }
}
