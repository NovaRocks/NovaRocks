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

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ManifestPurpose {
    Smoke,
    Formal,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Window {
    pub concurrency: usize,
    pub duration_ms: u64,
    #[serde(default)]
    pub warmup_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShortWorkload {
    pub queries: Vec<String>,
    pub plan_contains: Vec<String>,
    pub windows: Vec<Window>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProducerWorkload {
    pub kind: String,
    pub statements: Vec<String>,
    pub completion_query: String,
    pub minimum_completions: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MixedWorkload {
    pub foreground_sql: String,
    pub foreground_clients: usize,
    pub duration_ms: u64,
    pub producers: Vec<ProducerWorkload>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SlowOutputWorkload {
    pub query: String,
    pub bytes_per_interval: usize,
    pub interval_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Uea1WorkloadManifest {
    pub schema_version: u32,
    pub purpose: ManifestPurpose,
    pub short: ShortWorkload,
    pub mixed: MixedWorkload,
    pub slow_output: SlowOutputWorkload,
    #[serde(skip)]
    pub sha256: String,
}

impl Uea1WorkloadManifest {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = fs::read(path)
            .with_context(|| format!("read UEA-1 workload manifest {}", path.display()))?;
        let mut manifest: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse UEA-1 workload manifest {}", path.display()))?;
        manifest.sha256 = format!("{:x}", Sha256::digest(&bytes));
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == SCHEMA_VERSION,
            "unsupported UEA-1 workload manifest version {}",
            self.schema_version
        );
        ensure!(
            !self.short.queries.is_empty(),
            "short workload has no queries"
        );
        ensure!(
            self.short.queries.iter().all(|sql| !sql.trim().is_empty()),
            "short workload contains empty SQL"
        );
        ensure!(
            !self.short.windows.is_empty(),
            "short workload has no windows"
        );
        for window in &self.short.windows {
            validate_window(window)?;
        }
        ensure!(
            !self.mixed.foreground_sql.trim().is_empty(),
            "mixed foreground SQL is empty"
        );
        ensure!(
            self.mixed.foreground_clients > 0 && self.mixed.duration_ms > 0,
            "mixed workload requires positive clients and duration"
        );
        ensure!(
            self.mixed.producers.len() == 3,
            "mixed workload must define exactly MV, statistics and maintenance producers"
        );
        let mut kinds = self
            .mixed
            .producers
            .iter()
            .map(|producer| producer.kind.as_str())
            .collect::<Vec<_>>();
        kinds.sort_unstable();
        ensure!(
            kinds == ["maintenance", "mv-refresh", "statistics"],
            "mixed producer kinds must be maintenance, mv-refresh and statistics"
        );
        for producer in &self.mixed.producers {
            ensure!(
                !producer.statements.is_empty()
                    && producer.statements.iter().all(|sql| !sql.trim().is_empty())
                    && !producer.completion_query.trim().is_empty()
                    && producer.minimum_completions > 0,
                "producer {} has an empty or zero-valued contract",
                producer.kind
            );
        }
        ensure!(
            !self.slow_output.query.trim().is_empty()
                && self.slow_output.bytes_per_interval > 0
                && self.slow_output.interval_ms > 0
                && self.slow_output.duration_ms > 0,
            "slow-output workload requires SQL and positive byte/time limits"
        );
        if matches!(self.purpose, ManifestPurpose::Formal) {
            let concurrency = self
                .short
                .windows
                .iter()
                .map(|window| window.concurrency)
                .collect::<Vec<_>>();
            ensure!(
                concurrency == [1, 8, 32],
                "formal short workload concurrency must be exactly 1, 8, 32"
            );
            ensure!(
                self.short
                    .windows
                    .iter()
                    .all(|window| window.duration_ms >= 120_000),
                "formal short workload windows must run for at least 120 seconds"
            );
            ensure!(
                self.mixed.foreground_clients == 8 && self.mixed.duration_ms >= 120_000,
                "formal mixed workload requires eight clients and a 120 second window"
            );
        }
        Ok(())
    }
}

fn validate_window(window: &Window) -> Result<()> {
    if window.concurrency == 0 || window.duration_ms == 0 {
        bail!("workload windows require positive concurrency and duration");
    }
    ensure!(
        window.warmup_ms < window.duration_ms,
        "window warmup must be shorter than its duration"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn smoke() -> Uea1WorkloadManifest {
        Uea1WorkloadManifest {
            schema_version: 1,
            purpose: ManifestPurpose::Smoke,
            short: ShortWorkload {
                queries: vec!["SELECT 1".to_string()],
                plan_contains: vec!["RESULT".to_string()],
                windows: vec![Window {
                    concurrency: 1,
                    duration_ms: 10,
                    warmup_ms: 0,
                }],
            },
            mixed: MixedWorkload {
                foreground_sql: "SELECT 1".to_string(),
                foreground_clients: 1,
                duration_ms: 10,
                producers: ["maintenance", "mv-refresh", "statistics"]
                    .into_iter()
                    .map(|kind| ProducerWorkload {
                        kind: kind.to_string(),
                        statements: vec!["SELECT 1".to_string()],
                        completion_query: "SELECT 1".to_string(),
                        minimum_completions: 1,
                    })
                    .collect(),
            },
            slow_output: SlowOutputWorkload {
                query: "SELECT 1".to_string(),
                bytes_per_interval: 1,
                interval_ms: 1,
                duration_ms: 1,
            },
            sha256: String::new(),
        }
    }

    #[test]
    fn accepts_bounded_smoke_manifest() {
        smoke().validate().expect("valid smoke manifest");
    }

    #[test]
    fn rejects_missing_business_producer() {
        let mut manifest = smoke();
        manifest.mixed.producers.pop();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn formal_manifest_enforces_fixed_concurrency() {
        let mut manifest = smoke();
        manifest.purpose = ManifestPurpose::Formal;
        assert!(manifest.validate().is_err());
    }
}
