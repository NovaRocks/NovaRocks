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

const SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ManifestPurpose {
    Smoke,
    Formal,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    pub concurrency: usize,
    pub duration_ms: u64,
    #[serde(default)]
    pub warmup_ms: u64,
    pub repetitions: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ShortWorkload {
    pub queries: Vec<String>,
    pub plan_contains: Vec<String>,
    pub windows: Vec<Window>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BusinessKind {
    MvRefresh,
    Analyze,
    Optimize,
}

impl BusinessKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::MvRefresh => "mv-refresh",
            Self::Analyze => "analyze",
            Self::Optimize => "optimize",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerWorkload {
    pub kind: BusinessKind,
    /// A finite sequence of independent, identically seeded job targets.
    pub jobs: usize,
    pub minimum_completions: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum MixedFixtureRecipe {
    IcebergRest {
        rows_per_file: u64,
        files_per_table: usize,
        foreground_rows: u64,
    },
}

impl MixedFixtureRecipe {
    pub fn dimensions(&self) -> (u64, usize, u64) {
        match *self {
            Self::IcebergRest {
                rows_per_file,
                files_per_table,
                foreground_rows,
            } => (rows_per_file, files_per_table, foreground_rows),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MixedWorkload {
    pub fixture: MixedFixtureRecipe,
    pub foreground_clients: usize,
    pub duration_ms: u64,
    pub repetitions: usize,
    pub job_timeout_ms: u64,
    pub poll_interval_ms: u64,
    pub producers: Vec<ProducerWorkload>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SlowOutputWorkload {
    pub query: String,
    pub control_query: String,
    pub foreground_query: String,
    pub bytes_per_interval: usize,
    pub interval_ms: u64,
    pub duration_ms: u64,
    pub repetitions: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
            (1..=32).contains(&self.mixed.foreground_clients)
                && (1..=300_000).contains(&self.mixed.duration_ms)
                && (1..=20).contains(&self.mixed.repetitions)
                && (1..=300_000).contains(&self.mixed.job_timeout_ms)
                && self.mixed.poll_interval_ms > 0
                && self.mixed.poll_interval_ms < self.mixed.job_timeout_ms,
            "mixed workload requires bounded clients, repetitions, duration and polling"
        );
        let (rows_per_file, files_per_table, foreground_rows) = self.mixed.fixture.dimensions();
        ensure!(
            (1..=1_000_000).contains(&rows_per_file)
                && (2..=32).contains(&files_per_table)
                && (1..=10_000_000).contains(&foreground_rows),
            "mixed fixture requires positive bounded rows and at least two input files"
        );
        ensure!(
            self.mixed.producers.len() == 3,
            "mixed workload must define exactly MV, statistics and maintenance producers"
        );
        let mut kinds = self
            .mixed
            .producers
            .iter()
            .map(|producer| producer.kind)
            .collect::<Vec<_>>();
        kinds.sort_unstable();
        ensure!(
            kinds
                == [
                    BusinessKind::MvRefresh,
                    BusinessKind::Analyze,
                    BusinessKind::Optimize
                ],
            "mixed producer kinds must be mv-refresh, analyze and optimize"
        );
        for producer in &self.mixed.producers {
            ensure!(
                (1..=4096).contains(&producer.jobs)
                    && producer.minimum_completions > 0
                    && producer.minimum_completions <= producer.jobs as u64,
                "producer {} has an invalid finite job sequence",
                producer.kind.name()
            );
        }
        ensure!(
            !self.slow_output.query.trim().is_empty()
                && !self.slow_output.control_query.trim().is_empty()
                && !self.slow_output.foreground_query.trim().is_empty()
                && self.slow_output.bytes_per_interval > 0
                && self.slow_output.interval_ms > 0
                && self.slow_output.duration_ms > 0
                && (1..=20).contains(&self.slow_output.repetitions),
            "slow-output workload requires SQL and positive bounded byte/time/window limits"
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
                    .all(|window| window.duration_ms >= 120_000 && window.repetitions >= 5),
                "formal short workload requires five windows of at least 120 seconds per concurrency"
            );
            ensure!(
                self.mixed.foreground_clients == 8
                    && self.mixed.duration_ms >= 120_000
                    && self.mixed.repetitions >= 5,
                "formal mixed workload requires eight clients and five 120 second windows"
            );
            ensure!(
                self.slow_output.duration_ms >= 120_000 && self.slow_output.repetitions >= 5,
                "formal slow-output workload requires five windows of at least 120 seconds"
            );
        }
        Ok(())
    }
}

fn validate_window(window: &Window) -> Result<()> {
    if window.concurrency == 0 || window.duration_ms == 0 || !(1..=20).contains(&window.repetitions)
    {
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
            schema_version: 2,
            purpose: ManifestPurpose::Smoke,
            short: ShortWorkload {
                queries: vec!["SELECT 1".to_string()],
                plan_contains: vec!["RESULT".to_string()],
                windows: vec![Window {
                    concurrency: 1,
                    duration_ms: 10,
                    warmup_ms: 0,
                    repetitions: 1,
                }],
            },
            mixed: MixedWorkload {
                fixture: MixedFixtureRecipe::IcebergRest {
                    rows_per_file: 4,
                    files_per_table: 2,
                    foreground_rows: 8,
                },
                foreground_clients: 1,
                duration_ms: 10,
                repetitions: 1,
                job_timeout_ms: 1000,
                poll_interval_ms: 10,
                producers: [
                    BusinessKind::MvRefresh,
                    BusinessKind::Analyze,
                    BusinessKind::Optimize,
                ]
                .into_iter()
                .map(|kind| ProducerWorkload {
                    kind,
                    jobs: 2,
                    minimum_completions: 1,
                })
                .collect(),
            },
            slow_output: SlowOutputWorkload {
                query: "SELECT 1".to_string(),
                control_query: "SELECT 1".to_string(),
                foreground_query: "SELECT 1".to_string(),
                bytes_per_interval: 1,
                interval_ms: 1,
                duration_ms: 1,
                repetitions: 1,
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

    #[test]
    fn rejects_free_sql_as_a_business_completion_contract() {
        let mut value = serde_json::to_value(smoke()).expect("serialize smoke");
        value["mixed"]["producers"][0]["completion_query"] = "SELECT 1".into();
        assert!(serde_json::from_value::<Uea1WorkloadManifest>(value).is_err());
    }

    #[test]
    fn rejects_empty_or_unbounded_job_sequences() {
        for jobs in [0, 4097] {
            let mut manifest = smoke();
            manifest.mixed.producers[0].jobs = jobs;
            assert!(manifest.validate().is_err());
        }
    }

    #[test]
    fn rejects_unbounded_mixed_windows() {
        for duration_ms in [0, 300_001, u64::MAX] {
            let mut manifest = smoke();
            manifest.mixed.duration_ms = duration_ms;
            assert!(manifest.validate().is_err());
        }
    }

    #[test]
    fn rejects_single_file_optimize_fixture() {
        let mut manifest = smoke();
        manifest.mixed.fixture = MixedFixtureRecipe::IcebergRest {
            rows_per_file: 4,
            files_per_table: 1,
            foreground_rows: 8,
        };
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn shipped_manifests_use_real_typed_jobs() {
        for json in [
            include_str!("../../../benchmarks/uea1/workloads.json"),
            include_str!("../../../benchmarks/uea1/workloads-smoke.json"),
        ] {
            let manifest: Uea1WorkloadManifest =
                serde_json::from_str(json).expect("typed workload manifest");
            manifest.validate().expect("valid workload manifest");
            assert_eq!(manifest.mixed.producers.len(), 3);
            assert!(
                manifest
                    .mixed
                    .producers
                    .iter()
                    .all(|producer| producer.jobs > 0)
            );
        }
    }
}
