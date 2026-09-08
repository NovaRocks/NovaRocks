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

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct QuerySample {
    pub workload: String,
    pub window_index: usize,
    pub configured_concurrency: usize,
    pub client: usize,
    /// Start and end use the same monotonic origin as `MeasurementWindow` and
    /// `process-resources.json`.
    pub started_elapsed_micros: u128,
    pub ended_elapsed_micros: u128,
    pub first_row_micros: u128,
    pub total_micros: u128,
    pub rows: u64,
    pub bytes_read: Option<u64>,
    pub outcome: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MeasurementWindow {
    pub workload: String,
    pub window_index: usize,
    pub configured_concurrency: usize,
    pub started_elapsed_millis: u128,
    pub ended_elapsed_millis: u128,
    pub drain_ended_elapsed_millis: u128,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PreparationEvent {
    pub work_id: String,
    pub logical_execution_id: String,
    pub attempt_id: Option<String>,
    pub phase: String,
    pub operation: String,
    pub capability_path: String,
    pub call_count: u64,
    pub elapsed_ns: u64,
    pub io_wait_ns: Option<u64>,
    pub cache_hit: Option<bool>,
    pub outcome: String,
}

#[derive(Debug, Deserialize)]
pub struct PerformanceDiagnosticResponse {
    pub schema_version: u8,
    pub run_token: String,
    pub events: Vec<PreparationEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreparationDiagnosticFrame {
    pub schema_version: u8,
    pub run_token: String,
    pub started_elapsed_micros: u128,
    pub ended_elapsed_micros: u128,
}

#[derive(Debug, Serialize)]
pub struct PerformanceReport<'a> {
    pub schema_version: u32,
    pub run_id: &'a str,
    pub run_manifest_sha256: &'a str,
    pub manifest_sha256: &'a str,
    pub scenario: &'a str,
    pub query_samples: &'a [QuerySample],
    pub measurement_windows: &'a [MeasurementWindow],
    pub preparation_diagnostic: &'a PreparationDiagnosticFrame,
    pub preparation_events_status: &'a str,
    pub preparation_events: &'a [PreparationEvent],
}

pub struct PerformanceReportInput<'a> {
    pub root: &'a Path,
    pub run_id: &'a str,
    pub run_manifest_sha256: &'a str,
    pub manifest_sha256: &'a str,
    pub scenario: &'a str,
    pub samples: &'a [QuerySample],
    pub measurement_windows: &'a [MeasurementWindow],
    pub preparation_diagnostic: &'a PreparationDiagnosticFrame,
    pub preparation_events: &'a [PreparationEvent],
}

pub fn write_report(input: PerformanceReportInput<'_>) -> Result<()> {
    let PerformanceReportInput {
        root,
        run_id,
        run_manifest_sha256,
        manifest_sha256,
        scenario,
        samples,
        measurement_windows,
        preparation_diagnostic,
        preparation_events,
    } = input;
    let report = PerformanceReport {
        schema_version: 4,
        run_id,
        run_manifest_sha256,
        manifest_sha256,
        scenario,
        query_samples: samples,
        measurement_windows,
        preparation_diagnostic,
        preparation_events_status: if preparation_events.is_empty() {
            "unsupported-not-wired"
        } else {
            "available"
        },
        preparation_events,
    };
    let bytes = serde_json::to_vec_pretty(&report).context("serialize performance report")?;
    fs::write(root.join("uea1-performance.json"), bytes)
        .with_context(|| format!("write performance report under {}", root.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_schema_preserves_unknown_attempt() {
        let event = PreparationEvent {
            work_id: "w".to_string(),
            logical_execution_id: "q".to_string(),
            attempt_id: None,
            phase: "compile".to_string(),
            operation: "optimize".to_string(),
            capability_path: "not-applicable".to_string(),
            call_count: 1,
            elapsed_ns: 10,
            io_wait_ns: None,
            cache_hit: None,
            outcome: "success".to_string(),
        };
        let json = serde_json::to_value(event).expect("serialize event");
        assert!(json["attempt_id"].is_null());
        assert!(json["io_wait_ns"].is_null());
    }
}
