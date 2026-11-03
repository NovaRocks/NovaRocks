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

use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, ensure};
use reqwest::blocking::Client;
use serde_json::Value;
use std::time::{Duration, Instant};

// Convergence barriers observe actual owners. They do not change admission,
// evict replay history, or treat an absent counter as a successful zero.
fn required_count(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .with_context(|| format!("missing or invalid owner counter {key}"))
}

pub(super) fn metric(rows: &[Value], name: &str, labels: &[(&str, &str)]) -> Result<u64> {
    let mut matched = rows.iter().filter(|row| {
        row["tags"]["metric"] == name
            && labels
                .iter()
                .all(|(key, value)| row["tags"][*key] == *value)
    });
    let row = matched
        .next()
        .with_context(|| format!("missing metric {name} {labels:?}"))?;
    ensure!(
        matched.next().is_none(),
        "duplicate metric {name} {labels:?}"
    );
    let value = row["value"].as_f64().context("invalid metric number")?;
    ensure!(
        value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value < u64::MAX as f64,
        "invalid owner metric {name}"
    );
    Ok(value as u64)
}

fn idle_snapshot(
    context: &mut ScenarioContext,
    client: &Client,
    sample_tag: &str,
) -> Result<(Value, bool)> {
    let response = context
        .handle()
        .frontend_management_get("/v1/frontend/state", Duration::from_secs(1))?;
    std::fs::write(
        context
            .scenario_root()
            .join(format!("{sample_tag}-frontend.body")),
        response.body.as_bytes(),
    )?;
    std::fs::write(
        context
            .scenario_root()
            .join(format!("{sample_tag}-frontend-http.json")),
        serde_json::to_vec_pretty(
            &serde_json::json!({"path":"/v1/frontend/state","status":response.status}),
        )?,
    )?;
    ensure!(
        response.status == 200,
        "owner observation returned HTTP {}",
        response.status
    );
    let frontend: Value = serde_json::from_str(&response.body)?;
    let workload = &frontend["workload"];
    let governance = &workload["governance"];
    let mut idle = required_count(&workload["active"], "statement")? == 0
        && required_count(&workload["active"], "background")? == 0;
    for key in [
        "root_responsibilities",
        "admitted_queries",
        "preparation",
        "execution",
        "old_attempts",
        "unknown_creates",
        "obligations",
        "waiting_records",
        "control_ready",
        "control_inflight",
    ] {
        idle &= required_count(governance, key)? == 0;
    }
    let new_windows = governance.get("result_window_positions");
    let old_credits = governance.get("result_credit_held_bytes");
    ensure!(
        new_windows.is_some() != old_credits.is_some(),
        "ambiguous or absent result owner observation"
    );
    let root_class = if let Some(positions) = new_windows {
        let positions = positions.as_array().context("invalid result positions")?;
        ensure!(positions.len() == 4, "invalid result position dimensions");
        for position in positions {
            idle &= position.as_u64().context("invalid result position count")? == 0;
        }
        Some("root_result")
    } else {
        idle &= required_count(governance, "held_bytes")? == 0
            && required_count(governance, "result_credit_held_bytes")? == 0;
        None
    };
    let ports: Vec<_> = context
        .handle()
        .runtime()
        .be
        .iter()
        .map(|be| be.http)
        .collect();
    let mut backends = Vec::new();
    for (index, port) in ports.into_iter().enumerate() {
        let url = format!("http://127.0.0.1:{port}/metrics?type=json");
        let response = client.get(&url).send()?;
        let status = response.status();
        let body = response.bytes()?;
        // Preserve the actual HTTP body before status/JSON/field validation.
        std::fs::write(
            context
                .scenario_root()
                .join(format!("{sample_tag}-be-{index}.body")),
            &body,
        )?;
        std::fs::write(
            context
                .scenario_root()
                .join(format!("{sample_tag}-be-{index}-http.json")),
            serde_json::to_vec_pretty(&serde_json::json!({"url":url,"status":status.as_u16()}))?,
        )?;
        ensure!(
            status.is_success(),
            "backend owner observation returned HTTP {status}"
        );
        let rows: Value = serde_json::from_slice(&body)?;
        let rows = rows.as_array().context("invalid backend metric array")?;
        let available = metric(
            rows,
            "novarocks_backend_execution_quiescence_snapshot_available",
            &[],
        )?;
        ensure!(available <= 1, "invalid execution quiescence availability");
        let execution = if available == 1 {
            let mut values = serde_json::Map::new();
            for resource in [
                "active_tasks",
                "preparing_tasks",
                "preparing_bytes",
                "requests",
                "participations_with_execution_custody",
                "root_producers_running",
                "root_data_positions",
                "root_payload_bytes",
                "root_segments",
                "root_deliveries",
                "root_retained_reservations",
                "root_metadata_holders",
                "participation_records",
                "root_channels",
                "root_metadata_bytes",
            ] {
                let used = metric(
                    rows,
                    "novarocks_backend_execution_quiescence",
                    &[("resource", resource)],
                )?;
                if !matches!(
                    resource,
                    "participation_records" | "root_channels" | "root_metadata_bytes"
                ) {
                    idle &= used == 0;
                }
                values.insert(resource.into(), serde_json::json!(used));
            }
            Some(values)
        } else {
            ensure!(
                !rows
                    .iter()
                    .any(|row| row["tags"]["metric"] == "novarocks_backend_execution_quiescence"),
                "unavailable execution sample published stale values"
            );
            idle = false;
            None
        };
        let mut ingress = Vec::new();
        for class in [Some("ordinary"), Some("control"), root_class]
            .into_iter()
            .flatten()
        {
            for phase in ["running", "waiting"] {
                let used = metric(
                    rows,
                    "novarocks_backend_native_ingress_slots",
                    &[("class", class), ("phase", phase), ("dimension", "used")],
                )?;
                idle &= used == 0;
                ingress.push(serde_json::json!({"class":class,"phase":phase,"used":used}));
            }
        }
        backends.push(
            serde_json::json!({"index":index,"http_port":port,"execution_quiescence_available":available,
            "execution_quiescence":execution,"ingress":ingress}),
        );
    }
    Ok((
        serde_json::json!({"frontend_active":workload["active"],"frontend_governance":governance,
        "backends":backends,"idle":idle}),
        idle,
    ))
}

pub(super) fn await_idle(
    context: &mut ScenarioContext,
    window: &str,
    phase: &str,
    epoch: Instant,
) -> Result<()> {
    let deadline = context
        .deadline()
        .min(Instant::now() + Duration::from_secs(30));
    let client = Client::builder().timeout(Duration::from_secs(1)).build()?;
    let mut snapshots = Vec::new();
    let outcome = (|| -> Result<()> {
        let mut consecutive = 0;
        loop {
            context.remaining("result owner convergence barrier")?;
            let sample_tag = format!(
                "owner-barrier-{window}-{phase}-sample-{:03}",
                snapshots.len()
            );
            let (mut snapshot, idle) = idle_snapshot(context, &client, &sample_tag)?;
            snapshot["query_epoch_micros"] = serde_json::json!(epoch.elapsed().as_micros());
            snapshots.push(snapshot);
            consecutive = if idle { consecutive + 1 } else { 0 };
            ensure!(
                Instant::now() < deadline && snapshots.len() < 301,
                "result owners did not converge before {window} {phase} barrier deadline"
            );
            if consecutive == 2 {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    })();
    std::fs::write(
        context
            .scenario_root()
            .join(format!("owner-barrier-{window}-{phase}.json")),
        serde_json::to_vec_pretty(
            &serde_json::json!({"schema_version":1,"window":window,"phase":phase,
            "snapshots":snapshots,"error":outcome.as_ref().err().map(|e| format!("{e:#}"))}),
        )?,
    )?;
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn owner_observation_never_turns_missing_duplicate_or_invalid_into_idle() {
        assert!(required_count(&json!({}), "held_bytes").is_err());
        assert!(required_count(&json!({"held_bytes": -1}), "held_bytes").is_err());
        let row = json!({"tags":{"metric":"reservation","dimension":"used"},"value":0});
        assert_eq!(
            metric(
                std::slice::from_ref(&row),
                "reservation",
                &[("dimension", "used")]
            )
            .unwrap(),
            0
        );
        assert!(metric(&[], "reservation", &[("dimension", "used")]).is_err());
        assert!(metric(&[row.clone(), row], "reservation", &[("dimension", "used")]).is_err());
        for value in [json!(-1), json!(0.5), json!("0"), Value::Null] {
            assert!(
                metric(
                    &[json!({"tags":{"metric":"reservation"},"value":value})],
                    "reservation",
                    &[]
                )
                .is_err()
            );
        }
    }
}
