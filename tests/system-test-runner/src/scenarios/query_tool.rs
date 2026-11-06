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

//! Receipt-bound repository tools on the ordinary native topology.
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{LaunchProfile, TestClusterOwner};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Default)]
pub(super) struct QueryTool;
impl Scenario for QueryTool {
    fn name(&self) -> &'static str {
        "tools/trino-query"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module("query_tool", self.name(), true, false)
    }
    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            profile == LaunchProfile::FaultScenario,
            "Trino query tool requires the ordinary fault-scenario native launch profile"
        );
        receipt()?;
        Ok(())
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "Trino query tool requires native 1FE+3BE"
        );
        let epoch = Instant::now();
        super::result_delivery_support::await_idle(context, "trino", "query-tool-before", epoch)?;
        let root = context.scenario_root().join("query-tool-probe");
        let outcome = (|| -> Result<_> {
            let outcome = invoke(context, &root)?;
            let result = (|| -> Result<()> {
                ensure!(
                    outcome.group_exit_observed(),
                    "query tool payload group has not actually exited"
                );
                ensure!(
                    !outcome.timed_out && outcome.failures.is_empty(),
                    "query tool lifecycle failed: {:?}",
                    outcome.failures
                );
                ensure!(
                    outcome.output.status.success(),
                    "query tool probe exited {:?}; retained at {}",
                    outcome.output.status.code(),
                    root.display()
                );
                let report: Value = serde_json::from_slice(&fs::read(root.join("result.json"))?)?;
                ensure!(
                    report["status"] == "PASS",
                    "query tool probe did not record PASS: {report}"
                );
                Ok(())
            })();
            result?;
            Ok(outcome)
        })();
        // A failed external client does not replace the original native
        // convergence oracle or skip actual role cleanup by the runner.
        let convergence =
            super::result_delivery_support::await_idle(context, "trino", "query-tool-after", epoch);
        let outcome = match (outcome, convergence) {
            (Ok(outcome), Ok(())) => outcome,
            (Err(primary), Err(cleanup)) => {
                anyhow::bail!("{primary:#}; native convergence: {cleanup:#}");
            }
            (Err(error), _) | (_, Err(error)) => return Err(error),
        };
        outcome.release_diagnostics()?;
        context.action("receipt-bound plan helpers and Trino query tool verified finite tableless EXPLAIN, integer5050 and complete4MiB text on native1FE+3BE; external StarRocks comparison NOT_RUN");
        Ok(())
    }
}
fn receipt() -> Result<PathBuf> {
    let path = std::env::var_os("NOVA_CI_ARTIFACT_RECEIPT")
        .map(PathBuf::from)
        .context("NOVA_CI_ARTIFACT_RECEIPT is required for tools/trino-query")?;
    let path = path
        .canonicalize()
        .context("resolve original tool receipt")?;
    ensure!(path.is_file(), "tool receipt must be a regular file");
    Ok(path)
}
#[cfg(unix)]
fn invoke(
    context: &ScenarioContext,
    root: &std::path::Path,
) -> Result<novarocks_test_support::CapturedCommandOutcome> {
    use novarocks_test_support::{ManagedCommand, capture_command_until};
    use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
    let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let program = repository.join("tests/system-test-runner/fixtures/trino_query_probe.py");
    let directory = context.scenario_root().join("query-tool-command");
    fs::create_dir(&directory)?;
    // Capture wall time before reading remaining monotonic time so conversion
    // rounds conservatively. The original ManagedCommand deadline stays final.
    let wall = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let remaining = context.remaining("run original query tool probe")?;
    let deadline_unix_ms = wall
        .checked_add(remaining)
        .context("query tool wall deadline overflow")?
        .as_millis();
    let arguments = vec![
        program.display().to_string(),
        "--host".into(),
        "127.0.0.1".into(),
        "--port".into(),
        context.trino_port().to_string(),
        "--user".into(),
        context.sql_user().into(),
        "--receipt".into(),
        receipt()?.display().to_string(),
        "--deadline-unix-ms".into(),
        deadline_unix_ms.to_string(),
        "--output".into(),
        root.display().to_string(),
    ];
    fs::write(
        directory.join("command.json"),
        serde_json::to_vec_pretty(
            &json!({"program":"python3","arguments":arguments,"deadline_unix_ms":deadline_unix_ms}),
        )?,
    )?;
    let mut command = ManagedCommand::new("python3");
    command
        .args(&arguments)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .observe_group_exit_until(context.deadline());
    let outcome = capture_command_until(
        command,
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join("stdout.log"))?,
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join("stderr.log"))?,
        context.deadline(),
    )?;
    // Retain the original monitor record before ordinary diagnostic release.
    // Pending may contain private inherited inputs, so keep this copy private.
    let mut original = fs::File::open(outcome.diagnostic_root().join("record.jsonl"))?;
    let mut retained = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.join("monitor-original.jsonl"))?;
    std::io::copy(&mut original, &mut retained)?;
    fs::write(
        directory.join("exit.json"),
        serde_json::to_vec_pretty(
            &json!({"exit":outcome.output.status.code(),"timed_out":outcome.timed_out,"failures":outcome.failures,"group_exit_observed":outcome.group_exit_observed()}),
        )?,
    )?;
    Ok(outcome)
}
#[cfg(not(unix))]
fn invoke(
    _: &ScenarioContext,
    _: &std::path::Path,
) -> Result<novarocks_test_support::CapturedCommandOutcome> {
    anyhow::bail!("Trino query tool process ownership requires Unix")
}
