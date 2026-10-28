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

use crate::suite_manifest::PreparedDatasetRequirement;
use crate::types::RunnerConfig;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::NamedTempFile;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadyDatasetBinding {
    pub dataset_key: BTreeMap<String, String>,
    pub exact_warehouse: String,
    pub manifest_uri: String,
    pub ready_uri: String,
    pub publication_identity: String,
    pub publication_etag: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ResolvedDataset {
    schema_version: u32,
    dataset_key: BTreeMap<String, String>,
    dataset_root: String,
    ready_uri: String,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct CheckedDatasetResult {
    schema_version: u32,
    dataset_key: BTreeMap<String, String>,
    state: String,
    reused: bool,
    built: bool,
    exact_warehouse: String,
    manifest_uri: String,
    publication: Publication,
}

#[derive(Debug, Deserialize)]
struct Publication {
    ready_uri: String,
    etag: String,
    identity: String,
}

#[derive(Debug, Deserialize)]
struct FixtureError {
    schema_version: u32,
    error: String,
    dataset_key: BTreeMap<String, String>,
    message: String,
}

/// Read an already prepared input once. No supply mode is exposed by this consumer.
// Design: ADR-0173 (docs/adr/ADR-0173-prepared-dataset-consumption-is-check-only.md)
#[cfg(test)]
pub fn bind_prepared_dataset(
    requirement: &PreparedDatasetRequirement,
    runner_config: &RunnerConfig,
    base_dir: &Path,
) -> Result<ReadyDatasetBinding> {
    bind_prepared_dataset_impl(requirement, runner_config, base_dir, None)
}
pub(crate) fn bind_prepared_dataset_with_budget(
    requirement: &PreparedDatasetRequirement,
    runner_config: &RunnerConfig,
    base_dir: &Path,
    budget: &novarocks_cluster_harness::fixture_run::RunBudget,
) -> Result<ReadyDatasetBinding> {
    bind_prepared_dataset_impl(requirement, runner_config, base_dir, Some(budget))
}
fn bind_prepared_dataset_impl(
    requirement: &PreparedDatasetRequirement,
    runner_config: &RunnerConfig,
    base_dir: &Path,
    budget: Option<&novarocks_cluster_harness::fixture_run::RunBudget>,
) -> Result<ReadyDatasetBinding> {
    let suite = requirement.dataset.as_str();
    let scale = &requirement.scale;
    let resolved = resolve_dataset(runner_config, base_dir, suite, scale, budget)?;
    let mut resolved_file = NamedTempFile::new().context("create resolved dataset file")?;
    serde_json::to_writer(&mut resolved_file, &resolved).context("write resolved dataset")?;
    resolved_file
        .as_file_mut()
        .sync_all()
        .context("sync resolved dataset")?;
    let mut command = Command::new(fixture_check_script_path(runner_config, base_dir));
    command
        .args(["--suite", suite, "--scale", scale, "--resolved-dataset"])
        .arg(resolved_file.path())
        .arg("--check");
    if let Some(env_file) = runner_config.values.get("fixture_env_file") {
        command.env("NOVA_ENV_REST_ENV_FILE", env_file);
    }
    let preview = command_preview(&command);
    let output = bounded_output(command, budget)
        .with_context(|| format!("read prepared dataset: {preview}"))?;
    if output.stdout.is_empty() {
        bail!(
            "prepared dataset {} check failed: {}",
            suite,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    validate_typed_check_output(runner_config, base_dir, suite, scale, &output, budget)?;
    parse_check_output(&output, &resolved, &preview)
        .with_context(|| format!("prepared dataset required for {suite}"))
}

fn bounded_output(
    mut command: Command,
    budget: Option<&novarocks_cluster_harness::fixture_run::RunBudget>,
) -> Result<std::process::Output> {
    let Some(budget) = budget else {
        return command
            .output()
            .context("read prepared dataset command output");
    };
    budget.check_work()?;
    let mut managed = novarocks_test_support::ManagedCommand::new(command.get_program());
    managed.args(command.get_args());
    if let Some(dir) = command.get_current_dir() {
        managed.current_dir(dir);
    }
    for (key, value) in command.get_envs() {
        match value {
            Some(value) => {
                managed.env(key, value);
            }
            None => {
                managed.env_remove(key);
            }
        }
    }
    managed.env(
        novarocks_cluster_harness::fixture_run::RUN_CONTEXT_ENV,
        budget.to_json()?,
    );
    if let Some(storage) = std::env::var_os(novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV)
    {
        managed.env(
            novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV,
            storage,
        );
    }
    let outcome = novarocks_test_support::capture_command_until(
        managed,
        tempfile::tempfile()?,
        tempfile::tempfile()?,
        budget.work_deadline(),
    )?;
    anyhow::ensure!(
        !outcome.timed_out && outcome.failures.is_empty(),
        "prepared dataset original deadline or actual exit failed; diagnostics retained at {}",
        outcome.diagnostic_root().display()
    );
    outcome.release_diagnostics()?;
    Ok(outcome.output)
}

fn resolve_dataset(
    runner_config: &RunnerConfig,
    base_dir: &Path,
    suite: &str,
    scale: &str,
    budget: Option<&novarocks_cluster_harness::fixture_run::RunBudget>,
) -> Result<ResolvedDataset> {
    let resolver_path = fixture_resolver_path(runner_config, base_dir);
    let mut command = Command::new("python3");
    command
        .arg(&resolver_path)
        .arg("--workspace-root")
        .arg(base_dir)
        .arg("--suite")
        .arg(suite)
        .arg("--scale")
        .arg(scale);
    if let Some(shared_root) = fixture_shared_root(runner_config) {
        command.arg("--shared-root").arg(shared_root);
    }
    let kind = runner_config
        .values
        .get("fixture_object_store_kind")
        .map(String::as_str)
        .unwrap_or("minio");
    anyhow::ensure!(
        matches!(kind, "minio" | "aliyun-oss"),
        "unsupported prepared dataset object-store kind"
    );
    command.arg("--object-store-kind").arg(kind);
    let preview = command_preview(&command);
    let output = bounded_output(command, budget)
        .with_context(|| format!("failed to run dataset fixture resolver: {preview}"))?;
    if !output.status.success() {
        bail!(
            "dataset fixture resolver failed: {preview}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let resolved: ResolvedDataset = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("dataset fixture resolver returned malformed JSON: {preview}"))?;
    validate_resolved_dataset(&resolved, suite, scale)?;
    Ok(resolved)
}

fn fixture_resolver_path(runner_config: &RunnerConfig, base_dir: &Path) -> PathBuf {
    runner_config
        .values
        .get("benchmark_fixture_resolver")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                base_dir.join(path)
            }
        })
        .unwrap_or_else(|| {
            base_dir.join("tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py")
        })
}

fn fixture_shared_root(runner_config: &RunnerConfig) -> Option<String> {
    runner_config
        .values
        .get("benchmark_shared_root")
        .or_else(|| runner_config.values.get("benchmark_fixture_shared_root"))
        .cloned()
        .or_else(|| std::env::var("NOVA_ENV_SHARED_BENCHMARK_ROOT").ok())
        .filter(|value| !value.trim().is_empty())
}

fn validate_typed_check_output(
    runner_config: &RunnerConfig,
    base_dir: &Path,
    suite: &str,
    scale: &str,
    output: &std::process::Output,
    budget: Option<&novarocks_cluster_harness::fixture_run::RunBudget>,
) -> Result<()> {
    let mut output_file = NamedTempFile::new().context("create bootstrap result file")?;
    use std::io::Write;
    output_file
        .write_all(&output.stdout)
        .context("write bootstrap result")?;
    output_file
        .as_file_mut()
        .sync_all()
        .context("sync bootstrap result")?;

    let mut command = Command::new("python3");
    command
        .arg(fixture_resolver_path(runner_config, base_dir))
        .arg("--workspace-root")
        .arg(base_dir)
        .arg("--suite")
        .arg(suite)
        .arg("--scale")
        .arg(scale)
        .arg(if output.status.success() {
            "--validate-ensure-result"
        } else {
            "--validate-error"
        })
        .arg(output_file.path());
    if let Some(shared_root) = fixture_shared_root(runner_config) {
        command.arg("--shared-root").arg(shared_root);
    }
    let kind = runner_config
        .values
        .get("fixture_object_store_kind")
        .map(String::as_str)
        .unwrap_or("minio");
    anyhow::ensure!(
        matches!(kind, "minio" | "aliyun-oss"),
        "unsupported prepared dataset object-store kind"
    );
    command.arg("--object-store-kind").arg(kind);
    let preview = command_preview(&command);
    let validation = bounded_output(command, budget)
        .with_context(|| format!("failed to validate dataset fixture result: {preview}"))?;
    if validation.status.success() {
        return Ok(());
    }
    bail!(
        "dataset check emitted an invalid typed result: {preview}: {}",
        String::from_utf8_lossy(&validation.stderr).trim()
    )
}

fn validate_resolved_dataset(
    resolved: &ResolvedDataset,
    suite: &str,
    requested_scale: &str,
) -> Result<()> {
    if resolved.schema_version != 1 {
        bail!("ResolvedDataset has an unknown schema_version");
    }
    if resolved.dataset_key.get("suite").map(String::as_str) != Some(suite)
        || resolved.dataset_key.get("scale").map(String::as_str) != Some(requested_scale)
        || resolved
            .dataset_key
            .get("fixture_contract_id")
            .is_none_or(String::is_empty)
    {
        bail!("ResolvedDataset has an invalid dataset_key");
    }
    if !resolved.dataset_root.starts_with("s3://") || !resolved.ready_uri.starts_with("s3://") {
        bail!("ResolvedDataset has a non-S3 dataset location");
    }
    Ok(())
}

fn parse_check_output(
    output: &std::process::Output,
    resolved: &ResolvedDataset,
    preview: &str,
) -> Result<ReadyDatasetBinding> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let value: serde_json::Value = serde_json::from_str(stdout.trim())
        .with_context(|| format!("dataset check did not emit a typed JSON result: {preview}"))?;
    if output.status.success() {
        let result: CheckedDatasetResult = serde_json::from_value(value)
            .context("dataset check emitted malformed CheckedDatasetResult")?;
        return validate_checked_result(result, resolved);
    }

    if let Ok(error) = serde_json::from_value::<FixtureError>(value) {
        validate_fixture_error(error, resolved)?;
    }
    bail!(
        "dataset check failed: {preview}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    )
}

fn validate_checked_result(
    result: CheckedDatasetResult,
    resolved: &ResolvedDataset,
) -> Result<ReadyDatasetBinding> {
    if result.schema_version != 1
        || result.state != "ReadyValid"
        || !result.reused
        || result.built
        || result.dataset_key != resolved.dataset_key
        || result.publication.ready_uri != resolved.ready_uri
        || result.publication.etag.trim().is_empty()
        || result.publication.identity.trim().is_empty()
        || !result.exact_warehouse.starts_with("s3://")
        || !result.manifest_uri.starts_with("s3://")
    {
        bail!("dataset check emitted an invalid CheckedDatasetResult");
    }
    Ok(ReadyDatasetBinding {
        dataset_key: result.dataset_key,
        exact_warehouse: result.exact_warehouse,
        manifest_uri: result.manifest_uri,
        ready_uri: result.publication.ready_uri,
        publication_identity: result.publication.identity,
        publication_etag: result.publication.etag,
    })
}

fn validate_fixture_error(error: FixtureError, resolved: &ResolvedDataset) -> Result<()> {
    const KNOWN_ERRORS: &[&str] = &["ready_invalid", "writer_failed", "publication_failed"];
    if error.schema_version != 1
        || error.dataset_key != resolved.dataset_key
        || !KNOWN_ERRORS.contains(&error.error.as_str())
        || error.message.trim().is_empty()
    {
        bail!("dataset check emitted an invalid FixtureError");
    }
    bail!("dataset fixture {}: {}", error.error, error.message)
}

fn fixture_check_script_path(runner_config: &RunnerConfig, base_dir: &Path) -> PathBuf {
    runner_config
        .values
        .get("benchmark_bootstrap_script")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                base_dir.join(path)
            }
        })
        .unwrap_or_else(|| {
            base_dir
                .join("tests")
                .join("sql")
                .join("fixtures")
                .join("benchmarks")
                .join("bootstrap_benchmark_data.sh")
        })
}

fn command_preview(command: &Command) -> String {
    let mut parts = vec![shell_quote(command.get_program())];
    let mut redact_next = false;

    for arg in command.get_args() {
        if redact_next {
            parts.push("<redacted>".to_string());
            redact_next = false;
            continue;
        }

        parts.push(shell_quote(arg));
        if arg == "--mysql-password" {
            redact_next = true;
        }
    }

    parts.join(" ")
}

fn shell_quote(value: &OsStr) -> String {
    let value = value.to_string_lossy();
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        return value.into_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::suite_manifest::{DatasetName, PreparedDatasetRequirement};
    use std::os::unix::fs::PermissionsExt;

    fn fixture() -> (tempfile::TempDir, RunnerConfig) {
        let temp = tempfile::tempdir().unwrap();
        let resolver = temp.path().join("resolver.py");
        std::fs::write(&resolver, r#"import json,sys
args=sys.argv[1:]
suite=args[args.index('--suite')+1]; scale=args[args.index('--scale')+1]
key={'suite':suite,'scale':scale,'fixture_contract_id':'contract'}
if '--validate-ensure-result' in args or '--validate-error' in args:
 flag='--validate-ensure-result' if '--validate-ensure-result' in args else '--validate-error'
 value=json.load(open(args[args.index(flag)+1])); assert value['dataset_key']==key
else:
 print(json.dumps({'schema_version':1,'dataset_key':key,'dataset_root':'s3://fixture/'+suite,'ready_uri':'s3://fixture/'+suite+'/READY','contract':{'retained':True}}))
"#).unwrap();
        let driver = temp.path().join("check.sh");
        std::fs::write(&driver, r#"#!/usr/bin/env bash
set -euo pipefail
test "$7" = --check
test -n "$NOVA_ENV_REST_ENV_FILE"
printf '%s\n' "$*" >> "$(dirname "$0")/calls"
python3 - "$6" <<'END'
import json,sys
r=json.load(open(sys.argv[1])); assert r['contract']['retained']
suite=r['dataset_key']['suite']
print(json.dumps({'schema_version':1,'dataset_key':r['dataset_key'],'state':'ReadyValid','reused':True,'built':False,'exact_warehouse':'s3://fixture/'+suite+'/warehouse','manifest_uri':'s3://fixture/'+suite+'/warehouse/manifest','publication':{'ready_uri':r['ready_uri'],'etag':'etag-'+suite,'identity':'publication-'+suite}}))
END
"#).unwrap();
        std::fs::set_permissions(&driver, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut config = RunnerConfig::default();
        config.values.insert(
            "benchmark_fixture_resolver".to_string(),
            resolver.display().to_string(),
        );
        config.values.insert(
            "benchmark_bootstrap_script".to_string(),
            driver.display().to_string(),
        );
        config.values.insert(
            "fixture_env_file".to_string(),
            temp.path().join("published/env.sh").display().to_string(),
        );
        (temp, config)
    }

    #[test]
    fn each_suite_binds_exact_ready_and_warehouse_using_only_check() {
        let (temp, config) = fixture();
        let mut bindings = Vec::new();
        for (dataset, scale) in [
            (DatasetName::Ssb, "1"),
            (DatasetName::TpcH, "1"),
            (DatasetName::TpcDs, "1GB"),
        ] {
            bindings.push(
                bind_prepared_dataset(
                    &PreparedDatasetRequirement {
                        dataset,
                        scale: scale.to_string(),
                    },
                    &config,
                    temp.path(),
                )
                .unwrap(),
            );
        }
        for (binding, suite) in bindings.iter().zip(["ssb", "tpc-h", "tpc-ds"]) {
            assert_eq!(binding.dataset_key["suite"], suite);
            assert_eq!(
                binding.exact_warehouse,
                format!("s3://fixture/{suite}/warehouse")
            );
            assert_eq!(binding.publication_identity, format!("publication-{suite}"));
        }
        assert_eq!(
            std::fs::read_to_string(temp.path().join("calls"))
                .unwrap()
                .lines()
                .count(),
            3
        );
    }

    #[test]
    fn oss_dataset_check_preserves_its_explicit_producer_selection() {
        let (temp, mut config) = fixture();
        config
            .values
            .insert("fixture_object_store_kind".into(), "aliyun-oss".into());
        let resolver = PathBuf::from(&config.values["benchmark_fixture_resolver"]);
        let source = std::fs::read_to_string(&resolver).unwrap();
        let log = temp.path().join("producer-selection");
        let assertion = format!(
            "args=sys.argv[1:]\nassert args[args.index('--object-store-kind')+1]=='aliyun-oss'\nwith open({:?}, 'a') as selected: selected.write('checked\\n')",
            log.to_string_lossy()
        );
        std::fs::write(&resolver, source.replace("args=sys.argv[1:]", &assertion)).unwrap();
        bind_prepared_dataset(
            &PreparedDatasetRequirement {
                dataset: DatasetName::Ssb,
                scale: "1".into(),
            },
            &config,
            temp.path(),
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(log).unwrap().lines().count(),
            2,
            "resolution and typed result validation must use the same provider"
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("calls"))
                .unwrap()
                .lines()
                .count(),
            1,
            "cloud consumption remains one check-only call"
        );
    }

    #[test]
    fn missing_ready_fails_once_without_supply_retry() {
        let (temp, config) = fixture();
        let driver = PathBuf::from(&config.values["benchmark_bootstrap_script"]);
        std::fs::write(driver,r#"#!/usr/bin/env bash
set -euo pipefail
test "$7" = --check
printf x >> "$(dirname "$0")/calls"
python3 - "$6" <<'END'
import json,sys
r=json.load(open(sys.argv[1]))
print(json.dumps({'schema_version':1,'dataset_key':r['dataset_key'],'error':'ready_invalid','message':'READY missing'}))
END
exit 1
"#).unwrap();
        let error = bind_prepared_dataset(
            &PreparedDatasetRequirement {
                dataset: DatasetName::Ssb,
                scale: "1".to_string(),
            },
            &config,
            temp.path(),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("READY missing"));
        assert_eq!(
            std::fs::read_to_string(temp.path().join("calls")).unwrap(),
            "x"
        );
    }
}
