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
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct RunManifestReference {
    pub run_id: String,
    pub sha256: String,
}

pub struct RunManifestHandle {
    path: PathBuf,
    manifest: RunManifest,
    finished: bool,
}

#[derive(Debug, Serialize)]
struct RunManifest {
    schema_version: u32,
    formal: bool,
    run_id: String,
    scenario: String,
    command: Vec<String>,
    started_unix_millis: u128,
    ended_unix_millis: Option<u128>,
    exit_code: Option<i32>,
    status: String,
    source_revision: String,
    native_build_identity: String,
    source_tree_sha256: String,
    source_dirty: bool,
    binary_sha256: String,
    runner_executable_path: String,
    runner_executable_sha256: String,
    config_sha256: String,
    workload_manifest_sha256: String,
    fixture_sha256: String,
    tool_tree_sha256: String,
    cargo_lock_sha256: String,
    rustc_version: String,
    cargo_version: String,
    build_profile: String,
    platform: PlatformIdentity,
}

#[derive(Debug, Serialize)]
struct PlatformIdentity {
    os: String,
    os_version: String,
    architecture: String,
    cpu_model: String,
    logical_cpu_count: usize,
    physical_memory_bytes: u64,
    power_mode: String,
}

pub fn begin_run_manifest(
    context: &mut ScenarioContext,
    scenario: &str,
    workload_manifest_sha256: &str,
    workload_manifest_bytes: &[u8],
    fixture_spec: &[u8],
    formal: bool,
) -> Result<RunManifestHandle> {
    ensure!(
        is_sha256(workload_manifest_sha256),
        "performance workload manifest SHA256 is missing or malformed"
    );
    ensure!(
        sha256_bytes(workload_manifest_bytes) == workload_manifest_sha256,
        "performance workload manifest bytes do not match their SHA256"
    );
    let repository = repository_root()?;
    let source_revision = command_text(&repository, "git", &["rev-parse", "HEAD"])?;
    let status = command_text(&repository, "git", &["status", "--porcelain=v1"])?;
    ensure!(
        !formal || status.is_empty(),
        "formal UEA-1 measurement requires a clean source checkout"
    );
    if formal {
        ensure_checkout_release_binary(&repository, context.primary_binary())?;
    }
    let native_build_identity = observe_native_build_identity(context, &source_revision, formal)?;
    let source_tree_sha256 = source_tree_sha256(&repository, &source_revision, &status)?;
    let binary_sha256 = sha256_file(context.primary_binary())?;
    let (runner_executable_path, runner_executable_sha256) =
        runner_executable_identity(&repository, formal)?;
    let config_sha256 = sha256_file(context.base_config_path())?;
    let fixture_sha256 = sha256_bytes(fixture_spec);
    let tool_tree_sha256 = tool_tree_sha256(&repository)?;
    let cargo_lock_sha256 = sha256_file(&repository.join("Cargo.lock"))?;
    let started_unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    let identity_material = format!(
        "{scenario}\0{source_tree_sha256}\0{binary_sha256}\0{started_unix_millis}\0{}",
        std::process::id()
    );
    let run_id = sha256_bytes(identity_material.as_bytes());
    let manifest = RunManifest {
        schema_version: 2,
        formal,
        run_id: run_id.clone(),
        scenario: scenario.to_string(),
        command: std::env::args().collect(),
        started_unix_millis,
        ended_unix_millis: None,
        exit_code: None,
        status: "running".to_string(),
        source_revision,
        native_build_identity,
        source_tree_sha256,
        source_dirty: !status.is_empty(),
        binary_sha256,
        runner_executable_path,
        runner_executable_sha256,
        config_sha256,
        workload_manifest_sha256: workload_manifest_sha256.to_string(),
        fixture_sha256,
        tool_tree_sha256,
        cargo_lock_sha256,
        rustc_version: command_text(&repository, "rustc", &["-vV"])?,
        cargo_version: command_text(&repository, "cargo", &["-vV"])?,
        build_profile: binary_build_profile(context.primary_binary()),
        platform: platform_identity()?,
    };
    let path = context.scenario_root().join("run-manifest.json");
    fs::write(
        context.scenario_root().join("workload-manifest.json"),
        workload_manifest_bytes,
    )
    .context("write frozen workload manifest artifact")?;
    fs::write(
        context.scenario_root().join("fixture-spec.json"),
        fixture_spec,
    )
    .context("write frozen fixture specification artifact")?;
    write_manifest(&path, &manifest)?;
    Ok(RunManifestHandle {
        path,
        manifest,
        finished: false,
    })
}

fn observe_native_build_identity(
    context: &mut ScenarioContext,
    source_revision: &str,
    formal: bool,
) -> Result<String> {
    let expected_backends = context.process_ids().backends.len();
    let topology = context
        .handle()
        .frontend_backend_topology()
        .context("read live BE build identities through structured SHOW BACKENDS")?;
    let live = topology
        .iter()
        .filter(|row| row.is_eligible_live())
        .collect::<Vec<_>>();
    ensure!(
        live.len() == expected_backends,
        "UEA-1 provenance requires every launched BE to be live: launched={expected_backends}, live={}, topology={topology:?}",
        live.len()
    );
    if formal {
        ensure!(
            live.len() == 3,
            "formal UEA-1 provenance requires exactly three live BEs, observed {}",
            live.len()
        );
    }
    let identities = live
        .into_iter()
        .map(|row| row.build_identity.clone())
        .collect::<BTreeSet<_>>();
    validate_native_build_identities(&identities, source_revision, formal)
}

fn validate_native_build_identities(
    identities: &BTreeSet<String>,
    source_revision: &str,
    formal: bool,
) -> Result<String> {
    ensure!(
        identities.len() == 1,
        "UEA-1 provenance requires one native build identity across all live BEs, observed {identities:?}"
    );
    let identity = identities
        .first()
        .expect("one native build identity was checked")
        .clone();
    ensure!(
        !formal || identity == source_revision,
        "formal UEA-1 provenance requires live BE native build identity {identity:?} to equal source revision {source_revision}"
    );
    Ok(identity)
}

impl RunManifestHandle {
    pub fn run_id(&self) -> &str {
        &self.manifest.run_id
    }

    pub fn finish_success(mut self) -> Result<RunManifestReference> {
        self.manifest.ended_unix_millis = Some(now_unix_millis()?);
        self.manifest.exit_code = Some(0);
        self.manifest.status = "completed".to_string();
        let bytes = serde_json::to_vec_pretty(&self.manifest)
            .context("serialize completed UEA-1 run manifest")?;
        let reference = RunManifestReference {
            run_id: self.manifest.run_id.clone(),
            sha256: sha256_bytes(&bytes),
        };
        fs::write(&self.path, bytes).context("write completed UEA-1 run manifest")?;
        self.finished = true;
        Ok(reference)
    }
}

impl Drop for RunManifestHandle {
    fn drop(&mut self) {
        if !self.finished {
            self.manifest.ended_unix_millis = now_unix_millis().ok();
            self.manifest.status = "incomplete".to_string();
            let _ = write_manifest(&self.path, &self.manifest);
        }
    }
}

fn write_manifest(path: &Path, manifest: &RunManifest) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(manifest).context("serialize UEA-1 run manifest")?;
    fs::write(path, bytes).context("write UEA-1 run manifest")
}

fn now_unix_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis())
}

fn repository_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("resolve repository root for UEA-1 provenance")
}

fn binary_build_profile(path: &Path) -> String {
    for component in path.components() {
        let value = component.as_os_str().to_string_lossy();
        if matches!(value.as_ref(), "release" | "dev-opt" | "debug") {
            return value.into_owned();
        }
    }
    "unknown".to_string()
}

fn ensure_checkout_release_binary(repository: &Path, binary: &Path) -> Result<()> {
    let actual = fs::canonicalize(binary)
        .with_context(|| format!("resolve measured binary {}", binary.display()))?;
    let expected_path = repository.join("target/release/novarocks");
    let expected = fs::canonicalize(&expected_path).with_context(|| {
        format!(
            "resolve checkout-local release binary {}",
            expected_path.display()
        )
    })?;
    ensure!(
        actual == expected,
        "formal UEA-1 measurement requires this checkout's target/release/novarocks"
    );
    Ok(())
}

fn runner_executable_identity(repository: &Path, formal: bool) -> Result<(String, String)> {
    let executable = std::env::current_exe().context("resolve current system-test runner")?;
    let actual = fs::canonicalize(&executable).with_context(|| {
        format!(
            "resolve current system-test runner executable {}",
            executable.display()
        )
    })?;
    let actual_sha256 = sha256_file(&actual)?;
    if formal {
        ensure_checkout_release_runner(repository, &actual, &actual_sha256)?;
    }
    let actual_path = actual
        .to_str()
        .context("current system-test runner path is not UTF-8")?
        .to_string();
    Ok((actual_path, actual_sha256))
}

fn ensure_checkout_release_runner(
    repository: &Path,
    actual: &Path,
    actual_sha256: &str,
) -> Result<()> {
    let expected_path = repository.join("target/release/novarocks-system-tests");
    let expected = fs::canonicalize(&expected_path).with_context(|| {
        format!(
            "resolve checkout-local release runner {}",
            expected_path.display()
        )
    })?;
    ensure!(
        actual == expected,
        "formal UEA-1 measurement requires this checkout's target/release/novarocks-system-tests"
    );
    ensure!(
        actual_sha256 == sha256_file(&expected)?,
        "formal UEA-1 runner executable hash does not match the checkout-local release runner"
    );
    Ok(())
}

fn source_tree_sha256(repository: &Path, revision: &str, status: &str) -> Result<String> {
    let tracked = command_bytes(repository, "git", &["ls-files", "-s"])?;
    let diff = command_bytes(repository, "git", &["diff", "--binary", "HEAD"])?;
    let staged = command_bytes(repository, "git", &["diff", "--binary", "--cached"])?;
    let mut hasher = Sha256::new();
    hasher.update(revision.as_bytes());
    hasher.update([0]);
    hasher.update(status.as_bytes());
    hasher.update([0]);
    hasher.update(tracked);
    hasher.update([0]);
    hasher.update(diff);
    hasher.update([0]);
    hasher.update(staged);
    Ok(format!("{:x}", hasher.finalize()))
}

fn tool_tree_sha256(repository: &Path) -> Result<String> {
    let paths = tool_tree_paths(repository)?;
    let mut hasher = Sha256::new();
    for path in paths {
        let relative = path.strip_prefix(repository).unwrap_or(&path);
        hasher.update(relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        hasher.update(fs::read(&path).with_context(|| format!("read tool {}", path.display()))?);
        hasher.update([0]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn tool_tree_paths(repository: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for root in [
        repository.join("tests/benchmarks/uea1"),
        repository.join("tests/system-test-runner/src"),
        repository.join("tests/cluster-harness/src"),
    ] {
        collect_files(&root, &mut paths)?;
    }
    for path in [
        repository.join("tests/system-test-runner/Cargo.toml"),
        repository.join("tests/cluster-harness/Cargo.toml"),
    ] {
        ensure!(
            path.is_file(),
            "UEA-1 tool input {} is missing",
            path.display()
        );
        paths.push(path);
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn collect_files(root: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        fs::read_dir(root).with_context(|| format!("read tool directory {}", root.display()))?
    {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, paths)?;
        } else if path.is_file() && path.extension().is_none_or(|value| value != "pyc") {
            paths.push(path);
        }
    }
    Ok(())
}

fn platform_identity() -> Result<PlatformIdentity> {
    let logical_cpu_count = std::thread::available_parallelism()
        .context("read logical CPU count")?
        .get();
    #[cfg(target_os = "macos")]
    let (os_version, cpu_model, physical_memory_bytes, power_mode) = (
        command_text(Path::new("."), "sw_vers", &["-productVersion"])?,
        command_text(
            Path::new("."),
            "sysctl",
            &["-n", "machdep.cpu.brand_string"],
        )?,
        command_text(Path::new("."), "sysctl", &["-n", "hw.memsize"])?
            .parse()
            .context("parse macOS physical memory")?,
        command_text(Path::new("."), "pmset", &["-g", "batt"])
            .map(|output| output.lines().next().unwrap_or("unknown").to_string())
            .unwrap_or_else(|error| format!("unavailable: {error:#}")),
    );
    #[cfg(target_os = "linux")]
    let (os_version, cpu_model, physical_memory_bytes, power_mode) = {
        let os_version = fs::read_to_string("/etc/os-release")
            .unwrap_or_else(|error| format!("unavailable: {error}"));
        let cpu_info = fs::read_to_string("/proc/cpuinfo").context("read /proc/cpuinfo")?;
        let cpu_model = cpu_info
            .lines()
            .find_map(|line| line.strip_prefix("model name\t: "))
            .unwrap_or("unknown")
            .to_string();
        let mem_info = fs::read_to_string("/proc/meminfo").context("read /proc/meminfo")?;
        let physical_memory_bytes = mem_info
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
            .context("parse Linux physical memory")?
            .saturating_mul(1024);
        (
            os_version,
            cpu_model,
            physical_memory_bytes,
            "not-reported".to_string(),
        )
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let (os_version, cpu_model, physical_memory_bytes, power_mode) = (
        "unknown".to_string(),
        "unknown".to_string(),
        0,
        "unknown".to_string(),
    );
    ensure!(physical_memory_bytes > 0, "physical memory is unavailable");
    Ok(PlatformIdentity {
        os: std::env::consts::OS.to_string(),
        os_version,
        architecture: std::env::consts::ARCH.to_string(),
        cpu_model,
        logical_cpu_count,
        physical_memory_bytes,
        power_mode,
    })
}

fn command_text(directory: &Path, program: &str, arguments: &[&str]) -> Result<String> {
    let bytes = command_bytes(directory, program, arguments)?;
    let text =
        String::from_utf8(bytes).with_context(|| format!("{program} returned non-UTF8 output"))?;
    Ok(text.trim().to_string())
}

fn command_bytes(directory: &Path, program: &str, arguments: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new(program)
        .current_dir(directory)
        .args(arguments)
        .output()
        .with_context(|| format!("run {program} for UEA-1 provenance"))?;
    if !output.status.success() {
        bail!("{program} failed while collecting UEA-1 provenance")
    }
    Ok(output.stdout)
}

fn sha256_file(path: &Path) -> Result<String> {
    Ok(sha256_bytes(&fs::read(path).with_context(|| {
        format!("read {} for SHA256", path.display())
    })?))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_validation_rejects_missing_provenance() {
        assert!(is_sha256(&"a".repeat(64)));
        assert!(!is_sha256("not-a-hash"));
        assert!(!is_sha256(&"a".repeat(63)));
    }

    #[test]
    fn build_profile_comes_from_the_measured_binary_path() {
        assert_eq!(
            binary_build_profile(Path::new("/checkout/target/release/novarocks")),
            "release"
        );
        assert_eq!(
            binary_build_profile(Path::new("/checkout/external/novarocks")),
            "unknown"
        );
    }

    #[test]
    fn formal_binary_must_be_the_checkout_local_release_binary() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-uea1-provenance-{}-{}",
            std::process::id(),
            now_unix_millis().expect("clock")
        ));
        let repository = root.join("checkout");
        let expected = repository.join("target/release/novarocks");
        let external = root.join("external/release/novarocks");
        fs::create_dir_all(expected.parent().expect("expected parent")).expect("expected dir");
        fs::create_dir_all(external.parent().expect("external parent")).expect("external dir");
        fs::write(&expected, b"expected").expect("expected binary");
        fs::write(&external, b"external").expect("external binary");

        ensure_checkout_release_binary(&repository, &expected).expect("checkout binary");
        assert!(ensure_checkout_release_binary(&repository, &external).is_err());
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn formal_native_build_identity_must_equal_source_revision() {
        let identities = BTreeSet::from(["old-revision".to_string()]);
        let error = validate_native_build_identities(
            &identities,
            "0123456789abcdef0123456789abcdef01234567",
            true,
        )
        .expect_err("formal run must reject a stale embedded build identity");
        assert!(
            error
                .to_string()
                .contains("to equal source revision 0123456789abcdef")
        );
    }

    #[test]
    fn nonformal_native_build_identity_is_recorded_without_source_equality() {
        let identities = BTreeSet::from(["smoke-build".to_string()]);
        assert_eq!(
            validate_native_build_identities(
                &identities,
                "0123456789abcdef0123456789abcdef01234567",
                false,
            )
            .expect("smoke run may execute a different embedded build identity"),
            "smoke-build"
        );
    }

    #[test]
    fn tool_tree_includes_complete_runner_and_cluster_harness_inputs() {
        let repository = repository_root().expect("repository root");
        let paths = tool_tree_paths(&repository).expect("tool tree inputs");
        for expected in [
            "tests/benchmarks/uea1/artifact_protocol.py",
            "tests/system-test-runner/Cargo.toml",
            "tests/system-test-runner/src/scenarios/uea1_performance.rs",
            "tests/cluster-harness/Cargo.toml",
            "tests/cluster-harness/src/process_resources.rs",
        ] {
            assert!(
                paths.contains(&repository.join(expected)),
                "tool tree omitted {expected}"
            );
        }
    }

    #[test]
    fn formal_runner_must_be_checkout_local_with_its_exact_hash() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-uea1-runner-provenance-{}-{}",
            std::process::id(),
            now_unix_millis().expect("clock")
        ));
        let repository = root.join("checkout");
        let expected = repository.join("target/release/novarocks-system-tests");
        let external = root.join("external/release/novarocks-system-tests");
        fs::create_dir_all(expected.parent().expect("expected parent")).expect("expected dir");
        fs::create_dir_all(external.parent().expect("external parent")).expect("external dir");
        fs::write(&expected, b"expected runner").expect("expected runner");
        fs::write(&external, b"external runner").expect("external runner");
        let expected = fs::canonicalize(expected).expect("canonical expected runner");
        let external = fs::canonicalize(external).expect("canonical external runner");
        let expected_sha = sha256_file(&expected).expect("expected runner hash");

        ensure_checkout_release_runner(&repository, &expected, &expected_sha)
            .expect("checkout runner");
        assert!(ensure_checkout_release_runner(&repository, &external, &expected_sha).is_err());
        assert!(ensure_checkout_release_runner(&repository, &expected, &"0".repeat(64)).is_err());
        fs::remove_dir_all(root).expect("remove fixture");
    }
}
