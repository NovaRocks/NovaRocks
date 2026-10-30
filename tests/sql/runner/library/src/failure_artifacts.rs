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

use crate::cluster::{ServerFailureLogSources, ServerHandle};
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static ARTIFACT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const MAX_FAILURE_SNAPSHOTS: usize = 2;
const MAX_CAPTURED_BACKEND_LOGS: usize = 31;
const MAX_PROCESS_LOG_TAIL_BYTES: usize = 128 * 1024;
const MAX_FAILURE_SNAPSHOT_BYTES: usize =
    (MAX_CAPTURED_BACKEND_LOGS + 1) * MAX_PROCESS_LOG_TAIL_BYTES;

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

pub(crate) struct FailureArtifactContext {
    pub(crate) root: PathBuf,
    pub(crate) lane: String,
    pub(crate) suites: Vec<String>,
}

#[derive(Default)]
struct CaptureState {
    first_causal_claimed: bool,
    terminal_claimed: bool,
}

#[derive(Clone, Copy)]
enum SnapshotSlot {
    FirstCausal,
    Terminal,
}

impl SnapshotSlot {
    fn label(self) -> &'static str {
        match self {
            Self::FirstCausal => "first-causal",
            Self::Terminal => "terminal",
        }
    }
}

/// Coordinates failure snapshots across parallel cases and final run cleanup.
///
/// The recorder owns two immutable slots for the entire run: the first causal
/// failure and the terminal failed-run state. This keeps diagnostic work
/// bounded while retaining both the earliest evidence and failures that happen
/// during suite cleanup or final runner teardown.
pub(crate) struct FailureArtifactRecorder {
    context: FailureArtifactContext,
    state: Mutex<CaptureState>,
    provenance: Mutex<BTreeMap<String, serde_json::Value>>,
    case_artifact_root: Mutex<Option<PathBuf>>,
}

pub(crate) struct StagedRunFailure {
    captured: CapturedFailureLogs,
}

/// Recovery material defaults to retention, including unwinding or controller
/// exit. Possessing these bytes proves neither writer exit nor cleanup rights.
pub(crate) struct OriginalFixtureRunArtifact {
    artifact: PendingArtifactDir,
}
impl OriginalFixtureRunArtifact {
    pub(crate) fn path(&self) -> &Path {
        &self.artifact.path
    }

    /// Release only this private leaf after the original run cleanup succeeds.
    /// Reuse the artifact owner's existing best-effort local disposal policy.
    pub(crate) fn release_after_cleanup(mut self) -> bool {
        let path = self.artifact.path.clone();
        self.artifact.committed = false;
        drop(self);
        path.try_exists().is_ok_and(|present| !present)
    }
}

impl FailureArtifactRecorder {
    pub(crate) fn new(context: FailureArtifactContext) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let sequence = ARTIFACT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let attempt = format!("sql-{timestamp}-{}-{sequence}", std::process::id());
        let provenance = BTreeMap::from([(
            "attempt".to_string(),
            serde_json::json!({
                "identity": attempt, "lane": context.lane, "suites": context.suites,
            }),
        )]);
        Self {
            context,
            state: Mutex::new(CaptureState::default()),
            provenance: Mutex::new(provenance),
            case_artifact_root: Mutex::new(None),
        }
    }

    pub(crate) fn persist_original_fixture_run(
        &self,
        context: &str,
        storage: &str,
        env_file: &Path,
        owns_fixture_run: bool,
    ) -> Result<OriginalFixtureRunArtifact> {
        let attempt = self
            .provenance
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get("attempt")
            .cloned()
            .context("original SQL attempt provenance absent")?;
        let mut artifact = PendingArtifactDir::create(&self.context.root, "original-fixture-run")?;
        artifact.write_file("context.json", context)?;
        artifact.write_file("storage.json", storage)?;
        artifact.write_file(
            "origin.json",
            &serde_json::to_string_pretty(&serde_json::json!({
                "attempt": attempt, "env_file": env_file, "owns_fixture_run": owns_fixture_run,
                "authority_note": "original bytes do not prove writer exit or grant cleanup rights"
            }))?,
        )?;
        // A completed write must survive later ordinary errors and unwinding.
        // Case-diagnostic provenance is not frozen early by this separate leaf.
        artifact.committed = true;
        self.record_provenance(
            "original-fixture-run-material".into(),
            serde_json::json!({"directory": artifact.path, "owns_fixture_run": owns_fixture_run}),
        );
        Ok(OriginalFixtureRunArtifact { artifact })
    }

    pub(crate) fn persist_terminal_outcome(
        &self,
        snapshot: &Path,
        outcome: &serde_json::Value,
    ) -> Result<()> {
        let path = snapshot.join("outcome.json");
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&path)
            .context("create terminal cleanup outcome")?;
        file.write_all(serde_json::to_string_pretty(outcome)?.as_bytes())?;
        file.sync_all().context("sync terminal cleanup outcome")
    }

    pub(crate) fn record_provenance(&self, key: String, value: serde_json::Value) {
        self.provenance
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, value);
    }

    pub(crate) fn record_build_provenance(
        &self,
        base_dir: &Path,
        profile: novarocks_cluster_harness::LaunchProfile,
        mode: crate::cluster::ClusterMode,
        backend_count: usize,
    ) -> Result<()> {
        let binary_fact = |path: PathBuf| -> Result<serde_json::Value> {
            let path = path.canonicalize()?;
            let mut file = fs::File::open(&path)?;
            let mut hash = Sha256::new();
            let mut buffer = [0_u8; 65536];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hash.update(&buffer[..read]);
            }
            Ok(
                serde_json::json!({"path": path, "sha256": format!("{:x}", hash.finalize()),
                "compiled_capabilities": "unknown", "effective_cargo_profile": "unknown"}),
            )
        };
        let git = |args: &[&str]| -> Option<String> {
            std::process::Command::new("git")
                .current_dir(base_dir)
                .args(args)
                .output()
                .ok()
                .filter(|out| out.status.success())
                .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        };
        let server = crate::cluster::discover_novarocks_binary(base_dir)
            .and_then(&binary_fact)
            .unwrap_or_else(|error| serde_json::json!({"source_error": format!("{error:#}")}));
        let runner = std::env::current_exe()
            .map_err(anyhow::Error::from)
            .and_then(binary_fact)?;
        self.record_provenance("build-and-launch".to_string(), serde_json::json!({
            "source_head": git(&["rev-parse", "HEAD"]), "source_worktree_status": git(&["status", "--porcelain"]),
            "runner": runner, "server": server,
            "requested_launch_profile": format!("{profile:?}"), "cluster_mode": format!("{mode:?}"),
            "requested_backend_count": backend_count,
            "capability_note": "Binary hashes identify content. Each actual cross-process role launch separately checks and retains its binary-native build-info before spawn; unknown compiler settings remain unknown and self-report does not certify release acceptance."
        }));
        Ok(())
    }

    fn provenance_json(&self) -> Result<String> {
        serde_json::to_string_pretty(&*self.provenance.lock().unwrap_or_else(|p| p.into_inner()))
            .context("serialize SQL failure provenance")
    }

    pub(crate) fn case_artifact_root(&self) -> Result<PathBuf> {
        let mut root = self
            .case_artifact_root
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(path) = &*root {
            return Ok(path.clone());
        }
        let pending = PendingArtifactDir::create(&self.context.root, "case-diagnostics")?;
        pending.write_file("provenance.json", &self.provenance_json()?)?;
        let path = pending.commit();
        *root = Some(path.clone());
        Ok(path)
    }

    pub(crate) fn persist_case_diagnostic(
        &self,
        suite: &str,
        case_id: &str,
        source: &Path,
        expected: Option<&Path>,
        log: &str,
    ) -> Result<PathBuf> {
        let root = self.case_artifact_root()?;
        let pending = PendingArtifactDir::create(
            &root,
            &case_artifact_name_prefix(&self.context.lane, suite, case_id, None),
        )?;
        pending.write_file("case.json", &serde_json::to_string_pretty(&serde_json::json!({
            "suite": suite, "case": case_id, "source": source, "expected_source": expected,
            "actual_note": "Actual rows are retained only when query execution produced a row set."
        }))?)?;
        pending.write_file("failure.txt", log)?;
        pending.write_file(
            "case.sql",
            &fs::read_to_string(source)
                .with_context(|| format!("read failed case {}", source.display()))?,
        )?;
        if let Some(expected) = expected.filter(|p| p.is_file()) {
            pending.write_file("expected.tsv", &fs::read_to_string(expected)?)?;
        }
        Ok(pending.commit())
    }

    /// Preserve an actual row result even when its text oracle has no golden.
    pub(crate) fn persist_case_actual_result(
        &self,
        suite: &str,
        case_id: &str,
        query: usize,
        execution: &crate::types::QueryExecution,
    ) -> Result<PathBuf> {
        let root = self.case_artifact_root()?;
        let pending = PendingArtifactDir::create(
            &root,
            &case_artifact_name_prefix(&self.context.lane, suite, case_id, Some(query)),
        )?;
        pending.write_file(
            "actual.json",
            &serde_json::to_string_pretty(&serde_json::json!({
                "suite": suite, "case": case_id, "query": query,
                "header": &execution.header, "rows": &execution.rows,
            }))?,
        )?;
        pending.write_file("actual.txt", &execution.text_output)?;
        Ok(pending.commit())
    }

    pub(crate) fn persist_case_failure(
        &self,
        server_handle: &Mutex<Box<dyn ServerHandle>>,
        suite: &str,
        case_id: &str,
        step_number: Option<usize>,
    ) -> Result<Option<PathBuf>> {
        let prefix = case_artifact_name_prefix(&self.context.lane, suite, case_id, step_number);
        self.persist_claimed(server_handle, SnapshotSlot::FirstCausal, prefix)
    }

    pub(crate) fn persist_suite_failure(
        &self,
        server_handle: &Mutex<Box<dyn ServerHandle>>,
        suite: &str,
        phase: &str,
    ) -> Result<Option<PathBuf>> {
        let prefix = suite_artifact_name_prefix(&self.context.lane, suite, phase);
        self.persist_claimed(server_handle, SnapshotSlot::FirstCausal, prefix)
    }

    pub(crate) fn persist_run_failure(
        &self,
        server_handle: &Mutex<Box<dyn ServerHandle>>,
    ) -> Result<Option<PathBuf>> {
        let prefix = format!(
            "{}-terminal",
            artifact_name_prefix(&self.context.lane, &self.context.suites)
        );
        self.persist_claimed(server_handle, SnapshotSlot::Terminal, prefix)
    }

    /// Captures the terminal candidate while process logs still exist, without
    /// claiming a slot or creating an artifact. A successful shutdown drops
    /// this bounded value; a shutdown failure can persist it afterwards.
    pub(crate) fn stage_run_failure(
        &self,
        server_handle: &Mutex<Box<dyn ServerHandle>>,
    ) -> Result<Option<StagedRunFailure>> {
        detach_failure_log_sources(server_handle)?
            .map(|sources| {
                capture_failure_log_sources(sources, SnapshotSlot::Terminal)
                    .map(|captured| StagedRunFailure { captured })
            })
            .transpose()
    }

    pub(crate) fn persist_staged_run_failure(
        &self,
        staged: Option<StagedRunFailure>,
    ) -> Result<Option<PathBuf>> {
        let Some(mut staged) = staged else {
            return Ok(None);
        };
        if !self.claim(SnapshotSlot::Terminal) {
            return Ok(None);
        }
        let prefix = format!(
            "{}-terminal",
            artifact_name_prefix(&self.context.lane, &self.context.suites)
        );
        staged.captured.provenance = Some(self.provenance_json()?);
        let result =
            persist_captured_failure_logs(&self.context.root, &prefix, staged.captured).map(Some);
        if result.is_err() {
            self.release(SnapshotSlot::Terminal);
        }
        result
    }

    fn persist_claimed(
        &self,
        server_handle: &Mutex<Box<dyn ServerHandle>>,
        slot: SnapshotSlot,
        prefix: String,
    ) -> Result<Option<PathBuf>> {
        if !self.claim(slot) {
            return Ok(None);
        }

        // The server lock protects only path/Arc cloning and bounded history
        // tails. Durable-log reads, redaction, file creation, writes, and syncs
        // all happen after the global handle is free.
        let result = detach_failure_log_sources(server_handle)
            .and_then(|sources| {
                sources
                    .map(|sources| capture_failure_log_sources(sources, slot))
                    .transpose()
            })
            .and_then(|captured| {
                captured
                    .map(|mut captured| {
                        captured.provenance = Some(self.provenance_json()?);
                        persist_captured_failure_logs(&self.context.root, &prefix, captured)
                    })
                    .transpose()
            });
        if !matches!(result, Ok(Some(_))) {
            self.release(slot);
        }
        result
    }

    fn claim(&self, slot: SnapshotSlot) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let claimed = match slot {
            SnapshotSlot::FirstCausal => &mut state.first_causal_claimed,
            SnapshotSlot::Terminal => &mut state.terminal_claimed,
        };
        if *claimed {
            false
        } else {
            *claimed = true;
            true
        }
    }

    fn release(&self, slot: SnapshotSlot) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match slot {
            SnapshotSlot::FirstCausal => state.first_causal_claimed = false,
            SnapshotSlot::Terminal => state.terminal_claimed = false,
        }
    }
}

struct CapturedProcessLog {
    name: String,
    original_bytes: usize,
    contents: String,
}

struct CapturedFailureLogs {
    slot: SnapshotSlot,
    backend_count: usize,
    logs: Vec<CapturedProcessLog>,
    provenance: Option<String>,
}

struct PendingArtifactDir {
    path: PathBuf,
    committed: bool,
}

fn create_private_directory(path: &Path) -> std::io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(path)
}

fn prepare_artifact_root(root: &Path) -> Result<()> {
    match fs::symlink_metadata(root) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!(
                    "SQL failure artifact root must not be a symbolic link: {}",
                    root.display()
                );
            }
            if !metadata.is_dir() {
                bail!(
                    "SQL failure artifact root is not a directory: {}",
                    root.display()
                );
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = root.parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "create parent of SQL failure artifact root {}",
                        parent.display()
                    )
                })?;
            }
            match create_private_directory(root) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    prepare_artifact_root(root)
                }
                Err(error) => Err(error).with_context(|| {
                    format!("create SQL failure artifact root {}", root.display())
                }),
            }
        }
        Err(error) => Err(error)
            .with_context(|| format!("inspect SQL failure artifact root {}", root.display())),
    }
}

impl PendingArtifactDir {
    fn create(root: &Path, name_prefix: &str) -> Result<Self> {
        prepare_artifact_root(root)?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_nanos();
        let pid = std::process::id();

        for _ in 0..64 {
            let sequence = ARTIFACT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = root.join(format!("{name_prefix}-{timestamp:020}-{pid}-{sequence:04}"));
            match create_private_directory(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        committed: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("create SQL failure artifact directory {}", path.display())
                    });
                }
            }
        }

        bail!(
            "could not allocate a unique SQL failure artifact directory under {}",
            root.display()
        )
    }

    fn write_file(&self, name: &str, contents: &str) -> Result<()> {
        let mut components = Path::new(name).components();
        if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
            bail!("SQL failure artifact file name must not contain a directory: {name:?}");
        }
        let path = self.path.join(name);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&path)
            .with_context(|| format!("create SQL failure artifact {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("write SQL failure artifact {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync SQL failure artifact {}", path.display()))
    }

    fn commit(mut self) -> PathBuf {
        self.committed = true;
        self.path.clone()
    }
}

impl Drop for PendingArtifactDir {
    fn drop(&mut self) {
        if self.committed {
            return;
        }

        // The guard owns one freshly-created leaf. Never recurse into an
        // operator-configured artifact root when cleaning a partial write.
        if let Ok(entries) = fs::read_dir(&self.path) {
            for entry in entries.flatten() {
                let Ok(file_type) = entry.file_type() else {
                    continue;
                };
                if file_type.is_file() || file_type.is_symlink() {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
        let _ = fs::remove_dir(&self.path);
    }
}

#[cfg(test)]
fn persist_cross_process_failure_logs(
    server: &dyn ServerHandle,
    context: &FailureArtifactContext,
) -> Result<Option<PathBuf>> {
    capture_cross_process_failure_logs(server, SnapshotSlot::FirstCausal)?
        .map(|captured| {
            persist_captured_failure_logs(
                &context.root,
                &artifact_name_prefix(&context.lane, &context.suites),
                captured,
            )
        })
        .transpose()
}

#[cfg(test)]
fn capture_cross_process_failure_logs(
    server: &dyn ServerHandle,
    slot: SnapshotSlot,
) -> Result<Option<CapturedFailureLogs>> {
    server
        .failure_log_sources(MAX_CAPTURED_BACKEND_LOGS, MAX_PROCESS_LOG_TAIL_BYTES)?
        .map(|sources| capture_failure_log_sources(sources, slot))
        .transpose()
}

fn detach_failure_log_sources(
    server_handle: &Mutex<Box<dyn ServerHandle>>,
) -> Result<Option<ServerFailureLogSources>> {
    match server_handle.lock() {
        Ok(server) => server
            .failure_log_sources(MAX_CAPTURED_BACKEND_LOGS, MAX_PROCESS_LOG_TAIL_BYTES)
            .context("detach cross-process failure log sources"),
        Err(_) => Err(anyhow::anyhow!(
            "server handle lock poisoned while detaching failure log sources"
        )),
    }
}

fn capture_failure_log_sources(
    sources: ServerFailureLogSources,
    slot: SnapshotSlot,
) -> Result<CapturedFailureLogs> {
    let captured = sources
        .capture(MAX_PROCESS_LOG_TAIL_BYTES)
        .context("capture bounded cross-process failure log tails")?;
    Ok(CapturedFailureLogs {
        slot,
        backend_count: captured.backend_count,
        provenance: None,
        logs: captured
            .logs
            .into_iter()
            .map(|log| CapturedProcessLog {
                name: log.name,
                original_bytes: log.original_bytes,
                contents: log.contents,
            })
            .collect(),
    })
}

fn persist_captured_failure_logs(
    root: &Path,
    name_prefix: &str,
    captured: CapturedFailureLogs,
) -> Result<PathBuf> {
    if captured.logs.len() > MAX_CAPTURED_BACKEND_LOGS + 1 {
        bail!(
            "failure snapshot contains {} process logs, exceeding the limit {}",
            captured.logs.len(),
            MAX_CAPTURED_BACKEND_LOGS + 1
        );
    }
    let retained_snapshot_bytes = captured.logs.iter().try_fold(0_usize, |total, log| {
        total
            .checked_add(log.contents.len())
            .ok_or_else(|| anyhow::anyhow!("failure snapshot retained byte accounting overflowed"))
    })?;
    if retained_snapshot_bytes > MAX_FAILURE_SNAPSHOT_BYTES {
        bail!(
            "failure snapshot retained {retained_snapshot_bytes} bytes, exceeding the limit {MAX_FAILURE_SNAPSHOT_BYTES}"
        );
    }
    let pending = PendingArtifactDir::create(root, name_prefix)?;
    let mut manifest = format!(
        "schema_version=2\nslot={}\nmax_snapshots={}\nmax_backend_logs={}\nmax_process_log_tail_bytes={}\nmax_snapshot_bytes={}\nretained_snapshot_bytes={}\nredaction_scope=source-structured-redaction,known-sensitive-child-environment-values\nbackend_count={}\ncaptured_backend_logs={}\nomitted_backend_logs={}\n",
        captured.slot.label(),
        MAX_FAILURE_SNAPSHOTS,
        MAX_CAPTURED_BACKEND_LOGS,
        MAX_PROCESS_LOG_TAIL_BYTES,
        MAX_FAILURE_SNAPSHOT_BYTES,
        retained_snapshot_bytes,
        captured.backend_count,
        captured.logs.len().saturating_sub(1),
        captured
            .backend_count
            .saturating_sub(MAX_CAPTURED_BACKEND_LOGS),
    );
    for log in &captured.logs {
        let _ = writeln!(
            manifest,
            "log={} original_bytes={} retained_bytes={}",
            log.name,
            log.original_bytes,
            log.contents.len()
        );
        pending.write_file(&log.name, &log.contents)?;
    }
    pending.write_file("manifest.txt", &manifest)?;
    if let Some(provenance) = captured.provenance {
        pending.write_file("provenance.json", &provenance)?;
    }

    Ok(pending.commit())
}

fn case_claim(suite: &str, case_id: &str) -> String {
    let mut hasher = Sha256::new();
    for value in [suite, case_id] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    format!("case:{:x}", hasher.finalize())
}

fn case_artifact_name_prefix(
    lane: &str,
    suite: &str,
    case_id: &str,
    step_number: Option<usize>,
) -> String {
    let claim = case_claim(suite, case_id);
    let lane = sanitize_component(lane);
    let suite = safe_suite_label(suite).unwrap_or_else(|| "suite".to_string());
    let case = safe_suite_label(case_id).unwrap_or_else(|| "case".to_string());
    let step = step_number
        .map(|number| format!("step-{number}"))
        .unwrap_or_else(|| "case".to_string());
    format!("sql-failure-{lane}-{suite}-{case}-{step}-{}", &claim[5..17])
}

fn suite_artifact_name_prefix(lane: &str, suite: &str, phase: &str) -> String {
    let lane = sanitize_component(lane);
    let suite = safe_suite_label(suite).unwrap_or_else(|| "suite".to_string());
    let phase = sanitize_component(phase);
    format!("sql-failure-{lane}-{suite}-suite-{phase}")
}

fn artifact_name_prefix(lane: &str, suites: &[String]) -> String {
    let lane = sanitize_component(lane);
    let suite_label = match suites.len() {
        0 => "no-suite".to_string(),
        1 => safe_suite_label(&suites[0]).unwrap_or_else(|| "1-suite".to_string()),
        count => format!("{count}-suites"),
    };
    let mut hasher = Sha256::new();
    for suite in suites {
        hasher.update((suite.len() as u64).to_be_bytes());
        hasher.update(suite.as_bytes());
    }
    let digest = format!("{:x}", hasher.finalize());
    format!("sql-failure-{lane}-{suite_label}-{}", &digest[..12])
}

fn safe_suite_label(raw: &str) -> Option<String> {
    if raw.is_empty()
        || raw.len() > 48
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return None;
    }
    Some(sanitize_component(raw))
}

fn sanitize_component(raw: &str) -> String {
    let mut sanitized = String::with_capacity(raw.len().min(48));
    let mut previous_dash = false;
    for byte in raw.bytes().take(48) {
        let ch = match byte {
            b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' => byte as char,
            b'A'..=b'Z' => (byte + (b'a' - b'A')) as char,
            _ => '-',
        };
        if ch == '-' {
            if previous_dash {
                continue;
            }
            previous_dash = true;
        } else {
            previous_dash = false;
        }
        sanitized.push(ch);
    }
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "unnamed".to_string()
    } else {
        sanitized.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    struct LogServer {
        fe_log: String,
        be_logs: Vec<String>,
    }

    impl ServerHandle for LogServer {
        fn target_host(&self) -> Option<&str> {
            Some("127.0.0.1")
        }

        fn target_port(&self) -> Option<u16> {
            Some(9030)
        }

        fn be_count(&self) -> usize {
            self.be_logs.len()
        }

        fn failure_log_sources(
            &self,
            max_backend_logs: usize,
            max_history_tail_bytes: usize,
        ) -> Result<Option<ServerFailureLogSources>> {
            if self.be_logs.is_empty() {
                return Ok(None);
            }
            Ok(Some(ServerFailureLogSources::from_inline(
                self.be_logs.len(),
                self.fe_log.clone(),
                self.be_logs
                    .iter()
                    .take(max_backend_logs)
                    .cloned()
                    .collect(),
                max_history_tail_bytes,
            )))
        }
    }

    fn context(root: &Path) -> FailureArtifactContext {
        FailureArtifactContext {
            root: root.to_path_buf(),
            lane: "Correctness Lane".to_string(),
            suites: vec!["analytic".to_string()],
        }
    }

    fn original_fixture_material() -> (String, String) {
        use novarocks_cluster_harness::fixture_run::{
            FixturePolicy, RunBudget, RunStorageScope, ScratchIdentity,
        };
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../docker/iceberg-rest/tests/run_storage_scope_vectors.json"
        ))
        .unwrap();
        let identity: ScratchIdentity =
            serde_json::from_value(vectors["identity"].clone()).unwrap();
        let budget = RunBudget::admit_at(
            FixturePolicy::default(),
            identity,
            600,
            None,
            1_800_000_000_000,
            std::time::Instant::now(),
        )
        .unwrap();
        let storage = RunStorageScope::from_record(budget.identity(), &vectors["record"]).unwrap();
        (budget.to_json().unwrap(), storage.to_json().unwrap())
    }

    #[test]
    fn original_run_bytes_survive_controller_unwind_in_private_artifacts() {
        let temp = TempDir::new().unwrap();
        let recorder = FailureArtifactRecorder::new(context(temp.path()));
        let (original_context, original_storage) = original_fixture_material();
        let saved = recorder
            .persist_original_fixture_run(
                &original_context,
                &original_storage,
                Path::new("/original/publication/env.sh"),
                false,
            )
            .unwrap();
        let path = saved.path().to_path_buf();
        let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _original = saved;
            panic!("controller stopped after the original material was saved")
        }));
        assert!(stopped.is_err());
        assert_eq!(
            fs::read_to_string(path.join("context.json")).unwrap(),
            original_context
        );
        assert_eq!(
            fs::read_to_string(path.join("storage.json")).unwrap(),
            original_storage
        );
        let origin: serde_json::Value =
            serde_json::from_slice(&fs::read(path.join("origin.json")).unwrap()).unwrap();
        assert_eq!(origin["owns_fixture_run"], false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o700
            );
            for name in ["context.json", "storage.json", "origin.json"] {
                assert_eq!(
                    fs::metadata(path.join(name)).unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
        }
        let provenance = recorder.provenance_json().unwrap();
        let value: serde_json::Value = serde_json::from_str(&provenance).unwrap();
        assert_eq!(origin["attempt"], value["attempt"]);
        assert_eq!(
            value["original-fixture-run-material"],
            serde_json::json!({"directory": path, "owns_fixture_run": false})
        );
        assert!(!provenance.contains("deadline_unix_ms"));
        assert!(!provenance.contains("storage_owner"));
    }

    #[test]
    fn successful_original_cleanup_releases_only_its_material_leaf() {
        let temp = TempDir::new().unwrap();
        let recorder = FailureArtifactRecorder::new(context(temp.path()));
        let (original_context, original_storage) = original_fixture_material();
        let kept = recorder
            .persist_original_fixture_run(
                &original_context,
                &original_storage,
                Path::new("/original/publication/env.sh"),
                true,
            )
            .unwrap();
        let kept_path = kept.path().to_path_buf();
        drop(kept);
        assert_eq!(
            fs::read_to_string(kept_path.join("context.json")).unwrap(),
            original_context
        );
        let retired = recorder
            .persist_original_fixture_run(
                &original_context,
                &original_storage,
                Path::new("/original/publication/env.sh"),
                true,
            )
            .unwrap();
        let retired_path = retired.path().to_path_buf();
        assert!(retired.release_after_cleanup());
        assert!(!retired_path.exists());
        assert_eq!(
            fs::read_to_string(kept_path.join("storage.json")).unwrap(),
            original_storage
        );
        assert!(temp.path().is_dir());
    }

    #[test]
    fn failure_artifacts_use_stable_safe_names_and_only_copy_logs_and_manifest() {
        let temp = TempDir::new().expect("temp dir");
        let server = LogServer {
            fe_log: "FE diagnostic\n".to_string(),
            be_logs: vec!["BE zero\n".to_string(), "BE one\n".to_string()],
        };

        let path = persist_cross_process_failure_logs(&server, &context(temp.path()))
            .expect("persist logs")
            .expect("cross-process artifacts");
        let name = path.file_name().unwrap().to_string_lossy();
        assert!(
            name.starts_with("sql-failure-correctness-lane-analytic-"),
            "{name}"
        );
        assert_eq!(
            fs::read_to_string(path.join("fe.log")).unwrap(),
            "FE diagnostic\n"
        );
        assert_eq!(
            fs::read_to_string(path.join("be-000.log")).unwrap(),
            "BE zero\n"
        );
        assert_eq!(
            fs::read_to_string(path.join("be-001.log")).unwrap(),
            "BE one\n"
        );
        let names = fs::read_dir(&path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(
            names.len(),
            4,
            "only FE/BE logs and metadata may be persisted"
        );
        let manifest = fs::read_to_string(path.join("manifest.txt")).unwrap();
        assert!(manifest.contains("slot=first-causal"), "{manifest}");
        assert!(manifest.contains("backend_count=2"), "{manifest}");
        assert!(
            manifest.contains("redaction_scope=source-structured-redaction"),
            "{manifest}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn failure_artifact_leaf_is_owner_only_without_rewriting_an_existing_root() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("temp dir");
        let root = temp.path().join("private-artifacts");
        let server = LogServer {
            fe_log: "FE diagnostic\n".to_string(),
            be_logs: vec!["BE diagnostic\n".to_string()],
        };

        let artifact = persist_cross_process_failure_logs(&server, &context(&root))
            .expect("persist logs")
            .expect("cross-process artifacts");
        assert_eq!(
            fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&artifact).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for name in ["fe.log", "be-000.log", "manifest.txt"] {
            assert_eq!(
                fs::metadata(artifact.join(name))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600,
                "{name}"
            );
        }

        let existing_root = temp.path().join("shared-artifacts");
        fs::create_dir(&existing_root).unwrap();
        fs::set_permissions(&existing_root, fs::Permissions::from_mode(0o750)).unwrap();
        let artifact = persist_cross_process_failure_logs(&server, &context(&existing_root))
            .expect("persist under existing root")
            .expect("cross-process artifacts");
        assert_eq!(
            fs::metadata(&existing_root).unwrap().permissions().mode() & 0o777,
            0o750
        );
        assert_eq!(
            fs::metadata(artifact).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn non_cross_process_handle_does_not_create_artifact_root() {
        let temp = TempDir::new().expect("temp dir");
        let root = temp.path().join("not-created");
        let server = LogServer {
            fe_log: "ignored".to_string(),
            be_logs: Vec::new(),
        };

        assert!(
            persist_cross_process_failure_logs(&server, &context(&root))
                .expect("skip unsupported handle")
                .is_none()
        );
        assert!(!root.exists());
    }

    #[test]
    fn partial_artifact_cleanup_never_removes_the_configured_root() {
        let temp = TempDir::new().expect("temp dir");
        let sentinel = temp.path().join("keep.txt");
        fs::write(&sentinel, "keep").unwrap();
        let leaf = {
            let pending = PendingArtifactDir::create(temp.path(), "sql-failure-test").unwrap();
            pending.write_file("fe.log", "partial").unwrap();
            pending.path.clone()
        };

        assert!(!leaf.exists());
        assert_eq!(fs::read_to_string(sentinel).unwrap(), "keep");
        assert!(temp.path().exists());
    }

    #[test]
    fn artifact_prefix_does_not_embed_unsafe_suite_text() {
        let prefix = artifact_name_prefix(
            "Correctness / Secret",
            &["suite/../../password=do-not-copy".to_string()],
        );
        assert!(!prefix.contains('/'), "{prefix}");
        assert!(!prefix.contains("do-not-copy"), "{prefix}");
        assert!(prefix.len() < 120, "{prefix}");
    }

    #[test]
    fn first_case_snapshot_is_unique_and_terminal_snapshot_is_independent() {
        let temp = TempDir::new().expect("temp dir");
        let recorder = FailureArtifactRecorder::new(context(temp.path()));
        let server: Mutex<Box<dyn ServerHandle>> = Mutex::new(Box::new(LogServer {
            fe_log: "FE at first failure\n".to_string(),
            be_logs: vec!["BE at first failure\n".to_string()],
        }));

        let first = recorder
            .persist_case_failure(&server, "analytic", "case/../../sql=secret", Some(7))
            .expect("persist first case failure")
            .expect("cross-process case snapshot");
        assert!(
            recorder
                .persist_case_failure(&server, "analytic", "case/../../sql=secret", Some(8))
                .expect("deduplicate same case")
                .is_none()
        );
        let terminal = recorder
            .persist_run_failure(&server)
            .expect("persist terminal snapshot")
            .expect("cross-process terminal snapshot");
        assert!(
            recorder
                .persist_run_failure(&server)
                .expect("deduplicate terminal snapshot")
                .is_none()
        );

        let name = first.file_name().unwrap().to_string_lossy();
        assert!(name.contains("step-7"), "{name}");
        assert!(!name.contains("secret"), "{name}");
        assert!(
            terminal
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("terminal")
        );
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 2);
        assert_eq!(
            fs::read_to_string(first.join("fe.log")).unwrap(),
            "FE at first failure\n"
        );
    }

    #[test]
    fn separate_failed_cases_share_one_first_causal_snapshot() {
        let temp = TempDir::new().expect("temp dir");
        let recorder = FailureArtifactRecorder::new(context(temp.path()));
        let server: Mutex<Box<dyn ServerHandle>> = Mutex::new(Box::new(LogServer {
            fe_log: "FE diagnostic\n".to_string(),
            be_logs: vec!["BE diagnostic\n".to_string()],
        }));

        recorder
            .persist_case_failure(&server, "analytic", "case_one", Some(1))
            .unwrap()
            .unwrap();
        assert!(
            recorder
                .persist_case_failure(&server, "analytic", "case_two", Some(1))
                .unwrap()
                .is_none()
        );
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    }

    #[test]
    fn failed_attempt_diagnostics_do_not_overwrite_prior_attempt_or_invent_actual() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("case.sql");
        let expected = temp.path().join("case.result");
        fs::write(&source, "SELECT missing;\n").unwrap();
        fs::write(&expected, "answer\n42\n").unwrap();
        let mut roots = Vec::new();
        for _ in 0..2 {
            let recorder = FailureArtifactRecorder::new(context(&temp.path().join("artifacts")));
            recorder.record_provenance(
                "dataset/ssb".into(),
                serde_json::json!({"publication_identity":"ready-identity"}),
            );
            let path = recorder
                .persist_case_diagnostic(
                    "ssb",
                    "q1.1",
                    &source,
                    Some(&expected),
                    "statement failed",
                )
                .unwrap();
            assert_eq!(
                fs::read_to_string(path.join("case.sql")).unwrap(),
                "SELECT missing;\n"
            );
            assert_eq!(
                fs::read_to_string(path.join("expected.tsv")).unwrap(),
                "answer\n42\n"
            );
            assert!(!path.join("actual.tsv").exists());
            roots.push(path.parent().unwrap().to_path_buf());
        }
        assert_ne!(roots[0], roots[1]);
        assert!(
            fs::read_to_string(roots[0].join("provenance.json"))
                .unwrap()
                .contains("ready-identity")
        );
    }

    #[test]
    fn text_oracle_failure_without_a_golden_keeps_the_complete_actual_privately() {
        let temp = TempDir::new().unwrap();
        let recorder = FailureArtifactRecorder::new(context(temp.path()));
        let definition = "x".repeat(700);
        let execution = crate::types::QueryExecution {
            header: vec!["Name".into(), "Definition".into(), "Manageability".into()],
            rows: vec![vec!["a_mv".into(), definition.clone(), "MANAGEABLE".into()]],
            text_output: format!(
                "Name\tDefinition\tManageability\na_mv\t{definition}\tMANAGEABLE\n"
            ),
            elapsed: std::time::Duration::ZERO,
        };
        let step = crate::types::SqlStep {
            query_number: 3,
            sql: "SHOW MATERIALIZED VIEWS".into(),
            meta: crate::types::QueryMeta {
                result_contains: vec!["UNAVAILABLE".into()],
                ..Default::default()
            },
        };
        let (accepted, reason) = crate::results::verify_text_assertions(&step, &execution);
        assert!(!accepted);
        assert!(reason.contains("UNAVAILABLE"));
        let path = recorder
            .persist_case_actual_result("lnp-3d", "isolation", step.query_number, &execution)
            .unwrap();
        assert_eq!(
            fs::read_to_string(path.join("actual.txt")).unwrap(),
            execution.text_output
        );
        let actual: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(path.join("actual.json")).unwrap()).unwrap();
        assert_eq!(actual["header"][2], "Manageability");
        assert_eq!(actual["rows"][0][1], definition);
        assert_eq!(actual["rows"][0][2], "MANAGEABLE");
        assert!(!path.join("expected.tsv").exists());
        assert!(!path.join("case.result").exists());
        assert!(!crate::results::verify_text_assertions(&step, &execution).0);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o077, 0);
            for name in ["actual.txt", "actual.json"] {
                assert_eq!(
                    fs::metadata(path.join(name)).unwrap().permissions().mode() & 0o077,
                    0
                );
            }
        }
    }

    #[test]
    fn suite_failure_can_claim_the_first_causal_slot() {
        let temp = TempDir::new().expect("temp dir");
        let recorder = FailureArtifactRecorder::new(context(temp.path()));
        let server: Mutex<Box<dyn ServerHandle>> = Mutex::new(Box::new(LogServer {
            fe_log: "FE init failure\n".to_string(),
            be_logs: vec!["BE init failure\n".to_string()],
        }));

        let suite = recorder
            .persist_suite_failure(&server, "analytic", "init-target")
            .unwrap()
            .unwrap();
        assert!(
            suite
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("suite-init-target")
        );
        assert!(
            recorder
                .persist_case_failure(&server, "analytic", "case_one", Some(1))
                .unwrap()
                .is_none()
        );
        assert!(recorder.persist_run_failure(&server).unwrap().is_some());
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 2);
    }

    #[test]
    fn snapshots_retain_only_a_bounded_utf8_log_tail() {
        let temp = TempDir::new().expect("temp dir");
        let prefix = "discarded-prefix-雪".repeat(MAX_PROCESS_LOG_TAIL_BYTES);
        let suffix = "terminal evidence\n";
        let server = LogServer {
            fe_log: format!("{prefix}{suffix}"),
            be_logs: vec![format!("{prefix}{suffix}")],
        };

        let path = persist_cross_process_failure_logs(&server, &context(temp.path()))
            .unwrap()
            .unwrap();
        for name in ["fe.log", "be-000.log"] {
            let retained = fs::read_to_string(path.join(name)).unwrap();
            assert!(retained.len() <= MAX_PROCESS_LOG_TAIL_BYTES);
            assert!(retained.ends_with(suffix), "{name}");
        }
        let manifest = fs::read_to_string(path.join("manifest.txt")).unwrap();
        assert!(
            manifest.contains(&format!(
                "max_process_log_tail_bytes={MAX_PROCESS_LOG_TAIL_BYTES}"
            )),
            "{manifest}"
        );
    }

    #[test]
    fn snapshots_bound_backend_fanout_and_record_omissions() {
        let temp = TempDir::new().expect("temp dir");
        let backend_count = MAX_CAPTURED_BACKEND_LOGS + 3;
        let server = LogServer {
            fe_log: "FE diagnostic\n".to_string(),
            be_logs: (0..backend_count)
                .map(|index| format!("BE {index}\n"))
                .collect(),
        };

        let path = persist_cross_process_failure_logs(&server, &context(temp.path()))
            .unwrap()
            .unwrap();
        assert!(
            path.join(format!("be-{:03}.log", MAX_CAPTURED_BACKEND_LOGS - 1))
                .exists()
        );
        assert!(
            !path
                .join(format!("be-{MAX_CAPTURED_BACKEND_LOGS:03}.log"))
                .exists()
        );
        let manifest = fs::read_to_string(path.join("manifest.txt")).unwrap();
        assert!(
            manifest.contains(&format!("backend_count={backend_count}")),
            "{manifest}"
        );
        assert!(manifest.contains("omitted_backend_logs=3"), "{manifest}");
        assert!(
            manifest.contains(&format!("max_snapshot_bytes={MAX_FAILURE_SNAPSHOT_BYTES}")),
            "{manifest}"
        );
    }

    // Keep these imports exercised as trait objects too: production owns the
    // handle behind Arc<Mutex<Box<dyn ServerHandle>>>.
    #[test]
    fn log_server_is_compatible_with_runner_handle_shape() {
        let _: Arc<Mutex<Box<dyn ServerHandle>>> = Arc::new(Mutex::new(Box::new(LogServer {
            fe_log: String::new(),
            be_logs: vec![String::new()],
        })));
    }
}
