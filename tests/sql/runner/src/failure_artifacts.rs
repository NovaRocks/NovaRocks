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
use std::fmt::Write as FmtWrite;
use std::fs::{self, OpenOptions};
use std::io::Write;
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
}

pub(crate) struct StagedRunFailure {
    captured: CapturedFailureLogs,
}

impl FailureArtifactRecorder {
    pub(crate) fn new(context: FailureArtifactContext) -> Self {
        Self {
            context,
            state: Mutex::new(CaptureState::default()),
        }
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
        let Some(staged) = staged else {
            return Ok(None);
        };
        if !self.claim(SnapshotSlot::Terminal) {
            return Ok(None);
        }
        let prefix = format!(
            "{}-terminal",
            artifact_name_prefix(&self.context.lane, &self.context.suites)
        );
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
                    .map(|captured| {
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
