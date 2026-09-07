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

use crate::cluster::ServerHandle;
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::fmt::Write as FmtWrite;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static ARTIFACT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const MAX_FAILURE_SNAPSHOTS: usize = 2;
const MAX_CAPTURED_BACKEND_LOGS: usize = 31;
const MAX_PROCESS_LOG_TAIL_BYTES: usize = 128 * 1024;

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

    fn persist_claimed(
        &self,
        server_handle: &Mutex<Box<dyn ServerHandle>>,
        slot: SnapshotSlot,
        prefix: String,
    ) -> Result<Option<PathBuf>> {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let claimed = match slot {
                SnapshotSlot::FirstCausal => &mut state.first_causal_claimed,
                SnapshotSlot::Terminal => &mut state.terminal_claimed,
            };
            if *claimed {
                return Ok(None);
            }
            *claimed = true;
        }

        // The server lock protects only bounded in-memory capture. Filesystem
        // creation, writes, and syncs happen after the global handle is free.
        let captured = match server_handle.lock() {
            Ok(server) => capture_cross_process_failure_logs(server.as_ref(), slot),
            Err(_) => Err(anyhow::anyhow!(
                "server handle lock poisoned while capturing failure artifacts"
            )),
        };
        let result = captured.and_then(|captured| {
            captured
                .map(|captured| {
                    persist_captured_failure_logs(&self.context.root, &prefix, captured)
                })
                .transpose()
        });
        if !matches!(result, Ok(Some(_))) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match slot {
                SnapshotSlot::FirstCausal => state.first_causal_claimed = false,
                SnapshotSlot::Terminal => state.terminal_claimed = false,
            }
        }
        result
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

impl PendingArtifactDir {
    fn create(root: &Path, name_prefix: &str) -> Result<Self> {
        fs::create_dir_all(root)
            .with_context(|| format!("create SQL failure artifact root {}", root.display()))?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_nanos();
        let pid = std::process::id();

        for _ in 0..64 {
            let sequence = ARTIFACT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = root.join(format!("{name_prefix}-{timestamp:020}-{pid}-{sequence:04}"));
            match fs::create_dir(&path) {
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
        let path = self.path.join(name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
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

fn capture_cross_process_failure_logs(
    server: &dyn ServerHandle,
    slot: SnapshotSlot,
) -> Result<Option<CapturedFailureLogs>> {
    let be_count = server.be_count();
    if be_count == 0 {
        return Ok(None);
    }

    let fe_log = server
        .fe_log_contents()
        .context("capture cross-process FE log for failed SQL run")?;
    let mut logs = Vec::with_capacity(be_count.min(MAX_CAPTURED_BACKEND_LOGS) + 1);
    logs.push(bounded_process_log("fe.log".to_string(), fe_log));
    for index in 0..be_count.min(MAX_CAPTURED_BACKEND_LOGS) {
        let log = server
            .be_log_contents(index)
            .with_context(|| format!("capture cross-process BE[{index}] log for failed SQL run"))?;
        logs.push(bounded_process_log(format!("be-{index:03}.log"), log));
    }

    Ok(Some(CapturedFailureLogs {
        slot,
        backend_count: be_count,
        logs,
    }))
}

fn bounded_process_log(name: String, contents: String) -> CapturedProcessLog {
    let original_bytes = contents.len();
    let contents = if original_bytes <= MAX_PROCESS_LOG_TAIL_BYTES {
        contents
    } else {
        let mut start = original_bytes - MAX_PROCESS_LOG_TAIL_BYTES;
        while !contents.is_char_boundary(start) {
            start += 1;
        }
        contents[start..].to_string()
    };
    CapturedProcessLog {
        name,
        original_bytes,
        contents,
    }
}

fn persist_captured_failure_logs(
    root: &Path,
    name_prefix: &str,
    captured: CapturedFailureLogs,
) -> Result<PathBuf> {
    let pending = PendingArtifactDir::create(root, name_prefix)?;
    let mut manifest = format!(
        "schema_version=1\nslot={}\nmax_snapshots={}\nmax_backend_logs={}\nmax_process_log_tail_bytes={}\nbackend_count={}\ncaptured_backend_logs={}\nomitted_backend_logs={}\n",
        captured.slot.label(),
        MAX_FAILURE_SNAPSHOTS,
        MAX_CAPTURED_BACKEND_LOGS,
        MAX_PROCESS_LOG_TAIL_BYTES,
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

        fn fe_log_contents(&self) -> Result<String> {
            Ok(self.fe_log.clone())
        }

        fn be_log_contents(&self, index: usize) -> Result<String> {
            self.be_logs
                .get(index)
                .cloned()
                .with_context(|| format!("missing test BE log {index}"))
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
