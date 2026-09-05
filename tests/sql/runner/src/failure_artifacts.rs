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
use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static ARTIFACT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) struct FailureArtifactContext {
    pub(crate) root: PathBuf,
    pub(crate) lane: String,
    pub(crate) suites: Vec<String>,
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

pub(crate) fn persist_cross_process_failure_logs(
    server: &dyn ServerHandle,
    context: &FailureArtifactContext,
) -> Result<Option<PathBuf>> {
    let be_count = server.be_count();
    if be_count == 0 {
        return Ok(None);
    }

    // Capture all logs before creating any persistent path. A read failure
    // therefore cannot leave a misleading, incomplete artifact directory.
    let fe_log = server
        .fe_log_contents()
        .context("capture cross-process FE log for failed SQL run")?;
    let mut be_logs = Vec::with_capacity(be_count);
    for index in 0..be_count {
        be_logs.push(server.be_log_contents(index).with_context(|| {
            format!("capture cross-process BE[{index}] log for failed SQL run")
        })?);
    }

    let name_prefix = artifact_name_prefix(&context.lane, &context.suites);
    let pending = PendingArtifactDir::create(&context.root, &name_prefix)?;
    pending.write_file("fe.log", &fe_log)?;
    for (index, log) in be_logs.iter().enumerate() {
        pending.write_file(&format!("be-{index:03}.log"), log)?;
    }

    Ok(Some(pending.commit()))
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
    fn failure_artifacts_use_stable_safe_names_and_only_copy_logs() {
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
        assert_eq!(names.len(), 3, "only FE/BE logs may be persisted");
    }

    #[test]
    fn non_cross_process_handle_does_not_create_artifact_root() {
        let temp = TempDir::new().expect("temp dir");
        let root = temp.path().join("not-created");
        let server = LogServer {
            fe_log: "ignored".to_string(),
            be_logs: Vec::new(),
        };

        assert!(persist_cross_process_failure_logs(&server, &context(&root))
            .expect("skip unsupported handle")
            .is_none());
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
