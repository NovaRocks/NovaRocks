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

//! File-backed output and caller controls for an independently owned payload.
#[path = "captured_command.rs"]
mod captured_command;
use crate::{
    ManagedCommand,
    monitor_client::{Final, Material, OwnedMonitor, PayloadLifetimeExpired},
    unix,
};
use anyhow::{Context, Result, bail, ensure};
pub use captured_command::{CapturedCommandOutcome, capture_command_until};
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
const LOG_TAIL_BYTES: usize = 8192;
const FILE_SNAPSHOT_ATTEMPTS: usize = 3;
const TURN_BYTES: usize = 65536;
pub enum ReadyMarker {
    StdoutContains(String),
    FileContains { path: PathBuf, needle: String },
}

#[derive(Debug)]
enum ReadinessBaseline {
    Stdout,
    File {
        snapshot: Option<FileReadinessSnapshot>,
    },
}

#[derive(Debug)]
struct FileReadinessSnapshot {
    bytes: Vec<u8>,
    #[allow(dead_code)] // Used by Unix generation-regression tests.
    modified: Option<SystemTime>,
    generation: FileGeneration,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileGeneration {
    device: u64,
    inode: u64,
    change_seconds: i64,
    change_nanoseconds: i64,
    length: u64,
}

#[cfg(not(unix))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileGeneration;

impl FileGeneration {
    #[cfg(unix)]
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            change_seconds: metadata.ctime(),
            change_nanoseconds: metadata.ctime_nsec(),
            length: metadata.len(),
        }
    }

    #[cfg(not(unix))]
    fn from_metadata(_metadata: &fs::Metadata) -> Self {
        Self
    }

    #[cfg(unix)]
    fn same_file_as(&self, other: &Self) -> bool {
        self.device == other.device && self.inode == other.inode
    }

    #[cfg(not(unix))]
    fn same_file_as(&self, _other: &Self) -> bool {
        false
    }
}

impl FileReadinessSnapshot {
    fn read(path: &std::path::Path) -> std::io::Result<Option<Self>> {
        let mut last_unstable = None;
        for _ in 0..FILE_SNAPSHOT_ATTEMPTS {
            match Self::read_once_with_hook(path, || {}) {
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    last_unstable = Some(error);
                    thread::yield_now();
                }
                result => return result,
            }
        }
        Err(last_unstable.unwrap_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "readiness file did not produce a stable snapshot",
            )
        }))
    }

    fn read_once_with_hook(
        path: &std::path::Path,
        after_initial_metadata: impl FnOnce(),
    ) -> std::io::Result<Option<Self>> {
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let initial_metadata = file.metadata()?;
        let initial_generation = FileGeneration::from_metadata(&initial_metadata);
        after_initial_metadata();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let final_metadata = file.metadata()?;
        let final_generation = FileGeneration::from_metadata(&final_metadata);
        if initial_generation != final_generation || final_metadata.len() != bytes.len() as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                format!(
                    "readiness file changed while reading: initial={initial_generation:?} final={final_generation:?} bytes_read={}",
                    bytes.len(),
                ),
            ));
        }
        let modified = final_metadata.modified().ok();
        Ok(Some(Self {
            bytes,
            modified,
            generation: final_generation,
        }))
    }
}

impl ReadinessBaseline {
    fn capture(marker: &ReadyMarker) -> Result<Self> {
        let ReadyMarker::FileContains { path, .. } = marker else {
            return Ok(Self::Stdout);
        };
        #[cfg(not(unix))]
        bail!(
            "FileContains readiness requires Unix file generation metadata; path={}",
            path.display()
        );
        let snapshot = FileReadinessSnapshot::read(path)
            .with_context(|| format!("capture readiness file baseline {}", path.display()))?;
        Ok(Self::File { snapshot })
    }

    fn file_contains_fresh_marker(&self, current: &FileReadinessSnapshot, needle: &str) -> bool {
        let Self::File { snapshot: baseline } = self else {
            return false;
        };
        let needle = needle.as_bytes();
        match baseline {
            None => bytes_contain(current.bytes.as_slice(), needle),
            Some(baseline) if current.generation == baseline.generation => {
                if current.bytes == baseline.bytes {
                    return false;
                }
                bytes_contain(current.bytes.as_slice(), needle)
            }
            Some(baseline)
                if current.generation.same_file_as(&baseline.generation)
                    && current.bytes.len() > baseline.bytes.len()
                    && current.bytes.starts_with(&baseline.bytes) =>
            {
                let overlap = needle.len().saturating_sub(1).min(baseline.bytes.len());
                let overlap_start = baseline.bytes.len() - overlap;
                let scan = &current.bytes[overlap_start..];
                let suffix_boundary = baseline.bytes.len() - overlap_start;
                bytes_match_ending_after(scan, needle, suffix_boundary)
            }
            // A replacement inode or an in-place rewrite is a new generation,
            // so scan the full contents even when it preserves bytes or mtime.
            Some(_) => bytes_contain(current.bytes.as_slice(), needle),
        }
    }
}

fn bytes_contain(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn bytes_match_ending_after(haystack: &[u8], needle: &[u8], boundary: usize) -> bool {
    if needle.is_empty() {
        return false;
    }
    haystack
        .windows(needle.len())
        .enumerate()
        .any(|(start, window)| window == needle && start + needle.len() > boundary)
}

struct OriginalLog {
    file: File,
    path: PathBuf,
    generation: FileGeneration,
    _material: Arc<Material>,
}

impl OriginalLog {
    fn open(material: &Arc<Material>, name: &str, file: File) -> Result<Arc<Self>> {
        let path = material.path.join(name);
        // This is a clone of the file created before payload spawn. A pathname
        // replacement can never become the new output authority.
        let generation = FileGeneration::from_metadata(&file.metadata()?);
        Ok(Arc::new(Self {
            file,
            path,
            generation,
            _material: Arc::clone(material),
        }))
    }
    fn verify(&self, minimum: u64) -> Result<u64> {
        let open = self.file.metadata()?;
        let path =
            fs::symlink_metadata(&self.path).context("inspect original process output path")?;
        ensure!(
            open.is_file()
                && path.is_file()
                && self
                    .generation
                    .same_file_as(&FileGeneration::from_metadata(&open))
                && self
                    .generation
                    .same_file_as(&FileGeneration::from_metadata(&path))
                && open.len() >= minimum
                && path.len() >= minimum,
            "original process durable log was replaced or truncated: {}",
            self.path.display()
        );
        Ok(open.len())
    }
    fn delta(&self, cursor: &mut u64) -> Result<Vec<u8>> {
        let length = self.verify(*cursor)?;
        let count = usize::try_from((length - *cursor).min(TURN_BYTES as u64))?;
        let mut bytes = vec![0; count];
        let mut read = 0;
        while read < count {
            let n = self
                .file
                .read_at(&mut bytes[read..], *cursor + read as u64)?;
            ensure!(
                n != 0,
                "original process log truncated during incremental read"
            );
            read += n;
        }
        self.verify(*cursor + read as u64)?;
        *cursor += read as u64;
        Ok(bytes)
    }
}

/// A pin on one output source. Clones keep ordinary material alive across stop.
#[derive(Clone)]
pub struct ManagedProcessLogSource(Arc<OriginalLog>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedProcessLogTail {
    pub original_bytes: usize,
    pub bytes: Vec<u8>,
}

impl ManagedProcessLogSource {
    pub fn path(&self) -> &Path {
        &self.0.path
    }
    pub fn original_durable_log_identity(&self) -> Result<(u64, u64)> {
        // A frozen scalar fact grants no read authority; do not resample a
        // potentially replaced path just to identify the original writer.
        Ok((self.0.generation.device, self.0.generation.inode))
    }
    pub fn with_bounded_snapshot_reader<T>(
        &self,
        max_bytes: u64,
        visitor: impl FnOnce(&mut dyn Read, u64) -> Result<T>,
    ) -> Result<T> {
        let length = self.0.verify(0)?;
        ensure!(
            length <= max_bytes,
            "original process durable log exceeds snapshot bound"
        );
        let file = File::open(&self.0.path)?;
        ensure!(
            self.0
                .generation
                .same_file_as(&FileGeneration::from_metadata(&file.metadata()?)),
            "original process durable log path was replaced"
        );
        let mut reader = file.take(length);
        let value = visitor(&mut reader, length);
        let identity = self.0.verify(length);
        match (value, identity) {
            (Ok(value), Ok(_)) => Ok(value),
            (Err(primary), Ok(_)) => Err(primary),
            (Ok(_), Err(secondary)) => Err(secondary),
            (Err(primary), Err(secondary)) => Err(primary.context(format!(
                "original output verification also failed: {secondary:#}"
            ))),
        }
    }
    pub fn read_tail(&self, max_bytes: usize) -> Result<ManagedProcessLogTail> {
        let length = self.0.verify(0)?;
        let count = length.min(max_bytes as u64) as usize;
        let mut bytes = vec![0; count];
        let mut offset = 0;
        while offset < count {
            let read = self
                .0
                .file
                .read_at(&mut bytes[offset..], length - count as u64 + offset as u64)?;
            ensure!(
                read != 0,
                "original durable log truncated while reading tail"
            );
            offset += read;
        }
        self.0.verify(length)?;
        Ok(ManagedProcessLogTail {
            original_bytes: usize::try_from(length).unwrap_or(usize::MAX),
            bytes,
        })
    }
}

#[derive(Default)]
struct LineCursor {
    offset: u64,
    partial: Vec<u8>,
}

pub struct ManagedProcess {
    label: String,
    owner: OwnedMonitor,
    stdout: ManagedProcessLogSource,
    stderr: ManagedProcessLogSource,
    live_stdout: Mutex<LineCursor>,
    live_stderr: Mutex<LineCursor>,
    settled: bool,
}

impl std::fmt::Debug for ManagedProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedProcess")
            .field("label", &self.label)
            .field("pid", &self.owner.pid)
            .field("diagnostics", &self.owner.material.path)
            .finish_non_exhaustive()
    }
}

impl ManagedProcess {
    /// Exact observation contracts use stdout. Stderr remains independently
    /// available; diagnostics never pretend the two streams have a total order.
    pub fn log_source(&self) -> ManagedProcessLogSource {
        self.stdout_log_source()
    }
    pub fn stdout_log_source(&self) -> ManagedProcessLogSource {
        self.stdout.clone()
    }
    pub fn stderr_log_source(&self) -> ManagedProcessLogSource {
        self.stderr.clone()
    }
    pub fn diagnostic_root(&self) -> &Path {
        &self.owner.material.path
    }
    pub fn pid(&self) -> u32 {
        self.owner.pid
    }
    pub fn is_running(&self) -> Result<bool> {
        self.owner
            .is_running_until(Instant::now() + Duration::from_secs(1))
    }

    /// The hint is recorded as a locator for callers retaining a role-log name.
    /// Actual output always lives under the run-owned diagnostics directory.
    pub fn spawn(
        label: String,
        command: ManagedCommand,
        marker: ReadyMarker,
        timeout: Duration,
        log_hint: PathBuf,
    ) -> Result<Self> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .context("process startup deadline overflow")?;
        Self::spawn_until(label, command, marker, deadline, log_hint, false)
    }

    fn spawn_until(
        label: String,
        command: ManagedCommand,
        marker: ReadyMarker,
        deadline: Instant,
        log_hint: PathBuf,
        one_shot: bool,
    ) -> Result<Self> {
        let baseline = ReadinessBaseline::capture(&marker)?;
        let owner = OwnedMonitor::spawn(&label, command, deadline)?;
        let stdout = ManagedProcessLogSource(OriginalLog::open(
            &owner.material,
            "stdout.log",
            owner.stdout.try_clone()?,
        )?);
        let stderr = ManagedProcessLogSource(OriginalLog::open(
            &owner.material,
            "stderr.log",
            owner.stderr.try_clone()?,
        )?);
        let mut process = Self {
            label,
            owner,
            stdout,
            stderr,
            live_stdout: Mutex::new(LineCursor::default()),
            live_stderr: Mutex::new(LineCursor::default()),
            settled: false,
        };
        // A locator is not an output file and grants no cleanup ownership.
        if let Some(parent) = log_hint.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            &log_hint,
            format!(
                "stdout={}\nstderr={}\nrecord={}\n",
                process.stdout.path().display(),
                process.stderr.path().display(),
                process.diagnostic_root().join("record.jsonl").display()
            ),
        )?;
        let mut cursor = 0;
        let mut suffix = Vec::new();
        let result = (|| -> Result<()> {
            loop {
                ensure!(
                    Instant::now() < deadline,
                    "{} original readiness deadline expired; diagnostics={}",
                    process.label,
                    process.diagnostic_root().display()
                );
                let found = match &marker {
                    ReadyMarker::StdoutContains(needle) => {
                        ensure!(
                            !needle.is_empty(),
                            "stdout readiness marker must not be empty"
                        );
                        let bytes = process.stdout.0.delta(&mut cursor)?;
                        suffix.extend_from_slice(&bytes);
                        let found = bytes_contain(&suffix, needle.as_bytes());
                        let keep = needle.len().saturating_sub(1).min(suffix.len());
                        suffix.drain(..suffix.len() - keep);
                        found
                    }
                    ReadyMarker::FileContains { path, needle } => {
                        FileReadinessSnapshot::read(path)?.is_some_and(|snapshot| {
                            baseline.file_contains_fresh_marker(&snapshot, needle)
                        })
                    }
                };
                ensure!(
                    Instant::now() < deadline,
                    "original readiness deadline expired during observation"
                );
                if found {
                    ensure!(
                        one_shot || process.owner.is_running_until(deadline)?,
                        "{} exited before readiness; stdout_tail={:?} stderr_tail={:?}",
                        process.label,
                        process.stdout_tail(),
                        process.stderr_tail()
                    );
                    return Ok(());
                }
                ensure!(
                    process.owner.is_running_until(deadline)?,
                    "{} exited before readiness; stdout_tail={:?} stderr_tail={:?}",
                    process.label,
                    process.stdout_tail(),
                    process.stderr_tail()
                );
                thread::sleep(unix::POLL.min(deadline.saturating_duration_since(Instant::now())));
            }
        })();
        if let Err(primary) = result {
            return match process.stop() {
                Ok(()) => Err(primary),
                Err(cleanup) => {
                    Err(primary.context(format!("startup cleanup also failed: {cleanup:#}")))
                }
            };
        }
        Ok(process)
    }

    pub fn run_to_completion(
        label: String,
        command: ManagedCommand,
        marker: ReadyMarker,
        timeout: Duration,
        log_hint: PathBuf,
    ) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .context("one-shot deadline overflow")?;
        let mut process = Self::spawn_until(label, command, marker, deadline, log_hint, true)?;
        let final_record = process.wait_until(deadline);
        match final_record {
            Ok(final_record) => {
                ensure!(
                    final_record.status.success(),
                    "one-shot process did not exit successfully: {}",
                    final_record.status
                );
                Ok(())
            }
            Err(error) => {
                let cleanup = process.stop();
                Err(error.context(format!("one-shot cleanup: {cleanup:?}")))
            }
        }
    }

    fn wait_until(&mut self, deadline: Instant) -> Result<Final> {
        let result = self.owner.wait(deadline);
        // This typed error follows actual monitor exit and a clean Final. The
        // work failure remains an error even though cleanup has settled.
        if result.is_ok()
            || result
                .as_ref()
                .is_err_and(|error| error.is::<PayloadLifetimeExpired>())
        {
            self.settled = true;
        }
        result
    }

    /// True only after the monitor was actually waited and its clean Final
    /// was validated. Work failure and the original exit status are separate.
    pub fn cleanup_settled(&self) -> bool {
        self.settled
    }

    pub fn stop(&mut self) -> Result<()> {
        if self.settled {
            return Ok(());
        }
        self.owner.send(b'S')?;
        self.wait_until(Instant::now() + Duration::from_secs(10))?;
        Ok(())
    }
    pub fn kill_now(&mut self) -> Result<()> {
        if self.settled {
            return Ok(());
        }
        self.owner.send(b'K')?;
        self.wait_until(Instant::now() + Duration::from_secs(10))?;
        Ok(())
    }
    pub fn request_termination(&mut self) -> Result<()> {
        ensure!(
            !self.settled,
            "cannot request SIGTERM for {} after final stop",
            self.label
        );
        self.owner.send(b'T')
    }
    pub fn wait_for_successful_exit_until(&mut self, deadline: Instant) -> Result<ExitStatus> {
        ensure!(
            Instant::now() < deadline,
            "original successful-exit deadline expired"
        );
        let final_record = self.wait_until(deadline)?;
        ensure!(
            final_record.status.success(),
            "{} did not exit successfully after SIGTERM: {}",
            self.label,
            final_record.status
        );
        Ok(final_record.status)
    }
    pub fn interrupt_and_wait(&mut self, timeout: Duration) -> Result<ExitStatus> {
        ensure!(
            !self.settled,
            "cannot interrupt {} after final stop",
            self.label
        );
        let deadline = Instant::now()
            .checked_add(timeout)
            .context("interrupt deadline overflow")?;
        self.owner.send_until(b'I', deadline)?;
        let final_record = match self.wait_until(deadline) {
            Ok(record) => record,
            Err(primary) => {
                let cleanup = self.kill_now();
                return Err(primary.context(format!("interrupt timeout cleanup: {cleanup:?}")));
            }
        };
        ensure!(
            final_record.status.success(),
            "{} did not exit successfully after SIGINT: {}",
            self.label,
            final_record.status
        );
        Ok(final_record.status)
    }
    pub fn restart(
        &mut self,
        command: ManagedCommand,
        marker: ReadyMarker,
        timeout: Duration,
        log_hint: PathBuf,
    ) -> Result<()> {
        self.kill_now()?;
        let replacement = Self::spawn(self.label.clone(), command, marker, timeout, log_hint)?;
        *self = replacement;
        Ok(())
    }

    /// Call only after consuming the lifecycle result and all needed assertions.
    /// Clone log handles delay deletion; formal evidence is never released here.
    pub fn release_diagnostics(&self) -> Result<()> {
        ensure!(self.settled, "cannot release unsettled process diagnostics");
        self.owner.material.release();
        Ok(())
    }
    pub fn stdout_tail(&self) -> String {
        self.stdout
            .read_tail(LOG_TAIL_BYTES)
            .map(|v| String::from_utf8_lossy(&v.bytes).into_owned())
            .unwrap_or_else(|e| format!("output read failed: {e:#}"))
    }
    pub fn stderr_tail(&self) -> String {
        self.stderr
            .read_tail(LOG_TAIL_BYTES)
            .map(|v| String::from_utf8_lossy(&v.bytes).into_owned())
            .unwrap_or_else(|e| format!("output read failed: {e:#}"))
    }
    pub fn log_contents(&self) -> Result<String> {
        let read = |source: &ManagedProcessLogSource| {
            source.with_bounded_snapshot_reader(u64::MAX, |reader, _| {
                let mut text = String::new();
                reader.read_to_string(&mut text)?;
                Ok(text)
            })
        };
        Ok(format!(
            "[stdout]\n{}\n[stderr]\n{}",
            read(&self.stdout)?,
            read(&self.stderr)?
        ))
    }
    pub fn log_count(&self, needle: &str) -> Result<usize> {
        ensure!(!needle.is_empty(), "log count pattern must not be empty");
        let count = |source: &ManagedProcessLogSource| {
            source.with_bounded_snapshot_reader(u64::MAX, |reader, _| {
                let mut bytes = vec![];
                reader.read_to_end(&mut bytes)?;
                let mut count = 0;
                let mut offset = 0;
                while offset + needle.len() <= bytes.len() {
                    if bytes[offset..].starts_with(needle.as_bytes()) {
                        count += 1;
                        offset += needle.len();
                    } else {
                        offset += 1;
                    }
                }
                Ok(count)
            })
        };
        Ok(count(&self.stdout)? + count(&self.stderr)?)
    }
    pub fn assert_log_contains(&self, needle: &str) -> Result<()> {
        ensure!(
            self.log_count(needle)? > 0,
            "{} output lacks {needle:?}; stdout_tail={:?} stderr_tail={:?}",
            self.label,
            self.stdout_tail(),
            self.stderr_tail()
        );
        Ok(())
    }
    pub fn live_log_line(&self, matches: &dyn Fn(&str) -> bool) -> Result<Option<String>> {
        ensure!(
            self.is_running()?,
            "{} exited before the durable log observation",
            self.label
        );
        let mut found = None;
        for (source, cursor) in [
            (&self.stdout, &self.live_stdout),
            (&self.stderr, &self.live_stderr),
        ] {
            let mut cursor = cursor
                .lock()
                .map_err(|_| anyhow::anyhow!("live output cursor lock poisoned"))?;
            let bytes = source.0.delta(&mut cursor.offset)?;
            cursor.partial.extend(bytes);
            // This API returns whole diagnostic lines to a predicate. Bound
            // that one value without changing full raw capture or stdout readiness.
            ensure!(
                cursor.partial.len() <= 1024 * 1024,
                "complete diagnostic line exceeds the 1 MiB observation bound; original log retained"
            );
            let complete = cursor
                .partial
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map(|i| i + 1)
                .unwrap_or(0);
            for line in cursor.partial[..complete].split_inclusive(|byte| *byte == b'\n') {
                let line = std::str::from_utf8(line)
                    .context("complete durable process log line is not UTF-8")?;
                if found.is_none() && matches(line) {
                    found = Some(line.to_owned());
                }
            }
            cursor.partial.drain(..complete);
        }
        ensure!(
            self.is_running()?,
            "{} exited during the durable log observation",
            self.label
        );
        Ok(found)
    }
    pub fn wait_for_log_contains(&mut self, needle: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .context("log wait deadline overflow")?;
        ensure!(
            !needle.is_empty(),
            "durable log wait pattern must not be empty"
        );
        let mut cursors = [0u64; 2];
        let mut suffixes = [Vec::new(), Vec::new()];
        loop {
            if Instant::now() >= deadline {
                let cleanup = self.kill_now();
                bail!("durable log marker deadline expired; actual cleanup={cleanup:?}");
            }
            ensure!(
                self.is_running()?,
                "process exited waiting for durable log marker"
            );
            for (index, source) in [&self.stdout, &self.stderr].iter().enumerate() {
                suffixes[index].extend(source.0.delta(&mut cursors[index])?);
                if bytes_contain(&suffixes[index], needle.as_bytes()) {
                    ensure!(
                        Instant::now() < deadline,
                        "original log marker deadline expired during observation"
                    );
                    return Ok(());
                }
                let keep = needle.len().saturating_sub(1).min(suffixes[index].len());
                let discard = suffixes[index].len() - keep;
                suffixes[index].drain(..discard);
            }
            thread::sleep(unix::POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
    }
    pub fn runtime_diagnostic(
        &mut self,
        label: &str,
        endpoint: &str,
        config_path: &Path,
    ) -> Result<String> {
        if !self.is_running()? {
            let status = self
                .owner
                .final_record()?
                .map(|record| match record.status.code() {
                    Some(code) => format!("exit code {code}"),
                    None => format!("signal {}", record.status.signal().unwrap_or(0)),
                })
                .or(self.owner.observed_exit_description()?)
                .unwrap_or_else(|| "exit observed; final wait pending".into());
            bail!(
                "{label} exited status={status} pid={} endpoint={endpoint} config={} stdout_tail={:?} stderr_tail={:?}",
                self.pid(),
                config_path.display(),
                self.stdout_tail(),
                self.stderr_tail()
            );
        }
        Ok(format!(
            "{label}=running pid={} endpoint={endpoint} config={} stdout_tail={:?} stderr_tail={:?}",
            self.pid(),
            config_path.display(),
            self.stdout_tail(),
            self.stderr_tail()
        ))
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        if !self.settled
            && let Err(error) = self.stop()
        {
            self.owner.close_control();
            let _ = fs::write(
                self.diagnostic_root().join("caller-drop-error.txt"),
                format!("managed process drop cleanup failed: {error:#}\n"),
            );
        }
    }
}

#[cfg(test)]
mod lifetime_tests {
    use super::*;

    struct Directory(PathBuf);

    impl Directory {
        fn new() -> Self {
            static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "novarocks-managed-lifetime-{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn role_lifetime_outlives_startup_and_observes_the_original_group() {
        let directory = Directory::new();
        // These caller-supplied lifetime clocks are inputs to this owner test.
        // Actual run identity and budget inheritance are checked at the caller.
        // Readiness does not replace either lifetime clock.
        let admitted = Instant::now();
        let original_work = admitted + Duration::from_secs(8);
        let original_complete = admitted + Duration::from_secs(10);
        let startup = Duration::from_secs(2);
        let child_pid = directory.0.join("same-group-child.pid");
        let mut request = ManagedCommand::new("/bin/sh");
        request
            .args([
                "-c",
                "trap 'exit 0' TERM; sleep 30 & printf '%s' $! > \"$CHILD_PID\"; printf 'READY\\n'; while :; do sleep 1; done",
            ])
            .env("CHILD_PID", &child_pid)
            .terminate_at(original_work, original_complete);
        let mut process = ManagedProcess::spawn(
            "caller-bounded role".into(),
            request,
            ReadyMarker::StdoutContains("READY".into()),
            startup,
            directory.0.join("locator"),
        )
        .unwrap();
        let root = process.diagnostic_root().to_owned();
        let descendant: u32 = fs::read_to_string(child_pid).unwrap().parse().unwrap();
        assert!(descendant > 0 && descendant != process.pid());
        // The actual payload remains live beyond its startup observation.
        // There is no new production work budget or refreshed cleanup clock.
        thread::sleep(startup + Duration::from_millis(50));
        assert!(Instant::now() < original_work);
        assert!(process.owner.is_running_until(original_work).unwrap());
        assert!(!process.cleanup_settled());
        process.request_termination().unwrap();
        // Consume the original private typed Final only after actual monitor
        // wait; neither a marker nor a later PID probe authors group exit.
        let final_fact = process.wait_until(original_complete).unwrap();
        assert!(process.cleanup_settled());
        assert!(final_fact.status.success());
        assert!(final_fact.group_exit_observed);
        assert!(final_fact.cleanup_complete);
        assert!(!final_fact.owner_lost && !final_fact.lifetime_expired);
        assert!(final_fact.errors.is_empty());
        let text = fs::read_to_string(root.join("record.jsonl")).unwrap();
        let pending: serde_json::Value =
            serde_json::from_str(text.lines().next().unwrap()).unwrap();
        let final_record: serde_json::Value = text
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|record| record["event"] == "Final")
            .unwrap();
        assert!(pending["lifetime_ns"].is_array());
        assert!(pending.get("group_exit_deadline_ns").is_none());
        assert!(
            final_record["group_exit_observed_ns"].as_u64().unwrap()
                < pending["lifetime_ns"][1].as_u64().unwrap()
        );
        process.stop().unwrap();
        process.release_diagnostics().unwrap();
    }
}
