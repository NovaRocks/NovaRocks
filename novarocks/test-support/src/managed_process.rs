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

use anyhow::{Context, Result, bail};
#[cfg(all(
    any(target_os = "macos", target_os = "ios"),
    target_pointer_width = "64"
))]
use std::ffi::c_void;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::process::CommandExt;

const LOG_TAIL_BYTES: usize = 8 * 1024;
const FILE_SNAPSHOT_ATTEMPTS: usize = 3;
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const OUTPUT_JOIN_TIMEOUT: Duration = Duration::from_millis(250);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

fn short_output_join_deadline() -> Instant {
    Instant::now()
        .checked_add(OUTPUT_JOIN_TIMEOUT)
        .unwrap_or_else(Instant::now)
}

#[cfg(test)]
fn wait_siginfo_abi_supported(target_os: &str, target_arch: &str, pointer_width: u8) -> bool {
    match target_os {
        "linux" | "android" => pointer_width == 64 && !matches!(target_arch, "mips" | "mips64"),
        "macos" | "ios" => pointer_width == 64,
        _ => false,
    }
}

#[cfg(unix)]
const SIGTERM: i32 = 15;
#[cfg(unix)]
const SIGINT: i32 = 2;
#[cfg(unix)]
const SIGKILL: i32 = 9;
#[cfg(unix)]
const ESRCH: i32 = 3;
#[cfg(unix)]
const EPERM: i32 = 1;
#[cfg(unix)]
const P_PID: i32 = 1;
#[cfg(unix)]
const WNOHANG: i32 = 0x00000001;
#[cfg(unix)]
const WEXITED: i32 = 0x00000004;
#[cfg(unix)]
const CLD_EXITED: i32 = 1;
#[cfg(unix)]
const CLD_KILLED: i32 = 2;
#[cfg(unix)]
const CLD_DUMPED: i32 = 3;
#[cfg(any(target_os = "linux", target_os = "android"))]
const WNOWAIT: i32 = 0x01000000;
#[cfg(any(target_os = "macos", target_os = "ios"))]
const WNOWAIT: i32 = 0x00000020;

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
compile_error!(
    "ManagedProcess requires verified waitid WNOWAIT ABI constants for this Unix target"
);

#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    any(target_arch = "mips", target_arch = "mips64")
))]
compile_error!("ManagedProcess does not have a verified waitid siginfo_t layout for MIPS");

#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    not(target_pointer_width = "64")
))]
compile_error!(
    "ManagedProcess waitid siginfo_t support is limited to verified 64-bit Linux/Android ABIs; 32-bit and x32 targets are unsupported"
);

#[cfg(all(
    any(target_os = "macos", target_os = "ios"),
    not(target_pointer_width = "64")
))]
compile_error!("ManagedProcess waitid siginfo_t support is limited to verified 64-bit Apple ABIs");

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn send_signal(pid: i32, signal: i32) -> i32;
    fn waitid(idtype: i32, id: u32, info: *mut WaitSiginfo, options: i32) -> i32;
}

// Darwin's siginfo_t layout is declared in <sys/signal.h>. The trailing
// padding covers si_value, si_band, and the reserved words that follow them.
#[cfg(all(
    any(target_os = "macos", target_os = "ios"),
    target_pointer_width = "64"
))]
#[repr(C)]
struct WaitSiginfo {
    signal: i32,
    error: i32,
    code: i32,
    pid: i32,
    uid: u32,
    status: i32,
    address: *mut c_void,
    remaining: [usize; 9],
}

// Linux siginfo_t is 128 bytes. Its union begins at a pointer-aligned offset
// after the three common i32 fields; the SIGCHLD member starts with
// pid/uid/status. MIPS swaps common fields and is rejected above.
#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    not(any(target_arch = "mips", target_arch = "mips64")),
    target_pointer_width = "64"
))]
#[repr(C)]
struct WaitSiginfo {
    signal: i32,
    error: i32,
    code: i32,
    union_alignment: [usize; 0],
    pid: i32,
    uid: u32,
    status: i32,
    remaining: [u8; 100],
}

// Unsupported Unix targets get no guessed C layout. This opaque placeholder
// only keeps name resolution deterministic while the compile_error! above
// rejects the target before any waitid call can be built.
#[cfg(all(
    unix,
    not(any(
        all(
            any(target_os = "macos", target_os = "ios"),
            target_pointer_width = "64"
        ),
        all(
            any(target_os = "linux", target_os = "android"),
            not(any(target_arch = "mips", target_arch = "mips64")),
            target_pointer_width = "64"
        )
    ))
))]
#[repr(C)]
struct WaitSiginfo {
    unsupported: [u8; 0],
}

#[cfg(unix)]
impl WaitSiginfo {
    #[cfg(all(
        any(target_os = "macos", target_os = "ios"),
        target_pointer_width = "64"
    ))]
    fn zeroed() -> Self {
        Self {
            signal: 0,
            error: 0,
            code: 0,
            pid: 0,
            uid: 0,
            status: 0,
            address: std::ptr::null_mut(),
            remaining: [0; 9],
        }
    }

    #[cfg(all(
        any(target_os = "linux", target_os = "android"),
        not(any(target_arch = "mips", target_arch = "mips64")),
        target_pointer_width = "64"
    ))]
    fn zeroed() -> Self {
        Self {
            signal: 0,
            error: 0,
            code: 0,
            union_alignment: [],
            pid: 0,
            uid: 0,
            status: 0,
            remaining: [0; 100],
        }
    }

    #[cfg(all(
        unix,
        not(any(
            all(
                any(target_os = "macos", target_os = "ios"),
                target_pointer_width = "64"
            ),
            all(
                any(target_os = "linux", target_os = "android"),
                not(any(target_arch = "mips", target_arch = "mips64")),
                target_pointer_width = "64"
            )
        ))
    ))]
    fn zeroed() -> Self {
        Self { unsupported: [] }
    }

    #[cfg(any(
        all(
            any(target_os = "macos", target_os = "ios"),
            target_pointer_width = "64"
        ),
        all(
            any(target_os = "linux", target_os = "android"),
            not(any(target_arch = "mips", target_arch = "mips64")),
            target_pointer_width = "64"
        )
    ))]
    fn observed_exit_status(&self) -> Result<Option<ObservedExitStatus>> {
        if self.signal == 0 {
            return Ok(None);
        }
        match self.code {
            CLD_EXITED => Ok(Some(ObservedExitStatus::ExitCode(self.status))),
            CLD_KILLED | CLD_DUMPED => Ok(Some(ObservedExitStatus::Signal(self.status))),
            code => bail!(
                "waitid returned unexpected SIGCHLD code {code} with status {}",
                self.status
            ),
        }
    }

    #[cfg(all(
        unix,
        not(any(
            all(
                any(target_os = "macos", target_os = "ios"),
                target_pointer_width = "64"
            ),
            all(
                any(target_os = "linux", target_os = "android"),
                not(any(target_arch = "mips", target_arch = "mips64")),
                target_pointer_width = "64"
            )
        ))
    ))]
    fn observed_exit_status(&self) -> Result<Option<ObservedExitStatus>> {
        bail!("waitid siginfo_t is unsupported on this Unix ABI")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservedExitStatus {
    ExitCode(i32),
    Signal(i32),
}

impl std::fmt::Display for ObservedExitStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExitCode(code) => write!(formatter, "exit code {code}"),
            Self::Signal(signal) => write!(formatter, "signal {signal}"),
        }
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessGroupPhase {
    SignalsAllowed,
    FinalSignalSent,
    LeaderReaped,
}

#[cfg(unix)]
#[derive(Debug)]
struct ProcessGroupOwnership {
    id: u32,
    phase: ProcessGroupPhase,
}

#[cfg(unix)]
impl ProcessGroupOwnership {
    fn new(id: u32) -> Self {
        Self {
            id,
            phase: ProcessGroupPhase::SignalsAllowed,
        }
    }

    fn group_id_for_signal(&self) -> Result<u32> {
        if self.phase != ProcessGroupPhase::SignalsAllowed {
            bail!(
                "process group {} no longer permits group-directed signals after the final signal",
                self.id
            );
        }
        Ok(self.id)
    }

    fn record_final_group_signal(&mut self) -> Result<()> {
        if self.phase != ProcessGroupPhase::SignalsAllowed {
            bail!(
                "process group {} final signal was already recorded",
                self.id
            );
        }
        self.phase = ProcessGroupPhase::FinalSignalSent;
        Ok(())
    }

    fn permit_reap(&self) -> Result<()> {
        if self.phase != ProcessGroupPhase::FinalSignalSent {
            bail!(
                "process group {} leader cannot be reaped before the final group signal",
                self.id
            );
        }
        Ok(())
    }

    fn record_reaped(&mut self) -> Result<()> {
        self.permit_reap()?;
        self.phase = ProcessGroupPhase::LeaderReaped;
        Ok(())
    }

    fn record_natural_reap(&mut self) -> Result<()> {
        if self.phase != ProcessGroupPhase::SignalsAllowed {
            bail!(
                "process group {} cannot record a natural reap in phase {:?}",
                self.id,
                self.phase
            );
        }
        self.phase = ProcessGroupPhase::LeaderReaped;
        Ok(())
    }

    fn awaiting_reap(&self) -> bool {
        self.phase == ProcessGroupPhase::FinalSignalSent
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

type SharedLogWriter = Arc<Mutex<Box<dyn Write + Send>>>;
type SharedOutputIoError = Arc<Mutex<Option<String>>>;

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

pub struct ManagedProcess {
    label: String,
    child: Child,
    #[cfg(unix)]
    process_group: ProcessGroupOwnership,
    log_path: PathBuf,
    log_file: SharedLogWriter,
    output_io_error: SharedOutputIoError,
    stdout_buffer: Arc<Mutex<String>>,
    stderr_buffer: Arc<Mutex<String>>,
    stdout_thread: Mutex<Option<thread::JoinHandle<()>>>,
    stderr_thread: Mutex<Option<thread::JoinHandle<()>>>,
    stopped: bool,
}

impl std::fmt::Debug for ManagedProcess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManagedProcess")
            .field("label", &self.label)
            .field("pid", &self.child.id())
            .field("log_path", &self.log_path)
            .finish_non_exhaustive()
    }
}

impl ManagedProcess {
    pub fn run_to_completion(
        label: String,
        command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
    ) -> Result<()> {
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        let mut process = Self::spawn_impl(
            label,
            command,
            marker,
            timeout,
            log_path,
            None,
            false,
            Some(deadline),
        )?;
        process.wait_for_successful_exit(deadline, timeout)
    }

    pub fn spawn(
        label: String,
        command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
    ) -> Result<Self> {
        Self::spawn_impl(label, command, marker, timeout, log_path, None, false, None)
    }

    #[cfg(test)]
    fn spawn_with_log_writer(
        label: String,
        command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
        log_writer: Box<dyn Write + Send>,
    ) -> Result<Self> {
        Self::spawn_impl(
            label,
            command,
            marker,
            timeout,
            log_path,
            Some(log_writer),
            false,
            None,
        )
    }

    #[cfg(test)]
    fn spawn_with_poisoned_stdout_tail(
        label: String,
        command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
    ) -> Result<Self> {
        Self::spawn_impl(label, command, marker, timeout, log_path, None, true, None)
    }

    fn spawn_impl(
        label: String,
        mut command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
        log_writer: Option<Box<dyn Write + Send>>,
        poison_stdout_tail: bool,
        one_shot_deadline: Option<Instant>,
    ) -> Result<Self> {
        let started = Instant::now();
        let deadline =
            one_shot_deadline.unwrap_or_else(|| started.checked_add(timeout).unwrap_or(started));
        let one_shot = one_shot_deadline.is_some();
        let readiness_baseline = ReadinessBaseline::capture(&marker)?;
        let log_file: Box<dyn Write + Send> = match log_writer {
            Some(log_writer) => log_writer,
            None => Box::new(
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&log_path)
                    .with_context(|| format!("open durable process log {}", log_path.display()))?,
            ),
        };
        let log_file = Arc::new(Mutex::new(log_file));
        let output_io_error = Arc::new(Mutex::new(None));

        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);

        let mut child = command
            .spawn()
            .with_context(|| format!("spawn {label}; log={}", log_path.display()))?;
        let stdout = child
            .stdout
            .take()
            .with_context(|| format!("capture {label} stdout"))?;
        let stderr = child
            .stderr
            .take()
            .with_context(|| format!("capture {label} stderr"))?;

        let stdout_buffer = Arc::new(Mutex::new(String::new()));
        let stderr_buffer = Arc::new(Mutex::new(String::new()));
        #[cfg(test)]
        if poison_stdout_tail {
            poison_mutex(Arc::clone(&stdout_buffer));
        }
        #[cfg(not(test))]
        let _ = poison_stdout_tail;
        let (ready_tx, ready_rx) = mpsc::sync_channel::<()>(1);
        let stdout_marker = match &marker {
            ReadyMarker::StdoutContains(needle) => Some(needle.clone()),
            ReadyMarker::FileContains { .. } => None,
        };
        let stdout_thread = spawn_reader(
            "stdout",
            stdout,
            Arc::clone(&stdout_buffer),
            Arc::clone(&log_file),
            Arc::clone(&output_io_error),
            stdout_marker,
            Some(ready_tx),
        );
        let stderr_thread = spawn_reader(
            "stderr",
            stderr,
            Arc::clone(&stderr_buffer),
            Arc::clone(&log_file),
            Arc::clone(&output_io_error),
            None,
            None,
        );

        let mut process = Self {
            label,
            #[cfg(unix)]
            process_group: ProcessGroupOwnership::new(child.id()),
            child,
            log_path,
            log_file,
            output_io_error,
            stdout_buffer,
            stderr_buffer,
            stdout_thread: Mutex::new(Some(stdout_thread)),
            stderr_thread: Mutex::new(Some(stderr_thread)),
            stopped: false,
        };
        if let Err(error) = process.wait_for_ready(
            &marker,
            &readiness_baseline,
            &ready_rx,
            deadline,
            timeout,
            one_shot,
        ) {
            let cleanup = process.kill_now();
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(error.context(format!(
                    "also failed to clean up {}: {cleanup_error:#}",
                    process.label
                ))),
            };
        }
        Ok(process)
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn is_running(&self) -> Result<bool> {
        self.ensure_output_io_ok("inspect process state")?;
        #[cfg(unix)]
        {
            // A deliberate fault kill reaps the direct child immediately. For
            // resource convergence, that exit is the required proof that this
            // BE no longer owns heavy execution resources.
            if self.stopped {
                return Ok(false);
            }
            return Ok(self.leader_exit_status_observed()?.is_none());
        }
        #[cfg(not(unix))]
        {
            Ok(!self.stopped)
        }
    }

    pub fn stdout_tail(&self) -> String {
        read_tail(&self.stdout_buffer, "<stdout lock poisoned>")
    }

    pub fn stderr_tail(&self) -> String {
        read_tail(&self.stderr_buffer, "<stderr lock poisoned>")
    }

    pub fn assert_log_contains(&self, needle: &str) -> Result<()> {
        self.ensure_output_io_ok("assert durable log contents")?;
        let flush_deadline = short_output_join_deadline();
        loop {
            match self.log_file.try_lock() {
                Ok(mut log_file) => {
                    if let Err(error) = log_file.flush() {
                        record_output_io_error(
                            &self.output_io_error,
                            format!(
                                "flush durable process log {}: {error}",
                                self.log_path.display()
                            ),
                        );
                    }
                    break;
                }
                Err(std::sync::TryLockError::WouldBlock) if Instant::now() < flush_deadline => {
                    thread::sleep(
                        flush_deadline
                            .saturating_duration_since(Instant::now())
                            .min(POLL_INTERVAL),
                    );
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    record_output_io_error(
                        &self.output_io_error,
                        format!(
                            "durable process log writer is busy for {}",
                            self.log_path.display()
                        ),
                    );
                    break;
                }
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    record_output_io_error(
                        &self.output_io_error,
                        format!("lock durable process log {}", self.log_path.display()),
                    );
                    break;
                }
            }
        }
        self.ensure_output_io_ok("assert durable log contents")?;
        let log = fs::read_to_string(&self.log_path)
            .with_context(|| format!("read durable process log {}", self.log_path.display()))?;
        if log.contains(needle) {
            return Ok(());
        }
        bail!(
            "{} log {} does not contain {needle:?}; stdout_tail={:?}; stderr_tail={:?}",
            self.label,
            self.log_path.display(),
            self.stdout_tail(),
            self.stderr_tail()
        )
    }

    pub fn log_count(&self, needle: &str) -> Result<usize> {
        if needle.is_empty() {
            bail!("durable log count pattern must not be empty");
        }
        self.ensure_output_io_ok("count durable log contents")?;
        let log = fs::read_to_string(&self.log_path)
            .with_context(|| format!("read durable process log {}", self.log_path.display()))?;
        Ok(log.match_indices(needle).count())
    }

    pub fn log_contents(&self) -> Result<String> {
        self.ensure_output_io_ok("read durable log contents")?;
        fs::read_to_string(&self.log_path)
            .with_context(|| format!("read durable process log {}", self.log_path.display()))
    }

    /// Waits for a marker in the process's combined durable output log.
    ///
    /// This is for post-readiness observations. Startup readiness itself must
    /// use [`ReadyMarker`] when spawning the process, so an old log marker
    /// cannot make a new child appear ready.
    pub fn wait_for_log_contains(&mut self, needle: &str, timeout: Duration) -> Result<()> {
        if needle.is_empty() {
            bail!("durable log wait pattern must not be empty");
        }
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        loop {
            self.ensure_output_io_ok("wait for durable log marker")?;
            if self.log_contents()?.contains(needle) {
                return Ok(());
            }

            #[cfg(unix)]
            if self.leader_exit_observed()? {
                let status = self.finish_group_with_signal(
                    SIGKILL,
                    "wait after exit before durable log marker",
                    short_output_join_deadline(),
                )?;
                bail!(
                    "{} exited with status {status} before durable log marker {needle:?}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }
            #[cfg(not(unix))]
            if let Some(status) = self.child.try_wait()? {
                self.stopped = true;
                self.join_output_threads_until(short_output_join_deadline());
                bail!(
                    "{} exited with status {status} before durable log marker {needle:?}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }

            let now = Instant::now();
            if now >= deadline {
                let message = format!(
                    "{} timed out waiting for durable log marker {needle:?} after {timeout:?}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
                return match self.kill_now() {
                    Ok(()) => Err(anyhow::anyhow!(message)),
                    Err(cleanup_error) => Err(anyhow::anyhow!(message).context(format!(
                        "also failed to clean up {}: {cleanup_error:#}",
                        self.label
                    ))),
                };
            }
            thread::sleep(deadline.saturating_duration_since(now).min(POLL_INTERVAL));
        }
    }

    pub fn restart(
        &mut self,
        command: Command,
        marker: ReadyMarker,
        timeout: Duration,
        log_path: PathBuf,
    ) -> Result<()> {
        self.kill_now()
            .with_context(|| format!("kill {} before restart", self.label))?;
        let replacement = Self::spawn(self.label.clone(), command, marker, timeout, log_path)?;
        *self = replacement;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let started = Instant::now();
        let graceful_deadline = started.checked_add(STOP_TIMEOUT).unwrap_or(started);
        let cleanup_deadline = graceful_deadline
            .checked_add(OUTPUT_JOIN_TIMEOUT)
            .unwrap_or(graceful_deadline);
        if self.stopped {
            self.join_output_threads_until(short_output_join_deadline());
            return self.ensure_output_io_ok("stop process");
        }
        #[cfg(unix)]
        {
            if self.process_group.awaiting_reap() {
                self.reap_after_final_group_signal(
                    "retry wait after final group signal",
                    short_output_join_deadline(),
                )?;
                return self.ensure_output_io_ok("stop process");
            }
            self.signal_group(SIGTERM)?;
            loop {
                if self.leader_exit_observed()? {
                    self.finish_group_with_signal(
                        SIGKILL,
                        "wait for process-group cleanup after SIGTERM",
                        short_output_join_deadline(),
                    )?;
                    return self.ensure_output_io_ok("stop process");
                }
                if Instant::now() >= graceful_deadline {
                    break;
                }
                thread::sleep(
                    graceful_deadline
                        .saturating_duration_since(Instant::now())
                        .min(POLL_INTERVAL),
                );
            }
            self.finish_group_with_signal(SIGKILL, "wait after SIGKILL timeout", cleanup_deadline)?;
            return self.ensure_output_io_ok("stop process");
        }

        #[cfg(not(unix))]
        {
            if self.child.try_wait()?.is_none() {
                self.child.kill()?;
                let _ = self.child.wait()?;
            }
            self.stopped = true;
            self.join_output_threads_until(short_output_join_deadline());
            self.ensure_output_io_ok("stop process")
        }
    }

    pub fn kill_now(&mut self) -> Result<()> {
        let cleanup_deadline = short_output_join_deadline();
        if self.stopped {
            self.join_output_threads_until(cleanup_deadline);
            return self.ensure_output_io_ok("kill process");
        }
        #[cfg(unix)]
        {
            if self.process_group.awaiting_reap() {
                self.reap_after_final_group_signal(
                    "retry wait after final group signal",
                    cleanup_deadline,
                )?;
                return self.ensure_output_io_ok("kill process");
            }
            self.finish_group_with_signal(
                SIGKILL,
                "wait after immediate SIGKILL",
                cleanup_deadline,
            )?;
            return self.ensure_output_io_ok("kill process");
        }
        #[cfg(not(unix))]
        {
            if self.child.try_wait()?.is_none() {
                self.child.kill()?;
            }

            let _ = self
                .child
                .wait()
                .with_context(|| format!("wait for {} after immediate kill", self.label))?;
            self.stopped = true;
            self.join_output_threads_until(cleanup_deadline);
            self.ensure_output_io_ok("kill process")
        }
    }

    /// Sends SIGINT to the child process group and requires a successful exit.
    ///
    /// The group remains owned until its direct child is reaped, and surviving
    /// descendants are forcefully cleaned up after that exit observation.
    #[cfg(unix)]
    pub fn interrupt_and_wait(&mut self, timeout: Duration) -> Result<ExitStatus> {
        if self.stopped {
            bail!("cannot interrupt {} after it was stopped", self.label);
        }
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        self.signal_group(SIGINT)?;
        loop {
            if self.leader_exit_observed()? {
                let status = self.finish_group_with_signal(
                    SIGKILL,
                    "wait for process-group cleanup after SIGINT",
                    short_output_join_deadline(),
                )?;
                self.ensure_output_io_ok("wait for SIGINT process exit")?;
                if status.success() {
                    return Ok(status);
                }
                bail!(
                    "{} did not exit successfully after SIGINT: status={status}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }
            let now = Instant::now();
            if now >= deadline {
                let message = format!(
                    "{} timed out after SIGINT waiting for successful exit after {timeout:?}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
                return match self.kill_now() {
                    Ok(()) => Err(anyhow::anyhow!(message)),
                    Err(cleanup_error) => Err(anyhow::anyhow!(message).context(format!(
                        "also failed to clean up {}: {cleanup_error:#}",
                        self.label
                    ))),
                };
            }
            thread::sleep(deadline.saturating_duration_since(now).min(POLL_INTERVAL));
        }
    }

    fn wait_for_successful_exit(&mut self, deadline: Instant, timeout: Duration) -> Result<()> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                let message = format!(
                    "{} timed out waiting for successful completion after {timeout:?}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
                self.kill_now()
                    .with_context(|| format!("{message}; also failed to kill timed out process"))?;
                bail!("{message}");
            }
            #[cfg(unix)]
            if self.leader_exit_observed()? {
                let status = self.reap_natural_exit(deadline)?;
                self.ensure_output_io_ok("complete one-shot process")?;
                if Instant::now() >= deadline {
                    bail!(
                        "{} timed out waiting for successful completion after {timeout:?}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                        self.label,
                        self.stdout_tail(),
                        self.stderr_tail(),
                        self.log_path.display()
                    );
                }
                if status.success() {
                    return Ok(());
                }
                bail!(
                    "{} exited with status {status}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }
            #[cfg(not(unix))]
            if let Some(status) = self.child.try_wait()? {
                self.stopped = true;
                self.join_output_threads_until(deadline);
                self.ensure_output_io_ok("complete one-shot process")?;
                if Instant::now() >= deadline {
                    bail!(
                        "{} timed out waiting for successful completion after {timeout:?}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                        self.label,
                        self.stdout_tail(),
                        self.stderr_tail(),
                        self.log_path.display()
                    );
                }
                if status.success() {
                    return Ok(());
                }
                bail!(
                    "{} exited with status {status}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }

            thread::sleep(deadline.saturating_duration_since(now).min(POLL_INTERVAL));
        }
    }

    pub fn runtime_diagnostic(
        &mut self,
        label: &str,
        endpoint: &str,
        config_path: &std::path::Path,
    ) -> Result<String> {
        self.ensure_output_io_ok("collect runtime diagnostics")?;
        let pid = self.pid();
        let exit_status = self.runtime_exit_status(label, pid, endpoint)?;
        let stdout_tail = self.stdout_tail();
        let stderr_tail = self.stderr_tail();
        if let Some(exit_status) = exit_status {
            bail!(
                "{label} exited status={exit_status} pid={pid} endpoint={endpoint} config={} stdout_tail={stdout_tail:?} stderr_tail={stderr_tail:?}",
                config_path.display()
            );
        }
        Ok(format!(
            "{label}=running pid={pid} endpoint={endpoint} config={} stdout_tail={stdout_tail:?} stderr_tail={stderr_tail:?}",
            config_path.display()
        ))
    }

    #[cfg(unix)]
    fn runtime_exit_status(
        &self,
        label: &str,
        pid: u32,
        endpoint: &str,
    ) -> Result<Option<ObservedExitStatus>> {
        self.leader_exit_status_observed().with_context(|| {
            format!("inspect {label} pid={pid} endpoint={endpoint} process status")
        })
    }

    #[cfg(not(unix))]
    fn runtime_exit_status(
        &self,
        label: &str,
        pid: u32,
        endpoint: &str,
    ) -> Result<Option<ObservedExitStatus>> {
        unsupported_runtime_exit_status(label, pid, endpoint)
    }

    fn wait_for_ready(
        &mut self,
        marker: &ReadyMarker,
        readiness_baseline: &ReadinessBaseline,
        ready_rx: &mpsc::Receiver<()>,
        deadline: Instant,
        timeout: Duration,
        one_shot: bool,
    ) -> Result<()> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                bail!(
                    "{} timed out waiting for readiness marker after {timeout:?}; stdout_tail={:?}; stderr_tail={}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }
            if matches!(marker, ReadyMarker::StdoutContains(_))
                && matches!(ready_rx.try_recv(), Ok(()))
            {
                self.ensure_output_io_ok("confirm readiness")?;
                return Ok(());
            }
            #[cfg(unix)]
            if self.leader_exit_observed()? {
                if one_shot {
                    self.join_output_threads_until(deadline);
                    self.ensure_output_io_ok("confirm one-shot readiness")?;
                    if Instant::now() >= deadline {
                        bail!(
                            "{} timed out waiting for readiness marker after {timeout:?}; stdout_tail={:?}; stderr_tail={}; log={}",
                            self.label,
                            self.stdout_tail(),
                            self.stderr_tail(),
                            self.log_path.display()
                        );
                    }
                    if self.readiness_marker_observed(marker, readiness_baseline)? {
                        return Ok(());
                    }
                    let status = self.reap_natural_exit(deadline)?;
                    bail!(
                        "{} exited before readiness marker with status {status}; stdout_tail={:?}; stderr_tail={}; log={}",
                        self.label,
                        self.stdout_tail(),
                        self.stderr_tail(),
                        self.log_path.display()
                    );
                }
                let status = self.finish_group_with_signal(
                    SIGKILL,
                    "wait after exit before readiness marker",
                    Instant::now()
                        .checked_add(OUTPUT_JOIN_TIMEOUT)
                        .unwrap_or_else(Instant::now),
                )?;
                bail!(
                    "{} exited before readiness marker with status {status}; stdout_tail={:?}; stderr_tail={}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }
            #[cfg(not(unix))]
            if let Some(status) = self.child.try_wait()? {
                self.join_output_threads_until(
                    Instant::now()
                        .checked_add(OUTPUT_JOIN_TIMEOUT)
                        .unwrap_or_else(Instant::now),
                );
                bail!(
                    "{} exited before readiness marker with status {status}; stdout_tail={:?}; stderr_tail={}; log={}",
                    self.label,
                    self.stdout_tail(),
                    self.stderr_tail(),
                    self.log_path.display()
                );
            }

            match marker {
                ReadyMarker::StdoutContains(_) => match ready_rx.try_recv() {
                    Ok(()) => {
                        self.ensure_output_io_ok("confirm readiness")?;
                        return Ok(());
                    }
                    Err(mpsc::TryRecvError::Empty) => {}
                    Err(mpsc::TryRecvError::Disconnected) => {
                        self.ensure_output_io_ok("confirm readiness")?;
                        #[cfg(unix)]
                        if self.leader_exit_observed()? {
                            let status = self.finish_group_with_signal(
                                SIGKILL,
                                "wait after stdout closed before readiness marker",
                                Instant::now()
                                    .checked_add(OUTPUT_JOIN_TIMEOUT)
                                    .unwrap_or_else(Instant::now),
                            )?;
                            bail!(
                                "{} exited before readiness marker with status {status}; stdout_tail={:?}; stderr_tail={}; log={}",
                                self.label,
                                self.stdout_tail(),
                                self.stderr_tail(),
                                self.log_path.display()
                            );
                        }
                        #[cfg(not(unix))]
                        if let Some(status) = self.child.try_wait()? {
                            self.join_output_threads_until(
                                Instant::now()
                                    .checked_add(OUTPUT_JOIN_TIMEOUT)
                                    .unwrap_or_else(Instant::now),
                            );
                            bail!(
                                "{} exited before readiness marker with status {status}; stdout_tail={:?}; stderr_tail={}; log={}",
                                self.label,
                                self.stdout_tail(),
                                self.stderr_tail(),
                                self.log_path.display()
                            );
                        }
                        bail!(
                            "{} stdout closed before readiness marker while child was still running; stdout_tail={:?}; stderr_tail={}; log={}",
                            self.label,
                            self.stdout_tail(),
                            self.stderr_tail(),
                            self.log_path.display()
                        );
                    }
                },
                ReadyMarker::FileContains { path, needle } => {
                    match FileReadinessSnapshot::read(path) {
                        Ok(Some(snapshot))
                            if readiness_baseline.file_contains_fresh_marker(&snapshot, needle) =>
                        {
                            self.ensure_output_io_ok("confirm readiness")?;
                            return Ok(());
                        }
                        Ok(Some(_)) | Ok(None) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(error) => {
                            return Err(error).with_context(|| {
                                format!("poll readiness file {}", path.display())
                            });
                        }
                    }
                }
            }

            thread::sleep(deadline.saturating_duration_since(now).min(POLL_INTERVAL));
        }
    }

    fn readiness_marker_observed(
        &self,
        marker: &ReadyMarker,
        readiness_baseline: &ReadinessBaseline,
    ) -> Result<bool> {
        match marker {
            ReadyMarker::StdoutContains(needle) => Ok(self.stdout_tail().contains(needle)),
            ReadyMarker::FileContains { path, needle } => match FileReadinessSnapshot::read(path) {
                Ok(Some(snapshot)) => {
                    Ok(readiness_baseline.file_contains_fresh_marker(&snapshot, needle))
                }
                Ok(None) => Ok(false),
                Err(error) => {
                    Err(error).with_context(|| format!("read readiness file {}", path.display()))
                }
            },
        }
    }

    fn join_output_threads_until(&self, deadline: Instant) {
        loop {
            self.harvest_finished_output_threads();
            if !self.has_attached_output_threads() {
                return;
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            thread::sleep(deadline.saturating_duration_since(now).min(POLL_INTERVAL));
        }
        self.harvest_finished_output_threads();
        self.detach_reader_thread(&self.stdout_thread, "stdout");
        self.detach_reader_thread(&self.stderr_thread, "stderr");
    }

    fn harvest_finished_output_threads(&self) {
        self.harvest_reader_thread(&self.stdout_thread, "stdout");
        self.harvest_reader_thread(&self.stderr_thread, "stderr");
    }

    fn has_attached_output_threads(&self) -> bool {
        [&self.stdout_thread, &self.stderr_thread]
            .into_iter()
            .any(|slot| slot.lock().map_or(true, |slot| slot.is_some()))
    }

    fn harvest_reader_thread(
        &self,
        thread_slot: &Mutex<Option<thread::JoinHandle<()>>>,
        stream_name: &str,
    ) {
        let handle = match thread_slot.lock() {
            Ok(mut slot) => {
                let should_join = slot.as_ref().is_some_and(|handle| handle.is_finished());
                should_join.then(|| slot.take()).flatten()
            }
            Err(_) => {
                record_output_io_error(
                    &self.output_io_error,
                    format!("lock {stream_name} reader thread handle"),
                );
                None
            }
        };
        if let Some(handle) = handle
            && let Err(payload) = handle.join()
        {
            record_output_io_error(
                &self.output_io_error,
                format!(
                    "{stream_name} reader thread panicked: {}",
                    panic_payload_message(payload.as_ref())
                ),
            );
        }
    }

    fn detach_reader_thread(
        &self,
        thread_slot: &Mutex<Option<thread::JoinHandle<()>>>,
        stream_name: &str,
    ) {
        let detached = match thread_slot.lock() {
            Ok(mut slot) => slot.take().is_some(),
            Err(_) => {
                record_output_io_error(
                    &self.output_io_error,
                    format!("lock {stream_name} reader thread handle"),
                );
                false
            }
        };
        if detached {
            record_output_io_error(
                &self.output_io_error,
                format!(
                    "timed out waiting for {stream_name} reader thread; detached blocked reader"
                ),
            );
        }
    }

    fn ensure_output_io_ok(&self, operation: &str) -> Result<()> {
        self.harvest_finished_output_threads();
        self.record_tail_lock_errors();
        let error = self
            .output_io_error
            .lock()
            .map_err(|_| anyhow::anyhow!("process output I/O error state lock poisoned"))?
            .clone();
        if let Some(error) = error {
            bail!(
                "{} cannot {operation}: {error}; stdout_tail={:?}; stderr_tail={:?}; log={}",
                self.label,
                self.stdout_tail(),
                self.stderr_tail(),
                self.log_path.display()
            );
        }
        Ok(())
    }

    fn record_tail_lock_errors(&self) {
        if self.stdout_buffer.lock().is_err() {
            record_output_io_error(
                &self.output_io_error,
                "stdout tail lock poisoned".to_string(),
            );
        }
        if self.stderr_buffer.lock().is_err() {
            record_output_io_error(
                &self.output_io_error,
                "stderr tail lock poisoned".to_string(),
            );
        }
    }

    #[cfg(unix)]
    fn signal_group(&self, signal: i32) -> Result<()> {
        let process_group_id = i32::try_from(self.process_group.group_id_for_signal()?)
            .context("managed process group id exceeds i32")?;
        // SAFETY: POSIX kill accepts a negative process-group id. This id was
        // assigned to the child by process_group(0) immediately before spawn.
        if unsafe { send_signal(-process_group_id, signal) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(ESRCH) {
            return Ok(());
        }
        if error.raw_os_error() == Some(EPERM) && self.leader_exit_observed()? {
            // Darwin reports EPERM when a process group contains only the
            // unreaped leader zombie. The WNOWAIT observation proves that the
            // leader PID is still owned and cannot have been reused.
            return Ok(());
        }
        Err(error).with_context(|| {
            format!(
                "send signal {signal} to {} process group {}",
                self.label, process_group_id
            )
        })
    }

    #[cfg(unix)]
    fn leader_exit_observed(&self) -> Result<bool> {
        Ok(self.leader_exit_status_observed()?.is_some())
    }

    #[cfg(unix)]
    fn leader_exit_status_observed(&self) -> Result<Option<ObservedExitStatus>> {
        if self.stopped {
            bail!(
                "cannot observe {} process leader {} after it was reaped",
                self.label,
                self.child.id()
            );
        }

        let mut info = WaitSiginfo::zeroed();
        // SAFETY: WaitSiginfo has target-specific C layout assertions in the
        // focused tests below. WNOWAIT observes the direct child without
        // reaping its process-group leader PID.
        let result = unsafe {
            waitid(
                P_PID,
                self.child.id(),
                &mut info,
                WEXITED | WNOHANG | WNOWAIT,
            )
        };
        if result == 0 {
            return info.observed_exit_status();
        }
        let error = std::io::Error::last_os_error();
        Err(error).with_context(|| {
            format!(
                "observe {} process leader {} without reaping",
                self.label,
                self.child.id()
            )
        })
    }

    #[cfg(unix)]
    fn finish_group_with_signal(
        &mut self,
        signal: i32,
        wait_context: &str,
        cleanup_deadline: Instant,
    ) -> Result<ExitStatus> {
        self.signal_group(signal)?;
        self.process_group.record_final_group_signal()?;
        self.reap_after_final_group_signal(wait_context, cleanup_deadline)
    }

    #[cfg(unix)]
    fn reap_after_final_group_signal(
        &mut self,
        wait_context: &str,
        cleanup_deadline: Instant,
    ) -> Result<ExitStatus> {
        self.process_group.permit_reap()?;
        let status = self
            .child
            .wait()
            .with_context(|| format!("{wait_context} for {}", self.label))?;
        self.process_group.record_reaped()?;
        self.stopped = true;
        self.join_output_threads_until(cleanup_deadline);
        Ok(status)
    }

    #[cfg(unix)]
    fn reap_natural_exit(&mut self, cleanup_deadline: Instant) -> Result<ExitStatus> {
        let status = self
            .child
            .wait()
            .with_context(|| format!("wait for {} natural exit", self.label))?;
        self.process_group.record_natural_reap()?;
        self.stopped = true;
        self.join_output_threads_until(cleanup_deadline);
        Ok(status)
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn spawn_reader<R: Read + Send + 'static>(
    stream_name: &'static str,
    mut reader: R,
    tail: Arc<Mutex<String>>,
    log_file: SharedLogWriter,
    output_io_error: SharedOutputIoError,
    ready_marker: Option<String>,
    ready_tx: Option<mpsc::SyncSender<()>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        run_reader_with_panic_boundary(stream_name, &output_io_error, || {
            let mut buffer = [0_u8; 4096];
            let mut marker_scan = String::new();
            let mut ready_sent = false;
            loop {
                let count = match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => count,
                    Err(error) => {
                        record_output_io_error(
                            &output_io_error,
                            format!("read managed process {stream_name}: {error}"),
                        );
                        break;
                    }
                };
                let chunk = &buffer[..count];
                match tail.lock() {
                    Ok(mut output) => push_bounded_log_chunk(&mut output, chunk, LOG_TAIL_BYTES),
                    Err(_) => record_output_io_error(
                        &output_io_error,
                        format!("{stream_name} tail lock poisoned"),
                    ),
                }
                match log_file.lock() {
                    Ok(mut log) => {
                        let write_succeeded =
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                log.write_all(chunk)
                            })) {
                                Ok(Ok(())) => true,
                                Ok(Err(error)) => {
                                    record_output_io_error(
                                        &output_io_error,
                                        format!(
                                            "write durable process log from {stream_name}: {error}"
                                        ),
                                    );
                                    false
                                }
                                Err(payload) => {
                                    record_reader_panic(
                                        stream_name,
                                        &output_io_error,
                                        payload.as_ref(),
                                    );
                                    return;
                                }
                            };
                        if write_succeeded {
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                log.flush()
                            })) {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => record_output_io_error(
                                    &output_io_error,
                                    format!(
                                        "flush durable process log from {stream_name}: {error}"
                                    ),
                                ),
                                Err(payload) => {
                                    record_reader_panic(
                                        stream_name,
                                        &output_io_error,
                                        payload.as_ref(),
                                    );
                                    return;
                                }
                            }
                        }
                    }
                    Err(_) => record_output_io_error(
                        &output_io_error,
                        format!("lock durable process log for {stream_name}"),
                    ),
                }
                if !ready_sent && let Some(marker) = ready_marker.as_deref() {
                    marker_scan.push_str(&String::from_utf8_lossy(chunk));
                    if marker_scan.contains(marker) {
                        if let Some(ready_tx) = ready_tx.as_ref() {
                            let _ = ready_tx.try_send(());
                        }
                        ready_sent = true;
                        marker_scan.clear();
                    } else {
                        let scan_capacity = LOG_TAIL_BYTES.max(marker.len().saturating_mul(2));
                        truncate_front(&mut marker_scan, scan_capacity);
                    }
                }
            }
        });
    })
}

fn run_reader_with_panic_boundary(
    stream_name: &str,
    output_io_error: &SharedOutputIoError,
    body: impl FnOnce(),
) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        record_reader_panic(stream_name, output_io_error, payload.as_ref());
    }
}

fn record_reader_panic(
    stream_name: &str,
    output_io_error: &SharedOutputIoError,
    payload: &(dyn std::any::Any + Send),
) {
    record_output_io_error(
        output_io_error,
        format!(
            "{stream_name} reader thread panicked: {}",
            panic_payload_message(payload)
        ),
    );
}

fn record_output_io_error(errors: &SharedOutputIoError, error: String) {
    if let Ok(mut first_error) = errors.lock()
        && first_error.is_none()
    {
        *first_error = Some(error);
    }
}

#[allow(dead_code)] // Exercised by the platform capability-contract test.
fn unsupported_runtime_exit_status(
    label: &str,
    pid: u32,
    endpoint: &str,
) -> Result<Option<ObservedExitStatus>> {
    bail!(
        "non-reaping runtime diagnostics are unsupported on this platform for {label} pid={pid} endpoint={endpoint}"
    )
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "non-string panic payload".to_string()
}

#[cfg(test)]
fn poison_mutex<T: Send + 'static>(mutex: Arc<Mutex<T>>) {
    let _ = thread::spawn(move || {
        let _guard = mutex.lock().expect("lock mutex before poisoning");
        panic!("inject managed process mutex poison");
    })
    .join();
}

fn push_bounded_log_chunk(buffer: &mut String, chunk: &[u8], capacity: usize) {
    buffer.push_str(&String::from_utf8_lossy(chunk));
    truncate_front(buffer, capacity);
}

fn truncate_front(buffer: &mut String, capacity: usize) {
    if capacity == 0 {
        buffer.clear();
        return;
    }
    if buffer.len() <= capacity {
        return;
    }
    let mut start = buffer.len() - capacity;
    while start < buffer.len() && !buffer.is_char_boundary(start) {
        start += 1;
    }
    buffer.drain(..start);
}

fn read_tail(buffer: &Arc<Mutex<String>>, poisoned: &str) -> String {
    buffer
        .lock()
        .map(|buffer| buffer.clone())
        .unwrap_or_else(|_| poisoned.to_string())
}

#[cfg(all(test, unix))]
mod tests {
    use super::{
        FileReadinessSnapshot, ManagedProcess, ProcessGroupOwnership, ReadinessBaseline,
        ReadyMarker, WaitSiginfo, run_reader_with_panic_boundary, spawn_reader,
        unsupported_runtime_exit_status, wait_siginfo_abi_supported,
    };
    use std::fs;
    use std::io::{self, Cursor, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "novarocks-managed-process-{label}-{}-{unique}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create managed process test directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn shell(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(script);
        command
    }

    fn shell_with_arg(script: &str, arg: &Path) -> Command {
        let mut command = shell(script);
        command.arg("managed-process-fixture").arg(arg);
        command
    }

    fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_runtime_error(
        process: &mut ManagedProcess,
        config_path: &Path,
        timeout: Duration,
    ) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            match process.runtime_diagnostic("fixture", "local", config_path) {
                Ok(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                Ok(diagnostic) => {
                    panic!("process was still running after {timeout:?}: {diagnostic}")
                }
                Err(error) => return format!("{error:#}"),
            }
        }
    }

    fn pid_exists(pid: u32) -> bool {
        Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    fn copy_preserving_metadata(source: &Path, destination: &Path) {
        let status = Command::new("/bin/cp")
            .arg("-p")
            .arg(source)
            .arg(destination)
            .status()
            .expect("run cp -p");
        assert!(status.success(), "cp -p failed with {status}");
    }

    fn restore_mtime(reference: &Path, target: &Path) {
        let status = Command::new("/usr/bin/touch")
            .arg("-r")
            .arg(reference)
            .arg(target)
            .status()
            .expect("run touch -r");
        assert!(status.success(), "touch -r failed with {status}");
    }

    struct FailingLogWriter;

    impl Write for FailingLogWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("injected durable log write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("injected durable log flush failure"))
        }
    }

    struct FailAfterFirstWrite {
        writes: usize,
    }

    impl Write for FailAfterFirstWrite {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            if self.writes == 1 {
                Ok(buffer.len())
            } else {
                Err(io::Error::other("injected late durable log failure"))
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct PanicAfterFirstWrite {
        writes: usize,
        file: fs::File,
    }

    struct PanicOnFirstWrite;

    struct BlockingLogWriter {
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    struct BlockAfterFirstWrite {
        writes: usize,
        release: Arc<(Mutex<bool>, Condvar)>,
        file: fs::File,
    }

    struct PanicWhileStderrWaits {
        stdout_holds_log: Arc<(Mutex<bool>, Condvar)>,
        stderr_tail: Arc<Mutex<String>>,
    }

    impl Write for BlockingLogWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            let (lock, wake) = &*self.release;
            let mut released = lock.lock().expect("lock blocking writer release");
            while !*released {
                released = wake.wait(released).expect("wait for writer release");
            }
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Write for BlockAfterFirstWrite {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            if self.writes == 1 {
                return self.file.write(buffer);
            }
            let (lock, wake) = &*self.release;
            let mut released = lock.lock().expect("lock late writer release");
            while !*released {
                released = wake.wait(released).expect("wait for late writer release");
            }
            self.file.write(buffer)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.file.flush()
        }
    }

    impl Write for PanicOnFirstWrite {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            panic!("injected pre-readiness stdout reader panic");
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Write for PanicAfterFirstWrite {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            if self.writes > 1 {
                panic!("injected stdout reader panic");
            }
            self.file.write(buffer)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.file.flush()
        }
    }

    impl Write for PanicWhileStderrWaits {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            if buffer
                .windows(b"PANIC_STDOUT".len())
                .any(|window| window == b"PANIC_STDOUT")
            {
                let (lock, wake) = &*self.stdout_holds_log;
                *lock.lock().expect("lock stdout writer state") = true;
                wake.notify_one();

                let deadline = Instant::now() + Duration::from_secs(1);
                while !self
                    .stderr_tail
                    .lock()
                    .expect("lock stderr tail from writer")
                    .contains("STDERR_WAITING")
                {
                    assert!(
                        Instant::now() < deadline,
                        "stderr reader did not reach its tail before writer panic"
                    );
                    thread::yield_now();
                }
                thread::sleep(Duration::from_millis(25));
                panic!("injected coordinated stdout writer panic");
            }
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn spawn_reader_panic_fixture(temp: &TempDir) -> ManagedProcess {
        let log_path = temp.path().join("fixture.log");
        let log_file = fs::File::create(&log_path).expect("create injected durable log");
        ManagedProcess::spawn_with_log_writer(
            "reader panic fixture".to_string(),
            shell("printf 'READY\n'; sleep 0.1; printf 'PANIC_OUTPUT\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            log_path,
            Box::new(PanicAfterFirstWrite {
                writes: 0,
                file: log_file,
            }),
        )
        .expect("spawn reader panic fixture")
    }

    #[test]
    fn process_group_ownership_requires_final_signal_before_reap() {
        let mut ownership = ProcessGroupOwnership::new(42);

        assert!(ownership.permit_reap().is_err());
        assert_eq!(ownership.group_id_for_signal().expect("group owned"), 42);

        ownership
            .record_final_group_signal()
            .expect("record final group signal");
        ownership
            .permit_reap()
            .expect("final group signal permits reap");
        assert!(ownership.group_id_for_signal().is_err());

        ownership.record_reaped().expect("record leader reap");
        assert!(ownership.group_id_for_signal().is_err());
    }

    #[test]
    fn wait_siginfo_layout_matches_the_supported_platform_abi() {
        #[cfg(all(
            any(target_os = "macos", target_os = "ios"),
            target_pointer_width = "64"
        ))]
        {
            assert_eq!(std::mem::size_of::<WaitSiginfo>(), 104);
            assert_eq!(std::mem::align_of::<WaitSiginfo>(), 8);
            assert_eq!(std::mem::offset_of!(WaitSiginfo, signal), 0);
            assert_eq!(std::mem::offset_of!(WaitSiginfo, code), 8);
            assert_eq!(std::mem::offset_of!(WaitSiginfo, pid), 12);
            assert_eq!(std::mem::offset_of!(WaitSiginfo, status), 20);
        }
        #[cfg(all(
            any(target_os = "linux", target_os = "android"),
            target_pointer_width = "64"
        ))]
        {
            assert_eq!(std::mem::size_of::<WaitSiginfo>(), 128);
            assert_eq!(std::mem::align_of::<WaitSiginfo>(), 8);
            assert_eq!(std::mem::offset_of!(WaitSiginfo, signal), 0);
            assert_eq!(std::mem::offset_of!(WaitSiginfo, code), 8);
            assert_eq!(std::mem::offset_of!(WaitSiginfo, pid), 16);
            assert_eq!(std::mem::offset_of!(WaitSiginfo, status), 24);
        }
        #[cfg(all(
            any(target_os = "linux", target_os = "android"),
            target_pointer_width = "32"
        ))]
        compile_error!("32-bit Linux/Android waitid layouts must be rejected before tests run");
    }

    #[test]
    fn wait_siginfo_abi_policy_rejects_unverified_32_bit_targets() {
        assert!(wait_siginfo_abi_supported("linux", "x86_64", 64));
        assert!(wait_siginfo_abi_supported("macos", "aarch64", 64));
        assert!(!wait_siginfo_abi_supported("linux", "x86_64", 32));
        assert!(!wait_siginfo_abi_supported("android", "x86_64", 32));
        assert!(!wait_siginfo_abi_supported("macos", "x86", 32));
        assert!(!wait_siginfo_abi_supported("ios", "arm", 32));
        assert!(!wait_siginfo_abi_supported("linux", "mips64", 64));
    }

    #[test]
    fn runtime_diagnostics_preserves_process_group_ownership_until_cleanup() {
        let temp = TempDir::new("diagnostic-unreaped");
        let config_path = temp.path().join("fixture.toml");
        fs::write(&config_path, "fixture = true\n").expect("write fixture config");
        let mut process = ManagedProcess::spawn(
            "diagnostic unreaped fixture".to_string(),
            shell("printf 'READY\n'; sleep 0.05; sleep 30 & exit 23"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn diagnostic fixture");

        assert!(wait_until(Duration::from_secs(1), || process
            .runtime_diagnostic("fixture", "local", &config_path)
            .is_err()));
        assert_eq!(
            process
                .process_group
                .group_id_for_signal()
                .expect("runtime diagnostics must retain process-group ownership"),
            process.pid()
        );
        process
            .kill_now()
            .expect("cleanup after non-reaping runtime diagnostics");
    }

    #[test]
    fn non_unix_runtime_diagnostic_compile_contract_rejects_unsupported_probe() {
        let error = unsupported_runtime_exit_status("fixture", 42, "local")
            .expect_err("unsupported non-Unix semantics must return an explicit error");
        assert!(
            error
                .to_string()
                .contains("non-reaping runtime diagnostics are unsupported"),
            "{error:#}"
        );
    }

    #[test]
    fn runtime_diagnostics_reports_real_exit_code_without_reaping() {
        let temp = TempDir::new("diagnostic-exit-code");
        let config_path = temp.path().join("fixture.toml");
        fs::write(&config_path, "fixture = true\n").expect("write fixture config");
        let mut process = ManagedProcess::spawn(
            "diagnostic exit-code fixture".to_string(),
            shell("printf 'READY\n'; sleep 0.2; exit 23"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn diagnostic fixture");

        let diagnostic = wait_for_runtime_error(&mut process, &config_path, Duration::from_secs(2));
        assert!(diagnostic.contains("status=exit code 23"), "{diagnostic}");
        assert_eq!(
            process
                .process_group
                .group_id_for_signal()
                .expect("diagnostic must not reap the leader"),
            process.pid()
        );
        process
            .kill_now()
            .expect("cleanup after non-reaping exit diagnostic");
    }

    #[test]
    fn runtime_diagnostics_reports_real_signal_without_reaping() {
        let temp = TempDir::new("diagnostic-signal");
        let config_path = temp.path().join("fixture.toml");
        fs::write(&config_path, "fixture = true\n").expect("write fixture config");
        let mut process = ManagedProcess::spawn(
            "diagnostic signal fixture".to_string(),
            shell("printf 'READY\n'; sleep 0.2; kill -TERM $$"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn diagnostic fixture");

        let diagnostic = wait_for_runtime_error(&mut process, &config_path, Duration::from_secs(2));
        assert!(diagnostic.contains("status=signal 15"), "{diagnostic}");
        assert_eq!(
            process
                .process_group
                .group_id_for_signal()
                .expect("diagnostic must not reap the leader"),
            process.pid()
        );
        process
            .kill_now()
            .expect("cleanup after non-reaping signal diagnostic");
    }

    #[test]
    fn managed_process_captures_bounded_tails_and_durable_log() {
        let temp = TempDir::new("tails");
        let log_path = temp.path().join("fixture.log");
        let command = shell(
            "i=0; while [ $i -lt 1200 ]; do printf 'stdout-%04d-abcdefghij\\n' \"$i\"; printf 'stderr-%04d-abcdefghij\\n' \"$i\" >&2; i=$((i + 1)); done; printf 'STDOUT_TAIL_READY\\n'; printf 'STDERR_TAIL_DONE\\n' >&2; sleep 30",
        );

        let mut process = ManagedProcess::spawn(
            "tail fixture".to_string(),
            command,
            ReadyMarker::StdoutContains("STDOUT_TAIL_READY".to_string()),
            Duration::from_secs(5),
            log_path.clone(),
        )
        .expect("spawn tail fixture");

        assert!(process.stdout_tail().len() <= 8 * 1024);
        assert!(process.stdout_tail().contains("STDOUT_TAIL_READY"));
        assert!(wait_until(Duration::from_secs(1), || process
            .stderr_tail()
            .contains("STDERR_TAIL_DONE")));
        assert!(process.stderr_tail().len() <= 8 * 1024);
        process
            .assert_log_contains("STDOUT_TAIL_READY")
            .expect("durable log contains stdout");
        process
            .assert_log_contains("STDERR_TAIL_DONE")
            .expect("durable log contains stderr");
        let log = fs::read_to_string(log_path).expect("read durable log");
        assert!(log.contains("STDOUT_TAIL_READY"));
        assert!(log.contains("STDERR_TAIL_DONE"));
        process.kill_now().expect("kill tail fixture");
    }

    #[test]
    fn managed_process_surfaces_log_failure_after_draining_stdout() {
        let temp = TempDir::new("log-write-failure");
        let error = ManagedProcess::spawn_with_log_writer(
            "log write failure fixture".to_string(),
            shell(
                "printf 'FIRST_CHUNK\n'; i=0; while [ $i -lt 600 ]; do printf 'fill-%04d-abcdefghij\n' \"$i\"; i=$((i + 1)); done; printf 'DRAINED_AFTER_LOG_FAILURE\nREADY\n'; sleep 30",
            ),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
            Box::new(FailingLogWriter),
        )
        .expect_err("durable log failure must fail readiness");
        let message = format!("{error:#}");

        assert!(
            message.contains("injected durable log write failure"),
            "{message}"
        );
        assert!(message.contains("DRAINED_AFTER_LOG_FAILURE"), "{message}");
    }

    #[test]
    fn managed_process_stop_surfaces_log_failure_after_readiness() {
        let temp = TempDir::new("late-log-write-failure");
        let mut process = ManagedProcess::spawn_with_log_writer(
            "late log write failure fixture".to_string(),
            shell("printf 'READY\n'; sleep 0.05; printf 'LATE_OUTPUT\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
            Box::new(FailAfterFirstWrite { writes: 0 }),
        )
        .expect("first durable log write permits readiness");
        assert!(wait_until(Duration::from_secs(1), || process
            .stdout_tail()
            .contains("LATE_OUTPUT")));

        let error = process
            .kill_now()
            .expect_err("late durable log failure must surface during stop");
        let message = format!("{error:#}");
        assert!(
            message.contains("injected late durable log failure"),
            "{message}"
        );
        assert!(message.contains("LATE_OUTPUT"), "{message}");
    }

    #[test]
    fn managed_process_kill_surfaces_reader_thread_panic() {
        let temp = TempDir::new("kill-reader-panic");
        let mut process = spawn_reader_panic_fixture(&temp);
        assert!(wait_until(Duration::from_secs(1), || process
            .stdout_thread
            .lock()
            .expect("lock stdout thread")
            .as_ref()
            .is_some_and(thread::JoinHandle::is_finished)));

        let error = process
            .kill_now()
            .expect_err("kill must surface the reader JoinHandle panic");
        let message = format!("{error:#}");
        assert!(
            message.contains("stdout reader thread panicked"),
            "{message}"
        );
        assert!(
            message.contains("injected stdout reader panic"),
            "{message}"
        );
        assert!(message.contains("PANIC_OUTPUT"), "{message}");
    }

    #[test]
    fn managed_process_readiness_surfaces_reader_thread_panic() {
        let temp = TempDir::new("readiness-reader-panic");
        let error = ManagedProcess::spawn_with_log_writer(
            "readiness reader panic fixture".to_string(),
            shell("printf 'OUTPUT_BEFORE_READY\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
            Box::new(PanicOnFirstWrite),
        )
        .expect_err("readiness must surface the stdout reader panic");
        let message = format!("{error:#}");
        assert!(message.contains("cannot confirm readiness"), "{message}");
        assert!(
            message.contains("stdout reader thread panicked"),
            "{message}"
        );
        assert!(
            message.contains("injected pre-readiness stdout reader panic"),
            "{message}"
        );
    }

    #[test]
    fn writer_panic_preserves_payload_with_concurrent_stderr_reader() {
        let stdout_holds_log = Arc::new((Mutex::new(false), Condvar::new()));
        let stdout_tail = Arc::new(Mutex::new(String::new()));
        let stderr_tail = Arc::new(Mutex::new(String::new()));
        let stderr_tail_guard = stderr_tail.lock().expect("hold stderr tail before readers");
        let output_io_error = Arc::new(Mutex::new(None));
        let log_file: Arc<Mutex<Box<dyn Write + Send>>> =
            Arc::new(Mutex::new(Box::new(PanicWhileStderrWaits {
                stdout_holds_log: Arc::clone(&stdout_holds_log),
                stderr_tail: Arc::clone(&stderr_tail),
            })));

        let stderr_thread = spawn_reader(
            "stderr",
            Cursor::new(b"STDERR_WAITING\n".to_vec()),
            Arc::clone(&stderr_tail),
            Arc::clone(&log_file),
            Arc::clone(&output_io_error),
            None,
            None,
        );
        let stdout_thread = spawn_reader(
            "stdout",
            Cursor::new(b"PANIC_STDOUT\n".to_vec()),
            stdout_tail,
            Arc::clone(&log_file),
            Arc::clone(&output_io_error),
            None,
            None,
        );

        let (lock, wake) = &*stdout_holds_log;
        let mut holds_log = lock.lock().expect("lock stdout writer state");
        while !*holds_log {
            holds_log = wake
                .wait(holds_log)
                .expect("wait for stdout to hold durable log writer");
        }
        drop(holds_log);
        drop(stderr_tail_guard);

        stdout_thread.join().expect("join stdout reader");
        stderr_thread.join().expect("join stderr reader");
        let recorded = output_io_error
            .lock()
            .expect("lock output I/O error")
            .clone()
            .expect("writer panic must be recorded");

        assert!(
            !log_file.is_poisoned(),
            "writer panic must be caught while the guard is held; first_error={recorded}"
        );
        assert!(
            recorded.contains("injected coordinated stdout writer panic"),
            "writer panic payload was replaced by a concurrent lock error: {recorded}"
        );
    }

    #[test]
    fn reader_panic_is_recorded_before_readiness_sender_disconnects() {
        let errors: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let (ready_tx, ready_rx) = mpsc::sync_channel::<()>(1);
        let readiness_sender = Some(ready_tx);

        run_reader_with_panic_boundary("stdout", &errors, || {
            panic!("directed readiness disconnect panic");
        });

        let recorded = errors
            .lock()
            .expect("lock recorded reader panic")
            .clone()
            .expect("panic must be recorded while readiness sender is still alive");
        assert!(
            recorded.contains("directed readiness disconnect panic"),
            "{recorded}"
        );
        assert!(
            matches!(ready_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "readiness sender must still be connected after panic recording"
        );

        drop(readiness_sender);
        assert!(matches!(
            ready_rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn managed_process_log_assertion_surfaces_reader_thread_panic() {
        let temp = TempDir::new("assert-reader-panic");
        let mut process = spawn_reader_panic_fixture(&temp);
        assert!(wait_until(Duration::from_secs(1), || process
            .stdout_thread
            .lock()
            .expect("lock stdout thread")
            .as_ref()
            .is_some_and(thread::JoinHandle::is_finished)));

        let error = process
            .assert_log_contains("READY")
            .expect_err("log assertion must surface the reader JoinHandle panic");
        let message = format!("{error:#}");
        assert!(
            message.contains("stdout reader thread panicked"),
            "{message}"
        );
        assert!(
            message.contains("injected stdout reader panic"),
            "{message}"
        );
        assert!(message.contains("PANIC_OUTPUT"), "{message}");
        process.kill_now().expect_err("cleanup retains first panic");
    }

    #[test]
    fn managed_process_readiness_surfaces_stdout_tail_poison() {
        let temp = TempDir::new("readiness-tail-poison");
        let error = ManagedProcess::spawn_with_poisoned_stdout_tail(
            "readiness tail poison fixture".to_string(),
            shell("printf 'READY_AFTER_TAIL_POISON\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY_AFTER_TAIL_POISON".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect_err("readiness must surface the poisoned stdout tail");
        let message = format!("{error:#}");
        assert!(message.contains("stdout tail lock poisoned"), "{message}");
        assert!(
            fs::read_to_string(temp.path().join("fixture.log"))
                .expect("read durable log after poison")
                .contains("READY_AFTER_TAIL_POISON"),
            "tail poison must not stop pipe draining or marker scanning"
        );
    }

    #[test]
    fn managed_process_stop_surfaces_stdout_tail_poison() {
        let temp = TempDir::new("stop-tail-poison");
        let mut process = ManagedProcess::spawn(
            "stop tail poison fixture".to_string(),
            shell("printf 'READY\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn tail poison fixture");
        let stdout_tail = Arc::clone(&process.stdout_buffer);
        let _ = thread::spawn(move || {
            let _guard = stdout_tail.lock().expect("lock stdout tail before poison");
            panic!("inject stdout tail poison");
        })
        .join();

        let error = process
            .stop()
            .expect_err("stop must surface the poisoned stdout tail");
        assert!(
            format!("{error:#}").contains("stdout tail lock poisoned"),
            "{error:#}"
        );
    }

    #[test]
    fn managed_process_reports_early_exit_status() {
        let temp = TempDir::new("early-exit");
        let error = ManagedProcess::spawn(
            "early exit fixture".to_string(),
            shell("printf 'fatal fixture error\\n' >&2; exit 23"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect_err("fixture must exit before readiness");
        let message = format!("{error:#}");
        assert!(
            message.contains("exited before readiness marker"),
            "{message}"
        );
        assert!(message.contains("23"), "{message}");
        assert!(message.contains("fatal fixture error"), "{message}");
    }

    #[test]
    fn managed_process_early_exit_with_inherited_pipes_is_bounded() {
        let temp = TempDir::new("early-exit-inherited-pipes");
        let timeout = Duration::from_millis(100);
        let started = Instant::now();
        let error = ManagedProcess::spawn(
            "early exit inherited pipes fixture".to_string(),
            shell("sleep 2 & printf 'parent failed before ready\n' >&2; exit 23"),
            ReadyMarker::StdoutContains("READY".to_string()),
            timeout,
            temp.path().join("fixture.log"),
        )
        .expect_err("parent must exit before readiness");
        let elapsed = started.elapsed();
        let message = format!("{error:#}");

        assert!(
            elapsed <= timeout + Duration::from_millis(350),
            "early exit cleanup exceeded bound: timeout={timeout:?} elapsed={elapsed:?}; {message}"
        );
        assert!(message.contains("23"), "{message}");
        assert!(message.contains("parent failed before ready"), "{message}");
    }

    #[test]
    fn managed_process_readiness_timeout_is_bounded() {
        let temp = TempDir::new("timeout");
        let timeout = Duration::from_millis(150);
        let started = Instant::now();
        let error = ManagedProcess::spawn(
            "timeout fixture".to_string(),
            shell("sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            timeout,
            temp.path().join("fixture.log"),
        )
        .expect_err("fixture must time out");
        let elapsed = started.elapsed();
        assert!(
            elapsed <= timeout + Duration::from_millis(250),
            "timeout {timeout:?} took {elapsed:?}"
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for readiness marker"),
            "{error:#}"
        );
    }

    fn escaped_pipe_holder_command(pid_path: &Path, hold_seconds: u64) -> Command {
        let mut command = Command::new("python3");
        command
            .arg("-c")
            .arg(
                "import pathlib, subprocess, sys, time; child = subprocess.Popen(['/bin/sleep', sys.argv[2]], start_new_session=True); pathlib.Path(sys.argv[1]).write_text(str(child.pid)); print('READY', flush=True); time.sleep(30)",
            )
            .arg(pid_path)
            .arg(hold_seconds.to_string());
        command
    }

    fn force_kill_fixture(pid: u32) {
        let _ = Command::new("/bin/kill")
            .args(["-KILL", &pid.to_string()])
            .status();
    }

    #[test]
    fn managed_process_kill_is_bounded_when_escaped_descendant_holds_pipes() {
        let temp = TempDir::new("kill-escaped-pipe-holder");
        let escaped_pid_path = temp.path().join("escaped.pid");
        let mut process = ManagedProcess::spawn(
            "kill escaped pipe holder fixture".to_string(),
            escaped_pipe_holder_command(&escaped_pid_path, 2),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn escaped pipe holder fixture");
        let escaped_pid = fs::read_to_string(&escaped_pid_path)
            .expect("read escaped fixture pid")
            .parse::<u32>()
            .expect("parse escaped fixture pid");

        let started = Instant::now();
        let error = process
            .kill_now()
            .expect_err("escaped pipe holder must produce a bounded reader cleanup error");
        let elapsed = started.elapsed();
        force_kill_fixture(escaped_pid);

        assert!(
            elapsed < Duration::from_secs(1),
            "elapsed={elapsed:?}; {error:#}"
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for stdout reader thread"),
            "{error:#}"
        );
    }

    #[test]
    fn managed_process_stop_is_bounded_when_escaped_descendant_holds_pipes() {
        let temp = TempDir::new("stop-escaped-pipe-holder");
        let escaped_pid_path = temp.path().join("escaped.pid");
        let mut process = ManagedProcess::spawn(
            "stop escaped pipe holder fixture".to_string(),
            escaped_pipe_holder_command(&escaped_pid_path, 2),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn escaped pipe holder fixture");
        let escaped_pid = fs::read_to_string(&escaped_pid_path)
            .expect("read escaped fixture pid")
            .parse::<u32>()
            .expect("parse escaped fixture pid");

        let started = Instant::now();
        let error = process
            .stop()
            .expect_err("escaped pipe holder must produce a bounded reader cleanup error");
        let elapsed = started.elapsed();
        force_kill_fixture(escaped_pid);

        assert!(
            elapsed < Duration::from_secs(1),
            "elapsed={elapsed:?}; {error:#}"
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for stdout reader thread"),
            "{error:#}"
        );
    }

    #[test]
    fn managed_process_drop_is_bounded_when_escaped_descendant_holds_pipes() {
        let temp = TempDir::new("drop-escaped-pipe-holder");
        let escaped_pid_path = temp.path().join("escaped.pid");
        let process = ManagedProcess::spawn(
            "drop escaped pipe holder fixture".to_string(),
            escaped_pipe_holder_command(&escaped_pid_path, 2),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn escaped pipe holder fixture");
        let escaped_pid = fs::read_to_string(&escaped_pid_path)
            .expect("read escaped fixture pid")
            .parse::<u32>()
            .expect("parse escaped fixture pid");

        let started = Instant::now();
        drop(process);
        let elapsed = started.elapsed();
        force_kill_fixture(escaped_pid);

        assert!(elapsed < Duration::from_secs(1), "elapsed={elapsed:?}");
    }

    #[test]
    fn managed_process_spawn_cleanup_is_bounded_when_durable_writer_blocks() {
        let temp = TempDir::new("blocking-durable-writer");
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let release_after_delay = Arc::clone(&release);
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            let (lock, wake) = &*release_after_delay;
            *lock.lock().expect("lock writer release") = true;
            wake.notify_all();
        });

        let started = Instant::now();
        let error = ManagedProcess::spawn_with_log_writer(
            "blocking durable writer fixture".to_string(),
            shell("printf 'READY\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_millis(100),
            temp.path().join("fixture.log"),
            Box::new(BlockingLogWriter {
                release: Arc::clone(&release),
            }),
        )
        .expect_err("blocking durable writer must fail within the cleanup deadline");
        let elapsed = started.elapsed();
        releaser.join().expect("join blocking writer releaser");

        assert!(
            elapsed < Duration::from_secs(1),
            "elapsed={elapsed:?}; {error:#}"
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for stdout reader thread"),
            "{error:#}"
        );
    }

    #[test]
    fn managed_process_log_assertion_is_bounded_when_durable_writer_blocks() {
        let temp = TempDir::new("blocking-late-durable-writer");
        let log_path = temp.path().join("fixture.log");
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let release_after_delay = Arc::clone(&release);
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            let (lock, wake) = &*release_after_delay;
            *lock.lock().expect("lock late writer release") = true;
            wake.notify_all();
        });
        let mut process = ManagedProcess::spawn_with_log_writer(
            "blocking late durable writer fixture".to_string(),
            shell("printf 'READY\n'; sleep 0.1; printf 'LATE_OUTPUT\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            log_path.clone(),
            Box::new(BlockAfterFirstWrite {
                writes: 0,
                release: Arc::clone(&release),
                file: fs::File::create(&log_path).expect("create blocking writer log"),
            }),
        )
        .expect("first durable write permits readiness");
        assert!(wait_until(Duration::from_secs(1), || process
            .stdout_tail()
            .contains("LATE_OUTPUT")));

        let started = Instant::now();
        let error = process
            .assert_log_contains("READY")
            .expect_err("log assertion must not block behind the durable writer");
        let elapsed = started.elapsed();
        let _ = process.kill_now();
        releaser.join().expect("join late writer releaser");

        assert!(
            elapsed < Duration::from_secs(1),
            "elapsed={elapsed:?}; {error:#}"
        );
        assert!(
            format!("{error:#}").contains("durable process log writer is busy"),
            "{error:#}"
        );
    }

    #[test]
    fn managed_process_supports_file_readiness_markers() {
        let temp = TempDir::new("file-ready");
        let ready_path = temp.path().join("ready.txt");
        let command = shell_with_arg(
            "printf 'prefix FILE_READY suffix\\n' > \"$1\"; sleep 30",
            &ready_path,
        );
        let mut process = ManagedProcess::spawn(
            "file marker fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("file marker becomes ready");
        process.kill_now().expect("kill file marker fixture");
    }

    #[test]
    fn managed_process_file_readiness_surfaces_poll_io_error_immediately() {
        let temp = TempDir::new("file-ready-poll-error");
        let ready_path = temp.path().join("ready.txt");
        let timeout = Duration::from_secs(2);
        let started = Instant::now();

        let error = ManagedProcess::spawn(
            "file marker poll error fixture".to_string(),
            shell_with_arg("sleep 0.05; mkdir \"$1\"; sleep 30", &ready_path),
            ReadyMarker::FileContains {
                path: ready_path.clone(),
                needle: "FILE_READY".to_string(),
            },
            timeout,
            temp.path().join("fixture.log"),
        )
        .expect_err("a readiness path that becomes a directory must fail immediately");
        let elapsed = started.elapsed();
        let message = format!("{error:#}");

        assert!(
            elapsed < Duration::from_secs(1),
            "poll I/O error was swallowed until timeout: elapsed={elapsed:?}; {message}"
        );
        assert!(
            message.contains(&ready_path.display().to_string()),
            "{message}"
        );
        assert!(
            message.contains("Is a directory") || message.contains("os error 21"),
            "{message}"
        );
    }

    #[test]
    fn managed_process_rejects_stale_file_readiness_marker() {
        let temp = TempDir::new("stale-file-ready");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "STALE FILE_READY\n").expect("write stale readiness marker");
        let timeout = Duration::from_millis(120);
        let started = Instant::now();

        let error = ManagedProcess::spawn(
            "stale file marker fixture".to_string(),
            shell("sleep 30"),
            ReadyMarker::FileContains {
                path: ready_path.clone(),
                needle: "FILE_READY".to_string(),
            },
            timeout,
            temp.path().join("fixture.log"),
        )
        .expect_err("pre-existing marker must not satisfy readiness");

        assert!(
            started.elapsed() >= timeout,
            "stale marker returned before startup timeout"
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for readiness marker"),
            "{error:#}"
        );
        assert_eq!(
            fs::read_to_string(ready_path).expect("read preserved stale marker"),
            "STALE FILE_READY\n",
            "ManagedProcess must not delete caller-owned readiness files"
        );
    }

    #[test]
    fn managed_process_accepts_file_marker_appended_after_spawn() {
        let temp = TempDir::new("appended-file-ready");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "existing prefix\n").expect("write file baseline");
        let command = shell_with_arg(
            "sleep 0.05; printf 'FILE_READY\n' >> \"$1\"; sleep 30",
            &ready_path,
        );

        let mut process = ManagedProcess::spawn(
            "appended file marker fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("marker appended after spawn becomes ready");
        process.kill_now().expect("kill appended marker fixture");
    }

    #[test]
    fn managed_process_accepts_file_marker_split_across_append_boundary() {
        let temp = TempDir::new("append-boundary-file-ready");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "existing prefix FILE_RE").expect("write file baseline");
        let command = shell_with_arg(
            "sleep 0.05; printf 'ADY suffix\n' >> \"$1\"; sleep 30",
            &ready_path,
        );

        let mut process = ManagedProcess::spawn(
            "append-boundary file marker fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("marker split across baseline and appended suffix becomes ready");
        process
            .kill_now()
            .expect("kill append-boundary marker fixture");
    }

    #[test]
    fn managed_process_rejects_stale_file_marker_when_unrelated_bytes_are_appended() {
        let temp = TempDir::new("stale-marker-unrelated-append");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "STALE FILE_READY\n").expect("write stale file baseline");
        let command = shell_with_arg(
            "sleep 0.05; printf 'unrelated append\n' >> \"$1\"; sleep 30",
            &ready_path,
        );

        let error = ManagedProcess::spawn(
            "stale marker unrelated append fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_millis(180),
            temp.path().join("fixture.log"),
        )
        .expect_err("an unrelated append must not refresh a stale marker");
        assert!(
            format!("{error:#}").contains("timed out waiting for readiness marker"),
            "{error:#}"
        );
    }

    #[test]
    fn managed_process_accepts_file_marker_after_truncate() {
        let temp = TempDir::new("truncated-file-ready");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "old generation\n").expect("write file baseline");
        let command = shell_with_arg(
            "sleep 0.05; printf 'FILE_READY\n' > \"$1\"; sleep 30",
            &ready_path,
        );

        let mut process = ManagedProcess::spawn(
            "truncated file marker fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("marker written after truncate becomes ready");
        process.kill_now().expect("kill truncated marker fixture");
    }

    #[test]
    fn managed_process_accepts_same_marker_rewritten_after_truncate() {
        let temp = TempDir::new("same-marker-rewritten");
        let ready_path = temp.path().join("ready.txt");
        fs::write(&ready_path, "FILE_READY\n").expect("write stale marker generation");
        let command = shell_with_arg(
            "sleep 0.05; printf 'FILE_READY\n' > \"$1\"; sleep 30",
            &ready_path,
        );

        let mut process = ManagedProcess::spawn(
            "same marker rewritten fixture".to_string(),
            command,
            ReadyMarker::FileContains {
                path: ready_path,
                needle: "FILE_READY".to_string(),
            },
            Duration::from_millis(300),
            temp.path().join("fixture.log"),
        )
        .expect("same marker rewritten after truncate becomes fresh");
        process
            .kill_now()
            .expect("kill same marker rewritten fixture");
    }

    #[test]
    fn file_readiness_detects_same_bytes_rewrite_with_forced_same_mtime() {
        let temp = TempDir::new("same-marker-same-mtime");
        let ready_path = temp.path().join("ready.txt");
        let mtime_reference = temp.path().join("mtime-reference.txt");
        fs::write(&ready_path, "FILE_READY\n").expect("write stale marker generation");
        copy_preserving_metadata(&ready_path, &mtime_reference);
        let baseline = ReadinessBaseline::File {
            snapshot: Some(
                FileReadinessSnapshot::read(&ready_path)
                    .expect("read baseline")
                    .expect("baseline exists"),
            ),
        };

        thread::sleep(Duration::from_millis(20));
        fs::write(&ready_path, "FILE_READY\n").expect("rewrite marker with the same bytes");
        restore_mtime(&mtime_reference, &ready_path);
        let current = FileReadinessSnapshot::read(&ready_path)
            .expect("read current generation")
            .expect("current file exists");
        let ReadinessBaseline::File {
            snapshot: Some(original),
        } = &baseline
        else {
            panic!("file baseline expected");
        };
        assert_eq!(current.bytes, original.bytes, "test must preserve bytes");
        assert_eq!(
            current.modified, original.modified,
            "test must force the original mtime"
        );

        assert!(
            baseline.file_contains_fresh_marker(&current, "FILE_READY"),
            "same bytes and mtime must still be fresh after an in-place rewrite"
        );
    }

    #[test]
    fn file_readiness_rejects_a_generation_changed_during_the_same_handle_read() {
        let temp = TempDir::new("concurrent-snapshot-rewrite");
        let ready_path = temp.path().join("ready.txt");
        let old_bytes = vec![b'A'; 32 * 1024];
        let new_bytes = vec![b'B'; old_bytes.len()];
        fs::write(&ready_path, &old_bytes).expect("write snapshot baseline");

        let error = FileReadinessSnapshot::read_once_with_hook(&ready_path, || {
            fs::write(&ready_path, &new_bytes).expect("rewrite snapshot during read");
        })
        .expect_err("a read spanning two file generations must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock, "{error}");
        assert!(
            error
                .to_string()
                .contains("readiness file changed while reading"),
            "{error}"
        );
    }

    #[test]
    fn file_readiness_detects_rename_replacement_with_same_bytes_and_mtime() {
        let temp = TempDir::new("rename-same-marker-same-mtime");
        let ready_path = temp.path().join("ready.txt");
        let replacement_path = temp.path().join("replacement.txt");
        fs::write(&ready_path, "FILE_READY\n").expect("write stale marker generation");
        let baseline = ReadinessBaseline::File {
            snapshot: Some(
                FileReadinessSnapshot::read(&ready_path)
                    .expect("read baseline")
                    .expect("baseline exists"),
            ),
        };

        copy_preserving_metadata(&ready_path, &replacement_path);
        fs::rename(&replacement_path, &ready_path).expect("replace readiness path by rename");
        let current = FileReadinessSnapshot::read(&ready_path)
            .expect("read replacement generation")
            .expect("replacement exists");
        let ReadinessBaseline::File {
            snapshot: Some(original),
        } = &baseline
        else {
            panic!("file baseline expected");
        };
        assert_eq!(current.bytes, original.bytes, "test must preserve bytes");
        assert_eq!(
            current.modified, original.modified,
            "test must preserve mtime"
        );

        assert!(
            baseline.file_contains_fresh_marker(&current, "FILE_READY"),
            "a replacement inode must be a fresh file generation"
        );
    }

    #[test]
    fn managed_process_stop_delivers_sigterm() {
        let temp = TempDir::new("sigterm");
        let term_path = temp.path().join("term.txt");
        let command = shell_with_arg(
            "trap 'printf TERM > \"$1\"; exit 0' TERM; printf 'READY\\n'; while :; do sleep 1; done",
            &term_path,
        );
        let mut process = ManagedProcess::spawn(
            "SIGTERM fixture".to_string(),
            command,
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn SIGTERM fixture");

        process.stop().expect("stop fixture gracefully");
        assert_eq!(
            fs::read_to_string(term_path).expect("read TERM marker"),
            "TERM"
        );
    }

    #[test]
    fn managed_process_waits_for_post_readiness_log_marker() {
        let temp = TempDir::new("post-readiness-log-marker");
        let mut process = ManagedProcess::spawn(
            "post-readiness log marker fixture".to_string(),
            shell("printf 'READY\\n'; sleep 0.05; printf 'AFTER_READY\\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn post-readiness log marker fixture");

        process
            .wait_for_log_contains("AFTER_READY", Duration::from_secs(2))
            .expect("wait for post-readiness marker");
        process.kill_now().expect("kill post-readiness fixture");
    }

    #[test]
    fn managed_process_log_wait_kills_process_on_timeout() {
        let temp = TempDir::new("post-readiness-log-timeout");
        let mut process = ManagedProcess::spawn(
            "post-readiness log timeout fixture".to_string(),
            shell("printf 'READY\\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn post-readiness log timeout fixture");
        let pid = process.pid();

        let error = process
            .wait_for_log_contains("MISSING", Duration::from_millis(100))
            .expect_err("missing marker must time out");

        assert!(format!("{error:#}").contains("timed out"), "{error:#}");
        assert!(wait_until(Duration::from_secs(1), || !pid_exists(pid)));
    }

    #[test]
    fn managed_process_interrupt_requires_successful_exit() {
        let temp = TempDir::new("sigint");
        let interrupt_path = temp.path().join("interrupt.txt");
        let command = shell_with_arg(
            "trap 'printf INT > \"$1\"; exit 0' INT; printf 'READY\\n'; while :; do sleep 1; done",
            &interrupt_path,
        );
        let mut process = ManagedProcess::spawn(
            "SIGINT fixture".to_string(),
            command,
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn SIGINT fixture");

        let status = process
            .interrupt_and_wait(Duration::from_secs(2))
            .expect("SIGINT fixture exits successfully");

        assert!(status.success());
        assert_eq!(
            fs::read_to_string(interrupt_path).expect("read INT marker"),
            "INT"
        );
    }

    #[test]
    fn managed_process_drop_kills_descendants_and_reaps_child() {
        let temp = TempDir::new("descendant");
        let descendant_path = temp.path().join("descendant.pid");
        let child_pid;
        let descendant_pid;
        {
            let command = shell_with_arg(
                "sleep 30 & descendant=$!; printf '%s' \"$descendant\" > \"$1\"; printf 'READY\\n'; wait",
                &descendant_path,
            );
            let process = ManagedProcess::spawn(
                "descendant fixture".to_string(),
                command,
                ReadyMarker::StdoutContains("READY".to_string()),
                Duration::from_secs(2),
                temp.path().join("fixture.log"),
            )
            .expect("spawn descendant fixture");
            child_pid = process.pid();
            descendant_pid = fs::read_to_string(&descendant_path)
                .expect("read descendant pid")
                .parse::<u32>()
                .expect("parse descendant pid");
            assert!(pid_exists(child_pid));
            assert!(pid_exists(descendant_pid));
        }

        assert!(wait_until(Duration::from_secs(1), || !pid_exists(
            child_pid
        )));
        assert!(wait_until(Duration::from_secs(1), || !pid_exists(
            descendant_pid
        )));
    }

    #[test]
    fn managed_process_can_restart_with_a_new_command() {
        let temp = TempDir::new("restart");
        let mut process = ManagedProcess::spawn(
            "restart fixture".to_string(),
            shell("printf 'FIRST_READY\\n'; sleep 30"),
            ReadyMarker::StdoutContains("FIRST_READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("first.log"),
        )
        .expect("spawn first process");
        let first_pid = process.pid();

        process
            .restart(
                shell("printf 'SECOND_READY\\n'; sleep 30"),
                ReadyMarker::StdoutContains("SECOND_READY".to_string()),
                Duration::from_secs(2),
                temp.path().join("second.log"),
            )
            .expect("restart process");
        assert_ne!(process.pid(), first_pid);
        assert!(process.stdout_tail().contains("SECOND_READY"));
        process.kill_now().expect("kill restarted process");
    }

    #[test]
    fn managed_process_one_shot_accepts_fast_marker_and_successful_exit() {
        let temp = TempDir::new("one-shot-success");
        let log_path = temp.path().join("fixture.log");

        ManagedProcess::run_to_completion(
            "successful one-shot fixture".to_string(),
            shell("printf 'PASS_MARKER\\n'; printf 'diagnostic\\n' >&2; exit 0"),
            ReadyMarker::StdoutContains("PASS_MARKER".to_string()),
            Duration::from_secs(2),
            log_path.clone(),
        )
        .expect("a one-shot command must naturally exit successfully after its marker");

        let log = fs::read_to_string(log_path).expect("read one-shot durable log");
        assert!(log.contains("PASS_MARKER"), "{log:?}");
        assert!(log.contains("diagnostic"), "{log:?}");
    }

    #[test]
    fn managed_process_one_shot_rejects_nonzero_exit_after_marker() {
        let temp = TempDir::new("one-shot-nonzero");
        let pid_path = temp.path().join("fixture.pid");
        let command = shell_with_arg(
            "printf '%s' $$ > \"$1\"; printf 'PASS_MARKER\\n'; printf 'failed\\n' >&2; exit 7",
            &pid_path,
        );

        let error = ManagedProcess::run_to_completion(
            "failing one-shot fixture".to_string(),
            command,
            ReadyMarker::StdoutContains("PASS_MARKER".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect_err("a marker must not hide a later nonzero exit");

        let child_pid = fs::read_to_string(pid_path)
            .expect("read one-shot pid")
            .parse::<u32>()
            .expect("parse one-shot pid");
        assert!(format!("{error:#}").contains("status"), "{error:#}");
        assert!(format!("{error:#}").contains('7'), "{error:#}");
        assert!(wait_until(Duration::from_secs(1), || !pid_exists(
            child_pid
        )));
    }

    #[test]
    fn managed_process_one_shot_timeout_kills_and_reaps_process() {
        let temp = TempDir::new("one-shot-timeout");
        let pid_path = temp.path().join("fixture.pid");
        let command = shell_with_arg(
            "printf '%s' $$ > \"$1\"; printf 'PASS_MARKER\\n'; sleep 30",
            &pid_path,
        );

        let error = ManagedProcess::run_to_completion(
            "timed out one-shot fixture".to_string(),
            command,
            ReadyMarker::StdoutContains("PASS_MARKER".to_string()),
            Duration::from_millis(150),
            temp.path().join("fixture.log"),
        )
        .expect_err("a one-shot command must time out after the marker if it does not exit");

        let child_pid = fs::read_to_string(pid_path)
            .expect("read timed out one-shot pid")
            .parse::<u32>()
            .expect("parse timed out one-shot pid");
        assert!(format!("{error:#}").contains("timed out"), "{error:#}");
        assert!(wait_until(Duration::from_secs(1), || !pid_exists(
            child_pid
        )));
    }

    #[test]
    fn managed_process_one_shot_rejects_marker_observed_after_deadline() {
        let temp = TempDir::new("one-shot-late-marker");
        let marker = ReadyMarker::StdoutContains("PASS_MARKER".to_string());
        let baseline = ReadinessBaseline::capture(&marker).expect("capture readiness baseline");
        let mut process = ManagedProcess::spawn(
            "late marker one-shot fixture".to_string(),
            shell("printf 'READY\\n'; sleep 30"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn fixture");
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        ready_tx.send(()).expect("publish marker observation");

        let error = process
            .wait_for_ready(
                &marker,
                &baseline,
                &ready_rx,
                Instant::now() - Duration::from_millis(1),
                Duration::from_millis(10),
                true,
            )
            .expect_err("a marker observed after the absolute deadline must be rejected");
        process.kill_now().expect("clean up late marker fixture");
        assert!(format!("{error:#}").contains("timed out"), "{error:#}");
    }

    #[test]
    fn managed_process_one_shot_rejects_natural_exit_after_deadline() {
        let temp = TempDir::new("one-shot-late-exit");
        let mut process = ManagedProcess::spawn(
            "late exit one-shot fixture".to_string(),
            shell("printf 'READY\\n'; sleep 0.05; exit 0"),
            ReadyMarker::StdoutContains("READY".to_string()),
            Duration::from_secs(2),
            temp.path().join("fixture.log"),
        )
        .expect("spawn fixture");
        let child_pid = process.pid();
        assert!(wait_until(Duration::from_secs(1), || process
            .leader_exit_observed()
            .expect("observe leader exit")));

        let error = process
            .wait_for_successful_exit(
                Instant::now() - Duration::from_millis(1),
                Duration::from_millis(10),
            )
            .expect_err("a natural exit observed after the absolute deadline must be rejected");
        assert!(format!("{error:#}").contains("timed out"), "{error:#}");
        assert!(wait_until(Duration::from_secs(1), || !pid_exists(
            child_pid
        )));
    }

    #[test]
    fn managed_process_one_shot_reader_join_uses_original_deadline() {
        let temp = TempDir::new("one-shot-reader-deadline");
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let release_after_delay = Arc::clone(&release);
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(500));
            let (lock, wake) = &*release_after_delay;
            *lock.lock().expect("lock writer release") = true;
            wake.notify_all();
        });
        let timeout = Duration::from_millis(120);
        let started = Instant::now();
        let deadline = started + timeout;
        let mut process = ManagedProcess::spawn_impl(
            "reader deadline one-shot fixture".to_string(),
            shell("printf 'PASS_MARKER\\n'; sleep 0.03; printf 'late\\n' >&2; exit 0"),
            ReadyMarker::StdoutContains("PASS_MARKER".to_string()),
            timeout,
            temp.path().join("fixture.log"),
            Some(Box::new(BlockAfterFirstWrite {
                writes: 0,
                release: Arc::clone(&release),
                file: fs::File::create(temp.path().join("fixture.log"))
                    .expect("create fixture log"),
            })),
            false,
            Some(deadline),
        )
        .expect("marker must arrive within the deadline");

        let error = process
            .wait_for_successful_exit(deadline, timeout)
            .expect_err("reader drain past the original deadline must fail");
        let elapsed = started.elapsed();
        releaser.join().expect("join writer releaser");

        assert!(
            elapsed < Duration::from_millis(220),
            "reader join created a fresh cleanup budget: elapsed={elapsed:?}; {error:#}"
        );
        assert!(
            format!("{error:#}").contains("timed out waiting for stderr reader thread"),
            "{error:#}"
        );
    }
}
