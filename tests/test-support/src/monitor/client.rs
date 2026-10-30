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

// Design: ADR-0184 (docs/adr/ADR-0184-native-test-execution-and-process-monitor-ownership.md)
use crate::{run_retention, unix};
use anyhow::{Context, Result, bail, ensure};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

/// A payload request whose native command is the monitor from construction.
/// Environment and cwd naturally reach the payload; arbitrary pre_exec/Stdio
/// are deliberately absent, so they cannot weaken the monitor's isolation.
pub struct ManagedCommand {
    command: Command,
    program: OsString,
    arguments: Vec<OsString>,
    lifetime: Option<(Instant, Instant)>,
}

/// The admitted payload's work clock expired, independently of its native exit.
#[derive(Debug)]
pub struct PayloadLifetimeExpired;
impl std::fmt::Display for PayloadLifetimeExpired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("managed payload original work deadline expired")
    }
}
impl std::error::Error for PayloadLifetimeExpired {}

impl std::fmt::Debug for ManagedCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedCommand")
            .field("program", &self.program)
            .field("arguments", &self.arguments)
            .finish_non_exhaustive()
    }
}

impl ManagedCommand {
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            command: Command::new(env!("NOVAROCKS_TEST_MONITOR")),
            program: program.as_ref().to_owned(),
            arguments: vec![],
            lifetime: None,
        }
    }
    /// Stop the actual payload group at the original work clock, preserving
    /// the complete clock for escalation and actual exit. Repeated calls only
    /// narrow these clocks; ordinary commands retain startup-only semantics.
    pub fn terminate_at(&mut self, work: Instant, complete: Instant) -> &mut Self {
        self.lifetime = Some(
            self.lifetime
                .map_or((work, complete), |(old_work, old_end)| {
                    (work.min(old_work), complete.min(old_end))
                }),
        );
        self
    }
    pub fn arg(&mut self, argument: impl AsRef<OsStr>) -> &mut Self {
        self.arguments.push(argument.as_ref().to_owned());
        self
    }
    pub fn args<I, S>(&mut self, arguments: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        for argument in arguments {
            self.arg(argument);
        }
        self
    }
    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.command.env(key, value);
        self
    }
    pub fn envs<I, K, V>(&mut self, values: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.command.envs(values);
        self
    }
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        self.command.env_remove(key);
        self
    }
    pub fn env_clear(&mut self) -> &mut Self {
        self.command.env_clear();
        self
    }
    pub fn current_dir(&mut self, path: impl AsRef<Path>) -> &mut Self {
        self.command.current_dir(path);
        self
    }
    pub fn get_program(&self) -> &OsStr {
        &self.program
    }
    pub fn get_args(&self) -> impl Iterator<Item = &OsStr> {
        self.arguments.iter().map(OsString::as_os_str)
    }
    pub fn get_envs(&self) -> std::process::CommandEnvs<'_> {
        self.command.get_envs()
    }
}

pub(crate) struct Material {
    pub path: PathBuf,
    pub run: Arc<run_retention::RunContext>,
    release: std::sync::atomic::AtomicBool,
}

impl Material {
    pub fn release(&self) {
        if !self.run.formal {
            self.release
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

impl Drop for Material {
    fn drop(&mut self) {
        if self.release.load(std::sync::atomic::Ordering::Acquire)
            && !self.run.formal
            && let Err(error) = fs::remove_dir_all(&self.path)
        {
            // A blocked inherited stderr must never hold lifecycle/pins.
            let _ = fs::write(
                self.run
                    .root
                    .join(format!("release-error-{}", run_retention::unique())),
                format!("release {}: {error}\n", self.path.display()),
            );
        }
    }
}

pub(crate) struct OwnedMonitor {
    control: Option<File>,
    completion: mpsc::Receiver<std::io::Result<ExitStatus>>,
    monitor_status: Mutex<Option<ExitStatus>>,
    pub pid: u32,
    pub material: Arc<Material>,
    pub stdout: File,
    pub stderr: File,
    // None means a Q write had an ambiguous outcome. Cleanup controls remain
    // usable, but a later query must never reuse a possibly stale generation.
    liveness_generation: Mutex<Option<u64>>,
}

#[derive(Debug, Clone)]
pub(crate) struct Final {
    pub status: ExitStatus,
    pub owner_lost: bool,
    pub cleanup_complete: bool,
    pub group_exit_observed: bool,
    pub errors: Vec<String>,
    pub lifetime_expired: bool,
}

fn hex(value: &OsStr) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn write_liveness_query(
    generation: &mut Option<u64>,
    write: impl FnOnce() -> Result<()>,
) -> Result<u64> {
    let current = generation
        .context("monitor liveness generation is unusable after a query write failure")?;
    if let Err(error) = write() {
        // A frame can have reached the monitor even when its caller observes
        // an error, including expiry at the post-write deadline check.
        *generation = None;
        return Err(
            error.context("monitor liveness generation is unusable after a query write failure")
        );
    }
    let Some(next) = current.checked_add(1) else {
        *generation = None;
        bail!("monitor liveness generation overflow");
    };
    *generation = Some(next);
    Ok(next)
}

struct PendingRecord {
    file: File,
    transferred: bool,
}
impl std::ops::Deref for PendingRecord {
    type Target = File;
    fn deref(&self) -> &File {
        &self.file
    }
}
impl std::io::Write for PendingRecord {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.file.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
impl Drop for PendingRecord {
    fn drop(&mut self) {
        if !self.transferred {
            // No monitor can own a payload yet. Preserve a terminal startup
            // failure rather than manufacturing an eternal child obligation.
            let _ = writeln!(
                self.file,
                "{{\"schema\":1,\"event\":\"Final\",\"status_raw\":null,\"startup_error\":\"caller launch setup failed before monitor handoff; see original caller error\",\"owner_lost\":false,\"errors\":[],\"cleanup_complete\":true}}"
            );
        }
    }
}

impl OwnedMonitor {
    pub fn spawn(label: &str, request: ManagedCommand, deadline: Instant) -> Result<Self> {
        let (mut result, mut ack_read) = Self::spawn_unconfirmed(label, request, deadline)?;
        result.finish_startup(&mut ack_read, deadline)?;
        Ok(result)
    }

    fn spawn_unconfirmed(
        label: &str,
        mut request: ManagedCommand,
        deadline: Instant,
    ) -> Result<(Self, File)> {
        let absolute = unix::absolute_deadline(deadline)?;
        let lifetime = request
            .lifetime
            .map(|(work, complete)| -> Result<_> {
                ensure!(
                    work < complete,
                    "payload work deadline must precede complete deadline"
                );
                Ok((
                    unix::absolute_deadline(work)?,
                    unix::absolute_deadline(complete)?,
                ))
            })
            .transpose()?;
        let run = run_retention::ensure_run_retention(deadline)?;
        let gate = run.gate(deadline)?;
        let path = run.root.join("monitor").join(run_retention::unique());
        fs::create_dir_all(path.parent().unwrap())?;
        ensure!(
            fs::symlink_metadata(path.parent().unwrap())?.is_dir(),
            "monitor directory must not be a symlink"
        );
        fs::create_dir(&path)?;
        let material = Arc::new(Material {
            path,
            run,
            release: false.into(),
        });
        material.run.verify_file(&File::open(&material.path)?)?;
        let mut record = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(material.path.join("record.jsonl"))?;
        material.run.verify_file(&record)?;
        run_retention::lock_until(&record, false, deadline)?;
        let payload_hex: Vec<_> = std::iter::once(request.program.as_os_str())
            .chain(request.get_args())
            .map(hex)
            .collect();
        let native = [
            "NEXTEST_RUN_ID",
            "NEXTEST_BINARY_ID",
            "NEXTEST_TEST_NAME",
            "NEXTEST_VERSION",
        ]
        .into_iter()
        .map(|name| (name, std::env::var(name).ok()))
        .collect::<std::collections::BTreeMap<_, _>>();
        serde_json::to_writer(
            &mut record,
            &serde_json::json!({"schema":1,"event":"Pending","label":label,"payload_os_hex":payload_hex,"native":native,"stdout":"stdout.log","stderr":"stderr.log","deadline_ns":absolute,"lifetime_ns":lifetime}),
        )?;
        record.write_all(b"\n")?;
        record.flush()?;
        let mut record = PendingRecord {
            file: record,
            transferred: false,
        };
        let stdout = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(material.path.join("stdout.log"))?;
        let stderr = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(material.path.join("stderr.log"))?;
        let (control_read, control) = unix::fifo_pair(&material.path.join(".control"))?;
        let (ack_read, ack_write) = unix::fifo_pair(&material.path.join(".ack"))?;
        for file in [
            &stdout,
            &stderr,
            &control_read,
            &control,
            &ack_read,
            &ack_write,
        ] {
            material.run.verify_file(file)?;
        }
        let copies = [
            &control_read,
            &ack_write,
            &record.file,
            &gate,
            &material.run.pin,
            &stdout,
            &stderr,
        ]
        .into_iter()
        .map(unix::high_copy)
        .collect::<std::io::Result<Vec<_>>>()?;
        let numbers: Vec<_> = copies.iter().map(AsRawFd::as_raw_fd).collect();
        // A pre-created waiter owns the monitor Child even if caller timeout
        // precedes cleanup. Its eventual wait result is a separate fact.
        let (handoff, receive) = mpsc::channel::<Child>();
        let (done, completion) = mpsc::channel();
        std::thread::Builder::new()
            .name("test-payload-monitor-wait".into())
            .spawn(move || {
                if let Ok(mut child) = receive.recv() {
                    let _ = done.send(child.wait());
                }
            })?;
        request
            .command
            .arg("payload-monitor")
            .arg(absolute.to_string())
            .arg(lifetime.map_or(0, |value| value.0).to_string())
            .arg(lifetime.map_or(0, |value| value.1).to_string())
            .arg(&material.run.root)
            .arg("--")
            .arg(&request.program)
            .args(&request.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // SAFETY: no allocation/locking/environment access after fork. Sources
        // are >=64 and targets 3..=9, so mappings cannot overwrite another source.
        unsafe {
            request.command.pre_exec(move || {
                for (i, source) in numbers.iter().enumerate() {
                    unix::map_fd(*source, i as i32 + 3)?;
                }
                Ok(())
            });
        }
        let child = match request.command.spawn() {
            Ok(child) => {
                record.transferred = true;
                child
            }
            Err(error) => {
                // No monitor exists, so the caller still owns this record.
                writeln!(
                    record,
                    "{{\"schema\":1,\"event\":\"Final\",\"status_raw\":null,\"startup_error\":{},\"owner_lost\":false,\"errors\":[],\"cleanup_complete\":true}}",
                    run_retention::quote(&error.to_string())
                )?;
                record.transferred = true;
                return Err(error).context("spawn test payload monitor");
            }
        };
        if let Err(failed) = handoff.send(child) {
            let mut child = failed.0;
            let _ = child.kill();
            let _ = child.wait();
            bail!("monitor waiter handoff failed");
        }
        drop(copies);
        drop(control_read);
        drop(ack_write);
        // Parent only closes shared locked descriptions. It must never unlock.
        drop(record);
        drop(gate);
        let result = Self {
            control: Some(control),
            completion,
            monitor_status: Mutex::new(None),
            pid: 0,
            material,
            stdout,
            stderr,
            liveness_generation: Mutex::new(Some(0)),
        };
        Ok((result, ack_read))
    }

    fn finish_startup(&mut self, ack_read: &mut impl Read, deadline: Instant) -> Result<()> {
        let mut acknowledged = None;
        match self.read_startup(ack_read, deadline, &mut acknowledged) {
            Ok(()) => Ok(()),
            Err(primary) => {
                // The original startup verdict is final. Keep both lifelines
                // until this exact monitor is explicitly settled or cleanup fails.
                let cleanup_deadline = Instant::now() + std::time::Duration::from_secs(10);
                match self.settle_startup_failure(ack_read, acknowledged, cleanup_deadline) {
                    Ok(()) => Err(primary),
                    Err(secondary) => Err(primary.context(format!(
                        "monitor startup cleanup also failed: {secondary:#}"
                    ))),
                }
            }
        }
    }

    fn read_startup(
        &mut self,
        ack_read: &mut impl Read,
        deadline: Instant,
        acknowledged: &mut Option<bool>,
    ) -> Result<()> {
        loop {
            ensure!(
                Instant::now() < deadline,
                "monitor startup deadline expired; diagnostics={}",
                self.material.path.display()
            );
            let mut byte = [0u8];
            match ack_read.read(&mut byte) {
                Ok(1) => {
                    *acknowledged = Some(byte == *b"R");
                    ensure!(byte == *b"R", "invalid monitor startup acknowledgement");
                    break;
                }
                Err(error) if error.kind() != std::io::ErrorKind::WouldBlock => {
                    return Err(error.into());
                }
                _ => {}
            }
            self.final_record()?;
            std::thread::sleep(unix::POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
        let mut pid = [0u8; 4];
        let mut offset = 0;
        while offset < pid.len() {
            match ack_read.read(&mut pid[offset..]) {
                Ok(0) => bail!(
                    "monitor closed startup reply; diagnostics={}",
                    self.material.path.display()
                ),
                Ok(length) => offset += length,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
            ensure!(
                Instant::now() < deadline,
                "original deadline expired receiving payload identity"
            );
        }
        self.pid = u32::from_le_bytes(pid);
        ensure!(
            self.pid != 0,
            "monitor returned an invalid payload identity"
        );
        Ok(())
    }

    fn settle_startup_failure(
        &mut self,
        ack_read: &mut impl Read,
        mut acknowledged: Option<bool>,
        deadline: Instant,
    ) -> Result<()> {
        let mut failures = Vec::new();
        if acknowledged == Some(false) {
            failures.push("invalid startup acknowledgement cannot be resumed".into());
        }
        while acknowledged != Some(false) {
            // Final publication alone does not consume the actual wait result.
            match self.poll_monitor() {
                Ok(Some(_)) => break,
                Ok(None) => {}
                Err(error) => {
                    failures.push(format!("observe startup monitor completion: {error:#}"));
                    break;
                }
            }
            if acknowledged == Some(true) {
                // R proves that the monitor's initial control read has passed.
                // PID bytes may already have been partly consumed: never reread R.
                if let Err(error) = self.send_until(b'K', deadline) {
                    failures.push(format!("explicit startup cleanup control: {error:#}"));
                }
                break;
            }
            if Instant::now() >= deadline {
                failures.push("cleanup deadline expired awaiting startup acknowledgement".into());
                break;
            }
            let mut byte = [0u8];
            match ack_read.read(&mut byte) {
                Ok(1) if byte == *b"R" => acknowledged = Some(true),
                Ok(1) => {
                    failures.push("invalid monitor startup acknowledgement during cleanup".into());
                    break;
                }
                Ok(0) => break,
                Ok(_) => unreachable!("one-byte acknowledgement read"),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(
                        unix::POLL.min(deadline.saturating_duration_since(Instant::now())),
                    );
                }
                Err(error) => {
                    failures.push(format!(
                        "read startup acknowledgement during cleanup: {error}"
                    ));
                    break;
                }
            }
        }
        // Even a failed control/write/read must still consume the actual waiter
        // completion. wait() also preserves startup/Final/cleanup failures.
        if let Err(error) = self.wait(deadline) {
            failures.push(format!("actually settle startup monitor: {error:#}"));
        }
        ensure!(failures.is_empty(), "{}", failures.join("; "));
        Ok(())
    }

    pub fn close_control(&mut self) {
        // EOF must become observable before any fallible caller diagnostics.
        drop(self.control.take());
    }

    pub fn send(&mut self, operation: u8) -> Result<()> {
        self.send_until(
            operation,
            Instant::now() + std::time::Duration::from_secs(10),
        )
    }

    pub fn send_until(&self, operation: u8, deadline: Instant) -> Result<()> {
        // A diagnostic failure must never prevent lifecycle control. Actual
        // monitor wait completion is sufficient to avoid writing a closed FIFO.
        if self.poll_monitor()?.is_some() {
            return Ok(());
        }
        if matches!(self.final_record(), Ok(Some(_))) {
            return Ok(());
        }
        self.write_frame(operation, deadline)
    }

    fn write_frame(&self, operation: u8, deadline: Instant) -> Result<()> {
        let frame = unix::control_frame(operation, unix::absolute_deadline(deadline)?);
        loop {
            ensure!(
                Instant::now() < deadline,
                "original monitor control deadline expired"
            );
            let mut control = self.control.as_ref().context("monitor control is closed")?;
            match control.write(&frame) {
                Ok(16) => {
                    ensure!(
                        Instant::now() < deadline,
                        "monitor control completed after original deadline"
                    );
                    return Ok(());
                }
                Ok(_) => bail!("partial atomic monitor control frame"),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error).context("send owned monitor control"),
            }
            ensure!(
                Instant::now() < deadline,
                "original monitor control deadline expired"
            );
            std::thread::sleep(unix::POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
    }

    pub fn is_running_until(&self, deadline: Instant) -> Result<bool> {
        let mut generation = loop {
            ensure!(
                Instant::now() < deadline,
                "original liveness query deadline expired"
            );
            match self.liveness_generation.try_lock() {
                Ok(value) => break value,
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    bail!("monitor liveness query lock poisoned")
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    ensure!(Instant::now() < deadline, "liveness query deadline expired");
                    std::thread::sleep(unix::POLL);
                }
            }
        };
        ensure!(
            generation.is_some(),
            "monitor liveness generation is unusable after a query write failure"
        );
        let finished = self.final_record()?.is_some();
        ensure!(
            Instant::now() < deadline,
            "liveness observation exceeded original deadline"
        );
        if finished {
            return Ok(false);
        }
        let expected_generation =
            write_liveness_query(&mut generation, || self.write_frame(b'Q', deadline))?;
        loop {
            ensure!(
                Instant::now() < deadline,
                "original liveness query deadline expired"
            );
            let mut active = false;
            let records = self.records()?;
            ensure!(
                Instant::now() < deadline,
                "liveness history exceeded original deadline"
            );
            for value in records {
                if value["event"] == "ExitObserved" || value["event"] == "Final" {
                    ensure!(
                        Instant::now() < deadline,
                        "exit observation exceeded original deadline"
                    );
                    return Ok(false);
                }
                if value["event"] == "Liveness"
                    && value["generation"].as_u64() == Some(expected_generation)
                {
                    active = value["running"]
                        .as_bool()
                        .context("monitor liveness lacks running fact")?;
                }
            }
            if active {
                ensure!(
                    Instant::now() < deadline,
                    "liveness response exceeded original deadline"
                );
                return Ok(true);
            }
            self.final_record()?;
            std::thread::sleep(unix::POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
    }

    pub fn observed_exit_description(&self) -> Result<Option<String>> {
        for value in self.records()? {
            if value["event"] == "ExitObserved" {
                let code = value["code"]
                    .as_i64()
                    .context("exit observation lacks code")?;
                let status = value["status"]
                    .as_i64()
                    .context("exit observation lacks status")?;
                ensure!(status >= 0, "invalid observed exit status");
                return Ok(Some(match code {
                    1 => format!("exit code {status}"),
                    2 | 3 => format!("signal {status}"),
                    _ => bail!("unsupported observed exit code {code}"),
                }));
            }
        }
        Ok(None)
    }

    pub fn final_record(&self) -> Result<Option<Final>> {
        self.final_record_with_poll(|| self.poll_monitor())
    }

    fn final_record_with_poll(
        &self,
        poll_monitor: impl FnOnce() -> Result<Option<ExitStatus>>,
    ) -> Result<Option<Final>> {
        if let Some(record) = self.read_final_record()? {
            return Ok(Some(record));
        }
        if let Some(status) = poll_monitor()? {
            // Final is written before monitor exit. It may have been appended
            // after our first read, so only the post-exit read can prove absence.
            if let Some(record) = self.read_final_record()? {
                return Ok(Some(record));
            }
            bail!(
                "monitor exited {status} without a complete Final; diagnostics={}",
                self.material.path.display()
            );
        }
        Ok(None)
    }

    fn read_final_record(&self) -> Result<Option<Final>> {
        // Active appends may expose an unfinished final line; never accept it.
        for value in self.records()? {
            ensure!(value["schema"] == 1, "unsupported monitor record schema");
            if value["event"] == "Final" {
                if let Some(error) = value["startup_error"].as_str() {
                    bail!(
                        "payload startup failed: {error}; diagnostics={}",
                        self.material.path.display()
                    );
                }
                let raw = value["status_raw"]
                    .as_i64()
                    .context("monitor Final lacks original status")?;
                let raw = i32::try_from(raw).context("monitor original exit status overflow")?;
                let errors = value["errors"]
                    .as_array()
                    .context("monitor Final lacks errors")?
                    .iter()
                    .map(|e| {
                        e.as_str()
                            .map(str::to_owned)
                            .context("invalid monitor error")
                    })
                    .collect::<Result<Vec<_>>>()?;
                return Ok(Some(Final {
                    status: ExitStatus::from_raw(raw),
                    owner_lost: value["owner_lost"]
                        .as_bool()
                        .context("monitor Final lacks owner_lost")?,
                    cleanup_complete: value["cleanup_complete"]
                        .as_bool()
                        .context("monitor Final lacks cleanup fact")?,
                    // Absence is unknown, never a positive whole-group receipt.
                    group_exit_observed: value
                        .get("group_exit_observed")
                        .map(|value| {
                            value
                                .as_bool()
                                .context("monitor Final has invalid group exit fact")
                        })
                        .transpose()?
                        .unwrap_or(false),
                    errors,
                    lifetime_expired: value
                        .get("lifetime_expired")
                        .map(|v| {
                            v.as_bool()
                                .context("monitor Final has invalid lifetime expiry fact")
                        })
                        .transpose()?
                        .unwrap_or(false),
                }));
            }
        }
        Ok(None)
    }

    fn records(&self) -> Result<Vec<serde_json::Value>> {
        const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;
        let path = self.material.path.join("record.jsonl");
        ensure!(
            fs::metadata(&path)?.len() <= MAX_RECORD_BYTES,
            "monitor record exceeds bounded control history"
        );
        let bytes = fs::read(path)?;
        ensure!(
            bytes.len() as u64 <= MAX_RECORD_BYTES,
            "monitor record exceeds bounded control history"
        );
        // Validate UTF-8/JSON only for newline-terminated frames. An active
        // append may end in the middle of a multibyte error message.
        bytes
            .split_inclusive(|byte| *byte == b'\n')
            .filter(|line| line.ends_with(b"\n"))
            .map(|line| serde_json::from_slice(line).context("parse complete monitor record frame"))
            .collect()
    }

    fn poll_monitor(&self) -> Result<Option<ExitStatus>> {
        let mut status = self
            .monitor_status
            .lock()
            .map_err(|_| anyhow::anyhow!("monitor status lock poisoned"))?;
        if status.is_none() {
            match self.completion.try_recv() {
                Ok(result) => *status = Some(result.context("actually wait test payload monitor")?),
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => bail!("monitor wait owner disappeared"),
            }
        }
        Ok(*status)
    }

    pub fn wait(&mut self, deadline: Instant) -> Result<Final> {
        let mut diagnostic = None;
        loop {
            ensure!(
                Instant::now() < deadline,
                "original deadline expired waiting for managed process; diagnostics={}",
                self.material.path.display()
            );
            if let Err(error) = self.final_record()
                && diagnostic.is_none()
            {
                diagnostic = Some(error);
            }
            if let Some(status) = self.poll_monitor()? {
                if let Some(error) = diagnostic {
                    return Err(error);
                }
                let final_record = self
                    .final_record()?
                    .context("monitor actually exited without Final")?;
                ensure!(status.success(), "payload monitor exited {status}");
                ensure!(
                    final_record.cleanup_complete
                        && final_record.errors.is_empty()
                        && !final_record.owner_lost,
                    "managed process cleanup violation: {final_record:?}; diagnostics={}",
                    self.material.path.display()
                );
                if final_record.lifetime_expired {
                    return Err(PayloadLifetimeExpired.into());
                }
                ensure!(
                    Instant::now() < deadline,
                    "managed process completion exceeded original deadline"
                );
                return Ok(final_record);
            }
            std::thread::sleep(unix::POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
    }
}

impl Drop for OwnedMonitor {
    fn drop(&mut self) {
        // Closing the only lifeline writer triggers independent cleanup even
        // on panic. Never write inherited stdout/stderr while holding it.
        // The already-created waiter retains the actual monitor Child.
        self.close_control();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::io::{Seek, SeekFrom};

    fn monitor_fixture(root: &Path) -> (OwnedMonitor, mpsc::Sender<std::io::Result<ExitStatus>>) {
        fs::create_dir(root).unwrap();
        let record = root.join("record.jsonl");
        fs::write(&record, b"{\"schema\":1,\"event\":\"Started\"}\n").unwrap();
        let run = Arc::new(run_retention::RunContext {
            root: root.to_owned(),
            formal: false,
            pin: File::create(root.join("pin")).unwrap(),
            directory: File::open(root).unwrap(),
        });
        let material = Arc::new(Material {
            path: root.to_owned(),
            run,
            release: std::sync::atomic::AtomicBool::new(false),
        });
        let (completion, receive) = mpsc::channel();
        (
            OwnedMonitor {
                control: None,
                completion: receive,
                monitor_status: Mutex::new(None),
                pid: std::process::id(),
                material,
                stdout: File::open("/dev/null").unwrap(),
                stderr: File::open("/dev/null").unwrap(),
                liveness_generation: Mutex::new(Some(0)),
            },
            completion,
        )
    }

    #[test]
    fn partial_startup_pid_timeout_keeps_the_owner_until_kill_and_actual_wait() {
        struct ExpireDuringPid<'a> {
            inner: &'a mut File,
            deadline: Instant,
            acknowledged: bool,
            pid_bytes: usize,
        }
        impl Read for ExpireDuringPid<'_> {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                let limit = if self.acknowledged { 1 } else { bytes.len() };
                let length = self.inner.read(&mut bytes[..limit])?;
                if length != 0 {
                    if self.acknowledged {
                        self.pid_bytes += length;
                        // Force the actual original clock to expire in the PID
                        // read, after R and one real PID byte were consumed.
                        while Instant::now() < self.deadline {
                            std::thread::sleep(
                                unix::POLL
                                    .min(self.deadline.saturating_duration_since(Instant::now())),
                            );
                        }
                    } else {
                        assert_eq!(bytes, b"R");
                        self.acknowledged = true;
                    }
                }
                Ok(length)
            }
        }
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let mut command = ManagedCommand::new("/bin/sh");
        command.args(["-c", "sleep 30"]);
        let (mut monitor, mut ack) =
            OwnedMonitor::spawn_unconfirmed("partial startup PID timeout", command, deadline)
                .unwrap();
        let mut reader = ExpireDuringPid {
            inner: &mut ack,
            deadline,
            acknowledged: false,
            pid_bytes: 0,
        };
        let error = monitor.finish_startup(&mut reader, deadline).unwrap_err();
        assert_eq!(
            error.to_string(),
            "original deadline expired receiving payload identity"
        );
        assert!(reader.acknowledged);
        assert_eq!(reader.pid_bytes, 1);
        assert!(monitor.monitor_status.lock().unwrap().unwrap().success());
        let final_record = monitor.final_record().unwrap().unwrap();
        assert_eq!(final_record.status.signal(), Some(9));
        assert!(!final_record.owner_lost);
        assert!(final_record.cleanup_complete);
        assert!(final_record.errors.is_empty());
    }

    #[test]
    fn invalid_startup_ack_is_not_reread_as_a_later_r_byte() {
        let root = std::env::temp_dir().join(format!(
            "nr-startup-invalid-ack-{}",
            run_retention::unique()
        ));
        let (mut monitor, completion) = monitor_fixture(&root);
        let final_frame = serde_json::json!({
            "schema":1, "event":"Final", "status_raw":0,
            "owner_lost":false, "cleanup_complete":true, "errors":[]
        });
        OpenOptions::new()
            .append(true)
            .open(root.join("record.jsonl"))
            .unwrap()
            .write_all(format!("{final_frame}\n").as_bytes())
            .unwrap();
        completion.send(Ok(ExitStatus::from_raw(0))).unwrap();
        let mut reader = std::io::Cursor::new(vec![b'X', b'R']);
        let error = monitor
            .finish_startup(
                &mut reader,
                Instant::now() + std::time::Duration::from_secs(1),
            )
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("invalid monitor startup acknowledgement"));
        assert!(message.contains("invalid startup acknowledgement cannot be resumed"));
        assert_eq!(reader.position(), 1, "the later R is not a fresh ACK");
        assert!(monitor.monitor_status.lock().unwrap().unwrap().success());
        // Completion is already available: this fixture checks reply position,
        // original errors and actual wait; the real-monitor test checks killing.
        drop(monitor);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_startup_final_still_consumes_wait_and_preserves_primary_and_secondary() {
        let root =
            std::env::temp_dir().join(format!("nr-startup-final-wait-{}", run_retention::unique()));
        let (mut monitor, completion) = monitor_fixture(&root);
        let final_frame = serde_json::json!({
            "schema":1, "event":"Final", "status_raw":null,
            "startup_error":"original startup refused",
            "owner_lost":true, "cleanup_complete":true, "errors":[]
        });
        OpenOptions::new()
            .append(true)
            .open(root.join("record.jsonl"))
            .unwrap()
            .write_all(format!("{final_frame}\n").as_bytes())
            .unwrap();
        completion.send(Ok(ExitStatus::from_raw(256))).unwrap();
        let error = monitor
            .finish_startup(&mut std::io::empty(), Instant::now())
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("monitor startup deadline expired"),
            "{message}"
        );
        assert!(message.contains("original startup refused"), "{message}");
        assert!(monitor.monitor_status.lock().unwrap().is_some());
        drop(monitor);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn final_written_between_initial_read_and_monitor_exit_is_not_unknown() {
        let root = std::env::temp_dir().join(format!("nr-final-race-{}", run_retention::unique()));
        let (monitor, completion) = monitor_fixture(&root);
        let final_frame = serde_json::json!({
            "schema":1, "event":"Final", "status_raw":17 << 8,
            "owner_lost":false, "cleanup_complete":true, "errors":[]
        });
        let final_record = monitor
            .final_record_with_poll(|| {
                // This synchronous poll boundary runs only after the initial read.
                // The stand-in monitor writes Final and actually exits before its
                // owned wait result becomes visible. No timing assumption is used.
                let status = Command::new("/bin/sh")
                    .args(["-c", "printf '%s\\n' \"$1\" >> \"$2\"", "final-race"])
                    .arg(final_frame.to_string())
                    .arg(root.join("record.jsonl"))
                    .status()?;
                assert!(status.success());
                completion.send(Ok(status)).unwrap();
                monitor.poll_monitor()
            })
            .unwrap()
            .expect("post-exit Final must be observed");
        assert_eq!(final_record.status.code(), Some(17));
        assert!(!final_record.owner_lost);
        assert!(final_record.cleanup_complete);
        assert!(final_record.errors.is_empty());
        assert!(monitor.poll_monitor().unwrap().unwrap().success());
        drop(monitor);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn exited_monitor_still_requires_a_complete_and_valid_final() {
        for (suffix, expected) in [
            ("", "without a complete Final"),
            (
                "{\"schema\":1,\"event\":\"Final\"}",
                "without a complete Final",
            ),
            (
                "{\"schema\":1,\"event\":\"Final\"}\n",
                "monitor Final lacks original status",
            ),
            (
                "{\"schema\":2,\"event\":\"Final\"}\n",
                "unsupported monitor record schema",
            ),
        ] {
            let root =
                std::env::temp_dir().join(format!("nr-final-invalid-{}", run_retention::unique()));
            let (monitor, completion) = monitor_fixture(&root);
            let error = monitor
                .final_record_with_poll(|| {
                    OpenOptions::new()
                        .append(true)
                        .open(root.join("record.jsonl"))?
                        .write_all(suffix.as_bytes())?;
                    completion.send(Ok(ExitStatus::from_raw(0))).unwrap();
                    monitor.poll_monitor()
                })
                .unwrap_err();
            assert!(format!("{error:#}").contains(expected), "{error:#}");
            drop(monitor);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn lifetime_expiry_never_masks_an_incomplete_final_or_cleanup_violation() {
        for (cleanup_complete, owner_lost, errors) in [
            (false, false, serde_json::json!([])),
            (true, false, serde_json::json!(["original cleanup failure"])),
            (true, true, serde_json::json!([])),
        ] {
            let root = std::env::temp_dir().join(format!(
                "nr-final-expiry-cleanup-{}",
                run_retention::unique()
            ));
            let (mut monitor, completion) = monitor_fixture(&root);
            let final_frame = serde_json::json!({
                "schema":1, "event":"Final", "status_raw":0,
                "owner_lost":owner_lost, "cleanup_complete":cleanup_complete,
                "errors":errors, "lifetime_expired":true
            });
            // Reuse the client verdict fixture, not a deliberately broken
            // managed process registered with the outer formal cleanup owner.
            OpenOptions::new()
                .append(true)
                .open(root.join("record.jsonl"))
                .unwrap()
                .write_all(format!("{final_frame}\n").as_bytes())
                .unwrap();
            completion.send(Ok(ExitStatus::from_raw(0))).unwrap();
            let error = monitor
                .wait(Instant::now() + std::time::Duration::from_secs(1))
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("cleanup violation"),
                "{error:#}"
            );
            assert!(!error.is::<PayloadLifetimeExpired>(), "{error:#}");
            drop(monitor);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn post_write_query_error_prevents_another_query_but_preserves_cleanup_control() {
        let path = std::env::temp_dir().join(format!("nr-query-write-{}", run_retention::unique()));
        let mut control = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let mut generation = Some(0);
        let deadline = 987654321u64;
        assert_eq!(
            write_liveness_query(&mut generation, || {
                control.write_all(&unix::control_frame(b'Q', deadline))?;
                Ok(())
            })
            .unwrap(),
            1
        );

        // The second real frame was written. Inject only the caller's failing
        // post-write deadline verdict, without waiting for a scheduler race.
        let error = write_liveness_query(&mut generation, || {
            control.write_all(&unix::control_frame(b'Q', deadline))?;
            bail!("original monitor control completed after original deadline");
        })
        .unwrap_err();
        assert!(format!("{error:#}").contains("completed after original deadline"));
        assert_eq!(generation, None);
        let sent_again = Cell::new(false);
        let error = write_liveness_query(&mut generation, || {
            sent_again.set(true);
            control.write_all(&unix::control_frame(b'Q', deadline))?;
            Ok(())
        })
        .unwrap_err();
        assert!(format!("{error:#}").contains("generation is unusable"));
        assert!(
            !sent_again.get(),
            "an ambiguous generation must never emit another Q or reuse its stale response"
        );

        // Lifecycle commands use the same writer independently of Q state.
        control
            .write_all(&unix::control_frame(b'K', deadline))
            .unwrap();
        control.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        control.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes.len(), 3 * 16);
        let (frames, remainder) = bytes.as_chunks::<16>();
        assert!(remainder.is_empty());
        let operations = frames
            .iter()
            .map(|frame| unix::decode_control(frame).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            operations,
            vec![(b'Q', deadline), (b'Q', deadline), (b'K', deadline)]
        );
        drop(control);
        fs::remove_file(path).unwrap();
    }
}
