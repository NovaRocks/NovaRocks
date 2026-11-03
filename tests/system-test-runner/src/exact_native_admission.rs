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

//! Strict fixture-only admission before the one original FE/BE prelaunch clock.
//! Preparation bounds are harness inputs, never product result/capacity budgets.
use crate::scenarios::exact_mysql_native_driver::AdmittedExactNativeRun;
use anyhow::{Result, ensure};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs::{self, File, Metadata};
use std::io::Read;
use std::os::fd::OwnedFd;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const PREP_MS: u64 = 30_000;
const COMMAND_MS: u64 = 5_000;
const REAP_MS: u64 = 1_000;
const BINDING_BYTES: u64 = 131_072;
const INPUT_BYTES: u64 = 65_536;
const CONFIG_BYTES: u64 = 4_194_304;
const BINARY_BYTES: u64 = 4_294_967_296;
const GIT_STDOUT_BYTES: usize = 8_388_608;
const STDERR_BYTES: usize = 65_536;
const IDENTITY_STDOUT_BYTES: usize = 4096;
const SCRATCH: usize = 65_536;
const LARGE_PATH: &str = "logs/mem-1-m07/exact-original-large-row-native-freeze.draft.json";
const TINY_PATH: &str = "logs/mem-1-m07/exact-new-tiny-row-native-freeze.draft.json";
const LARGE_SHA: &str = "d4bbd8c0cd3c3d647c6a0c948692db381360406584f25e3f6653fc91b73feff2";
const TINY_SHA: &str = "a250875085fd54bc3c35e0c1e7a08173d2f36920bc7dc6ab83ba6dbf2745d127";
pub(crate) const EFFECTIVE_POLICY: &str = "freeze_original_prepared_before_role_spawn";
const IDENTITY_ARGUMENT: &str = "--mem-1-m07-build-identity";
const IDENTITY_PREFIX: &str = novarocks_failpoint::observation_marker::MEM_1_M07_BUILD;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    schema_version: u32,
    kind: String,
    runnable: bool,
    frozen_before_execution: bool,
    clean_revision: String,
    source_tree_sha256: String,
    server_binary_sha256: String,
    server_build_identity: String,
    runner_binary_sha256: String,
    base_config_sha256: String,
    large_input_sha256: String,
    tiny_input_sha256: String,
    prelaunch_effective_config_policy: String,
    preparation: Preparation,
    scene: Scene,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    deadline_ms: u64,
    command_ms: u64,
    reap_ms: u64,
    binding_bytes: u64,
    input_bytes: u64,
    config_bytes: u64,
    binary_bytes: u64,
    git_stdout_bytes: usize,
    stderr_bytes: usize,
    identity_stdout_bytes: usize,
    scratch_bytes: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scene {
    original_prelaunch_ms: u64,
    metadata_ms: u64,
    held_ms: u64,
    kill_return_observation_ms: u64,
    production_write_ms: u64,
    production_closing_ms: u64,
    sample_interval_ms: u64,
    public_sample_positions: usize,
    control_commands: usize,
    control_wire_cap: usize,
    control_reply_max: usize,
    arm: usize,
    held_snapshots: usize,
    resume_snapshots: usize,
    exit_snapshots: usize,
    stop: usize,
    segment_bytes: u64,
    window_positions: usize,
    case_count: usize,
}
impl Binding {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1
                && self.kind == "mem-1-m07-exact-native-admission-v1"
                && self.runnable
                && self.frozen_before_execution,
            "binding is not an explicitly frozen exact execution admission"
        );
        revision(&self.clean_revision)?;
        ensure!(
            self.server_build_identity == self.clean_revision,
            "frozen build identity differs from full clean revision"
        );
        for value in [
            &self.source_tree_sha256,
            &self.server_binary_sha256,
            &self.runner_binary_sha256,
            &self.base_config_sha256,
            &self.large_input_sha256,
            &self.tiny_input_sha256,
        ] {
            hash(value)?;
        }
        ensure!(
            self.large_input_sha256 == LARGE_SHA && self.tiny_input_sha256 == TINY_SHA,
            "binding changed immutable original ten input bytes"
        );
        ensure!(
            self.prelaunch_effective_config_policy == EFFECTIVE_POLICY,
            "binding changed original prepared-effective artifact policy"
        );
        let p = &self.preparation;
        ensure!(
            p.deadline_ms == PREP_MS
                && p.command_ms == COMMAND_MS
                && p.reap_ms == REAP_MS
                && p.binding_bytes == BINDING_BYTES
                && p.input_bytes == INPUT_BYTES
                && p.config_bytes == CONFIG_BYTES
                && p.binary_bytes == BINARY_BYTES
                && p.git_stdout_bytes == GIT_STDOUT_BYTES
                && p.stderr_bytes == STDERR_BYTES
                && p.identity_stdout_bytes == IDENTITY_STDOUT_BYTES
                && p.scratch_bytes == SCRATCH,
            "binding changed closed harness preparation bounds"
        );
        let s = &self.scene;
        ensure!(
            s.original_prelaunch_ms == 20000
                && s.metadata_ms == 5000
                && s.held_ms == 5000
                && s.kill_return_observation_ms == 2000
                && s.production_write_ms == 30000
                && s.production_closing_ms == 5000
                && s.sample_interval_ms == 100
                && s.public_sample_positions == 51
                && s.control_commands == 16
                && s.control_wire_cap == 4096
                && s.control_reply_max == 744
                && s.arm == 1
                && s.held_snapshots == 6
                && s.resume_snapshots == 6
                && s.exit_snapshots == 2
                && s.stop == 1
                && s.segment_bytes == 1048576
                && s.window_positions == 2
                && s.case_count == 10,
            "binding changed original exact matrix clocks, protocol bounds or command reservation"
        );
        Ok(())
    }
}
fn revision(value: &str) -> Result<()> {
    ensure!(
        value.len() == 40
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && value.bytes().any(|byte| byte != b'0'),
        "invalid full canonical revision"
    );
    Ok(())
}
fn hash(value: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && value.bytes().any(|byte| byte != b'0'),
        "invalid canonical SHA256 pin"
    );
    Ok(())
}
fn check(deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "independent immutable preparation clock expired"
    );
    Ok(())
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Stamp {
    dev: u64,
    ino: u64,
    len: u64,
    mode: u32,
    mtime: i64,
    mtime_ns: i64,
    ctime: i64,
    ctime_ns: i64,
}
impl Stamp {
    fn from(metadata: &Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            len: metadata.len(),
            mode: metadata.mode(),
            mtime: metadata.mtime(),
            mtime_ns: metadata.mtime_nsec(),
            ctime: metadata.ctime(),
            ctime_ns: metadata.ctime_nsec(),
        }
    }
}
struct InputOwner {
    path: PathBuf,
    canonical: PathBuf,
    file: File,
    original: Stamp,
}
impl InputOwner {
    fn open(path: &Path, cap: u64, deadline: Instant) -> Result<Self> {
        check(deadline)?;
        let metadata = fs::symlink_metadata(path)?;
        ensure!(
            metadata.is_file()
                && !metadata.file_type().is_symlink()
                && metadata.len() > 0
                && metadata.len() <= cap,
            "admission input is not a bounded original regular file"
        );
        let canonical = path.canonicalize()?;
        let file = File::open(path)?;
        let original = Stamp::from(&file.metadata()?);
        ensure!(
            original == Stamp::from(&metadata),
            "input changed while original file was opened"
        );
        let owner = Self {
            path: path.to_path_buf(),
            canonical,
            file,
            original,
        };
        owner.recheck(deadline)?;
        Ok(owner)
    }
    fn recheck(&self, deadline: Instant) -> Result<()> {
        check(deadline)?;
        let metadata = fs::symlink_metadata(&self.path)?;
        ensure!(
            metadata.is_file()
                && !metadata.file_type().is_symlink()
                && self.path.canonicalize()? == self.canonical
                && Stamp::from(&metadata) == self.original
                && Stamp::from(&self.file.metadata()?) == self.original,
            "original admission file was replaced, truncated or changed"
        );
        check(deadline)
    }
    fn read_small(&mut self, cap: u64, deadline: Instant) -> Result<Vec<u8>> {
        let mut value = Vec::new();
        let mut scratch = [0; SCRATCH];
        loop {
            check(deadline)?;
            let count = self.file.read(&mut scratch)?;
            check(deadline)?;
            ensure!(
                value.len() as u64 + count as u64 <= cap,
                "bounded input grew during read"
            );
            if count == 0 {
                break;
            }
            value.extend_from_slice(&scratch[..count]);
        }
        ensure!(
            value.len() as u64 == self.original.len,
            "original input length changed"
        );
        self.recheck(deadline)?;
        Ok(value)
    }
    fn stream_hash(&mut self, deadline: Instant) -> Result<String> {
        let mut hash = Sha256::new();
        let mut scratch = [0; SCRATCH];
        let mut count = 0u64;
        loop {
            check(deadline)?;
            let n = self.file.read(&mut scratch)?;
            check(deadline)?;
            count = count
                .checked_add(n as u64)
                .ok_or_else(|| anyhow::anyhow!("stream hash counter overflow"))?;
            ensure!(
                count <= self.original.len,
                "original hashed input grew during read"
            );
            if n == 0 {
                break;
            }
            hash.update(&scratch[..n]);
        }
        ensure!(
            count == self.original.len,
            "original hashed input shrank during read"
        );
        self.recheck(deadline)?;
        Ok(format!("{:x}", hash.finalize()))
    }
}

struct Capture {
    bytes: Vec<u8>,
    total: u64,
    cap: usize,
    eof: bool,
}
impl Capture {
    fn new(cap: usize) -> Self {
        Self {
            bytes: Vec::new(),
            total: 0,
            cap,
            eof: false,
        }
    }
    fn poll(&mut self, stream: &mut UnixStream) -> std::io::Result<bool> {
        let mut scratch = [0; 4096];
        for _ in 0..32 {
            // Fixed work per drain turn; try_wait/deadline cannot starve.
            match stream.read(&mut scratch) {
                Ok(0) => {
                    self.eof = true;
                    return Ok(false);
                }
                Ok(n) => {
                    self.total = self
                        .total
                        .checked_add(n as u64)
                        .ok_or_else(|| std::io::Error::other("capture count overflow"))?;
                    if n > self.cap - self.bytes.len() {
                        return Ok(true);
                    }
                    self.bytes.extend_from_slice(&scratch[..n]);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }
}
#[derive(Clone, Copy, Debug)]
enum CommandClass {
    Spawn,
    Read,
    Poll,
    Deadline,
    OutputBound,
    Nonzero,
    Stderr,
    Panic,
}
pub(crate) struct CommandFailure {
    class: CommandClass,
    cause: Option<std::io::Error>,
    cleanup_causes: [Option<std::io::Error>; 2],
    status: Option<ExitStatus>,
    stdout_bytes: u64,
    stderr_bytes: u64,
    // Exactly the original unreaped Child. Error ownership is retained, never a guessed PID.
    retained_child: Option<Mutex<Child>>,
    _panic_payload: Option<Mutex<Box<dyn std::any::Any + Send>>>,
}
impl std::fmt::Debug for CommandFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandFailure")
            .field("class", &self.class)
            .field("status_code", &self.status.and_then(|status| status.code()))
            .field("stdout_bytes", &self.stdout_bytes)
            .field("stderr_bytes", &self.stderr_bytes)
            .field("original_child_reaped", &self.status.is_some())
            .field("original_child_retained", &self.retained_child.is_some())
            .field("original_io_retained", &self.cause.is_some())
            .field(
                "cleanup_io_retained",
                &self.cleanup_causes.each_ref().map(Option::is_some),
            )
            .finish()
    }
}
impl std::fmt::Display for CommandFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for CommandFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_ref()
            .or_else(|| self.cleanup_causes.iter().flatten().next())
            .map(|error| error as &dyn std::error::Error)
    }
}
struct OriginalChild {
    child: Option<Child>,
    status: Option<ExitStatus>,
}
impl OriginalChild {
    fn poll(&mut self) -> std::io::Result<()> {
        if self.status.is_none()
            && let Some(child) = self.child.as_mut()
            && let Some(status) = child.try_wait()?
        {
            self.status = Some(status);
            drop(self.child.take());
        }
        Ok(())
    }
    fn abort_and_reap(&mut self) -> [Option<std::io::Error>; 2] {
        let mut errors = [None, None];
        if let Some(child) = self.child.as_mut()
            && let Err(cause) = child.kill()
        {
            errors[0] = Some(cause);
        }
        let deadline = Instant::now() + Duration::from_millis(REAP_MS); // Failure-only cleanup, never admission/scene time.
        loop {
            if let Err(cause) = self.poll() {
                errors[1] = Some(cause);
                break;
            }
            if self.child.is_none() || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(
                Duration::from_millis(2).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
        errors
    }
}
fn bounded_command(
    mut command: Command,
    stdout_cap: usize,
    preparation: Instant,
) -> Result<Vec<u8>> {
    // Setup and spawn consume the same command budget as capture and reap.
    let command_deadline = preparation.min(Instant::now() + Duration::from_millis(COMMAND_MS));
    check(preparation)?;
    let (mut stdout, child_stdout) = UnixStream::pair()?;
    let (mut stderr, child_stderr) = UnixStream::pair()?;
    stdout.set_nonblocking(true)?;
    stderr.set_nonblocking(true)?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(File::from(OwnedFd::from(child_stdout))))
        .stderr(Stdio::from(File::from(OwnedFd::from(child_stderr))));
    let mut out = Capture::new(stdout_cap);
    let mut err = Capture::new(STDERR_BYTES);
    check(command_deadline)?;
    let child = match command.spawn() {
        Ok(child) => child,
        Err(cause) => {
            return Err(CommandFailure {
                class: CommandClass::Spawn,
                cause: Some(cause),
                cleanup_causes: [None, None],
                status: None,
                stdout_bytes: 0,
                stderr_bytes: 0,
                retained_child: None,
                _panic_payload: None,
            }
            .into());
        }
    };
    // Command itself must release the parent copies of stdout/stderr after spawn.
    drop(command);
    let mut owner = OriginalChild {
        child: Some(child),
        status: None,
    };
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        loop {
            if Instant::now() >= command_deadline {
                return Err((CommandClass::Deadline, None));
            }
            match out.poll(&mut stdout) {
                Ok(true) => return Err((CommandClass::OutputBound, None)),
                Err(io) => return Err((CommandClass::Read, Some(io))),
                _ => {}
            }
            match err.poll(&mut stderr) {
                Ok(true) => return Err((CommandClass::OutputBound, None)),
                Err(io) => return Err((CommandClass::Read, Some(io))),
                _ => {}
            }
            if let Err(io) = owner.poll() {
                return Err((CommandClass::Poll, Some(io)));
            }
            if let Some(status) = owner.status
                && out.eof
                && err.eof
            {
                if !status.success() {
                    return Err((CommandClass::Nonzero, None));
                }
                if err.total != 0 {
                    return Err((CommandClass::Stderr, None));
                }
                if Instant::now() >= command_deadline {
                    return Err((CommandClass::Deadline, None));
                }
                return Ok(());
            }
            std::thread::sleep(
                Duration::from_millis(2)
                    .min(command_deadline.saturating_duration_since(Instant::now())),
            );
        }
    }));
    let (class, cause, panic_payload) = match run {
        Ok(Ok(())) => {
            check(preparation)?;
            return Ok(out.bytes);
        }
        Ok(Err((class, cause))) => (class, cause, None),
        Err(payload) => (CommandClass::Panic, None, Some(Mutex::new(payload))),
    };
    let cleanup_causes = owner.abort_and_reap();
    Err(CommandFailure {
        class,
        cause,
        cleanup_causes,
        status: owner.status,
        stdout_bytes: out.total,
        stderr_bytes: err.total,
        retained_child: owner.child.take().map(Mutex::new),
        _panic_payload: panic_payload,
    }
    .into())
}
fn git(repository: &Path, args: &[&str], deadline: Instant) -> Result<Vec<u8>> {
    let mut command = Command::new("/usr/bin/git");
    command
        .env_clear()
        .env("LC_ALL", "C")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .arg("--no-pager")
        .arg("-c")
        .arg("core.fsmonitor=false")
        .arg("-c")
        .arg("core.hooksPath=/dev/null")
        .args(args)
        .current_dir(repository);
    bounded_command(command, GIT_STDOUT_BYTES, deadline)
}
#[derive(Debug, PartialEq, Eq)]
struct Source {
    revision: String,
    tree: String,
}
fn source(repository: &Path, deadline: Instant) -> Result<Source> {
    let revision_bytes = git(repository, &["rev-parse", "HEAD"], deadline)?;
    let revision_text = std::str::from_utf8(&revision_bytes)?.trim();
    revision(revision_text)?;
    let status = git(repository, &["status", "--porcelain=v1"], deadline)?;
    let status_text = std::str::from_utf8(&status)?.trim();
    ensure!(
        status_text.is_empty(),
        "exact execution admission rejects dirty source"
    );
    let mut hasher = Sha256::new();
    for part in [revision_text.as_bytes(), status_text.as_bytes()] {
        hasher.update(part);
        hasher.update([0]);
    }
    for args in [
        &["ls-files", "-s"][..],
        &["diff", "--binary", "HEAD", "--no-ext-diff", "--no-textconv"][..],
        &[
            "diff",
            "--binary",
            "--cached",
            "--no-ext-diff",
            "--no-textconv",
        ][..],
    ] {
        let bytes = git(repository, args, deadline)?;
        hasher.update(&bytes);
        hasher.update([0]);
    }
    check(deadline)?;
    Ok(Source {
        revision: revision_text.into(),
        tree: format!("{:x}", hasher.finalize()),
    })
}
fn require_compiled_runner(commit: &str, build: &str, expected: &str) -> Result<()> {
    revision(expected)?;
    ensure!(
        commit == expected && build == expected,
        "actual runner compiled commit/build differs from frozen clean source"
    );
    Ok(())
}
fn server_identity(bytes: &[u8], expected: &str) -> Result<String> {
    revision(expected)?;
    let text = std::str::from_utf8(bytes)?;
    let required = format!(
        "{IDENTITY_PREFIX} commit={expected} build_identity={expected} exact_mysql_write=true\n"
    );
    ensure!(
        text == required,
        "actual diagnostic is not the exact feature/full-clean-build identity record"
    );
    Ok(expected.into())
}

/// Complete prelaunch admission; no role exists and no scene20 clock is captured here.
/// A failure that retains an unreaped command Child is a hard retained-owner failure.
pub(crate) fn admit(
    binding_path: &Path,
    server: &Path,
    base_config: &Path,
    build: &crate::build_information::BuildInformation,
) -> Result<AdmittedExactNativeRun> {
    admit_until(
        binding_path,
        server,
        base_config,
        build,
        Instant::now() + Duration::from_millis(PREP_MS),
    )
}
// The held gate shares this one preparation clock. Cleanup-only reap does not admit late success.
fn admit_until(
    binding_path: &Path,
    server: &Path,
    base_config: &Path,
    build: &crate::build_information::BuildInformation,
    deadline: Instant,
) -> Result<AdmittedExactNativeRun> {
    check(deadline)?;
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .ok_or_else(|| anyhow::anyhow!("runner workspace owner is unavailable"))?
        .canonicalize()?;
    let mut binding_owner = InputOwner::open(binding_path, BINDING_BYTES, deadline)?;
    let binding_bytes = binding_owner.read_small(BINDING_BYTES, deadline)?;
    let binding: Binding = serde_json::from_slice(&binding_bytes)?;
    binding.validate()?;
    let dot_git = fs::symlink_metadata(repository.join(".git"))?;
    ensure!(
        !dot_git.file_type().is_symlink(),
        "source Git owner locator is a symlink"
    );
    let dot_git_stamp = Stamp::from(&dot_git);
    let before = source(&repository, deadline)?;
    ensure!(
        before.revision == binding.clean_revision && before.tree == binding.source_tree_sha256,
        "actual clean source differs from frozen admission"
    );
    let mut server_owner = InputOwner::open(server, BINARY_BYTES, deadline)?;
    let mut runner_owner = InputOwner::open(&std::env::current_exe()?, BINARY_BYTES, deadline)?;
    let mut config_owner = InputOwner::open(base_config, CONFIG_BYTES, deadline)?;
    let mut large_owner = InputOwner::open(&repository.join(LARGE_PATH), INPUT_BYTES, deadline)?;
    let mut tiny_owner = InputOwner::open(&repository.join(TINY_PATH), INPUT_BYTES, deadline)?;
    let server_hash = server_owner.stream_hash(deadline)?;
    let runner_hash = runner_owner.stream_hash(deadline)?;
    let config_hash = config_owner.stream_hash(deadline)?;
    let large_hash = large_owner.stream_hash(deadline)?;
    let tiny_hash = tiny_owner.stream_hash(deadline)?;
    ensure!(
        server_hash == binding.server_binary_sha256
            && runner_hash == binding.runner_binary_sha256
            && config_hash == binding.base_config_sha256
            && large_hash == LARGE_SHA
            && tiny_hash == TINY_SHA,
        "actual original binary/config/input differs from frozen admission"
    );
    require_compiled_runner(
        build.source_commit,
        build.native_build_identity,
        &before.revision,
    )?;
    server_owner.recheck(deadline)?;
    let mut command = Command::new(&server_owner.canonical);
    command.env_clear().arg(IDENTITY_ARGUMENT);
    let build = server_identity(
        &bounded_command(command, IDENTITY_STDOUT_BYTES, deadline)?,
        &before.revision,
    )?;
    let after = source(&repository, deadline)?;
    ensure!(
        before == after
            && Stamp::from(&fs::symlink_metadata(repository.join(".git"))?) == dot_git_stamp,
        "actual source/Git owner changed during admission"
    );
    for owner in [
        &binding_owner,
        &server_owner,
        &runner_owner,
        &config_owner,
        &large_owner,
        &tiny_owner,
    ] {
        owner.recheck(deadline)?;
    }
    check(deadline)?;
    Ok(AdmittedExactNativeRun {
        clean_revision: before.revision,
        source_tree_sha256: before.tree,
        server_binary_sha256: server_hash,
        server_build_identity: build,
        runner_binary_sha256: runner_hash,
        base_config_sha256: config_hash,
        frozen_execution_binding_sha256: digest(&binding_bytes),
        large_input_sha256: large_hash,
        tiny_input_sha256: tiny_hash,
    })
}

#[cfg(test)]
#[path = "exact_native_admission_tests.rs"]
mod tests;

#[path = "held_native_admission.rs"]
pub(crate) mod held_native_admission;

#[path = "hms_native_admission.rs"]
pub(crate) mod hms_native_admission;
