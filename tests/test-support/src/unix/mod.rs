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

//! The same std-only Unix primitives are compiled into the library and monitor.
#![allow(dead_code)]

use std::ffi::{CString, c_void};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::process::Child;
use std::time::{Duration, Instant};
mod process_group;
pub fn process_group_running(child: &Child) -> io::Result<bool> {
    process_group::running(child)
}

#[cfg(not(all(
    target_pointer_width = "64",
    any(
        target_os = "macos",
        all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )
    )
)))]
compile_error!("test monitors require native 64-bit macOS or Linux x86_64/aarch64");

pub const TERM: i32 = 15;
pub const KILL: i32 = 9;
pub const INTERRUPT: i32 = 2;
pub const POLL: Duration = Duration::from_millis(10);
#[cfg(target_os = "macos")]
pub const NONBLOCK: i32 = 0x4;
#[cfg(target_os = "linux")]
pub const NONBLOCK: i32 = 0x800;
#[cfg(target_os = "macos")]
pub const CLOCK_KIND: u8 = 2;
#[cfg(target_os = "linux")]
pub const CLOCK_KIND: u8 = 1;

/// Fixed 16-byte v1 frames stay below POSIX PIPE_BUF. No argv or paths enter
/// this protocol, and the native monotonic clock is explicit.
pub fn control_frame(operation: u8, deadline: u64) -> [u8; 16] {
    let mut frame = [0u8; 16];
    frame[..5].copy_from_slice(&[b'N', b'M', 1, operation, CLOCK_KIND]);
    frame[8..].copy_from_slice(&deadline.to_le_bytes());
    frame
}

pub fn decode_control(frame: &[u8]) -> io::Result<(u8, u64)> {
    if frame.len() != 16
        || frame[..3] != [b'N', b'M', 1]
        || frame[4] != CLOCK_KIND
        || frame[5..8] != [0, 0, 0]
    {
        return Err(error(
            "invalid monitor control version, length or native clock",
        ));
    }
    Ok((frame[3], u64::from_le_bytes(frame[8..].try_into().unwrap())))
}

unsafe extern "C" {
    fn kill(pid: i32, signal: i32) -> i32;
    fn setsid() -> i32;
    fn getppid() -> i32;
    fn fcntl(fd: i32, command: i32, ...) -> i32;
    fn dup2(old: i32, new: i32) -> i32;
    fn close(fd: i32) -> i32;
    fn mkfifo(path: *const i8, mode: u32) -> i32;
    fn clock_gettime(clock: i32, value: *mut Timespec) -> i32;
    fn waitid(kind: i32, pid: u32, info: *mut Siginfo, options: i32) -> i32;
    fn fstatfs(fd: i32, buffer: *mut c_void) -> i32;
}

pub fn error(message: impl std::fmt::Display) -> io::Error {
    io::Error::other(message.to_string())
}

fn checked(value: i32) -> io::Result<i32> {
    if value < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

pub fn parent_id() -> i32 {
    // SAFETY: getppid has no memory arguments.
    unsafe { getppid() }
}

pub fn detach() -> io::Result<()> {
    // SAFETY: called in the monitor's own process before any payload exists.
    checked(unsafe { setsid() }).map(|_| ())
}

pub fn cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: FD_CLOEXEC is per descriptor; this is never cleared in a parent.
    checked(unsafe { fcntl(fd, 2, 1) }).map(|_| ())
}

pub fn high_copy(file: &File) -> io::Result<File> {
    #[cfg(target_os = "macos")]
    const DUP_CLOEXEC: i32 = 67;
    #[cfg(target_os = "linux")]
    const DUP_CLOEXEC: i32 = 1030;
    // SAFETY: atomically duplicate above the monitor's fixed descriptor range.
    let fd = checked(unsafe { fcntl(file.as_raw_fd(), DUP_CLOEXEC, 64) })?;
    // SAFETY: successful fcntl returned a new owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

/// Only called in the target child's pre_exec closure with precomputed numbers.
pub unsafe fn map_fd(source: RawFd, target: RawFd) -> io::Result<()> {
    // SAFETY: caller owns source and target is reserved in this child only.
    checked(unsafe { dup2(source, target) }).map(|_| ())
}

pub fn close_unlisted(first: RawFd) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    let directory = "/proc/self/fd";
    #[cfg(target_os = "macos")]
    let directory = "/dev/fd";
    // This runs after exec, so directory enumeration and allocation are safe.
    let descriptors = fs::read_dir(directory)?
        .map(|entry| {
            entry.and_then(|e| {
                e.file_name()
                    .to_string_lossy()
                    .parse::<i32>()
                    .map_err(error)
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    for fd in descriptors.into_iter().filter(|fd| *fd >= first) {
        // SAFETY: these are this process's inherited surplus descriptors. The
        // enumeration descriptor is already closed; EBADF is harmless here.
        unsafe {
            close(fd);
        }
    }
    Ok(())
}

pub fn fifo_pair(path: &Path) -> io::Result<(File, File)> {
    let name = CString::new(path.as_os_str().as_bytes()).map_err(error)?;
    // SAFETY: valid terminated pathname, private parent directory.
    checked(unsafe { mkfifo(name.as_ptr(), 0o600) })?;
    // std OpenOptions atomically includes O_CLOEXEC on both supported targets.
    let read = OpenOptions::new()
        .read(true)
        .custom_flags(NONBLOCK)
        .open(path)?;
    let write = OpenOptions::new()
        .write(true)
        .custom_flags(NONBLOCK)
        .open(path)?;
    fs::remove_file(path)?;
    Ok((read, write))
}

#[repr(C)]
struct Timespec {
    seconds: i64,
    nanos: i64,
}

pub fn clock_ns() -> io::Result<u64> {
    #[cfg(target_os = "macos")]
    const CLOCK: i32 = 8; // CLOCK_UPTIME_RAW, as used by this toolchain's Instant.
    #[cfg(target_os = "linux")]
    const CLOCK: i32 = 1; // CLOCK_MONOTONIC, excluding suspend.
    let mut value = Timespec {
        seconds: 0,
        nanos: 0,
    };
    // SAFETY: value has the native 64-bit timespec layout.
    checked(unsafe { clock_gettime(CLOCK, &mut value) })?;
    if value.seconds < 0 || !(0..1_000_000_000).contains(&value.nanos) {
        return Err(error("invalid monotonic clock result"));
    }
    (value.seconds as u64)
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(value.nanos as u64))
        .ok_or_else(|| error("monotonic clock overflow"))
}

pub fn absolute_deadline(deadline: Instant) -> io::Result<u64> {
    // Read OS clock first: conversion can shorten, but never extend the budget.
    let clock = clock_ns()?;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| error("original deadline expired before monitor spawn"))?;
    clock
        .checked_add(u64::try_from(remaining.as_nanos()).map_err(error)?)
        .ok_or_else(|| error("original deadline overflow"))
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct Siginfo {
    signal: i32,
    error: i32,
    code: i32,
    pid: i32,
    uid: u32,
    status: i32,
    address: usize,
    remaining: [usize; 9],
}
#[cfg(target_os = "linux")]
#[repr(C)]
struct Siginfo {
    signal: i32,
    error: i32,
    code: i32,
    alignment: [usize; 0],
    pid: i32,
    uid: u32,
    status: i32,
    remaining: [u8; 100],
}

pub fn observe(child: &Child) -> io::Result<Option<(i32, i32)>> {
    // SAFETY: every field is an integer or byte; all-zero is valid output storage.
    let mut info: Siginfo = unsafe { std::mem::zeroed() };
    #[cfg(target_os = "macos")]
    const NOWAIT: i32 = 0x20;
    #[cfg(target_os = "linux")]
    const NOWAIT: i32 = 0x01000000;
    // SAFETY: this Child remains owned and unreaped; P_PID/WEXITED/WNOHANG.
    checked(unsafe { waitid(1, child.id(), &mut info, 4 | 1 | NOWAIT) })?;
    if info.pid == 0 {
        return Ok(None);
    }
    if !matches!(info.code, 1..=3) {
        return Err(error("unexpected waitid child status"));
    }
    Ok(Some((info.code, info.status)))
}

enum SignalChildObservation {
    NotRequested,
    NotExited,
    Exited { code: i32, status: i32 },
    Failed { errno: Option<i32> },
}

struct GroupSignalObservation {
    raw_group_errno: Option<i32>,
    child: SignalChildObservation,
}

impl GroupSignalObservation {
    fn diagnostic(&self) -> String {
        let child = match &self.child {
            SignalChildObservation::NotRequested => "not_requested".to_owned(),
            SignalChildObservation::NotExited => "not_exited".to_owned(),
            SignalChildObservation::Exited { code, status } => {
                format!("exited(code={code},status={status})")
            }
            SignalChildObservation::Failed { errno } => format!("failed(errno={errno:?})"),
        };
        format!(
            "raw_group_errno={:?}; pinned_child_observation={child}; child_observation_order=after_group_refusal_before_fallback; group_exit_observation=later_Final",
            self.raw_group_errno
        )
    }
}

fn refused_group_signal(
    child: &Child,
    cause: io::Error,
    observation: &mut Option<GroupSignalObservation>,
) -> io::Result<()> {
    let facts = observation.insert(GroupSignalObservation {
        raw_group_errno: cause.raw_os_error(),
        child: SignalChildObservation::NotRequested,
    });
    if cause.raw_os_error() == Some(3) {
        return Ok(());
    }
    // Preserve the existing Darwin EPERM decision and its one original
    // unreaped-Child observation. Diagnostics never observe the child again.
    if cause.raw_os_error() == Some(1) {
        match observe(child) {
            Ok(Some((code, status))) => {
                facts.child = SignalChildObservation::Exited { code, status };
                return Ok(());
            }
            Ok(None) => facts.child = SignalChildObservation::NotExited,
            Err(error) => {
                facts.child = SignalChildObservation::Failed {
                    errno: error.raw_os_error(),
                };
                return Err(error);
            }
        }
    }
    Err(cause)
}

fn signal_group(
    child: &Child,
    signal: i32,
    observation: &mut Option<GroupSignalObservation>,
) -> io::Result<()> {
    let pid = i32::try_from(child.id()).map_err(error)?;
    // SAFETY: caller still owns unreaped group leader; never called after wait.
    if unsafe { kill(-pid, signal) } == 0 {
        return Ok(());
    }
    refused_group_signal(child, io::Error::last_os_error(), observation)
}

pub fn signal_or_kill_original(
    child: &mut Child,
    signal: i32,
    failures: &mut Vec<String>,
    stage: &str,
) {
    signal_or_kill_with(child, signal, failures, stage, signal_group);
}

fn signal_or_kill_with(
    child: &mut Child,
    signal: i32,
    failures: &mut Vec<String>,
    stage: &str,
    group: impl FnOnce(&Child, i32, &mut Option<GroupSignalObservation>) -> io::Result<()>,
) {
    let mut observation = None;
    if let Err(error) = group(child, signal, &mut observation) {
        let context = observation.as_ref().map(GroupSignalObservation::diagnostic);
        // Keep the original refusal and fallback order. This is context in
        // the existing Final.errors string, not a new monitor protocol event.
        failures.push(format!(
            "original process group refused signal {signal}: {error}; signal_stage={stage}; signal_observation={context:?}"
        ));
        // A failure to signal the group cannot waive the independently owned
        // direct Child obligation, or be turned into cleanup success.
        if let Err(error) = child.kill() {
            failures.push(format!(
                "kill actual original child after group refusal: {error}"
            ));
        }
    }
}

pub fn payload_parent_guard(expected: i32) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        unsafe extern "C" {
            fn prctl(option: i32, ...) -> i32;
        }
        // SAFETY: invoked in the monitor main thread's payload pre_exec. The
        // parent thread must live until actual wait completes (PDEATHSIG ABI).
        checked(unsafe { prctl(1, KILL, 0usize, 0usize, 0usize) })?;
    }
    if parent_id() != expected {
        // Keep this post-fork failure path allocation-free as well.
        return Err(io::Error::from_raw_os_error(3));
    }
    Ok(())
}

pub fn validate_local_fs(root: &File) -> io::Result<()> {
    // Native fstatfs writes at most 2168 bytes on Darwin and 120 on the two
    // supported Linux ABIs. Overaligned opaque storage avoids exposing an
    // invented cross-platform statfs struct; only documented native fields are read.
    let mut buffer = [0u64; 512];
    // SAFETY: storage is writable, 8-byte aligned and larger than native statfs.
    checked(unsafe { fstatfs(root.as_raw_fd(), buffer.as_mut_ptr().cast()) })?;
    #[cfg(target_os = "linux")]
    let valid = matches!(buffer[0], 0xef53 | 0x58465342 | 0x9123683e | 0x01021994);
    #[cfg(target_os = "macos")]
    let valid = {
        // Darwin statfs: f_flags at 64, f_fstypename[16] at 72 (<sys/mount.h>).
        let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), 4096) };
        let flags = u32::from_ne_bytes(bytes[64..68].try_into().unwrap());
        flags & 0x1000 != 0 && bytes[72..88].starts_with(b"apfs\0")
    };
    if valid {
        Ok(())
    } else {
        Err(error(
            "test run state requires a supported local filesystem (APFS, ext, XFS, Btrfs or tmpfs); containers must bind-mount local state",
        ))
    }
}

#[cfg(target_os = "linux")]
pub struct ExitObserver(File);
#[cfg(target_os = "linux")]
impl ExitObserver {
    pub fn new(pid: i32) -> io::Result<Self> {
        unsafe extern "C" {
            fn syscall(number: i64, ...) -> i64;
        }
        // SAFETY: pidfd_open syscall number 434 on native x86_64 and aarch64;
        // flags zero creates a kernel process handle with CLOEXEC.
        let fd = unsafe { syscall(434, pid, 0u32) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(unsafe { File::from_raw_fd(fd as i32) }))
    }
    pub fn exited(&self) -> io::Result<bool> {
        #[repr(C)]
        struct PollFd {
            fd: i32,
            events: i16,
            returned: i16,
        }
        unsafe extern "C" {
            fn poll(fds: *mut PollFd, count: usize, timeout: i32) -> i32;
        }
        let mut fd = PollFd {
            fd: self.0.as_raw_fd(),
            events: 1,
            returned: 0,
        };
        checked(unsafe { poll(&mut fd, 1, 0) })?;
        if fd.returned & (8 | 32) != 0 {
            return Err(error("pidfd exit observation failed"));
        }
        Ok(fd.returned & (1 | 16) != 0)
    }
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct Kevent {
    ident: usize,
    filter: i16,
    flags: u16,
    fflags: u32,
    data: isize,
    user: usize,
}
#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn kqueue() -> i32;
    fn kevent(
        fd: i32,
        changes: *const Kevent,
        count: i32,
        events: *mut Kevent,
        capacity: i32,
        timeout: *const Timespec,
    ) -> i32;
    fn proc_pidpath(pid: i32, buffer: *mut c_void, size: u32) -> i32;
}
#[cfg(target_os = "macos")]
pub struct ExitObserver(File);
#[cfg(target_os = "macos")]
impl ExitObserver {
    pub fn new(pid: i32) -> io::Result<Self> {
        let fd = checked(unsafe { kqueue() })?;
        let file = unsafe { File::from_raw_fd(fd) };
        cloexec(fd)?;
        let event = Kevent {
            ident: pid as usize,
            filter: -5,
            flags: 1 | 4 | 0x10,
            fflags: 0x80000000,
            data: 0,
            user: 0,
        };
        checked(unsafe { kevent(fd, &event, 1, std::ptr::null_mut(), 0, std::ptr::null()) })?;
        Ok(Self(file))
    }
    pub fn exited(&self) -> io::Result<bool> {
        let mut event = Kevent {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            user: 0,
        };
        let timeout = Timespec {
            seconds: 0,
            nanos: 0,
        };
        let count = checked(unsafe {
            kevent(
                self.0.as_raw_fd(),
                std::ptr::null(),
                0,
                &mut event,
                1,
                &timeout,
            )
        })?;
        if count == 0 {
            return Ok(false);
        }
        if event.flags & 0x4000 != 0 {
            return Err(error(format!(
                "kqueue exit observation error {}",
                event.data
            )));
        }
        Ok(event.fflags & 0x80000000 != 0)
    }
}

pub fn verify_nextest_parent(expected: i32) -> io::Result<()> {
    if parent_id() != expected {
        return Err(error(
            "nextest direct parent changed during retention bootstrap",
        ));
    }
    #[cfg(target_os = "linux")]
    let path = fs::read_link(format!("/proc/{expected}/exe"))?;
    #[cfg(target_os = "macos")]
    let path = {
        let mut bytes = [0u8; 4096];
        let length = checked(unsafe {
            proc_pidpath(expected, bytes.as_mut_ptr().cast(), bytes.len() as u32)
        })?;
        if length == 0 {
            return Err(error("cannot identify nextest direct parent"));
        }
        let end = bytes.iter().position(|v| *v == 0).unwrap_or(bytes.len());
        std::path::PathBuf::from(std::ffi::OsStr::from_bytes(&bytes[..end]))
    };
    if path.file_name().is_none_or(|name| name != "cargo-nextest") {
        return Err(error(
            "first managed operation must be in a direct cargo-nextest test child; outer tests must ensure run retention before spawning nested runners",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::{CommandExt, ExitStatusExt};

    #[test]
    fn group_refusal_retains_failure_and_reaps_actual_original_child() {
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let mut failures = vec![];
        let raw_refusal = io::Error::from_raw_os_error(1).to_string();
        signal_or_kill_with(
            &mut child,
            TERM,
            &mut failures,
            "test_group_refusal",
            |_, _, _| Err(io::Error::from_raw_os_error(1)),
        );
        let status = child.wait().unwrap();
        assert_eq!(status.signal(), Some(KILL));
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("refused signal"));
        assert!(failures[0].contains(&raw_refusal));
        assert!(failures[0].contains("signal_stage=test_group_refusal"));
        assert!(failures[0].contains("signal_observation=None"));
    }

    #[test]
    fn control_frames_reject_another_clock_and_keep_absolute_deadline() {
        let original = 987654321u64;
        let mut frame = control_frame(b'I', original);
        assert_eq!(decode_control(&frame).unwrap(), (b'I', original));
        frame[4] ^= 3;
        assert!(decode_control(&frame).is_err());
        assert!(decode_control(&frame[..15]).is_err());
    }
}
