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

#[path = "../run_retention.rs"]
mod run_retention;
#[path = "../unix/mod.rs"]
mod unix;

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::FromRawFd;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn inherited(fd: i32) -> io::Result<File> {
    unix::cloexec(fd)?;
    // SAFETY: each reserved descriptor is mapped exactly once by the library
    // child's pre_exec; this program takes its sole process-local owner.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn event(record: &mut File, contents: &str) -> io::Result<()> {
    writeln!(record, "{{\"schema\":1,{contents}}}")?;
    record.flush()
}

fn next_arg(args: &mut impl Iterator<Item = std::ffi::OsString>) -> io::Result<std::ffi::OsString> {
    args.next()
        .ok_or_else(|| unix::error("missing monitor argument"))
}

fn guard(mut args: impl Iterator<Item = std::ffi::OsString>) -> io::Result<()> {
    let root = PathBuf::from(next_arg(&mut args)?);
    let parent: i32 = next_arg(&mut args)?
        .to_str()
        .ok_or_else(|| unix::error("invalid nextest PID"))?
        .parse()
        .map_err(unix::error)?;
    let deadline: u64 = next_arg(&mut args)?
        .to_str()
        .ok_or_else(|| unix::error("invalid bootstrap deadline"))?
        .parse()
        .map_err(unix::error)?;
    let mut control = inherited(3)?;
    let mut ack = inherited(4)?;
    let _pin = inherited(5)?;
    let _lease = inherited(6)?;
    unix::close_unlisted(7)?;
    unix::detach()?;
    let observer = unix::ExitObserver::new(parent)?;
    if observer.exited()? {
        return Err(unix::error(
            "nextest exited before retention acknowledgement",
        ));
    }
    ack.write_all(b"R")?;
    let mut acceptance = [0u8; 16];
    let mut received = 0;
    loop {
        if unix::clock_ns()? >= deadline {
            return Err(unix::error("retention acceptance deadline expired"));
        }
        match control.read(&mut acceptance[received..]) {
            Ok(length) if length > 0 => {
                received += length;
                if received == acceptance.len() {
                    let (operation, accepted_deadline) = unix::decode_control(&acceptance)?;
                    if operation != b'A'
                        || accepted_deadline > deadline
                        || unix::clock_ns()? >= accepted_deadline
                    {
                        return Err(unix::error(
                            "invalid retention acceptance deadline or operation",
                        ));
                    }
                    break;
                }
            }
            Ok(0) => {
                return Err(unix::error(
                    "retention initializer disappeared before acceptance",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
            _ => return Err(unix::error("invalid retention acceptance")),
        }
        if observer.exited()? {
            return Err(unix::error("nextest exited before retention acceptance"));
        }
        std::thread::sleep(unix::POLL);
    }
    if root.join("closing").exists() {
        return Err(unix::error("retention root closed before acceptance"));
    }
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("Ready"))?
        .write_all(b"schema=1\n")?;
    // ACCEPT is irrevocable: after Ready, lost acknowledgement must not kill
    // a lease that another caller could eventually join under the init lock.
    let _ = ack.write_all(b"Y");
    drop(ack);
    drop(control);
    loop {
        if observer.exited()? {
            break;
        }
        std::thread::sleep(unix::POLL);
    }
    run_retention::close_registration(&root, Instant::now() + Duration::from_secs(30))
}

fn payload(mut args: impl Iterator<Item = std::ffi::OsString>) -> io::Result<()> {
    let deadline: u64 = next_arg(&mut args)?
        .to_str()
        .ok_or_else(|| unix::error("invalid payload deadline"))?
        .parse()
        .map_err(unix::error)?;
    let work: u64 = next_arg(&mut args)?
        .to_str()
        .ok_or_else(|| unix::error("invalid payload work deadline"))?
        .parse()
        .map_err(unix::error)?;
    let complete: u64 = next_arg(&mut args)?
        .to_str()
        .ok_or_else(|| unix::error("invalid payload complete deadline"))?
        .parse()
        .map_err(unix::error)?;
    if (work == 0) != (complete == 0) || (work != 0 && work >= complete) {
        return Err(unix::error("invalid payload lifetime clocks"));
    }
    // Reserve part of cleanup for observing the actual group exit after KILL;
    // signaling exactly at the complete clock cannot prove timely settlement.
    let lifetime_kill_at =
        complete.saturating_sub((complete.saturating_sub(work) / 2).min(1_000_000_000));
    let root = PathBuf::from(next_arg(&mut args)?);
    if next_arg(&mut args)? != "--" {
        return Err(unix::error("missing payload argument boundary"));
    }
    let program = next_arg(&mut args)?;
    let mut control = inherited(3)?;
    let mut ack = inherited(4)?;
    let mut record = inherited(5)?;
    let gate = inherited(6)?;
    let _pin = inherited(7)?;
    let stdout = inherited(8)?;
    let stderr = inherited(9)?;
    unix::close_unlisted(10)?;
    unix::detach()?;
    let mut initial = [0u8];
    let alive = match control.read(&mut initial) {
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => true,
        Ok(0) => false,
        Err(error) => return Err(error),
        _ => return Err(unix::error("control message before payload startup")),
    };
    if !alive
        || unix::clock_ns()? >= deadline
        || (work != 0 && unix::clock_ns()? >= work)
        || root.join("closing").exists()
        || root.join("Closed").exists()
    {
        event(
            &mut record,
            "\"event\":\"Final\",\"status_raw\":null,\"startup_error\":\"owner closed or original deadline expired before spawn\",\"owner_lost\":true,\"errors\":[],\"cleanup_complete\":true",
        )?;
        return Err(unix::error("payload startup refused"));
    }
    let expected = std::process::id() as i32;
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .process_group(0);
    // This main thread creates and retains the Child until actual final wait.
    unsafe {
        command.pre_exec(move || unix::payload_parent_guard(expected));
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            event(
                &mut record,
                &format!(
                    "\"event\":\"Final\",\"status_raw\":null,\"startup_error\":{},\"owner_lost\":false,\"errors\":[],\"cleanup_complete\":true",
                    run_retention::quote(&error.to_string())
                ),
            )?;
            return Err(error);
        }
    };
    // After spawn, every error path below still belongs to this exact Child.
    // Record failures are remembered; they never drop the Child obligation.
    let mut failures = Vec::<String>::new();
    if let Err(error) = event(
        &mut record,
        &format!("\"event\":\"Spawned\",\"pid\":{}", child.id()),
    ) {
        failures.push(error.to_string());
    }
    let mut response = vec![b'R'];
    response.extend_from_slice(&child.id().to_le_bytes());
    let acknowledged = ack.write_all(&response).is_ok();
    drop(gate);
    drop(ack);
    let mut owner_lost = !acknowledged;
    let mut stopping: Option<u64> = None;
    let mut graceful_deadline = u64::MAX;
    let mut frame = [0u8; 16];
    let mut received = 0;
    let mut liveness_generation = 0u64;
    let mut killed = false;
    let mut incomplete = false;
    let mut observation_failed = false;
    let mut lifetime_expired = false;
    loop {
        let now = unix::clock_ns().unwrap_or_else(|error| {
            failures.push(error.to_string());
            u64::MAX
        });
        if work != 0 && now >= work && !lifetime_expired {
            lifetime_expired = true;
            if let Err(error) = event(&mut record, "\"event\":\"LifetimeExpired\"") {
                failures.push(error.to_string());
            }
            if stopping.is_none() {
                unix::signal_or_kill_original(
                    &mut child,
                    unix::TERM,
                    &mut failures,
                    "lifetime_expiry_term",
                );
                stopping = Some(now);
                graceful_deadline = now.saturating_add(5_000_000_000).min(lifetime_kill_at);
            }
        }
        if work != 0 && stopping.is_some() {
            graceful_deadline = graceful_deadline.min(lifetime_kill_at);
        }
        let operation = match control.read(&mut frame[received..]) {
            Ok(0) => {
                if received != 0 {
                    failures.push("truncated monitor control frame".into());
                    received = 0;
                }
                owner_lost = true;
                Some((b'S', now.saturating_add(10_000_000_000)))
            }
            Ok(length) => {
                received += length;
                if received == frame.len() {
                    received = 0;
                    match unix::decode_control(&frame) {
                        Ok(value) => Some(value),
                        Err(error) => {
                            failures.push(error.to_string());
                            owner_lost = true;
                            Some((b'S', now.saturating_add(10_000_000_000)))
                        }
                    }
                } else {
                    None
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => None,
            Err(error) => {
                failures.push(error.to_string());
                owner_lost = true;
                Some((b'S', now.saturating_add(10_000_000_000)))
            }
        };
        if let Some((operation, operation_deadline)) = operation {
            match operation {
                b'Q' => {
                    liveness_generation += 1;
                    match unix::observe(&child) {
                        Ok(Some((code, status))) => {
                            if let Err(error) = event(
                                &mut record,
                                &format!(
                                    "\"event\":\"ExitObserved\",\"code\":{code},\"status\":{status}"
                                ),
                            ) {
                                failures.push(error.to_string());
                            }
                            break;
                        }
                        Ok(None) => {
                            if let Err(error) = event(
                                &mut record,
                                &format!(
                                    "\"event\":\"Liveness\",\"generation\":{liveness_generation},\"running\":true"
                                ),
                            ) {
                                failures.push(error.to_string());
                            }
                        }
                        Err(error) => {
                            failures.push(error.to_string());
                            if stopping.is_none() {
                                stopping = Some(now);
                                graceful_deadline = now;
                            }
                        }
                    }
                }
                b'T' | b'I' => {
                    // Signal requests preserve the caller's wait contract.
                    // Only explicit cleanup (or owner loss/error) may start
                    // the monitor's escalation and cleanup budget.
                    let signal = if operation == b'I' {
                        unix::INTERRUPT
                    } else {
                        unix::TERM
                    };
                    let prior_failures = failures.len();
                    unix::signal_or_kill_original(
                        &mut child,
                        signal,
                        &mut failures,
                        if operation == b'I' {
                            "control_interrupt"
                        } else {
                            "control_term"
                        },
                    );
                    if failures.len() != prior_failures && stopping.is_none() {
                        stopping = Some(now);
                        graceful_deadline = now;
                    }
                }
                b'S' | b'K' => {
                    if stopping.is_none() {
                        let signal = match operation {
                            b'K' => unix::KILL,
                            _ => unix::TERM,
                        };
                        unix::signal_or_kill_original(
                            &mut child,
                            signal,
                            &mut failures,
                            if operation == b'K' {
                                "control_kill"
                            } else {
                                "stop_request_term"
                            },
                        );
                        stopping = Some(now);
                        graceful_deadline =
                            now.saturating_add(5_000_000_000).min(operation_deadline);
                        killed = operation == b'K';
                    } else if operation == b'K' && !killed {
                        unix::signal_or_kill_original(
                            &mut child,
                            unix::KILL,
                            &mut failures,
                            "control_kill_after_stop",
                        );
                        killed = true;
                    }
                }
                _ => {
                    failures.push("invalid monitor control operation".into());
                    owner_lost = true;
                    if stopping.is_none() {
                        stopping = Some(now);
                        graceful_deadline = now.saturating_add(5_000_000_000);
                        unix::signal_or_kill_original(
                            &mut child,
                            unix::TERM,
                            &mut failures,
                            "invalid_control_term",
                        );
                    }
                }
            }
        }
        match unix::observe(&child) {
            Ok(Some((code, status))) => {
                if let Err(error) = event(
                    &mut record,
                    &format!("\"event\":\"ExitObserved\",\"code\":{code},\"status\":{status}"),
                ) {
                    failures.push(error.to_string());
                }
                break;
            }
            Ok(None) => {}
            Err(error) => {
                if !observation_failed {
                    failures.push(format!("observe original child: {error}"));
                    observation_failed = true;
                }
                if stopping.is_none() {
                    stopping = Some(now);
                    graceful_deadline = now.saturating_add(5_000_000_000);
                    unix::signal_or_kill_original(
                        &mut child,
                        unix::TERM,
                        &mut failures,
                        "child_observation_error_term",
                    );
                }
            }
        }
        if let Some(started) = stopping {
            if work != 0 && now >= complete && !incomplete {
                let _ = event(
                    &mut record,
                    "\"event\":\"CleanupIncomplete\",\"reason\":\"original complete lifetime deadline expired before child exit\"",
                );
                incomplete = true;
            }
            let elapsed = now.saturating_sub(started);
            if now >= graceful_deadline && !killed {
                unix::signal_or_kill_original(
                    &mut child,
                    unix::KILL,
                    &mut failures,
                    "grace_deadline_kill",
                );
                killed = true;
            }
            if elapsed >= 10_000_000_000 && !incomplete {
                let _ = event(
                    &mut record,
                    "\"event\":\"CleanupIncomplete\",\"reason\":\"original child has not exited within cleanup budget\"",
                );
                incomplete = true;
            }
        }
        std::thread::sleep(unix::POLL);
    }
    // Keep the unreaped leader through the final group signal even when the
    // normal command exited by itself. Never signal this identity afterwards.
    unix::signal_or_kill_original(&mut child, unix::KILL, &mut failures, "final_group_kill");
    let mut group_exit_observed = false;
    let mut group_exit_observed_ns = 0;
    let mut group_observation_failed = false;
    if work != 0 {
        // The unreaped leader pins the PGID through actual group observation.
        // A late or unknown exit publishes incomplete, never cleanup success.
        loop {
            match unix::process_group_running(&child) {
                Ok(false) => {
                    group_exit_observed = true;
                    match unix::clock_ns() {
                        Ok(now) => {
                            group_exit_observed_ns = now;
                            if now >= complete {
                                failures.push(
                                    "original group exit observed after complete deadline".into(),
                                );
                                incomplete = true;
                            }
                        }
                        Err(error) => {
                            failures.push(format!("observe group exit clock: {error}"));
                            incomplete = true;
                        }
                    }
                    break;
                }
                Ok(true) => {
                    let late = match unix::clock_ns() {
                        Ok(now) => now >= complete,
                        Err(error) => {
                            if !group_observation_failed {
                                failures.push(format!("observe group exit clock: {error}"));
                                group_observation_failed = true;
                            }
                            true
                        }
                    };
                    if late && !incomplete {
                        let _ = event(
                            &mut record,
                            "\"event\":\"CleanupIncomplete\",\"reason\":\"original process group remains live at complete deadline\"",
                        );
                        incomplete = true;
                    }
                    std::thread::sleep(unix::POLL);
                }
                Err(error) => {
                    if !group_observation_failed {
                        failures.push(format!("observe original process group exit: {error}"));
                        let _ = event(
                            &mut record,
                            "\"event\":\"CleanupIncomplete\",\"reason\":\"original process group exit is unobserved\"",
                        );
                        group_observation_failed = true;
                    }
                    incomplete = true;
                    std::thread::sleep(unix::POLL);
                }
            }
        }
    }
    let status = child.wait()?;
    let errors = failures
        .iter()
        .map(|v| run_retention::quote(v))
        .collect::<Vec<_>>()
        .join(",");
    let complete = failures.is_empty() && !incomplete;
    event(
        &mut record,
        &format!(
            "\"event\":\"Final\",\"status_raw\":{},\"startup_error\":null,\"owner_lost\":{owner_lost},\"lifetime_expired\":{lifetime_expired},\"group_exit_observed\":{group_exit_observed},\"group_exit_observed_ns\":{group_exit_observed_ns},\"errors\":[{errors}],\"cleanup_complete\":{complete}",
            status.into_raw()
        ),
    )?;
    Ok(())
}

fn main() {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    let guard_root = (arguments.first().and_then(|mode| mode.to_str()) == Some("retention-guard"))
        .then(|| arguments.get(1).map(PathBuf::from))
        .flatten();
    let mut args = arguments.into_iter();
    let result = match args.next().as_deref().and_then(std::ffi::OsStr::to_str) {
        Some("payload-monitor") => payload(args),
        Some("retention-guard") => guard(args),
        _ => Err(unix::error("unknown test monitor mode")),
    };
    if let Err(error) = result {
        if let Some(root) = guard_root {
            if let Ok(mut diagnostic) = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(root.join(".guard-error"))
            {
                let _ = writeln!(diagnostic, "{error}");
            }
        }
        // Standard streams are private null descriptors. Per-launch records
        // carry payload failures; a missing guard Ready/Closed remains incomplete.
        eprintln!("test monitor failed: {error}");
        std::process::exit(1);
    }
}
