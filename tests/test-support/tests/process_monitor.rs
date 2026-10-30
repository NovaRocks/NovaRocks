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

//! OS-observed lifecycle correctness, without a NovaRocks runtime or fixture.
//! The test-support owner runs this target when process helpers change. Raw
//! helper children are owned and actually waited by the outer test; only the
//! managed payload is owned by its independent monitor.
use novarocks_test_support::{ManagedCommand, ManagedProcess, ReadyMarker, capture_command_until};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "novarocks-process-monitor-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

fn command(script: &str) -> ManagedCommand {
    let mut command = ManagedCommand::new("/bin/sh");
    command.args(["-c", script]);
    command
}
fn service(directory: &Directory, script: &str) -> ManagedProcess {
    ManagedProcess::spawn(
        "monitor fixture".into(),
        command(script),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(3),
        directory.0.join("process.log"),
    )
    .unwrap()
}
const SERVICE: &str = "trap 'exit 0' TERM; printf 'READY\\n'; while :; do sleep 1; done";

#[test]
fn normal_stop_preserves_payload_status_and_pins_cloned_output() {
    let directory = Directory::new();
    let mut process = service(&directory, SERVICE);
    let root = process.diagnostic_root().to_owned();
    let log = process.stdout_log_source();
    let pid = process.pid();
    assert!(pid > 0 && process.is_running().unwrap());
    process.request_termination().unwrap();
    let status = process
        .wait_for_successful_exit_until(Instant::now() + Duration::from_secs(3))
        .unwrap();
    assert!(status.success());
    process.stop().unwrap();
    process.release_diagnostics().unwrap();
    drop(process);
    assert!(root.exists(), "a clone still owns the output material");
    assert!(
        log.read_tail(64)
            .unwrap()
            .bytes
            .windows(5)
            .any(|v| v == b"READY")
    );
    // This target runs through both ordinary native invocations and formal
    // wrappers. Release deletes ordinary diagnostics only after the last pin;
    // formal evidence remains owned by the wrapper in either case.
    let formal = root.ancestors().any(|path| path.join(".formal").is_file());
    drop(log);
    assert_eq!(
        root.exists(),
        formal,
        "the last output pin releases ordinary material and retains formal evidence"
    );
}

#[test]
fn stderr_marker_cannot_establish_stdout_readiness() {
    let directory = Directory::new();
    let error = ManagedProcess::spawn(
        "stderr readiness".into(),
        command("printf 'READY\\n' >&2; exit 0"),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(2),
        directory.0.join("locator"),
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("exited before readiness"));
}

#[test]
fn one_shot_accepts_fast_stdout_marker_but_preserves_nonzero_status() {
    let directory = Directory::new();
    ManagedProcess::run_to_completion(
        "one-shot".into(),
        command("printf 'READY\\n'; exit 0"),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(2),
        directory.0.join("success"),
    )
    .unwrap();
    let error = ManagedProcess::run_to_completion(
        "one-shot".into(),
        command("printf 'READY\\n'; exit 7"),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(2),
        directory.0.join("failure"),
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("did not exit successfully"));
}

#[test]
fn captured_command_keeps_complete_large_output_and_actual_exit() {
    let directory = Directory::new();
    let stdout = File::create(directory.0.join("stdout")).unwrap();
    let stderr = File::create(directory.0.join("stderr")).unwrap();
    let result = capture_command_until(command("i=0; while [ $i -lt 2000 ]; do printf 'large golden output line\\n'; i=$((i+1)); done; printf 'diagnostic\\n' >&2; exit 7"), stdout, stderr, Instant::now() + Duration::from_secs(4)).unwrap();
    assert_eq!(result.output.status.code(), Some(7));
    assert!(!result.timed_out && result.failures.is_empty());
    assert_eq!(
        result.output.stdout,
        b"large golden output line\n".repeat(2000)
    );
    assert_eq!(result.output.stderr, b"diagnostic\n");
    assert_eq!(
        fs::read(directory.0.join("stdout")).unwrap(),
        result.output.stdout
    );
}

#[test]
fn expired_capture_never_spawns() {
    let directory = Directory::new();
    let mut request = ManagedCommand::new("/bin/sh");
    request
        .args(["-c", "touch \"$1\"", "sh"])
        .arg(directory.0.join("spawned"));
    assert!(
        capture_command_until(
            request,
            File::create(directory.0.join("out")).unwrap(),
            File::create(directory.0.join("err")).unwrap(),
            Instant::now() - Duration::from_secs(1)
        )
        .is_err()
    );
    assert!(!directory.0.join("spawned").exists());
}

#[test]
fn output_identity_rejects_replacement_and_tail_is_bounded() {
    let directory = Directory::new();
    let mut process = service(&directory, SERVICE);
    let source = process.stdout_log_source();
    assert!(source.read_tail(3).unwrap().bytes.len() <= 3);
    let identity = source.original_durable_log_identity().unwrap();
    assert!(identity.1 != 0);
    fs::rename(source.path(), source.path().with_file_name("old-stdout")).unwrap();
    fs::write(source.path(), b"READY fake\n").unwrap();
    assert!(source.read_tail(64).is_err());
    process.stop().unwrap();
}

struct Helper(Child);
impl Drop for Helper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn helper(directory: &Directory) -> Helper {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--ignored",
            "--exact",
            "caller_lifetime_helper",
            "--nocapture",
        ])
        .env_clear()
        .env("HOME", &directory.0)
        .env("XDG_STATE_HOME", directory.0.join("state"))
        .env("MONITOR_HELPER_READY", directory.0.join("ready"))
        .env("MONITOR_HELPER_LOCATOR", directory.0.join("locator"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Helper(command.spawn().unwrap())
}

fn wait_file(path: &Path, deadline: Instant) -> Vec<u8> {
    loop {
        if let Ok(bytes) = fs::read(path)
            && !bytes.is_empty()
        {
            return bytes;
        }
        assert!(
            Instant::now() < deadline,
            "helper did not publish {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn caller_death_is_cleaned_by_an_independent_monitor_with_original_record() {
    let directory = Directory::new();
    let mut helper = helper(&directory);
    let root = PathBuf::from(
        String::from_utf8(wait_file(
            &directory.0.join("ready"),
            Instant::now() + Duration::from_secs(5),
        ))
        .unwrap(),
    );
    helper.0.kill().unwrap();
    assert!(!helper.0.wait().unwrap().success());
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let text = fs::read_to_string(root.join("record.jsonl")).unwrap();
        if text.contains("\"event\":\"Final\"") {
            let record: serde_json::Value =
                serde_json::from_str(text.lines().last().unwrap()).unwrap();
            assert_eq!(record["owner_lost"], true);
            assert_eq!(record["cleanup_complete"], true);
            assert!(record["status_raw"].is_i64());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "monitor never actually settled original payload: {text}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "internal exact helper; the outer test owns and waits this process"]
fn caller_lifetime_helper() {
    let ready =
        std::env::var_os("MONITOR_HELPER_READY").expect("outer test must supply ready path");
    let locator =
        std::env::var_os("MONITOR_HELPER_LOCATOR").expect("outer test must supply locator");
    let process = ManagedProcess::spawn(
        "caller lifetime".into(),
        command(SERVICE),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(3),
        locator.into(),
    )
    .unwrap();
    fs::write(
        ready,
        process.diagnostic_root().as_os_str().as_encoded_bytes(),
    )
    .unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn caller_drop_settles_with_a_full_unread_stderr_pipe() {
    let directory = Directory::new();
    // Keep every locator and monitor record if the bounded regression fails.
    let result = std::panic::catch_unwind(|| {
        let deadline = Instant::now() + Duration::from_secs(18);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "caller_drop_full_stderr_helper",
                "--nocapture",
            ])
            .env_clear()
            .env("HOME", &directory.0)
            .env("XDG_STATE_HOME", directory.0.join("state"))
            .env("MONITOR_HELPER_READY", directory.0.join("ready"))
            .env("MONITOR_HELPER_LOCATOR", directory.0.join("locator"))
            .env("MONITOR_HELPER_DONE", directory.0.join("done"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut helper = Helper(command.spawn().unwrap());
        // Retain the read end without draining it or waiting for writer EOF.
        // The exact helper Child remains owned even on assertion failure.
        let _unread_stderr = helper.0.stderr.take().unwrap();
        let ready: serde_json::Value =
            serde_json::from_slice(&wait_file(&directory.0.join("ready"), deadline)).unwrap();
        assert!(Instant::now() < deadline);
        assert!(ready["filled_bytes"].as_u64().unwrap() > 0);
        let root = PathBuf::from(ready["root"].as_str().unwrap());
        let pid = u32::try_from(ready["pid"].as_u64().unwrap()).unwrap();
        loop {
            assert!(
                Instant::now() < deadline,
                "caller Drop blocked with full stderr; diagnostics={}",
                root.display()
            );
            if let Some(status) = helper.0.try_wait().unwrap() {
                assert!(Instant::now() < deadline);
                assert!(status.success(), "helper actually exited {status}");
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(fs::read(directory.0.join("done")).unwrap(), b"dropped");
        loop {
            assert!(Instant::now() < deadline, "monitor Final deadline expired");
            let text = fs::read_to_string(root.join("record.jsonl")).unwrap();
            let record = text
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .find(|record| record["event"] == "Final");
            assert!(Instant::now() < deadline);
            if let Some(record) = record {
                assert_eq!(record["cleanup_complete"], true, "{record}");
                assert_eq!(record["owner_lost"], false, "{record}");
                assert_eq!(record["errors"], serde_json::json!([]), "{record}");
                assert!(record["status_raw"].is_i64(), "{record}");
                assert_pid_reaped(pid);
                assert!(Instant::now() < deadline);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    if let Err(error) = result {
        eprintln!(
            "full-stderr failure evidence retained at {}",
            directory.0.display()
        );
        std::mem::forget(directory);
        std::panic::resume_unwind(error);
    }
}

#[test]
#[ignore = "internal exact helper; the outer test owns and waits this process"]
fn caller_drop_full_stderr_helper() {
    let ready =
        std::env::var_os("MONITOR_HELPER_READY").expect("outer test must supply ready path");
    let locator =
        std::env::var_os("MONITOR_HELPER_LOCATOR").expect("outer test must supply locator");
    let done = std::env::var_os("MONITOR_HELPER_DONE").expect("outer test must supply done path");
    let process = ManagedProcess::spawn(
        "full-stderr caller drop".into(),
        command(SERVICE),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(3),
        locator.into(),
    )
    .unwrap();
    let root = process.diagnostic_root().to_owned();
    let pid = process.pid();

    // Only this isolated helper owns the writer being changed. Restore blocking
    // mode before Drop so a synchronous inherited-stderr diagnostic would hang.
    unsafe extern "C" {
        fn fcntl(fd: i32, command: i32, ...) -> i32;
        fn write(fd: i32, buffer: *const u8, length: usize) -> isize;
    }
    #[cfg(target_os = "macos")]
    const NONBLOCK: i32 = 0x4;
    #[cfg(target_os = "linux")]
    const NONBLOCK: i32 = 0x800;
    let flags = unsafe { fcntl(2, 3) };
    assert!(flags >= 0);
    assert_eq!(unsafe { fcntl(2, 4, flags | NONBLOCK) }, 0);
    let filled = (|| -> std::io::Result<usize> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let bytes = [b'x'; 4096];
        let mut filled = 0;
        for length in [bytes.len(), 1] {
            loop {
                if Instant::now() >= deadline || filled >= 16 * 1024 * 1024 {
                    return Err(std::io::Error::other("helper stderr never became full"));
                }
                let count = unsafe { write(2, bytes.as_ptr(), length) };
                if count > 0 {
                    filled += count as usize;
                    continue;
                }
                let error = std::io::Error::last_os_error();
                match error.kind() {
                    std::io::ErrorKind::WouldBlock => break,
                    std::io::ErrorKind::Interrupted => continue,
                    _ => return Err(error),
                }
            }
        }
        Ok(filled)
    })();
    let restored = unsafe { fcntl(2, 4, flags) };
    assert_eq!(restored, 0);
    let filled = filled.unwrap();
    fs::write(
        ready,
        serde_json::to_vec(&serde_json::json!({
            "root": root,
            "pid": pid,
            "filled_bytes": filled,
        }))
        .unwrap(),
    )
    .unwrap();
    drop(process);
    fs::write(done, b"dropped").unwrap();
}

fn command_with_paths(script: &str, directory: &Directory) -> ManagedCommand {
    let mut request = command(script);
    request
        .env("FIXTURE_DIRECTORY", &directory.0)
        .env("FIXTURE_BORN", directory.0.join("born"))
        .env("FIXTURE_GATE", directory.0.join("gate"));
    request
}

fn final_record(root: &Path) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let text = fs::read_to_string(root.join("record.jsonl")).unwrap();
        if let Some(record) = text
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|record| record["event"] == "Final")
        {
            assert_eq!(record["cleanup_complete"], true, "{record}");
            assert_eq!(record["errors"], serde_json::json!([]), "{record}");
            return record;
        }
        assert!(Instant::now() < deadline, "monitor did not settle: {text}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn assert_pid_reaped(pid: u32) {
    assert!(
        !Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success(),
        "the original payload {pid} must have actually exited and been reaped"
    );
}

const FILE_SERVICE: &str =
    "trap 'exit 0' TERM; printf born > \"$FIXTURE_BORN\"; while :; do sleep 1; done";

fn file_readiness_after_action(initial: &[u8], action: impl FnOnce(&Path), expected_ready: bool) {
    let directory = Directory::new();
    let marker = directory.0.join("readiness");
    fs::write(&marker, initial).unwrap();
    std::thread::scope(|scope| {
        let pending = scope.spawn(|| {
            ManagedProcess::spawn(
                "file-generation readiness".into(),
                command_with_paths(FILE_SERVICE, &directory),
                ReadyMarker::FileContains {
                    path: marker.clone(),
                    needle: "FILE_READY".into(),
                },
                Duration::from_secs(2),
                directory.0.join("locator"),
            )
        });
        // The payload publishes this only after spawn has captured the file
        // baseline. No wall-clock delay establishes the append/rewrite order.
        wait_file(
            &directory.0.join("born"),
            Instant::now() + Duration::from_secs(5),
        );
        action(&marker);
        let result = pending.join().unwrap();
        if expected_ready {
            let mut process = result.expect("a newly published marker must establish readiness");
            let pid = process.pid();
            process.stop().unwrap();
            final_record(process.diagnostic_root());
            assert_pid_reaped(pid);
        } else {
            let error = result.expect_err("stale bytes must not establish readiness");
            let text = format!("{error:#}");
            assert!(
                text.contains("deadline") || text.contains("timed out"),
                "{text}"
            );
            assert!(marker.exists(), "caller-owned marker files are retained");
        }
    });
}

#[test]
fn file_readiness_rejects_stale_marker() {
    file_readiness_after_action(b"STALE FILE_READY\n", |_| {}, false);
}

#[test]
fn file_readiness_rejects_stale_marker_after_unrelated_append() {
    use std::io::Write;
    file_readiness_after_action(
        b"STALE FILE_READY\n",
        |path| {
            fs::OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap()
                .write_all(b"unrelated appended bytes\n")
                .unwrap();
        },
        false,
    );
}

#[test]
fn file_readiness_accepts_marker_crossing_append_boundary() {
    use std::io::Write;
    file_readiness_after_action(
        b"old prefix FILE_RE",
        |path| {
            fs::OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap()
                .write_all(b"ADY suffix\n")
                .unwrap();
        },
        true,
    );
}

#[test]
fn file_readiness_accepts_same_bytes_rewritten_in_place() {
    file_readiness_after_action(
        b"FILE_READY\n",
        |path| fs::write(path, b"FILE_READY\n").unwrap(),
        true,
    );
}

#[test]
fn original_snapshot_accepts_append_but_rejects_replacement_and_truncation() {
    use std::io::Write;
    let directory = Directory::new();
    let mut process = service(&directory, SERVICE);
    let source = process.stdout_log_source();
    let observed = source
        .with_bounded_snapshot_reader(128, |reader, length| {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            fs::OpenOptions::new()
                .append(true)
                .open(source.path())?
                .write_all(b"APPENDED\n")?;
            Ok((length, bytes))
        })
        .unwrap();
    assert_eq!(observed, (6, b"READY\n".to_vec()));
    let error = source
        .with_bounded_snapshot_reader(128, |reader, _| {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            File::options()
                .write(true)
                .open(source.path())?
                .set_len(0)?;
            Ok(())
        })
        .unwrap_err();
    assert!(format!("{error:#}").contains("truncated"), "{error:#}");
    process.stop().unwrap();

    let mut process = service(&directory, SERVICE);
    let source = process.stdout_log_source();
    let error = source
        .with_bounded_snapshot_reader(128, |reader, _| {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            fs::rename(
                source.path(),
                source.path().with_file_name("retained-original"),
            )?;
            fs::write(source.path(), b"forged replacement\n")?;
            Ok(())
        })
        .unwrap_err();
    assert!(format!("{error:#}").contains("replaced"), "{error:#}");
    let error = source
        .with_bounded_snapshot_reader(128, |_, _| -> anyhow::Result<()> {
            panic!("replaced paths must fail before the visitor runs");
        })
        .unwrap_err();
    assert!(format!("{error:#}").contains("replaced"), "{error:#}");
    process.stop().unwrap();
}

#[test]
fn original_snapshot_preserves_visitor_io_error_even_after_replacement() {
    let directory = Directory::new();
    let mut process = service(&directory, SERVICE);
    let source = process.stdout_log_source();
    let error = source
        .with_bounded_snapshot_reader(128, |_, _| -> anyhow::Result<()> {
            fs::rename(
                source.path(),
                source.path().with_file_name("retained-original"),
            )?;
            fs::write(source.path(), b"forged replacement\n")?;
            Err(File::open(directory.0.join("absent-visitor-input"))
                .unwrap_err()
                .into())
        })
        .unwrap_err();
    assert!(
        error.chain().any(|source| source
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)),
        "{error:#}"
    );
    assert!(
        format!("{error:#}").contains("replaced"),
        "the original visitor error must retain the independent post-visitor integrity failure: {error:#}"
    );
    process.stop().unwrap();
}

#[test]
fn interrupt_success_delivers_sigint_and_waits_actual_exit() {
    let directory = Directory::new();
    let mut process = ManagedProcess::spawn(
        "successful SIGINT".into(),
        command_with_paths("trap 'printf INT > \"$FIXTURE_BORN\"; exit 0' INT; printf 'READY\\n'; while :; do sleep 1; done", &directory),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(3),
        directory.0.join("locator"),
    ).unwrap();
    let pid = process.pid();
    assert!(
        process
            .interrupt_and_wait(Duration::from_secs(3))
            .unwrap()
            .success()
    );
    assert_eq!(fs::read(directory.0.join("born")).unwrap(), b"INT");
    let record = final_record(process.diagnostic_root());
    assert_eq!(record["status_raw"], 0);
    assert_pid_reaped(pid);
}

fn signal_request_respects_the_caller_owned_completion_gate(interrupt: bool) {
    let directory = Directory::new();
    let signal = if interrupt { "INT" } else { "TERM" };
    let script = format!(
        "trap 'printf SIGNALLED > \"$FIXTURE_BORN\"; while [ ! -e \"$FIXTURE_GATE\" ]; do sleep 0.01; done; exit 0' {signal}; printf 'READY\\n'; while :; do sleep 1; done"
    );
    let mut process = ManagedProcess::spawn(
        format!("caller-owned {signal} completion gate"),
        command_with_paths(&script, &directory),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(10),
        directory.0.join("locator"),
    )
    .unwrap();
    let pid = process.pid();
    let original_deadline = Instant::now() + Duration::from_secs(30);
    std::thread::scope(|scope| {
        let (completed, completion) = std::sync::mpsc::channel();
        let worker = scope.spawn(move || {
            let result = if interrupt {
                process.interrupt_and_wait(Duration::from_secs(30))
            } else {
                process
                    .request_termination()
                    .and_then(|()| process.wait_for_successful_exit_until(original_deadline))
            };
            let _ = completed.send(
                result
                    .as_ref()
                    .map(|status| status.to_string())
                    .map_err(|error| format!("{error:#}")),
            );
            (process, result)
        });
        assert_eq!(
            wait_file(&directory.0.join("born"), original_deadline),
            b"SIGNALLED"
        );
        // This minimum hold is a functional regression boundary, not a speed
        // assertion: signal-only requests must survive the former five-second
        // cleanup escalation while only the caller can open the exit gate.
        let before_gate = completion.recv_timeout(Duration::from_secs(6));
        fs::write(
            directory.0.join("gate"),
            b"caller permits successful exit\n",
        )
        .unwrap();
        let (mut process, result) = worker.join().unwrap();
        let settlement = process.stop();
        assert!(
            settlement.is_ok(),
            "actual signal fixture settlement: {settlement:?}"
        );
        assert!(process.request_termination().is_err());
        assert!(process.interrupt_and_wait(Duration::from_secs(1)).is_err());
        assert!(
            matches!(before_gate, Err(std::sync::mpsc::RecvTimeoutError::Timeout)),
            "{signal} completed before its caller released the gate: {before_gate:?}"
        );
        assert!(
            result.as_ref().is_ok_and(|status| status.success()),
            "caller-owned {signal} completion: {result:?}"
        );
        let record = final_record(process.diagnostic_root());
        assert_eq!(record["status_raw"], 0);
        assert_pid_reaped(pid);
    });
}

#[test]
fn termination_request_does_not_start_a_five_second_cleanup_deadline() {
    signal_request_respects_the_caller_owned_completion_gate(false);
}

#[test]
fn interrupt_wait_keeps_its_original_deadline_beyond_five_seconds() {
    signal_request_respects_the_caller_owned_completion_gate(true);
}

#[test]
fn interrupt_nonzero_exit_is_an_error_with_original_status() {
    use std::os::unix::process::ExitStatusExt;
    let directory = Directory::new();
    let mut process = ManagedProcess::spawn(
        "failed SIGINT".into(),
        command_with_paths("trap 'printf INT > \"$FIXTURE_BORN\"; exit 23' INT; printf 'READY\\n'; while :; do sleep 1; done", &directory),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(3), directory.0.join("locator"),
    ).unwrap();
    let pid = process.pid();
    let error = process
        .interrupt_and_wait(Duration::from_secs(3))
        .unwrap_err();
    assert_eq!(fs::read(directory.0.join("born")).unwrap(), b"INT");
    let text = format!("{error:#}");
    assert!(text.contains("SIGINT") && text.contains("23"), "{text}");
    let record = final_record(process.diagnostic_root());
    let status = std::process::ExitStatus::from_raw(record["status_raw"].as_i64().unwrap() as i32);
    assert_eq!(status.code(), Some(23));
    assert_pid_reaped(pid);
}

#[test]
fn interrupt_timeout_kills_and_reaps_payload_that_ignores_sigint() {
    let directory = Directory::new();
    let mut process = service(
        &directory,
        "trap '' INT TERM; printf 'READY\\n'; while :; do sleep 1; done",
    );
    let pid = process.pid();
    let error = process
        .interrupt_and_wait(Duration::from_millis(300))
        .unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("deadline") || text.contains("timed out"),
        "{text}"
    );
    final_record(process.diagnostic_root());
    assert_pid_reaped(pid);
}

#[test]
fn post_readiness_marker_timeout_kills_and_reaps_payload() {
    let directory = Directory::new();
    let mut process = service(&directory, SERVICE);
    let pid = process.pid();
    let error = process
        .wait_for_log_contains("NEVER_PUBLISHED", Duration::from_millis(300))
        .unwrap_err();
    let text = format!("{error:#}");
    assert!(
        text.contains("deadline") || text.contains("timed out"),
        "{text}"
    );
    final_record(process.diagnostic_root());
    assert_pid_reaped(pid);
}

#[test]
fn restart_waits_for_old_payload_and_uses_new_pid_and_logs() {
    let directory = Directory::new();
    let mut process = service(&directory, SERVICE);
    let old_pid = process.pid();
    let original = process.stdout_log_source();
    process
        .restart(
            command("trap 'exit 0' TERM; printf 'REPLACEMENT\\n'; while :; do sleep 1; done"),
            ReadyMarker::StdoutContains("REPLACEMENT".into()),
            Duration::from_secs(3),
            directory.0.join("replacement-locator"),
        )
        .unwrap();
    assert_ne!(old_pid, process.pid());
    assert_pid_reaped(old_pid);
    assert!(
        original
            .read_tail(128)
            .unwrap()
            .bytes
            .starts_with(b"READY\n")
    );
    assert_ne!(
        original.original_durable_log_identity().unwrap(),
        process
            .stdout_log_source()
            .original_durable_log_identity()
            .unwrap()
    );
    process.assert_log_contains("REPLACEMENT").unwrap();
    process.stop().unwrap();
}

fn runtime_exit_diagnostic(script: &str, expected_status: &str) {
    let directory = Directory::new();
    let config = directory.0.join("fixture.toml");
    fs::write(&config, b"fixture = true\n").unwrap();
    let mut process = ManagedProcess::spawn(
        "runtime diagnostic fixture".into(),
        command_with_paths(script, &directory),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(3),
        directory.0.join("locator"),
    )
    .unwrap();
    let pid = process.pid();
    fs::write(directory.0.join("gate"), b"exit").unwrap();
    final_record(process.diagnostic_root());
    let error = process
        .runtime_diagnostic("fixture", "local", &config)
        .unwrap_err();
    let text = format!("{error:#}");
    assert!(text.contains(expected_status), "{text}");
    assert!(
        text.contains(&format!("pid={pid}"))
            && text.contains("endpoint=local")
            && text.contains("fixture.toml"),
        "{text}"
    );
    assert!(text.contains("last stderr"), "{text}");
    process.stop().unwrap();
    assert_pid_reaped(pid);
}

#[test]
fn runtime_diagnostic_reports_real_exit_code() {
    runtime_exit_diagnostic(
        "printf 'READY\\n'; while [ ! -e \"$FIXTURE_GATE\" ]; do sleep 0.01; done; printf 'last stderr\\n' >&2; exit 23",
        "status=exit code 23",
    );
}

#[test]
fn runtime_diagnostic_reports_real_exit_signal() {
    runtime_exit_diagnostic(
        "printf 'READY\\n'; while [ ! -e \"$FIXTURE_GATE\" ]; do sleep 0.01; done; printf 'last stderr\\n' >&2; kill -TERM $$",
        "status=signal 15",
    );
}

fn root_from_locator(path: &Path) -> PathBuf {
    let locator = fs::read_to_string(path).unwrap();
    PathBuf::from(
        locator
            .lines()
            .find_map(|line| line.strip_prefix("record="))
            .unwrap(),
    )
    .parent()
    .unwrap()
    .to_owned()
}

#[test]
fn one_shot_marker_does_not_renew_original_completion_deadline() {
    let directory = Directory::new();
    let locator = directory.0.join("locator");
    std::thread::scope(|scope| {
        let (started, receive) = std::sync::mpsc::channel();
        let fixture = &directory;
        let fixture_locator = &locator;
        let pending = scope.spawn(move || {
            let start = Instant::now();
            started.send(start).unwrap();
            ManagedProcess::run_to_completion("original one-shot deadline".into(), command_with_paths("printf born > \"$FIXTURE_BORN\"; while [ ! -e \"$FIXTURE_DIRECTORY/marker-gate\" ]; do sleep 0.01; done; printf 'READY\\n'; while [ ! -e \"$FIXTURE_GATE\" ]; do sleep 0.01; done; exit 0", fixture), ReadyMarker::StdoutContains("READY".into()), Duration::from_secs(2), fixture_locator.clone())
        });
        let start = receive.recv().unwrap();
        wait_file(&directory.0.join("born"), start + Duration::from_secs(5));
        std::thread::sleep(
            (start + Duration::from_secs(1)).saturating_duration_since(Instant::now()),
        );
        fs::write(directory.0.join("marker-gate"), b"publish readiness").unwrap();
        let root = root_from_locator(&locator);
        assert_eq!(
            wait_file(&root.join("stdout.log"), start + Duration::from_secs(2)),
            b"READY\n"
        );
        std::thread::sleep(
            (start + Duration::from_millis(2500)).saturating_duration_since(Instant::now()),
        );
        fs::write(
            directory.0.join("gate"),
            b"publish successful exit after original deadline",
        )
        .unwrap();
        let error = pending
            .join()
            .unwrap()
            .expect_err("readiness must not reset the completion budget");
        let text = format!("{error:#}");
        assert!(
            text.contains("deadline") || text.contains("timed out"),
            "{text}"
        );
    });
    final_record(&root_from_locator(&locator));
}

#[test]
fn one_shot_rejects_marker_published_after_original_deadline() {
    let directory = Directory::new();
    let locator = directory.0.join("locator");
    std::thread::scope(|scope| {
        let (started, receive) = std::sync::mpsc::channel();
        let fixture = &directory;
        let fixture_locator = &locator;
        let pending = scope.spawn(move || {
            let start = Instant::now();
            started.send(start).unwrap();
            ManagedProcess::run_to_completion(
                "late one-shot marker".into(),
                command_with_paths("printf born > \"$FIXTURE_BORN\"; while [ ! -e \"$FIXTURE_GATE\" ]; do sleep 0.01; done; exit 0", fixture),
                ReadyMarker::FileContains { path: fixture.0.join("late-marker"), needle: "READY".into() },
                Duration::from_secs(1), fixture_locator.clone())
        });
        let start = receive.recv().unwrap();
        wait_file(&directory.0.join("born"), start + Duration::from_secs(5));
        std::thread::sleep(
            (start + Duration::from_millis(1500)).saturating_duration_since(Instant::now()),
        );
        // The caller owns this file; its actual publication remains possible
        // after the monitor has correctly terminated the expired payload.
        fs::write(directory.0.join("late-marker"), b"READY\n").unwrap();
        fs::write(directory.0.join("gate"), b"late readiness publication").unwrap();
        let error = pending
            .join()
            .unwrap()
            .expect_err("late marker cannot salvage an expired attempt");
        let text = format!("{error:#}");
        assert!(
            text.contains("deadline") || text.contains("timed out"),
            "{text}"
        );
    });
    final_record(&root_from_locator(&locator));
}

#[test]
fn capture_output_copy_failure_keeps_original_run_owned_output() {
    let directory = Directory::new();
    let out = directory.0.join("consumer-stdout");
    fs::write(&out, b"").unwrap();
    let outcome = capture_command_until(
        command("printf 'authoritative stdout\\n'; printf 'authoritative stderr\\n' >&2; exit 7"),
        File::open(&out).unwrap(),
        File::create(directory.0.join("consumer-stderr")).unwrap(),
        Instant::now() + Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(outcome.output.status.code(), Some(7));
    assert!(
        !outcome.failures.is_empty(),
        "read-only consumer output must fail to copy"
    );
    let root = outcome.diagnostic_root().to_owned();
    assert_eq!(
        fs::read(root.join("stdout.log")).unwrap(),
        b"authoritative stdout\n"
    );
    assert_eq!(
        fs::read(root.join("stderr.log")).unwrap(),
        b"authoritative stderr\n"
    );
    assert!(outcome.release_diagnostics().is_err());
    drop(outcome);
    assert!(
        root.exists(),
        "failed output material must outlive the consumer"
    );
    final_record(&root);
}

#[test]
fn capture_timeout_keeps_original_run_owned_output_and_actual_cleanup() {
    let directory = Directory::new();
    let outcome = capture_command_until(command("printf 'before timeout stdout\\n'; printf 'before timeout stderr\\n' >&2; while :; do sleep 1; done"), File::create(directory.0.join("consumer-stdout")).unwrap(), File::create(directory.0.join("consumer-stderr")).unwrap(), Instant::now() + Duration::from_secs(1)).unwrap();
    assert!(outcome.timed_out);
    assert!(!outcome.failures.is_empty());
    let root = outcome.diagnostic_root().to_owned();
    assert_eq!(
        fs::read(root.join("stdout.log")).unwrap(),
        b"before timeout stdout\n"
    );
    assert_eq!(
        fs::read(root.join("stderr.log")).unwrap(),
        b"before timeout stderr\n"
    );
    assert!(outcome.release_diagnostics().is_err());
    final_record(&root);
    drop(outcome);
    assert!(
        root.exists(),
        "timeout output material must outlive the consumer"
    );
}

#[test]
fn original_log_identity_is_the_spawn_time_scalar_after_path_replacement() {
    let directory = Directory::new();
    let mut process = service(&directory, SERVICE);
    let source = process.stdout_log_source();
    let original = source.original_durable_log_identity().unwrap();
    fs::rename(
        source.path(),
        source.path().with_file_name("retained-original"),
    )
    .unwrap();
    fs::write(source.path(), b"unrelated replacement\n").unwrap();
    assert_eq!(
        source.original_durable_log_identity().unwrap(),
        original,
        "spawn-time identity is a scalar observation, not permission to read the replacement"
    );
    assert!(source.read_tail(128).is_err());
    process.stop().unwrap();
}

#[test]
fn log_count_preserves_nonoverlapping_match_semantics() {
    let directory = Directory::new();
    let mut process = service(
        &directory,
        "trap 'exit 0' TERM; printf 'AAAA READY\\n'; while :; do sleep 1; done",
    );
    assert_eq!(process.log_count("AA").unwrap(), 2);
    process.stop().unwrap();
}

#[test]
fn admitted_lifetime_stops_writes_while_the_owner_remains_alive() {
    let directory = Directory::new();
    let writes = directory.0.join("writes");
    let now = Instant::now();
    let mut request = command(
        "trap 'exit 0' TERM; printf 'READY\\n'; while :; do printf x >> \"$WRITES\"; sleep 0.02; done",
    );
    request
        .env("WRITES", &writes)
        .terminate_at(now + Duration::from_secs(2), now + Duration::from_secs(4));
    let mut process = ManagedProcess::spawn(
        "lifetime writes".into(),
        request,
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(2),
        directory.0.join("locator"),
    )
    .unwrap();
    let root = process.diagnostic_root().to_owned();
    let error = process
        .wait_for_successful_exit_until(now + Duration::from_secs(6))
        .unwrap_err();
    assert!(
        error.is::<novarocks_test_support::PayloadLifetimeExpired>(),
        "{error:#}"
    );
    let record = final_record(&root);
    assert_eq!(record["lifetime_expired"], true);
    assert_eq!(record["owner_lost"], false);
    assert_eq!(record["group_exit_observed"], true);
    assert_eq!(
        record["status_raw"], 0,
        "a cooperative native exit0 cannot erase expiration"
    );
    let size = fs::metadata(&writes).unwrap().len();
    assert!(size > 0);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(fs::metadata(writes).unwrap().len(), size);
    assert_pid_reaped(process.pid());
    assert_eq!(record["cleanup_complete"], true, "{record}");
    assert_eq!(record["errors"], serde_json::json!([]), "{record}");
    drop(process);
    assert!(
        !root.join("caller-drop-error.txt").exists(),
        "an explicitly observed lifetime failure must not become a second cleanup failure"
    );
}

fn cleanup_after_admitted_lifetime(kill_first: bool) {
    let directory = Directory::new();
    let now = Instant::now();
    let mut request = command(SERVICE);
    request.terminate_at(now + Duration::from_secs(2), now + Duration::from_secs(4));
    let mut process = ManagedProcess::spawn(
        "expired service cleanup".into(),
        request,
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(2),
        directory.0.join("locator"),
    )
    .unwrap();
    let root = process.diagnostic_root().to_owned();
    // Observe the original monitor's Final before invoking either cleanup
    // method. Neither cleanup request is what causes this admitted expiry.
    let record = final_record(&root);
    assert_eq!(record["lifetime_expired"], true, "{record}");
    assert_eq!(record["owner_lost"], false, "{record}");
    assert_eq!(record["group_exit_observed"], true, "{record}");
    let text = fs::read_to_string(root.join("record.jsonl")).unwrap();
    let pending: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert!(
        record["group_exit_observed_ns"].as_u64().unwrap()
            < pending["lifetime_ns"][1].as_u64().unwrap(),
        "actual group exit must remain within the admitted complete clock"
    );
    assert!(!process.cleanup_settled());
    let error = if kill_first {
        process.kill_now().unwrap_err()
    } else {
        process.stop().unwrap_err()
    };
    assert!(
        error.is::<novarocks_test_support::PayloadLifetimeExpired>(),
        "{error:#}"
    );
    assert!(process.cleanup_settled());
    process.stop().unwrap();
    process.kill_now().unwrap();
    assert!(process.cleanup_settled());
    // The cleanup calls above must actually wait the monitor; PID absence is
    // an additional observation rather than permission to claim settlement.
    assert_pid_reaped(process.pid());
    assert_eq!(final_record(&root), record);
}

#[test]
fn stop_after_admitted_lifetime_settles_and_is_idempotent() {
    cleanup_after_admitted_lifetime(false);
}

#[test]
fn kill_after_admitted_lifetime_settles_and_is_idempotent() {
    cleanup_after_admitted_lifetime(true);
}

#[test]
fn admitted_lifetime_escalates_term_ignoring_group_and_observes_descendants_exit() {
    let directory = Directory::new();
    let child_pid = directory.0.join("child.pid");
    let writes = directory.0.join("descendant-writes");
    let now = Instant::now();
    let mut request = command(
        "trap '' TERM; (trap '' TERM; while :; do printf x >> \"$WRITES\"; sleep 0.02; done) & printf '%s' $! > \"$CHILD_PID\"; printf 'READY\\n'; while :; do sleep 1; done",
    );
    request
        .env("WRITES", &writes)
        .env("CHILD_PID", &child_pid)
        .terminate_at(now + Duration::from_secs(2), now + Duration::from_secs(5));
    let mut process = ManagedProcess::spawn(
        "lifetime descendants".into(),
        request,
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(2),
        directory.0.join("locator"),
    )
    .unwrap();
    let root = process.diagnostic_root().to_owned();
    let error = process
        .wait_for_successful_exit_until(now + Duration::from_secs(7))
        .unwrap_err();
    assert!(
        error.is::<novarocks_test_support::PayloadLifetimeExpired>(),
        "{error:#}"
    );
    let text = fs::read_to_string(root.join("record.jsonl")).unwrap();
    let record: serde_json::Value = text
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|r| r["event"] == "Final")
        .unwrap();
    assert_eq!(record["lifetime_expired"], true);
    assert_eq!(record["owner_lost"], false);
    assert_eq!(record["group_exit_observed"], true, "{record}");
    let pending: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert!(
        record["group_exit_observed_ns"].as_u64().unwrap()
            < pending["lifetime_ns"][1].as_u64().unwrap()
    );
    assert_eq!(record["cleanup_complete"], true, "{record}");
    let status = <std::process::ExitStatus as std::os::unix::process::ExitStatusExt>::from_raw(
        record["status_raw"].as_i64().unwrap() as i32,
    );
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(9)
    );
    let size = fs::metadata(&writes).unwrap().len();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(fs::metadata(writes).unwrap().len(), size);
    let pid = fs::read_to_string(child_pid).unwrap();
    let observed = Command::new("/bin/ps")
        .args(["-o", "stat=", "-p", pid.trim()])
        .output()
        .unwrap();
    let state = String::from_utf8(observed.stdout).unwrap();
    assert!(
        state.trim().is_empty() || state.trim().starts_with('Z'),
        "descendant is still executing: {state}"
    );
    assert_pid_reaped(process.pid());
    assert_eq!(record["errors"], serde_json::json!([]), "{record}");
    drop(process);
    assert!(
        !root.join("caller-drop-error.txt").exists(),
        "an explicitly observed lifetime failure must not become a second cleanup failure"
    );
}

#[test]
fn optional_lifetime_never_reinterprets_default_startup_clock_as_runtime_limit() {
    let directory = Directory::new();
    let mut process = ManagedProcess::spawn(
        "startup only".into(),
        command(SERVICE),
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(1),
        directory.0.join("locator"),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(1200));
    assert!(process.is_running().unwrap());
    process.stop().unwrap();
    let record = final_record(process.diagnostic_root());
    assert_eq!(record["lifetime_expired"], false);
}

#[test]
fn repeated_lifetime_configuration_only_narrows_original_clocks() {
    let directory = Directory::new();
    let now = Instant::now();
    let mut request = command(SERVICE);
    request.terminate_at(now + Duration::from_secs(2), now + Duration::from_secs(4));
    request.terminate_at(now + Duration::from_secs(30), now + Duration::from_secs(60));
    let mut process = ManagedProcess::spawn(
        "bounded lifetime".into(),
        request,
        ReadyMarker::StdoutContains("READY".into()),
        Duration::from_secs(2),
        directory.0.join("locator"),
    )
    .unwrap();
    let error = process
        .wait_for_successful_exit_until(now + Duration::from_secs(6))
        .unwrap_err();
    assert!(
        error.is::<novarocks_test_support::PayloadLifetimeExpired>(),
        "{error:#}"
    );
    assert!(Instant::now() < now + Duration::from_secs(6));
    let root = process.diagnostic_root().to_owned();
    let record = final_record(&root);
    assert_eq!(record["lifetime_expired"], true);
    assert_eq!(record["cleanup_complete"], true, "{record}");
    assert_eq!(record["owner_lost"], false, "{record}");
    assert_eq!(record["errors"], serde_json::json!([]), "{record}");
    drop(process);
    assert!(
        !root.join("caller-drop-error.txt").exists(),
        "an explicitly observed lifetime failure must not become a second cleanup failure"
    );
}

#[test]
fn already_expired_lifetime_refuses_payload_side_effects_before_spawn() {
    let directory = Directory::new();
    let written = directory.0.join("must-not-exist");
    let mut request = command("printf forbidden > \"$WRITES\"; printf 'READY\\n'; sleep 10");
    request.env("WRITES", &written).terminate_at(
        Instant::now() - Duration::from_secs(1),
        Instant::now() + Duration::from_secs(2),
    );
    assert!(
        ManagedProcess::spawn(
            "expired before spawn".into(),
            request,
            ReadyMarker::StdoutContains("READY".into()),
            Duration::from_secs(2),
            directory.0.join("locator")
        )
        .is_err()
    );
    assert!(!written.exists());
}
