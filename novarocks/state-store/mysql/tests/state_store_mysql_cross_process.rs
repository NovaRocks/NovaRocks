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

#![cfg(all(
    feature = "mysql-state-store-provider",
    feature = "state-store-test-hooks"
))]

use std::io::{Read, Write};
#[cfg(target_os = "linux")]
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

mod common;

const HELPER: &str = env!("CARGO_BIN_EXE_state-store-mysql-helper");
const TEST_DIAGNOSTIC_CAP_BYTES: usize = 1024;
const HELPER_DIAGNOSTIC_CAP_BYTES: usize = 64 * 1024;
const HELPER_TOTAL_DEADLINE: Duration = Duration::from_secs(15);

enum HelperEnvironment<'a> {
    Empty,
    Ordinary { database: &'a str },
}

struct BoundedCommandOutput {
    output: Output,
    pid: u32,
    timed_out: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

struct CappedCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

impl CappedCapture {
    fn empty() -> Self {
        Self {
            bytes: Vec::new(),
            truncated: false,
        }
    }

    fn append(&mut self, chunk: &[u8], cap: usize) {
        let retained = cap.saturating_sub(self.bytes.len()).min(chunk.len());
        self.bytes.extend_from_slice(&chunk[..retained]);
        self.truncated |= retained < chunk.len();
    }
}

fn run_command_bounded(
    command: &mut Command,
    input: Option<&[u8]>,
    deadline: Duration,
    diagnostic_cap: usize,
) -> BoundedCommandOutput {
    run_command_bounded_observed(command, input, deadline, diagnostic_cap, |_| {})
}

fn run_command_bounded_observed(
    command: &mut Command,
    input: Option<&[u8]>,
    deadline: Duration,
    diagnostic_cap: usize,
    observe: impl FnOnce(u32),
) -> BoundedCommandOutput {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn bounded child");
    let pid = child.id();
    let mut stdin = Some(child.stdin.take().expect("take bounded child stdin"));
    let stdout = child.stdout.take().expect("take bounded child stdout");
    let stderr = child.stderr.take().expect("take bounded child stderr");
    let mut stdout_reader = Some(spawn_capped_reader(stdout, diagnostic_cap));
    let mut stderr_reader = Some(spawn_capped_reader(stderr, diagnostic_cap));
    if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observe(pid))) {
        let _ = child.kill();
        let _ = child.wait();
        drop(stdin.take());
        let _ = stdout_reader
            .take()
            .expect("stdout reader is installed")
            .join();
        let _ = stderr_reader
            .take()
            .expect("stderr reader is installed")
            .join();
        std::panic::resume_unwind(panic);
    }
    let mut writer = if let Some(input) = input {
        let input = input.to_vec();
        let mut stdin = stdin.take().expect("stdin is installed");
        Some(std::thread::spawn(move || stdin.write_all(&input)))
    } else {
        drop(stdin.take());
        None
    };
    let stop_at = std::time::Instant::now() + deadline;
    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait().expect("poll bounded child") {
            break (status, false);
        }
        if std::time::Instant::now() >= stop_at {
            let _ = child.kill();
            break (child.wait().expect("wait for killed bounded child"), true);
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    if let Some(writer) = writer.take() {
        let write_result = writer.join().expect("join bounded child stdin writer");
        if !timed_out {
            write_result.expect("write bounded child input");
        }
    }
    let stdout = stdout_reader
        .take()
        .expect("stdout reader is installed")
        .join()
        .expect("join bounded child stdout reader");
    let stderr = stderr_reader
        .take()
        .expect("stderr reader is installed")
        .join()
        .expect("join bounded child stderr reader");
    BoundedCommandOutput {
        output: Output {
            status,
            stdout: stdout.bytes,
            stderr: stderr.bytes,
        },
        pid,
        timed_out,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
    }
}

fn spawn_capped_reader(
    mut reader: impl Read + Send + 'static,
    cap: usize,
) -> JoinHandle<CappedCapture> {
    std::thread::spawn(move || {
        let mut bytes = Vec::with_capacity(cap.min(8192));
        let mut truncated = false;
        let mut buffer = [0_u8; 8192];
        loop {
            let read = reader.read(&mut buffer).expect("read bounded child stream");
            if read == 0 {
                break;
            }
            let remaining = cap.saturating_sub(bytes.len());
            let retained = remaining.min(read);
            bytes.extend_from_slice(&buffer[..retained]);
            truncated |= retained < read;
        }
        CappedCapture { bytes, truncated }
    })
}

fn spawn_interactive_stdout(
    mut reader: impl Read + Send + 'static,
    sender: mpsc::Sender<String>,
    capture: Arc<Mutex<CappedCapture>>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        let mut pending = Vec::new();
        loop {
            let read = reader.read(&mut buffer).expect("read helper stdout");
            if read == 0 {
                break;
            }
            capture
                .lock()
                .expect("lock helper stdout capture")
                .append(&buffer[..read], HELPER_DIAGNOSTIC_CAP_BYTES);
            for byte in &buffer[..read] {
                if *byte == b'\n' {
                    let line = String::from_utf8(std::mem::take(&mut pending));
                    if line.ok().is_none_or(|line| sender.send(line).is_err()) {
                        return;
                    }
                } else if pending.len() < HELPER_DIAGNOSTIC_CAP_BYTES {
                    pending.push(*byte);
                }
            }
        }
    })
}

fn spawn_interactive_capture(
    mut reader: impl Read + Send + 'static,
    capture: Arc<Mutex<CappedCapture>>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        loop {
            let read = reader
                .read(&mut buffer)
                .expect("read helper diagnostic stream");
            if read == 0 {
                break;
            }
            capture
                .lock()
                .expect("lock helper diagnostic capture")
                .append(&buffer[..read], HELPER_DIAGNOSTIC_CAP_BYTES);
        }
    })
}

fn process_is_alive(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    return Path::new(&format!("/proc/{pid}")).exists();
    #[cfg(not(target_os = "linux"))]
    return Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .is_ok_and(|output| output.status.success());
}

fn run_helper(input: &[u8], environment: HelperEnvironment<'_>) -> Output {
    run_helper_observed(input, environment, |_| {})
}

fn run_helper_observed(
    input: &[u8],
    environment: HelperEnvironment<'_>,
    observe: impl FnOnce(u32),
) -> Output {
    let mut command = Command::new(HELPER);
    command.env_clear();
    if let HelperEnvironment::Ordinary { database } = environment {
        configure_ordinary_helper(&mut command, database);
    }
    let result = run_command_bounded_observed(
        &mut command,
        Some(input),
        HELPER_TOTAL_DEADLINE,
        HELPER_DIAGNOSTIC_CAP_BYTES,
        observe,
    );
    let diagnostics = bounded_output_diagnostics(&result);
    assert!(
        !result.timed_out,
        "helper exceeded total deadline: {diagnostics}"
    );
    assert!(
        !result.stdout_truncated && !result.stderr_truncated,
        "helper diagnostics exceeded cap: {diagnostics}"
    );
    result.output
}

#[test]
fn mysql_helper_pure_protocol_child_has_empty_environment() {
    let parent_path = std::env::var_os("PATH").expect("parent PATH must exist");
    assert!(!parent_path.is_empty(), "parent PATH must not be empty");
    let output = run_helper_observed(
        b"{\"id\":1,\"command\":\"Shutdown\"}\n",
        HelperEnvironment::Empty,
        |pid| {
            let environment = process_environment(pid);
            assert!(
                !contains_bytes(&environment, b"PATH="),
                "pure protocol helper inherited parent environment"
            );
        },
    );
    assert!(output.status.success());
}

#[test]
fn mysql_helper_shutdown_observer_runs_before_stdin_delivery() {
    for iteration in 0..8 {
        let marker = format!("observer-order-{iteration}");
        let mut command = Command::new(HELPER);
        command.env_clear().env("SS3_OBSERVER_ORDER", &marker);
        let result = run_command_bounded_observed(
            &mut command,
            Some(b"{\"id\":1,\"command\":\"Shutdown\"}\n"),
            HELPER_TOTAL_DEADLINE,
            HELPER_DIAGNOSTIC_CAP_BYTES,
            |pid| {
                std::thread::sleep(Duration::from_millis(100));
                let environment = process_environment(pid);
                assert!(
                    contains_bytes(
                        &environment,
                        format!("SS3_OBSERVER_ORDER={marker}").as_bytes()
                    ),
                    "observer must inspect the live child before Shutdown is delivered"
                );
            },
        );
        assert!(result.output.status.success());
    }
}

#[test]
fn mysql_helper_runner_times_out_silent_child_without_leaking_process() {
    let mut command = Command::new("/bin/sleep");
    command.arg("1");
    let started = std::time::Instant::now();
    let result = run_command_bounded(
        &mut command,
        None,
        Duration::from_millis(100),
        TEST_DIAGNOSTIC_CAP_BYTES,
    );
    assert!(result.timed_out, "silent child must hit the total deadline");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "silent child timeout must be bounded"
    );
    assert!(
        !process_is_alive(result.pid),
        "timed-out child must be reaped"
    );
}

#[test]
fn mysql_helper_runner_caps_stderr_flood_and_reaps_child() {
    let mut command = Command::new("/bin/sh");
    command.args([
        "-c",
        "i=0; while [ $i -lt 10000 ]; do echo helper-stdout-flood; echo helper-stderr-flood >&2; i=$((i + 1)); done",
    ]);
    let result = run_command_bounded(
        &mut command,
        None,
        Duration::from_secs(5),
        TEST_DIAGNOSTIC_CAP_BYTES,
    );
    assert!(!result.timed_out, "finite stderr flood child must finish");
    assert!(
        result.stdout_truncated,
        "stdout flood must report truncation"
    );
    assert!(
        result.stderr_truncated,
        "stderr flood must report truncation"
    );
    assert!(result.output.stdout.len() <= TEST_DIAGNOSTIC_CAP_BYTES);
    assert!(result.output.stderr.len() <= TEST_DIAGNOSTIC_CAP_BYTES);
    assert!(!process_is_alive(result.pid), "flood child must be reaped");
}

#[test]
fn mysql_helper_runner_reaps_child_when_observer_panics() {
    let observed_pid = Arc::new(AtomicU32::new(0));
    let observed = Arc::clone(&observed_pid);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut command = Command::new("/bin/sleep");
        command.arg("0.2");
        let _ = run_command_bounded_observed(
            &mut command,
            None,
            Duration::from_secs(1),
            TEST_DIAGNOSTIC_CAP_BYTES,
            move |pid| {
                observed.store(pid, Ordering::Release);
                panic!("observer panic for reap verification");
            },
        );
    }));
    assert!(panic.is_err(), "observer panic must propagate");
    let pid = observed_pid.load(Ordering::Acquire);
    assert!(pid > 0, "observer must receive the child PID");
    let was_alive_after_unwind = process_is_alive(pid);
    if was_alive_after_unwind {
        std::thread::sleep(Duration::from_millis(300));
    }
    assert!(
        !was_alive_after_unwind,
        "observer panic must kill and reap the child before unwinding"
    );
}

#[test]
fn mysql_helper_interactive_timeout_kills_child_without_diagnostic_deadlock() {
    let mut helper = HelperProcess::spawn_silent_for_test(Duration::from_secs(1));
    let pid = helper.id();
    let started = std::time::Instant::now();
    let diagnostic = helper
        .receive_with_timeout(Duration::from_millis(100))
        .expect_err("silent interactive child must time out");
    assert!(diagnostic.contains("timed out"));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "interactive timeout must not block on diagnostics"
    );
    assert!(!process_is_alive(pid), "interactive child must be reaped");
}

#[test]
fn mysql_helper_interactive_timeout_diagnostic_omits_raw_protocol_payload() {
    let canaries = [
        "timeout-key-canary",
        "timeout-value-canary",
        "timeout-records-canary",
        "timeout-hints-canary",
    ];
    let payload = format!(
        "{{\"id\":77,\"event\":\"Record\",\"code\":\"Pending\",\"record\":{{\"key\":\"{}\",\"value\":\"{}\"}},\"records\":[\"{}\"],\"hints\":[\"{}\"]}}",
        canaries[0], canaries[1], canaries[2], canaries[3]
    );
    let mut command = Command::new("/bin/sh");
    command.args([
        "-c",
        &format!("printf '%s' '{payload}'; while :; do :; done"),
    ]);
    let mut helper = HelperProcess::spawn_command(command);
    let pid = helper.id();
    let diagnostic = helper
        .receive_with_timeout(Duration::from_millis(100))
        .expect_err("unterminated protocol payload must time out");
    assert!(diagnostic.contains("timed out"));
    for canary in canaries {
        assert!(
            !diagnostic.contains(canary),
            "timeout diagnostic exposed raw protocol payload"
        );
    }
    assert!(!process_is_alive(pid), "timed-out helper must be reaped");
}

#[test]
fn mysql_helper_one_shot_timeout_diagnostic_omits_raw_protocol_payload() {
    let canaries = [
        "one-shot-key-canary",
        "one-shot-value-canary",
        "one-shot-records-canary",
        "one-shot-hints-canary",
    ];
    let payload = format!(
        "{{\"id\":88,\"event\":\"Record\",\"code\":\"Pending\",\"record\":{{\"key\":\"{}\",\"value\":\"{}\"}},\"records\":[\"{}\"],\"hints\":[\"{}\"]}}",
        canaries[0], canaries[1], canaries[2], canaries[3]
    );
    let mut command = Command::new("/bin/sh");
    command.args([
        "-c",
        &format!("printf '%s' '{payload}'; while :; do :; done"),
    ]);
    let result = run_command_bounded(
        &mut command,
        None,
        Duration::from_millis(100),
        TEST_DIAGNOSTIC_CAP_BYTES,
    );
    assert!(result.timed_out, "unterminated payload must time out");
    let diagnostic = bounded_output_diagnostics(&result);
    for canary in canaries {
        assert!(
            !diagnostic.contains(canary),
            "one-shot timeout diagnostic exposed raw protocol payload"
        );
    }
    assert!(
        !process_is_alive(result.pid),
        "timed-out child must be reaped"
    );
}

#[test]
fn mysql_helper_interactive_drop_reaps_live_child() {
    let helper = HelperProcess::spawn_silent_for_test(Duration::from_secs(1));
    let pid = helper.id();
    drop(helper);
    assert!(
        !process_is_alive(pid),
        "dropped helper child must be reaped"
    );
}

fn response_lines(output: &Output) -> Vec<JsonValue> {
    String::from_utf8(output.stdout.clone())
        .expect("helper stdout is UTF-8 JSONL")
        .lines()
        .map(|line| serde_json::from_str(line).expect("decode helper JSON response"))
        .collect()
}

fn assert_terminal_protocol_error(input: &[u8], expected_code: &str) {
    assert_terminal_protocol_error_with_id(input, expected_code, 1);
}

fn assert_terminal_protocol_error_with_id(input: &[u8], expected_code: &str, expected_id: u64) {
    let output = run_helper(input, HelperEnvironment::Empty);
    assert!(
        !output.status.success(),
        "invalid protocol input must terminate the helper"
    );
    let responses = response_lines(&output);
    assert_eq!(
        responses.len(),
        1,
        "the protocol error must be flushed once"
    );
    assert_eq!(responses[0]["ok"], false);
    assert_eq!(responses[0]["event"], "Error");
    assert_eq!(responses[0]["code"], expected_code);
    assert_eq!(responses[0]["id"], expected_id);
    assert!(responses[0]["pid"].as_u64().is_some_and(|pid| pid > 0));
}

#[test]
fn mysql_helper_protocol_correlates_only_unique_top_level_u64_id() {
    for (input, expected_id) in [
        (r#"{"command":"Crash", "id" : 7}"#, 7),
        (r#"{"command":"Crash","probe":true,"id":8}"#, 8),
        (r#"{"probe":{"id":99},"command":"Crash","id":9}"#, 9),
        (r#"{"id":10,"command":"Crash","id":11}"#, 0),
        (r#"{"command":"Crash"}"#, 0),
        (r#"{"id":"12","command":"Crash"}"#, 0),
    ] {
        let input = format!("{input}\n");
        assert_terminal_protocol_error_with_id(input.as_bytes(), "InvalidJson", expected_id);
    }
}

#[test]
fn mysql_helper_protocol_accepts_only_frozen_commands_and_hex_payloads() {
    let output = run_helper(
        b"{\"id\":1,\"command\":\"Shutdown\"}\n",
        HelperEnvironment::Empty,
    );
    assert!(output.status.success(), "Shutdown must exit successfully");
    let responses = response_lines(&output);
    assert_eq!(responses.len(), 1, "Shutdown must emit one response");
    assert_eq!(responses[0]["id"], 1);
    assert_eq!(responses[0]["ok"], true);
    assert_eq!(responses[0]["event"], "Shutdown");
    assert!(responses[0]["pid"].as_u64().is_some_and(|pid| pid > 0));

    for command in [
        json!({"id": 1, "command": "Begin", "handle": Uuid::nil(), "description": "protocol"}),
        json!({"id": 1, "command": "Get", "handle": Uuid::nil(), "key": "00ff"}),
        json!({"id": 1, "command": "Range", "handle": Uuid::nil(), "start": "00", "end": "ff", "direction": "Forward", "page_size": 1}),
        json!({"id": 1, "command": "Put", "handle": Uuid::nil(), "key": "00", "value": "ff", "precondition": "Any"}),
        json!({"id": 1, "command": "Delete", "handle": Uuid::nil(), "key": "00", "precondition": "Any"}),
        json!({"id": 1, "command": "Commit", "handle": Uuid::nil(), "lose_response": false}),
        json!({"id": 1, "command": "Resolve", "handle": Uuid::nil()}),
    ] {
        let mut encoded = serde_json::to_vec(&command).expect("encode frozen command");
        encoded.push(b'\n');
        assert_terminal_protocol_error(&encoded, "InvalidOrder");
    }

    for forbidden in ["Release", "Pause", "Barrier", "Sleep", "Inject", "Crash"] {
        let input = format!("{{\"id\":1,\"command\":\"{forbidden}\"}}\n");
        assert_terminal_protocol_error(input.as_bytes(), "InvalidJson");
    }
    assert_terminal_protocol_error(
        b"{\"id\":1,\"command\":\"Put\",\"handle\":\"00000000-0000-0000-0000-000000000000\",\"key\":\"not-hex\",\"value\":\"00\",\"precondition\":\"Any\"}\n",
        "InvalidHex",
    );
    assert_terminal_protocol_error(
        b"{\"id\":1,\"command\":\"Get\",\"handle\":\"00000000-0000-0000-0000-000000000000\",\"key\":\"00\",\"unexpected\":true}\n",
        "InvalidJson",
    );
    assert_terminal_protocol_error(
        b"{\"id\":1,\"command\":\"Delete\",\"handle\":\"00000000-0000-0000-0000-000000000000\",\"key\":\"00\",\"precondition\":{\"version\":\"00\",\"unexpected\":true}}\n",
        "InvalidJson",
    );
    assert_terminal_protocol_error_with_id(
        b"{\"id\":1,\"id\":1,\"command\":\"Shutdown\"}\n",
        "InvalidJson",
        0,
    );
    let oversized = format!(
        "{{\"id\":1,\"command\":\"Put\",\"handle\":\"00000000-0000-0000-0000-000000000000\",\"key\":\"00\",\"value\":\"{}\",\"precondition\":\"Any\"}}\n",
        "00".repeat(90_000)
    );
    assert_terminal_protocol_error_with_id(oversized.as_bytes(), "LineTooLong", 0);
    let oversized_key = format!(
        "{{\"id\":1,\"command\":\"Get\",\"handle\":\"00000000-0000-0000-0000-000000000000\",\"key\":\"{}\"}}\n",
        "00".repeat(3_073)
    );
    assert_terminal_protocol_error(oversized_key.as_bytes(), "HexTooLong");

    let canaries = [
        "ordinary-password-canary",
        "mysql://credential-canary@database-canary",
        "database-canary",
        "logical-key-canary",
        "logical-value-canary",
    ];
    let diagnostic_probe = format!(
        "{{\"id\":1,\"command\":\"Crash\",\"password\":\"{}\",\"dsn\":\"{}\",\"database\":\"{}\",\"key\":\"{}\",\"value\":\"{}\"}}\n",
        canaries[0], canaries[1], canaries[2], canaries[3], canaries[4]
    );
    let output = run_helper(diagnostic_probe.as_bytes(), HelperEnvironment::Empty);
    let diagnostics = [output.stdout, output.stderr].concat();
    for canary in canaries {
        assert!(
            !diagnostics
                .windows(canary.len())
                .any(|window| window == canary.as_bytes()),
            "helper diagnostics exposed sensitive protocol material"
        );
    }
}

#[test]
fn mysql_helper_open_failure_shuts_down_runtime_and_exits_deterministically() {
    let output = run_helper(
        b"{\"id\":1,\"command\":\"Open\",\"cluster_id\":\"cross-process-open-failure\"}\n",
        HelperEnvironment::Ordinary {
            database: "missing_cross_process_database",
        },
    );
    assert!(
        !output.status.success(),
        "a store-open failure must retire the helper"
    );
    let responses = response_lines(&output);
    assert_eq!(responses.len(), 1, "the open error must be flushed once");
    assert_eq!(responses[0]["id"], 1);
    assert_eq!(responses[0]["ok"], false);
    assert_eq!(responses[0]["event"], "Error");
    assert_eq!(responses[0]["code"], "OpenFailed");
    let error = responses[0]["error"]
        .as_str()
        .expect("open failure includes the public state-store error");
    let kind = responses[0]["error_kind"]
        .as_str()
        .expect("open failure includes the public error kind");
    assert!(
        error.starts_with(kind),
        "the helper must preserve the original public open error"
    );
    assert!(!error.contains("missing_cross_process_database"));
}

#[test]
fn mysql_cross_process_suite() {
    run_mysql_cross_process_suite();
}

#[test]
fn mysql_helper_response_loss_predispatch_terminal_is_bounded() {
    predispatch_response_loss_case();
}

struct TestDatabase {
    name: String,
}

impl TestDatabase {
    fn provision(case_id: &str) -> Self {
        let script =
            common::repo_root().join("docker/mysql-state-store/provision-test-database.sh");
        let output = Command::new(script)
            .args(["create", case_id])
            .output()
            .expect("run MySQL test database provisioner");
        assert!(
            output.status.success(),
            "MySQL test database provisioner create failed"
        );
        let name = String::from_utf8(output.stdout)
            .expect("database name is UTF-8")
            .trim()
            .to_owned();
        assert!(name.starts_with("novarocks_ss3_"));
        Self { name }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let script =
            common::repo_root().join("docker/mysql-state-store/provision-test-database.sh");
        let status = Command::new(script).args(["drop", &self.name]).status();
        if !status.is_ok_and(|status| status.success()) && !std::thread::panicking() {
            panic!("MySQL test database provisioner drop failed");
        }
    }
}

struct HelperProcess {
    child: Child,
    stdin: ChildStdin,
    responses: mpsc::Receiver<String>,
    stdout_reader: Option<JoinHandle<()>>,
    stdout: Arc<Mutex<CappedCapture>>,
    stderr: Arc<Mutex<CappedCapture>>,
    stderr_reader: Option<JoinHandle<()>>,
    next_id: u64,
}

impl HelperProcess {
    fn spawn_silent_for_test(duration: Duration) -> Self {
        let mut command = Command::new("/bin/sleep");
        command.arg(duration.as_secs_f64().to_string());
        Self::spawn_command(command)
    }

    fn spawn(database: &str) -> Self {
        let mut command = Command::new(HELPER);
        command.env_clear();
        configure_ordinary_helper(&mut command, database);
        Self::spawn_command(command)
    }

    fn spawn_command(mut command: Command) -> Self {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn interactive helper child");
        let stdin = child.stdin.take().expect("take helper stdin");
        let stdout = child.stdout.take().expect("take helper stdout");
        let child_stderr = child.stderr.take().expect("take helper stderr");
        let (sender, responses) = mpsc::channel();
        let stdout_capture = Arc::new(Mutex::new(CappedCapture::empty()));
        let stderr = Arc::new(Mutex::new(CappedCapture::empty()));
        let stdout_reader = spawn_interactive_stdout(stdout, sender, Arc::clone(&stdout_capture));
        let stderr_reader = spawn_interactive_capture(child_stderr, Arc::clone(&stderr));
        Self {
            child,
            stdin,
            responses,
            stdout_reader: Some(stdout_reader),
            stdout: stdout_capture,
            stderr,
            stderr_reader: Some(stderr_reader),
            next_id: 0,
        }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn request(&mut self, mut command: JsonValue) -> JsonValue {
        self.next_id += 1;
        command["id"] = json!(self.next_id);
        self.send(&command);
        let response = self.receive();
        assert_eq!(response["id"], self.next_id);
        assert_eq!(response["ok"], true, "helper request failed");
        response
    }

    fn request_with_timeout(
        &mut self,
        mut command: JsonValue,
        timeout: Duration,
    ) -> Result<JsonValue, String> {
        self.next_id += 1;
        command["id"] = json!(self.next_id);
        self.send(&command);
        let response = self.receive_with_timeout(timeout)?;
        if response["id"] != self.next_id {
            return Err("helper response identifier mismatch".to_owned());
        }
        Ok(response)
    }

    fn send(&mut self, command: &JsonValue) {
        serde_json::to_writer(&mut self.stdin, &command).expect("encode helper request");
        self.stdin.write_all(b"\n").expect("write JSONL delimiter");
        self.stdin.flush().expect("flush helper request");
    }

    fn receive(&mut self) -> JsonValue {
        self.receive_with_timeout(Duration::from_secs(15))
            .unwrap_or_else(|diagnostic| panic!("{diagnostic}"))
    }

    fn receive_with_timeout(&mut self, timeout: Duration) -> Result<JsonValue, String> {
        let line = match self.responses.recv_timeout(timeout) {
            Ok(line) => line,
            Err(_) => {
                self.terminate_and_join();
                return Err(format!(
                    "helper response timed out: {}",
                    self.safe_diagnostics()
                ));
            }
        };
        serde_json::from_str(&line).map_err(|_| "helper returned invalid JSON".to_owned())
    }

    fn request_unchecked(&mut self, mut command: JsonValue) -> JsonValue {
        self.next_id += 1;
        command["id"] = json!(self.next_id);
        self.send(&command);
        let response = self.receive();
        assert_eq!(response["id"], self.next_id);
        response
    }

    fn send_with_id(&mut self, id: u64, mut command: JsonValue) -> JsonValue {
        command["id"] = json!(id);
        self.send(&command);
        self.receive()
    }

    fn open(&mut self, cluster_id: &str) -> JsonValue {
        self.request(json!({"command": "Open", "cluster_id": cluster_id}))
    }

    fn shutdown(mut self) {
        let response = self.request(json!({"command": "Shutdown"}));
        assert_eq!(response["event"], "Shutdown");
        let status = self.child.wait().expect("wait for helper shutdown");
        assert!(
            status.success(),
            "helper shutdown failed: {}",
            self.safe_diagnostics()
        );
        self.join_readers();
        assert!(self.safe_stderr().is_empty(), "unexpected helper stderr");
    }

    fn wait_for_failure(&mut self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("poll helper exit") {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                self.terminate_and_join();
                panic!(
                    "helper must exit after a terminal protocol error: {}",
                    self.safe_diagnostics()
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(!status.success(), "terminal helper error must be nonzero");
        self.join_readers();
    }

    fn safe_stderr(&self) -> String {
        let stderr = self.stderr.lock().expect("lock helper stderr");
        render_capture(&stderr)
    }

    fn safe_diagnostics(&self) -> String {
        let stdout = self.stdout.lock().expect("lock helper stdout");
        let stderr = self.stderr.lock().expect("lock helper stderr");
        format!(
            "stdout={} stderr={}",
            protocol_stdout_summary(&stdout.bytes, stdout.truncated),
            render_capture(&stderr)
        )
    }

    fn terminate_and_join(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.join_readers();
    }

    fn join_readers(&mut self) {
        if let Some(reader) = self.stdout_reader.take() {
            reader.join().expect("join helper stdout reader");
        }
        if let Some(reader) = self.stderr_reader.take() {
            reader.join().expect("join helper stderr reader");
        }
    }
}

fn required_process_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("required MySQL fixture variable is missing"))
}

fn configure_ordinary_helper(command: &mut Command, database: &str) {
    let password_env = required_process_env("NOVAROCKS_MYSQL_PASSWORD_ENV");
    let password = required_process_env(&password_env);
    command
        .env(
            "NOVAROCKS_MYSQL_HOST",
            required_process_env("NOVAROCKS_MYSQL_HOST"),
        )
        .env(
            "NOVAROCKS_MYSQL_PORT",
            required_process_env("NOVAROCKS_MYSQL_PORT"),
        )
        .env(
            "NOVAROCKS_MYSQL_USERNAME",
            required_process_env("NOVAROCKS_MYSQL_USERNAME"),
        )
        .env("NOVAROCKS_MYSQL_PASSWORD_ENV", &password_env)
        .env(&password_env, password)
        .env("NOVAROCKS_MYSQL_DATABASE", database);
}

impl Drop for HelperProcess {
    fn drop(&mut self) {
        self.terminate_and_join();
    }
}

fn render_capture(capture: &CappedCapture) -> String {
    let mut rendered = redact_test_material(&String::from_utf8_lossy(&capture.bytes));
    if capture.truncated {
        rendered.push_str("<truncated>");
    }
    rendered
}

fn bounded_output_diagnostics(result: &BoundedCommandOutput) -> String {
    let stderr = CappedCapture {
        bytes: result.output.stderr.clone(),
        truncated: result.stderr_truncated,
    };
    format!(
        "stdout={} stderr={}",
        protocol_stdout_summary(&result.output.stdout, result.stdout_truncated),
        render_capture(&stderr)
    )
}

fn protocol_stdout_summary(bytes: &[u8], truncated: bool) -> String {
    let metadata = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_slice::<JsonValue>(line).ok())
        .filter_map(|value| {
            let id = value.get("id").and_then(JsonValue::as_u64)?;
            let event = safe_protocol_token(value.get("event")?)?;
            let code = value.get("code").and_then(safe_protocol_token);
            Some((id, event.to_owned(), code.map(str::to_owned)))
        })
        .next_back();
    let mut summary = format!("bytes={} truncated={truncated}", bytes.len());
    if let Some((id, event, code)) = metadata {
        summary.push_str(&format!(" last_id={id} last_event={event}"));
        if let Some(code) = code {
            summary.push_str(&format!(" last_code={code}"));
        }
    }
    summary
}

fn safe_protocol_token(value: &JsonValue) -> Option<&str> {
    let token = value.as_str()?;
    (!token.is_empty()
        && token.len() <= 64
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')))
    .then_some(token)
}

fn redact_test_material(raw: &str) -> String {
    let mut redacted = raw.to_owned();
    for name in [
        "NOVAROCKS_MYSQL_PASSWORD",
        "NOVA_MYSQL_PROVISIONER_PASSWORD",
        "NOVAROCKS_MYSQL_DATABASE",
    ] {
        if let Ok(value) = std::env::var(name)
            && !value.is_empty()
        {
            redacted = redacted.replace(&value, "<redacted>");
        }
    }
    redacted
}

fn run_mysql_cross_process_suite() {
    predispatch_response_loss_case();
    same_key_cas_case();
    write_skew_case();
    range_phantom_case();
    response_loss_resolution_case();
    foreign_handle_is_not_resolvable_case();
    cluster_and_database_mismatch_case();
    request_identifier_case();
}

/// A commit whose response is lost before dispatch must not publish, and the
/// store must stay usable afterwards.
///
/// The old version opened by asking a second process to resolve an identifier
/// nobody had issued yet, and expected "NotCommitted". That question cannot be
/// posed any more -- an attempt identity is minted by one open instance and is
/// meaningless to another -- so only the physical half of the case remains.
fn predispatch_response_loss_case() {
    let (_database, mut left, right) = open_pair("predispatch-loss");
    let handle = Uuid::now_v7();
    begin(&mut left, handle, "predispatch response loss");
    put(
        &mut left,
        handle,
        b"predispatch/key",
        b"must-not-publish",
        json!("Any"),
    );
    let response = left
        .request_with_timeout(
            json!({
                "command": "Commit",
                "handle": handle,
                "lose_response": true,
            }),
            Duration::from_secs(2),
        )
        .expect("predispatch terminal commit must return before hook deadline");
    assert_eq!(response["event"], "Commit");
    assert_ne!(response["outcome"], "Committed");
    seed(&mut left, &[(b"predispatch/next", b"committed")]);
    left.shutdown();
    right.shutdown();
}

fn open_pair(case_id: &str) -> (TestDatabase, HelperProcess, HelperProcess) {
    let database = TestDatabase::provision(case_id);
    let mut left = HelperProcess::spawn(&database.name);
    let mut right = HelperProcess::spawn(&database.name);
    assert_no_provisioner_environment(left.id());
    assert_no_provisioner_environment(right.id());
    let left_open = left.open("mysql-cross-process-cluster");
    let right_open = right.open("mysql-cross-process-cluster");
    assert_eq!(left_open["event"], "Opened");
    assert_eq!(right_open["event"], "Opened");
    assert_eq!(left_open["pid"], left.id());
    assert_eq!(right_open["pid"], right.id());
    assert_ne!(
        left.id(),
        right.id(),
        "helpers must be distinct exec processes"
    );
    (database, left, right)
}

fn assert_no_provisioner_environment(pid: u32) {
    let environment = process_environment(pid);
    for forbidden in [
        b"NOVA_MYSQL_PROVISIONER_USERNAME".as_slice(),
        b"NOVA_MYSQL_PROVISIONER_PASSWORD".as_slice(),
        b"NOVA_MYSQL_COMPOSE_ENV".as_slice(),
    ] {
        assert!(
            !contains_bytes(&environment, forbidden),
            "helper inherited provisioner environment"
        );
    }
}

fn process_environment(pid: u32) -> Vec<u8> {
    #[cfg(target_os = "linux")]
    let environment =
        std::fs::read(format!("/proc/{pid}/environ")).expect("read helper process environment");
    #[cfg(not(target_os = "linux"))]
    let output = Command::new("ps")
        .args(["eww", "-p", &pid.to_string(), "-o", "command="])
        .output()
        .expect("inspect helper process environment");
    #[cfg(not(target_os = "linux"))]
    assert!(
        output.status.success(),
        "helper process must remain observable"
    );
    #[cfg(not(target_os = "linux"))]
    let environment = output.stdout;
    environment
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn begin(helper: &mut HelperProcess, handle: Uuid, description: &str) {
    let response = helper.request(json!({
        "command": "Begin",
        "handle": handle,
        "description": description,
    }));
    assert_eq!(response["event"], "Begun");
}

fn get(helper: &mut HelperProcess, handle: Uuid, key: &[u8]) -> JsonValue {
    let response = helper.request(json!({
        "command": "Get",
        "handle": handle,
        "key": hex::encode(key),
    }));
    assert_eq!(response["event"], "Get");
    response["record"].clone()
}

fn range(helper: &mut HelperProcess, handle: Uuid, start: &[u8], end: &[u8]) -> JsonValue {
    let response = helper.request(json!({
        "command": "Range",
        "handle": handle,
        "start": hex::encode(start),
        "end": hex::encode(end),
        "direction": "Forward",
        "page_size": 32,
    }));
    assert_eq!(response["event"], "Range");
    response
}

fn put(
    helper: &mut HelperProcess,
    handle: Uuid,
    key: &[u8],
    value: &[u8],
    precondition: JsonValue,
) {
    let response = helper.request(json!({
        "command": "Put",
        "handle": handle,
        "key": hex::encode(key),
        "value": hex::encode(value),
        "precondition": precondition,
    }));
    assert_eq!(response["event"], "Staged");
}

fn commit(helper: &mut HelperProcess, handle: Uuid, lose_response: bool) -> JsonValue {
    let response = helper.request(json!({
        "command": "Commit",
        "handle": handle,
        "lose_response": lose_response,
    }));
    assert_eq!(response["event"], "Commit");
    response
}

fn resolve(helper: &mut HelperProcess, handle: Uuid) -> JsonValue {
    let response = helper.request(json!({
        "command": "Resolve",
        "handle": handle,
    }));
    assert_eq!(response["event"], "Resolve");
    response
}

fn seed(helper: &mut HelperProcess, rows: &[(&[u8], &[u8])]) {
    let handle = Uuid::now_v7();
    begin(helper, handle, "cross-process seed");
    for (key, value) in rows {
        put(helper, handle, key, value, json!("Any"));
    }
    assert_eq!(commit(helper, handle, false)["outcome"], "Committed");
}

fn same_key_cas_case() {
    let (_database, mut left, mut right) = open_pair("cross-process-cas");
    seed(&mut left, &[(b"cas/key", b"seed")]);
    let left_id = Uuid::now_v7();
    let right_id = Uuid::now_v7();
    begin(&mut left, left_id, "same-key CAS left");
    begin(&mut right, right_id, "same-key CAS right");
    let left_record = get(&mut left, left_id, b"cas/key");
    let right_record = get(&mut right, right_id, b"cas/key");
    assert_eq!(left_record["value"], hex::encode(b"seed"));
    assert_eq!(right_record["version"], left_record["version"]);
    put(
        &mut left,
        left_id,
        b"cas/key",
        b"left",
        json!({"version": left_record["version"]}),
    );
    put(
        &mut right,
        right_id,
        b"cas/key",
        b"right",
        json!({"version": right_record["version"]}),
    );
    assert_eq!(commit(&mut left, left_id, false)["outcome"], "Committed");
    assert_eq!(commit(&mut right, right_id, false)["outcome"], "Conflict");
    left.shutdown();
    right.shutdown();
}

fn write_skew_case() {
    let (_database, mut left, mut right) = open_pair("cross-process-skew");
    seed(&mut left, &[(b"skew/left", b"on"), (b"skew/right", b"on")]);
    let left_id = Uuid::now_v7();
    let right_id = Uuid::now_v7();
    begin(&mut left, left_id, "write-skew left");
    begin(&mut right, right_id, "write-skew right");
    assert_eq!(
        get(&mut left, left_id, b"skew/right")["value"],
        hex::encode(b"on")
    );
    assert_eq!(
        get(&mut right, right_id, b"skew/left")["value"],
        hex::encode(b"on")
    );
    put(&mut left, left_id, b"skew/left", b"off", json!("Present"));
    put(
        &mut right,
        right_id,
        b"skew/right",
        b"off",
        json!("Present"),
    );
    assert_eq!(commit(&mut left, left_id, false)["outcome"], "Committed");
    assert_eq!(commit(&mut right, right_id, false)["outcome"], "Conflict");
    left.shutdown();
    right.shutdown();
}

fn range_phantom_case() {
    let (_database, mut left, mut right) = open_pair("cross-process-phantom");
    let reader = Uuid::now_v7();
    begin(&mut left, reader, "range phantom reader");
    assert_eq!(
        range(&mut left, reader, b"phantom/", b"phantom0")["records"],
        json!([])
    );
    let writer = Uuid::now_v7();
    begin(&mut right, writer, "range phantom writer");
    put(
        &mut right,
        writer,
        b"phantom/key",
        b"inserted",
        json!("Any"),
    );
    assert_eq!(commit(&mut right, writer, false)["outcome"], "Committed");
    put(
        &mut left,
        reader,
        b"phantom/result",
        b"must-not-publish",
        json!("Any"),
    );
    assert_eq!(commit(&mut left, reader, false)["outcome"], "Conflict");
    left.shutdown();
    right.shutdown();
}

/// A lost commit response is still resolvable from durable evidence.
///
/// The resolution is asked of `left`, the process that issued the attempt.
/// `right` stays in the case because the write has to survive being read by a
/// separate process, which is the cross-process fact worth checking; it is no
/// longer asked to adjudicate somebody else's attempt.
fn response_loss_resolution_case() {
    let (_database, mut left, mut right) = open_pair("cross-process-unknown");
    let handle = Uuid::now_v7();
    begin(&mut left, handle, "response-loss commit");
    put(
        &mut left,
        handle,
        b"unknown/key",
        b"committed",
        json!("Any"),
    );
    assert_eq!(commit(&mut left, handle, true)["outcome"], "CommitUnknown");
    let resolution = resolve(&mut left, handle);
    assert_eq!(resolution["resolution"], "Committed");
    assert!(
        resolution["revision"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    // The row itself is physically durable and visible to the other process.
    let reader = Uuid::now_v7();
    begin(&mut right, reader, "response-loss cross-process read");
    assert_eq!(
        get(&mut right, reader, b"unknown/key")["value"],
        hex::encode(b"committed")
    );
    left.shutdown();
    right.shutdown();
}

/// A handle one process opened is meaningless to another.
///
/// This replaces the old "tombstoned transaction reuse" case, and the change
/// feed's ordering case with it. The first asked a second process to resolve an
/// identifier the first had used and expected a durable `NotCommitted`
/// tombstone; both halves are gone by construction, because an attempt identity
/// carries the instance scope that issued it and nothing writes a tombstone for
/// an identity it never saw. What is still worth pinning is that the second
/// process says so plainly instead of inventing a verdict.
fn foreign_handle_is_not_resolvable_case() {
    let (_database, mut left, mut right) = open_pair("cross-process-foreign-handle");
    let handle = Uuid::now_v7();
    begin(&mut left, handle, "foreign handle");
    put(
        &mut left,
        handle,
        b"foreign/key",
        b"committed",
        json!("Any"),
    );
    assert_eq!(commit(&mut left, handle, false)["outcome"], "Committed");

    // Same handle value, different process: not a verdict, an error.
    let response = right.request_unchecked(json!({
        "command": "Resolve",
        "handle": handle,
    }));
    assert_eq!(response["ok"], false);
    assert_eq!(response["code"], "UnknownAttempt");
    right.wait_for_failure();
    assert!(right.safe_stderr().is_empty());

    // The issuing process still answers for its own attempt.
    assert_eq!(resolve(&mut left, handle)["resolution"], "Committed");
    left.shutdown();
}

fn cluster_and_database_mismatch_case() {
    let database = TestDatabase::provision("cross-process-mismatch");
    let mut owner = HelperProcess::spawn(&database.name);
    owner.open("mysql-cross-process-cluster");

    let mut wrong_cluster = HelperProcess::spawn(&database.name);
    let response = wrong_cluster.request_unchecked(json!({
        "command": "Open",
        "cluster_id": "wrong-cross-process-cluster",
    }));
    assert_eq!(response["ok"], false);
    assert_eq!(response["code"], "OpenFailed");
    wrong_cluster.wait_for_failure();
    assert!(wrong_cluster.safe_stderr().is_empty());

    let mut missing_database = HelperProcess::spawn("novarocks_ss3_missing_cross_process");
    let response = missing_database.request_unchecked(json!({
        "command": "Open",
        "cluster_id": "mysql-cross-process-cluster",
    }));
    assert_eq!(response["ok"], false);
    assert_eq!(response["code"], "OpenFailed");
    missing_database.wait_for_failure();
    assert!(missing_database.safe_stderr().is_empty());
    owner.shutdown();
}

fn request_identifier_case() {
    let database = TestDatabase::provision("cross-process-ids");
    let mut duplicate = HelperProcess::spawn(&database.name);
    duplicate.open("mysql-cross-process-cluster");
    let response =
        duplicate.send_with_id(1, json!({"command": "Resolve", "handle": Uuid::now_v7()}));
    assert_eq!(response["ok"], false);
    assert_eq!(response["code"], "DuplicateId");
    duplicate.wait_for_failure();
    assert!(duplicate.safe_stderr().is_empty());

    let mut out_of_order = HelperProcess::spawn(&database.name);
    out_of_order.open("mysql-cross-process-cluster");
    let response =
        out_of_order.send_with_id(3, json!({"command": "Resolve", "handle": Uuid::now_v7()}));
    assert_eq!(response["ok"], false);
    assert_eq!(response["code"], "OutOfOrderId");
    out_of_order.wait_for_failure();
    assert!(out_of_order.safe_stderr().is_empty());
}
