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

//! Real process-run retention. Every helper Child is owned and actually waited
//! by its outer test; each managed payload has an original monitor Final.
//! Helpers clear nextest context deliberately to exercise ordinary process runs.
use novarocks_test_support::{ManagedCommand, ManagedProcess, ReadyMarker, ensure_run_retention};
use serde_json::{Value, json};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const WATCHDOG: Duration = Duration::from_secs(30);
const RETENTION_CHILD_TEST: &str =
    "ordinary_default_keeps_three_completed_runs_and_excludes_an_active_pin";

struct Evidence {
    root: PathBuf,
    next_helper: usize,
}

impl Evidence {
    fn new(name: &str) -> Self {
        let base = std::env::var_os("NOVAROCKS_RETENTION_TEST_EVIDENCE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("novarocks-retention-tests"));
        let root = base.join(format!(
            "{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join("home")).unwrap();
        fs::create_dir(root.join("state")).unwrap();
        eprintln!("retention test evidence: {}", root.display());
        Self {
            root,
            next_helper: 0,
        }
    }

    fn spawn(&mut self, mode: &str, formal: Option<&Path>) -> Helper {
        let directory = self
            .root
            .join(format!("helper-{:03}-{mode}", self.next_helper));
        self.next_helper += 1;
        fs::create_dir(&directory).unwrap();
        let executable = std::env::current_exe().unwrap();
        let mut command = Command::new(&executable);
        command
            .args(["--exact", RETENTION_CHILD_TEST, "--nocapture"])
            .env_clear()
            .env("HOME", self.root.join("home"))
            .env("XDG_STATE_HOME", self.root.join("state"))
            .env("RETENTION_HELPER_DIRECTORY", &directory)
            .env("RETENTION_HELPER_MODE", mode)
            .stdin(Stdio::null())
            .stdout(Stdio::from(
                File::create(directory.join("stdout.log")).unwrap(),
            ))
            .stderr(Stdio::from(
                File::create(directory.join("stderr.log")).unwrap(),
            ));
        if let Some(formal) = formal {
            command.env("NOVAROCKS_TEST_RUN_ROOT", formal);
        }
        let retention_count = (mode == "failed-initialization").then_some("0");
        if let Some(count) = retention_count {
            command.env("NOVAROCKS_TEST_RUN_RETENTION_COUNT", count);
        }
        fs::write(
            directory.join("command.json"),
            serde_json::to_vec_pretty(&json!({
                "program": executable,
                "arguments": ["--exact", RETENTION_CHILD_TEST, "--nocapture"],
                "environment_cleared": true,
                "environment": {
                    "HOME": self.root.join("home"),
                    "XDG_STATE_HOME": self.root.join("state"),
                    "RETENTION_HELPER_DIRECTORY": directory,
                    "RETENTION_HELPER_MODE": mode,
                    "NOVAROCKS_TEST_RUN_ROOT": formal,
                    "NOVAROCKS_TEST_RUN_RETENTION_COUNT": retention_count,
                },
            }))
            .unwrap(),
        )
        .unwrap();
        let child = command.spawn().unwrap();
        let mut helper = Helper {
            child,
            directory,
            reaped: false,
            ready: Value::Null,
        };
        let deadline = Instant::now() + WATCHDOG;
        loop {
            if let Ok(bytes) = fs::read(helper.directory.join("ready.json"))
                && let Ok(value) = serde_json::from_slice(&bytes)
            {
                helper.ready = value;
                return helper;
            }
            if let Some(status) = helper.child.try_wait().unwrap() {
                helper.reaped = true;
                panic!(
                    "helper exited before readiness: {status}; {}",
                    helper.diagnostics()
                );
            }
            assert!(
                Instant::now() < deadline,
                "helper readiness watchdog; {}",
                helper.diagnostics()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn completed(&mut self, mode: &str) -> PathBuf {
        let mut helper = self.spawn(mode, None);
        let root = helper.run_root();
        helper.finish();
        root
    }
}

struct Helper {
    child: Child,
    directory: PathBuf,
    reaped: bool,
    ready: Value,
}

impl Helper {
    fn run_root(&self) -> PathBuf {
        PathBuf::from(self.ready["run_root"].as_str().expect("helper run root"))
    }

    fn diagnostics(&self) -> String {
        format!(
            "directory={} ready={} stdout={} stderr={}",
            self.directory.display(),
            self.ready,
            fs::read_to_string(self.directory.join("stdout.log")).unwrap_or_default(),
            fs::read_to_string(self.directory.join("stderr.log")).unwrap_or_default(),
        )
    }

    fn finish(&mut self) {
        fs::write(self.directory.join("finish"), b"finish\n").unwrap();
        let deadline = Instant::now() + WATCHDOG;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                self.reaped = true;
                fs::write(self.directory.join("exit.txt"), format!("{status}\n")).unwrap();
                assert!(
                    status.success(),
                    "helper failed: {status}; {}",
                    self.diagnostics()
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "helper actual-exit watchdog; {}",
                self.diagnostics()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Helper {
    fn drop(&mut self) {
        if !self.reaped {
            let kill = self.child.kill();
            let wait = self.child.wait();
            eprintln!(
                "outer helper cleanup: kill={kill:?} actual_wait={wait:?}; {}",
                self.diagnostics()
            );
        }
    }
}

fn eventual(description: &str, evidence: &Evidence, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WATCHDOG;
    loop {
        if predicate() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{description}; evidence={}",
            evidence.root.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn actual_final(launch: &Path) -> Value {
    let record = fs::read_to_string(launch.join("record.jsonl")).unwrap();
    assert!(
        record.ends_with('\n'),
        "incomplete original record at {}",
        launch.display()
    );
    let value: Value = serde_json::from_str(record.lines().last().unwrap()).unwrap();
    assert_eq!(value["event"], "Final", "{record}");
    assert_eq!(value["cleanup_complete"], true, "{record}");
    assert_eq!(value["owner_lost"], false, "{record}");
    assert!(
        value["status_raw"].is_i64(),
        "actual original status is missing: {record}"
    );
    assert_eq!(value["errors"], json!([]), "{record}");
    value
}

#[test]
fn ordinary_default_keeps_three_completed_runs_and_excludes_an_active_pin() {
    if std::env::var_os("RETENTION_HELPER_DIRECTORY").is_some() {
        retention_process_helper();
        return;
    }
    let mut evidence = Evidence::new("default-three-active-pin");
    let oldest = evidence.completed("forgotten");
    let mut active = evidence.spawn("forgotten", None);
    let active_root = active.run_root();
    let mut completed = vec![];
    for _ in 0..4 {
        completed.push(evidence.completed("forgotten"));
    }
    let mut collector = evidence.spawn("ensure", None);
    eventual("oldest completed run was not collected", &evidence, || {
        !oldest.exists() && !completed[0].exists()
    });
    assert!(
        active_root.exists(),
        "the held process's active run pin was ignored"
    );
    for root in &completed[1..] {
        assert!(
            root.exists(),
            "newest diagnostic run disappeared: {}",
            root.display()
        );
    }
    active.finish();
    collector.finish();
    let mut collector = evidence.spawn("ensure", None);
    eventual(
        "completed old active run was not collected after its real owner exited",
        &evidence,
        || !active_root.exists(),
    );
    collector.finish();
}

#[test]
fn released_empty_runs_do_not_displace_the_three_diagnostic_runs() {
    let mut evidence = Evidence::new("empty-roots");
    let retained = (0..3)
        .map(|_| evidence.completed("forgotten"))
        .collect::<Vec<_>>();
    let empty = (0..4)
        .map(|_| evidence.completed("released"))
        .collect::<Vec<_>>();
    let mut collector = evidence.spawn("ensure", None);
    eventual(
        "safely released empty roots were not collected",
        &evidence,
        || empty.iter().all(|root| !root.exists()),
    );
    for root in retained {
        assert!(
            root.exists(),
            "empty root displaced diagnostic run: {}",
            root.display()
        );
    }
    collector.finish();
}

#[test]
fn formal_output_survives_explicit_release_and_later_ordinary_collection() {
    let mut evidence = Evidence::new("formal-retention");
    // Place it inside the collector's namespace to exercise its formal exclusion.
    let formal = evidence
        .root
        .join("state/novarocks/test-runs/process/formal-evidence");
    fs::create_dir_all(&formal).unwrap();
    fs::write(formal.join(".formal"), b"schema=1\n").unwrap();
    let mut helper = evidence.spawn("released", Some(&formal));
    let launch = PathBuf::from(helper.ready["launch"].as_str().unwrap());
    assert_eq!(helper.run_root(), formal);
    let final_before = actual_final(&launch);
    let record_before = fs::read(launch.join("record.jsonl")).unwrap();
    helper.finish();
    for _ in 0..5 {
        evidence.completed("forgotten");
    }
    let mut collector = evidence.spawn("ensure", None);
    // Wait for an actual ordinary deletion before assessing formal exclusion.
    let ordinary = fs::read_dir(formal.parent().unwrap())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path != &formal && path.join(".ordinary").is_file())
        .collect::<Vec<_>>();
    assert!(ordinary.len() >= 3);
    eventual("ordinary collection never completed", &evidence, || {
        fs::read_dir(formal.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().join("monitor").is_dir() && entry.path().join(".ordinary").is_file()
            })
            .count()
            == 3
    });
    assert_eq!(
        fs::read(launch.join("record.jsonl")).unwrap(),
        record_before
    );
    assert_eq!(actual_final(&launch), final_before);
    assert!(launch.join("stdout.log").is_file());
    collector.finish();
}

#[test]
fn incomplete_record_parser_excludes_original_material_from_collection() {
    let mut evidence = Evidence::new("incomplete-record-parser");
    let mut helper = evidence.spawn("forgotten", None);
    let root = helper.run_root();
    let launch = PathBuf::from(helper.ready["launch"].as_str().unwrap());
    actual_final(&launch);
    helper.finish();
    let record_path = launch.join("record.jsonl");
    let original = fs::read(&record_path).unwrap();
    fs::write(
        evidence.root.join("original-complete-record.jsonl"),
        &original,
    )
    .unwrap();
    // Parser-only corruption AFTER the actual original Child was waited. No
    // synthetic Final or claim that an unwaited OS process completed is written.
    let partial = &original[..original.len() - 1];
    fs::write(&record_path, partial).unwrap();
    let mut completed = vec![];
    for _ in 0..4 {
        completed.push(evidence.completed("forgotten"));
    }
    let mut collector = evidence.spawn("ensure", None);
    eventual("ordinary complete run was not collected", &evidence, || {
        !completed[0].exists()
    });
    assert!(
        root.exists(),
        "incomplete original record was treated as complete"
    );
    assert_eq!(fs::read(record_path).unwrap(), partial);
    collector.finish();
}

#[test]
fn failed_process_initialization_stays_rejected_on_same_process_retry() {
    let mut evidence = Evidence::new("failed-initialization");
    let mut helper = evidence.spawn("failed-initialization", None);
    let root = helper.run_root();
    assert!(
        root.is_dir(),
        "the original failed initialization root was not retained"
    );
    assert!(!root.join(".process-ready").exists());
    assert!(
        !root.join("monitor").exists(),
        "rejected initialization registered a launch"
    );
    assert!(!helper.directory.join("payload-born").exists());
    let errors = &helper.ready["initialization_errors"];
    assert!(
        errors["first"]
            .as_str()
            .unwrap()
            .contains("must be positive")
    );
    assert!(!errors["second"].as_str().unwrap().is_empty());
    assert!(!errors["spawn"].as_str().unwrap().is_empty());
    helper.finish();
    assert!(
        root.is_dir(),
        "failed initialization evidence disappeared after actual helper wait"
    );
    assert!(!root.join(".process-ready").exists());
    assert!(!root.join("monitor").exists());
}

fn retention_process_helper() {
    let directory = PathBuf::from(
        std::env::var_os("RETENTION_HELPER_DIRECTORY").expect("outer helper directory"),
    );
    let mode = std::env::var("RETENTION_HELPER_MODE").expect("outer helper mode");
    assert!(
        std::env::var_os("NEXTEST_RUN_ID").is_none(),
        "ordinary helper must not inherit a native run identity"
    );
    let mut initialization_errors = None;
    let (root, launch) = if mode == "failed-initialization" {
        assert_eq!(
            std::env::var("NOVAROCKS_TEST_RUN_RETENTION_COUNT").unwrap(),
            "0"
        );
        let first = ensure_run_retention(Instant::now() + WATCHDOG)
            .expect_err("zero retention count must reject original initialization");
        assert!(first.to_string().contains("must be positive"), "{first}");
        let second = ensure_run_retention(Instant::now() + WATCHDOG)
            .expect_err("same-process retry must not adopt the partial failed root");
        let born = directory.join("payload-born");
        let mut command = ManagedCommand::new("/bin/sh");
        command.args([
            "-c",
            "printf born > \"$1\"; trap 'exit 0' TERM; printf 'REJECTED_READY\\n'; while :; do sleep 1; done",
            "sh",
        ]).arg(&born);
        let spawn = match ManagedProcess::spawn(
            "must remain rejected after failed initialization".into(),
            command,
            ReadyMarker::StdoutContains("REJECTED_READY".into()),
            WATCHDOG,
            directory.join("rejected-output.locator"),
        ) {
            Err(error) => format!("{error:#}"),
            Ok(mut process) => {
                let cleanup = process.stop();
                panic!("failed initialization admitted a payload; actual cleanup={cleanup:?}");
            }
        };
        assert!(
            !born.exists(),
            "rejected initialization executed the payload"
        );
        let parent = PathBuf::from(std::env::var_os("XDG_STATE_HOME").unwrap())
            .join("novarocks/test-runs/process");
        let roots = fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(
            roots.len(),
            1,
            "failed retry must preserve the same single process root"
        );
        let root = roots.into_iter().next().unwrap();
        assert!(!root.join(".process-ready").exists());
        assert!(!root.join("monitor").exists());
        initialization_errors =
            Some(json!({"first":first.to_string(),"second":second.to_string(),"spawn":spawn}));
        (root, None)
    } else if mode == "ensure" {
        ensure_run_retention(Instant::now() + WATCHDOG).unwrap();
        let parent = PathBuf::from(std::env::var_os("XDG_STATE_HOME").unwrap())
            .join("novarocks/test-runs/process");
        let own_id = format!("-{}-", std::process::id());
        let roots = fs::read_dir(parent)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(&own_id))
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        assert_eq!(
            roots.len(),
            1,
            "ensure helper must own exactly one process run"
        );
        (roots.into_iter().next().unwrap(), None)
    } else {
        assert!(
            mode == "forgotten" || mode == "released",
            "unknown helper mode: {mode}"
        );
        let mut command = ManagedCommand::new("/bin/sh");
        command.args([
            "-c",
            "trap 'exit 0' TERM; printf 'RETENTION_READY\\n'; while :; do sleep 1; done",
        ]);
        let mut process = ManagedProcess::spawn(
            "retention original payload".into(),
            command,
            ReadyMarker::StdoutContains("RETENTION_READY".into()),
            WATCHDOG,
            directory.join("output.locator"),
        )
        .unwrap();
        let launch = process.diagnostic_root().to_owned();
        let root = launch.parent().unwrap().parent().unwrap().to_owned();
        process.stop().unwrap();
        actual_final(&launch);
        if mode == "released" {
            process.release_diagnostics().unwrap();
        }
        drop(process);
        if mode == "released" && std::env::var_os("NOVAROCKS_TEST_RUN_ROOT").is_none() {
            assert!(
                !launch.exists(),
                "released material still has an unexpected owner"
            );
        }
        (root, Some(launch))
    };
    fs::write(
        directory.join("ready.json"),
        serde_json::to_vec_pretty(&json!({"run_root": root, "launch": launch, "mode": mode, "initialization_errors": initialization_errors}))
            .unwrap(),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    while !directory.join("finish").exists() {
        assert!(
            Instant::now() < deadline,
            "outer owner never released retention helper"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
