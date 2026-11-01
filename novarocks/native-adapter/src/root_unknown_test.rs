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

//! Process custody for existing Root tests that intentionally prove Unknown.
//!
//! Such a Node cannot honestly drain. The same exact case runs under the
//! existing independent process monitor; only completed original assertions
//! authorize its receipt and intentional process termination. The parent
//! checks that receipt, SIGABRT and the monitor's actual group exit. This is
//! neither a healthy Node join nor a fabricated physical completion.

use novarocks_test_support::{ManagedCommand, capture_command_until};
use std::{
    fs::File,
    io::Write,
    os::unix::process::ExitStatusExt,
    time::{Duration, Instant},
};

const CHILD_CASE: &str = "NOVAROCKS_NATIVE_ROOT_UNKNOWN_CHILD";
const RECEIPT: &str = "NOVAROCKS_ROOT_UNKNOWN_ASSERTIONS_COMPLETED";

/// Returns true only in the independently monitored exact child case.
/// The original test returns immediately when this returns false.
pub(crate) fn enter() -> bool {
    let thread = std::thread::current();
    let case = thread.name().expect("original named Rust test");
    if let Some(child) = std::env::var_os(CHILD_CASE) {
        assert_eq!(
            child, case,
            "Unknown child must run its exact original case"
        );
        return true;
    }
    let mut command = ManagedCommand::new(std::env::current_exe().unwrap());
    command.args(["--exact", case, "--nocapture", "--test-threads=1"]);
    command.env(CHILD_CASE, case);
    let started = Instant::now();
    let complete = started + Duration::from_secs(30);
    // Declared payload/complete lifetimes activate the monitor's original
    // process-group census while the unreaped leader still pins its PGID.
    command.terminate_at(started + Duration::from_secs(20), complete);
    let output = tempfile::tempdir().unwrap();
    let outcome = capture_command_until(
        command,
        File::create(output.path().join("stdout.log")).unwrap(),
        File::create(output.path().join("stderr.log")).unwrap(),
        complete,
    )
    .expect("independent Unknown process monitor must return the original outcome");
    assert!(
        !outcome.timed_out,
        "Unknown case timed out: {}",
        outcome.diagnostic_root().display()
    );
    assert!(
        outcome.failures.is_empty(),
        "Unknown process monitor failures: {:?}",
        outcome.failures
    );
    let expected = format!("{RECEIPT} {case}");
    let stderr = String::from_utf8_lossy(&outcome.output.stderr);
    assert!(
        stderr.lines().any(|line| line == expected),
        "original Unknown assertions did not complete; material={}\n{stderr}",
        outcome.diagnostic_root().display()
    );
    assert_eq!(
        outcome.output.status.signal(),
        Some(libc::SIGABRT),
        "Unknown owner must end with its intentional process abort"
    );
    assert!(
        outcome.group_exit_observed(),
        "the monitor must observe the original process group gone"
    );
    // Formal wrappers retain original Pending/Final and logs independently.
    outcome.release_diagnostics().unwrap();
    false
}

/// Call after every original Unknown assertion, while its original holders are
/// still alive. Returning from the test would incorrectly drop that Node.
pub(crate) fn complete_and_abort() -> ! {
    let thread = std::thread::current();
    let case = thread.name().expect("original named Rust test");
    assert_eq!(
        std::env::var_os(CHILD_CASE).as_deref(),
        Some(std::ffi::OsStr::new(case))
    );
    let mut stderr = std::io::stderr().lock();
    writeln!(stderr, "{RECEIPT} {case}").unwrap();
    stderr.flush().unwrap();
    std::process::abort();
}
