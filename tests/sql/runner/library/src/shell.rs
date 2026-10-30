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

use crate::results::parse_output;
use crate::types::QueryExecution;
use std::process::Command;
use std::time::Instant;

/// Returns true if the SQL step is a shell command (starts with "shell:")
pub fn is_shell_step(sql: &str) -> bool {
    sql.trim_start().starts_with("shell:")
}

/// Execute a shell step and preserve its process outcome separately from stdout.
///
/// Successful steps retain the historical `"<exit_code>\n<stdout>"` result text so
/// existing text assertions and golden files continue to see the same value.
pub fn execute_shell_step(sql: &str) -> (bool, Option<QueryExecution>, String) {
    let cmd = sql
        .trim_start()
        .strip_prefix("shell:")
        .expect("shell steps must start with shell:")
        .trim();
    execute_command(Command::new("sh").arg("-c").arg(cmd))
}

/// The controlled caller supplies its existing absolute scenario clock.
#[cfg(test)]
pub(crate) fn execute_shell_step_until(
    sql: &str,
    deadline: Option<Instant>,
) -> (bool, Option<QueryExecution>, String) {
    execute_shell_step_in_context(sql, deadline, None, false)
}

#[cfg(test)]
pub(crate) fn execute_shell_step_in_context(
    sql: &str,
    deadline: Option<Instant>,
    context: Option<&str>,
    cleanup: bool,
) -> (bool, Option<QueryExecution>, String) {
    let storage = std::env::var(novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV).ok();
    execute_shell_step_observed(sql, deadline, context, storage.as_deref(), cleanup, None)
}

type ShellExecution = (bool, Option<QueryExecution>, String);

pub(crate) fn execute_shell_step_observed(
    sql: &str,
    deadline: Option<Instant>,
    context: Option<&str>,
    storage: Option<&str>,
    cleanup: bool,
    writer_exit: Option<&std::sync::atomic::AtomicBool>,
) -> (bool, Option<QueryExecution>, String) {
    execute_shell_step_core(sql, deadline, context, storage, cleanup, writer_exit, None).0
}

/// Only a concrete domain owner can combine this local observation with its
/// own remote producer exit. It does not update the generic writer gate.
pub(crate) fn execute_shell_step_with_group_observation(
    sql: &str,
    work_deadline: Instant,
    complete_deadline: Instant,
    context: Option<&str>,
    storage: Option<&str>,
) -> (ShellExecution, bool) {
    execute_shell_step_core(
        sql,
        Some(work_deadline),
        context,
        storage,
        false,
        None,
        Some((work_deadline, complete_deadline)),
    )
}

fn execute_shell_step_core(
    sql: &str,
    deadline: Option<Instant>,
    context: Option<&str>,
    storage: Option<&str>,
    cleanup: bool,
    writer_exit: Option<&std::sync::atomic::AtomicBool>,
    lifetime: Option<(Instant, Instant)>,
) -> (ShellExecution, bool) {
    let mut group_exit_observed = false;
    let result = (|| -> ShellExecution {
        let Some(deadline) = deadline else {
            return execute_shell_step(sql);
        };
        #[cfg(not(unix))]
        {
            let _ = (deadline, writer_exit);
            return (
                false,
                None,
                "bounded shell commands require Unix process ownership".into(),
            );
        }
        #[cfg(unix)]
        {
            let started = Instant::now();
            let directory = match tempfile::Builder::new()
                .prefix("nr-sql-shell-")
                .tempdir_in("/tmp")
            {
                Ok(directory) => directory,
                Err(error) => return (false, None, format!("prepare shell output: {error}")),
            };
            let mut command = novarocks_test_support::ManagedCommand::new("sh");
            if let Some((work, complete)) = lifetime {
                command.terminate_at(work, complete);
            }
            command.arg("-c").arg(
                sql.trim_start()
                    .strip_prefix("shell:")
                    .expect("shell step")
                    .trim(),
            );
            if let Some(context) = context {
                command
                    .env(
                        novarocks_cluster_harness::fixture_run::RUN_CONTEXT_ENV,
                        context,
                    )
                    .env(
                        "NOVA_FIXTURE_CLEANUP_MODE",
                        if cleanup { "true" } else { "false" },
                    );
            }
            if let Some(storage) = storage {
                command.env(
                    novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV,
                    storage,
                );
            }
            let capture = (|| -> anyhow::Result<_> {
                use std::os::unix::fs::OpenOptionsExt;
                let stdout = std::fs::OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(directory.path().join("stdout"))?;
                let stderr = std::fs::OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(directory.path().join("stderr"))?;
                novarocks_test_support::capture_command_until(command, stdout, stderr, deadline)
            })();
            match capture {
                Ok(outcome) => {
                    group_exit_observed = outcome.group_exit_observed();
                    let timed_out = outcome.timed_out || Instant::now() >= deadline;
                    let failures = &outcome.failures;
                    // A failed producer may have reported an unknown remote writer
                    // outcome even after its host CLI exited. Keep files until its
                    // original owner can supply a successful completion fact.
                    if (!failures.is_empty() || !outcome.output.status.success())
                        && let Some(observed) = writer_exit
                    {
                        observed.store(false, std::sync::atomic::Ordering::Release);
                    }
                    let result = render_command_output(&outcome.output, started.elapsed());
                    if timed_out || Instant::now() >= deadline || !failures.is_empty() {
                        let path = directory.keep();
                        return (
                            false,
                            result.1,
                            format!(
                                "shell original deadline expired={timed_out}; cleanup={failures:?}; actual output retained at {}",
                                path.display()
                            ),
                        );
                    }
                    if !result.0 {
                        let path = directory.keep();
                        return (
                            result.0,
                            result.1,
                            format!(
                                "{}\nactual shell output retained at {}",
                                result.2,
                                path.display()
                            ),
                        );
                    }
                    if let Err(error) = outcome.release_diagnostics() {
                        return (
                            false,
                            result.1,
                            format!("release successful shell diagnostics: {error:#}"),
                        );
                    }
                    result
                }
                Err(error) => {
                    if let Some(observed) = writer_exit {
                        observed.store(false, std::sync::atomic::Ordering::Release);
                    }
                    let path = directory.keep();
                    (
                        false,
                        None,
                        format!(
                            "bounded shell command failed: {error:#}; original output retained at {}",
                            path.display()
                        ),
                    )
                }
            }
        }
    })();
    (result, group_exit_observed)
}

fn render_command_output(
    out: &std::process::Output,
    elapsed: std::time::Duration,
) -> (bool, Option<QueryExecution>, String) {
    let code = out.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout_trimmed = stdout.trim_end_matches('\n');
    let text = if stdout_trimmed.is_empty() {
        code.to_string()
    } else {
        format!("{code}\n{stdout_trimmed}")
    };
    let (header, rows) = parse_output(&text);
    let execution = QueryExecution {
        text_output: text,
        header,
        rows,
        elapsed,
    };
    if out.status.success() {
        (true, Some(execution), String::new())
    } else {
        (
            false,
            Some(execution),
            format!(
                "shell command exited with status {code}\nstdout:\n{}\nstderr:\n{}",
                stdout.trim_end(),
                stderr.trim_end()
            ),
        )
    }
}

fn execute_command(command: &mut Command) -> (bool, Option<QueryExecution>, String) {
    let started = Instant::now();
    let result = command.output();
    let elapsed = started.elapsed();

    match result {
        Ok(out) => render_command_output(&out, elapsed),
        Err(err) => (false, None, format!("failed to start shell command: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{execute_shell_step, execute_shell_step_observed};
    use std::time::{Duration, Instant};

    #[test]
    fn unsuccessful_producer_keeps_outer_cleanup_unconfirmed_even_when_host_cli_exits() {
        let exited = std::sync::atomic::AtomicBool::new(true);
        let result = execute_shell_step_observed(
            "shell: exit 17",
            Some(Instant::now() + Duration::from_secs(5)),
            None,
            None,
            false,
            Some(&exited),
        );
        assert!(!result.0);
        assert!(!exited.load(std::sync::atomic::Ordering::Acquire));
        let success = std::sync::atomic::AtomicBool::new(true);
        let result = execute_shell_step_observed(
            "shell: printf done",
            Some(Instant::now() + Duration::from_secs(5)),
            None,
            None,
            false,
            Some(&success),
        );
        assert!(result.0, "{}", result.2);
        assert!(success.load(std::sync::atomic::Ordering::Acquire));
    }

    #[cfg(unix)]
    #[test]
    fn local_group_receipt_and_remote_unknown_remain_independent_of_business_status() {
        let remote_unknown = std::sync::atomic::AtomicBool::new(true);
        let deadline = Instant::now() + Duration::from_secs(5);
        let generic = execute_shell_step_observed(
            "shell: exit 17",
            Some(deadline),
            None,
            None,
            false,
            Some(&remote_unknown),
        );
        assert!(!generic.0);
        assert!(!remote_unknown.load(std::sync::atomic::Ordering::Acquire));
        let work = Instant::now() + Duration::from_secs(5);
        let complete = work + Duration::from_secs(5);
        let (business, local_group) = super::execute_shell_step_with_group_observation(
            "shell: sleep 30 & exit 17",
            work,
            complete,
            None,
            None,
        );
        assert!(!business.0);
        assert!(business.2.contains("status 17"), "{}", business.2);
        assert!(
            local_group,
            "the original monitor must observe the background group gone"
        );
        assert!(
            !remote_unknown.load(std::sync::atomic::Ordering::Acquire),
            "a later local group receipt cannot settle an earlier remote-unknown writer"
        );
    }

    #[cfg(unix)]
    #[test]
    fn leader_exit_without_original_group_observation_stays_unknown() {
        let deadline = Instant::now() + Duration::from_secs(5);
        let (business, group) = super::execute_shell_step_core(
            "shell: exit 17",
            Some(deadline),
            None,
            None,
            false,
            None,
            None,
        );
        assert!(!business.0);
        assert!(!group);
    }

    #[test]
    fn successful_shell_step_preserves_stdout_as_result_text() {
        let (ok, execution, error) = execute_shell_step("shell: printf 'SHELL_OK\\n'");

        assert!(ok, "{error}");
        assert!(error.is_empty());
        assert_eq!(
            execution.expect("successful execution").text_output,
            "0\nSHELL_OK"
        );
    }

    #[test]
    fn failed_shell_step_reports_status_stdout_and_stderr() {
        let (ok, execution, error) = execute_shell_step(
            "shell: printf 'stdout evidence\\n'; printf 'stderr evidence\\n' >&2; exit 7",
        );

        assert!(!ok);
        assert_eq!(
            execution.expect("failed execution is retained").text_output,
            "7\nstdout evidence"
        );
        assert!(error.contains("status 7"), "{error}");
        assert!(error.contains("stdout evidence"), "{error}");
        assert!(error.contains("stderr evidence"), "{error}");
    }

    #[test]
    fn shell_start_failure_is_reported() {
        let mut command =
            std::process::Command::new("/definitely-not-an-executable-novarocks-sql-runner");
        let (ok, execution, error) = super::execute_command(&mut command);

        assert!(!ok);
        assert!(execution.is_none());
        assert!(error.contains("failed to start shell command"), "{error}");
    }
}

#[cfg(all(test, unix))]
mod bounded_tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn foreground_golden_is_identical_with_complete_large_stdout_and_stderr() {
        let sql = "shell: printf 'head\n'; i=0; while [ $i -lt 12000 ]; do printf x; printf y >&2; i=$((i+1)); done; printf tail";
        let ordinary = execute_shell_step(sql);
        let bounded = execute_shell_step_until(sql, Some(Instant::now() + Duration::from_secs(5)));
        assert!(ordinary.0 && bounded.0, "{}", bounded.2);
        assert_eq!(
            ordinary.1.unwrap().text_output,
            bounded.1.unwrap().text_output
        );
    }
    #[test]
    fn nonzero_and_signal_status_remain_failures_with_original_diagnostics() {
        let result = execute_shell_step_until(
            "shell: printf 'OUT'; printf 'ERR' >&2; exit 7",
            Some(Instant::now() + Duration::from_secs(2)),
        );
        assert!(!result.0);
        assert_eq!(result.1.unwrap().text_output, "7\nOUT");
        assert!(result.2.contains("status 7") && result.2.contains("ERR"));
        let signal = execute_shell_step_until(
            "shell: kill -TERM $$",
            Some(Instant::now() + Duration::from_secs(2)),
        );
        assert!(!signal.0);
        assert_eq!(signal.1.unwrap().text_output, "-1");
    }
    #[test]
    fn nested_shell_receives_the_original_context_and_explicit_cleanup_mode() {
        let original =
            r#"{"schema":1,"deadline_unix_ms":1792541400000,"identity":{"run_id":"original"}}"#;
        let sql =
            "shell: printf '%s\\n%s' \"$NOVA_FIXTURE_RUN_CONTEXT\" \"$NOVA_FIXTURE_CLEANUP_MODE\"";
        let (ok, result, error) = execute_shell_step_in_context(
            sql,
            Some(Instant::now() + Duration::from_secs(2)),
            Some(original),
            true,
        );
        assert!(ok, "{error}");
        assert_eq!(result.unwrap().text_output, format!("0\n{original}\ntrue"));
    }
    #[test]
    fn nested_shell_receives_original_storage_without_renewing_budget() {
        let context = "original-budget";
        let storage = "original-fixed-storage";
        let (ok, result, error) = execute_shell_step_observed(
            "shell: printf '%s\n%s' \"$NOVA_FIXTURE_RUN_CONTEXT\" \"$NOVA_FIXTURE_RUN_STORAGE\"",
            Some(Instant::now() + Duration::from_secs(2)),
            Some(context),
            Some(storage),
            false,
            None,
        );
        assert!(ok, "{error}");
        assert_eq!(
            result.unwrap().text_output,
            format!("0\n{context}\n{storage}")
        );
    }
    #[test]
    fn expired_clock_never_spawns_and_timeout_stops_the_owned_group() {
        // This fixture exercises a short payload deadline. Establish its
        // invocation lease before starting that clock; a failed first lease
        // must never be adopted by later tests in the same native invocation.
        novarocks_test_support::ensure_run_retention(Instant::now() + Duration::from_secs(10))
            .unwrap();
        let directory = tempfile::TempDir::new().unwrap();
        let marker = directory.path().join("spawned");
        let sql = format!("shell: printf created > '{}'", marker.display());
        let result = execute_shell_step_until(&sql, Some(Instant::now()));
        assert!(!result.0);
        assert!(!marker.exists());
        let later = directory.path().join("late-descendant");
        let sql = format!(
            "shell: (sleep 0.3; printf late > '{}') & wait",
            later.display()
        );
        let began = Instant::now();
        let deadline = began + Duration::from_millis(50);
        let result = execute_shell_step_until(&sql, Some(deadline));
        assert!(!result.0 && result.2.contains("deadline"), "{}", result.2);
        assert!(began.elapsed() < Duration::from_secs(2));
        std::thread::sleep(Duration::from_millis(400));
        assert!(
            !later.exists(),
            "the created descendant must not outlive actual group cleanup"
        );
    }
}
