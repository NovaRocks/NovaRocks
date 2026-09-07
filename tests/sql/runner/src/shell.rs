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

fn execute_command(command: &mut Command) -> (bool, Option<QueryExecution>, String) {
    let started = Instant::now();
    let result = command.output();
    let elapsed = started.elapsed();

    match result {
        Ok(out) => {
            let code = out.status.code().unwrap_or(-1);
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            let stdout_trimmed = stdout.trim_end_matches('\n');
            let text = if stdout_trimmed.is_empty() {
                code.to_string()
            } else {
                format!("{}\n{}", code, stdout_trimmed)
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
                        stderr.trim_end(),
                    ),
                )
            }
        }
        Err(err) => (false, None, format!("failed to start shell command: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::execute_shell_step;

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
