use crate::results::parse_output;
use crate::types::QueryExecution;
use std::process::Command;
use std::time::Instant;

/// Returns true if the SQL step is a shell command (starts with "shell:")
pub fn is_shell_step(sql: &str) -> bool {
    sql.trim_start().starts_with("shell:")
}

/// Execute a shell command and return the result as a QueryExecution.
/// Output format: "<exit_code>\n<stdout>" (to match dev/test framework convention).
/// The exit code is the first line, stdout follows.
pub fn execute_shell_command(cmd: &str) -> QueryExecution {
    let started = Instant::now();
    let result = Command::new("sh").arg("-c").arg(cmd).output();
    let elapsed = started.elapsed();

    let text = match result {
        Ok(out) => {
            let code = out.status.code().unwrap_or(-1);
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stdout_trimmed = stdout.trim_end_matches('\n');
            if stdout_trimmed.is_empty() {
                code.to_string()
            } else {
                format!("{}\n{}", code, stdout_trimmed)
            }
        }
        Err(err) => format!("-1\nerror: {}", err),
    };

    let (header, rows) = parse_output(&text);
    QueryExecution {
        text_output: text,
        header,
        rows,
        elapsed,
    }
}
