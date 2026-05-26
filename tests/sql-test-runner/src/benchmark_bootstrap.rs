#![allow(dead_code)]

use crate::types::RunnerConfig;
use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BenchmarkBootstrapOptions {
    pub enabled: bool,
    pub rebuild: bool,
    pub scales: BTreeMap<String, String>,
}

pub fn is_benchmark_suite(suite: &str) -> bool {
    matches!(suite, "ssb" | "tpc-h" | "tpc-ds")
}

pub fn is_auto_bootstrap_supported_suite(suite: &str) -> bool {
    matches!(suite, "ssb" | "tpc-h" | "tpc-ds")
}

pub fn parse_benchmark_scale_override(
    raw: &str,
    options: &mut BenchmarkBootstrapOptions,
) -> Result<()> {
    let (suite, scale) = raw
        .split_once('=')
        .with_context(|| format!("invalid benchmark scale override: {raw}"))?;
    let suite = suite.trim();
    let scale = scale.trim();

    if !is_benchmark_suite(suite) {
        bail!("unknown benchmark suite in scale override: {suite}");
    }
    if scale.is_empty() {
        bail!("benchmark scale override for {suite} must not be empty");
    }
    if scale.contains('=') {
        bail!("invalid benchmark scale override: {raw}");
    }

    options.scales.insert(suite.to_string(), scale.to_string());
    Ok(())
}

pub fn parse_scale_overrides(raw_overrides: &[String]) -> Result<BTreeMap<String, String>> {
    let mut options = BenchmarkBootstrapOptions::default();
    for raw in raw_overrides {
        parse_benchmark_scale_override(raw, &mut options)?;
    }
    Ok(options.scales)
}

pub fn benchmark_scale_for_suite(
    options: &BenchmarkBootstrapOptions,
    suite: &str,
) -> Result<String> {
    if !is_benchmark_suite(suite) {
        bail!("unknown benchmark suite: {suite}");
    }

    Ok(options
        .scales
        .get(suite)
        .cloned()
        .unwrap_or_else(|| default_benchmark_scale(suite).to_string()))
}

pub fn default_benchmark_scale(suite: &str) -> &'static str {
    match suite {
        "ssb" => "1",
        "tpc-h" => "1",
        "tpc-ds" => "1GB",
        _ => "",
    }
}

#[allow(clippy::too_many_arguments)]
pub fn build_benchmark_bootstrap_command(
    script_path: &Path,
    suite: &str,
    scale: &str,
    target_catalog: &str,
    mysql_host: &str,
    mysql_port: &str,
    mysql_user: &str,
    mysql_password: Option<&str>,
    check: bool,
    rebuild: bool,
) -> Command {
    let mut command = Command::new(script_path);
    command
        .arg("--suite")
        .arg(suite)
        .arg("--scale")
        .arg(scale)
        .arg("--target-catalog")
        .arg(target_catalog)
        .arg("--mysql-host")
        .arg(mysql_host)
        .arg("--mysql-port")
        .arg(mysql_port)
        .arg("--mysql-user")
        .arg(mysql_user);

    if let Some(password) = mysql_password.filter(|password| !password.is_empty()) {
        command.arg("--mysql-password").arg(password);
    }
    if check {
        command.arg("--check");
    }
    if rebuild {
        command.arg("--rebuild");
    }

    command
}

pub fn command_preview(command: &Command) -> String {
    let mut parts = vec![shell_quote(command.get_program())];
    let mut redact_next = false;

    for arg in command.get_args() {
        if redact_next {
            parts.push("<redacted>".to_string());
            redact_next = false;
            continue;
        }

        parts.push(shell_quote(arg));
        if arg == "--mysql-password" {
            redact_next = true;
        }
    }

    parts.join(" ")
}

pub fn run_benchmark_bootstrap_command(command: &mut Command) -> Result<bool> {
    let preview = command_preview(command);
    let status = command
        .status()
        .with_context(|| format!("failed to run benchmark bootstrap command: {preview}"))?;
    Ok(status.success())
}

#[allow(clippy::too_many_arguments)]
pub fn ensure_benchmark_data(
    options: &BenchmarkBootstrapOptions,
    runner_config: &RunnerConfig,
    base_dir: &Path,
    suite: &str,
    target_catalog: &str,
    mysql_host: &str,
    mysql_port: &str,
    mysql_user: &str,
    mysql_password: Option<&str>,
) -> Result<()> {
    if !options.enabled || !is_auto_bootstrap_supported_suite(suite) {
        return Ok(());
    }

    let script_path = benchmark_bootstrap_script_path(runner_config, base_dir);
    let scale = benchmark_scale_for_suite(options, suite)?;
    let mut check_command = build_benchmark_bootstrap_command(
        &script_path,
        suite,
        &scale,
        target_catalog,
        mysql_host,
        mysql_port,
        mysql_user,
        mysql_password,
        true,
        options.rebuild,
    );
    if run_benchmark_bootstrap_command(&mut check_command)? {
        return Ok(());
    }

    let mut bootstrap_command = build_benchmark_bootstrap_command(
        &script_path,
        suite,
        &scale,
        target_catalog,
        mysql_host,
        mysql_port,
        mysql_user,
        mysql_password,
        false,
        options.rebuild,
    );
    if !run_benchmark_bootstrap_command(&mut bootstrap_command)? {
        bail!(
            "benchmark bootstrap failed: {}",
            command_preview(&bootstrap_command)
        );
    }

    let mut recheck_command = build_benchmark_bootstrap_command(
        &script_path,
        suite,
        &scale,
        target_catalog,
        mysql_host,
        mysql_port,
        mysql_user,
        mysql_password,
        true,
        false,
    );
    if !run_benchmark_bootstrap_command(&mut recheck_command)? {
        bail!(
            "benchmark bootstrap recheck failed: {}",
            command_preview(&recheck_command)
        );
    }

    Ok(())
}

fn benchmark_bootstrap_script_path(runner_config: &RunnerConfig, base_dir: &Path) -> PathBuf {
    runner_config
        .values
        .get("benchmark_bootstrap_script")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                base_dir.join(path)
            }
        })
        .unwrap_or_else(|| {
            base_dir
                .join("sql-tests")
                .join("bootstrap")
                .join("bootstrap_benchmark_data.sh")
        })
}

fn shell_quote(value: &OsStr) -> String {
    let value = value.to_string_lossy();
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        return value.into_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn recognizes_supported_benchmark_suites() {
        assert!(is_benchmark_suite("ssb"));
        assert!(is_benchmark_suite("tpc-h"));
        assert!(is_benchmark_suite("tpc-ds"));
        assert!(!is_benchmark_suite("join"));
    }

    #[test]
    fn auto_bootstrap_supports_standard_benchmark_suites() {
        assert!(is_auto_bootstrap_supported_suite("ssb"));
        assert!(is_auto_bootstrap_supported_suite("tpc-h"));
        assert!(is_auto_bootstrap_supported_suite("tpc-ds"));
        assert!(!is_auto_bootstrap_supported_suite("join"));
    }

    #[test]
    fn ensure_benchmark_data_runs_bootstrap_for_tpc_h() {
        let options = BenchmarkBootstrapOptions {
            enabled: true,
            rebuild: false,
            scales: BTreeMap::new(),
        };
        let mut runner_config = RunnerConfig::default();
        runner_config.values.insert(
            "benchmark_bootstrap_script".to_string(),
            "/definitely/missing/bootstrap_benchmark_data.sh".to_string(),
        );

        ensure_benchmark_data(
            &options,
            &runner_config,
            Path::new("."),
            "tpc-h",
            "iceberg_cat",
            "127.0.0.1",
            "23223",
            "root",
            Some("secret"),
        )
        .expect_err("supported suite should attempt to run the configured bootstrap script");
    }

    #[test]
    fn ensure_benchmark_data_runs_bootstrap_for_tpc_ds() {
        let options = BenchmarkBootstrapOptions {
            enabled: true,
            rebuild: false,
            scales: BTreeMap::new(),
        };
        let mut runner_config = RunnerConfig::default();
        runner_config.values.insert(
            "benchmark_bootstrap_script".to_string(),
            "/definitely/missing/bootstrap_benchmark_data.sh".to_string(),
        );

        ensure_benchmark_data(
            &options,
            &runner_config,
            Path::new("."),
            "tpc-ds",
            "iceberg_cat",
            "127.0.0.1",
            "23223",
            "root",
            Some("secret"),
        )
        .expect_err("supported suite should attempt to run the configured bootstrap script");
    }

    #[test]
    fn parses_scale_overrides_and_defaults() {
        let mut options = BenchmarkBootstrapOptions::default();
        parse_benchmark_scale_override("ssb=10", &mut options).unwrap();
        parse_benchmark_scale_override("tpc-h=100", &mut options).unwrap();

        assert_eq!(benchmark_scale_for_suite(&options, "ssb").unwrap(), "10");
        assert_eq!(benchmark_scale_for_suite(&options, "tpc-h").unwrap(), "100");
        assert_eq!(
            benchmark_scale_for_suite(&options, "tpc-ds").unwrap(),
            "1GB"
        );
    }

    #[test]
    fn parses_cli_scale_override_list() {
        let overrides = vec!["ssb=10".to_string(), "tpc-ds=100GB".to_string()];

        let scales = parse_scale_overrides(&overrides).unwrap();

        assert_eq!(scales.get("ssb").map(String::as_str), Some("10"));
        assert_eq!(scales.get("tpc-ds").map(String::as_str), Some("100GB"));
        assert_eq!(scales.get("tpc-h"), None);
    }

    #[test]
    fn rejects_bad_scale_overrides() {
        let mut options = BenchmarkBootstrapOptions::default();

        assert!(parse_benchmark_scale_override("ssb", &mut options).is_err());
        assert!(parse_benchmark_scale_override("ssb=", &mut options).is_err());
        assert!(parse_benchmark_scale_override("ssb=1=2", &mut options).is_err());
        assert!(parse_benchmark_scale_override("unknown=1", &mut options).is_err());
        assert!(parse_benchmark_scale_override("=1", &mut options).is_err());
    }

    #[test]
    fn builds_check_and_rebuild_command_arguments() {
        let command = build_benchmark_bootstrap_command(
            Path::new("sql-tests/bootstrap/bootstrap_benchmark_data.sh"),
            "ssb",
            "1",
            "iceberg_cat",
            "127.0.0.1",
            "23223",
            "root",
            Some("secret"),
            true,
            true,
        );

        let program = command.get_program().to_string_lossy();
        let args: Vec<_> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert_eq!(program, "sql-tests/bootstrap/bootstrap_benchmark_data.sh");
        assert_eq!(
            args,
            vec![
                "--suite",
                "ssb",
                "--scale",
                "1",
                "--target-catalog",
                "iceberg_cat",
                "--mysql-host",
                "127.0.0.1",
                "--mysql-port",
                "23223",
                "--mysql-user",
                "root",
                "--mysql-password",
                "secret",
                "--check",
                "--rebuild",
            ]
        );
    }

    #[test]
    fn command_preview_redacts_mysql_password() {
        let command = build_benchmark_bootstrap_command(
            Path::new("sql-tests/bootstrap/bootstrap_benchmark_data.sh"),
            "ssb",
            "1",
            "iceberg_cat",
            "127.0.0.1",
            "23223",
            "root",
            Some("very-secret-password"),
            true,
            false,
        );

        let preview = command_preview(&command);

        assert!(preview.contains("--mysql-password <redacted>"));
        assert!(!preview.contains("very-secret-password"));
    }

    #[test]
    fn skips_empty_mysql_password_argument() {
        let command = build_benchmark_bootstrap_command(
            Path::new("sql-tests/bootstrap/bootstrap_benchmark_data.sh"),
            "ssb",
            "1",
            "iceberg_cat",
            "127.0.0.1",
            "23223",
            "root",
            Some(""),
            true,
            false,
        );

        let args: Vec<_> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert!(!args.iter().any(|arg| arg == "--mysql-password"));
    }
}
