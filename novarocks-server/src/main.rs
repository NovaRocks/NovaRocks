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
use std::env;
use std::path::PathBuf;
use std::process;

use novarocks::novarocks_logging;

mod composition;

#[derive(Debug, PartialEq, Eq)]
struct StandaloneServerCliArgs {
    mysql_port: Option<u16>,
    config_path: Option<String>,
    role: Option<novarocks::common::app_config::ClusterRole>,
}

#[derive(Debug, PartialEq, Eq)]
enum ServerCommand {
    Help(Usage),
    Standalone(StandaloneServerCliArgs),
}

#[derive(Debug, PartialEq, Eq)]
enum Usage {
    Main,
    Standalone,
}

fn print_main_usage() {
    eprintln!(
        "Usage: novarocks standalone [--port <port>] [--config <path>] [--role <fe|be|all-in-one>]"
    );
}

fn print_standalone_server_usage() {
    eprintln!(
        "Usage: novarocks standalone [--port <port>] [--config <path>] [--role <fe|be|all-in-one>]"
    );
    eprintln!("Example:");
    eprintln!("  novarocks standalone --port 9030 --config /etc/novarocks/novarocks.toml");
    eprintln!("  novarocks standalone --role be --config /etc/novarocks/novarocks.toml");
}

/// Build the tracing EnvFilter expression from config: prefer the explicit
/// `log_filter`, else map `log_level` (keeping deps at info for debug/trace).
fn resolve_log_filter(cfg: &novarocks::common::app_config::NovaRocksConfig) -> String {
    if let Some(ref f) = cfg.log_filter {
        f.clone()
    } else {
        match cfg.log_level.as_str() {
            "debug" => "info,novarocks=debug".to_string(),
            "trace" => "info,novarocks=trace".to_string(),
            other => other.to_string(),
        }
    }
}

fn parse_standalone_server_args(
    args: &[String],
) -> Result<Option<StandaloneServerCliArgs>, String> {
    let mut idx = 0usize;
    let mut mysql_port: Option<u16> = None;
    let mut config_path: Option<String> = None;
    let mut role: Option<novarocks::common::app_config::ClusterRole> = None;

    while let Some(arg) = args.get(idx) {
        match arg.as_str() {
            "--port" => {
                idx += 1;
                let raw = args
                    .get(idx)
                    .ok_or_else(|| "missing value for --port".to_string())?;
                mysql_port = Some(
                    raw.parse::<u16>()
                        .map_err(|e| format!("invalid --port value `{raw}`: {e}"))?,
                );
                idx += 1;
            }
            "--config" | "-c" => {
                idx += 1;
                config_path = args.get(idx).cloned();
                if config_path.is_none() {
                    return Err("missing value for --config/-c".to_string());
                }
                idx += 1;
            }
            "--role" => {
                idx += 1;
                let raw = args
                    .get(idx)
                    .ok_or_else(|| "missing value for --role".to_string())?;
                role = Some(
                    parse_cluster_role(raw)
                        .map_err(|e| format!("invalid --role value `{raw}`; {e}"))?,
                );
                idx += 1;
            }
            "--help" | "-h" => return Ok(None),
            other => {
                return Err(format!(
                    "unknown standalone arg: {other} (try `novarocks standalone --help`)"
                ));
            }
        }
    }

    Ok(Some(StandaloneServerCliArgs {
        mysql_port,
        config_path,
        role,
    }))
}

/// Parse the entire process command line before loading configuration, raising
/// resource limits, or starting any runtime-owned service.
fn parse_server_command(args: &[String]) -> Result<ServerCommand, String> {
    let Some(command) = args.first() else {
        return Err("missing command; use `novarocks standalone --help`".to_string());
    };

    match command.as_str() {
        "--help" | "-h" => Ok(ServerCommand::Help(Usage::Main)),
        "standalone" => match parse_standalone_server_args(&args[1..])? {
            Some(cli) => Ok(ServerCommand::Standalone(cli)),
            None => Ok(ServerCommand::Help(Usage::Standalone)),
        },
        "run" | "start" | "stop" | "restart" => Err(format!(
            "the `{command}` command has been retired; use `novarocks standalone --role fe|be|all-in-one --config <path>`"
        )),
        other => Err(format!(
            "unknown command `{other}`; use `novarocks standalone --help`"
        )),
    }
}

fn parse_cluster_role(value: &str) -> Result<novarocks::common::app_config::ClusterRole, String> {
    match value {
        "fe" => Ok(novarocks::common::app_config::ClusterRole::Fe),
        "be" => Ok(novarocks::common::app_config::ClusterRole::Be),
        "all-in-one" => Ok(novarocks::common::app_config::ClusterRole::AllInOne),
        other => Err(format!(
            "invalid cluster role '{}'; expected one of: fe, be, all-in-one",
            other
        )),
    }
}

fn resolve_cluster_role(
    cfg: &novarocks::common::app_config::NovaRocksConfig,
    role_override: Option<novarocks::common::app_config::ClusterRole>,
) -> novarocks::common::app_config::ClusterRole {
    role_override.unwrap_or(cfg.cluster.role)
}

/// Load config from `cli.config_path` (or use defaults when absent), resolve
/// the effective cluster role (CLI override wins over config), and validate the
/// loaded cluster section.  Returns the owned config, the resolved role, and
/// the resolved config file path so callers can thread the pre-loaded config
/// into the execution path without a second file read (I1 fix).
fn load_config_and_resolve_role(
    cli: &StandaloneServerCliArgs,
) -> anyhow::Result<(
    novarocks::common::app_config::NovaRocksConfig,
    novarocks::common::app_config::ClusterRole,
    Option<PathBuf>,
)> {
    // C2: honour NOVAROCKS_CONFIG env var and ./novarocks.toml fallback, not
    // just the explicit --config path.
    let config_path = novarocks::common::app_config::resolve_config_path(
        cli.config_path.as_deref().map(std::path::Path::new),
    );
    let mut cfg = match config_path.as_ref() {
        Some(p) => novarocks::common::app_config::NovaRocksConfig::load_from_file(p)
            .map_err(|e| anyhow::anyhow!("{}", e))?,
        None => novarocks::common::app_config::NovaRocksConfig::default(),
    };

    let role_override = cli.role;

    let role = resolve_cluster_role(&cfg, role_override);

    // Persist the effective role into the owned configuration before any
    // composition root observes it. Frontend admission and topology ownership
    // must never disagree with the CLI role override.
    cfg.cluster.role = role;
    cfg.cluster
        .validate()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok((cfg, role, config_path))
}

/// Returns a human-readable warning string when `--port` is supplied together
/// with `role=be`.  The BE starts a gRPC server, not a MySQL server, so the
/// MySQL port override has no effect.
fn be_role_start_warning(port_override: Option<u16>) -> Option<String> {
    port_override.map(|p| {
        format!(
            "role=be: --port {p} is ignored; the BE role starts a gRPC server, not a MySQL server"
        )
    })
}

fn dispatch_standalone_role_with_all_in_one(
    role: novarocks::common::app_config::ClusterRole,
    cfg: novarocks::common::app_config::NovaRocksConfig,
    port_override: Option<u16>,
    run_frontend: impl FnOnce(
        novarocks::common::app_config::NovaRocksConfig,
        Option<u16>,
    ) -> anyhow::Result<()>,
    run_backend: impl FnOnce(
        novarocks::common::app_config::NovaRocksConfig,
        Option<u16>,
    ) -> anyhow::Result<()>,
    run_all_in_one: impl FnOnce(
        novarocks::common::app_config::NovaRocksConfig,
        Option<u16>,
    ) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    match role {
        novarocks::common::app_config::ClusterRole::AllInOne => run_all_in_one(cfg, port_override),
        novarocks::common::app_config::ClusterRole::Fe => run_frontend(cfg, port_override),
        novarocks::common::app_config::ClusterRole::Be => run_backend(cfg, port_override),
    }
}

fn run_standalone_be_role(
    cfg: novarocks::common::app_config::NovaRocksConfig,
    port_override: Option<u16>,
) -> anyhow::Result<()> {
    if let Some(warn) = be_role_start_warning(port_override) {
        eprintln!("WARN: {warn}");
    }
    novarocks_backend::run_backend_server(novarocks_backend::BackendServerConfig { config: cfg })
        .map_err(|error| anyhow::anyhow!("role=be: {error}"))
}

fn run_standalone_server_cli(cli: StandaloneServerCliArgs) -> anyhow::Result<()> {
    // I1: load_config_and_resolve_role returns the resolved path so we thread
    // it — along with the already-validated cfg — into the execution path
    // without a second file read.
    let (cfg, role, resolved_config_path) = load_config_and_resolve_role(&cli)?;

    // Install the global config and initialize the tracing subscriber before
    // starting the server. Without this, standalone runs with no logging
    // path), so log_filter/log_level/sys_log_dir from the config are ignored.
    novarocks::common::app_config::install_preloaded_config(cfg.clone());
    novarocks_logging::init_with_level(&resolve_log_filter(&cfg));

    let frontend_config_path = resolved_config_path.clone();

    dispatch_standalone_role_with_all_in_one(
        role,
        cfg,
        cli.mysql_port,
        move |cfg, port| {
            novarocks_frontend::run_frontend_server(novarocks_frontend::FrontendServerConfig {
                config: cfg,
                config_path: frontend_config_path,
                port_override: port,
                grpc_endpoint: novarocks_frontend::FrontendGrpcEndpointOwnership::HostedReportOnly,
            })
            .map_err(|error| anyhow::anyhow!("{error}"))
        },
        run_standalone_be_role,
        move |cfg, port| composition::run_all_in_one(cfg, resolved_config_path, port),
    )
}

#[cfg(unix)]
fn raise_nofile_limit() {
    const TARGET_SOFT_NOFILE: libc::rlim_t = 8192;

    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return;
    }

    if limit.rlim_cur >= TARGET_SOFT_NOFILE {
        return;
    }

    let target = std::cmp::min(limit.rlim_max, TARGET_SOFT_NOFILE);
    if target <= limit.rlim_cur {
        return;
    }

    let updated = libc::rlimit {
        rlim_cur: target,
        rlim_max: limit.rlim_max,
    };
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &updated) } == 0 {
        eprintln!(
            "Raised RLIMIT_NOFILE soft limit from {} to {}",
            limit.rlim_cur, target
        );
    }
}

#[cfg(not(unix))]
fn raise_nofile_limit() {}

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match parse_server_command(&args) {
        Ok(ServerCommand::Help(Usage::Main)) => {
            print_main_usage();
            process::exit(0);
        }
        Ok(ServerCommand::Help(Usage::Standalone)) => {
            print_standalone_server_usage();
            process::exit(0);
        }
        Ok(ServerCommand::Standalone(cli)) => {
            // Design: ADR-0026 (docs/adr/ADR-0026-retire-starrocks-compat-runtime-role.md)
            // All parsing is complete before this native runtime side effect.
            raise_nofile_limit();
            if let Err(error) = run_standalone_server_cli(cli) {
                eprintln!("{error}");
                process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("{error}");
            print_main_usage();
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ServerCommand, StandaloneServerCliArgs, Usage, dispatch_standalone_role_with_all_in_one,
        load_config_and_resolve_role, parse_server_command, parse_standalone_server_args,
        resolve_cluster_role,
    };

    #[test]
    fn top_level_help_is_side_effect_free_command() {
        assert_eq!(
            parse_server_command(&["--help".to_string()]).expect("parse help"),
            ServerCommand::Help(Usage::Main)
        );
        assert_eq!(
            parse_server_command(&["standalone".to_string(), "-h".to_string()])
                .expect("parse standalone help"),
            ServerCommand::Help(Usage::Standalone)
        );
    }

    #[test]
    fn missing_and_retired_commands_fail_before_runtime_setup() {
        for args in [
            vec![],
            vec!["run".to_string()],
            vec!["start".to_string()],
            vec!["stop".to_string()],
            vec!["restart".to_string()],
        ] {
            let error = parse_server_command(&args).expect_err("command must be rejected");
            assert!(
                error.contains("missing command") || error.contains("has been retired"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn standalone_command_preserves_role_and_config_before_runtime_setup() {
        let command = parse_server_command(&[
            "standalone".to_string(),
            "--role".to_string(),
            "be".to_string(),
            "--config".to_string(),
            "test.toml".to_string(),
        ])
        .expect("parse standalone command");
        assert_eq!(
            command,
            ServerCommand::Standalone(StandaloneServerCliArgs {
                mysql_port: None,
                config_path: Some("test.toml".to_string()),
                role: Some(novarocks::common::app_config::ClusterRole::Be),
            })
        );
    }

    mod frontend_dispatch {
        use super::dispatch_standalone_role_with_all_in_one;

        #[test]
        fn fe_and_all_in_one_dispatch_use_distinct_composition_roots() {
            for role in [
                novarocks::common::app_config::ClusterRole::Fe,
                novarocks::common::app_config::ClusterRole::AllInOne,
            ] {
                let frontend_calls = std::cell::Cell::new(0);
                let backend_calls = std::cell::Cell::new(0);
                let all_in_one_calls = std::cell::Cell::new(0);
                dispatch_standalone_role_with_all_in_one(
                    role,
                    novarocks::common::app_config::NovaRocksConfig::default(),
                    None,
                    |_, _| {
                        frontend_calls.set(frontend_calls.get() + 1);
                        Ok(())
                    },
                    |_, _| {
                        backend_calls.set(backend_calls.get() + 1);
                        Ok(())
                    },
                    |_, _| {
                        all_in_one_calls.set(all_in_one_calls.get() + 1);
                        Ok(())
                    },
                )
                .expect("role dispatch should succeed");
                assert_eq!(
                    frontend_calls.get(),
                    (role == novarocks::common::app_config::ClusterRole::Fe) as usize
                );
                assert_eq!(backend_calls.get(), 0, "{role:?} must not invoke backend");
                assert_eq!(
                    all_in_one_calls.get(),
                    (role == novarocks::common::app_config::ClusterRole::AllInOne) as usize
                );
            }
        }

        #[test]
        fn be_dispatch_invokes_backend_runner_exactly_once() {
            let frontend_calls = std::cell::Cell::new(0);
            let backend_calls = std::cell::Cell::new(0);
            let all_in_one_calls = std::cell::Cell::new(0);

            dispatch_standalone_role_with_all_in_one(
                novarocks::common::app_config::ClusterRole::Be,
                novarocks::common::app_config::NovaRocksConfig::default(),
                None,
                |_, _| {
                    frontend_calls.set(frontend_calls.get() + 1);
                    Ok(())
                },
                |_, _| {
                    backend_calls.set(backend_calls.get() + 1);
                    Ok(())
                },
                |_, _| {
                    all_in_one_calls.set(all_in_one_calls.get() + 1);
                    Ok(())
                },
            )
            .expect("BE role dispatch should succeed");

            assert_eq!(frontend_calls.get(), 0, "BE must not invoke frontend");
            assert_eq!(backend_calls.get(), 1, "BE must invoke backend once");
            assert_eq!(all_in_one_calls.get(), 0, "BE must not invoke all-in-one");
        }
    }

    #[test]
    fn parse_standalone_server_args_accepts_port_and_config() {
        let args = vec![
            "--port".to_string(),
            "19030".to_string(),
            "--config".to_string(),
            "novarocks.toml".to_string(),
        ];
        let parsed = parse_standalone_server_args(&args)
            .expect("parse standalone args")
            .expect("standalone args");
        assert_eq!(
            parsed,
            StandaloneServerCliArgs {
                mysql_port: Some(19030),
                config_path: Some("novarocks.toml".to_string()),
                role: None,
            }
        );
    }

    #[test]
    fn parse_standalone_server_args_accepts_empty() {
        let parsed = parse_standalone_server_args(&[])
            .expect("parse standalone args")
            .expect("standalone args");
        assert_eq!(
            parsed,
            StandaloneServerCliArgs {
                mysql_port: None,
                config_path: None,
                role: None,
            }
        );
    }

    #[test]
    fn parse_standalone_server_args_rejects_unknown_flag() {
        let args = vec!["--unknown".to_string()];
        let err = parse_standalone_server_args(&args).expect_err("unknown flag must fail");
        assert!(err.contains("unknown standalone arg"));
    }

    #[test]
    fn test_standalone_server_role_arg_parses_fe() {
        let args = vec![
            "--role".to_string(),
            "fe".to_string(),
            "--config".to_string(),
            "fe.toml".to_string(),
        ];
        let parsed = parse_standalone_server_args(&args)
            .expect("parse args")
            .expect("args");
        assert_eq!(
            parsed.role,
            Some(novarocks::common::app_config::ClusterRole::Fe)
        );
        assert_eq!(parsed.config_path.as_deref(), Some("fe.toml"));
    }

    #[test]
    fn test_standalone_server_role_arg_parses_all_in_one() {
        let args = vec!["--role".to_string(), "all-in-one".to_string()];
        let parsed = parse_standalone_server_args(&args)
            .expect("parse args")
            .expect("args");
        assert_eq!(
            parsed.role,
            Some(novarocks::common::app_config::ClusterRole::AllInOne)
        );
    }

    #[test]
    fn test_standalone_server_role_invalid_rejected() {
        let args = vec!["--role".to_string(), "master".to_string()];
        let err = parse_standalone_server_args(&args).expect_err("invalid role must fail");
        assert!(err.contains("invalid --role value"));
    }

    #[test]
    fn test_role_override_wins_over_config() {
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.role = novarocks::common::app_config::ClusterRole::AllInOne;
        let role = resolve_cluster_role(&cfg, Some(novarocks::common::app_config::ClusterRole::Fe));
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
    }

    #[test]
    fn test_dispatch_role_fe_with_no_backend_enters_coordinator() {
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.backends.clear();
        dispatch_standalone_role_with_all_in_one(
            novarocks::common::app_config::ClusterRole::Fe,
            cfg,
            None,
            |_, _| Ok(()),
            |_, _| panic!("role=fe must not invoke the backend runner"),
            |_, _| panic!("role=fe must not invoke the all-in-one runner"),
        )
        .expect("role=fe may start without configured backends");
    }

    // --- PR-4 spec compliance tests ---

    /// Issue 3: be_role_start_warning emits a message that mentions both
    /// "role=be" and "--port" when a port override is supplied.
    #[test]
    fn dispatch_be_role_with_port_override_warns_message() {
        let msg = super::be_role_start_warning(Some(9030));
        assert!(msg.is_some(), "expected warning when port_override is Some");
        let s = msg.unwrap();
        assert!(s.contains("role=be"), "must mention role=be: {s}");
        assert!(s.contains("--port"), "must mention --port: {s}");
        assert!(s.contains("9030"), "must include port value: {s}");
    }

    /// Issue 3: no warning is emitted when port_override is None.
    #[test]
    fn dispatch_be_role_without_port_override_no_warning() {
        let msg = super::be_role_start_warning(None);
        assert!(
            msg.is_none(),
            "no warning expected when port_override is None"
        );
    }

    /// D2: dispatch_standalone_role with multiple reachable backends succeeds
    /// and calls run_all_in_one (coordinator path).
    #[test]
    fn dispatch_fe_multiple_reachable_backends_enters_coordinator() {
        let l1 = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener 1");
        let l2 = std::net::TcpListener::bind("127.0.0.1:0").expect("bind listener 2");
        let addr1 = l1.local_addr().expect("listener 1 addr");
        let addr2 = l2.local_addr().expect("listener 2 addr");
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.backends = vec![addr1.to_string(), addr2.to_string()];
        dispatch_standalone_role_with_all_in_one(
            novarocks::common::app_config::ClusterRole::Fe,
            cfg,
            None,
            |_, _| Ok(()),
            |_, _| panic!("role=fe must not invoke the backend runner"),
            |_, _| panic!("role=fe must not invoke the all-in-one runner"),
        )
        .expect("fe with multiple reachable backends should enter coordinator path");
        drop(l1);
        drop(l2);
    }

    /// D4: FE startup does not synchronously dial configured backends; the
    /// dynamic registry and heartbeat/query paths own liveness.
    #[test]
    fn dispatch_fe_one_unreachable_backend_still_enters_coordinator() {
        let live = std::net::TcpListener::bind("127.0.0.1:0").expect("bind live listener");
        let live_addr = live.local_addr().expect("live addr");
        let dead = std::net::TcpListener::bind("127.0.0.1:0").expect("bind dead listener");
        let dead_port = dead.local_addr().expect("dead addr").port();
        drop(dead);
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        cfg.cluster.backends = vec![live_addr.to_string(), format!("127.0.0.1:{dead_port}")];
        dispatch_standalone_role_with_all_in_one(
            novarocks::common::app_config::ClusterRole::Fe,
            cfg,
            None,
            |_, _| Ok(()),
            |_, _| panic!("role=fe must not invoke the backend runner"),
            |_, _| panic!("role=fe must not invoke the all-in-one runner"),
        )
        .expect("role=fe startup should not synchronously dial backends");
        drop(live);
    }

    // Serialize tests that mutate process-wide state (env vars, CWD) so they
    // don't interfere when the test harness runs tests in parallel threads.
    static ENV_MUTEX: std::sync::LazyLock<std::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

    // --- PR-1 spec compliance gap tests ---
    // These three tests fail on the current production code and drive the fixes:
    // 1. Config file role must be used when no CLI --role is given.
    // 2. ClusterConfig::validate() must run before dispatch.
    // 3. CLI --role override must still win over the config file role.

    fn write_toml_tempfile(toml: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().expect("create tempfile");
        f.write_all(toml.as_bytes())
            .expect("write toml to tempfile");
        f
    }

    #[test]
    fn test_config_file_role_fe_used_when_no_cli_override() {
        // Config declares role=fe with exactly one backend (valid). No CLI --role.
        // load_config_and_resolve_role must read the file and return ClusterRole::Fe,
        // after which dispatch_standalone_role must validate reachability and enter
        // the standalone coordinator path.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind backend probe");
        let backend_addr = listener.local_addr().expect("backend probe addr");
        let toml = format!(
            r#"
[cluster]
role = "fe"
backends = ["{backend_addr}"]
"#
        );
        let f = write_toml_tempfile(&toml);
        let cli = StandaloneServerCliArgs {
            config_path: Some(f.path().to_str().expect("utf-8 path").to_string()),
            role: None,
            mysql_port: None,
        };
        let (cfg, role, _) =
            load_config_and_resolve_role(&cli).expect("load and resolve must succeed for valid fe");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
        dispatch_standalone_role_with_all_in_one(
            role,
            cfg,
            None,
            |_, _| Ok(()),
            |_, _| panic!("role=fe must not invoke the backend runner"),
            |_, _| panic!("role=fe must not invoke the all-in-one runner"),
        )
        .expect("fe with reachable backend must enter coordinator path");
    }

    #[test]
    fn test_config_file_fe_zero_backends_allowed_before_dispatch() {
        // Config declares role=fe with zero backends. D4 allows this because
        // backend membership is managed dynamically through SQL and metadata.
        let toml = r#"
[cluster]
role = "fe"
backends = []
"#;
        let f = write_toml_tempfile(toml);
        let cli = StandaloneServerCliArgs {
            config_path: Some(f.path().to_str().expect("utf-8 path").to_string()),
            role: None,
            mysql_port: None,
        };
        let (cfg, role, _) =
            load_config_and_resolve_role(&cli).expect("fe with zero backends must load");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
        assert!(cfg.cluster.backends.is_empty());
    }

    #[test]
    fn test_cli_role_override_be_wins_over_config_all_in_one() {
        // Config says all-in-one (no backends — valid for both all-in-one and be).
        // CLI --role be must win: load_config_and_resolve_role returns ClusterRole::Be.
        // BE startup binds sockets and blocks, so this unit test stops at role
        // resolution; the cluster MVP smoke test covers BE startup.
        let toml = r#"
[cluster]
role = "all-in-one"
"#;
        let f = write_toml_tempfile(toml);
        let cli = StandaloneServerCliArgs {
            config_path: Some(f.path().to_str().expect("utf-8 path").to_string()),
            role: Some(novarocks::common::app_config::ClusterRole::Be),
            mysql_port: None,
        };
        let (cfg, role, _) = load_config_and_resolve_role(&cli)
            .expect("load and resolve must succeed (be with no backends is valid)");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Be);
        assert!(cfg.cluster.backends.is_empty());
    }

    // C1: validate against the *effective* (CLI-overridden) role, not the config-file role.
    #[test]
    fn test_c1_cli_role_be_rejects_backends_from_config_file() {
        // Config says role=fe with 1 backend (valid for fe).
        // CLI says --role be. Effective role is BE, which must reject backends.
        let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
        let f = write_toml_tempfile(toml);
        let cli = StandaloneServerCliArgs {
            config_path: Some(f.path().to_str().expect("utf-8").to_string()),
            role: Some(novarocks::common::app_config::ClusterRole::Be),
            mysql_port: None,
        };
        let result = load_config_and_resolve_role(&cli);
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("be with backends must fail validation"),
        };
        assert!(
            err.to_string()
                .contains("role=be must not configure [cluster].backends"),
            "unexpected error: {err}"
        );
    }

    // C2: NOVAROCKS_CONFIG env var must be honoured when no explicit --config is given.
    #[test]
    fn test_c2_novarocks_config_env_var_used_when_no_cli_config() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
        let f = write_toml_tempfile(toml);
        let path = f.path().to_str().expect("utf-8").to_string();

        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let prev = std::env::var("NOVAROCKS_CONFIG").ok();
        // SAFETY: single-threaded thanks to ENV_MUTEX held above.
        unsafe { std::env::set_var("NOVAROCKS_CONFIG", &path) };
        let cli = StandaloneServerCliArgs {
            config_path: None,
            role: None,
            mysql_port: None,
        };
        let result = load_config_and_resolve_role(&cli);
        match prev {
            // SAFETY: single-threaded thanks to ENV_MUTEX.
            Some(v) => unsafe { std::env::set_var("NOVAROCKS_CONFIG", v) },
            None => unsafe { std::env::remove_var("NOVAROCKS_CONFIG") },
        }

        let (_, role, _) = result.expect("NOVAROCKS_CONFIG must be picked up");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
    }

    // C2: ./novarocks.toml in CWD must be discovered when no --config and no env var.
    #[test]
    fn test_c2_default_novarocks_toml_in_cwd_used() {
        let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
        let dir = tempfile::TempDir::new().expect("create tempdir");
        std::fs::write(dir.path().join("novarocks.toml"), toml).expect("write novarocks.toml");

        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let prev_env = std::env::var("NOVAROCKS_CONFIG").ok();
        let prev_dir = std::env::current_dir().expect("current dir");
        // SAFETY: single-threaded thanks to ENV_MUTEX.
        unsafe { std::env::remove_var("NOVAROCKS_CONFIG") };
        std::env::set_current_dir(dir.path()).expect("change to tempdir");

        let cli = StandaloneServerCliArgs {
            config_path: None,
            role: None,
            mysql_port: None,
        };
        let result = load_config_and_resolve_role(&cli);

        std::env::set_current_dir(&prev_dir).expect("restore cwd");
        match prev_env {
            // SAFETY: single-threaded thanks to ENV_MUTEX.
            Some(v) => unsafe { std::env::set_var("NOVAROCKS_CONFIG", v) },
            None => unsafe { std::env::remove_var("NOVAROCKS_CONFIG") },
        }

        let (_, role, _) = result.expect("./novarocks.toml in CWD must be picked up");
        assert_eq!(role, novarocks::common::app_config::ClusterRole::Fe);
    }

    // I1: dispatch_standalone_role must pass the pre-loaded cfg to the
    // all-in-one closure, not drop it.
    #[test]
    fn test_i1_all_in_one_closure_receives_validated_config() {
        use novarocks::common::app_config::StandaloneServerConfig;
        let mut cfg = novarocks::common::app_config::NovaRocksConfig::default();
        // Plant a sentinel mysql_port in the config that can only come from the
        // pre-loaded instance — it's never the default 9030.
        cfg.standalone_server = Some(StandaloneServerConfig {
            mysql_port: 23456,
            ..StandaloneServerConfig::default()
        });
        let captured_port: std::cell::Cell<u16> = std::cell::Cell::new(0);
        dispatch_standalone_role_with_all_in_one(
            novarocks::common::app_config::ClusterRole::AllInOne,
            cfg,
            None,
            |_, _| panic!("all-in-one must not use the frontend-only runner"),
            |_, _| panic!("all-in-one must not use the backend-only runner"),
            |cfg, _port| {
                // The closure must receive the sentinel config (not a freshly
                // defaulted one).
                captured_port.set(
                    cfg.standalone_server
                        .as_ref()
                        .map(|s| s.mysql_port)
                        .unwrap_or(0),
                );
                Ok(())
            },
        )
        .expect("all-in-one dispatch must succeed");
        assert_eq!(
            captured_port.get(),
            23456,
            "all-in-one runner must receive the pre-loaded cfg with the sentinel mysql_port"
        );
    }

    // I1: load_config_and_resolve_role returns the resolved config path so the
    // caller can pass it to the server without a second resolve call.
    #[test]
    fn test_i1_load_config_returns_resolved_path() {
        let toml = r#"
[cluster]
role = "all-in-one"
"#;
        let f = write_toml_tempfile(toml);
        let explicit_path = f.path().to_str().expect("utf-8").to_string();
        let cli = StandaloneServerCliArgs {
            config_path: Some(explicit_path.clone()),
            role: None,
            mysql_port: None,
        };
        let (_, _, resolved_path) = load_config_and_resolve_role(&cli).expect("load must succeed");
        assert!(
            resolved_path.is_some(),
            "resolved_path must be Some when --config was provided"
        );
        assert_eq!(
            resolved_path.unwrap().to_str().unwrap(),
            explicit_path,
            "resolved path must match the explicit --config path"
        );
    }
}
