mod encoding;

use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use async_trait::async_trait;
use mysql_common::scramble::scramble_native;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, ErrorKind, InitWriter, OkResponse, ParamParser,
    QueryResultWriter, StatementMetaWriter,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio::task;
use tracing::{info, warn};

use crate::common::engine_error::{EngineError, EngineErrorCode};
use crate::common::failpoint::{self, FailPointMode};
use crate::novarocks_config::NovaRocksConfig;
use crate::version;

use self::encoding::write_query_result;
use crate::engine::catalog::{DEFAULT_DATABASE, normalize_identifier};
use crate::engine::mv_maintenance::MaintenanceCoordinatorConfig;
use crate::engine::mv_scheduler::RefreshCoordinatorConfig;
use crate::engine::statement::{
    looks_like_show_alter_table_optimize, looks_like_show_create_table,
};
use crate::engine::{StandaloneNovaRocks, StandaloneOptions, StatementResult};
use crate::sql::optimizer::options::SessionOptimizerSettings;
use crate::sql::parser::dialect::StarRocksDialect;
use crate::sql::parser::dialect::backend::{
    looks_like_add_backend, looks_like_drop_backend, looks_like_show_backends,
};

const DEFAULT_MYSQL_PORT: u16 = 9030;
const DEFAULT_CATALOG: &str = "default_catalog";
const ROOT_USER: &str = "root";
static NEXT_CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

struct ClientDisconnectWatcher {
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ClientDisconnectWatcher {
    fn drop(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            handle.abort();
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StandaloneServerOptions {
    pub config_path: Option<PathBuf>,
    pub mysql_port: Option<u16>,
}

struct ResolvedStandaloneServerOptions {
    config_path: Option<PathBuf>,
    mysql_port: u16,
    user: String,
    refresh_coordinator: RefreshCoordinatorConfig,
    maintenance: MaintenanceCoordinatorConfig,
    /// Pre-loaded config to pass directly to engine open, bypassing a second
    /// disk read.  `None` falls back to the legacy disk/env load path.
    preloaded_config: Option<NovaRocksConfig>,
    /// Whether this process may execute local fragments that need the exchange
    /// registry.
    start_local_exchange_execution: bool,
    /// Whether this process should expose the NovaRocksGrpc endpoint that can
    /// receive standalone coordinator reports.
    start_coordinator_report_grpc: bool,
    /// Host to bind the standalone NovaRocksGrpc report/exchange endpoint on.
    grpc_bind_host: String,
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TestResolvedServerOptions {
    pub(crate) start_local_exchange_execution: bool,
    pub(crate) start_coordinator_report_grpc: bool,
    pub(crate) grpc_bind_host: String,
}

#[cfg(test)]
pub(crate) fn test_resolve_fe_server_options(
    cfg: NovaRocksConfig,
    port_override: Option<u16>,
) -> Result<TestResolvedServerOptions, String> {
    let resolved = resolve_server_options_from_config(&cfg, port_override)?;
    let resolved = ResolvedStandaloneServerOptions {
        config_path: None,
        preloaded_config: Some(cfg),
        start_local_exchange_execution: false,
        start_coordinator_report_grpc: true,
        ..resolved
    };
    Ok(TestResolvedServerOptions {
        start_local_exchange_execution: resolved.start_local_exchange_execution,
        start_coordinator_report_grpc: resolved.start_coordinator_report_grpc,
        grpc_bind_host: resolved.grpc_bind_host,
    })
}

/// Legacy standalone server entrypoint that loads config from disk/env inside
/// [`StandaloneNovaRocks::open`].  New callers that already hold a validated
/// config should prefer [`run_standalone_server_with_config`].
#[deprecated(
    note = "prefer run_standalone_server_with_config when a validated config is available"
)]
pub fn run_standalone_server(opts: StandaloneServerOptions) -> Result<(), String> {
    let resolved = resolve_server_options(&opts)?;
    run_with_resolved_options(resolved)
}

/// Run the standalone server using an already-loaded, validated [`NovaRocksConfig`].
///
/// `cfg` is installed as the process-wide active config before the engine
/// opens — no second disk read occurs.  `config_path` is preserved only for
/// resolving relative paths (e.g. SQLite metadata DB paths); pass `None` to
/// use built-in path defaults.
///
/// This variant starts local exchange execution and the coordinator report
/// gRPC endpoint (all-in-one mode).
/// For `role=fe` use [`run_standalone_fe_server_with_config`] instead.
pub fn run_standalone_server_with_config(
    cfg: NovaRocksConfig,
    config_path: Option<PathBuf>,
    port_override: Option<u16>,
) -> Result<(), String> {
    let resolved = resolve_server_options_from_config(&cfg, port_override)?;
    let resolved = ResolvedStandaloneServerOptions {
        config_path,
        preloaded_config: Some(cfg),
        start_local_exchange_execution: true,
        start_coordinator_report_grpc: true,
        ..resolved
    };
    run_with_resolved_options(resolved)
}

/// Run the standalone server for `role=fe`.
///
/// Identical to [`run_standalone_server_with_config`] except local fragment
/// exchange execution is disabled.  In role=fe all fragments (including root)
/// run on the remote BE; the FE only runs MySQL, the optimizer, the
/// `RemoteDispatcher` coordinator, and the report-capable NovaRocksGrpc
/// endpoint.
pub fn run_standalone_fe_server_with_config(
    cfg: NovaRocksConfig,
    config_path: Option<PathBuf>,
    port_override: Option<u16>,
) -> Result<(), String> {
    let resolved = resolve_server_options_from_config(&cfg, port_override)?;
    let resolved = ResolvedStandaloneServerOptions {
        config_path,
        preloaded_config: Some(cfg),
        start_local_exchange_execution: false,
        start_coordinator_report_grpc: true,
        ..resolved
    };
    run_with_resolved_options(resolved)
}

fn run_with_resolved_options(resolved: ResolvedStandaloneServerOptions) -> Result<(), String> {
    let opts = StandaloneOptions {
        config_path: resolved.config_path.clone(),
    };
    let engine = match resolved.preloaded_config {
        Some(cfg) => StandaloneNovaRocks::open_with_config(opts, cfg)?,
        None => StandaloneNovaRocks::open(opts)?,
    };
    crate::engine::register_stream_load_engine(engine.clone());
    let _refresh_coordinator = crate::engine::mv_scheduler::start_refresh_coordinator_for_server(
        &engine,
        resolved.refresh_coordinator.clone(),
    );
    let _maintenance_coordinator =
        crate::engine::mv_maintenance::start_maintenance_coordinator_for_server(
            &engine,
            resolved.maintenance.clone(),
        );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|e| format!("build tokio runtime failed: {e}"))?;

    runtime.block_on(serve_forever(
        engine,
        resolved.mysql_port,
        resolved.user.clone(),
        resolved.grpc_bind_host.clone(),
        resolved.start_local_exchange_execution,
        resolved.start_coordinator_report_grpc,
    ))
}

fn resolve_server_options(
    opts: &StandaloneServerOptions,
) -> Result<ResolvedStandaloneServerOptions, String> {
    let active_config_path = resolve_active_config_path(opts.config_path.as_deref());
    let file_cfg = load_active_config(active_config_path.as_deref())?;
    let standalone = file_cfg.as_ref().and_then(|c| c.standalone_server.as_ref());
    let grpc_bind_host = file_cfg
        .as_ref()
        .map(|cfg| cfg.server.host.clone())
        .unwrap_or_else(|| NovaRocksConfig::default().server.host);
    let (mysql_port, user, refresh_coordinator, maintenance) =
        extract_server_settings(standalone, opts.mysql_port)?;
    Ok(ResolvedStandaloneServerOptions {
        config_path: opts.config_path.clone(),
        mysql_port,
        user,
        refresh_coordinator,
        maintenance,
        preloaded_config: None,
        start_local_exchange_execution: true,
        start_coordinator_report_grpc: true,
        grpc_bind_host,
    })
}

fn resolve_active_config_path(explicit: Option<&Path>) -> Option<PathBuf> {
    crate::common::app_config::resolve_config_path(explicit)
}

/// Extract server-layer settings (port, user, refresh coordinator, maintenance) from an
/// optional [`StandaloneServerConfig`], applying `port_override` last.
/// Shared by both the disk-load path and the pre-loaded-config path to keep
/// validation logic in one place.
fn extract_server_settings(
    standalone: Option<&crate::common::app_config::StandaloneServerConfig>,
    port_override: Option<u16>,
) -> Result<
    (
        u16,
        String,
        RefreshCoordinatorConfig,
        MaintenanceCoordinatorConfig,
    ),
    String,
> {
    let mut mysql_port = DEFAULT_MYSQL_PORT;
    let mut user = ROOT_USER.to_string();
    let mut refresh_coordinator = RefreshCoordinatorConfig::default();
    let mut maintenance = MaintenanceCoordinatorConfig::from_standalone_config(
        &crate::common::app_config::StandaloneServerConfig::default(),
    );

    if let Some(sc) = standalone {
        mysql_port = sc.mysql_port;
        if sc.user != ROOT_USER {
            return Err(format!(
                "standalone server only supports user `{ROOT_USER}`, got `{}`",
                sc.user
            ));
        }
        user = sc.user.clone();
        refresh_coordinator = RefreshCoordinatorConfig::from_standalone_config(sc);
        maintenance = MaintenanceCoordinatorConfig::from_standalone_config(sc);
    }

    if let Some(port) = port_override {
        mysql_port = port;
    }

    Ok((mysql_port, user, refresh_coordinator, maintenance))
}

/// Extract server-layer settings directly from a pre-loaded [`NovaRocksConfig`].
fn resolve_server_options_from_config(
    cfg: &NovaRocksConfig,
    port_override: Option<u16>,
) -> Result<ResolvedStandaloneServerOptions, String> {
    let (mysql_port, user, refresh_coordinator, maintenance) =
        extract_server_settings(cfg.standalone_server.as_ref(), port_override)?;
    Ok(ResolvedStandaloneServerOptions {
        // config_path is intentionally None here; callers that need the path
        // (e.g. run_standalone_server_with_config) set it after this call.
        config_path: None,
        mysql_port,
        user,
        refresh_coordinator,
        maintenance,
        preloaded_config: None,
        // Callers may override these; default to all-in-one behavior.
        start_local_exchange_execution: true,
        start_coordinator_report_grpc: true,
        grpc_bind_host: cfg.server.host.clone(),
    })
}

fn load_active_config(path: Option<&Path>) -> Result<Option<NovaRocksConfig>, String> {
    match path {
        Some(path) if path.exists() => NovaRocksConfig::load_from_file(path)
            .map(Some)
            .map_err(|e| format!("load config {} failed: {e}", path.display())),
        _ => Ok(None),
    }
}

async fn serve_forever(
    engine: StandaloneNovaRocks,
    mysql_port: u16,
    user: String,
    grpc_bind_host: String,
    start_local_exchange_execution: bool,
    start_coordinator_report_grpc: bool,
) -> Result<(), String> {
    // Start the shared NovaRocksGrpc endpoint. It handles local exchange in
    // all-in-one mode and standalone write reports in role=fe coordinator mode.
    if start_local_exchange_execution || start_coordinator_report_grpc {
        let grpc_port = crate::common::config::grpc_port();
        let start_result = if start_local_exchange_execution {
            crate::service::grpc_server::start_grpc_exchange_server(&grpc_bind_host, grpc_port)
        } else {
            crate::service::grpc_server::start_grpc_report_server(&grpc_bind_host, grpc_port)
        };
        match start_result {
            Ok(()) => {
                info!(
                    "standalone coordinator grpc report/exchange endpoint started on {}:{}",
                    grpc_bind_host, grpc_port
                );
            }
            Err(e) if start_coordinator_report_grpc => {
                return Err(format!(
                    "failed to start required standalone coordinator grpc report/exchange endpoint on {}:{}: {}",
                    grpc_bind_host, grpc_port, e
                ));
            }
            Err(e) => {
                warn!(
                    "failed to start standalone coordinator grpc report/exchange endpoint on {}:{}: {} \
                     (standalone writer reports and local multi-fragment exchange may not work)",
                    grpc_bind_host, grpc_port, e
                );
            }
        }
    }

    let bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, mysql_port));
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| format!("bind standalone mysql server on {bind_addr} failed: {e}"))?;
    info!(
        "standalone mysql server listening on {} (user={}, db={})",
        bind_addr, user, DEFAULT_DATABASE
    );
    // Emit a parser-friendly readiness marker on stdout. Orchestration
    // scripts must wait for this exact line before connecting; probing the
    // mysql port alone cannot distinguish a freshly-bound server from a
    // pre-existing process that already owned the port. The keyword
    // `NOVAROCKS_READY` is the wait-for-ready contract — do not change it
    // without updating callers (CLAUDE.md, sql-tests harness, etc.).
    println!(
        "NOVAROCKS_READY mysql_port={mysql_port} pid={}",
        std::process::id()
    );
    loop {
        let (stream, peer_addr) = listener
            .accept()
            .await
            .map_err(|e| format!("accept standalone mysql connection failed: {e}"))?;
        let engine = engine.clone();
        let user = user.clone();
        tokio::spawn(async move {
            let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
            let (client_disconnect_signal, disconnect_watcher) =
                spawn_client_disconnect_watcher(&stream);
            let connection_disconnect_signal = Arc::clone(&client_disconnect_signal);
            let shim = NovaRocksMysqlShim::new(
                engine,
                user,
                connection_id,
                client_disconnect_signal,
                disconnect_watcher,
            );
            let (reader, writer) = stream.into_split();
            let result = AsyncMysqlIntermediary::run_on(shim, reader, writer).await;
            connection_disconnect_signal.store(true, Ordering::SeqCst);
            if let Err(err) = result {
                warn!(
                    "standalone mysql connection failed: peer={}, connection_id={}, err={}",
                    peer_addr, connection_id, err
                );
            }
        });
    }
}

#[cfg(unix)]
fn spawn_client_disconnect_watcher(
    stream: &tokio::net::TcpStream,
) -> (Arc<AtomicBool>, ClientDisconnectWatcher) {
    let disconnected = Arc::new(AtomicBool::new(false));
    let fd = unsafe { libc::dup(stream.as_raw_fd()) };
    if fd < 0 {
        return (disconnected, ClientDisconnectWatcher { join_handle: None });
    }

    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    if let Err(err) = std_stream.set_nonblocking(true) {
        warn!(
            "failed to configure disconnect monitor fd as nonblocking: {}",
            err
        );
        return (disconnected, ClientDisconnectWatcher { join_handle: None });
    }
    let watcher_stream = match tokio::net::TcpStream::from_std(std_stream) {
        Ok(stream) => stream,
        Err(err) => {
            warn!("failed to create async disconnect monitor stream: {}", err);
            return (disconnected, ClientDisconnectWatcher { join_handle: None });
        }
    };

    let watcher_disconnected = Arc::clone(&disconnected);
    let join_handle = tokio::spawn(async move {
        let mut buf = [0u8; 1];
        loop {
            if watcher_disconnected.load(Ordering::SeqCst) {
                break;
            }
            match watcher_stream.peek(&mut buf).await {
                Ok(0) => {
                    watcher_disconnected.store(true, Ordering::SeqCst);
                    break;
                }
                Ok(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(err) => match err.kind() {
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => {}
                    io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::NotConnected => {
                        watcher_disconnected.store(true, Ordering::SeqCst);
                        break;
                    }
                    _ => {
                        watcher_disconnected.store(true, Ordering::SeqCst);
                        break;
                    }
                },
            }
        }
    });
    (
        disconnected,
        ClientDisconnectWatcher {
            join_handle: Some(join_handle),
        },
    )
}

#[cfg(not(unix))]
fn spawn_client_disconnect_watcher(
    _stream: &tokio::net::TcpStream,
) -> (Arc<AtomicBool>, ClientDisconnectWatcher) {
    let disconnected = Arc::new(AtomicBool::new(false));
    (disconnected, ClientDisconnectWatcher { join_handle: None })
}

struct NovaRocksMysqlShim {
    engine: StandaloneNovaRocks,
    user: String,
    connection_id: u32,
    client_disconnect_signal: Arc<AtomicBool>,
    _disconnect_watcher: ClientDisconnectWatcher,
    current_catalog: Option<String>,
    current_db: String,
    /// Per-session query timeout (in seconds). `None` means no timeout.
    /// Set via `SET query_timeout = N`. `N == 0` clears the timeout.
    query_timeout_secs: Option<u64>,
    /// Per-session group_concat limit (in bytes).
    /// Set via `SET group_concat_max_len = N`.
    group_concat_max_len: i64,
    /// Per-session pipeline DOP override. `None` (or `SET pipeline_dop = 0`) means auto
    /// (cores/2 via `exec_env::calc_pipeline_dop`); a positive value pins the DOP for this session.
    pipeline_dop: Option<i32>,
    optimizer_settings: SessionOptimizerSettings,
    user_variables: BTreeMap<String, String>,
}

impl NovaRocksMysqlShim {
    fn new(
        engine: StandaloneNovaRocks,
        user: String,
        connection_id: u32,
        client_disconnect_signal: Arc<AtomicBool>,
        disconnect_watcher: ClientDisconnectWatcher,
    ) -> Self {
        Self {
            engine,
            user,
            connection_id,
            client_disconnect_signal,
            _disconnect_watcher: disconnect_watcher,
            current_catalog: None,
            current_db: DEFAULT_DATABASE.to_string(),
            query_timeout_secs: None,
            group_concat_max_len: 1024,
            pipeline_dop: None,
            optimizer_settings: SessionOptimizerSettings::default(),
            user_variables: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for NovaRocksMysqlShim {
    type Error = io::Error;

    fn version(&self) -> String {
        format!("{}-standalone-mysql", version::short_version())
    }

    fn connect_id(&self) -> u32 {
        self.connection_id
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        if auth_plugin != "mysql_native_password" || username != self.user.as_bytes() {
            return false;
        }
        if auth_data.is_empty() {
            return true;
        }
        scramble_native(salt, b"")
            .map(|expected| auth_data == expected.as_slice())
            .unwrap_or(false)
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.error(
            ErrorKind::ER_NOT_SUPPORTED_YET,
            b"prepared statements are not supported in standalone server v1",
        )
        .await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results
            .error(
                ErrorKind::ER_NOT_SUPPORTED_YET,
                b"prepared statements are not supported in standalone server v1",
            )
            .await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_init<'a>(
        &'a mut self,
        schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        match resolve_database_context_in_worker(
            self.engine.clone(),
            self.current_catalog.clone(),
            schema.to_string(),
        )
        .await
        {
            Ok(context) => {
                self.current_catalog = context.catalog;
                self.current_db = context.database;
                writer.ok().await
            }
            Err(err) => {
                writer
                    .error(ErrorKind::ER_BAD_DB_ERROR, err.as_bytes())
                    .await
            }
        }
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        let statements = match split_sql_statements(query) {
            Ok(statements) => statements,
            Err(err) => {
                return results
                    .error(ErrorKind::ER_PARSE_ERROR, err.as_bytes())
                    .await;
            }
        };
        if statements.is_empty() {
            return results.completed(OkResponse::default()).await;
        }
        let mut last_query_result = None;
        for statement in statements {
            match execute_statement_text(self, &statement).await {
                Ok(StatementResult::Query(result)) => last_query_result = Some(result),
                Ok(StatementResult::Ok) => {}
                Err((kind, message)) => {
                    return results.error(kind, message.as_bytes()).await;
                }
            }
        }
        if let Some(result) = last_query_result {
            write_query_result(result, results).await
        } else {
            results.completed(OkResponse::default()).await
        }
    }
}

fn trim_query(query: &str) -> &str {
    query.trim().trim_end_matches(';').trim()
}

fn is_session_noop(query: &str) -> bool {
    let lower = query.to_ascii_lowercase();
    // Note: "alter " is NOT a noop — it's handled by engine.rs (ADD FILES)
    // Note: "update " was previously listed here as a stub when UPDATE was
    // unimplemented. UPDATE is now a real DML statement routed through
    // mutation_flow::execute_update_statement, so it must reach the engine
    // instead of being silently swallowed.
    lower.starts_with("set ") || lower.starts_with("show ") || lower.starts_with("submit ")
}

fn is_backend_management_statement(query: &str) -> bool {
    let dialect = StarRocksDialect;
    let Ok(parser) = sqlparser::parser::Parser::new(&dialect).try_with_sql(query) else {
        return false;
    };
    looks_like_add_backend(&parser)
        || looks_like_drop_backend(&parser)
        || looks_like_show_backends(&parser)
}

fn is_materialized_view_management_statement(query: &str) -> bool {
    let lower = query.to_ascii_lowercase();
    lower.starts_with("create materialized view ")
        || lower.starts_with("drop materialized view ")
        || lower.starts_with("refresh materialized view ")
        || lower == "show materialized views"
        || lower.starts_with("show materialized views ")
        || lower.starts_with("show alter materialized view ")
}

fn split_sql_statements(query: &str) -> Result<Vec<String>, String> {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum QuoteState {
        Single,
        Double,
        Backtick,
    }

    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut quote_state = None;

    for (idx, ch) in query.char_indices() {
        match quote_state {
            Some(QuoteState::Single) if ch == '\'' => quote_state = None,
            Some(QuoteState::Double) if ch == '"' => quote_state = None,
            Some(QuoteState::Backtick) if ch == '`' => quote_state = None,
            Some(_) => {}
            None => match ch {
                '\'' => quote_state = Some(QuoteState::Single),
                '"' => quote_state = Some(QuoteState::Double),
                '`' => quote_state = Some(QuoteState::Backtick),
                ';' => {
                    let statement = trim_query(&query[start..idx]);
                    if !statement.is_empty() {
                        statements.push(statement.to_string());
                    }
                    start = idx + ch.len_utf8();
                }
                _ => {}
            },
        }
    }

    if quote_state.is_some() {
        return Err("unterminated quoted string in SQL batch".to_string());
    }

    let trailing = trim_query(&query[start..]);
    if !trailing.is_empty() {
        statements.push(trailing.to_string());
    }
    Ok(statements)
}

fn parse_use_database_query(query: &str) -> Option<&str> {
    let mut parts = query.split_whitespace();
    let head = parts.next()?;
    if !head.eq_ignore_ascii_case("use") {
        return None;
    }
    let database = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    Some(database)
}

fn parse_set_catalog_query(query: &str) -> Option<&str> {
    let mut parts = query.split_whitespace();
    let head = parts.next()?;
    if !head.eq_ignore_ascii_case("set") {
        return None;
    }
    let keyword = parts.next()?;
    if !keyword.eq_ignore_ascii_case("catalog") {
        return None;
    }
    let value = parts.next()?;
    if value == "=" {
        let catalog = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        return Some(catalog);
    }
    if parts.next().is_some() {
        return None;
    }
    Some(value)
}

fn parse_set_non_negative_integer(query: &str, keyword: &str) -> Option<u64> {
    // Normalize: collapse whitespace around `=` so we can split simply.
    let normalized = query.replace('=', " = ");
    let mut parts = normalized.split_whitespace();
    let head = parts.next()?;
    if !head.eq_ignore_ascii_case("set") {
        return None;
    }
    let actual_keyword = parts.next()?;
    if !actual_keyword.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let next = parts.next()?;
    let value_str = if next == "=" { parts.next()? } else { next };
    if parts.next().is_some() {
        return None;
    }
    value_str.parse::<u64>().ok()
}

/// Parse `SET <keyword> = <float>`. Uses the same keyword-exact-match logic as
/// `parse_set_non_negative_integer`: the keyword must be followed by whitespace
/// or `=`, so `..._min_size` cannot match `..._min_selectivity` and vice versa.
fn parse_set_f64(query: &str, keyword: &str) -> Option<f64> {
    let normalized = query.replace('=', " = ");
    let mut parts = normalized.split_whitespace();
    let head = parts.next()?;
    if !head.eq_ignore_ascii_case("set") {
        return None;
    }
    let actual_keyword = parts.next()?;
    if !actual_keyword.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let next = parts.next()?;
    let value_str = if next == "=" { parts.next()? } else { next };
    if parts.next().is_some() {
        return None;
    }
    value_str.parse::<f64>().ok()
}

/// Parse `SET query_timeout = N` and `SET query_timeout=N`. Returns the
/// integer seconds value if the statement matches that shape. The optional
/// `=` separator may have spaces around it or be glued to the keyword/value.
/// `N` must be a non-negative integer; `N == 0` clears the session timeout.
fn parse_set_query_timeout(query: &str) -> Option<u64> {
    parse_set_non_negative_integer(query, "query_timeout")
}

/// Parse `SET pipeline_dop = N`. `N` must be a non-negative integer that fits in `i32`;
/// `N == 0` clears the session override (auto = cores/2).
fn parse_set_pipeline_dop(query: &str) -> Option<i32> {
    parse_set_non_negative_integer(query, "pipeline_dop").and_then(|v| i32::try_from(v).ok())
}

/// Parse `SET group_concat_max_len = N` and `SET group_concat_max_len=N`.
/// `N` must be a non-negative integer and is clamped later by FE-compatible
/// lowering rules.
fn parse_set_group_concat_max_len(query: &str) -> Option<i64> {
    let value = parse_set_non_negative_integer(query, "group_concat_max_len")?;
    i64::try_from(value).ok()
}

fn apply_broadcast_profile_set(settings: &mut SessionOptimizerSettings, trimmed: &str) -> bool {
    if let Some(v) = parse_set_non_negative_integer(trimmed, "cbo_broadcast_backend_count") {
        settings.cbo_broadcast_backend_count = Some(v as f64);
        return true;
    }
    if let Some(v) = parse_set_non_negative_integer(trimmed, "cbo_broadcast_node_mem_budget_bytes")
    {
        settings.cbo_broadcast_node_mem_budget_bytes = Some(v as f64);
        return true;
    }
    false
}

fn parse_set_boolean(query: &str) -> Option<(String, bool)> {
    let normalized = query.replace('=', " = ");
    let mut parts = normalized.split_whitespace();
    let head = parts.next()?;
    if !head.eq_ignore_ascii_case("set") {
        return None;
    }
    let name = parts.next()?.to_ascii_lowercase();
    // User variables (@name) are handled by parse_set_user_variable_query.
    // Returning None here ensures the user variable path is reached before
    // parse_set_boolean can silently swallow `SET @i = 1` (which matches the
    // boolean pattern because `"1"` is a recognised bool token).
    if name.starts_with('@') {
        return None;
    }
    let next = parts.next()?;
    let value = if next == "=" { parts.next()? } else { next };
    if parts.next().is_some() {
        return None;
    }
    let enabled = match value.to_ascii_lowercase().as_str() {
        "true" | "on" | "1" => true,
        "false" | "off" | "0" => false,
        _ => return None,
    };
    Some((name, enabled))
}

/// Parse `SET <name> = '<comma-list>'` or `SET <name> = <comma-list>`.
/// Inner items are comma-separated, whitespace-trimmed, and empty items are dropped.
/// Returns the list (possibly empty) when the statement matches the
/// expected name, else None.
///
/// The name match requires a word boundary: e.g. `expected_name` of
/// "disable_optimizer_rules" does NOT match
/// "disable_optimizer_rules_extra".
fn parse_set_string_csv(query: &str, expected_name: &str) -> Option<Vec<String>> {
    let trimmed = query.trim();
    let head = trimmed.get(..3)?;
    if !head.eq_ignore_ascii_case("set") {
        return None;
    }
    let after_set = trimmed[3..].trim_start();

    let prefix_len = expected_name.len();
    let head = after_set.get(..prefix_len)?;
    if !head.eq_ignore_ascii_case(expected_name) {
        return None;
    }
    let following = after_set.as_bytes().get(prefix_len)?;
    if !matches!(*following, b' ' | b'\t' | b'=') {
        return None;
    }
    let rest = after_set[prefix_len..].trim_start();

    let value_str = rest.strip_prefix('=')?.trim();
    let inner = value_str
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(value_str);

    let items: Vec<String> = inner
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect();
    Some(items)
}

fn parse_set_user_variable_query(query: &str) -> Option<(String, String)> {
    let trimmed = query.trim();
    if !trimmed
        .get(..3)
        .is_some_and(|head| head.eq_ignore_ascii_case("set"))
    {
        return None;
    }
    let rest = trimmed[3..].trim_start();
    if !rest.starts_with('@') {
        return None;
    }

    let name_end = rest
        .char_indices()
        .find_map(|(idx, ch)| {
            (idx > 0 && !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '@')).then_some(idx)
        })
        .unwrap_or(rest.len());
    let name = rest[..name_end].to_ascii_lowercase();
    let after_name = rest[name_end..].trim_start();
    let value = after_name.strip_prefix('=')?.trim();
    if value.is_empty() {
        return None;
    }
    Some((name, value.to_string()))
}

fn substitute_session_user_variables(
    query: &str,
    user_variables: &BTreeMap<String, String>,
) -> Result<String, String> {
    if user_variables.is_empty() {
        return Ok(query.to_string());
    }
    let assignments = user_variables
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    crate::sql::parser::dialect::substitute_user_variables(query, &assignments)
}

fn is_supported_embedded_statement(query: &str) -> bool {
    // Skip leading SQL line comments (-- ...)
    let trimmed = query
        .lines()
        .map(|l| l.trim())
        .find(|l| !l.is_empty() && !l.starts_with("--"))
        .unwrap_or("");
    let mut parts = trimmed.split_whitespace();
    let Some(head) = parts.next() else {
        return false;
    };
    head.eq_ignore_ascii_case("select")
        || head.eq_ignore_ascii_case("with")
        || head.eq_ignore_ascii_case("create")
        || head.eq_ignore_ascii_case("drop")
        || head.eq_ignore_ascii_case("insert")
        || head.eq_ignore_ascii_case("delete")
        || head.eq_ignore_ascii_case("update")
        || head.eq_ignore_ascii_case("merge")
        || head.eq_ignore_ascii_case("explain")
        || head.eq_ignore_ascii_case("truncate")
        || head.eq_ignore_ascii_case("alter")
        || head.eq_ignore_ascii_case("analyze")
        || head.eq_ignore_ascii_case("call")
        || head.eq_ignore_ascii_case("admin")
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SessionDatabaseContext {
    catalog: Option<String>,
    database: String,
}

async fn resolve_catalog_name_in_worker(
    engine: StandaloneNovaRocks,
    catalog_name: String,
) -> Result<Option<String>, String> {
    task::spawn_blocking(move || resolve_catalog_name(&engine, &catalog_name))
        .await
        .map_err(|err| format!("standalone catalog resolver worker failed: {err}"))?
}

async fn resolve_database_context_in_worker(
    engine: StandaloneNovaRocks,
    current_catalog: Option<String>,
    schema: String,
) -> Result<SessionDatabaseContext, String> {
    task::spawn_blocking(move || {
        resolve_database_context(&engine, current_catalog.as_deref(), &schema)
    })
    .await
    .map_err(|err| format!("standalone database resolver worker failed: {err}"))?
}

async fn execute_statement_text(
    shim: &mut NovaRocksMysqlShim,
    statement: &str,
) -> Result<StatementResult, (ErrorKind, String)> {
    let trimmed = trim_query(statement);
    if trimmed.is_empty() {
        return Ok(StatementResult::Ok);
    }
    // Treat SQL line comments (-- ...) as no-ops
    if trimmed.starts_with("--") {
        return Ok(StatementResult::Ok);
    }

    if let Some(catalog_name) = parse_set_catalog_query(trimmed) {
        let catalog = resolve_catalog_name_in_worker(shim.engine.clone(), catalog_name.to_string())
            .await
            .map_err(|err| (ErrorKind::ER_BAD_DB_ERROR, err))?;
        shim.current_catalog = catalog;
        if shim.current_catalog.is_none()
            && !shim
                .engine
                .database_exists(&shim.current_db)
                .unwrap_or(false)
        {
            shim.current_db = DEFAULT_DATABASE.to_string();
        }
        return Ok(StatementResult::Ok);
    }

    if let Some(secs) = parse_set_query_timeout(trimmed) {
        shim.query_timeout_secs = if secs == 0 { None } else { Some(secs) };
        return Ok(StatementResult::Ok);
    }

    if let Some(max_len) = parse_set_group_concat_max_len(trimmed) {
        shim.group_concat_max_len = max_len;
        return Ok(StatementResult::Ok);
    }

    if let Some(dop) = parse_set_pipeline_dop(trimmed) {
        shim.pipeline_dop = if dop <= 0 { None } else { Some(dop) };
        return Ok(StatementResult::Ok);
    }

    if apply_broadcast_profile_set(&mut shim.optimizer_settings, trimmed) {
        return Ok(StatementResult::Ok);
    }

    if let Some((name, enabled)) = parse_set_boolean(trimmed) {
        match name.as_str() {
            "enable_ukfk_opt" => shim.optimizer_settings.enable_ukfk_opt = enabled,
            "enable_query_rewrite_table_prune" => {
                shim.optimizer_settings.enable_query_rewrite_table_prune = enabled
            }
            "enable_cbo_table_prune" => shim.optimizer_settings.enable_cbo_table_prune = enabled,
            "enable_table_prune_on_update" => {
                shim.optimizer_settings.enable_table_prune_on_update = enabled
            }
            "enable_eliminate_agg" => shim.optimizer_settings.enable_eliminate_agg = enabled,
            "enable_common_subexpr_reuse" => {
                shim.optimizer_settings.enable_common_subexpr_reuse = Some(enabled)
            }
            // Tri-state Option<bool> field: store Some(enabled) so an explicit
            // SET is preserved as an override; None elsewhere means "default".
            "enable_materialized_view_rewrite" => {
                shim.optimizer_settings.enable_materialized_view_rewrite = Some(enabled)
            }
            "cbo_enable_dp_join_reorder" => {
                shim.optimizer_settings.enable_dp_join_reorder = Some(enabled)
            }
            "cbo_enable_greedy_join_reorder" => {
                shim.optimizer_settings.enable_greedy_join_reorder = Some(enabled)
            }
            _ => return Ok(StatementResult::Ok),
        }
        return Ok(StatementResult::Ok);
    }

    for name in ["disable_optimizer_rules", "cbo_disabled_rules"] {
        if let Some(rules) = parse_set_string_csv(trimmed, name) {
            for rule in &rules {
                if !crate::sql::optimizer::is_known_rule_name(rule) {
                    warn!("unknown optimizer rule disabled via session: {rule}");
                }
            }
            shim.optimizer_settings.disabled_rules = rules;
            return Ok(StatementResult::Ok);
        }
    }

    if let Some(v) = parse_set_non_negative_integer(trimmed, "global_runtime_filter_build_max_size")
    {
        shim.optimizer_settings.rf_build_max_bytes = Some(v);
        return Ok(StatementResult::Ok);
    }

    if let Some(v) = parse_set_non_negative_integer(trimmed, "global_runtime_filter_build_min_size")
    {
        shim.optimizer_settings.rf_build_min_bytes = Some(v);
        return Ok(StatementResult::Ok);
    }

    if let Some(v) = parse_set_non_negative_integer(trimmed, "global_runtime_filter_probe_min_size")
    {
        shim.optimizer_settings.rf_probe_min_bytes = Some(v);
        return Ok(StatementResult::Ok);
    }

    // In-memo join-reorder size cutoffs (StarRocks `cbo_max_reorder_node*`).
    // Exact keyword match, so the `_use_*` variants never collide with the
    // shorter `cbo_max_reorder_node`.
    if let Some(v) = parse_set_non_negative_integer(trimmed, "cbo_max_reorder_node_use_exhaustive")
    {
        shim.optimizer_settings.max_reorder_node_use_exhaustive = Some(v as usize);
        return Ok(StatementResult::Ok);
    }
    if let Some(v) = parse_set_non_negative_integer(trimmed, "cbo_max_reorder_node_use_dp") {
        shim.optimizer_settings.max_reorder_node_use_dp = Some(v as usize);
        return Ok(StatementResult::Ok);
    }
    if let Some(v) = parse_set_non_negative_integer(trimmed, "cbo_max_reorder_node_use_greedy") {
        shim.optimizer_settings.max_reorder_node_use_greedy = Some(v as usize);
        return Ok(StatementResult::Ok);
    }
    if let Some(v) = parse_set_non_negative_integer(trimmed, "cbo_max_reorder_node") {
        shim.optimizer_settings.max_reorder_node = Some(v as usize);
        return Ok(StatementResult::Ok);
    }

    if let Some(v) = parse_set_f64(trimmed, "global_runtime_filter_probe_min_selectivity") {
        shim.optimizer_settings.rf_probe_min_selectivity = Some(v);
        return Ok(StatementResult::Ok);
    }

    if let Some((name, value)) = parse_set_user_variable_query(trimmed) {
        let value = materialize_user_variable_value(shim, &value).await?;
        shim.user_variables.insert(name, value);
        return Ok(StatementResult::Ok);
    }

    if let Some((name, mode)) =
        parse_admin_failpoint_query(trimmed).map_err(|err| (classify_query_error(&err), err))?
    {
        failpoint::update(&name, mode).map_err(|err| (classify_query_error(&err), err))?;
        return Ok(StatementResult::Ok);
    }

    match parse_admin_raise_engine_error_query(trimmed) {
        Ok(Some(err)) => return Err(format_engine_error_for_mysql(err)),
        Ok(None) => {}
        Err(err) => return Err((ErrorKind::ER_PARSE_ERROR, err)),
    }

    if is_session_noop(trimmed)
        && !is_backend_management_statement(trimmed)
        && !is_materialized_view_management_statement(trimmed)
        && !looks_like_show_alter_table_optimize(trimmed)
        && !looks_like_show_create_table(trimmed)
        && !crate::engine::statement::looks_like_show_create_view(trimmed)
        && !crate::engine::statement::looks_like_show_views(trimmed)
    {
        return Ok(StatementResult::Ok);
    }

    if let Some(schema) = parse_use_database_query(trimmed) {
        let context = resolve_database_context_in_worker(
            shim.engine.clone(),
            shim.current_catalog.clone(),
            schema.to_string(),
        )
        .await
        .map_err(|err| (ErrorKind::ER_BAD_DB_ERROR, err))?;
        shim.current_catalog = context.catalog;
        shim.current_db = context.database;
        return Ok(StatementResult::Ok);
    }

    let rewritten = substitute_session_user_variables(trimmed, &shim.user_variables)
        .map_err(|err| (ErrorKind::ER_PARSE_ERROR, err))?;

    if !is_supported_embedded_statement(&rewritten)
        && !is_backend_management_statement(&rewritten)
        && !is_materialized_view_management_statement(&rewritten)
        && !looks_like_show_alter_table_optimize(&rewritten)
        && !looks_like_show_create_table(&rewritten)
        && !crate::engine::statement::looks_like_show_create_view(&rewritten)
        && !crate::engine::statement::looks_like_show_views(&rewritten)
    {
        return Err((
            ErrorKind::ER_NOT_SUPPORTED_YET,
            "unsupported sql in standalone server v1".to_string(),
        ));
    }

    execute_sql_in_worker(shim, rewritten).await
}

async fn execute_sql_in_worker(
    shim: &NovaRocksMysqlShim,
    sql: String,
) -> Result<StatementResult, (ErrorKind, String)> {
    let session = shim.engine.session();
    let current_catalog = shim.current_catalog.clone();
    let current_db = shim.current_db.clone();
    let query_timeout = shim.query_timeout_secs;
    let optimizer_settings = shim.optimizer_settings.clone();
    let allow_throw_exception =
        crate::sql::parser::set_var_hint::extract_allow_throw_exception(&sql);
    let query_options = crate::thrift::internal_service::TQueryOptions {
        group_concat_max_len: Some(shim.group_concat_max_len),
        query_timeout: query_timeout.and_then(|secs| i32::try_from(secs).ok()),
        pipeline_dop: shim.pipeline_dop,
        allow_throw_exception: if allow_throw_exception {
            Some(true)
        } else {
            None
        },
        ..Default::default()
    };
    let client_disconnect_signal = Arc::clone(&shim.client_disconnect_signal);

    let join_handle = task::spawn_blocking(move || {
        crate::runtime::query_cancel::with_client_disconnect_signal(
            client_disconnect_signal,
            || {
                crate::sql::optimizer::options::with_session_optimizer_settings(
                    optimizer_settings,
                    || {
                        session.execute_in_context(
                            &sql,
                            current_catalog.as_deref(),
                            &current_db,
                            Some(query_options),
                        )
                    },
                )
            },
        )
    });
    let result = if let Some(secs) = query_timeout.filter(|secs| *secs > 0) {
        match tokio::time::timeout(std::time::Duration::from_secs(secs), join_handle).await {
            Ok(result) => result,
            Err(_) => {
                shim.client_disconnect_signal.store(true, Ordering::SeqCst);
                return Err(format_engine_error_for_mysql(EngineError::query_timeout(
                    format!("query timed out after {} ms", secs * 1000),
                )));
            }
        }
    } else {
        join_handle.await
    };

    match result {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => {
            let kind = classify_query_error(&err);
            Err((kind, err))
        }
        Err(err) => Err((
            ErrorKind::ER_UNKNOWN_ERROR,
            format!("standalone query worker failed: {err}"),
        )),
    }
}

async fn materialize_user_variable_value(
    shim: &NovaRocksMysqlShim,
    value: &str,
) -> Result<String, (ErrorKind, String)> {
    let Some(inner_query) = parenthesized_query(value) else {
        return Ok(value.to_string());
    };
    let rewritten = substitute_session_user_variables(inner_query, &shim.user_variables)
        .map_err(|err| (ErrorKind::ER_PARSE_ERROR, err))?;
    match execute_sql_in_worker(shim, rewritten).await? {
        StatementResult::Query(result) => query_result_to_user_variable_literal(&result),
        StatementResult::Ok => Err((
            ErrorKind::ER_WRONG_VALUE,
            "user variable assignment query did not return a value".to_string(),
        )),
    }
}

fn parenthesized_query(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    let inner = trimmed.strip_prefix('(')?.strip_suffix(')')?.trim();
    let lower = inner.to_ascii_lowercase();
    (lower.starts_with("select ") || lower.starts_with("with ")).then_some(inner)
}

fn query_result_to_user_variable_literal(
    result: &crate::engine::QueryResult,
) -> Result<String, (ErrorKind, String)> {
    if result.columns.len() != 1 {
        return Err((
            ErrorKind::ER_OPERAND_COLUMNS,
            format!(
                "user variable assignment expected 1 column, got {}",
                result.columns.len()
            ),
        ));
    }
    let row_count = result.row_count();
    if row_count == 0 {
        return Ok("null".to_string());
    }
    if row_count > 1 {
        return Err((
            ErrorKind::ER_SUBQUERY_NO_1_ROW,
            "Subquery returns more than 1 row".to_string(),
        ));
    }
    for chunk in &result.chunks {
        if chunk.len() == 0 {
            continue;
        }
        let column = chunk
            .columns()
            .first()
            .ok_or((ErrorKind::ER_UNKNOWN_ERROR, "empty query chunk".to_string()))?;
        let declared = result.columns.first().ok_or((
            ErrorKind::ER_UNKNOWN_ERROR,
            "user variable assignment missing column metadata".to_string(),
        ))?;
        return Ok(
            query_result_cell_to_user_variable_sql(column, &declared.data_type, 0)
                .map_err(|err| (ErrorKind::ER_UNKNOWN_ERROR, err))?,
        );
    }
    Ok("null".to_string())
}

fn query_result_cell_to_user_variable_sql(
    column: &arrow::array::ArrayRef,
    declared_type: &arrow::datatypes::DataType,
    row_idx: usize,
) -> Result<String, String> {
    if column.is_null(row_idx) {
        return Ok("NULL".to_string());
    }
    if let Some(text) = arrow_text_cell(column, row_idx) {
        return user_variable_text_to_sql(&text?, declared_type);
    }
    let literal = crate::engine::sql_expr::literal_from_batch(column, row_idx)?;
    user_variable_literal_to_sql(&literal)
}

fn arrow_text_cell(
    column: &arrow::array::ArrayRef,
    row_idx: usize,
) -> Option<Result<String, String>> {
    match column.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| "failed to downcast user variable value to StringArray".to_string());
            Some(arr.map(|arr| arr.value(row_idx).to_string()))
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            let arr = column
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| {
                    "failed to downcast user variable value to LargeStringArray".to_string()
                });
            Some(arr.map(|arr| arr.value(row_idx).to_string()))
        }
        arrow::datatypes::DataType::Binary => {
            let arr = column
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .ok_or_else(|| "failed to downcast user variable value to BinaryArray".to_string());
            Some(arr.map(|arr| String::from_utf8_lossy(arr.value(row_idx)).into_owned()))
        }
        arrow::datatypes::DataType::LargeBinary => {
            let arr = column
                .as_any()
                .downcast_ref::<arrow::array::LargeBinaryArray>()
                .ok_or_else(|| {
                    "failed to downcast user variable value to LargeBinaryArray".to_string()
                });
            Some(arr.map(|arr| String::from_utf8_lossy(arr.value(row_idx)).into_owned()))
        }
        _ => None,
    }
}

fn user_variable_text_to_sql(
    text: &str,
    declared_type: &arrow::datatypes::DataType,
) -> Result<String, String> {
    use arrow::datatypes::DataType;

    Ok(match declared_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => text.to_string(),
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            text.to_string()
        }
        DataType::Null => "NULL".to_string(),
        _ => single_quoted_user_variable_sql(text),
    })
}

fn user_variable_literal_to_sql(
    literal: &crate::sql::parser::ast::Literal,
) -> Result<String, String> {
    use crate::sql::parser::ast::Literal;

    Ok(match literal {
        Literal::Null => "NULL".to_string(),
        Literal::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => {
            if !value.is_finite() {
                return Err(format!(
                    "non-finite floating literal is not supported: {value}"
                ));
            }
            value.to_string()
        }
        Literal::String(value) | Literal::Date(value) => single_quoted_user_variable_sql(value),
        Literal::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(user_variable_literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        Literal::Map(entries) => {
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(user_variable_literal_to_sql(key)?);
                args.push(user_variable_literal_to_sql(value)?);
            }
            format!("map({})", args.join(", "))
        }
        Literal::Struct(values) => format!(
            "row({})",
            values
                .iter()
                .map(user_variable_literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
    })
}

fn single_quoted_user_variable_sql(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    for ch in value.chars() {
        match ch {
            '\'' => escaped.push_str("''"),
            '\\' => escaped.push_str(r"\\"),
            _ => escaped.push(ch),
        }
    }
    format!("'{escaped}'")
}

fn resolve_catalog_name(
    engine: &StandaloneNovaRocks,
    catalog_name: &str,
) -> Result<Option<String>, String> {
    let normalized = normalize_identifier(catalog_name)?;
    if normalized == DEFAULT_CATALOG {
        return Ok(None);
    }
    if engine.iceberg_catalog_exists(&normalized)? {
        Ok(Some(normalized))
    } else {
        Err(format!("unknown catalog `{catalog_name}`"))
    }
}

fn resolve_database_context(
    engine: &StandaloneNovaRocks,
    current_catalog: Option<&str>,
    schema: &str,
) -> Result<SessionDatabaseContext, String> {
    let parts = parse_object_name(schema)?;
    match parts.as_slice() {
        [database] => {
            let database = normalize_identifier(database)?;
            if let Some(catalog) = normalize_current_catalog(current_catalog)? {
                if engine.iceberg_namespace_exists(&catalog, &database)? {
                    Ok(SessionDatabaseContext {
                        catalog: Some(catalog),
                        database,
                    })
                } else {
                    Err(format!("unknown database `{schema}`"))
                }
            } else if engine.database_exists(&database)? {
                Ok(SessionDatabaseContext {
                    catalog: None,
                    database,
                })
            } else {
                Err(format!("unknown database `{schema}`"))
            }
        }
        [catalog_name, database_name] => {
            let catalog = resolve_catalog_name(engine, catalog_name)?;
            let database = normalize_identifier(database_name)?;
            match catalog {
                Some(catalog) => {
                    if engine.iceberg_namespace_exists(&catalog, &database)? {
                        Ok(SessionDatabaseContext {
                            catalog: Some(catalog),
                            database,
                        })
                    } else {
                        Err(format!("unknown database `{schema}`"))
                    }
                }
                None => {
                    if engine.database_exists(&database)? {
                        Ok(SessionDatabaseContext {
                            catalog: None,
                            database,
                        })
                    } else {
                        Err(format!("unknown database `{schema}`"))
                    }
                }
            }
        }
        _ => Err(format!(
            "unknown database `{schema}`; expected `<database>` or `<catalog>.<database>`"
        )),
    }
}

fn parse_object_name(raw: &str) -> Result<Vec<&str>, String> {
    // MySQL COM_INIT_DB strips the outermost backtick pair, producing strings
    // like: catalog`.`db  (original was `catalog`.`db`).
    // Split on the "`.`" pattern first, then fall back to plain '.'.
    let parts: Vec<&str> = if raw.contains("`.`") {
        raw.split("`.`")
            .map(|s| s.trim().trim_matches('`'))
            .collect()
    } else {
        raw.split('.')
            .map(str::trim)
            .map(strip_identifier_quotes)
            .collect()
    };

    for part in &parts {
        if part.is_empty() {
            return Err(format!("unsupported identifier `{raw}`"));
        }
    }
    Ok(parts)
}

fn strip_identifier_quotes(raw: &str) -> &str {
    raw.strip_prefix('`')
        .and_then(|inner| inner.strip_suffix('`'))
        .unwrap_or(raw)
}

fn normalize_current_catalog(current_catalog: Option<&str>) -> Result<Option<String>, String> {
    match current_catalog {
        Some(catalog) => resolve_catalog_name_for_context(catalog).map(Some),
        None => Ok(None),
    }
}

fn resolve_catalog_name_for_context(catalog_name: &str) -> Result<String, String> {
    let normalized = normalize_identifier(catalog_name)?;
    if normalized == DEFAULT_CATALOG {
        Err(format!(
            "default catalog `{DEFAULT_CATALOG}` must use local standalone catalog context"
        ))
    } else {
        Ok(normalized)
    }
}

fn classify_query_error(err: &str) -> ErrorKind {
    let lower = err.to_ascii_lowercase();
    if lower.contains("database already exists") {
        ErrorKind::ER_DB_CREATE_EXISTS
    } else if lower.contains("unknown database") || lower.contains("unknown catalog") {
        ErrorKind::ER_BAD_DB_ERROR
    } else if lower.contains("table already exists") {
        ErrorKind::ER_TABLE_EXISTS_ERROR
    } else if lower.contains("unknown table") {
        ErrorKind::ER_NO_SUCH_TABLE
    } else if lower.contains("unknown column") {
        ErrorKind::ER_BAD_FIELD_ERROR
    } else if lower.contains("unsupported") || lower.contains("does not support") {
        ErrorKind::ER_NOT_SUPPORTED_YET
    } else if lower.contains("expected")
        || lower.contains("unexpected")
        || lower.contains("identifier")
        || lower.contains("unterminated")
        || lower.contains("invalid")
    {
        ErrorKind::ER_PARSE_ERROR
    } else {
        ErrorKind::ER_UNKNOWN_ERROR
    }
}

fn format_engine_error_for_mysql(err: EngineError) -> (ErrorKind, String) {
    let kind = err.to_mysql_error_kind();
    let message = err.to_bracketed_user_message();
    (kind, message)
}

fn parse_admin_failpoint_query(query: &str) -> Result<Option<(String, FailPointMode)>, String> {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() < 3
        || !parts[0].eq_ignore_ascii_case("admin")
        || !parts[2].eq_ignore_ascii_case("failpoint")
    {
        return Ok(None);
    }

    let mode = if parts[1].eq_ignore_ascii_case("enable") {
        FailPointMode::Enable
    } else if parts[1].eq_ignore_ascii_case("disable") {
        FailPointMode::Disable
    } else {
        return Ok(None);
    };

    if parts.len() != 4 {
        return Err("expected ADMIN ENABLE/DISABLE FAILPOINT '<failpoint_name>'".to_string());
    }

    let name = strip_string_quotes(parts[3])
        .ok_or_else(|| "expected ADMIN ENABLE/DISABLE FAILPOINT '<failpoint_name>'".to_string())?;
    if name.is_empty() {
        return Err("failpoint name must not be empty".to_string());
    }

    Ok(Some((name.to_string(), mode)))
}

fn parse_admin_raise_engine_error_query(query: &str) -> Result<Option<EngineError>, String> {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() < 4
        || !parts[0].eq_ignore_ascii_case("admin")
        || !parts[1].eq_ignore_ascii_case("raise")
        || !parts[2].eq_ignore_ascii_case("engine")
        || !parts[3].eq_ignore_ascii_case("error")
    {
        return Ok(None);
    }

    if parts.len() != 5 {
        return Err("expected ADMIN RAISE ENGINE ERROR '<engine_error_code>'".to_string());
    }

    let raw_code = strip_string_quotes(parts[4])
        .ok_or_else(|| "expected ADMIN RAISE ENGINE ERROR '<engine_error_code>'".to_string())?;
    let code = EngineErrorCode::parse(raw_code)
        .ok_or_else(|| format!("unknown engine error code: {raw_code}"))?;
    let err = match code {
        EngineErrorCode::UnsupportedDistributedDmlShape => {
            EngineError::unsupported_distributed_dml_shape(
                "ADMIN RAISE ENGINE ERROR",
                "forced P8 SQL runner error-code smoke",
            )
        }
        EngineErrorCode::IcebergWriteDescriptorMismatch => {
            EngineError::iceberg_write_descriptor_mismatch("forced P8 SQL runner error-code smoke")
        }
        EngineErrorCode::CommitKnownUncommitted => {
            EngineError::commit_known_uncommitted("forced P8 SQL runner error-code smoke")
        }
        EngineErrorCode::CommitUnknown => {
            EngineError::commit_unknown("forced P8 SQL runner error-code smoke")
        }
        EngineErrorCode::CommitKnownCommittedFinalizeFailed => {
            EngineError::commit_known_committed_finalize_failed(
                "forced P8 SQL runner error-code smoke",
            )
        }
        EngineErrorCode::ProtocolDecodeError => {
            EngineError::protocol_decode("forced P8 SQL runner error-code smoke")
        }
        _ => {
            return Err(format!(
                "unsupported engine error code for ADMIN RAISE ENGINE ERROR: {raw_code}"
            ));
        }
    };

    Ok(Some(err))
}

fn strip_string_quotes(raw: &str) -> Option<&str> {
    raw.strip_prefix('\'')
        .and_then(|inner| inner.strip_suffix('\''))
        .or_else(|| {
            raw.strip_prefix('"')
                .and_then(|inner| inner.strip_suffix('"'))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_disconnect_watcher_treats_unexpected_peek_errors_as_disconnects() {
        let source = include_str!("mod.rs");
        assert!(
            source.contains(
                "_ => {\n                        watcher_disconnected.store(true, Ordering::SeqCst);\n                        break;\n                    }"
            ),
            "unexpected peek errors should conservatively mark the client disconnected"
        );
    }

    #[test]
    fn parse_set_query_timeout_accepts_common_forms() {
        assert_eq!(parse_set_query_timeout("SET query_timeout = 60"), Some(60));
        assert_eq!(parse_set_query_timeout("set query_timeout=30"), Some(30));
        assert_eq!(parse_set_query_timeout("SET QUERY_TIMEOUT = 0"), Some(0));
        assert_eq!(
            parse_set_query_timeout("SET    query_timeout    =    120"),
            Some(120)
        );
    }

    #[test]
    fn parse_set_query_timeout_rejects_unrelated_set_statements() {
        assert_eq!(parse_set_query_timeout("SET catalog = foo"), None);
        assert_eq!(parse_set_query_timeout("SELECT 1"), None);
        assert_eq!(parse_set_query_timeout("SET query_timeout = abc"), None);
        assert_eq!(parse_set_query_timeout("SET query_timeout"), None);
        assert_eq!(
            parse_set_query_timeout("SET query_timeout = 60 extra"),
            None
        );
    }

    #[test]
    fn parse_set_pipeline_dop_accepts_and_rejects() {
        assert_eq!(parse_set_pipeline_dop("SET pipeline_dop = 8"), Some(8));
        assert_eq!(parse_set_pipeline_dop("set pipeline_dop=1"), Some(1));
        // 0 is accepted by the parser and interpreted as "clear override" by the dispatcher.
        assert_eq!(parse_set_pipeline_dop("SET PIPELINE_DOP = 0"), Some(0));
        // Unrelated / malformed statements do not match.
        assert_eq!(parse_set_pipeline_dop("SET query_timeout = 8"), None);
        assert_eq!(parse_set_pipeline_dop("SELECT 1"), None);
        assert_eq!(parse_set_pipeline_dop("SET pipeline_dop = abc"), None);
    }

    #[test]
    fn parse_set_group_concat_max_len_accepts_common_forms() {
        assert_eq!(
            parse_set_group_concat_max_len("SET group_concat_max_len = 65535"),
            Some(65535)
        );
        assert_eq!(
            parse_set_group_concat_max_len("set group_concat_max_len=4096"),
            Some(4096)
        );
        assert_eq!(
            parse_set_group_concat_max_len("SET GROUP_CONCAT_MAX_LEN = 0"),
            Some(0)
        );
    }

    #[test]
    fn parse_set_group_concat_max_len_rejects_unrelated_statements() {
        assert_eq!(
            parse_set_group_concat_max_len("SET query_timeout = 60"),
            None
        );
        assert_eq!(parse_set_group_concat_max_len("SELECT 1"), None);
        assert_eq!(
            parse_set_group_concat_max_len("SET group_concat_max_len = abc"),
            None
        );
        assert_eq!(
            parse_set_group_concat_max_len("SET group_concat_max_len"),
            None
        );
    }

    #[test]
    fn parse_set_user_variable_accepts_expression_assignment() {
        assert_eq!(
            parse_set_user_variable_query(
                "SET @var = array_map(x -> CAST(x AS STRING), array_generate(1, 2000000, 1))"
            ),
            Some((
                "@var".to_string(),
                "array_map(x -> CAST(x AS STRING), array_generate(1, 2000000, 1))".to_string()
            ))
        );
    }

    #[test]
    fn parse_set_user_variable_accepts_integer_literal() {
        assert_eq!(
            parse_set_user_variable_query("SET @i = 1"),
            Some(("@i".to_string(), "1".to_string()))
        );
        assert_eq!(
            parse_set_user_variable_query("SET @counter = 42"),
            Some(("@counter".to_string(), "42".to_string()))
        );
    }

    #[test]
    fn parse_set_user_variable_accepts_string_literal() {
        assert_eq!(
            parse_set_user_variable_query("SET @s = 'hello'"),
            Some(("@s".to_string(), "'hello'".to_string()))
        );
    }

    #[test]
    fn parse_set_user_variable_accepts_null() {
        assert_eq!(
            parse_set_user_variable_query("SET @n = NULL"),
            Some(("@n".to_string(), "NULL".to_string()))
        );
    }

    #[test]
    fn query_result_to_user_variable_literal_preserves_array_expression() {
        let mut builder = arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
        builder.values().append_value("alpha");
        builder.values().append_value("a'b\\c");
        builder.append(true);
        let array = std::sync::Arc::new(builder.finish()) as arrow::array::ArrayRef;
        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("arr", array.data_type().clone(), true),
            ])),
            vec![array],
        )
        .expect("record batch");
        let chunk = crate::engine::record_batch_to_chunk(batch).expect("chunk");
        let result = crate::runtime::query_result::QueryResult {
            columns: vec![crate::runtime::query_result::QueryResultColumn {
                name: "arr".to_string(),
                data_type: chunk.columns()[0].data_type().clone(),
                nullable: true,
                logical_type: None,
            }],
            chunks: vec![chunk],
        };

        assert_eq!(
            query_result_to_user_variable_literal(&result).unwrap(),
            "['alpha', 'a''b\\\\c']"
        );
    }

    #[test]
    fn query_result_to_user_variable_literal_preserves_remote_text_array_expression() {
        let array = std::sync::Arc::new(arrow::array::BinaryArray::from(vec![Some(
            br#"["alpha","beta"]"#.as_slice(),
        )])) as arrow::array::ArrayRef;
        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("arr", arrow::datatypes::DataType::Binary, true),
            ])),
            vec![array],
        )
        .expect("record batch");
        let chunk = crate::engine::record_batch_to_chunk(batch).expect("chunk");
        let result = crate::runtime::query_result::QueryResult {
            columns: vec![crate::runtime::query_result::QueryResultColumn {
                name: "arr".to_string(),
                data_type: arrow::datatypes::DataType::List(std::sync::Arc::new(
                    arrow::datatypes::Field::new("item", arrow::datatypes::DataType::Utf8, true),
                )),
                nullable: true,
                logical_type: None,
            }],
            chunks: vec![chunk],
        };

        assert_eq!(
            query_result_to_user_variable_literal(&result).unwrap(),
            r#"["alpha","beta"]"#
        );
    }

    #[test]
    fn user_variable_literal_to_sql_formats_struct_and_map() {
        use crate::sql::parser::ast::Literal;

        let literal = Literal::Struct(vec![
            Literal::Int(1),
            Literal::Map(vec![(
                Literal::String("k".to_string()),
                Literal::Bool(true),
            )]),
        ]);

        assert_eq!(
            user_variable_literal_to_sql(&literal).unwrap(),
            "row(1, map('k', TRUE))"
        );
    }

    /// Regression test: `parse_set_boolean` must not swallow `SET @i = 1`
    /// before `parse_set_user_variable_query` gets a chance to handle it.
    /// Previously, `"1"` matched the boolean token `"1"` and the user
    /// variable was silently discarded.
    #[test]
    fn parse_set_boolean_does_not_swallow_user_variables() {
        // @i = 1 must NOT be matched by the boolean handler.
        assert_eq!(parse_set_boolean("SET @i = 1"), None);
        assert_eq!(parse_set_boolean("SET @i = 0"), None);
        assert_eq!(parse_set_boolean("SET @flag = true"), None);
    }

    #[test]
    fn apply_broadcast_profile_set_accepts_zero_values() {
        let mut settings = SessionOptimizerSettings::default();

        assert!(apply_broadcast_profile_set(
            &mut settings,
            "SET cbo_broadcast_backend_count = 0"
        ));
        assert_eq!(settings.cbo_broadcast_backend_count, Some(0.0));

        assert!(apply_broadcast_profile_set(
            &mut settings,
            "SET cbo_broadcast_node_mem_budget_bytes = 0"
        ));
        assert_eq!(settings.cbo_broadcast_node_mem_budget_bytes, Some(0.0));
    }

    #[test]
    fn apply_broadcast_profile_set_accepts_one_values() {
        let mut settings = SessionOptimizerSettings::default();

        assert!(apply_broadcast_profile_set(
            &mut settings,
            "SET cbo_broadcast_backend_count = 1"
        ));
        assert_eq!(settings.cbo_broadcast_backend_count, Some(1.0));

        assert!(apply_broadcast_profile_set(
            &mut settings,
            "SET cbo_broadcast_node_mem_budget_bytes = 1"
        ));
        assert_eq!(settings.cbo_broadcast_node_mem_budget_bytes, Some(1.0));
    }

    #[test]
    fn apply_broadcast_profile_set_accepts_normal_values() {
        let mut settings = SessionOptimizerSettings::default();

        assert!(apply_broadcast_profile_set(
            &mut settings,
            "SET cbo_broadcast_backend_count = 3"
        ));
        assert_eq!(settings.cbo_broadcast_backend_count, Some(3.0));

        assert!(apply_broadcast_profile_set(
            &mut settings,
            "SET cbo_broadcast_node_mem_budget_bytes = 3"
        ));
        assert_eq!(settings.cbo_broadcast_node_mem_budget_bytes, Some(3.0));
    }

    #[test]
    fn execute_statement_text_applies_broadcast_profile_before_boolean_set() {
        let source = include_str!("mod.rs");
        let helper_call = source
            .find("apply_broadcast_profile_set(&mut shim.optimizer_settings, trimmed)")
            .expect("execute_statement_text should call broadcast profile helper");
        let boolean_call = source
            .find("parse_set_boolean(trimmed)")
            .expect("execute_statement_text should call boolean parser");

        assert!(
            helper_call < boolean_call,
            "broadcast profile SET parser must run before generic boolean SET parser"
        );
    }

    #[test]
    fn substitute_session_user_variables_replaces_integer() {
        let mut vars = BTreeMap::new();
        vars.insert("@i".to_string(), "1".to_string());
        assert_eq!(
            substitute_session_user_variables("SELECT @i AS val", &vars).unwrap(),
            "SELECT 1 AS val"
        );
    }

    #[test]
    fn substitute_session_user_variables_replaces_string() {
        let mut vars = BTreeMap::new();
        vars.insert("@s".to_string(), "'hello'".to_string());
        assert_eq!(
            substitute_session_user_variables("SELECT @s AS val", &vars).unwrap(),
            "SELECT 'hello' AS val"
        );
    }

    #[test]
    fn substitute_session_user_variables_leaves_unbound_as_null_placeholder() {
        let vars = BTreeMap::new();
        // Unbound: pass-through (will fail downstream in the engine, but that
        // is handled by treating @unbound as NULL after substitution lands).
        assert_eq!(
            substitute_session_user_variables("SELECT @unbound AS v", &vars).unwrap(),
            "SELECT @unbound AS v"
        );
    }

    #[test]
    fn substitute_session_user_variables_in_insert_values() {
        let mut vars = BTreeMap::new();
        vars.insert("@i".to_string(), "5".to_string());
        assert_eq!(
            substitute_session_user_variables("INSERT INTO t VALUES (@i, @i)", &vars).unwrap(),
            "INSERT INTO t VALUES (5, 5)"
        );
    }

    #[test]
    fn delete_is_dispatched_to_embedded_engine_not_session_noop() {
        let sql = "DELETE FROM ice.ns.orders WHERE id = 1";
        assert!(
            !is_session_noop(sql),
            "DELETE must reach the embedded engine so Iceberg row deletes are committed"
        );
        assert!(is_supported_embedded_statement(sql));
    }

    #[test]
    fn call_is_dispatched_to_embedded_engine() {
        let sql = "CALL ice.system.rewrite_manifests(table => 'ns.orders')";
        assert!(!is_session_noop(sql));
        assert!(is_supported_embedded_statement(sql));
    }

    #[test]
    fn backend_management_reaches_embedded_engine() {
        assert!(
            is_backend_management_statement("ADD BACKEND '127.0.0.1:19050'"),
            "ADD BACKEND must bypass the standalone-server unsupported-SQL gate"
        );
        assert!(
            is_backend_management_statement("DROP BACKEND '127.0.0.1:19050' FORCE"),
            "DROP BACKEND must route to the engine-owned parser"
        );
        assert!(
            is_backend_management_statement("SHOW BACKENDS"),
            "SHOW BACKENDS must not be swallowed as a session no-op"
        );
    }

    #[test]
    fn parse_admin_failpoint_accepts_enable_disable() {
        assert_eq!(
            parse_admin_failpoint_query("admin enable failpoint 'agg_hash_set_bad_alloc'"),
            Ok(Some((
                "agg_hash_set_bad_alloc".to_string(),
                FailPointMode::Enable
            )))
        );
        assert_eq!(
            parse_admin_failpoint_query(
                "ADMIN DISABLE FAILPOINT \"aggregate_build_hash_map_bad_alloc\""
            ),
            Ok(Some((
                "aggregate_build_hash_map_bad_alloc".to_string(),
                FailPointMode::Disable
            )))
        );
    }

    #[test]
    fn parse_admin_failpoint_rejects_malformed_target() {
        assert!(parse_admin_failpoint_query("admin enable failpoint").is_err());
        assert!(
            parse_admin_failpoint_query("admin enable failpoint agg_hash_set_bad_alloc").is_err()
        );
        assert_eq!(parse_admin_failpoint_query("admin show config"), Ok(None));
    }

    #[test]
    fn parse_admin_raise_engine_error_accepts_supported_code() {
        let err = parse_admin_raise_engine_error_query(
            "ADMIN RAISE ENGINE ERROR 'IcebergWriteDescriptorMismatch'",
        )
        .expect("parse ok")
        .expect("matched");

        assert_eq!(
            err.to_bracketed_user_message(),
            "[IcebergWriteDescriptorMismatch] forced P8 SQL runner error-code smoke"
        );
    }

    #[test]
    fn parse_admin_raise_engine_error_rejects_unknown_code() {
        let err = parse_admin_raise_engine_error_query("ADMIN RAISE ENGINE ERROR 'NotARealCode'")
            .expect_err("unknown code should fail");

        assert!(
            err.contains("unknown engine error code"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_set_string_csv_accepts_quoted_value() {
        assert_eq!(
            parse_set_string_csv(
                "SET disable_optimizer_rules = 'JoinCommutativity'",
                "disable_optimizer_rules"
            ),
            Some(vec!["JoinCommutativity".to_string()]),
        );
    }

    #[test]
    fn parse_set_string_csv_accepts_unquoted_value() {
        assert_eq!(
            parse_set_string_csv(
                "SET disable_optimizer_rules = CommonSubexpressionReuse",
                "disable_optimizer_rules"
            ),
            Some(vec!["CommonSubexpressionReuse".to_string()]),
        );
    }

    #[test]
    fn parse_set_string_csv_splits_comma_list() {
        assert_eq!(
            parse_set_string_csv(
                "SET disable_optimizer_rules = 'A,B,C'",
                "disable_optimizer_rules"
            ),
            Some(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
        );
    }

    #[test]
    fn parse_set_string_csv_trims_spaces_within_list() {
        assert_eq!(
            parse_set_string_csv(
                "SET disable_optimizer_rules = ' A , B '",
                "disable_optimizer_rules"
            ),
            Some(vec!["A".to_string(), "B".to_string()]),
        );
    }

    #[test]
    fn parse_set_string_csv_empty_value_returns_empty_list() {
        assert_eq!(
            parse_set_string_csv(
                "SET disable_optimizer_rules = ''",
                "disable_optimizer_rules"
            ),
            Some(vec![]),
        );
    }

    #[test]
    fn parse_set_string_csv_accepts_alias_target_name() {
        assert_eq!(
            parse_set_string_csv("SET cbo_disabled_rules = 'X'", "cbo_disabled_rules"),
            Some(vec!["X".to_string()]),
        );
        // And rejects the wrong name.
        assert_eq!(
            parse_set_string_csv("SET disable_optimizer_rules = 'X'", "cbo_disabled_rules"),
            None,
        );
    }

    #[test]
    fn parse_set_string_csv_rejects_unrelated_set_statements() {
        assert_eq!(
            parse_set_string_csv("SET query_timeout = 60", "disable_optimizer_rules"),
            None,
        );
        assert_eq!(
            parse_set_string_csv("SELECT 1", "disable_optimizer_rules"),
            None,
        );
    }

    #[test]
    fn parse_set_string_csv_requires_word_boundary_after_name() {
        // disable_optimizer_rules_extra should NOT match disable_optimizer_rules.
        assert_eq!(
            parse_set_string_csv(
                "SET disable_optimizer_rules_extra = 'X'",
                "disable_optimizer_rules"
            ),
            None,
        );
    }

    #[test]
    fn parse_set_string_csv_accepts_unknown_rule_name() {
        // The server-side warn! path is in execute_statement_text; we can't
        // easily integration-test that without a running shim. Here we just
        // confirm parse_set_string_csv itself doesn't filter unknown names —
        // they pass through and the runtime is_known_rule_name check fires
        // the warn separately.
        let rules = parse_set_string_csv(
            "SET disable_optimizer_rules = 'TotallyNotARealRule'",
            "disable_optimizer_rules",
        )
        .expect("parse ok");
        assert_eq!(rules, vec!["TotallyNotARealRule".to_string()]);
        assert!(!crate::sql::optimizer::is_known_rule_name(
            "TotallyNotARealRule"
        ));
    }

    // I1: resolve_server_options_from_config must extract settings from a
    // pre-loaded NovaRocksConfig without touching the filesystem.
    #[test]
    fn resolve_settings_from_cfg_uses_sentinel_mysql_port() {
        use crate::common::app_config::StandaloneServerConfig;
        let mut cfg = NovaRocksConfig::default();
        cfg.standalone_server = Some(StandaloneServerConfig {
            mysql_port: 12345,
            ..StandaloneServerConfig::default()
        });
        let resolved =
            resolve_server_options_from_config(&cfg, None).expect("extract settings from cfg");
        assert_eq!(
            resolved.mysql_port, 12345,
            "mysql_port must come from the pre-loaded cfg, not from a fresh file load"
        );
    }

    #[test]
    fn resolve_settings_from_cfg_port_override_wins() {
        use crate::common::app_config::StandaloneServerConfig;
        let mut cfg = NovaRocksConfig::default();
        cfg.standalone_server = Some(StandaloneServerConfig {
            mysql_port: 12345,
            ..StandaloneServerConfig::default()
        });
        let resolved = resolve_server_options_from_config(&cfg, Some(19030))
            .expect("extract settings with port override");
        assert_eq!(
            resolved.mysql_port, 19030,
            "explicit port override must win over config"
        );
    }

    #[test]
    fn resolve_settings_from_cfg_defaults_when_no_standalone_section() {
        // I1: When standalone_server section is absent, DEFAULT_MYSQL_PORT is used.
        let cfg = NovaRocksConfig::default();
        let resolved =
            resolve_server_options_from_config(&cfg, None).expect("defaults from empty cfg");
        assert_eq!(
            resolved.mysql_port, DEFAULT_MYSQL_PORT,
            "default port when no [standalone_server] section"
        );
    }

    #[test]
    fn role_fe_server_options_start_report_endpoint_without_local_exchange_execution() {
        let mut cfg = NovaRocksConfig::default();
        cfg.cluster.role = crate::common::app_config::ClusterRole::Fe;
        cfg.cluster.backends = vec!["127.0.0.1:19070".to_string()];

        let opts = test_resolve_fe_server_options(cfg, None).expect("resolve role=fe options");
        assert!(opts.start_coordinator_report_grpc);
        assert!(!opts.start_local_exchange_execution);
    }

    #[test]
    fn role_fe_server_options_use_configured_grpc_bind_host() {
        let mut cfg = NovaRocksConfig::default();
        cfg.server.host = "0.0.0.0".to_string();
        cfg.cluster.role = crate::common::app_config::ClusterRole::Fe;
        cfg.cluster.backends = vec!["127.0.0.1:19070".to_string()];

        let opts = test_resolve_fe_server_options(cfg, None).expect("resolve role=fe options");
        assert_eq!(opts.grpc_bind_host, "0.0.0.0");
    }

    #[test]
    fn parse_rf_build_max_size_var() {
        assert_eq!(
            parse_set_non_negative_integer(
                "SET global_runtime_filter_build_max_size = 1048576",
                "global_runtime_filter_build_max_size"
            ),
            Some(1048576)
        );
    }

    #[test]
    fn parse_rf_probe_min_selectivity_float() {
        let v = parse_set_f64(
            "SET global_runtime_filter_probe_min_selectivity = 0.9",
            "global_runtime_filter_probe_min_selectivity",
        );
        assert!((v.unwrap() - 0.9).abs() < 1e-9);
    }

    #[test]
    fn parse_set_f64_rejects_non_matching_keyword() {
        assert_eq!(
            parse_set_f64(
                "SET something_else = 0.5",
                "global_runtime_filter_probe_min_selectivity"
            ),
            None
        );
    }

    #[test]
    fn standalone_server_source_has_no_native_disconnect_watcher_thread() {
        let source = include_str!("mod.rs");
        let needle = ["std::thread", "::JoinHandle<()>"].concat();
        assert!(
            !source.contains(&needle),
            "standalone server should not keep a native disconnect watcher thread per session"
        );
    }
}
