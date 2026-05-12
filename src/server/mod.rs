mod encoding;

use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

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

use crate::common::failpoint::{self, FailPointMode};
use crate::novarocks_config::{
    NovaRocksConfig, StandaloneServerConfig as AppStandaloneServerConfig,
};
use crate::version;

use self::encoding::write_query_result;
use crate::engine::catalog::{DEFAULT_DATABASE, normalize_identifier};
use crate::engine::statement::looks_like_show_alter_table_optimize;
use crate::engine::{StandaloneNovaRocks, StandaloneOptions, StatementResult};

const DEFAULT_MYSQL_PORT: u16 = 9030;
const DEFAULT_CATALOG: &str = "default_catalog";
const ROOT_USER: &str = "root";
static NEXT_CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneTableConfig {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StandaloneServerOptions {
    pub config_path: Option<PathBuf>,
    pub mysql_port: Option<u16>,
    pub tables: Vec<StandaloneTableConfig>,
}

#[derive(Clone, Debug)]
struct ResolvedStandaloneServerOptions {
    config_path: Option<PathBuf>,
    mysql_port: u16,
    user: String,
    tables: Vec<StandaloneTableConfig>,
}

pub fn run_standalone_server(opts: StandaloneServerOptions) -> Result<(), String> {
    let resolved = resolve_server_options(&opts)?;
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: resolved.config_path.clone(),
    })?;
    preload_tables(&engine, &resolved.tables)?;
    crate::engine::register_stream_load_engine(engine.clone());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("build tokio runtime failed: {e}"))?;

    runtime.block_on(serve_forever(
        engine,
        resolved.mysql_port,
        resolved.user.clone(),
    ))
}

fn resolve_server_options(
    opts: &StandaloneServerOptions,
) -> Result<ResolvedStandaloneServerOptions, String> {
    let active_config_path = resolve_active_config_path(opts.config_path.as_deref());
    let file_cfg = load_active_config(active_config_path.as_deref())?;
    let config_base_dir = active_config_path
        .as_deref()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    let mut mysql_port = DEFAULT_MYSQL_PORT;
    let mut user = ROOT_USER.to_string();
    let mut tables = BTreeMap::new();

    if let Some(app_cfg) = file_cfg.as_ref()
        && let Some(standalone) = app_cfg.standalone_server.as_ref()
    {
        mysql_port = standalone.mysql_port;
        if standalone.user != ROOT_USER {
            return Err(format!(
                "standalone server only supports user `{ROOT_USER}`, got `{}`",
                standalone.user
            ));
        }
        user = standalone.user.clone();
        merge_config_tables(&mut tables, standalone, &config_base_dir)?;
    }

    if let Some(port) = opts.mysql_port {
        mysql_port = port;
    }
    for table in &opts.tables {
        let key = normalize_identifier(&table.name)?;
        tables.insert(
            key,
            StandaloneTableConfig {
                name: normalize_identifier(&table.name)?,
                path: table.path.clone(),
            },
        );
    }
    let tables = tables.into_values().collect::<Vec<_>>();

    Ok(ResolvedStandaloneServerOptions {
        config_path: opts.config_path.clone(),
        mysql_port,
        user,
        tables,
    })
}

fn resolve_active_config_path(explicit: Option<&Path>) -> Option<PathBuf> {
    explicit
        .map(Path::to_path_buf)
        .or_else(|| {
            std::env::var("NOVAROCKS_CONFIG")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        })
        .or_else(|| {
            let default_path = PathBuf::from("novarocks.toml");
            default_path.exists().then_some(default_path)
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

fn merge_config_tables(
    tables: &mut BTreeMap<String, StandaloneTableConfig>,
    standalone: &AppStandaloneServerConfig,
    base_dir: &Path,
) -> Result<(), String> {
    for table in &standalone.tables {
        let key = normalize_identifier(&table.name)?;
        let path = if table.path.is_absolute() {
            table.path.clone()
        } else {
            base_dir.join(&table.path)
        };
        tables.insert(
            key,
            StandaloneTableConfig {
                name: normalize_identifier(&table.name)?,
                path,
            },
        );
    }
    Ok(())
}

fn preload_tables(
    engine: &StandaloneNovaRocks,
    tables: &[StandaloneTableConfig],
) -> Result<(), String> {
    for table in tables {
        engine.register_parquet_table(&table.name, &table.path)?;
    }
    Ok(())
}

async fn serve_forever(
    engine: StandaloneNovaRocks,
    mysql_port: u16,
    user: String,
) -> Result<(), String> {
    // Start gRPC exchange server for multi-fragment CTE execution.
    // Uses the configured http_port (default 8040).
    let grpc_port = crate::common::config::http_port();
    match crate::service::grpc_server::start_grpc_exchange_server("127.0.0.1", grpc_port) {
        Ok(()) => {
            info!(
                "standalone grpc exchange server started on 127.0.0.1:{}",
                grpc_port
            );
        }
        Err(e) => {
            warn!(
                "failed to start standalone grpc exchange server on port {}: {} \
                 (multi-fragment CTE queries will not work)",
                grpc_port, e
            );
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
            let shim = NovaRocksMysqlShim::new(engine, user, connection_id);
            let (reader, writer) = stream.into_split();
            if let Err(err) = AsyncMysqlIntermediary::run_on(shim, reader, writer).await {
                warn!(
                    "standalone mysql connection failed: peer={}, connection_id={}, err={}",
                    peer_addr, connection_id, err
                );
            }
        });
    }
}

struct NovaRocksMysqlShim {
    engine: StandaloneNovaRocks,
    user: String,
    connection_id: u32,
    current_catalog: Option<String>,
    current_db: String,
    /// Per-session query timeout (in seconds). `None` means no timeout.
    /// Set via `SET query_timeout = N`. `N == 0` clears the timeout.
    query_timeout_secs: Option<u64>,
    /// Per-session group_concat limit (in bytes).
    /// Set via `SET group_concat_max_len = N`.
    group_concat_max_len: i64,
    user_variables: BTreeMap<String, String>,
}

impl NovaRocksMysqlShim {
    fn new(engine: StandaloneNovaRocks, user: String, connection_id: u32) -> Self {
        Self {
            engine,
            user,
            connection_id,
            current_catalog: None,
            current_db: DEFAULT_DATABASE.to_string(),
            query_timeout_secs: None,
            group_concat_max_len: 1024,
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

/// Parse `SET query_timeout = N` and `SET query_timeout=N`. Returns the
/// integer seconds value if the statement matches that shape. The optional
/// `=` separator may have spaces around it or be glued to the keyword/value.
/// `N` must be a non-negative integer; `N == 0` clears the session timeout.
fn parse_set_query_timeout(query: &str) -> Option<u64> {
    parse_set_non_negative_integer(query, "query_timeout")
}

/// Parse `SET group_concat_max_len = N` and `SET group_concat_max_len=N`.
/// `N` must be a non-negative integer and is clamped later by FE-compatible
/// lowering rules.
fn parse_set_group_concat_max_len(query: &str) -> Option<i64> {
    let value = parse_set_non_negative_integer(query, "group_concat_max_len")?;
    i64::try_from(value).ok()
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

    if let Some((name, value)) = parse_set_user_variable_query(trimmed) {
        shim.user_variables.insert(name, value);
        return Ok(StatementResult::Ok);
    }

    if let Some((name, mode)) =
        parse_admin_failpoint_query(trimmed).map_err(|err| (classify_query_error(&err), err))?
    {
        failpoint::update(&name, mode).map_err(|err| (classify_query_error(&err), err))?;
        return Ok(StatementResult::Ok);
    }

    if is_session_noop(trimmed)
        && !is_materialized_view_management_statement(trimmed)
        && !looks_like_show_alter_table_optimize(trimmed)
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
        && !is_materialized_view_management_statement(&rewritten)
        && !looks_like_show_alter_table_optimize(&rewritten)
    {
        return Err((
            ErrorKind::ER_NOT_SUPPORTED_YET,
            "unsupported sql in standalone server v1".to_string(),
        ));
    }

    let session = shim.engine.session();
    let sql = rewritten;
    let current_catalog = shim.current_catalog.clone();
    let current_db = shim.current_db.clone();
    let query_timeout = shim.query_timeout_secs;
    let query_options = crate::internal_service::TQueryOptions {
        group_concat_max_len: Some(shim.group_concat_max_len),
        ..Default::default()
    };

    let join_handle = task::spawn_blocking(move || {
        session.execute_in_context(
            &sql,
            current_catalog.as_deref(),
            &current_db,
            Some(query_options),
        )
    });

    let result = match query_timeout {
        Some(secs) => {
            match tokio::time::timeout(std::time::Duration::from_secs(secs), join_handle).await {
                Ok(join_result) => join_result,
                Err(_elapsed) => {
                    // The blocking task continues to run in the background
                    // (tokio cannot cancel spawn_blocking work). The client
                    // sees an error and may disconnect; the worker thread
                    // will finish on its own and its result is discarded.
                    return Err((
                        ErrorKind::ER_QUERY_INTERRUPTED,
                        format!("Query exceeded timeout of {secs}s"),
                    ));
                }
            }
        }
        None => join_handle.await,
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
    fn delete_is_dispatched_to_embedded_engine_not_session_noop() {
        let sql = "DELETE FROM ice.ns.orders WHERE id = 1";
        assert!(
            !is_session_noop(sql),
            "DELETE must reach the embedded engine so Iceberg row deletes are committed"
        );
        assert!(is_supported_embedded_statement(sql));
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
}
