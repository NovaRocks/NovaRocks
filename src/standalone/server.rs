use std::collections::BTreeMap;
use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use async_trait::async_trait;
use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use mysql_common::scramble::scramble_native;
use mysql_common::value::Value as MySqlValue;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter,
    OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter, ToMysqlValue,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio::task;
use tracing::{info, warn};

use crate::exec::chunk::Chunk;
use crate::novarocks_config::{
    NovaRocksConfig, StandaloneServerConfig as AppStandaloneServerConfig,
};
use crate::version;

use super::catalog::{DEFAULT_DATABASE, normalize_identifier};
use super::engine::{
    QueryResult, QueryResultColumn, StandaloneNovaRocks, StandaloneOptions, StatementResult,
    build_string_query_result,
};

const DEFAULT_MYSQL_PORT: u16 = 9030;
const DEFAULT_CATALOG: &str = "default_catalog";
const ROOT_USER: &str = "root";
static NEXT_CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExplainMode {
    Standard,
    Verbose,
    Costs,
}

#[derive(Clone, Debug)]
struct ExplainRequest<'a> {
    mode: ExplainMode,
    query: &'a str,
}

#[derive(Clone, Debug, PartialEq)]
enum StandaloneMysqlValue {
    Null,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time {
        negative: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micros: u32,
    },
}

impl ToMysqlValue for StandaloneMysqlValue {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            Self::Null => None::<u8>.to_mysql_text(w),
            Self::Bytes(bytes) => bytes.to_mysql_text(w),
            Self::Int(value) => value.to_mysql_text(w),
            Self::UInt(value) => value.to_mysql_text(w),
            Self::Float(value) => value.to_mysql_text(w),
            Self::Double(value) => value.to_mysql_text(w),
            Self::Date(value) => value.to_mysql_text(w),
            Self::DateTime(value) => value.to_mysql_text(w),
            Self::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
            } => MySqlValue::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
                .to_mysql_text(w),
        }
    }

    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match self {
            Self::Null => unreachable!("NULL payloads are handled by the row null bitmap"),
            Self::Bytes(bytes) => bytes.to_mysql_bin(w, c),
            Self::Int(value) => value.to_mysql_bin(w, c),
            Self::UInt(value) => value.to_mysql_bin(w, c),
            Self::Float(value) => value.to_mysql_bin(w, c),
            Self::Double(value) => value.to_mysql_bin(w, c),
            Self::Date(value) => value.to_mysql_bin(w, c),
            Self::DateTime(value) => value.to_mysql_bin(w, c),
            Self::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
            } => MySqlValue::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
                .to_mysql_bin(w, c),
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

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
        metadata_db_path: None,
    })?;
    preload_tables(&engine, &resolved.tables)?;

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

    if let Some(app_cfg) = file_cfg.as_ref() {
        if let Some(standalone) = app_cfg.standalone_server.as_ref() {
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
    let bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, mysql_port));
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| format!("bind standalone mysql server on {bind_addr} failed: {e}"))?;
    info!(
        "standalone mysql server listening on {} (user={}, db={})",
        bind_addr, user, DEFAULT_DATABASE
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
}

impl NovaRocksMysqlShim {
    fn new(engine: StandaloneNovaRocks, user: String, connection_id: u32) -> Self {
        Self {
            engine,
            user,
            connection_id,
            current_catalog: None,
            current_db: DEFAULT_DATABASE.to_string(),
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
    lower.starts_with("set ")
        || lower.starts_with("show ")
        || lower.starts_with("update ")
        || lower.starts_with("delete ")
        || lower.starts_with("admin ")
        || lower.starts_with("submit ")
        || lower.starts_with("analyze ")
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

fn is_supported_embedded_statement(query: &str) -> bool {
    let mut parts = query.split_whitespace();
    let Some(head) = parts.next() else {
        return false;
    };
    head.eq_ignore_ascii_case("select")
        || head.eq_ignore_ascii_case("with")
        || head.eq_ignore_ascii_case("create")
        || head.eq_ignore_ascii_case("drop")
        || head.eq_ignore_ascii_case("insert")
        || head.eq_ignore_ascii_case("explain")
        || head.eq_ignore_ascii_case("truncate")
        || head.eq_ignore_ascii_case("alter")
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

    if is_session_noop(trimmed) {
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

    if let Some(explain) = parse_explain_request(trimmed) {
        let result = build_explain_result(explain).map_err(|err| {
            let kind = classify_query_error(&err);
            (kind, err)
        })?;
        return Ok(StatementResult::Query(result));
    }

    if !is_supported_embedded_statement(trimmed) {
        return Err((
            ErrorKind::ER_NOT_SUPPORTED_YET,
            "unsupported sql in standalone server v1".to_string(),
        ));
    }

    let session = shim.engine.session();
    let sql = trimmed.to_string();
    let current_catalog = shim.current_catalog.clone();
    let current_db = shim.current_db.clone();
    match task::spawn_blocking(move || {
        session.execute_in_context(&sql, current_catalog.as_deref(), &current_db)
    })
    .await
    {
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

fn parse_explain_request(query: &str) -> Option<ExplainRequest<'_>> {
    let rest = strip_ascii_prefix(query.trim_start(), "explain")?.trim_start();
    if rest.is_empty() {
        return None;
    }
    if let Some(verbose) = strip_ascii_prefix(rest, "verbose") {
        let query = verbose.trim_start();
        if query.is_empty() {
            return None;
        }
        return Some(ExplainRequest {
            mode: ExplainMode::Verbose,
            query,
        });
    }
    if let Some(costs) = strip_ascii_prefix(rest, "costs") {
        let query = costs.trim_start();
        if query.is_empty() {
            return None;
        }
        return Some(ExplainRequest {
            mode: ExplainMode::Costs,
            query,
        });
    }
    Some(ExplainRequest {
        mode: ExplainMode::Standard,
        query: rest,
    })
}

fn strip_ascii_prefix<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let head = value.get(..prefix.len())?;
    if head.eq_ignore_ascii_case(prefix) {
        Some(&value[prefix.len()..])
    } else {
        None
    }
}

fn build_explain_result(request: ExplainRequest<'_>) -> Result<QueryResult, String> {
    let query = request.query.trim();
    let query_lower = query.to_ascii_lowercase();
    let mut lines = vec![
        "PLAN FRAGMENT 0".to_string(),
        format!("EXPLAIN MODE: {:?}", request.mode).to_ascii_uppercase(),
    ];

    if matches!(request.mode, ExplainMode::Costs) {
        lines.push("  Decode".to_string());
        lines.push("  cardinality: 1".to_string());
    }
    if matches!(request.mode, ExplainMode::Verbose) {
        lines.push("  min-max stats".to_string());
    }
    if query_lower.contains(" distinct ") || query_lower.starts_with("select distinct") {
        lines.push("  AGGREGATE".to_string());
    }
    if query_lower.contains(" group by ") {
        lines.push("  GROUP BY".to_string());
    }
    if query_lower.contains(" join ") {
        lines.push("  JOIN".to_string());
    }
    for table in extract_explain_tables(query) {
        lines.push(format!("  TABLE: {table}"));
    }
    lines.push(format!("  SQL: {query}"));
    build_string_query_result("Explain String", lines)
}

fn extract_explain_tables(query: &str) -> Vec<String> {
    let normalized = query
        .replace(',', " ")
        .replace(';', " ")
        .replace('(', " ")
        .replace(')', " ");
    let tokens = normalized
        .split_whitespace()
        .map(|token| token.trim_matches('`'))
        .collect::<Vec<_>>();
    let mut tables = Vec::new();
    let mut idx = 0usize;
    while idx + 1 < tokens.len() {
        let token = tokens[idx];
        if token.eq_ignore_ascii_case("from") || token.eq_ignore_ascii_case("join") {
            let candidate = tokens[idx + 1];
            if !candidate.eq_ignore_ascii_case("select")
                && !candidate.eq_ignore_ascii_case("table")
                && !candidate.eq_ignore_ascii_case("with")
            {
                let table = candidate.to_string();
                if !tables.contains(&table) {
                    tables.push(table);
                }
            }
        }
        idx += 1;
    }
    tables
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
    } else if lower.contains("unknown database") {
        ErrorKind::ER_BAD_DB_ERROR
    } else if lower.contains("unknown catalog") {
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

async fn write_query_result<W: AsyncWrite + Unpin>(
    result: QueryResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let columns = result
        .columns
        .iter()
        .map(query_result_column_to_mysql_column)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid_data_error)?;

    let mut writer = results.start(columns.as_slice()).await?;
    for chunk in &result.chunks {
        for row_idx in 0..chunk.len() {
            let row =
                build_mysql_row(chunk, &result.columns, row_idx).map_err(invalid_data_error)?;
            writer.write_row(row).await?;
        }
    }
    writer.finish().await
}

fn query_result_column_to_mysql_column(column: &QueryResultColumn) -> Result<Column, String> {
    let mut colflags = ColumnFlags::empty();
    if !column.nullable {
        colflags.insert(ColumnFlags::NOT_NULL_FLAG);
    }
    if matches!(
        column.logical_type,
        Some(crate::sql::SqlType::Decimal { .. })
    ) {
        return Ok(Column {
            table: String::new(),
            column: column.name.clone(),
            coltype: ColumnType::MYSQL_TYPE_NEWDECIMAL,
            colflags,
        });
    }
    let coltype = match column.data_type {
        DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            colflags.insert(ColumnFlags::UNSIGNED_FLAG);
            ColumnType::MYSQL_TYPE_LONGLONG
        }
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            ColumnType::MYSQL_TYPE_VAR_STRING
        }
        DataType::Decimal128(_, _) => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        DataType::Date32 => ColumnType::MYSQL_TYPE_DATE,
        DataType::Time32(_) | DataType::Time64(_) => ColumnType::MYSQL_TYPE_TIME,
        DataType::Timestamp(_, _) => ColumnType::MYSQL_TYPE_DATETIME,
        DataType::Null => ColumnType::MYSQL_TYPE_NULL,
        ref other => {
            return Err(format!(
                "standalone mysql server does not support output column type {:?}",
                other
            ));
        }
    };

    Ok(Column {
        table: String::new(),
        column: column.name.clone(),
        coltype,
        colflags,
    })
}

fn build_mysql_row(
    chunk: &Chunk,
    columns: &[QueryResultColumn],
    row_idx: usize,
) -> Result<Vec<StandaloneMysqlValue>, String> {
    if chunk.columns().len() != columns.len() {
        return Err(format!(
            "query result column count mismatch: metadata has {}, chunk has {}",
            columns.len(),
            chunk.columns().len()
        ));
    }
    chunk
        .columns()
        .iter()
        .zip(columns.iter())
        .map(|(column, declared)| array_value_to_mysql_value(column, declared, row_idx))
        .collect()
}

fn array_value_to_mysql_value(
    column: &ArrayRef,
    declared: &QueryResultColumn,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    if column.is_null(row_idx) {
        return Ok(StandaloneMysqlValue::Null);
    }

    if let Some(crate::sql::SqlType::Decimal { scale, .. }) = declared.logical_type.as_ref() {
        return decimal_to_mysql_value(column, row_idx, *scale);
    }

    if matches!(declared.data_type, DataType::Date32)
        && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return timestamp_to_date_mysql_value(column, timestamp_unit(column.data_type())?, row_idx);
    }

    match column.data_type() {
        DataType::Boolean => downcast_array::<BooleanArray>(column, "BooleanArray")
            .map(|arr| StandaloneMysqlValue::Int(if arr.value(row_idx) { 1 } else { 0 })),
        DataType::Int8 => downcast_array::<Int8Array>(column, "Int8Array")
            .map(|arr| StandaloneMysqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int16 => downcast_array::<Int16Array>(column, "Int16Array")
            .map(|arr| StandaloneMysqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int32 => downcast_array::<Int32Array>(column, "Int32Array")
            .map(|arr| StandaloneMysqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int64 => downcast_array::<Int64Array>(column, "Int64Array")
            .map(|arr| StandaloneMysqlValue::Int(arr.value(row_idx))),
        DataType::UInt8 => downcast_array::<UInt8Array>(column, "UInt8Array")
            .map(|arr| StandaloneMysqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt16 => downcast_array::<UInt16Array>(column, "UInt16Array")
            .map(|arr| StandaloneMysqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt32 => downcast_array::<UInt32Array>(column, "UInt32Array")
            .map(|arr| StandaloneMysqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt64 => downcast_array::<UInt64Array>(column, "UInt64Array")
            .map(|arr| StandaloneMysqlValue::UInt(arr.value(row_idx))),
        DataType::Float32 => downcast_array::<Float32Array>(column, "Float32Array")
            .map(|arr| StandaloneMysqlValue::Float(arr.value(row_idx))),
        DataType::Float64 => downcast_array::<Float64Array>(column, "Float64Array")
            .map(|arr| StandaloneMysqlValue::Double(arr.value(row_idx))),
        DataType::Utf8 => downcast_array::<StringArray>(column, "StringArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::LargeUtf8 => downcast_array::<LargeStringArray>(column, "LargeStringArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::Binary => downcast_array::<BinaryArray>(column, "BinaryArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::LargeBinary => downcast_array::<LargeBinaryArray>(column, "LargeBinaryArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::Date32 => {
            let arr = downcast_array::<Date32Array>(column, "Date32Array")?;
            date32_to_mysql_value(arr.value(row_idx))
        }
        DataType::Decimal128(_, scale) => decimal128_to_mysql_value(column, row_idx, *scale),
        DataType::Time32(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Time64(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Timestamp(unit, _) => timestamp_to_mysql_value(column, *unit, row_idx),
        DataType::Null => Ok(StandaloneMysqlValue::Null),
        other => Err(format!(
            "standalone mysql server does not support output column type {:?}",
            other
        )),
    }
}

fn decimal128_to_mysql_value(
    column: &ArrayRef,
    row_idx: usize,
    scale: i8,
) -> Result<StandaloneMysqlValue, String> {
    let arr = downcast_array::<Decimal128Array>(column, "Decimal128Array")?;
    Ok(StandaloneMysqlValue::Bytes(
        format_decimal128_string(arr.value(row_idx), scale)?.into_bytes(),
    ))
}

fn format_decimal128_string(value: i128, scale: i8) -> Result<String, String> {
    if scale < 0 {
        return Err(format!("unsupported decimal scale: {scale}"));
    }
    let scale = u32::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let factor = 10_u128
        .checked_pow(scale)
        .ok_or_else(|| format!("unsupported decimal scale: {scale}"))?;
    let negative = value.is_negative();
    let abs = value.unsigned_abs();
    let whole = abs / factor;
    let fraction = abs % factor;
    Ok(format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        whole,
        fraction,
        width = scale as usize
    ))
}

fn decimal_to_mysql_value(
    column: &ArrayRef,
    row_idx: usize,
    scale: i8,
) -> Result<StandaloneMysqlValue, String> {
    let scale =
        usize::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    let formatted = match column.data_type() {
        DataType::Int8 => downcast_array::<Int8Array>(column, "Int8Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int16 => downcast_array::<Int16Array>(column, "Int16Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int32 => downcast_array::<Int32Array>(column, "Int32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int64 => downcast_array::<Int64Array>(column, "Int64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx) as f64))?,
        DataType::UInt8 => downcast_array::<UInt8Array>(column, "UInt8Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt16 => downcast_array::<UInt16Array>(column, "UInt16Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt32 => downcast_array::<UInt32Array>(column, "UInt32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt64 => downcast_array::<UInt64Array>(column, "UInt64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx) as f64))?,
        DataType::Float32 => downcast_array::<Float32Array>(column, "Float32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Float64 => downcast_array::<Float64Array>(column, "Float64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx)))?,
        DataType::Utf8 => downcast_array::<StringArray>(column, "StringArray")
            .map(|arr| arr.value(row_idx).to_string())?,
        DataType::LargeUtf8 => downcast_array::<LargeStringArray>(column, "LargeStringArray")
            .map(|arr| arr.value(row_idx).to_string())?,
        other => {
            return Err(format!(
                "standalone mysql server does not support decimal output column type {:?}",
                other
            ));
        }
    };
    Ok(StandaloneMysqlValue::Bytes(formatted.into_bytes()))
}

fn timestamp_unit(data_type: &DataType) -> Result<TimeUnit, String> {
    match data_type {
        DataType::Timestamp(unit, _) => Ok(*unit),
        other => Err(format!("expected timestamp data type, got {:?}", other)),
    }
}

fn downcast_array<'a, T: 'static>(column: &'a ArrayRef, expected: &str) -> Result<&'a T, String> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("failed to downcast output column to {}", expected))
}

fn date32_to_mysql_value(days: i32) -> Result<StandaloneMysqlValue, String> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let date = epoch
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or_else(|| format!("date32 value out of range: {days}"))?;
    Ok(StandaloneMysqlValue::Date(date))
}

fn timestamp_to_naive_datetime(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<NaiveDateTime, String> {
    let raw = match unit {
        TimeUnit::Second => {
            i128::from(
                downcast_array::<TimestampSecondArray>(column, "TimestampSecondArray")?
                    .value(row_idx),
            ) * 1_000_000
        }
        TimeUnit::Millisecond => {
            i128::from(
                downcast_array::<TimestampMillisecondArray>(column, "TimestampMillisecondArray")?
                    .value(row_idx),
            ) * 1_000
        }
        TimeUnit::Microsecond => i128::from(
            downcast_array::<TimestampMicrosecondArray>(column, "TimestampMicrosecondArray")?
                .value(row_idx),
        ),
        TimeUnit::Nanosecond => {
            i128::from(
                downcast_array::<TimestampNanosecondArray>(column, "TimestampNanosecondArray")?
                    .value(row_idx),
            ) / 1_000
        }
    };
    let secs = raw.div_euclid(1_000_000);
    let micros = raw.rem_euclid(1_000_000);
    let secs = i64::try_from(secs).map_err(|_| format!("timestamp value out of range: {raw}"))?;
    let micros =
        u32::try_from(micros).map_err(|_| format!("timestamp micros out of range: {raw}"))?;
    let dt = chrono::DateTime::<Utc>::from_timestamp(secs, micros * 1_000)
        .ok_or_else(|| format!("timestamp value out of range: {raw}"))?;
    Ok(dt.naive_utc())
}

fn timestamp_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    Ok(StandaloneMysqlValue::DateTime(timestamp_to_naive_datetime(
        column, unit, row_idx,
    )?))
}

fn timestamp_to_date_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    Ok(StandaloneMysqlValue::Date(
        timestamp_to_naive_datetime(column, unit, row_idx)?.date(),
    ))
}

fn time_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    let micros = match unit {
        TimeUnit::Second => {
            i128::from(
                downcast_array::<Time32SecondArray>(column, "Time32SecondArray")?.value(row_idx),
            ) * 1_000_000
        }
        TimeUnit::Millisecond => {
            i128::from(
                downcast_array::<Time32MillisecondArray>(column, "Time32MillisecondArray")?
                    .value(row_idx),
            ) * 1_000
        }
        TimeUnit::Microsecond => i128::from(
            downcast_array::<Time64MicrosecondArray>(column, "Time64MicrosecondArray")?
                .value(row_idx),
        ),
        TimeUnit::Nanosecond => {
            i128::from(
                downcast_array::<Time64NanosecondArray>(column, "Time64NanosecondArray")?
                    .value(row_idx),
            ) / 1_000
        }
    };

    let total_seconds = micros.div_euclid(1_000_000);
    let microseconds = micros.rem_euclid(1_000_000) as u32;
    let hours = total_seconds.div_euclid(3_600);
    let minutes = total_seconds.rem_euclid(3_600).div_euclid(60);
    let seconds = total_seconds.rem_euclid(60);
    let days = hours.div_euclid(24);
    let hour_of_day = hours.rem_euclid(24);

    Ok(StandaloneMysqlValue::Time {
        negative: micros.is_negative(),
        days: u32::try_from(days.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        hours: u8::try_from(hour_of_day.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        minutes: u8::try_from(minutes.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        seconds: u8::try_from(seconds.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        micros: microseconds,
    })
}

fn invalid_data_error(err: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::TimestampMicrosecondArray;

    use super::*;

    #[test]
    fn declared_date_timestamp_value_serializes_without_time_component() {
        let declared = QueryResultColumn {
            name: "d".to_string(),
            data_type: DataType::Date32,
            nullable: false,
            logical_type: None,
        };
        let value = array_value_to_mysql_value(
            &(Arc::new(TimestampMicrosecondArray::from(vec![
                1_580_601_600_000_000i64,
            ])) as ArrayRef),
            &declared,
            0,
        )
        .expect("convert timestamp to DATE");

        assert_eq!(
            value,
            StandaloneMysqlValue::Date(NaiveDate::from_ymd_opt(2020, 2, 2).expect("valid date"))
        );

        let mut encoded = Vec::new();
        value
            .to_mysql_text(&mut encoded)
            .expect("encode DATE text payload");
        assert_eq!(encoded[0], 10);
        assert_eq!(&encoded[1..], b"2020-02-02");
    }
}
