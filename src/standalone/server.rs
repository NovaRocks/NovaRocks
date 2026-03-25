use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use async_trait::async_trait;
use chrono::{Datelike, Duration, NaiveDate, Timelike, Utc};
use mysql_common::scramble::scramble_native;
use mysql_common::value::Value as MySqlValue;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter,
    OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter,
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
};

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
    lower.starts_with("set ")
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
        || head.eq_ignore_ascii_case("create")
        || head.eq_ignore_ascii_case("drop")
        || head.eq_ignore_ascii_case("insert")
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
    raw.split('.')
        .map(str::trim)
        .map(strip_identifier_quotes)
        .map(|part| {
            if part.is_empty() {
                Err(format!("unsupported identifier `{raw}`"))
            } else {
                Ok(part)
            }
        })
        .collect()
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
            let row = build_mysql_row(chunk, row_idx).map_err(invalid_data_error)?;
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

fn build_mysql_row(chunk: &Chunk, row_idx: usize) -> Result<Vec<MySqlValue>, String> {
    chunk
        .columns()
        .iter()
        .map(|column| array_value_to_mysql_value(column, row_idx))
        .collect()
}

fn array_value_to_mysql_value(column: &ArrayRef, row_idx: usize) -> Result<MySqlValue, String> {
    if column.is_null(row_idx) {
        return Ok(MySqlValue::NULL);
    }

    match column.data_type() {
        DataType::Boolean => downcast_array::<BooleanArray>(column, "BooleanArray")
            .map(|arr| MySqlValue::Int(if arr.value(row_idx) { 1 } else { 0 })),
        DataType::Int8 => downcast_array::<Int8Array>(column, "Int8Array")
            .map(|arr| MySqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int16 => downcast_array::<Int16Array>(column, "Int16Array")
            .map(|arr| MySqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int32 => downcast_array::<Int32Array>(column, "Int32Array")
            .map(|arr| MySqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int64 => downcast_array::<Int64Array>(column, "Int64Array")
            .map(|arr| MySqlValue::Int(arr.value(row_idx))),
        DataType::UInt8 => downcast_array::<UInt8Array>(column, "UInt8Array")
            .map(|arr| MySqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt16 => downcast_array::<UInt16Array>(column, "UInt16Array")
            .map(|arr| MySqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt32 => downcast_array::<UInt32Array>(column, "UInt32Array")
            .map(|arr| MySqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt64 => downcast_array::<UInt64Array>(column, "UInt64Array")
            .map(|arr| MySqlValue::UInt(arr.value(row_idx))),
        DataType::Float32 => downcast_array::<Float32Array>(column, "Float32Array")
            .map(|arr| MySqlValue::Float(arr.value(row_idx))),
        DataType::Float64 => downcast_array::<Float64Array>(column, "Float64Array")
            .map(|arr| MySqlValue::Double(arr.value(row_idx))),
        DataType::Utf8 => downcast_array::<StringArray>(column, "StringArray")
            .map(|arr| MySqlValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::LargeUtf8 => downcast_array::<LargeStringArray>(column, "LargeStringArray")
            .map(|arr| MySqlValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::Binary => downcast_array::<BinaryArray>(column, "BinaryArray")
            .map(|arr| MySqlValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::LargeBinary => downcast_array::<LargeBinaryArray>(column, "LargeBinaryArray")
            .map(|arr| MySqlValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::Date32 => {
            let arr = downcast_array::<Date32Array>(column, "Date32Array")?;
            date32_to_mysql_value(arr.value(row_idx))
        }
        DataType::Time32(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Time64(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Timestamp(unit, _) => timestamp_to_mysql_value(column, *unit, row_idx),
        DataType::Null => Ok(MySqlValue::NULL),
        other => Err(format!(
            "standalone mysql server does not support output column type {:?}",
            other
        )),
    }
}

fn downcast_array<'a, T: 'static>(column: &'a ArrayRef, expected: &str) -> Result<&'a T, String> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("failed to downcast output column to {}", expected))
}

fn date32_to_mysql_value(days: i32) -> Result<MySqlValue, String> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let date = epoch
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or_else(|| format!("date32 value out of range: {days}"))?;
    Ok(MySqlValue::Date(
        date.year() as u16,
        date.month() as u8,
        date.day() as u8,
        0,
        0,
        0,
        0,
    ))
}

fn timestamp_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<MySqlValue, String> {
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
    let naive = dt.naive_utc();
    Ok(MySqlValue::Date(
        naive.year() as u16,
        naive.month() as u8,
        naive.day() as u8,
        naive.hour() as u8,
        naive.minute() as u8,
        naive.second() as u8,
        naive.and_utc().timestamp_subsec_micros(),
    ))
}

fn time_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<MySqlValue, String> {
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

    Ok(MySqlValue::Time(
        micros.is_negative(),
        u32::try_from(days.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        u8::try_from(hour_of_day.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        u8::try_from(minutes.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        u8::try_from(seconds.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        microseconds,
    ))
}

fn invalid_data_error(err: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}
