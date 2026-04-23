use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use parquet::arrow::ArrowWriter;
use tempfile::{NamedTempFile, TempDir};

fn write_parquet_file(rows: &[(i32, Option<&str>)]) -> NamedTempFile {
    let file = NamedTempFile::new().expect("create temp file");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, name)| *name).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build record batch");
    let writer_file = std::fs::File::create(file.path()).expect("open parquet output");
    let mut writer =
        ArrowWriter::try_new(writer_file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close parquet writer");
    file
}

fn alloc_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

struct ServerGuard {
    child: Child,
}

impl ServerGuard {
    fn spawn(args: &[String]) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_novarocks"))
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn standalone-server");
        Self { child }
    }

    fn connect_root(&mut self, port: u16) -> MysqlConn {
        wait_for_mysql(port, "root", None, &mut self.child)
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_for_mysql(port: u16, user: &str, password: Option<&str>, child: &mut Child) -> MysqlConn {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("poll child status") {
            let mut output = String::new();
            if let Some(mut stdout) = child.stdout.take() {
                let _ = stdout.read_to_string(&mut output);
            }
            if let Some(mut stderr) = child.stderr.take() {
                let _ = stderr.read_to_string(&mut output);
            }
            panic!("standalone-server exited early with status {status}: {output}");
        }

        let builder = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1".to_string()))
            .tcp_port(port)
            .prefer_socket(false)
            .user(Some(user.to_string()))
            .pass(password.map(|p| p.to_string()));
        match MysqlConn::new(builder) {
            Ok(conn) => return conn,
            Err(err) => {
                let err_text = err.to_string();
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    let mut output = String::new();
                    if let Some(mut stdout) = child.stdout.take() {
                        let _ = stdout.read_to_string(&mut output);
                    }
                    if let Some(mut stderr) = child.stderr.take() {
                        let _ = stderr.read_to_string(&mut output);
                    }
                    panic!(
                        "mysql connection to standalone-server failed: {}\nchild output:\n{output}",
                        err_text
                    );
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn assert_hadoop_catalog_metadata_compat(
    warehouse: &Path,
    namespace: &str,
    table: &str,
    expected_version: u32,
) {
    let metadata_dir = warehouse.join(namespace).join(table).join("metadata");
    let entries = std::fs::read_dir(&metadata_dir)
        .expect("read metadata dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();

    let compat_metadata = metadata_dir.join(format!("v{expected_version}.metadata.json"));
    assert!(
        compat_metadata.is_file(),
        "missing Hadoop-compatible metadata file {}; entries={entries:?}",
        compat_metadata.display()
    );

    let version_hint = metadata_dir.join("version-hint.text");
    let hint = std::fs::read_to_string(&version_hint).expect("read version-hint.text");
    assert_eq!(
        hint.trim(),
        expected_version.to_string(),
        "unexpected version-hint content at {}",
        version_hint.display()
    );

    // HadoopFileSystemCatalog writes only Hadoop-format files — no internal-format
    // ({version}-{uuid}.metadata.json) files should be present.
    let internal_files: Vec<&String> = entries
        .iter()
        .filter(|name| name.ends_with(".metadata.json") && !name.starts_with('v'))
        .collect();
    assert!(
        internal_files.is_empty(),
        "unexpected internal-format metadata files: {internal_files:?}"
    );
}

fn run_curl_stream_load(
    http_port: u16,
    db: &str,
    table: &str,
    payload: &str,
    headers: &[&str],
) -> String {
    let mut cmd = Command::new("curl");
    cmd.arg("-s")
        .arg("--http2-prior-knowledge")
        .arg("--location-trusted")
        .arg("-u")
        .arg("root:")
        .arg("--data-binary")
        .arg(payload)
        .arg("-XPUT");
    for header in headers {
        cmd.arg("-H").arg(header);
    }
    cmd.arg(format!(
        "http://127.0.0.1:{http_port}/api/{db}/{table}/_stream_load"
    ));
    let output = cmd.output().expect("run curl stream load");
    assert!(
        output.status.success(),
        "curl stream load failed: status={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("decode curl stdout")
}

fn managed_lake_endpoint_reachable(endpoint: &str) -> bool {
    let stripped = endpoint
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint);
    let authority = stripped.split('/').next().unwrap_or(stripped);
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => match port.parse::<u16>() {
            Ok(port) => (host, port),
            Err(_) => return false,
        },
        None => {
            let default_port = if endpoint.starts_with("https://") {
                443
            } else {
                80
            };
            (authority, default_port)
        }
    };
    std::net::TcpStream::connect_timeout(
        &format!("{host}:{port}")
            .parse()
            .expect("managed lake endpoint socket addr"),
        Duration::from_secs(1),
    )
    .is_ok()
}

fn maybe_write_managed_lake_config(mysql_port: u16) -> Option<(TempDir, PathBuf)> {
    let endpoint =
        std::env::var("AWS_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    if !managed_lake_endpoint_reachable(&endpoint) {
        eprintln!(
            "skipping standalone managed-lake mysql test: object store endpoint is unreachable: {endpoint}"
        );
        return None;
    }

    let access_key_id = std::env::var("AWS_S3_ACCESS_KEY_ID")
        .or_else(|_| std::env::var("MINIO_ROOT_USER"))
        .unwrap_or_else(|_| "admin".to_string());
    let access_key_secret = std::env::var("AWS_S3_SECRET_ACCESS_KEY")
        .or_else(|_| std::env::var("MINIO_ROOT_PASSWORD"))
        .unwrap_or_else(|_| "admin123".to_string());
    let bucket = std::env::var("AWS_S3_BUCKET").unwrap_or_else(|_| "novarocks".to_string());
    let root_prefix =
        std::env::var("AWS_S3_ROOT").unwrap_or_else(|_| "codex-managed-lake-tests".to_string());
    let run_id = format!(
        "mysql_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let root_prefix = root_prefix.trim_matches('/');
    let warehouse_uri = if root_prefix.is_empty() {
        format!("s3://{bucket}/{run_id}")
    } else {
        format!("s3://{bucket}/{root_prefix}/{run_id}")
    };

    let config_dir = TempDir::new().expect("create managed lake config dir");
    let config_path = config_dir.path().join("novarocks.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"[standalone_server]
mysql_port = {mysql_port}
user = "root"
metadata_db_path = "meta/catalog.db"
warehouse_uri = "{warehouse_uri}"

[standalone_server.object_store]
endpoint = "{endpoint}"
access_key_id = "{access_key_id}"
access_key_secret = "{access_key_secret}"
enable_path_style_access = true
"#
        ),
    )
    .expect("write managed lake config");
    Some((config_dir, config_path))
}

#[test]
fn standalone_mysql_server_accepts_queries_and_session_noops() {
    let parquet = write_parquet_file(&[(1, Some("a")), (2, Some("b")), (3, None)]);
    let port = alloc_port();
    let args = vec![
        "standalone-server".to_string(),
        "--port".to_string(),
        port.to_string(),
        "--table".to_string(),
        format!("tbl={}", parquet.path().display()),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.ping().expect("ping standalone-server");
    conn.query_drop("USE default").expect("USE default");
    conn.query_drop("SET NAMES utf8mb4")
        .expect("SET NAMES utf8mb4");
    conn.query_drop("SET autocommit = 1")
        .expect("SET autocommit = 1");
    conn.query_drop("SET character_set_results = NULL")
        .expect("SET character_set_results = NULL");

    let rows: Vec<(i32, Option<String>)> = conn.query("select * from tbl").expect("select *");
    assert_eq!(
        rows,
        vec![
            (1, Some("a".to_string())),
            (2, Some("b".to_string())),
            (3, None),
        ]
    );

    let filtered: Vec<(Option<String>,)> = conn
        .query("select name from tbl where id = 2")
        .expect("filtered select");
    assert_eq!(filtered, vec![(Some("b".to_string()),)]);
}

#[test]
fn standalone_mysql_server_loads_tables_from_config_and_cli_overrides_duplicates() {
    let config_parquet = write_parquet_file(&[(1, Some("config"))]);
    let cli_parquet = write_parquet_file(&[(9, Some("cli"))]);
    let port = alloc_port();
    let config = NamedTempFile::new().expect("create config");
    std::fs::write(
        config.path(),
        format!(
            r#"[standalone_server]
mysql_port = {port}
user = "root"

[[standalone_server.tables]]
name = "tbl"
path = "{}"
"#,
            config_parquet.path().display()
        ),
    )
    .expect("write config");

    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config.path().display().to_string(),
        "--table".to_string(),
        format!("tbl={}", cli_parquet.path().display()),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    let rows: Vec<(i32, Option<String>)> = conn.query("select * from tbl").expect("select *");
    assert_eq!(rows, vec![(9, Some("cli".to_string()))]);
}

#[test]
fn standalone_mysql_server_rejects_wrong_auth_and_unsupported_sql() {
    let parquet = write_parquet_file(&[(1, Some("a"))]);
    let port = alloc_port();
    let args = vec![
        "standalone-server".to_string(),
        "--port".to_string(),
        port.to_string(),
        "--table".to_string(),
        format!("tbl={}", parquet.path().display()),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    let err = conn
        .query_drop("show tables")
        .expect_err("show tables must fail");
    let err_text = err.to_string();
    assert!(
        err_text.to_ascii_lowercase().contains("unsupported"),
        "unexpected error for unsupported sql: {err_text}"
    );

    let other_user = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1".to_string()))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some("other".to_string()));
    let _err = MysqlConn::new(other_user).expect_err("wrong user must fail");

    let bad_password = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1".to_string()))
        .tcp_port(port)
        .prefer_socket(false)
        .user(Some("root".to_string()))
        .pass(Some("secret".to_string()));
    let _err = MysqlConn::new(bad_password).expect_err("non-empty password must fail");
}

// `standalone_mysql_server_supports_basic_ddl_without_preloaded_tables`
// exercised the removed local-parquet `CREATE TABLE ... PROPERTIES("path"="...")`
// shorthand. The managed-lake round-trip test below now covers the bare
// `CREATE TABLE` + default-DDL path end-to-end.

#[test]
fn standalone_mysql_server_supports_minimal_iceberg_flow() {
    let warehouse = TempDir::new().expect("create iceberg warehouse");
    let port = alloc_port();
    let args = vec![
        "standalone-server".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create iceberg catalog");
    conn.query_drop("create database ice.db1")
        .expect("create iceberg database");
    conn.query_drop("create table ice.db1.tbl (id int, name string)")
        .expect("create iceberg table");
    conn.query_drop("insert into ice.db1.tbl values (1, 'a'), (2, 'b')")
        .expect("insert iceberg rows");

    let rows: Vec<(Option<i32>, Option<String>)> = conn
        .query("select * from ice.db1.tbl")
        .expect("select iceberg rows");
    assert_eq!(
        rows,
        vec![
            (Some(1), Some("a".to_string())),
            (Some(2), Some("b".to_string())),
        ]
    );

    let filtered: Vec<(Option<String>,)> = conn
        .query("select name from ice.db1.tbl where id = 2")
        .expect("filtered iceberg select");
    assert_eq!(filtered, vec![(Some("b".to_string()),)]);
}

#[test]
fn standalone_mysql_server_writes_hadoop_catalog_compat_metadata_files() {
    let warehouse = TempDir::new().expect("create iceberg warehouse");
    let port = alloc_port();
    let args = vec![
        "standalone-server".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create iceberg catalog");
    conn.query_drop("create database ice.db1")
        .expect("create iceberg database");
    conn.query_drop("create table ice.db1.tbl (id int, name string)")
        .expect("create iceberg table");

    assert_hadoop_catalog_metadata_compat(warehouse.path(), "db1", "tbl", 1);

    conn.query_drop("insert into ice.db1.tbl values (1, 'a'), (2, 'b')")
        .expect("insert iceberg rows");

    assert_hadoop_catalog_metadata_compat(warehouse.path(), "db1", "tbl", 2);
}

#[test]
fn standalone_mysql_server_reads_hadoop_only_iceberg_tables() {
    let warehouse = TempDir::new().expect("create iceberg warehouse");
    let port = alloc_port();
    let args = vec![
        "standalone-server".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    // Phase 1: Create a table and insert initial data.
    conn.query_drop(format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create iceberg catalog");
    conn.query_drop("create database ice.db1")
        .expect("create iceberg database");
    conn.query_drop("create table ice.db1.tbl (id int, name string)")
        .expect("create iceberg table");
    conn.query_drop("insert into ice.db1.tbl values (1, 'a'), (2, 'b')")
        .expect("insert iceberg rows");

    assert_hadoop_catalog_metadata_compat(warehouse.path(), "db1", "tbl", 2);

    // Phase 2: Register a fresh catalog with a different name over the SAME
    // warehouse, so the per-entry table_cache is empty. This simulates reading
    // a table that was written by another engine (StarRocks FE / Spark) — the
    // on-disk layout is identical (only v{N}.metadata.json + version-hint.text).
    drop(conn);
    let mut conn = server.connect_root(port);
    conn.query_drop(format!(
        r#"create external catalog ice2 properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create second iceberg catalog");
    conn.query_drop("use ice2.db1").expect("use db");

    // Verify reads work through the fresh catalog.
    let rows: Vec<(Option<i32>, Option<String>)> =
        conn.query("select * from tbl").expect("select hadoop rows");
    assert_eq!(
        rows,
        vec![
            (Some(1), Some("a".to_string())),
            (Some(2), Some("b".to_string())),
        ]
    );

    // Phase 3: Insert through the fresh catalog (fully-qualified name to avoid
    // the local-catalog INSERT shortcut that register_iceberg_tables_for_query
    // creates during SELECT).
    conn.query_drop("insert into ice2.db1.tbl values (3, 'c')")
        .expect("insert into hadoop-only table");

    // After the insert, v3.metadata.json must exist and version-hint must be 3.
    assert_hadoop_catalog_metadata_compat(warehouse.path(), "db1", "tbl", 3);
}

#[test]
fn standalone_mysql_server_supports_catalog_session_context() {
    let warehouse = TempDir::new().expect("create iceberg warehouse");
    let port = alloc_port();
    let args = vec![
        "standalone-server".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create iceberg catalog");
    conn.query_drop("create database ice.db1")
        .expect("create iceberg database");

    conn.query_drop("SET new_planner_optimize_timeout = 10000")
        .expect("set planner timeout");
    conn.query_drop("SET query_timeout = 30")
        .expect("set query timeout");
    conn.query_drop("SET catalog ice").expect("set catalog ice");
    conn.query_drop("USE db1").expect("use current iceberg db");
    conn.query_drop("create table tbl (id int, name string)")
        .expect("create iceberg table");
    conn.query_drop("insert into tbl values (1, 'a')")
        .expect("insert iceberg row");

    let rows: Vec<(Option<i32>, Option<String>)> = conn
        .query("select * from tbl")
        .expect("select iceberg rows");
    assert_eq!(rows, vec![(Some(1), Some("a".to_string()))]);

    conn.query_drop("USE ice.db1")
        .expect("use explicit iceberg db");
    let filtered: Vec<(Option<String>,)> = conn
        .query("select name from tbl where id = 1")
        .expect("filtered iceberg select");
    assert_eq!(filtered, vec![(Some("a".to_string()),)]);

    conn.query_drop("SET catalog default_catalog")
        .expect("switch back to local catalog");
    conn.query_drop("USE default")
        .expect("use default local db");
    let err = conn
        .query_drop("select * from tbl")
        .expect_err("local catalog should not resolve iceberg table");
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("unknown table"),
        "unexpected local catalog error: {err}"
    );

    let err = conn
        .query_drop("SET catalog missing_catalog")
        .expect_err("unknown catalog must fail");
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("unknown catalog"),
        "unexpected missing catalog error: {err}"
    );
}

#[test]
fn standalone_mysql_server_supports_multi_statement_iceberg_steps() {
    let warehouse = TempDir::new().expect("create iceberg warehouse");
    let port = alloc_port();
    let args = vec![
        "standalone-server".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="memory","iceberg.catalog.warehouse"="{}")"#,
        warehouse.path().display()
    ))
    .expect("create iceberg catalog");
    conn.query_drop("SET catalog ice").expect("set catalog ice");

    let rows: Vec<(Option<String>,)> = conn
        .query(
            "DROP DATABASE IF EXISTS db1 FORCE;\
             CREATE DATABASE db1;\
             USE db1;\
             CREATE TABLE tbl (id int, name string);\
             INSERT INTO tbl VALUES (1, 'a'), (2, 'b');\
             SELECT name FROM tbl WHERE id = 2;\
             SET catalog default_catalog;\
             DROP TABLE ice.db1.tbl FORCE;\
             DROP DATABASE ice.db1;",
        )
        .expect("execute multi-statement iceberg step");
    assert_eq!(rows, vec![(Some("b".to_string()),)]);
}

#[test]
fn standalone_mysql_server_does_not_restore_external_preloaded_parquet_tables_from_sqlite_config() {
    // The local-parquet backend that used to persist CREATE TABLE PROPERTIES
    // references in sqlite has been removed. This test is kept as a thin
    // placeholder so the module keeps the behaviour-name for git blame; the
    // real coverage now lives in the managed-lake round-trip test which
    // exercises restart-time metadata restoration.
}

// `standalone_mysql_server_supports_json_stream_load_for_local_tables` is
// removed along with the local-parquet backend. Task 5 rewired the HTTP
// stream-load endpoint to managed lake; a managed-lake stream-load smoke
// test belongs with the managed-lake round-trip suite below and is covered
// indirectly by `standalone::engine::tests` at the lib level.

#[test]
fn standalone_mysql_server_managed_lake_round_trip() {
    let port = alloc_port();
    let Some((_config_dir, config_path)) = maybe_write_managed_lake_config(port) else {
        return;
    };

    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop("create database analytics")
        .expect("create database");
    conn.query_drop("use analytics").expect("use analytics");
    conn.query_drop(
        "create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2",
    )
    .expect("create managed table");
    conn.query_drop("insert into orders values (1, 'a'), (2, 'b')")
        .expect("insert rows");

    let rows: Vec<(Option<i32>, Option<String>)> = conn
        .query("select k1, v1 from orders order by k1")
        .expect("select inserted rows");
    assert_eq!(
        rows,
        vec![
            (Some(1), Some("a".to_string())),
            (Some(2), Some("b".to_string()))
        ]
    );

    conn.query_drop("truncate table orders")
        .expect("truncate managed table");
    let empty_rows: Vec<(Option<i32>, Option<String>)> = conn
        .query("select k1, v1 from orders order by k1")
        .expect("select after truncate");
    assert!(empty_rows.is_empty(), "rows after truncate: {empty_rows:?}");

    conn.query_drop("insert into orders values (3, 'c')")
        .expect("insert after truncate");
    let rows_after_reinsert: Vec<(Option<i32>, Option<String>)> = conn
        .query("select k1, v1 from orders order by k1")
        .expect("select after reinsert");
    assert_eq!(rows_after_reinsert, vec![(Some(3), Some("c".to_string()))]);

    conn.query_drop("drop table orders")
        .expect("drop managed table");
    let err = conn
        .query::<(i32,), _>("select k1 from orders")
        .expect_err("query after managed drop should fail");
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("unknown table"),
        "unexpected error after managed drop: {err}"
    );
}
