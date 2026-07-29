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

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder, Row};
use tempfile::TempDir;

fn alloc_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

struct ServerGuard {
    child: Child,
    _lock: MutexGuard<'static, ()>,
}

static STANDALONE_SERVER_TEST_LOCK: Mutex<()> = Mutex::new(());

impl ServerGuard {
    fn spawn(args: &[String]) -> Self {
        let lock = STANDALONE_SERVER_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let child = Command::new(env!("CARGO_BIN_EXE_novarocks"))
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn standalone");
        Self { child, _lock: lock }
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
            panic!("standalone exited early with status {status}: {output}");
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
                        "mysql connection to standalone failed: {}\nchild output:\n{output}",
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

#[allow(dead_code)]
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

fn write_standalone_metadata_config(mysql_port: u16) -> (TempDir, PathBuf) {
    let config_dir = TempDir::new().expect("create standalone server config dir");
    let config_path = config_dir.path().join("novarocks.toml");
    let state_store_path = config_dir.path().join("frontend-state.sqlite");
    std::fs::write(
        &config_path,
        format!(
            r#"[metadata]
provider = "sqlite"
path = "meta/catalog.db"

[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "standalone-mysql-server-test"
deployment_owner = "all-in-one"

[standalone_server]
mysql_port = {mysql_port}
user = "root"
"#,
            state_store_path.display(),
        ),
    )
    .expect("write standalone server config");
    (config_dir, config_path)
}

fn standalone_server_args_with_metadata(mysql_port: u16) -> (TempDir, Vec<String>) {
    let (config_dir, config_path) = write_standalone_metadata_config(mysql_port);
    let args = vec![
        "standalone".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    (config_dir, args)
}

fn write_legacy_starrocks_table_config(mysql_port: u16) -> (TempDir, PathBuf) {
    let config_dir = TempDir::new().expect("create StarRocks table config dir");
    let config_path = config_dir.path().join("novarocks.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"[metadata]
provider = "sqlite"
path = "meta/catalog.db"

[standalone_server]
mysql_port = {mysql_port}
user = "root"
warehouse_uri = "s3://novarocks/legacy-starrocks-table"

[standalone_server.object_store]
endpoint = "http://127.0.0.1:9000"
access_key_id = "admin"
access_key_secret = "admin123"
enable_path_style_access = true
"#
        ),
    )
    .expect("write StarRocks table config");
    (config_dir, config_path)
}

fn s3_test_value(primary: &str, fallback_env: &str, default: &str) -> String {
    std::env::var(primary)
        .or_else(|_| std::env::var(fallback_env))
        .unwrap_or_else(|_| default.to_string())
}

fn unique_iceberg_warehouse(prefix: &str) -> String {
    let bucket = std::env::var("AWS_S3_BUCKET").unwrap_or_else(|_| "novarocks".to_string());
    let root_prefix =
        std::env::var("AWS_S3_ROOT").unwrap_or_else(|_| "codex-starrocks-table-tests".to_string());
    let run_id = format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let root_prefix = root_prefix.trim_matches('/');
    if root_prefix.is_empty() {
        format!("s3://{bucket}/{run_id}")
    } else {
        format!("s3://{bucket}/{root_prefix}/{run_id}")
    }
}

fn create_s3_iceberg_catalog_sql(catalog_name: &str, warehouse_uri: &str) -> String {
    let endpoint =
        std::env::var("AWS_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let access_key_id = s3_test_value("AWS_S3_ACCESS_KEY_ID", "MINIO_ROOT_USER", "admin");
    let access_key_secret = s3_test_value(
        "AWS_S3_SECRET_ACCESS_KEY",
        "MINIO_ROOT_PASSWORD",
        "admin123",
    );
    format!(
        r#"create external catalog {catalog_name} properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{warehouse_uri}","aws.s3.endpoint"="{endpoint}","aws.s3.access_key"="{access_key_id}","aws.s3.secret_key"="{access_key_secret}","aws.s3.enable_path_style_access"="true")"#
    )
}

#[test]
fn standalone_mysql_server_accepts_queries_and_session_noops_without_bootstrap_tables() {
    let port = alloc_port();
    let args = vec![
        "standalone".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.ping().expect("ping standalone");
    conn.query_drop("USE default").expect("USE default");
    conn.query_drop("SET NAMES utf8mb4")
        .expect("SET NAMES utf8mb4");
    conn.query_drop("SET autocommit = 1")
        .expect("SET autocommit = 1");
    conn.query_drop("SET character_set_results = NULL")
        .expect("SET character_set_results = NULL");

    let rows: Vec<(i32,)> = conn.query("select 1").expect("select constant");
    assert_eq!(rows, vec![(1,)]);
}

#[test]
fn standalone_mysql_server_rejects_wrong_auth_and_unsupported_sql() {
    let port = alloc_port();
    let args = vec![
        "standalone".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    let err = conn
        .query_drop("grant select on tbl to root")
        .expect_err("grant must fail");
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

#[test]
fn standalone_mysql_server_supports_minimal_iceberg_flow() {
    let warehouse = TempDir::new().expect("create iceberg warehouse");
    let port = alloc_port();
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
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
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
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
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
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

    // Each INSERT publishes one data metadata commit in the distributed write path.
    assert_hadoop_catalog_metadata_compat(warehouse.path(), "db1", "tbl", 3);
}

#[test]
fn standalone_mysql_server_supports_catalog_session_context() {
    let warehouse = TempDir::new().expect("create iceberg warehouse");
    let port = alloc_port();
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
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
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
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
fn standalone_mysql_server_rejects_no_catalog_persistent_table() {
    let port = alloc_port();
    let args = vec![
        "standalone".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    let err = conn
        .query_drop("create table t_no_catalog (id int)")
        .expect_err("CREATE TABLE without Iceberg catalog should fail");
    let err = err.to_string().to_ascii_lowercase();
    assert!(err.contains("iceberg catalog"), "unexpected error: {err}");
}

#[test]
fn standalone_mysql_server_rejects_default_catalog_persistent_table() {
    let port = alloc_port();
    let args = vec![
        "standalone".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    let err = conn
        .query_drop("create table default_catalog.db1.t_default_catalog (id int)")
        .expect_err("default_catalog should not be a user table catalog");
    let err = err.to_string().to_ascii_lowercase();
    assert!(
        err.contains("default_catalog") || err.contains("iceberg catalog"),
        "unexpected error: {err}"
    );
}

#[test]
fn standalone_mysql_server_rejects_legacy_starrocks_table_config_target() {
    let port = alloc_port();
    let (_config_dir, config_path) = write_legacy_starrocks_table_config(port);

    let args = vec![
        "standalone".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    let err = conn
        .query_drop("create database analytics")
        .expect_err("legacy StarRocks table config must not enable local CREATE DATABASE");
    let err = err.to_string().to_ascii_lowercase();
    assert!(err.contains("iceberg catalog"), "unexpected error: {err}");

    let err = conn
        .query_drop(
            "create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2",
        )
        .expect_err("legacy StarRocks table config must not enable local CREATE TABLE");
    let err = err.to_string().to_ascii_lowercase();
    assert!(err.contains("iceberg catalog"), "unexpected error: {err}");
}

#[test]
fn standalone_mysql_server_mv_create_and_manual_refresh_round_trip() {
    let port = alloc_port();
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
    let iceberg_warehouse = unique_iceberg_warehouse("mv_happy");

    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse))
        .expect("create iceberg catalog");
    conn.query_drop("create database ice.ns")
        .expect("create iceberg namespace");
    conn.query_drop("set catalog ice").expect("set catalog");
    conn.query_drop("use ns").expect("use namespace");
    conn.query_drop(
        r#"create table orders (k1 int, v2 bigint)
           TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")"#,
    )
    .expect("create iceberg orders");
    conn.query_drop("insert into orders values (1, 10), (2, 20), (3, 50)")
        .expect("seed iceberg rows");
    let base_rows: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, v2 from orders order by k1")
        .expect("select base rows");
    assert_eq!(
        base_rows,
        vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(50)),
        ]
    );

    conn.query_drop(
        "create materialized view orders_mv \
         distributed by hash(k1) buckets 2 \
         as select k1, v2 from orders",
    )
    .expect("create mv");

    let pre_rows: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, v2 from orders_mv")
        .expect("select before refresh");
    assert!(pre_rows.is_empty(), "pre-refresh rows: {pre_rows:?}");

    conn.query_drop("refresh materialized view orders_mv")
        .expect("refresh mv");
    let rows: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, v2 from orders_mv order by k1")
        .expect("select after refresh");
    assert_eq!(
        rows,
        vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(50)),
        ]
    );

    conn.query_drop("insert into orders values (4, 70)")
        .expect("second iceberg write");
    let stable: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, v2 from orders_mv order by k1")
        .expect("select post-write pre-refresh");
    assert_eq!(
        stable,
        vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(50)),
        ],
        "MV should not see new rows until the next REFRESH"
    );

    conn.query_drop("refresh materialized view orders_mv")
        .expect("second refresh mv");
    let post: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, v2 from orders_mv order by k1")
        .expect("select after second refresh");
    assert_eq!(
        post,
        vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(50)),
            (Some(4), Some(70)),
        ]
    );

    conn.query_drop("drop materialized view orders_mv")
        .expect("drop mv");
    let err = conn
        .query::<(i32,), _>("select k1 from orders_mv")
        .expect_err("query after drop should fail");
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("unknown table")
            || err
                .to_string()
                .to_ascii_lowercase()
                .contains("does not exist")
            || err
                .to_string()
                .to_ascii_lowercase()
                .contains("no metadata files"),
        "unexpected error after mv drop: {err}"
    );
}

#[test]
fn standalone_mysql_server_mv_incremental_refresh_noops_when_snapshot_unchanged() {
    let port = alloc_port();
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
    let iceberg_warehouse = unique_iceberg_warehouse("mv_incremental_noop");

    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse))
        .expect("create iceberg catalog");
    conn.query_drop("create database ice.ns")
        .expect("create iceberg namespace");
    conn.query_drop("set catalog ice").expect("set catalog");
    conn.query_drop("use ns").expect("use namespace");
    conn.query_drop(
        r#"create table orders (k1 int, v2 bigint)
           TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")"#,
    )
    .expect("create iceberg orders");
    conn.query_drop("insert into orders values (1, 10), (2, 20)")
        .expect("seed iceberg rows");

    conn.query_drop(
        "create materialized view orders_mv \
         distributed by hash(k1) buckets 2 \
         as select k1, v2 from orders where v2 >= 10",
    )
    .expect("create mv");

    conn.query_drop("refresh materialized view orders_mv")
        .expect("first refresh mv");
    conn.query_drop("refresh materialized view orders_mv")
        .expect("second refresh mv without base append");

    let rows: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, v2 from orders_mv order by k1")
        .expect("select after unchanged-snapshot refresh");
    assert_eq!(rows, vec![(Some(1), Some(10)), (Some(2), Some(20))]);
}

#[test]
fn standalone_mysql_server_mv_incremental_refresh_appends_only_new_rows() {
    let port = alloc_port();
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
    let iceberg_warehouse = unique_iceberg_warehouse("mv_incremental_append");

    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse))
        .expect("create iceberg catalog");
    conn.query_drop("create database ice.ns")
        .expect("create iceberg namespace");
    conn.query_drop("set catalog ice").expect("set catalog");
    conn.query_drop("use ns").expect("use namespace");
    conn.query_drop(
        r#"create table orders (k1 int, v2 bigint)
           TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")"#,
    )
    .expect("create iceberg orders");
    conn.query_drop("insert into orders values (1, 10), (2, 20)")
        .expect("seed iceberg rows");

    conn.query_drop(
        "create materialized view orders_mv \
         distributed by hash(k1) buckets 2 \
         as select k1, v2 from orders where v2 >= 20",
    )
    .expect("create mv");

    conn.query_drop("refresh materialized view orders_mv")
        .expect("first refresh mv");
    conn.query_drop("insert into orders values (3, 30), (4, 5)")
        .expect("append iceberg rows");
    conn.query_drop("refresh materialized view orders_mv")
        .expect("second refresh mv");

    let rows: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, v2 from orders_mv order by k1")
        .expect("select after incremental append refresh");
    assert_eq!(rows, vec![(Some(2), Some(20)), (Some(3), Some(30))]);
}

#[test]
fn standalone_mysql_server_mv_show_output_matches_expected_columns() {
    let port = alloc_port();
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
    let iceberg_warehouse = unique_iceberg_warehouse("mv_show");

    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse))
        .expect("create iceberg catalog");
    conn.query_drop("create database ice.ns")
        .expect("create iceberg namespace");
    conn.query_drop("set catalog ice").expect("set catalog");
    conn.query_drop("use ns").expect("use namespace");
    conn.query_drop(
        r#"create table orders (k1 int, v2 bigint)
           TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")"#,
    )
    .expect("create iceberg orders");
    conn.query_drop(
        "create materialized view orders_mv \
         distributed by hash(k1) buckets 2 \
         as select k1 from orders",
    )
    .expect("create mv");

    let rows: Vec<Row> = conn
        .query("show materialized views from ns")
        .expect("show mvs");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 15);
    assert_eq!(row.get::<String, _>(0), Some("orders_mv".to_string()));
    assert_eq!(row.get::<String, _>(1), Some("ns".to_string()));
    assert_eq!(row.get::<String, _>(2), Some("iceberg".to_string()));
    assert_eq!(row.get::<String, _>(3), Some("DEFERRED_MANUAL".to_string()));
    assert_eq!(row.get::<Option<String>, _>(4), Some(None));
    assert_eq!(row.get::<Option<String>, _>(5), Some(None));
    assert_eq!(row.get::<String, _>(6), Some("ice.ns.orders".to_string()));
    assert!(
        row.get::<String, _>(7)
            .expect("select text")
            .to_ascii_lowercase()
            .contains("select")
    );
    assert_eq!(row.get::<String, _>(8), Some("ice.ns.orders".to_string()));
    assert_eq!(row.get::<String, _>(9), Some("false".to_string()));
    assert_eq!(row.get::<Option<String>, _>(10), Some(None));
    assert_eq!(row.get::<Option<String>, _>(11), Some(None));
    assert_eq!(row.get::<Option<String>, _>(12), Some(None));
    assert_eq!(row.get::<String, _>(13), Some("MANUAL".to_string()));
    assert_eq!(row.get::<Option<String>, _>(14), Some(None));
}

#[test]
fn standalone_mysql_server_mv_refresh_policy_ddl_updates_show_metadata() {
    let port = alloc_port();
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
    let iceberg_warehouse = unique_iceberg_warehouse("mv_refresh_policy_ddl");

    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse))
        .expect("create iceberg catalog");
    conn.query_drop("create database ice.ns")
        .expect("create iceberg namespace");
    conn.query_drop("set catalog ice").expect("set catalog");
    conn.query_drop("use ns").expect("use namespace");
    conn.query_drop(
        r#"create table orders (k1 int, v2 bigint)
           TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")"#,
    )
    .expect("create iceberg orders");
    conn.query_drop(
        "create materialized view orders_mv \
         distributed by hash(k1) buckets 2 \
         refresh async every interval 5 minute \
         as select k1 from orders",
    )
    .expect("create mv with refresh policy");

    let created: Vec<Row> = conn
        .query("show materialized views from ns")
        .expect("show mvs after create");
    assert_eq!(created.len(), 1);
    assert_eq!(
        created[0].get::<String, _>(3),
        Some("ASYNC_INTERVAL".to_string())
    );
    assert_eq!(created[0].get::<String, _>(9), Some("false".to_string()));

    conn.query_drop("alter materialized view orders_mv pause refresh")
        .expect("pause refresh");
    let paused: Vec<Row> = conn
        .query("show materialized views from ns")
        .expect("show mvs after pause");
    assert_eq!(
        paused[0].get::<String, _>(3),
        Some("ASYNC_INTERVAL".to_string())
    );
    assert_eq!(paused[0].get::<String, _>(9), Some("true".to_string()));

    conn.query_drop("alter materialized view orders_mv set refresh async on change")
        .expect("set refresh on change");
    conn.query_drop("alter materialized view orders_mv resume refresh")
        .expect("resume refresh");
    let resumed: Vec<Row> = conn
        .query("show materialized views from ns")
        .expect("show mvs after resume");
    assert_eq!(
        resumed[0].get::<String, _>(3),
        Some("ASYNC_ON_CHANGE".to_string())
    );
    assert_eq!(resumed[0].get::<String, _>(9), Some("false".to_string()));
    assert_eq!(resumed[0].get::<Option<String>, _>(10), Some(None));
}

#[test]
fn standalone_mysql_server_mv_create_rejects_starrocks_storage_engine() {
    let port = alloc_port();
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
    let iceberg_warehouse = unique_iceberg_warehouse("mv_reject_starrocks");

    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse))
        .expect("create iceberg catalog");
    conn.query_drop("create database ice.ns")
        .expect("create iceberg namespace");
    conn.query_drop("set catalog ice").expect("set catalog");
    conn.query_drop("use ns").expect("use namespace");
    conn.query_drop(
        r#"create table base_table (k1 int, v2 bigint)
           TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")"#,
    )
    .expect("create iceberg base table");

    let err = conn
        .query_drop(
            "create materialized view mv1 \
             distributed by hash(k1) buckets 2 \
             properties ('storage_engine'='starrocks') \
             as select k1 from base_table",
        )
        .expect_err("should reject StarRocks MV storage engine");
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("storage_engine='starrocks'"),
        "unexpected error: {err}"
    );
}

#[test]
fn standalone_mysql_server_mv_reopen_preserves_iceberg_mv() {
    let port = alloc_port();
    let (_config_dir, args) = standalone_server_args_with_metadata(port);
    let iceberg_warehouse = unique_iceberg_warehouse("mv_reopen");

    {
        let mut server = ServerGuard::spawn(&args);
        let mut conn = server.connect_root(port);
        conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse))
            .expect("create iceberg catalog");
        conn.query_drop("create database ice.ns")
            .expect("create iceberg namespace");
        conn.query_drop("set catalog ice").expect("set catalog");
        conn.query_drop("use ns").expect("use namespace");
        conn.query_drop(
            r#"create table orders (k1 int)
               TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")"#,
        )
        .expect("create iceberg orders");
        conn.query_drop("insert into orders values (1), (2)")
            .expect("seed iceberg rows");
        conn.query_drop(
            "create materialized view orders_mv \
             distributed by hash(k1) buckets 1 \
             as select k1 from orders",
        )
        .expect("create mv");
        conn.query_drop("refresh materialized view orders_mv")
            .expect("first refresh");
    }

    {
        let mut server = ServerGuard::spawn(&args);
        let mut conn = server.connect_root(port);
        conn.query_drop("set catalog ice").expect("set catalog");
        conn.query_drop("use ns").expect("use namespace");
        let rows: Vec<(Option<i32>,)> = conn
            .query("select k1 from orders_mv order by k1")
            .expect("select after reopen");
        assert_eq!(rows, vec![(Some(1),), (Some(2),)]);
    }
}
