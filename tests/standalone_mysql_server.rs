use std::io::Read;
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

#[test]
fn standalone_mysql_server_supports_basic_ddl_without_preloaded_tables() {
    let parquet = write_parquet_file(&[(1, Some("a")), (2, Some("b"))]);
    let port = alloc_port();
    let args = vec![
        "standalone-server".to_string(),
        "--port".to_string(),
        port.to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop("create database analytics")
        .expect("create database");
    conn.query_drop("use analytics").expect("use analytics");
    conn.query_drop(format!(
        r#"create table tbl properties("path"="{}")"#,
        parquet.path().display()
    ))
    .expect("create table");

    let rows: Vec<(i32, Option<String>)> = conn.query("select * from tbl").expect("select *");
    assert_eq!(
        rows,
        vec![(1, Some("a".to_string())), (2, Some("b".to_string()))]
    );

    conn.query_drop("drop table tbl").expect("drop table");
    let err = conn
        .query_drop("select * from tbl")
        .expect_err("querying dropped table must fail");
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("unknown table"),
        "unexpected error after drop table: {err}"
    );
}

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
fn standalone_mysql_server_restores_local_metadata_from_sqlite_config() {
    let parquet = write_parquet_file(&[(1, Some("a")), (2, Some("b"))]);
    let config_dir = TempDir::new().expect("create config dir");
    let config_path = config_dir.path().join("novarocks.toml");

    let write_config = |port: u16| {
        std::fs::write(
            &config_path,
            format!(
                r#"[standalone_server]
mysql_port = {port}
user = "root"
metadata_db_path = "meta/catalog.db"
"#,
            ),
        )
        .expect("write config");
    };

    let port = alloc_port();
    write_config(port);
    {
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
        conn.query_drop(format!(
            r#"create table tbl properties("path"="{}")"#,
            parquet.path().display()
        ))
        .expect("create table");
    }

    let restart_port = alloc_port();
    write_config(restart_port);
    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(restart_port);
    conn.query_drop("use analytics").expect("use analytics");

    let rows: Vec<(i32, Option<String>)> = conn.query("select * from tbl").expect("select *");
    assert_eq!(
        rows,
        vec![(1, Some("a".to_string())), (2, Some("b".to_string()))]
    );
}
