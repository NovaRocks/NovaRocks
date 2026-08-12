// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

#![cfg(feature = "mv-first-refresh-staging-test-support")]

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use mysql::prelude::Queryable;
use mysql::{Conn as MysqlConn, OptsBuilder};
use novarocks::common::app_config::NovaRocksConfig;
use novarocks::common::query_lifecycle_fault::{QueryLifecycleFaultKind, arm_path, trigger_path};
use novarocks_frontend::{FrontendServerConfig, run_frontend_server_until_shutdown};
use novarocks_server::composition::{
    IcebergMvStorageObservationAdapter, compose_frontend_control_factories,
};
use tempfile::{NamedTempFile, TempDir};

struct ReservedPort {
    _listener: TcpListener,
    port: u16,
}

impl ReservedPort {
    fn new() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve TCP port");
        let port = listener.local_addr().expect("reserved port address").port();
        Self {
            _listener: listener,
            port,
        }
    }

    fn release(self) -> u16 {
        self.port
    }
}

struct BackendProcess {
    child: Child,
    stderr_log: PathBuf,
}

impl BackendProcess {
    fn spawn(
        config: &Path,
        lifecycle_fault_backend_index: usize,
        fragment_failure_trigger: &Path,
        stderr_log: PathBuf,
    ) -> Self {
        let stderr = std::fs::File::create(&stderr_log).expect("create backend stderr log");
        let child = Command::new(env!("CARGO_BIN_EXE_novarocks"))
            .arg("standalone")
            .arg("--config")
            .arg(config)
            .env(
                "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_BACKEND_INDEX",
                lifecycle_fault_backend_index.to_string(),
            )
            .env(
                "NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE",
                fragment_failure_trigger,
            )
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn backend process");
        Self { child, stderr_log }
    }

    fn contains_stderr_marker(&self, marker: &str) -> bool {
        std::fs::read_to_string(&self.stderr_log)
            .map(|contents| contents.contains(marker))
            .unwrap_or(false)
    }

    fn kill(&mut self) {
        if self
            .child
            .try_wait()
            .expect("inspect backend process")
            .is_none()
        {
            self.child.kill().expect("kill backend process");
            self.child.wait().expect("reap killed backend process");
        }
    }
}

impl Drop for BackendProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

struct ThreeBackendFixture {
    _root: TempDir,
    _configs: Vec<NamedTempFile>,
    processes: Vec<BackendProcess>,
    endpoints: Vec<SocketAddr>,
    fragment_failure_trigger: PathBuf,
}

impl ThreeBackendFixture {
    fn start(query_lifecycle_fault_dir: &Path, fragment_failure_trigger: &Path) -> Self {
        let root = tempfile::tempdir().expect("create backend fixture root");
        let mut reservations = (0..3)
            .map(|_| (ReservedPort::new(), ReservedPort::new()))
            .collect::<Vec<_>>();
        let endpoints = reservations
            .iter()
            .map(|(_, grpc)| SocketAddr::from(([127, 0, 0, 1], grpc.port)))
            .collect::<Vec<_>>();
        let mut configs = Vec::new();
        for (index, (http, grpc)) in reservations.iter().enumerate() {
            let config = tempfile::Builder::new()
                .prefix(&format!("mvx2w-be-{index}-"))
                .suffix(".toml")
                .tempfile_in(root.path())
                .expect("create backend config");
            std::fs::write(
                config.path(),
                format!(
                    r#"
sys_log_dir = "{}"

[server]
host = "127.0.0.1"
http_port = {}
grpc_port = {}

[cluster]
role = "be"
"#,
                    root.path().join(format!("be-{index}")).display(),
                    http.port,
                    grpc.port,
                ),
            )
            .expect("write backend config");
            configs.push(config);
        }
        let mut processes = Vec::new();
        for (index, ((http, grpc), config)) in
            reservations.drain(..).zip(configs.iter()).enumerate()
        {
            let _ = http.release();
            let grpc_port = grpc.release();
            let stderr_log = root.path().join(format!("be-{index}.stderr.log"));
            processes.push(BackendProcess::spawn(
                config.path(),
                index,
                fragment_failure_trigger,
                stderr_log,
            ));
            wait_for_tcp(grpc_port, "backend gRPC endpoint");
        }
        Self {
            _root: root,
            _configs: configs,
            processes,
            endpoints,
            fragment_failure_trigger: fragment_failure_trigger.to_path_buf(),
        }
    }

    fn wait_for_init_ack_marker(&self, token: &str) -> usize {
        let marker = format!("NOVAROCKS_QUERY_INIT_ACK_OBSERVED ");
        let token_marker = format!("token={token}");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(index) = self
                .processes
                .iter()
                .enumerate()
                .find_map(|(index, process)| {
                    (process.contains_stderr_marker(&marker)
                        && process.contains_stderr_marker(&token_marker))
                    .then_some(index)
                })
            {
                return index;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for token-scoped BE InitAck marker"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn kill_backend(&mut self, backend_index: usize) {
        self.processes
            .get_mut(backend_index)
            .expect("fixture backend index")
            .kill();
    }

    fn restart_backend(&mut self, backend_index: usize) {
        let config = self
            ._configs
            .get(backend_index)
            .expect("fixture backend config")
            .path()
            .to_path_buf();
        let endpoint = *self
            .endpoints
            .get(backend_index)
            .expect("fixture backend endpoint");
        let stderr_log = self
            ._root
            .path()
            .join(format!("be-{backend_index}.restart.stderr.log"));
        let restarted = BackendProcess::spawn(
            &config,
            backend_index,
            &self.fragment_failure_trigger,
            stderr_log,
        );
        wait_for_tcp(endpoint.port(), "restarted backend gRPC endpoint");
        self.processes[backend_index] = restarted;
    }
}

fn arm_query_lifecycle_fault(
    root: &Path,
    backend_index: usize,
    kind: QueryLifecycleFaultKind,
    token: &str,
) {
    std::fs::write(
        arm_path(root, backend_index, kind),
        format!("token={token}\nbackend_index={backend_index}\n"),
    )
    .expect("arm query lifecycle fault");
}

fn arm_query_lifecycle_fault_for_any_backend(
    root: &Path,
    kind: QueryLifecycleFaultKind,
    token: &str,
) {
    for backend_index in 0..3 {
        arm_query_lifecycle_fault(root, backend_index, kind, token);
    }
}

fn clear_query_lifecycle_fault(root: &Path, kind: QueryLifecycleFaultKind) {
    for backend_index in 0..3 {
        for path in [
            arm_path(root, backend_index, kind),
            trigger_path(root, backend_index, kind),
            trigger_path(root, backend_index, kind).with_extension("release"),
        ] {
            if let Err(error) = std::fs::remove_file(path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                panic!("clear query lifecycle fault: {error}");
            }
        }
    }
}

fn arm_fragment_failure(trigger: &Path, token: &str) {
    std::fs::write(trigger, token).expect("arm native fragment failure");
    std::fs::write(trigger.with_extension("release"), token)
        .expect("release native fragment failure after Start");
}

fn wait_for_tcp(port: u16, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {label} on {port}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn connect_mysql(port: u16) -> MysqlConn {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let builder = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some("root"));
        match MysqlConn::new(builder) {
            Ok(connection) => return connection,
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for MVX-2W test MySQL server: {error}");
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => panic!("connect MVX-2W test MySQL server: {error}"),
        }
    }
}

/// This uses three independent BE processes and the ordinary frontend host.
/// Fragment submission, staging, publication, recovery evidence and
/// cancellation all use the production frontend-owned MV refresh lifecycle.
#[cfg(unix)]
#[test]
#[ignore = "requires native 1FE+3BE processes"]
fn projection_first_refresh_stages_on_three_backend_processes() {
    let query_lifecycle_fault_dir = tempfile::tempdir().expect("create query lifecycle fault root");
    let fragment_failure_trigger = query_lifecycle_fault_dir
        .path()
        .join("fragment-failure.trigger");
    // The production config loader requires this runner-owned path to match
    // the frontend and each BE configuration.  This test process owns one
    // fixture at a time, so the inherited environment is unambiguous.
    unsafe {
        std::env::set_var(
            "NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR",
            query_lifecycle_fault_dir.path(),
        );
    }
    let mut backends =
        ThreeBackendFixture::start(query_lifecycle_fault_dir.path(), &fragment_failure_trigger);
    let fe_mysql = ReservedPort::new();
    let fe_http = ReservedPort::new();
    let fe_grpc = ReservedPort::new();
    let fe_mysql_port = fe_mysql.port;
    let fe_http_port = fe_http.port;
    let fe_grpc_port = fe_grpc.port;
    let state_root = tempfile::tempdir().expect("create frontend state root");
    let state_path = state_root.path().join("state.sqlite");
    let metadata_path = state_root.path().join("metadata.sqlite");
    let config_file = tempfile::NamedTempFile::new().expect("create frontend config");
    let backend_list = backends
        .endpoints
        .iter()
        .map(|endpoint| format!("\"{endpoint}\""))
        .collect::<Vec<_>>()
        .join(", ");
    std::fs::write(
        config_file.path(),
        format!(
            r#"
[server]
host = "127.0.0.1"
http_port = {}
grpc_port = {}

[standalone_server]
mysql_port = {}

[cluster]
role = "fe"
backends = [{}]

[metadata]
provider = "sqlite"
path = "{}"

[state_store]
provider = "sqlite"
path = "{}"
cluster_id = "mvx2w-native-staging"
deployment_owner = "fe-1"
"#,
            fe_http_port,
            fe_grpc_port,
            fe_mysql_port,
            backend_list,
            metadata_path.display(),
            state_path.display(),
        ),
    )
    .expect("write frontend config");
    let config = NovaRocksConfig::load_from_file(config_file.path()).expect("load frontend config");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build frontend runtime");
    let connector_control_factories =
        compose_frontend_control_factories(&config, runtime.handle().clone())
            .expect("compose frontend control factories");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let _ = fe_mysql.release();
    let _ = fe_http.release();
    let _ = fe_grpc.release();
    let server = run_frontend_server_until_shutdown(
        FrontendServerConfig {
            config,
            config_path: Some(config_file.path().to_path_buf()),
            port_override: None,
            connector_control_factories,
            mv_storage_observation: Arc::new(IcebergMvStorageObservationAdapter::default()),
            state_store_host_config: None,
        },
        async move {
            let _ = shutdown_rx.await;
        },
    );
    let server_task = runtime.spawn(server);
    let mut conn = connect_mysql(fe_mysql_port);
    conn.query_drop(format!(
        r#"CREATE EXTERNAL CATALOG staging_ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
        state_root.path().join("warehouse").display(),
    ))
    .expect("create Iceberg catalog");
    conn.query_drop("CREATE DATABASE staging_ice.ns")
        .expect("create Iceberg namespace");
    conn.query_drop("SET CATALOG staging_ice")
        .expect("select Iceberg catalog");
    conn.query_drop("USE ns").expect("select Iceberg namespace");
    conn.query_drop(
        "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create base table");
    conn.query_drop("INSERT INTO orders VALUES (1, 10), (2, 20)")
        .expect("seed base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
    )
    .expect("create MV target");

    conn.query_drop("REFRESH MATERIALIZED VIEW orders_mv")
        .expect("stage projection first refresh through native FE session");
    let main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_mv ORDER BY k1")
        .expect("read published MV main ref");
    assert_eq!(main_rows, vec![(1, 10), (2, 20)]);

    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_agg_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
    )
    .expect("create aggregate MV target");
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_agg_mv")
        .expect("stage aggregate first refresh through native FE session");
    let aggregate_main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, total_v2 FROM orders_agg_mv ORDER BY k1")
        .expect("read published aggregate MV main ref");
    assert_eq!(aggregate_main_rows, vec![(1, 10), (2, 20)]);

    conn.query_drop(
        "CREATE TABLE customers (k1 INT, region VARCHAR(16)) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create join base table");
    conn.query_drop("INSERT INTO customers VALUES (1, 'east'), (2, 'west')")
        .expect("seed join base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_join_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT o.k1, o.v2, c.region FROM orders o JOIN customers c ON o.k1 = c.k1",
    )
    .expect("create join MV target");
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_join_mv")
        .expect("stage join first refresh through native FE session");
    let join_main_rows: Vec<(i32, i64, String)> = conn
        .query("SELECT k1, v2, region FROM orders_join_mv ORDER BY k1")
        .expect("read published join MV main ref");
    assert_eq!(
        join_main_rows,
        vec![(1, 10, "east".to_string()), (2, 20, "west".to_string())]
    );

    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_empty_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders WHERE k1 < 0",
    )
    .expect("create empty MV target");
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_empty_mv")
        .expect("stage empty first refresh through native FE session");

    conn.query_drop(
        "CREATE TABLE orders_extra (k1 INT, v2 BIGINT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    )
    .expect("create union base table");
    conn.query_drop("INSERT INTO orders_extra VALUES (3, 30)")
        .expect("seed union base table");
    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_union_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders UNION ALL SELECT k1, v2 FROM orders_extra",
    )
    .expect("create union MV target");
    conn.query_drop("REFRESH MATERIALIZED VIEW orders_union_mv")
        .expect("stage union first refresh through native FE session");
    let union_main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_union_mv ORDER BY k1")
        .expect("read published union MV main ref");
    assert_eq!(union_main_rows, vec![(1, 10), (2, 20), (3, 30)]);

    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_start_fault_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
    )
    .expect("create start-fault MV target");
    arm_query_lifecycle_fault_for_any_backend(
        query_lifecycle_fault_dir.path(),
        QueryLifecycleFaultKind::StartAckSuppress,
        "mvx2w-start-abort",
    );
    let start_fault = conn
        .query_drop("REFRESH MATERIALIZED VIEW orders_start_fault_mv")
        .expect_err("a partial native start must not produce a staging completion");
    assert!(
        !start_fault.to_string().is_empty(),
        "failed native start must preserve an explanatory error"
    );
    let start_fault_main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_start_fault_mv ORDER BY k1")
        .expect("read start-fault MV main ref");
    assert!(
        start_fault_main_rows.is_empty(),
        "a failed native start must never publish the MV main ref"
    );
    clear_query_lifecycle_fault(
        query_lifecycle_fault_dir.path(),
        QueryLifecycleFaultKind::StartAckSuppress,
    );

    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_terminal_conflict_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
    )
    .expect("create terminal-conflict MV target");
    arm_query_lifecycle_fault_for_any_backend(
        query_lifecycle_fault_dir.path(),
        QueryLifecycleFaultKind::TerminalSnapshotConflict,
        "mvx2w-terminal-conflict",
    );
    let terminal_conflict = conn
        .query_drop("REFRESH MATERIALIZED VIEW orders_terminal_conflict_mv")
        .expect_err("a conflicting terminal report must not produce a staging completion");
    assert!(
        !terminal_conflict.to_string().is_empty(),
        "conflicting terminal report must preserve an explanatory error"
    );
    let terminal_conflict_main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_terminal_conflict_mv ORDER BY k1")
        .expect("read terminal-conflict MV main ref");
    assert!(
        terminal_conflict_main_rows.is_empty(),
        "a conflicting terminal report must never publish the MV main ref"
    );
    clear_query_lifecycle_fault(
        query_lifecycle_fault_dir.path(),
        QueryLifecycleFaultKind::TerminalSnapshotConflict,
    );

    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_fragment_failure_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
    )
    .expect("create fragment-failure MV target");
    arm_fragment_failure(&fragment_failure_trigger, "mvx2w-fragment-failure");
    let fragment_failure = conn
        .query_drop("REFRESH MATERIALIZED VIEW orders_fragment_failure_mv")
        .expect_err("a failed native writer fragment must not produce a staging completion");
    assert!(
        !fragment_failure.to_string().is_empty(),
        "failed native writer fragment must preserve an explanatory error"
    );
    let fragment_failure_main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_fragment_failure_mv ORDER BY k1")
        .expect("read fragment-failure MV main ref");
    assert!(
        fragment_failure_main_rows.is_empty(),
        "a failed native writer fragment must never publish the MV main ref"
    );
    let fragment_failure_release = fragment_failure_trigger.with_extension("release");
    for path in [&fragment_failure_trigger, &fragment_failure_release] {
        if let Err(error) = std::fs::remove_file(path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            panic!("clear native fragment failure trigger: {error}");
        }
    }

    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_backend_loss_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
    )
    .expect("create backend-loss MV target");
    const BACKEND_LOSS_TOKEN: &str = "mvx2w-be-restart";
    arm_query_lifecycle_fault_for_any_backend(
        query_lifecycle_fault_dir.path(),
        QueryLifecycleFaultKind::RestartAfterInitAck,
        BACKEND_LOSS_TOKEN,
    );
    let (backend_loss_tx, backend_loss_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut staging = connect_mysql(fe_mysql_port);
        staging
            .query_drop("SET CATALOG staging_ice")
            .expect("select backend-loss staging catalog");
        staging
            .query_drop("USE ns")
            .expect("select backend-loss staging database");
        let result = staging.query_drop("REFRESH MATERIALIZED VIEW orders_backend_loss_mv");
        let _ = backend_loss_tx.send(result);
    });
    let admitted_backend = backends.wait_for_init_ack_marker(BACKEND_LOSS_TOKEN);
    backends.kill_backend(admitted_backend);
    let backend_loss = backend_loss_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("lost admitted BE must terminate native staging")
        .expect_err("a lost admitted BE must not produce a staging completion");
    assert!(
        !backend_loss.to_string().is_empty(),
        "lost admitted BE must preserve an explanatory error"
    );
    backends.restart_backend(admitted_backend);
    clear_query_lifecycle_fault(
        query_lifecycle_fault_dir.path(),
        QueryLifecycleFaultKind::RestartAfterInitAck,
    );
    let backend_loss_main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_backend_loss_mv ORDER BY k1")
        .expect("read backend-loss MV main ref");
    assert!(
        backend_loss_main_rows.is_empty(),
        "a lost admitted BE must never publish the MV main ref"
    );

    conn.query_drop(
        "CREATE MATERIALIZED VIEW orders_kill_mv DISTRIBUTED BY HASH(k1) BUCKETS 2 AS SELECT k1, v2 FROM orders",
    )
    .expect("create KILL QUERY MV target");
    const KILL_QUERY_TOKEN: &str = "mvx2w-kill-query";
    arm_query_lifecycle_fault_for_any_backend(
        query_lifecycle_fault_dir.path(),
        QueryLifecycleFaultKind::RestartAfterInitAck,
        KILL_QUERY_TOKEN,
    );
    let (kill_target_ready_tx, kill_target_ready_rx) = mpsc::sync_channel(1);
    let (kill_target_done_tx, kill_target_done_rx) = mpsc::sync_channel(1);
    let kill_target = std::thread::spawn(move || {
        let mut target = connect_mysql(fe_mysql_port);
        target
            .query_drop("SET CATALOG staging_ice")
            .expect("select KILL QUERY target catalog");
        target
            .query_drop("USE ns")
            .expect("select KILL QUERY target database");
        kill_target_ready_tx
            .send(target.connection_id())
            .expect("publish KILL QUERY target connection id");
        let result = target.query_drop("REFRESH MATERIALIZED VIEW orders_kill_mv");
        kill_target_done_tx
            .send(result)
            .expect("publish KILL QUERY target result");
    });
    let kill_target_connection_id = kill_target_ready_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("receive KILL QUERY target connection id");
    let kill_admitted_backend = backends.wait_for_init_ack_marker(KILL_QUERY_TOKEN);
    if let Ok(result) = kill_target_done_rx.try_recv() {
        panic!("native MV first refresh completed before KILL QUERY: {result:?}");
    }
    conn.query_drop(format!("KILL QUERY {kill_target_connection_id}"))
        .expect("KILL QUERY must acknowledge the staged MV attempt");
    let kill_release = trigger_path(
        query_lifecycle_fault_dir.path(),
        kill_admitted_backend,
        QueryLifecycleFaultKind::RestartAfterInitAck,
    )
    .with_extension("release");
    std::fs::write(&kill_release, KILL_QUERY_TOKEN)
        .expect("release native Init rendezvous after KILL QUERY");
    let kill_target_error = kill_target_done_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("KILL QUERY target must terminate")
        .expect_err("KILL QUERY must not allow native MV staging to succeed");
    assert!(
        !kill_target_error.to_string().is_empty(),
        "KILL QUERY must preserve an explanatory target error"
    );
    kill_target.join().expect("join KILL QUERY target thread");
    let kill_main_rows: Vec<(i32, i64)> = conn
        .query("SELECT k1, v2 FROM orders_kill_mv ORDER BY k1")
        .expect("read KILL QUERY MV main ref");
    assert!(
        kill_main_rows.is_empty(),
        "KILL QUERY must never publish the MV main ref"
    );
    clear_query_lifecycle_fault(
        query_lifecycle_fault_dir.path(),
        QueryLifecycleFaultKind::RestartAfterInitAck,
    );

    drop(conn);
    shutdown_tx
        .send(())
        .expect("request frontend server shutdown");
    let server_result = runtime
        .block_on(server_task)
        .expect("join frontend server task");
    server_result.expect("shutdown frontend server");
}
