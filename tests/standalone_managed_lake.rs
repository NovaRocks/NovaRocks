mod common;

use std::sync::{Mutex, MutexGuard, OnceLock};

use novarocks::service::grpc_client::proto::starrocks::TabletSchemaPb;
use prost::Message;
use rusqlite::Connection;

use common::object_store::ManagedLakeTestHarness;
use novarocks::standalone::{StandaloneNovaRocks, StandaloneOptions};

/// Managed-lake integration tests write into a process-global tablet runtime
/// registry (plus segment/batch caches) via the StarRocks lake connector. Run
/// them serially so allocations of tablet_id=1, 2, … across independent SQLite
/// metadata stores don't collide with each other in that shared in-memory
/// state.
///
/// NOTE: `novarocks_config::CONFIG` is a `OnceLock`, so once a single test has
/// initialized it the entire process shares that config. In practice that
/// means running multiple managed-lake tests in one `cargo test` invocation
/// will make later tests observe the earlier tempdir's SQLite/warehouse URIs
/// and fail. Invoke each test individually (`cargo test --test
/// standalone_managed_lake <name>`) until the global config is refactored to
/// allow per-session overrides.
fn managed_lake_test_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let mutex = LOCK.get_or_init(|| Mutex::new(()));
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[test]
fn create_table_bootstraps_tablets_into_object_store() {
    let _guard = managed_lake_test_lock();
    let Some(harness) =
        ManagedLakeTestHarness::maybe_new("create_table_bootstraps_tablets_into_object_store")
            .expect("create managed lake harness")
    else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("open standalone engine");

    let create = engine.session().execute(
        "create table tbl (id int, name string) duplicate key(id) distributed by hash(id) buckets 2",
    );
    assert!(create.is_ok(), "create table failed: {create:?}");

    let info = engine
        .managed_table_info("default", "tbl")
        .expect("inspect managed table");
    assert_eq!(info.table_name, "tbl");
    assert_eq!(info.bucket_num, 2);
    assert_eq!(info.visible_version, 1);
    assert_eq!(info.tablets.len(), 2);
    assert!(
        info.tablets.iter().all(|tablet| tablet.runtime_registered),
        "all tablet runtimes should be registered: {:?}",
        info.tablets
    );
    for tablet in &info.tablets {
        let objects = harness
            .list_tablet_objects(&tablet.tablet_root_path)
            .expect("list tablet objects");
        assert!(
            objects
                .iter()
                .any(|path| path.ends_with("_0000000000000001.meta")),
            "expected initial metadata object for tablet {}: {objects:?}",
            tablet.tablet_id
        );
    }

    let conn = Connection::open(&harness.metadata_db_path).expect("open sqlite metadata");
    let table_row: (i64, i64, i64, String) = conn
        .query_row(
            "SELECT bucket_num, current_schema_id, db_id, state FROM tables WHERE name = 'tbl'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("managed table row");
    assert_eq!(table_row.0, 2);
    assert_eq!(table_row.3, "ACTIVE");
    let schema_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM table_schemas", [], |row| row.get(0))
        .expect("schema count");
    let tablet_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM tablets", [], |row| row.get(0))
        .expect("tablet count");
    assert_eq!(schema_count, 1);
    assert_eq!(tablet_count, 2);
}

#[test]
fn reopen_restores_managed_table_snapshot() {
    let _guard = managed_lake_test_lock();
    let Some(harness) = ManagedLakeTestHarness::maybe_new("reopen_restores_managed_table_snapshot")
        .expect("create managed lake harness")
    else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let initial = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("open standalone engine");
    initial
        .session()
        .execute(
            "create table tbl (id int, name string) duplicate key(id) distributed by hash(id) buckets 2",
        )
        .expect("create managed table");
    let created = initial
        .managed_table_info("default", "tbl")
        .expect("inspect created managed table");
    assert_eq!(created.tablets.len(), 2);
    drop(initial);

    let reopened = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("reopen standalone engine");
    let restored = reopened
        .managed_table_info("default", "tbl")
        .expect("inspect restored managed table");
    assert_eq!(restored.table_id, created.table_id);
    assert_eq!(restored.current_schema_id, created.current_schema_id);
    assert_eq!(
        restored
            .tablets
            .iter()
            .map(|tablet| tablet.tablet_id)
            .collect::<Vec<_>>(),
        created
            .tablets
            .iter()
            .map(|tablet| tablet.tablet_id)
            .collect::<Vec<_>>()
    );
    assert!(
        restored
            .tablets
            .iter()
            .all(|tablet| tablet.runtime_registered),
        "restored runtimes should be re-registered: {:?}",
        restored.tablets
    );
    assert!(
        restored
            .tablets
            .iter()
            .all(|tablet| tablet.snapshot_version == Some(1)),
        "restored runtimes should load object-store metadata at version 1: {:?}",
        restored.tablets
    );

    let conn = Connection::open(&harness.metadata_db_path).expect("open sqlite metadata");
    let table_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tables WHERE name = 'tbl'",
            [],
            |row| row.get(0),
        )
        .expect("table count");
    assert_eq!(table_count, 1);
}

#[test]
fn select_from_empty_managed_table_returns_empty_result() {
    let _guard = managed_lake_test_lock();
    let Some(harness) =
        ManagedLakeTestHarness::maybe_new("select_from_empty_managed_table_returns_empty_result")
            .expect("create managed lake harness")
    else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("open standalone engine");

    engine
        .session()
        .execute(
            "create table tbl (id int, name string) duplicate key(id) distributed by hash(id) buckets 2",
        )
        .expect("create managed table");

    let result = engine
        .session()
        .query("select * from tbl")
        .expect("query managed table");
    assert_eq!(result.row_count(), 0);
    assert_eq!(result.chunks.len(), 0);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0].name, "id");
    assert_eq!(result.columns[1].name, "name");
}

#[test]
fn create_table_rejects_invalid_key_columns() {
    let _guard = managed_lake_test_lock();
    let Some(harness) =
        ManagedLakeTestHarness::maybe_new("create_table_rejects_invalid_key_columns")
            .expect("create managed lake harness")
    else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("open standalone engine");
    let session = engine.session();

    let missing = session
        .execute(
            "create table missing_key (v int, k int) duplicate key(missing) distributed by hash(v) buckets 2",
        )
        .expect_err("missing key column should fail");
    assert!(
        missing.contains("key columns are missing from table schema"),
        "err={missing}"
    );

    let non_prefix = session
        .execute(
            "create table non_prefix_key (v int, k int) duplicate key(k) distributed by hash(v) buckets 2",
        )
        .expect_err("non-prefix key column should fail");
    assert!(
        non_prefix.contains("leading column prefix"),
        "err={non_prefix}"
    );
}

#[test]
fn managed_lake_insert_and_select_round_trips() {
    let _guard = managed_lake_test_lock();
    let Some(harness) =
        ManagedLakeTestHarness::maybe_new("managed_lake_insert_and_select_round_trips")
            .expect("create managed lake harness")
    else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("open standalone engine");
    let session = engine.session();

    session
        .execute(
            "create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2",
        )
        .expect("create managed table");

    session
        .execute("insert into orders values (1, 'a'), (2, 'b'), (3, NULL)")
        .expect("insert values");

    let info = engine
        .managed_table_info("default", "orders")
        .expect("inspect managed table");
    assert_eq!(info.visible_version, 2);

    let result = session
        .query("select k1, v1 from orders order by k1")
        .expect("select rows");
    assert_eq!(result.row_count(), 3);

    let conn = Connection::open(&harness.metadata_db_path).expect("open sqlite metadata");
    let txn_row: (String, i64, i64) = conn
        .query_row(
            "SELECT state, base_version, commit_version FROM txns ORDER BY txn_id DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("txn row");
    assert_eq!(txn_row.0, "VISIBLE");
    assert_eq!(txn_row.1, 1);
    assert_eq!(txn_row.2, 2);
    let partition_version: i64 = conn
        .query_row(
            "SELECT visible_version FROM partitions WHERE table_id = (SELECT table_id FROM tables WHERE name = 'orders')",
            [],
            |row| row.get(0),
        )
        .expect("partition version");
    assert_eq!(partition_version, 2);
}

#[test]
fn managed_lake_insert_hashes_rows_across_tablets() {
    let _guard = managed_lake_test_lock();
    let Some(harness) =
        ManagedLakeTestHarness::maybe_new("managed_lake_insert_hashes_rows_across_tablets")
            .expect("create managed lake harness")
    else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("open standalone engine");
    let session = engine.session();

    session
        .execute(
            "create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2",
        )
        .expect("create managed table");

    session
        .execute("insert into orders values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
        .expect("insert values");

    let info = engine
        .managed_table_info("default", "orders")
        .expect("inspect managed table");
    assert_eq!(info.tablets.len(), 2);
    for tablet in &info.tablets {
        let objects = harness
            .list_tablet_objects(&tablet.tablet_root_path)
            .expect("list tablet objects");
        let has_data_file = objects.iter().any(|path| path.contains("/data/"));
        assert!(
            has_data_file,
            "expected insert to produce a data file for tablet {}: {objects:?}",
            tablet.tablet_id
        );
    }
    assert_eq!(info.visible_version, 2);

    let result = session
        .query("select k1, v1 from orders order by k1")
        .expect("select rows");
    assert_eq!(result.row_count(), 4);
}

#[test]
fn reopen_reconciles_written_txn_into_visible_state() {
    let _guard = managed_lake_test_lock();
    let Some(harness) =
        ManagedLakeTestHarness::maybe_new("reopen_reconciles_written_txn_into_visible_state")
            .expect("create managed lake harness")
    else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("open standalone engine");
    engine
        .session()
        .execute(
            "create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2",
        )
        .expect("create managed table");
    engine
        .session()
        .execute("insert into orders values (1, 'a'), (2, 'b'), (3, 'c')")
        .expect("insert values");
    let info = engine
        .managed_table_info("default", "orders")
        .expect("inspect managed table");
    assert_eq!(info.visible_version, 2);
    drop(engine);

    // Rewind the control plane to the WRITTEN-but-not-yet-VISIBLE state as if
    // the process had crashed between rowset append and publish. The next open
    // of the engine must complete the publish and promote the txn to VISIBLE.
    let conn = Connection::open(&harness.metadata_db_path).expect("open sqlite");
    conn.execute(
        "UPDATE txns SET state = 'WRITTEN' WHERE txn_id = (SELECT MAX(txn_id) FROM txns)",
        [],
    )
    .expect("rewind txn to WRITTEN");
    conn.execute(
        "UPDATE partitions SET visible_version = 1, next_version = 2
         WHERE table_id = (SELECT table_id FROM tables WHERE name = 'orders')",
        [],
    )
    .expect("rewind partition to v1");

    let reopened = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("reopen engine triggers reconciliation");

    let conn = Connection::open(&harness.metadata_db_path).expect("reopen sqlite");
    let txn_state: String = conn
        .query_row(
            "SELECT state FROM txns ORDER BY txn_id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .expect("txn state post-reconcile");
    assert_eq!(txn_state, "VISIBLE");
    let partition_version: i64 = conn
        .query_row(
            "SELECT visible_version FROM partitions WHERE table_id = (SELECT table_id FROM tables WHERE name = 'orders')",
            [],
            |row| row.get(0),
        )
        .expect("partition version post-reconcile");
    assert_eq!(partition_version, 2);

    let result = reopened
        .session()
        .query("select k1 from orders order by k1")
        .expect("select after recovery");
    assert_eq!(result.row_count(), 3);
}

#[test]
fn create_table_preserves_largeint_and_not_null_columns() {
    let _guard = managed_lake_test_lock();
    let Some(harness) =
        ManagedLakeTestHarness::maybe_new("create_table_preserves_largeint_and_not_null_columns")
            .expect("create managed lake harness")
    else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    })
    .expect("open standalone engine");

    engine
        .session()
        .execute(
            "create table typed_tbl (id largeint not null, note string null) duplicate key(id) distributed by hash(id) buckets 2",
        )
        .expect("create managed table");

    let conn = Connection::open(&harness.metadata_db_path).expect("open sqlite metadata");
    let columns = conn
        .prepare(
            "SELECT column_name, logical_type, nullable
             FROM table_columns
             ORDER BY ordinal",
        )
        .expect("prepare column query")
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
            ))
        })
        .expect("query managed columns")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect managed columns");
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), "LARGEINT".to_string(), false),
            ("note".to_string(), "STRING".to_string(), true),
        ]
    );
    let tablet_schema_pb: Vec<u8> = conn
        .query_row(
            "SELECT s.tablet_schema_pb
             FROM table_schemas s
             JOIN tables t ON t.current_schema_id = s.schema_id
             WHERE t.name = 'typed_tbl'",
            [],
            |row| row.get(0),
        )
        .expect("load tablet schema pb");
    let tablet_schema =
        TabletSchemaPb::decode(tablet_schema_pb.as_slice()).expect("decode tablet schema pb");
    assert_eq!(tablet_schema.column.len(), 2);
    assert_eq!(tablet_schema.column[0].r#type, "LARGEINT");
    assert_eq!(tablet_schema.column[0].is_nullable, Some(false));
    assert_eq!(tablet_schema.column[1].r#type, "VARCHAR");
    assert_eq!(tablet_schema.column[1].is_nullable, Some(true));
}
