mod common;

use rusqlite::Connection;

use common::object_store::ManagedLakeTestHarness;
use novarocks::standalone::{StandaloneNovaRocks, StandaloneOptions};

#[test]
fn create_table_bootstraps_tablets_into_object_store() {
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
fn create_table_rejects_invalid_key_columns() {
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
