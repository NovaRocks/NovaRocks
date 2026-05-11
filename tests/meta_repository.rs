use std::collections::BTreeMap;

use bytes::Bytes;
use novarocks::meta::keys::NS_MANAGED_TXN;
use novarocks::meta::repository::iceberg_catalog::{
    IcebergCatalogMetaRepository, IcebergCatalogProperties,
};
use novarocks::meta::repository::managed_lake::{
    CreateManagedDatabaseRequest, CreateManagedTableRequest, ManagedLakeMetaRepository,
    ManagedPartitionState, ManagedTableKind, ManagedTableState,
};
use novarocks::meta::repository::managed_txn::{
    ManagedLakeTxnRepository, ManagedTxnState, StoredManagedTxn,
};
use novarocks::meta::repository::mv::{
    CreateMvDefinitionRequest, MvMetaRepository, MvRefreshFinalizeRequest, MvRefreshState,
    MvTargetLookup, RefreshExternalOutcome,
};
use novarocks::meta::repository::{
    RepositoryError, RepositoryErrorKind, decode_json_payload, encode_json_payload, id_scopes,
};
use novarocks::meta::{
    ExpectedRevision, MetaKey, MetaRecordKind, MetaRecordPut, MetaStoreProvider,
    SqliteMetaStoreProvider,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SamplePayload {
    id: i64,
    name: String,
}

fn create_managed_table_with_partition(
    provider: &SqliteMetaStoreProvider,
    repository: &ManagedLakeMetaRepository,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    create_named_managed_table_with_partition(provider, repository, "orders")
}

fn create_named_managed_table_with_partition(
    provider: &SqliteMetaStoreProvider,
    repository: &ManagedLakeMetaRepository,
    table_name: &str,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    let mut txn = provider.begin_write("create managed lake objects")?;
    let database = repository.create_database(
        txn.as_mut(),
        CreateManagedDatabaseRequest {
            name: format!("{table_name}_db"),
        },
    )?;
    let table = repository.create_table(
        txn.as_mut(),
        CreateManagedTableRequest {
            db_id: database.db_id,
            name: table_name.to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 2,
            current_schema_id: 10,
            state: ManagedTableState::Active,
            kind: ManagedTableKind::Table,
        },
    )?;
    let partition = repository.create_partition(txn.as_mut(), table.table_id, table_name, 1)?;
    txn.commit()?;
    Ok((table.table_id, partition.partition_id))
}

fn put_managed_txn_record(
    txn: &mut dyn novarocks::meta::MetaWriteTxn,
    managed_txn: StoredManagedTxn,
) -> Result<(), Box<dyn std::error::Error>> {
    txn.put(MetaRecordPut::new(
        MetaKey::new(NS_MANAGED_TXN, [managed_txn.txn_id.to_string()])?,
        MetaRecordKind::new("managed.txn")?,
        ExpectedRevision::NotExists,
        encode_json_payload(1, &managed_txn)?,
    ))?;
    Ok(())
}

#[test]
fn repository_payload_json_round_trips() {
    let payload = SamplePayload {
        id: 7,
        name: "orders".to_string(),
    };
    let encoded = encode_json_payload(1, &payload).expect("encode payload");
    assert_eq!(encoded.schema_version, 1);
    assert_eq!(
        encoded.bytes,
        Bytes::from_static(br#"{"id":7,"name":"orders"}"#)
    );

    let decoded: SamplePayload = decode_json_payload(&encoded).expect("decode payload");
    assert_eq!(decoded, payload);
}

#[test]
fn repository_id_scopes_are_stable_strings() {
    assert_eq!(id_scopes::managed_db().as_str(), "managed.db");
    assert_eq!(id_scopes::managed_table().as_str(), "managed.table");
    assert_eq!(id_scopes::managed_partition().as_str(), "managed.partition");
    assert_eq!(id_scopes::managed_index().as_str(), "managed.index");
    assert_eq!(id_scopes::managed_tablet().as_str(), "managed.tablet");
    assert_eq!(id_scopes::managed_txn().as_str(), "managed.txn");
    assert_eq!(id_scopes::mv_id().as_str(), "mv.id");
    assert_eq!(id_scopes::refresh_id().as_str(), "refresh.id");
    assert_eq!(id_scopes::erase_job().as_str(), "job.erase");
    assert_eq!(
        id_scopes::iceberg_optimize_job().as_str(),
        "job.iceberg_optimize"
    );
}

#[test]
fn repository_namespaces_are_stable_strings() {
    assert_eq!(NS_MANAGED_TXN, "managed.txn");
}

#[test]
fn repository_error_display_is_domain_facing() {
    let err = RepositoryError::conflict("managed txn state changed");
    assert_eq!(
        err.to_string(),
        "metadata repository conflict: managed txn state changed"
    );
}

#[test]
fn managed_lake_repository_creates_database_table_and_active_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = ManagedLakeMetaRepository::default();

    {
        let mut txn = provider.begin_write("create managed lake objects")?;
        let database = repository.create_database(
            txn.as_mut(),
            CreateManagedDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        let table = repository.create_table(
            txn.as_mut(),
            CreateManagedTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Creating,
                kind: ManagedTableKind::MaterializedView,
            },
        )?;
        repository.create_partition(txn.as_mut(), table.table_id, "orders", 1)?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let snapshot = repository.load_snapshot(read.as_ref())?;
    assert_eq!(snapshot.databases.len(), 1);
    assert_eq!(snapshot.tables.len(), 1);
    assert_eq!(snapshot.partitions.len(), 1);
    assert!(snapshot.schemas.is_empty());
    assert!(snapshot.columns.is_empty());
    assert!(snapshot.indexes.is_empty());
    assert!(snapshot.tablets.is_empty());

    assert_eq!(snapshot.databases[0].name, "db1");
    assert_eq!(snapshot.tables[0].db_id, snapshot.databases[0].db_id);
    assert_eq!(snapshot.tables[0].name, "orders");
    assert_eq!(snapshot.tables[0].keys_type, "DUP_KEYS");
    assert_eq!(snapshot.tables[0].bucket_num, 2);
    assert_eq!(snapshot.tables[0].current_schema_id, 10);
    assert_eq!(snapshot.tables[0].state, ManagedTableState::Creating);
    assert_eq!(snapshot.tables[0].kind, ManagedTableKind::MaterializedView);
    assert_eq!(snapshot.partitions[0].table_id, snapshot.tables[0].table_id);
    assert_eq!(snapshot.partitions[0].name, "orders");
    assert_eq!(snapshot.partitions[0].state, ManagedPartitionState::Active);
    assert_eq!(snapshot.partitions[0].visible_version, 1);
    assert_eq!(snapshot.partitions[0].next_version, 2);

    Ok(())
}

#[test]
fn managed_lake_repository_rejects_duplicate_table_name() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = ManagedLakeMetaRepository::default();

    let err = {
        let mut txn = provider.begin_write("create duplicate managed lake table")?;
        let database = repository.create_database(
            txn.as_mut(),
            CreateManagedDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        repository.create_table(
            txn.as_mut(),
            CreateManagedTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            },
        )?;
        repository
            .create_table(
                txn.as_mut(),
                CreateManagedTableRequest {
                    db_id: database.db_id,
                    name: "ORDERS".to_string(),
                    keys_type: "DUP_KEYS".to_string(),
                    bucket_num: 2,
                    current_schema_id: 10,
                    state: ManagedTableState::Active,
                    kind: ManagedTableKind::Table,
                },
            )
            .expect_err("case-insensitive duplicate table name should fail")
    };

    assert!(err.to_string().contains("already exists"));

    Ok(())
}

#[test]
fn managed_txn_repository_prepare_written_visible_advances_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();

    let (table_id, partition_id) = {
        let mut txn = provider.begin_write("create managed lake objects")?;
        let database = meta_repo.create_database(
            txn.as_mut(),
            CreateManagedDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        let table = meta_repo.create_table(
            txn.as_mut(),
            CreateManagedTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            },
        )?;
        let partition = meta_repo.create_partition(txn.as_mut(), table.table_id, "orders", 1)?;
        txn.commit()?;
        (table.table_id, partition.partition_id)
    };

    let txn_id = {
        let mut txn = provider.begin_write("commit managed lake txn")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        assert_eq!(managed_txn.table_id, table_id);
        assert_eq!(managed_txn.partition_id, partition_id);
        assert_eq!(managed_txn.base_version, 1);
        assert_eq!(managed_txn.commit_version, 2);
        assert_eq!(managed_txn.state, ManagedTxnState::Prepared);
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_visible(&meta_repo, txn.as_mut(), managed_txn.txn_id)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("managed txn should persist");
    assert_eq!(loaded.state, ManagedTxnState::Visible);

    let partition = meta_repo
        .load_partition(read.as_ref(), partition_id)?
        .expect("partition should persist");
    assert_eq!(partition.visible_version, 2);
    assert_eq!(partition.next_version, 3);

    Ok(())
}

#[test]
fn managed_txn_repository_abort_does_not_advance_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();

    let (table_id, partition_id) = {
        let mut txn = provider.begin_write("create managed lake objects")?;
        let database = meta_repo.create_database(
            txn.as_mut(),
            CreateManagedDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        let table = meta_repo.create_table(
            txn.as_mut(),
            CreateManagedTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            },
        )?;
        let partition = meta_repo.create_partition(txn.as_mut(), table.table_id, "orders", 1)?;
        txn.commit()?;
        (table.table_id, partition.partition_id)
    };

    let txn_id = {
        let mut txn = provider.begin_write("abort managed lake txn")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_aborted(txn.as_mut(), managed_txn.txn_id)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("managed txn should persist");
    assert_eq!(loaded.state, ManagedTxnState::Aborted);

    let partition = meta_repo
        .load_partition(read.as_ref(), partition_id)?
        .expect("partition should persist");
    assert_eq!(partition.visible_version, 1);

    Ok(())
}

#[test]
fn managed_txn_repository_mark_written_is_retry_safe() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let (table_id, partition_id) = create_managed_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("retry mark written")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("managed txn should persist");
    assert_eq!(loaded.state, ManagedTxnState::Written);

    Ok(())
}

#[test]
fn managed_txn_repository_mark_visible_is_retry_safe() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let (table_id, partition_id) = create_managed_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("retry mark visible")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_visible(&meta_repo, txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_visible(&meta_repo, txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("managed txn should persist");
    assert_eq!(loaded.state, ManagedTxnState::Visible);
    let partition = meta_repo
        .load_partition(read.as_ref(), partition_id)?
        .expect("partition should persist");
    assert_eq!(partition.visible_version, 2);
    assert_eq!(partition.next_version, 3);

    Ok(())
}

#[test]
fn managed_txn_repository_rejects_illegal_commit_version() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let (table_id, partition_id) = create_managed_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("create invalid managed txn")?;
        let txn_id = txn.allocate_id(id_scopes::managed_txn())?;
        put_managed_txn_record(
            txn.as_mut(),
            StoredManagedTxn {
                txn_id,
                table_id,
                partition_id,
                base_version: 1,
                commit_version: 3,
                state: ManagedTxnState::Written,
                retry_at_ms: None,
                updated_at_ms: 0,
            },
        )?;
        txn.commit()?;
        txn_id
    };

    let mut txn = provider.begin_write("mark invalid managed txn visible")?;
    let err = txn_repo
        .mark_visible(&meta_repo, txn.as_mut(), txn_id)
        .expect_err("illegal commit version should fail");
    assert_eq!(err.kind(), RepositoryErrorKind::Provider);

    Ok(())
}

#[test]
fn managed_txn_repository_rejects_partition_table_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();

    let (table_id, other_partition_id) = {
        let (table_id, _) = create_managed_table_with_partition(&provider, &meta_repo)?;
        let (other_table_id, other_partition_id) =
            create_named_managed_table_with_partition(&provider, &meta_repo, "lineitem")?;
        assert_ne!(table_id, other_table_id);
        (table_id, other_partition_id)
    };

    let txn_id = {
        let mut txn = provider.begin_write("create mismatched managed txn")?;
        let txn_id = txn.allocate_id(id_scopes::managed_txn())?;
        put_managed_txn_record(
            txn.as_mut(),
            StoredManagedTxn {
                txn_id,
                table_id,
                partition_id: other_partition_id,
                base_version: 1,
                commit_version: 2,
                state: ManagedTxnState::Written,
                retry_at_ms: None,
                updated_at_ms: 0,
            },
        )?;
        txn.commit()?;
        txn_id
    };

    let mut txn = provider.begin_write("mark mismatched managed txn visible")?;
    let err = txn_repo
        .mark_visible(&meta_repo, txn.as_mut(), txn_id)
        .expect_err("partition table mismatch should fail");
    assert_eq!(err.kind(), RepositoryErrorKind::Conflict);

    Ok(())
}

#[test]
fn managed_txn_repository_rejects_partition_next_version_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let (table_id, partition_id) = create_managed_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("prepare managed txn with stale partition next")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        let (revision, mut partition) = meta_repo
            .load_versioned_partition(txn.as_ref(), partition_id)?
            .expect("partition should persist");
        partition.next_version = 99;
        meta_repo.update_partition_exact(txn.as_mut(), &partition, revision)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let mut txn = provider.begin_write("mark managed txn visible with stale partition next")?;
    let err = txn_repo
        .mark_visible(&meta_repo, txn.as_mut(), txn_id)
        .expect_err("partition next_version mismatch should fail");
    assert_eq!(err.kind(), RepositoryErrorKind::Conflict);

    Ok(())
}

#[test]
fn key_helpers_reject_unescaped_path_separators() {
    let err = MetaKey::new("managed", ["table", "bad/name"]).expect_err("slash must fail");
    assert!(
        err.to_string()
            .contains("invalid metadata key path segment")
    );
}

#[test]
fn mv_repository_creates_definition_and_target_lookup() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let created = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition
    };

    let read = provider.begin_read()?;
    let loaded = repository
        .load_by_id(read.as_ref(), created.mv_id)?
        .expect("definition should exist");
    assert_eq!(loaded.mv_id, created.mv_id);
    assert_eq!(loaded.select_sql, created.select_sql);

    let target = repository
        .find_by_target(read.as_ref(), "ICE", "Ns", "ORDERS_MV")?
        .expect("target lookup should be case-insensitive");
    assert_eq!(target.mv_id, created.mv_id);

    Ok(())
}

#[test]
fn mv_repository_refresh_intent_finalizes_once() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition.mv_id
    };

    let refresh_id = {
        let mut txn = provider.begin_write("begin mv refresh")?;
        let mut target_snapshots = BTreeMap::new();
        target_snapshots.insert("ice.ns.orders_mv".to_string(), 100);
        let refresh = repository.begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)?;
        assert!(refresh.refresh_id > 0);
        assert_eq!(refresh.mv_id, mv_id);
        assert_eq!(refresh.state, MvRefreshState::IntentCreated);
        assert_eq!(refresh.target_snapshots["ice.ns.orders_mv"], 100);
        txn.commit()?;
        refresh.refresh_id
    };

    {
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), refresh_id)?
            .expect("refresh intent should persist");
        assert_eq!(refresh.state, MvRefreshState::IntentCreated);
        assert_eq!(refresh.target_snapshots["ice.ns.orders_mv"], 100);
    }

    {
        let mut txn = provider.begin_write("record external mv commit")?;
        repository.record_external_commit_outcome(
            txn.as_mut(),
            refresh_id,
            RefreshExternalOutcome {
                target_snapshot_id: Some(200),
                commit_id: "commit-1".to_string(),
            },
        )?;
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), refresh_id)?
            .expect("refresh should exist after external commit");
        assert_eq!(refresh.state.as_str(), "EXTERNAL_COMMITTED");
        let outcome = refresh
            .external_outcome
            .expect("external outcome should persist");
        assert_eq!(outcome.target_snapshot_id, Some(200));
        assert_eq!(outcome.commit_id, "commit-1");
    }

    {
        let mut txn = provider.begin_write("finalize mv refresh")?;
        let mut base_snapshots = BTreeMap::new();
        base_snapshots.insert("iceberg.sales.orders".to_string(), 50);
        let mut base_table_uuids = BTreeMap::new();
        base_table_uuids.insert(
            "iceberg.sales.orders".to_string(),
            "uuid-orders".to_string(),
        );
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id,
                rows: 3,
                base_snapshots,
                base_table_uuids,
                target_snapshot_id: Some(200),
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("finalize mv refresh again")?;
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id,
                rows: 3,
                base_snapshots: BTreeMap::new(),
                base_table_uuids: BTreeMap::new(),
                target_snapshot_id: Some(200),
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let refresh = repository
        .load_refresh(read.as_ref(), refresh_id)?
        .expect("refresh should exist");
    assert_eq!(refresh.state, MvRefreshState::Finalized);

    let definition = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should exist");
    assert_eq!(definition.last_refresh_rows, Some(3));
    assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(200));
    assert!(!definition.refresh_in_progress);
    assert_eq!(definition.active_refresh_id, None);
    assert!(definition.refresh_target_snapshots.is_empty());

    Ok(())
}

#[test]
fn mv_repository_rejects_second_refresh_intent() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition.mv_id
    };

    let refresh_a = {
        let mut txn = provider.begin_write("begin mv refresh a")?;
        let mut target_snapshots = BTreeMap::new();
        target_snapshots.insert("ice.ns.orders_mv".to_string(), 100);
        let refresh = repository.begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)?;
        txn.commit()?;
        refresh
    };

    {
        let mut txn = provider.begin_write("begin mv refresh b")?;
        let mut target_snapshots = BTreeMap::new();
        target_snapshots.insert("ice.ns.orders_mv".to_string(), 999);
        let err = repository
            .begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)
            .expect_err("second refresh should be rejected");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    let read = provider.begin_read()?;
    let definition = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should exist");
    assert!(definition.refresh_in_progress);
    assert_eq!(definition.active_refresh_id, Some(refresh_a.refresh_id));
    assert_eq!(definition.refresh_target_snapshots["ice.ns.orders_mv"], 100);

    let persisted_refresh_a = repository
        .load_refresh(read.as_ref(), refresh_a.refresh_id)?
        .expect("refresh a should persist");
    assert_eq!(
        persisted_refresh_a.target_snapshots["ice.ns.orders_mv"],
        100
    );

    Ok(())
}

#[test]
fn mv_repository_rejects_definition_schema_version_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let key = MetaKey::new("mv", ["by-id", "1"])?;
    let payload = serde_json::json!({
        "mv_id": 1,
        "select_sql": "SELECT id FROM iceberg.sales.orders",
        "base_table_refs": ["iceberg.sales.orders"],
        "primary_key_columns": ["id"],
        "storage_engine": "iceberg",
        "target_catalog": "ice",
        "target_namespace": "ns",
        "target_table": "orders_mv",
        "last_refresh_ms": null,
        "last_refresh_rows": null,
        "last_refresh_snapshots": {},
        "last_refresh_table_uuids": {},
        "last_refreshed_iceberg_snapshot_id": null,
        "refresh_in_progress": false,
        "active_refresh_id": null,
        "refresh_target_snapshots": {},
        "created_at_ms": 11
    });

    {
        let mut txn = provider.begin_write("write mismatched mv definition")?;
        txn.put(MetaRecordPut::new(
            key,
            MetaRecordKind::new("mv.definition")?,
            ExpectedRevision::NotExists,
            encode_json_payload(999, &payload)?,
        ))?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let err = repository
        .load_by_id(read.as_ref(), 1)
        .expect_err("schema version mismatch should fail");
    assert!(
        err.to_string()
            .contains("metadata record by-id/1 has schema version 999")
    );

    Ok(())
}

#[test]
fn iceberg_catalog_repository_registers_catalog_namespace_and_table()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergCatalogMetaRepository::default();

    {
        let mut txn = provider.begin_write("register iceberg table")?;
        repository.upsert_catalog(
            txn.as_mut(),
            "ice",
            IcebergCatalogProperties {
                properties: vec![("type".to_string(), "rest".to_string())],
            },
        )?;
        repository.upsert_namespace(txn.as_mut(), "ice", "ns")?;
        repository.upsert_table(txn.as_mut(), "ice", "ns", "orders")?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    assert!(repository.catalog_exists(read.as_ref(), "ICE")?);
    assert!(repository.namespace_exists(read.as_ref(), "ice", "NS")?);
    assert!(repository.table_exists(read.as_ref(), "ICE", "ns", "ORDERS")?);

    Ok(())
}

#[test]
fn iceberg_catalog_repository_deletes_table_and_related_mv_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let catalog_repo = IcebergCatalogMetaRepository::default();
    let mv_repo = MvMetaRepository::default();

    {
        let mut txn = provider.begin_write("seed iceberg table and mv target")?;
        catalog_repo.upsert_catalog(
            txn.as_mut(),
            "ice",
            IcebergCatalogProperties {
                properties: vec![("type".to_string(), "rest".to_string())],
            },
        )?;
        catalog_repo.upsert_namespace(txn.as_mut(), "ice", "ns")?;
        catalog_repo.upsert_table(txn.as_mut(), "ice", "ns", "orders_mv")?;
        mv_repo.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("delete iceberg table and mv lookup")?;
        catalog_repo.delete_table_and_mv_relationships(
            txn.as_mut(),
            &mv_repo,
            "ICE",
            "NS",
            "ORDERS_MV",
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    assert!(!catalog_repo.table_exists(read.as_ref(), "ICE", "ns", "ORDERS_MV")?);
    assert!(
        mv_repo
            .find_by_target(read.as_ref(), "ice", "ns", "orders_mv")?
            .is_none()
    );

    Ok(())
}

#[test]
fn iceberg_catalog_repository_rejects_delete_when_target_mv_refresh_is_active()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let catalog_repo = IcebergCatalogMetaRepository::default();
    let mv_repo = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("seed active mv target")?;
        catalog_repo.upsert_catalog(
            txn.as_mut(),
            "ice",
            IcebergCatalogProperties {
                properties: vec![("type".to_string(), "rest".to_string())],
            },
        )?;
        catalog_repo.upsert_namespace(txn.as_mut(), "ice", "ns")?;
        catalog_repo.upsert_table(txn.as_mut(), "ice", "ns", "orders_mv")?;
        let definition = mv_repo.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition.mv_id
    };

    {
        let mut txn = provider.begin_write("begin mv refresh")?;
        let mut target_snapshots = BTreeMap::new();
        target_snapshots.insert("ice.ns.orders_mv".to_string(), 100);
        mv_repo.begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("delete active mv target")?;
        let err = catalog_repo
            .delete_table_and_mv_relationships(txn.as_mut(), &mv_repo, "ICE", "NS", "ORDERS_MV")
            .expect_err("active refresh should block target deletion");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    let read = provider.begin_read()?;
    assert!(catalog_repo.table_exists(read.as_ref(), "ICE", "ns", "ORDERS_MV")?);
    assert!(
        mv_repo
            .find_by_target(read.as_ref(), "ice", "ns", "orders_mv")?
            .is_some()
    );
    let definition = mv_repo
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should be preserved");
    assert!(definition.refresh_in_progress);
    assert!(definition.active_refresh_id.is_some());

    Ok(())
}

#[test]
fn mv_repository_rejects_stale_target_lookup_without_deleting_wrong_definition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("seed mismatched mv lookup")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("other_mv".to_string()),
                created_at_ms: 11,
            },
        )?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("mv", ["by-target", "ice", "ns", "orders_mv"])?,
            MetaRecordKind::new("mv.target_lookup")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                1,
                &MvTargetLookup {
                    mv_id: definition.mv_id,
                },
            )?,
        ))?;
        txn.commit()?;
        definition.mv_id
    };

    {
        let mut txn = provider.begin_write("drop stale target lookup")?;
        let err = repository
            .drop_by_target(txn.as_mut(), "ice", "ns", "orders_mv")
            .expect_err("mismatched lookup should be rejected");
        assert_eq!(err.kind(), RepositoryErrorKind::Provider);
    }

    let read = provider.begin_read()?;
    let err = repository
        .find_by_target(read.as_ref(), "ice", "ns", "orders_mv")
        .expect_err("mismatched lookup read should be rejected");
    assert_eq!(err.kind(), RepositoryErrorKind::Provider);
    assert!(repository.load_by_id(read.as_ref(), mv_id)?.is_some());
    assert!(
        repository
            .find_by_target(read.as_ref(), "ice", "ns", "other_mv")?
            .is_some()
    );

    Ok(())
}

#[test]
fn iceberg_catalog_repository_rejects_wrong_kind_and_schema_in_exists_apis()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergCatalogMetaRepository::default();

    {
        let mut txn = provider.begin_write("write invalid iceberg metadata records")?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("iceberg_catalog", ["catalog", "ice"])?,
            MetaRecordKind::new("iceberg.namespace")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                1,
                &IcebergCatalogProperties {
                    properties: vec![("type".to_string(), "rest".to_string())],
                },
            )?,
        ))?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("iceberg_catalog", ["namespace", "ice", "bad_schema"])?,
            MetaRecordKind::new("iceberg.namespace")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                999,
                &serde_json::json!({
                    "catalog": "ice",
                    "namespace": "bad_schema"
                }),
            )?,
        ))?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("iceberg_catalog", ["table", "ice", "ns", "orders"])?,
            MetaRecordKind::new("iceberg.catalog")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                1,
                &serde_json::json!({
                    "catalog": "ice",
                    "namespace": "ns",
                    "table": "orders"
                }),
            )?,
        ))?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("iceberg_catalog", ["table", "ice", "ns", "bad_schema"])?,
            MetaRecordKind::new("iceberg.table_registration")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                999,
                &serde_json::json!({
                    "catalog": "ice",
                    "namespace": "ns",
                    "table": "bad_schema"
                }),
            )?,
        ))?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let err = repository
        .catalog_exists(read.as_ref(), "ice")
        .expect_err("wrong catalog kind should fail");
    assert!(
        err.to_string()
            .contains("metadata record catalog/ice has kind iceberg.namespace")
    );

    let err = repository
        .namespace_exists(read.as_ref(), "ice", "bad_schema")
        .expect_err("wrong namespace schema should fail");
    assert!(
        err.to_string()
            .contains("metadata record namespace/ice/bad_schema has schema version 999")
    );

    let err = repository
        .table_exists(read.as_ref(), "ice", "ns", "orders")
        .expect_err("wrong table kind should fail");
    assert!(
        err.to_string()
            .contains("metadata record table/ice/ns/orders has kind iceberg.catalog")
    );

    let err = repository
        .table_exists(read.as_ref(), "ice", "ns", "bad_schema")
        .expect_err("wrong table schema should fail");
    assert!(
        err.to_string()
            .contains("metadata record table/ice/ns/bad_schema has schema version 999")
    );

    Ok(())
}
