use std::collections::BTreeMap;

use bytes::Bytes;
use novarocks::meta::repository::iceberg_catalog::{
    IcebergCatalogMetaRepository, IcebergCatalogProperties,
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
fn repository_error_display_is_domain_facing() {
    let err = RepositoryError::conflict("managed txn state changed");
    assert_eq!(
        err.to_string(),
        "metadata repository conflict: managed txn state changed"
    );
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
