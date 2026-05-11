use std::collections::BTreeMap;

use bytes::Bytes;
use novarocks::meta::repository::mv::{
    CreateMvDefinitionRequest, MvMetaRepository, MvRefreshFinalizeRequest, MvRefreshState,
    RefreshExternalOutcome,
};
use novarocks::meta::repository::{
    RepositoryError, decode_json_payload, encode_json_payload, id_scopes,
};
use novarocks::meta::{MetaKey, MetaStoreProvider, SqliteMetaStoreProvider};
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
    assert!(definition.refresh_target_snapshots.is_empty());

    Ok(())
}
