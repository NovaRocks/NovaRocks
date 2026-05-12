use std::collections::BTreeMap;

use novarocks::meta::repository::mv::{
    CreateMvDefinitionRequest, MvMetaRepository, MvRefreshFinalizeRequest, RefreshExternalOutcome,
};
use novarocks::meta::{MetaStoreProvider, SqliteMetaStoreProvider};

#[test]
fn refresh_transaction_can_recover_after_external_commit_before_finalize()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repo = MvMetaRepository::default();

    let mut txn = provider.begin_write("create mv and intent")?;
    let mv = repo.create_definition(
        &mut *txn,
        CreateMvDefinitionRequest {
            select_sql: "select id from ice.ns.orders".to_string(),
            base_table_refs: vec!["ice.ns.orders".to_string()],
            primary_key_columns: vec!["id".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("ns".to_string()),
            target_table: Some("orders_mv".to_string()),
            created_at_ms: 1,
        },
    )?;
    let intent = repo.begin_refresh_intent(&mut *txn, mv.mv_id, BTreeMap::new())?;
    txn.commit()?;

    let mut txn = provider.begin_write("record external only")?;
    repo.record_external_commit_outcome(
        &mut *txn,
        intent.refresh_id,
        RefreshExternalOutcome {
            target_snapshot_id: Some(42),
            commit_id: "snapshot-42".to_string(),
        },
    )?;
    txn.commit()?;

    let mut txn = provider.begin_write("recover finalize")?;
    repo.finalize_refresh(
        &mut *txn,
        MvRefreshFinalizeRequest {
            refresh_id: intent.refresh_id,
            rows: 10,
            base_snapshots: BTreeMap::new(),
            base_table_uuids: BTreeMap::new(),
            target_snapshot_id: Some(42),
        },
    )?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let mv_after = repo.load_by_id(&*read, mv.mv_id)?.expect("mv");
    assert_eq!(mv_after.last_refreshed_iceberg_snapshot_id, Some(42));
    Ok(())
}
