use std::error::Error;

use bytes::Bytes;
use novarocks::meta::{
    ExpectedRevision, IdScope, MetaErrorKind, MetaKey, MetaKeyPrefix, MetaPayload, MetaRecordKind,
    MetaRecordPut, MetaStoreProvider,
};

type TestResult = Result<(), Box<dyn Error>>;

#[allow(dead_code)]
pub fn run_meta_provider_conformance<P, G, F>(mut new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnMut() -> Result<(G, P), Box<dyn Error>>,
{
    put_not_exists_commits_visible_record(|| new_provider())?;
    exact_revision_updates_record_and_advances_revision(|| new_provider())?;
    delete_exists_hides_committed_record(|| new_provider())?;
    scan_prefix_returns_records_in_key_order(|| new_provider())?;
    allocate_id_is_scoped_and_persists_after_commit(|| new_provider())?;
    put_exists_updates_existing_and_rejects_missing_record(|| new_provider())?;
    read_txn_keeps_snapshot_from_begin(|| new_provider())?;
    abort_discards_record_and_id_mutations(|| new_provider())?;
    stale_exact_revision_returns_conflict(|| new_provider())?;
    any_upserts_missing_and_existing_records(|| new_provider())?;
    Ok(())
}

pub fn put_not_exists_commits_visible_record<P, G, F>(new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let key = MetaKey::new("mv", ["by-id", "123"])?;
    let kind = MetaRecordKind::new("mv.definition")?;
    let payload = MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv1"}"#));

    {
        let mut txn = provider.begin_write("create mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind.clone(),
            ExpectedRevision::NotExists,
            payload.clone(),
        ))?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let record = read.get(&key)?.expect("record should be visible");
    assert_eq!(record.key, key);
    assert_eq!(record.kind, kind);
    assert_eq!(record.payload, payload);
    assert!(record.created_at_ms > 0);
    assert_eq!(record.created_at_ms, record.updated_at_ms);
    Ok(())
}

pub fn exact_revision_updates_record_and_advances_revision<P, G, F>(new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let key = MetaKey::new("mv", ["by-id", "123"])?;
    let kind = MetaRecordKind::new("mv.definition")?;
    let initial_payload = MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv1"}"#));

    {
        let mut txn = provider.begin_write("create mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind.clone(),
            ExpectedRevision::NotExists,
            initial_payload,
        ))?;
        txn.commit()?;
    }

    let first = provider
        .begin_read()?
        .get(&key)?
        .expect("record should exist");
    let updated_payload = MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv2"}"#));

    {
        let mut txn = provider.begin_write("update mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind,
            ExpectedRevision::Exact(first.revision.clone()),
            updated_payload.clone(),
        ))?;
        txn.commit()?;
    }

    let second = provider
        .begin_read()?
        .get(&key)?
        .expect("record should still exist");
    assert_eq!(second.payload, updated_payload);
    assert_ne!(second.revision, first.revision);
    assert_eq!(second.created_at_ms, first.created_at_ms);
    assert!(second.updated_at_ms >= first.updated_at_ms);
    Ok(())
}

pub fn delete_exists_hides_committed_record<P, G, F>(new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let key = MetaKey::new("mv", ["by-id", "123"])?;
    let kind = MetaRecordKind::new("mv.definition")?;
    let payload = MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv1"}"#));

    {
        let mut txn = provider.begin_write("create mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind,
            ExpectedRevision::NotExists,
            payload,
        ))?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("delete mv")?;
        txn.delete(&key, ExpectedRevision::Exists)?;
        txn.commit()?;
    }

    assert!(provider.begin_read()?.get(&key)?.is_none());
    Ok(())
}

pub fn scan_prefix_returns_records_in_key_order<P, G, F>(new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let kind = MetaRecordKind::new("mv.definition")?;

    {
        let mut txn = provider.begin_write("seed mv records")?;
        for key in [
            MetaKey::new("mv", ["by-id", "002"])?,
            MetaKey::new("mv", ["by-target", "ice", "db", "mv1"])?,
            MetaKey::new("mv", ["by-id", "001"])?,
        ] {
            txn.put(MetaRecordPut::new(
                key,
                kind.clone(),
                ExpectedRevision::NotExists,
                MetaPayload::json(1, Bytes::from_static(br#"{}"#)),
            ))?;
        }
        txn.commit()?;
    }

    let prefix = MetaKeyPrefix::new("mv", ["by-id"])?;
    let records = provider.begin_read()?.scan(&prefix, None)?;
    let paths = records
        .iter()
        .map(|record| record.key.canonical_path())
        .collect::<Vec<_>>();
    assert_eq!(paths, vec!["by-id/001", "by-id/002"]);
    Ok(())
}

pub fn allocate_id_is_scoped_and_persists_after_commit<P, G, F>(new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let mv_scope = IdScope::new("mv.id")?;
    let job_scope = IdScope::new("job.erase")?;

    {
        let mut txn = provider.begin_write("allocate ids")?;
        assert_eq!(txn.allocate_id(mv_scope.clone())?, 1);
        assert_eq!(txn.allocate_id(mv_scope.clone())?, 2);
        assert_eq!(txn.allocate_id(job_scope.clone())?, 1);
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("allocate more ids")?;
        assert_eq!(txn.allocate_id(mv_scope)?, 3);
        assert_eq!(txn.allocate_id(job_scope)?, 2);
        txn.commit()?;
    }

    Ok(())
}

pub fn put_exists_updates_existing_and_rejects_missing_record<P, G, F>(
    new_provider: F,
) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let key = MetaKey::new("mv", ["by-id", "123"])?;
    let missing_key = MetaKey::new("mv", ["by-id", "missing"])?;
    let kind = MetaRecordKind::new("mv.definition")?;

    {
        let mut txn = provider.begin_write("create mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind.clone(),
            ExpectedRevision::NotExists,
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv1"}"#)),
        ))?;
        txn.commit()?;
    }

    let missing_err = {
        let mut txn = provider.begin_write("update missing mv")?;
        txn.put(MetaRecordPut::new(
            missing_key,
            kind.clone(),
            ExpectedRevision::Exists,
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"missing"}"#)),
        ))
        .expect_err("missing Exists update should fail")
    };
    assert_eq!(missing_err.kind(), MetaErrorKind::NotFound);

    {
        let mut txn = provider.begin_write("update existing mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind,
            ExpectedRevision::Exists,
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv2"}"#)),
        ))?;
        txn.commit()?;
    }

    let record = provider
        .begin_read()?
        .get(&key)?
        .expect("record should exist after Exists update");
    assert_eq!(
        record.payload.bytes,
        Bytes::from_static(br#"{"name":"mv2"}"#)
    );
    Ok(())
}

pub fn read_txn_keeps_snapshot_from_begin<P, G, F>(new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let key = MetaKey::new("mv", ["by-id", "123"])?;
    let kind = MetaRecordKind::new("mv.definition")?;
    let read = provider.begin_read()?;

    {
        let mut txn = provider.begin_write("create mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind,
            ExpectedRevision::NotExists,
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv1"}"#)),
        ))?;
        txn.commit()?;
    }

    assert!(read.get(&key)?.is_none());
    assert!(provider.begin_read()?.get(&key)?.is_some());
    Ok(())
}

pub fn abort_discards_record_and_id_mutations<P, G, F>(new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let key = MetaKey::new("mv", ["by-id", "123"])?;
    let kind = MetaRecordKind::new("mv.definition")?;
    let scope = IdScope::new("mv.id")?;

    {
        let mut txn = provider.begin_write("abort mv create")?;
        assert_eq!(txn.allocate_id(scope.clone())?, 1);
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind,
            ExpectedRevision::NotExists,
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv1"}"#)),
        ))?;
        txn.abort()?;
    }

    assert!(provider.begin_read()?.get(&key)?.is_none());
    {
        let mut txn = provider.begin_write("allocate after abort")?;
        assert_eq!(txn.allocate_id(scope)?, 1);
        txn.commit()?;
    }
    Ok(())
}

pub fn stale_exact_revision_returns_conflict<P, G, F>(new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let key = MetaKey::new("mv", ["by-id", "123"])?;
    let kind = MetaRecordKind::new("mv.definition")?;

    {
        let mut txn = provider.begin_write("create mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind.clone(),
            ExpectedRevision::NotExists,
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv1"}"#)),
        ))?;
        txn.commit()?;
    }
    let first = provider
        .begin_read()?
        .get(&key)?
        .expect("record should exist");

    {
        let mut txn = provider.begin_write("update mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind.clone(),
            ExpectedRevision::Exists,
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv2"}"#)),
        ))?;
        txn.commit()?;
    }

    let err = {
        let mut txn = provider.begin_write("stale update")?;
        txn.put(MetaRecordPut::new(
            key,
            kind,
            ExpectedRevision::Exact(first.revision),
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv3"}"#)),
        ))
        .expect_err("stale Exact update should fail")
    };
    assert_eq!(err.kind(), MetaErrorKind::Conflict);
    Ok(())
}

pub fn any_upserts_missing_and_existing_records<P, G, F>(new_provider: F) -> TestResult
where
    P: MetaStoreProvider,
    F: FnOnce() -> Result<(G, P), Box<dyn Error>>,
{
    let (_guard, provider) = new_provider()?;
    let key = MetaKey::new("mv", ["by-id", "123"])?;
    let kind = MetaRecordKind::new("mv.definition")?;

    {
        let mut txn = provider.begin_write("upsert missing mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind.clone(),
            ExpectedRevision::Any,
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv1"}"#)),
        ))?;
        txn.commit()?;
    }
    {
        let mut txn = provider.begin_write("upsert existing mv")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind,
            ExpectedRevision::Any,
            MetaPayload::json(1, Bytes::from_static(br#"{"name":"mv2"}"#)),
        ))?;
        txn.commit()?;
    }

    let record = provider
        .begin_read()?
        .get(&key)?
        .expect("record should exist");
    assert_eq!(
        record.payload.bytes,
        Bytes::from_static(br#"{"name":"mv2"}"#)
    );
    Ok(())
}
