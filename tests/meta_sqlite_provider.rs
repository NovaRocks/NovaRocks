mod common;

use common::meta_provider_conformance as conformance;
use novarocks::meta::SqliteMetaStoreProvider;

type TestResult = Result<(), Box<dyn std::error::Error>>;
type SqliteProviderFixture = (tempfile::TempDir, SqliteMetaStoreProvider);

fn new_sqlite_provider() -> Result<SqliteProviderFixture, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    Ok((dir, provider))
}

#[test]
fn sqlite_provider_put_not_exists_commits_visible_record() -> TestResult {
    conformance::put_not_exists_commits_visible_record(new_sqlite_provider)
}

#[test]
fn sqlite_provider_exact_revision_updates_record_and_advances_revision() -> TestResult {
    conformance::exact_revision_updates_record_and_advances_revision(new_sqlite_provider)
}

#[test]
fn sqlite_provider_delete_exists_hides_committed_record() -> TestResult {
    conformance::delete_exists_hides_committed_record(new_sqlite_provider)
}

#[test]
fn sqlite_provider_scan_prefix_returns_records_in_key_order() -> TestResult {
    conformance::scan_prefix_returns_records_in_key_order(new_sqlite_provider)
}

#[test]
fn sqlite_provider_allocate_id_is_scoped_and_persists_after_commit() -> TestResult {
    conformance::allocate_id_is_scoped_and_persists_after_commit(new_sqlite_provider)
}

#[test]
fn sqlite_provider_put_exists_updates_existing_and_rejects_missing_record() -> TestResult {
    conformance::put_exists_updates_existing_and_rejects_missing_record(new_sqlite_provider)
}

#[test]
fn sqlite_provider_read_txn_keeps_snapshot_from_begin() -> TestResult {
    conformance::read_txn_keeps_snapshot_from_begin(new_sqlite_provider)
}

#[test]
fn sqlite_provider_abort_discards_record_and_id_mutations() -> TestResult {
    conformance::abort_discards_record_and_id_mutations(new_sqlite_provider)
}

#[test]
fn sqlite_provider_stale_exact_revision_returns_conflict() -> TestResult {
    conformance::stale_exact_revision_returns_conflict(new_sqlite_provider)
}

#[test]
fn sqlite_provider_any_upserts_missing_and_existing_records() -> TestResult {
    conformance::any_upserts_missing_and_existing_records(new_sqlite_provider)
}
