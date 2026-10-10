// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `ALTER TABLE x EXPIRE SNAPSHOTS` — drops obsolete snapshots from
//! metadata.json and physically deletes their orphan files.
//!
//! Algorithm (standard Iceberg `expireSnapshots`):
//!
//! 1. Compute the PROTECTED set: the current snapshot of every branch / tag
//!    ref, plus the most-recent `retain_last` snapshots of the main ancestor
//!    chain (default keep = 1 so the table is never left headless).
//! 2. `candidates` = snapshots NOT protected. This prunes OLD snapshots on a
//!    normal linearly appended main chain (not just dangling snapshots).
//! 3. Apply `OLDER THAN`: retain only those with `timestamp_ms < threshold`.
//! 4. If candidates empty → early return Ok (no metadata write).
//! 5. Enumerate files for candidates; protect files referenced by all
//!    remaining (non-candidate) snapshots.
//! 6. Puffin half-reference protection.
//! 7. Commit `TableUpdate::RemoveSnapshots` via `commit_with_retry`.
//!    The vendored iceberg-rs builder (`table_metadata_builder.rs`) auto-prunes
//!    refs whose snapshot was removed and (`update_snapshot_log`) truncates the
//!    snapshot_log. Because the current snapshot of every ref is protected, no
//!    live ref snapshot is removed and refs are never auto-pruned; the current
//!    snapshot stays the last snapshot_log entry, so `build()` succeeds even
//!    when mid-chain snapshots are removed.
//! 8. Best-effort physical delete each path in `to_delete`.

use crate as novarocks_connector_iceberg;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use novarocks_connector_iceberg::iceberg::io::FileIO;
use novarocks_connector_iceberg::iceberg::spec::{Snapshot, TableMetadata};
use novarocks_connector_iceberg::iceberg::{
    Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate,
};

use novarocks_connector_iceberg::commit::commit_with_retry;
use novarocks_connector_iceberg::commit::{
    FileSet, enumerate_files_for_snapshots, puffin_half_reference_protection,
};

/// Parameters for an EXPIRE SNAPSHOTS operation.
/// At least one of `older_than_ms` or `retain_last` must be `Some`
/// (enforced by the parser; this struct does not re-validate).
pub struct ExpireParams {
    /// Epoch-ms threshold. Only snapshots with `timestamp_ms < older_than_ms`
    /// are candidates for expiry (in conjunction with other filters).
    pub older_than_ms: Option<i64>,
    /// Protect at most the N most-recent snapshots in the main ancestor chain
    /// from expiry. N must be >= 1 if `Some` (enforced by parser).
    pub retain_last: Option<u32>,
}

/// Result of a successful EXPIRE SNAPSHOTS execution.
#[derive(Debug)]
pub struct ExpireOutcome {
    /// Number of snapshot metadata entries removed from the table.
    pub expired_snapshot_count: usize,
    /// Number of physical files successfully deleted from object storage.
    pub deleted_file_count: usize,
}

/// Top-level entry point called from `engine::iceberg_maintenance`.
///
/// Wraps `commit_with_retry` for OCC retries (spec §2.2). The outcome
/// (expired / deleted counts) is captured via shared state since
/// `commit_with_retry` returns `()`.
pub async fn run_expire_snapshots(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    params: ExpireParams,
) -> Result<ExpireOutcome, String> {
    run_expire_snapshots_with_marker(catalog, table_ident, params, None).await
}

/// Executes snapshot expiry while atomically carrying the provider-owned
/// operation marker in the same metadata commit.
pub async fn run_expire_snapshots_with_marker(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    params: ExpireParams,
    marker: Option<String>,
) -> Result<ExpireOutcome, String> {
    use std::sync::Mutex;
    // Shared state to capture the outcome across the retry closure boundary.
    let outcome: Arc<Mutex<Option<ExpireOutcome>>> = Arc::new(Mutex::new(None));
    let outcome_clone = outcome.clone();
    let older = params.older_than_ms;
    let retain = params.retain_last;
    commit_with_retry(move |_attempt| {
        let outcome_inner = outcome_clone.clone();
        let catalog = catalog.clone();
        let table_ident = table_ident.clone();
        let marker = marker.clone();
        async move {
            let res =
                run_expire_one_attempt(&catalog, &table_ident, older, retain, marker.as_deref())
                    .await?;
            *outcome_inner.lock().unwrap() = Some(res);
            Ok(())
        }
    })
    .await?;
    Ok(outcome.lock().unwrap().take().unwrap_or(ExpireOutcome {
        expired_snapshot_count: 0,
        deleted_file_count: 0,
    }))
}

/// Runs one frozen-plan attempt without the legacy OCC retry loop.
pub async fn run_expire_snapshots_once_with_marker(
    catalog: Arc<dyn Catalog>,
    table: novarocks_connector_iceberg::iceberg::table::Table,
    file_io: FileIO,
    params: ExpireParams,
    marker: Option<String>,
) -> Result<ExpireOutcome, novarocks_connector_iceberg::iceberg::Error> {
    run_expire_with_table(
        &catalog,
        table,
        file_io,
        params.older_than_ms,
        params.retain_last,
        marker.as_deref(),
    )
    .await
}

/// Single attempt body; returns an `novarocks_connector_iceberg::iceberg::Error` on failure so
/// `commit_with_retry` can classify it as retryable or not.
async fn run_expire_one_attempt(
    catalog: &Arc<dyn Catalog>,
    table_ident: &TableIdent,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
    marker: Option<&str>,
) -> Result<ExpireOutcome, novarocks_connector_iceberg::iceberg::Error> {
    let table = catalog.load_table(table_ident).await?;
    let file_io = table.file_io().clone();
    run_expire_with_table(catalog, table, file_io, older_than_ms, retain_last, marker).await
}

/// Run one snapshot expiry from a caller-bound table and FileIO. The catalog
/// remains the metadata commit owner; all manifest reads and file deletion use
/// the supplied action-scoped FileIO.
async fn run_expire_with_table(
    catalog: &Arc<dyn Catalog>,
    table: novarocks_connector_iceberg::iceberg::table::Table,
    file_io: FileIO,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
    marker: Option<&str>,
) -> Result<ExpireOutcome, novarocks_connector_iceberg::iceberg::Error> {
    let metadata = table.metadata();

    // Compute candidate snapshot ids (standard Iceberg expireSnapshots:
    // everything not protected by a ref or the main retain window).
    let candidates = compute_expire_candidates(metadata, older_than_ms, retain_last);

    // No candidates → early return, no metadata change.
    if candidates.is_empty() {
        return Ok(ExpireOutcome {
            expired_snapshot_count: 0,
            deleted_file_count: 0,
        });
    }

    // Enumerate files referenced by the candidate snapshots (algorithm step 5).
    let candidate_set: HashSet<i64> = candidates.iter().copied().collect();
    let files_for_candidates =
        enumerate_files_for_snapshots(&file_io, metadata, &candidate_set).await?;

    let all_snapshot_ids: HashSet<i64> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
    let protected_snapshots: HashSet<i64> = all_snapshot_ids
        .difference(&candidate_set)
        .copied()
        .collect();
    let protected_files =
        enumerate_files_for_snapshots(&file_io, metadata, &protected_snapshots).await?;

    let mut to_delete: FileSet = files_for_candidates
        .difference(&protected_files)
        .cloned()
        .collect();

    // Puffin half-reference protection (algorithm step 6).
    // NOTE(R7 spike): `DataFile::referenced_data_file()` is pub in iceberg-0.9.0
    // (`vendor/iceberg-0.9.0/src/spec/manifest/data_file.rs:276`), so the full
    // DV index can be built. If it were absent, we would return an empty index
    // (puffin protection becomes no-op for non-puffin candidates).
    let dv_index = build_dv_index_from_metadata(metadata, &file_io, &all_snapshot_ids).await?;
    puffin_half_reference_protection(&mut to_delete, &dv_index, &protected_files);

    // Commit the metadata change via OCC (algorithm step 7).
    // The `RemoveSnapshots` update tells the catalog to drop the listed snapshot
    // entries from metadata. The iceberg-rs builder automatically:
    //   - prunes refs whose snapshot id was removed (table_metadata_builder.rs),
    //   - truncates snapshot_log entries (update_snapshot_log — called during build()).
    // Because the current snapshot of every ref is protected (never a candidate),
    // no live ref snapshot id is removed, so no refs are auto-pruned and the
    // current snapshot remains the last snapshot_log entry (build() succeeds even
    // when mid-chain snapshots are removed).
    //
    // OCC requirements guard against concurrent EXPIRE races:
    //   - CurrentSchemaIdMatch: ensures schema has not changed since we read metadata.
    //   - RefSnapshotIdMatch on "main": ensures the main branch has not advanced
    //     (a concurrent EXPIRE or append could have changed it).
    //     Tables without a "main" ref (empty / branch-only tables) pin schema-id alone.
    let mut requirements = vec![TableRequirement::CurrentSchemaIdMatch {
        current_schema_id: metadata.current_schema_id(),
    }];
    if let Some(main_ref) = metadata.refs().get("main") {
        requirements.push(TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".to_string(),
            snapshot_id: Some(main_ref.snapshot_id),
        });
    }
    let mut updates = vec![TableUpdate::RemoveSnapshots {
        snapshot_ids: candidates.clone(),
    }];
    if let Some(marker) = marker {
        updates.push(TableUpdate::SetProperties {
            updates: [(
                "novarocks.connector.maintenance.v1".to_string(),
                marker.to_string(),
            )]
            .into_iter()
            .collect(),
        });
    }
    let commit = TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(updates)
        .requirements(requirements)
        .build();
    catalog.update_table(commit).await?;

    // Best-effort physical delete (algorithm step 8).
    let deleted_file_count = best_effort_delete_files(&file_io, &to_delete).await;

    Ok(ExpireOutcome {
        expired_snapshot_count: candidates.len(),
        deleted_file_count,
    })
}

/// Compute the set of snapshot ids eligible for expiry (standard Iceberg
/// `expireSnapshots` semantics).
///
/// A snapshot is EXPIRABLE iff it is NOT protected, where the protected set is:
///   * the current snapshot of EVERY branch / tag ref, plus
///   * the most-recent `retain_last` snapshots of the main ancestor chain.
///
/// When `older_than_ms` is set, only protected-complement snapshots strictly
/// older than the threshold are expired. Genuine danglers (in no ref and
/// outside the main retain window) are not protected, so they remain expirable.
///
/// Default keep = 1 so the table is never left headless even when the caller
/// passes no `retain_last`. This prunes old snapshots on a normal linearly
/// appended main chain, which the previous dangling-only logic never did.
pub(crate) fn compute_expire_candidates(
    metadata: &TableMetadata,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
) -> Vec<i64> {
    // Protect the current snapshot of EVERY ref (branches AND tags).
    let mut protected: HashSet<i64> = metadata.refs().values().map(|r| r.snapshot_id).collect();
    // Protect the most-recent `retain_last` snapshots of the main ancestor
    // chain. Default keep = 1 so the table is never left headless even when
    // the caller passes no retain_last.
    let keep = retain_last.map(|n| n as usize).unwrap_or(1).max(1);
    for id in main_ancestor_chain(metadata, keep) {
        protected.insert(id);
    }
    metadata
        .snapshots()
        .map(|s| s.as_ref())
        .filter(|s| !protected.contains(&s.snapshot_id()))
        .filter(|s| older_than_ms.is_none_or(|t| s.timestamp_ms() < t))
        .map(|s| s.snapshot_id())
        .collect()
}

/// Walk the main ref's parent chain (newest-first) and return up to `n` ids.
///
/// Returns an empty vec if there is no "main" ref in the table metadata.
/// This is defensive — a table without a main ref is unusual but valid.
fn main_ancestor_chain(metadata: &TableMetadata, n: usize) -> Vec<i64> {
    let Some(main_ref) = metadata.refs().get("main") else {
        return Vec::new();
    };
    let snapshot_by_id: HashMap<i64, &Snapshot> = metadata
        .snapshots()
        .map(|s| (s.snapshot_id(), s.as_ref()))
        .collect();
    let mut chain: Vec<i64> = Vec::new();
    let mut sid = Some(main_ref.snapshot_id);
    while let Some(id) = sid {
        chain.push(id);
        sid = snapshot_by_id.get(&id).and_then(|s| s.parent_snapshot_id());
    }
    // Already in newest-first order (main → parent → grandparent).
    chain.into_iter().take(n).collect()
}

/// Build a DV index (puffin path → set of referenced data file paths) by
/// scanning manifest entries across the given `snapshot_ids`.
///
/// Only entries whose `data_file().referenced_data_file()` is `Some(...)` are
/// indexed (i.e., DV puffin delete files that track a specific data file).
///
/// Spike result (R7): `DataFile::referenced_data_file()` is public in
/// iceberg-0.9.0 (`vendor/iceberg-0.9.0/src/spec/manifest/data_file.rs:276`).
/// If a future upgrade hides this getter, return an empty HashMap and remove
/// this call site — puffin protection degrades to no-op (conservative, not
/// incorrect; DV puffins for non-live snapshots may linger but won't corrupt
/// data).
async fn build_dv_index_from_metadata(
    metadata: &TableMetadata,
    file_io: &FileIO,
    snapshot_ids: &HashSet<i64>,
) -> Result<HashMap<String, HashSet<String>>, novarocks_connector_iceberg::iceberg::Error> {
    let mut idx: HashMap<String, HashSet<String>> = HashMap::new();
    for sid in snapshot_ids {
        let Some(snapshot) = metadata.snapshot_by_id(*sid) else {
            continue;
        };
        let manifest_list = snapshot.load_manifest_list(file_io, metadata).await?;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                let df = entry.data_file();
                if let Some(ref_data_file) = df.referenced_data_file() {
                    idx.entry(df.file_path().to_string())
                        .or_default()
                        .insert(ref_data_file);
                }
            }
        }
    }
    Ok(idx)
}

/// Delete each path in `files` via `file_io.delete`, logging warnings on
/// failure. Returns the number of successfully deleted files.
///
/// Failures are NOT propagated — the metadata commit has already landed and
/// rolling back is not possible. Best-effort is the correct semantic per spec
/// §6 ("physical delete failure: log::warn, no rollback").
async fn best_effort_delete_files(file_io: &FileIO, files: &FileSet) -> usize {
    let mut deleted = 0;
    for path in files {
        match file_io.delete(path).await {
            Ok(()) => deleted += 1,
            Err(e) => {
                tracing::warn!(
                    path = %path,
                    error = %e,
                    "expire_snapshots: best-effort file delete failed"
                );
            }
        }
    }
    deleted
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    use super::*;
    use novarocks_connector_iceberg::commit::compute_live_snapshot_set;
    use novarocks_connector_iceberg::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadata,
        TableMetadataBuilder, Type,
    };
    use novarocks_connector_iceberg::iceberg::transaction::{ApplyTransactionAction, Transaction};
    use novarocks_connector_iceberg::iceberg::{NamespaceIdent, TableCreation};

    struct LocalCatalogFixture {
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        _warehouse: tempfile::TempDir,
    }

    fn local_test_binding() -> novarocks_connector_iceberg::access_binding::IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        novarocks_connector_iceberg::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }

    fn build_test_metadata_with_snapshots(
        snapshots: Vec<(i64, Option<i64>)>,
        refs: Vec<(&str, i64)>,
    ) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("build schema");

        let mut builder = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "/tmp/test_table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("TableMetadataBuilder::new");

        for (sequence_number, (snapshot_id, parent_snapshot_id)) in
            snapshots.into_iter().enumerate()
        {
            let sequence_number = sequence_number as i64 + 1;
            let snapshot = Snapshot::builder()
                .with_snapshot_id(snapshot_id)
                .with_parent_snapshot_id(parent_snapshot_id)
                .with_sequence_number(sequence_number)
                .with_timestamp_ms(1_700_000_000_000i64 + sequence_number * 1000)
                .with_manifest_list(format!(
                    "/tmp/test_table/metadata/snap-{snapshot_id}-ml.avro"
                ))
                .with_summary(Summary {
                    operation: novarocks_connector_iceberg::iceberg::spec::Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build();
            builder = builder.add_snapshot(snapshot).expect("add_snapshot");
        }

        for (ref_name, snapshot_id) in refs {
            builder = builder
                .set_ref(
                    ref_name,
                    SnapshotReference::new(
                        snapshot_id,
                        SnapshotRetention::branch(None, None, None),
                    ),
                )
                .expect("set_ref");
        }

        builder.build().expect("build").metadata
    }

    async fn empty_local_v3_table() -> LocalCatalogFixture {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let binding = local_test_binding();
        let catalog: Arc<dyn Catalog> = Arc::new(
            novarocks_connector_iceberg::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
                novarocks_connector_iceberg::fs_io::build_file_io_for_location(
                    &warehouse_uri,
                    binding.clone(),
                ),
                warehouse_uri,
                binding,
            ),
        );
        let namespace = NamespaceIdent::new("db".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("build schema");
        let table_ident = TableIdent::new(namespace.clone(), "t".to_string());
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(FormatVersion::V3)
                    .build(),
            )
            .await
            .expect("create table");

        LocalCatalogFixture {
            catalog,
            table_ident,
            _warehouse: warehouse,
        }
    }

    async fn append_empty_snapshot(fixture: &LocalCatalogFixture) {
        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("load table before append");
        let transaction = Transaction::new(&table);
        let transaction = transaction
            .fast_append()
            .set_snapshot_properties(HashMap::from([(
                "novarocks.test.empty-append".to_string(),
                "true".to_string(),
            )]))
            .apply(transaction)
            .expect("stage empty fast append");
        transaction
            .commit(fixture.catalog.as_ref())
            .await
            .expect("commit empty fast append");
    }

    // ---- compute_expire_candidates tests (graph-shape, no I/O) ----

    #[test]
    fn expire_prunes_old_main_chain_beyond_retain() {
        // s1 <- s2 <- s3 <- s4 (main); all live on the main ancestor chain.
        // Standard Iceberg expireSnapshots with RETAIN LAST 1 must prune every
        // main-chain snapshot except the retained head (s4).
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2)), (4, Some(3))],
            vec![("main", 4)],
        );
        let mut candidates = compute_expire_candidates(&metadata, Some(i64::MAX), Some(1));
        candidates.sort();
        assert_eq!(
            candidates,
            vec![1, 2, 3],
            "RETAIN LAST 1 keeps only the main head; older main-chain snapshots expire"
        );
    }

    #[test]
    fn candidates_no_filters_keeps_only_protected_head() {
        // s1 <- s2 (main), s3 dangling (no parent in live chain, no ref).
        // No filters, no explicit retain → default keep = 1 protects only the
        // main head (s2). s1 is an old main-chain snapshot and s3 is a dangling
        // snapshot — both expire under standard expireSnapshots semantics.
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, None)],
            vec![("main", 2)],
        );
        let mut candidates = compute_expire_candidates(&metadata, None, None);
        candidates.sort();
        assert_eq!(
            candidates,
            vec![1, 3],
            "only the protected main head (s2) survives; old s1 and dangling s3 expire"
        );
    }

    #[test]
    fn candidates_older_than_filter() {
        // 3 snapshots where s1=main (live), s2,s3 both non-live (dangling).
        // build_test_metadata_with_snapshots assigns timestamps:
        //   seq 1 → 1_700_000_001_000, seq 2 → 1_700_000_002_000, seq 3 → 1_700_000_003_000
        //   (1_700_000_000_000 + seq*1000 where seq is insertion order starting at 1)
        // s1 is first inserted (seq=1, ts=...001000)
        // s2 is second (seq=2, ts=...002000) and s3 third (seq=3, ts=...003000).
        // main = s1 → s2,s3 are non-live candidates.
        // Threshold = 1_700_000_002_500 → only s2 (ts=...002000 < threshold) expires,
        // not s3 (ts=...003000 >= threshold).
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, None), (3, None)],
            vec![("main", 1)],
        );
        // s2, s3 are unprotected (only s1 = main head is protected)
        let threshold = 1_700_000_002_500i64;
        let mut candidates = compute_expire_candidates(&metadata, Some(threshold), None);
        candidates.sort();
        assert_eq!(candidates, vec![2]);
    }

    #[test]
    fn candidates_retain_last_prunes_main_chain_beyond_window() {
        // s1 <- s2 <- s3 <- s4 (main); all reachable from main.
        // RETAIN LAST 2 protects the two most-recent main-chain snapshots
        // (s4, s3). The older main-chain snapshots (s1, s2) are expirable.
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2)), (4, Some(3))],
            vec![("main", 4)],
        );
        let mut candidates = compute_expire_candidates(&metadata, None, Some(2));
        candidates.sort();
        assert_eq!(
            candidates,
            vec![1, 2],
            "RETAIN LAST 2 keeps s3,s4; older main-chain s1,s2 expire"
        );
    }

    #[test]
    fn candidates_dangling_with_retain_n_main_chain_unaffected() {
        // s1 <- s2 (main), s3 dangling (no ref, no parent in live chain).
        // RETAIN LAST 5 protects the whole 2-snapshot main chain (s1, s2), so
        // only the dangling s3 (not on the main chain) remains a candidate.
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, None)],
            vec![("main", 2)],
        );
        let mut candidates = compute_expire_candidates(&metadata, None, Some(5));
        candidates.sort();
        assert_eq!(candidates, vec![3]);
    }

    #[test]
    fn main_ancestor_chain_returns_top_n() {
        // s1 <- s2 <- s3 <- s4 (main)
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2)), (4, Some(3))],
            vec![("main", 4)],
        );
        let chain = main_ancestor_chain(&metadata, 2);
        // Newest-first: s4, s3
        assert_eq!(chain, vec![4, 3]);
    }

    #[test]
    fn main_ancestor_chain_handles_no_main_ref() {
        // No refs at all → empty chain (defensive behavior).
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1))],
            vec![], // no refs
        );
        let chain = main_ancestor_chain(&metadata, 5);
        assert!(
            chain.is_empty(),
            "no main ref → main_ancestor_chain must return empty vec"
        );
    }

    #[test]
    fn expire_prunes_old_main_chain_snapshot() {
        // s1 <- s2 (main). With default keep = 1 and OLDER THAN in the far
        // future, the protected set is just the main head (s2); the older
        // main-chain snapshot s1 is expirable.
        // (This is the synchronous candidate-computation path; full async tested
        // via the v3_table tests below.)
        let metadata =
            build_test_metadata_with_snapshots(vec![(1, None), (2, Some(1))], vec![("main", 2)]);
        let candidates = compute_expire_candidates(&metadata, Some(i64::MAX), None);
        assert_eq!(
            candidates,
            vec![1],
            "old main-chain s1 expires; only the protected head s2 survives"
        );
    }

    #[test]
    fn expire_noop_when_retain_covers_whole_chain() {
        // s1 <- s2 (main). RETAIN LAST 2 covers the entire main chain, so even
        // with OLDER THAN in the far future there is nothing to expire.
        let metadata =
            build_test_metadata_with_snapshots(vec![(1, None), (2, Some(1))], vec![("main", 2)]);
        let candidates = compute_expire_candidates(&metadata, Some(i64::MAX), Some(2));
        assert!(
            candidates.is_empty(),
            "RETAIN LAST 2 protects the whole 2-snapshot main chain → no candidates"
        );
    }

    #[test]
    fn expire_older_than_in_future_expires_all_unprotected() {
        // OLDER THAN far in the future → the time filter admits every snapshot,
        // so the only thing standing between a snapshot and expiry is the
        // protected set. With default keep = 1 only the main head (s3) is
        // protected; the older main-chain snapshots s1, s2 expire.
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2))],
            vec![("main", 3)],
        );
        let mut candidates = compute_expire_candidates(&metadata, Some(i64::MAX), None);
        candidates.sort();
        assert_eq!(
            candidates,
            vec![1, 2],
            "far-future OLDER THAN admits all; only protected head s3 survives"
        );
    }

    #[test]
    fn expire_older_than_far_future_with_dangling_snapshot() {
        // s1 (main), s2 dangling. OLDER THAN far-future → s2 still a candidate
        // (all timestamps are < i64::MAX).
        let metadata =
            build_test_metadata_with_snapshots(vec![(1, None), (2, None)], vec![("main", 1)]);
        let mut candidates = compute_expire_candidates(&metadata, Some(i64::MAX), None);
        candidates.sort();
        assert_eq!(candidates, vec![2]);
    }

    #[test]
    fn expire_preserves_branches_and_tags() {
        // s1 <- s2 <- s3 (main), s2 also pointed to by branch "dev", s1 by tag "v1".
        // s4 is dangling (no ref). Even with default keep = 1 and OLDER THAN in
        // the far future, s1 and s2 are protected by the "v1" / "dev" refs, so
        // only the dangling s4 is a candidate. This is the key defense-in-depth
        // case: every ref's current snapshot is protected.
        let metadata = build_test_metadata_with_snapshots(
            vec![(1, None), (2, Some(1)), (3, Some(2)), (4, None)],
            vec![("main", 3), ("dev", 2), ("v1", 1)],
        );
        let live = compute_live_snapshot_set(&metadata);
        // live = {1,2,3}; s4 is non-live
        assert!(live.contains(&1));
        assert!(live.contains(&2));
        assert!(live.contains(&3));
        assert!(!live.contains(&4));

        let mut candidates = compute_expire_candidates(&metadata, Some(i64::MAX), None);
        candidates.sort();
        assert_eq!(
            candidates,
            vec![4],
            "refs protect s1/s2/s3; only the dangling s4 should expire"
        );
    }

    #[test]
    fn expire_table_with_no_snapshots() {
        // Empty table (no snapshots at all) → no candidates, noop.
        let metadata = build_test_metadata_with_snapshots(vec![], vec![]);
        let candidates = compute_expire_candidates(&metadata, Some(i64::MAX), None);
        assert!(candidates.is_empty());
    }

    #[tokio::test]
    async fn expire_real_table_prunes_old_main_chain() {
        let fixture = empty_local_v3_table().await;
        append_empty_snapshot(&fixture).await;
        append_empty_snapshot(&fixture).await;
        let head_snapshot_id = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("load table before expire")
            .metadata()
            .current_snapshot_id()
            .expect("table has a current snapshot");

        let outcome = run_expire_snapshots(
            fixture.catalog.clone(),
            fixture.table_ident.clone(),
            ExpireParams {
                older_than_ms: Some(i64::MAX),
                retain_last: None,
            },
        )
        .await
        .expect("run expire snapshots");
        assert_eq!(outcome.expired_snapshot_count, 1);

        let after = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("load table after expire");
        assert_eq!(
            after.metadata().current_snapshot_id(),
            Some(head_snapshot_id)
        );
        assert_eq!(
            after
                .metadata()
                .snapshots()
                .map(|snapshot| snapshot.snapshot_id())
                .collect::<Vec<_>>(),
            vec![head_snapshot_id]
        );
    }

    #[tokio::test]
    async fn expire_real_table_no_snapshots_succeeds() {
        // Table with no snapshots at all → noop, no error.
        let fixture = empty_local_v3_table().await;
        let outcome = run_expire_snapshots(
            fixture.catalog.clone(),
            fixture.table_ident.clone(),
            ExpireParams {
                older_than_ms: Some(i64::MAX),
                retain_last: None,
            },
        )
        .await
        .expect("run_expire_snapshots on empty table should succeed");
        assert_eq!(outcome.expired_snapshot_count, 0);
        assert_eq!(outcome.deleted_file_count, 0);
    }

    // Graph-shape correctness covers protected refs, main-chain retention, and
    // timestamp filtering. The provider-local catalog test covers the no-op
    // control path without depending on Core's commit-action fixture.
}
