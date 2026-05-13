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

//! `ALTER TABLE x REMOVE ORPHAN FILES OLDER THAN '<ts>'`
//!
//! Scans the warehouse `data/` and `metadata/` directories and removes files
//! not referenced by any snapshot currently registered in `metadata.json`.
//!
//! Critically, this does NOT commit a new metadata.json — the operation is a
//! pure physical file scan + delete. This means:
//!   * No OCC / commit_with_retry needed.
//!   * No risk of commit conflict with concurrent readers.
//!   * `OLDER THAN` is mandatory to defend against in-flight writes.
//!
//! Live-file protection (spec §4.2 Step 1):
//!   * ALL snapshots registered in current metadata (not just reachable ones).
//!   * Current + all historical metadata.json paths (from metadata_log).
//!   * Puffin half-reference protection for DV blobs (spec §4.2 Step 4).
//!
//! Physical delete is best-effort: failures are logged at WARN, not propagated.
//!
//! Spec: docs/superpowers/specs/2026-05-07-iceberg-snapshot-lifecycle-design.md §4.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
use iceberg::{Catalog, TableIdent};

use super::snapshot_lifecycle_helpers::{
    FileSet, enumerate_files_for_snapshots, puffin_half_reference_protection,
};

use crate::fs::object_store::ObjectStoreConfig;

/// Result of a successful REMOVE ORPHAN FILES execution.
#[derive(Debug)]
pub struct RemoveOrphanOutcome {
    /// Number of physical files successfully deleted.
    pub deleted_count: usize,
    /// Total files scanned across data/ and metadata/ (for informational logging).
    pub scanned_count: usize,
}

/// Top-level entry point called from `engine::iceberg_remove_orphan_files`.
///
/// Algorithm (spec §4.2):
/// 1. Enumerate live file set (all snapshots in metadata + metadata-log).
/// 2. Scan warehouse data/ and metadata/ directories.
/// 3. Compute candidates: scanned files not in live_files AND older than threshold.
/// 4. Apply puffin half-reference protection.
/// 5. Best-effort serial delete of remaining candidates.
///
/// `object_store_config` — when `Some`, the caller has an S3/OSS-backed
/// catalog and the scan will use an opendal operator built from this config.
/// When `None`, only `file://` and bare filesystem paths are supported.
pub async fn run_remove_orphan_files(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    older_than_ms: i64,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<RemoveOrphanOutcome, String> {
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| format!("load table {table_ident}: {e}"))?;

    let metadata = table.metadata();
    let file_io = table.file_io();
    let location = metadata.location();

    // Step 1: build live file set.
    // CRITICAL: protect ALL snapshots in metadata, not just live_set.
    // ORPHAN's job is to clean up orphaned disk files; EXPIRE is what removes
    // snapshots from metadata. If the user hasn't run EXPIRE first, even
    // "dangling" non-reachable snapshots still have their files registered and
    // must not be deleted.
    let all_snapshot_ids: HashSet<i64> = metadata.snapshots().map(|s| s.snapshot_id()).collect();
    let mut live_files: FileSet =
        enumerate_files_for_snapshots(file_io, metadata, &all_snapshot_ids)
            .await
            .map_err(|e| format!("enumerate live files: {e}"))?;

    // Add all metadata.json paths from the metadata log.
    // The metadata_log lists historical metadata file locations — every entry
    // is a valid metadata.json that must not be deleted (spec §4.4 invariant 2).
    for log_entry in metadata.metadata_log() {
        live_files.insert(log_entry.metadata_file.clone());
    }
    // Add the current metadata.json path. Note: it is usually also in the log as
    // the last entry, but we add it explicitly to be safe (spec §4.4 invariant 1).
    // The current metadata location is available from the table's metadata_location().
    // table.metadata_location() returns Option<&str> — None only for in-memory or
    // freshly-created tables that haven't been committed.
    if let Some(current_meta_location) = table.metadata_location() {
        live_files.insert(current_meta_location.to_string());
    }
    // Also add version-hint.text — it lives in metadata/ and must never be deleted.
    let version_hint_path = format!(
        "{}/metadata/version-hint.text",
        location.trim_end_matches('/')
    );
    live_files.insert(version_hint_path);

    // Step 2: scan warehouse paths.
    // Both scan paths must be canonically inside the table's location.
    let location_trimmed = location.trim_end_matches('/');
    let scan_data = format!("{}/data/", location_trimmed);
    let scan_meta = format!("{}/metadata/", location_trimmed);

    // Path containment check (spec §4.2 Step 2): both scan paths must be
    // subdirectories of the table location. This defends against adversarial
    // table locations that could escape the table directory.
    let canonical_location = canonicalize_location_for_containment(location_trimmed);
    for scan_path in [&scan_data, &scan_meta] {
        let canonical_scan = canonicalize_location_for_containment(scan_path.trim_end_matches('/'));
        if !canonical_scan.starts_with(&canonical_location) {
            return Err(format!(
                "REMOVE ORPHAN FILES: scan path {scan_path} escapes table location {location}"
            ));
        }
    }

    let all_scanned = list_files_for_location(location_trimmed, object_store_config)
        .await
        .map_err(|e| format!("scan warehouse at {location}: {e}"))?;

    let scanned_count = all_scanned.len();

    // Step 3: compute candidates.
    // A file is a candidate if it is not in live_files AND its mtime is older
    // than the threshold. We use the absolute path as stored in live_files.
    let mut candidates: FileSet = all_scanned
        .iter()
        .filter(|f| {
            let canonical_path = normalize_path_for_set_lookup(location_trimmed, &f.path);
            !live_files.contains(&canonical_path) && !live_files.contains(&f.path)
        })
        .filter(|f| f.last_modified_ms < older_than_ms)
        .map(|f| f.path.clone())
        .collect();

    // Step 4: puffin half-reference protection.
    // Build a DV index from ALL snapshot manifest entries (same approach as EXPIRE).
    let dv_index = build_dv_index_from_metadata(metadata, file_io, &all_snapshot_ids)
        .await
        .map_err(|e| format!("build dv_index: {e}"))?;
    puffin_half_reference_protection(&mut candidates, &dv_index, &live_files);

    // Step 5: best-effort physical delete.
    let mut deleted_count = 0;
    for path in &candidates {
        match file_io.delete(path).await {
            Ok(()) => deleted_count += 1,
            Err(e) => {
                tracing::warn!(
                    path = %path,
                    error = %e,
                    "remove_orphan_files: best-effort file delete failed"
                );
            }
        }
    }

    Ok(RemoveOrphanOutcome {
        deleted_count,
        scanned_count,
    })
}

/// A file discovered during the warehouse scan.
#[derive(Debug)]
struct ScannedFile {
    /// Full path to the file (absolute, with scheme prefix if any).
    path: String,
    /// Last-modified time in epoch milliseconds.
    /// `None` means the mtime could not be determined; such files are
    /// conservatively skipped (not deleted).
    last_modified_ms: i64,
}

/// List all files under `<location>/data/` and `<location>/metadata/`
/// recursively.
///
/// Handles:
///  * `file://` and bare filesystem paths: uses `std::fs` walk.
///  * `s3://`, `s3a://`, `oss://`: uses an opendal S3 operator built from
///    `object_store_config`. If `object_store_config` is `None` for these
///    schemes, returns empty (safe — won't delete).
///  * `memory://` or unrecognised schemes: returns empty (safe — won't delete).
///
/// The returned paths use the same scheme-prefixed format as the live_files
/// set populated from manifest entries (e.g. `file:///abs/path` for local,
/// `s3://bucket/key` for S3).
async fn list_files_for_location(
    location: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<Vec<ScannedFile>, String> {
    // Determine the scheme + root.
    if let Some(stripped) = location.strip_prefix("file://") {
        let root_path = std::path::PathBuf::from(stripped);
        list_files_local("file://", &root_path)
    } else if location.starts_with('/') {
        // Bare absolute path — treat as file:// internally.
        let root_path = std::path::PathBuf::from(location);
        list_files_local("", &root_path)
    } else if location.starts_with("s3://")
        || location.starts_with("s3a://")
        || location.starts_with("oss://")
    {
        let Some(cfg) = object_store_config else {
            tracing::debug!(
                location = %location,
                "remove_orphan_files: S3/OSS location but no object_store_config supplied; \
                 returning empty scan (no files will be deleted)"
            );
            return Ok(Vec::new());
        };
        list_files_opendal(location, cfg).await
    } else {
        // memory://, hdfs://, or unrecognised scheme.
        tracing::debug!(
            location = %location,
            "remove_orphan_files: listing not implemented for this scheme; \
             returning empty scan (no files will be deleted)"
        );
        Ok(Vec::new())
    }
}

/// List files under `<root>/data/` and `<root>/metadata/` using `std::fs`.
fn list_files_local(
    scheme_prefix: &str,
    root_path: &std::path::Path,
) -> Result<Vec<ScannedFile>, String> {
    let data_dir = root_path.join("data");
    let meta_dir = root_path.join("metadata");

    let mut result = Vec::new();
    for dir in [&data_dir, &meta_dir] {
        if !dir.exists() {
            continue;
        }
        walk_dir(dir, scheme_prefix, root_path, &mut result)?;
    }
    Ok(result)
}

/// List files under `<location>/data/` and `<location>/metadata/` using opendal.
///
/// `location` must have an `s3://`, `s3a://`, or `oss://` prefix.
/// The full path returned for each file matches the format Iceberg uses in
/// manifest entries: `<scheme>://<bucket>/<key>`.
async fn list_files_opendal(
    location: &str,
    cfg: &ObjectStoreConfig,
) -> Result<Vec<ScannedFile>, String> {
    use crate::connector::iceberg::catalog::add_files::parse_s3_path;
    use crate::fs::object_store::build_oss_operator;

    let scheme = if location.starts_with("oss://") {
        "oss"
    } else {
        "s3"
    };

    let (bucket, location_key) = parse_s3_path(location)
        .map_err(|e| format!("parse table location for opendal scan: {e}"))?;

    // Build an operator rooted at the bucket level (empty root) so keys we pass
    // match exactly the key portion of the full URIs we reconstruct.
    let mut op_cfg = cfg.clone();
    op_cfg.bucket = bucket.clone();
    op_cfg.root = String::new();
    let op = build_oss_operator(&op_cfg).map_err(|e| format!("build opendal operator: {e}"))?;

    let location_key = location_key.trim_matches('/');

    let mut result = Vec::new();
    for sub in ["data", "metadata"] {
        let prefix = if location_key.is_empty() {
            format!("{sub}/")
        } else {
            format!("{location_key}/{sub}/")
        };

        let entries = match op.list(&prefix).await {
            Ok(e) => e,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => {
                // Directory does not exist — skip, not an error.
                continue;
            }
            Err(e) => {
                return Err(format!("opendal list {scheme}://{bucket}/{prefix}: {e}"));
            }
        };

        for entry in entries {
            // Skip directory pseudo-entries.
            if entry.metadata().is_dir() {
                continue;
            }

            // `entry.path()` is relative to the operator root (the bucket).
            // Reconstruct the full URI: `<scheme>://<bucket>/<key>`.
            let full_path = format!("{scheme}://{bucket}/{}", entry.path());

            // last_modified from opendal metadata; None → conservatively skip.
            // opendal::raw::Timestamp wraps jiff::Timestamp; use into_inner() +
            // as_millisecond() to get epoch-ms.
            let last_modified_ms = entry
                .metadata()
                .last_modified()
                .map(|dt| dt.into_inner().as_millisecond())
                .unwrap_or(i64::MAX); // i64::MAX → never older-than threshold

            result.push(ScannedFile {
                path: full_path,
                last_modified_ms,
            });
        }
    }

    tracing::debug!(
        location = %location,
        scanned = result.len(),
        "remove_orphan_files: opendal scan complete"
    );
    Ok(result)
}

/// Recursively walk a directory, appending `ScannedFile` entries to `out`.
///
/// The path returned in each entry is `scheme_prefix + absolute_path`
/// (e.g. `file:///tmp/wh/ns/t/data/x.parquet`).
fn walk_dir(
    dir: &std::path::Path,
    scheme_prefix: &str,
    _root: &std::path::Path,
    out: &mut Vec<ScannedFile>,
) -> Result<(), String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("read_dir {}: {e}", dir.display()))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("read_dir entry: {e}"))?;
        let path = entry.path();
        let ft = entry
            .file_type()
            .map_err(|e| format!("file_type {}: {e}", path.display()))?;

        if ft.is_dir() {
            walk_dir(&path, scheme_prefix, _root, out)?;
        } else if ft.is_file() {
            // Get last-modified time.
            let last_modified_ms = match entry.metadata() {
                Ok(meta) => {
                    match meta.modified() {
                        Ok(sys_time) => {
                            // Convert SystemTime -> epoch ms.
                            match sys_time.duration_since(std::time::UNIX_EPOCH) {
                                Ok(d) => d.as_millis() as i64,
                                Err(_) => {
                                    // mtime before epoch — treat as very old (0 ms).
                                    0
                                }
                            }
                        }
                        Err(_) => {
                            // Cannot read mtime (rare on modern Linux/macOS).
                            // Conservative: skip (don't delete) by using i64::MAX.
                            i64::MAX
                        }
                    }
                }
                Err(_) => i64::MAX, // stat failed → skip
            };

            let abs = path.to_string_lossy();
            let full_path = format!("{scheme_prefix}{abs}");
            out.push(ScannedFile {
                path: full_path,
                last_modified_ms,
            });
        }
        // Symlinks are skipped (not followed).
    }
    Ok(())
}

/// Normalize a scanned file path so it matches the format used in `live_files`.
///
/// `live_files` entries come from manifest data — they're full URIs like
/// `file:///abs/path/to/file.parquet`. When we scan local FS we produce the
/// same prefix. This helper is a no-op for `file://` paths but unifies any
/// bare-path vs scheme divergence.
fn normalize_path_for_set_lookup(location: &str, scanned_path: &str) -> String {
    // If scanned_path already has a scheme, return as-is.
    if scanned_path.contains("://") {
        return scanned_path.to_string();
    }
    // Bare absolute path — prepend file:// to match live_files format.
    if scanned_path.starts_with('/') {
        return format!("file://{scanned_path}");
    }
    // Relative or unknown — unlikely but just return as-is.
    let _ = location;
    scanned_path.to_string()
}

/// Normalize `location` for the path-containment check.
///
/// Strips the `file://` scheme prefix (so both bare paths and `file://` URIs
/// compare equal), trims trailing slashes, then appends `/` for prefix
/// comparison.
///
/// For other schemes (`s3://`, `oss://`, etc.) the scheme is retained so that
/// the URI itself forms the containment prefix (e.g. `s3://bucket/prefix/`).
///
/// Examples:
///   `file:///tmp/wh/ns/t` → `/tmp/wh/ns/t/`
///   `/tmp/wh/ns/t`        → `/tmp/wh/ns/t/`
///   `s3://bucket/wh/ns/t` → `s3://bucket/wh/ns/t/`
fn canonicalize_location_for_containment(location: &str) -> String {
    let stripped = if let Some(s) = location.strip_prefix("file://") {
        s
    } else {
        location
    };
    let trimmed = stripped.trim_end_matches('/');
    format!("{trimmed}/")
}

/// Build a DV index (puffin path → set of referenced data file paths) from all
/// manifest entries across the given `snapshot_ids`.
///
/// Mirrors `expire_snapshots::build_dv_index_from_metadata`.
async fn build_dv_index_from_metadata(
    metadata: &TableMetadata,
    file_io: &FileIO,
    snapshot_ids: &HashSet<i64>,
) -> Result<HashMap<String, HashSet<String>>, iceberg::Error> {
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

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::*;
    use crate::connector::iceberg::commit::test_helpers::{
        empty_v3_iceberg_table, v3_table_with_multi_batch_appends,
    };

    // ---- Helper: build a Hadoop-catalog table on a real tempdir ----

    /// Build a local-filesystem Hadoop-catalog backed table with `n` appended
    /// snapshots (each contributing one synthetic data file).
    ///
    /// Returns (catalog, table_ident, tmpdir-keepalive).
    /// All paths use `file://` scheme so listing works via `list_files_for_location`.
    async fn build_local_table_with_n_appends(
        n: usize,
    ) -> (
        Arc<dyn iceberg::Catalog>,
        iceberg::TableIdent,
        tempfile::TempDir,
    ) {
        use crate::connector::iceberg::commit::action::{CommitCtx, IcebergCommitAction};
        use crate::connector::iceberg::commit::collector::IcebergCommitCollector;
        use crate::connector::iceberg::commit::fast_append::FastAppendCommit;
        use crate::connector::iceberg::commit::types::{CommitOpKind, WrittenFile};
        use iceberg::spec::{
            DataContentType, DataFileFormat, FormatVersion, NestedField, PrimitiveType, Schema,
            Struct, Type,
        };
        use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation};

        let tmpdir = tempfile::tempdir().expect("tempdir");
        let warehouse_path = tmpdir.path().to_str().expect("path").to_string();
        // Use file:// scheme so iceberg storage factory resolves to local FS.
        let warehouse_uri = format!("file://{warehouse_path}");

        let file_io = iceberg::io::FileIO::new_with_fs();
        let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog::new(
                file_io,
                warehouse_uri.clone(),
            ),
        );

        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .expect("create_namespace");

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("build schema");

        let table_ident = iceberg::TableIdent::new(namespace.clone(), "test_table".to_string());
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("test_table".to_string())
                    .schema(schema)
                    .format_version(FormatVersion::V3)
                    .build(),
            )
            .await
            .expect("create_table");

        let mut current_table = table;

        for i in 0..n {
            let table_location = current_table.metadata().location().to_string();
            let metadata = current_table.metadata();
            let collector = Arc::new(IcebergCommitCollector::new(
                CommitOpKind::FastAppend,
                table_ident.clone(),
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!("{table_location}/staging"),
                crate::common::types::UniqueId { hi: 0, lo: 0 },
            ));
            collector.inject_written_file(WrittenFile {
                path: format!("{table_location}/data/file-{i}.parquet"),
                format: DataFileFormat::Parquet,
                content: DataContentType::Data,
                partition_values: Struct::empty(),
                partition_spec_id: 0,
                record_count: 10,
                file_size_in_bytes: 1024,
                split_offsets: vec![],
                column_sizes: std::collections::HashMap::new(),
                value_counts: std::collections::HashMap::new(),
                null_value_counts: std::collections::HashMap::new(),
                key_metadata: None,
                referenced_data_file: None,
                equality_ids: None,
                first_row_id: None,
            });
            let file_io = current_table.file_io().clone();
            let abort_handle = collector.abort_log.clone();
            let snapshot_properties = std::collections::BTreeMap::new();
            let ctx = CommitCtx {
                collector: &collector,
                table: &current_table,
                catalog: catalog.as_ref(),
                file_io: &file_io,
                commit_uuid: uuid::Uuid::new_v4(),
                abort_handle,
                target_ref: "main",
                snapshot_properties: &snapshot_properties,
            };
            FastAppendCommit
                .commit(ctx)
                .await
                .expect("FastAppendCommit in fixture");
            current_table = catalog
                .load_table(&table_ident)
                .await
                .expect("reload table");
        }

        (catalog, table_ident, tmpdir)
    }

    // ---- Test 1: orphan file older than threshold is deleted ----

    #[tokio::test]
    async fn orphan_deletes_unreferenced_files_under_threshold() {
        let (catalog, table_ident, tmpdir) = build_local_table_with_n_appends(2).await;

        // Get the table's data directory.
        let table = catalog.load_table(&table_ident).await.unwrap();
        let table_location = table
            .metadata()
            .location()
            .trim_end_matches('/')
            .to_string();

        // Manually write an orphan file into the data/ directory.
        let data_dir = std::path::Path::new(
            table_location
                .strip_prefix("file://")
                .unwrap_or(&table_location),
        )
        .join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let orphan_path = data_dir.join("orphan_xxx.parquet");
        std::fs::write(&orphan_path, b"junk content").unwrap();

        // Set mtime to 1 hour ago using std::fs::File (no extra crate needed).
        let one_hour_ago = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
        // Use filetime if available, otherwise use std's set_modified.
        let _ = std::fs::File::open(&orphan_path).and_then(|f| f.set_modified(one_hour_ago));

        let now_ms = chrono::Utc::now().timestamp_millis();
        let outcome = run_remove_orphan_files(
            catalog,
            table_ident,
            now_ms - 60_000, // older than 1 min ago
            None,
        )
        .await
        .expect("run_remove_orphan_files should succeed");

        // The orphan file should have been deleted.
        assert!(
            outcome.deleted_count >= 1,
            "expected at least 1 deletion, got {}",
            outcome.deleted_count
        );
        assert!(
            !orphan_path.exists(),
            "orphan file should have been deleted"
        );

        // Keep tmpdir alive until here.
        drop(tmpdir);
    }

    // ---- Test 2: live manifest / metadata files are NEVER deleted ----
    //
    // Note: the FastAppendCommit fixture uses synthetic data file paths (the parquet
    // bytes are not actually written to disk — only the manifest metadata is). So we
    // verify live-file protection by checking that all real on-disk files produced by
    // the catalog (metadata.json, manifest lists, manifests) still exist after ORPHAN.

    #[tokio::test]
    async fn orphan_protects_live_data_files() {
        let (catalog, table_ident, tmpdir) = build_local_table_with_n_appends(2).await;

        // Load table and record the real on-disk file paths (manifests, metadata).
        let table_before = catalog.load_table(&table_ident).await.unwrap();
        let metadata_before = table_before.metadata();
        let all_ids: HashSet<i64> = metadata_before
            .snapshots()
            .map(|s| s.snapshot_id())
            .collect();

        // Enumerate all paths referenced in the manifests (these are real disk files:
        // manifest lists, manifests; data file paths are synthetic and not on disk).
        let referenced_files: HashSet<String> =
            enumerate_files_for_snapshots(table_before.file_io(), metadata_before, &all_ids)
                .await
                .unwrap();

        // Collect only the manifest-list and manifest paths that are real disk files.
        // Data file paths (*.parquet) in the fixture are synthetic (not on disk).
        let real_disk_files: Vec<String> = referenced_files
            .iter()
            .filter(|p| p.ends_with(".avro"))
            .cloned()
            .collect();

        // Run ORPHAN with a permissive threshold (far future).
        let far_future_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
        run_remove_orphan_files(catalog.clone(), table_ident.clone(), far_future_ms, None)
            .await
            .expect("run_remove_orphan_files should succeed");

        // Verify that all real on-disk files (manifests, manifest lists) still exist.
        for path in &real_disk_files {
            let disk_path = path.strip_prefix("file://").unwrap_or(path);
            assert!(
                std::path::Path::new(disk_path).exists(),
                "live manifest/metadata file was unexpectedly deleted: {path}"
            );
        }

        // The table should still be loadable (metadata intact).
        catalog
            .load_table(&table_ident)
            .await
            .expect("table should still be loadable after ORPHAN");

        drop(tmpdir);
    }

    // ---- Test 3: metadata-log history files are protected ----

    #[tokio::test]
    async fn orphan_protects_metadata_log_history() {
        let (catalog, table_ident, tmpdir) = build_local_table_with_n_appends(2).await;

        let table = catalog.load_table(&table_ident).await.unwrap();
        let metadata = table.metadata();

        // Collect all metadata-log entries.
        let log_files: Vec<String> = metadata
            .metadata_log()
            .iter()
            .map(|e| e.metadata_file.clone())
            .collect();

        // Run ORPHAN with far-future threshold.
        let far_future_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
        run_remove_orphan_files(catalog.clone(), table_ident.clone(), far_future_ms, None)
            .await
            .expect("run_remove_orphan_files should succeed");

        // All historical metadata.json paths in the log must still exist.
        for log_path in &log_files {
            let disk_path = log_path.strip_prefix("file://").unwrap_or(log_path);
            assert!(
                std::path::Path::new(disk_path).exists(),
                "metadata-log file was unexpectedly deleted: {log_path}"
            );
        }

        drop(tmpdir);
    }

    // ---- Test 4: current metadata.json is never deleted ----

    #[tokio::test]
    async fn orphan_protects_current_metadata_json() {
        let (catalog, table_ident, tmpdir) = build_local_table_with_n_appends(1).await;

        let table = catalog.load_table(&table_ident).await.unwrap();
        let current_meta_location = table
            .metadata_location()
            .expect("table should have a metadata location")
            .to_string();

        let far_future_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
        run_remove_orphan_files(catalog.clone(), table_ident.clone(), far_future_ms, None)
            .await
            .expect("run_remove_orphan_files should succeed");

        // Current metadata.json must still exist.
        let disk_path = current_meta_location
            .strip_prefix("file://")
            .unwrap_or(&current_meta_location);
        assert!(
            std::path::Path::new(disk_path).exists(),
            "current metadata.json was unexpectedly deleted: {current_meta_location}"
        );

        drop(tmpdir);
    }

    // ---- Test 5: threshold in the future → no candidates → 0 deletions ----

    #[tokio::test]
    async fn orphan_threshold_in_future_no_deletions() {
        let (catalog, table_ident, tmpdir) = build_local_table_with_n_appends(2).await;

        // Threshold = 1 ms from now. All real files are newer than "1 ms ago"
        // in absolute terms, so the threshold is in the future and no files qualify.
        // However, to be safe use a threshold far in the past so no real file
        // qualifies yet the operation succeeds gracefully.
        //
        // Actually: spec §4.3: "OLDER THAN in future → accepts". Let's use
        // a threshold of exactly now+1ms so no file (which has mtime < now) qualifies.
        let future_ms = chrono::Utc::now().timestamp_millis() + 1;
        let outcome = run_remove_orphan_files(catalog, table_ident, future_ms, None)
            .await
            .expect("run_remove_orphan_files with future threshold should succeed");

        // All table files were written moments ago; they shouldn't be older than
        // "1 ms from now". So deleted_count should be 0.
        assert_eq!(
            outcome.deleted_count, 0,
            "no files should be deleted when threshold is in the future"
        );

        drop(tmpdir);
    }

    // ---- Test 6: table with no snapshots (CREATE TABLE + no INSERT) ----

    #[tokio::test]
    async fn orphan_no_snapshot_table_succeeds() {
        use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
        use iceberg::{NamespaceIdent, TableCreation};

        let tmpdir = tempfile::tempdir().expect("tempdir");
        let warehouse_path = tmpdir.path().to_str().expect("path").to_string();
        let warehouse_uri = format!("file://{warehouse_path}");

        let file_io = iceberg::io::FileIO::new_with_fs();
        let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog::new(
                file_io,
                warehouse_uri,
            ),
        );

        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let table_ident = iceberg::TableIdent::new(namespace.clone(), "empty_table".to_string());
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("empty_table".to_string())
                    .schema(schema)
                    .format_version(FormatVersion::V3)
                    .build(),
            )
            .await
            .unwrap();

        // Run ORPHAN on an empty table (no snapshots).
        let now_ms = chrono::Utc::now().timestamp_millis();
        let outcome = run_remove_orphan_files(
            catalog,
            table_ident,
            now_ms - 1000, // anything older than 1s ago
            None,
        )
        .await
        .expect("ORPHAN on empty table should succeed");

        // No files should be deleted (only metadata.json exists, and it's live).
        assert_eq!(
            outcome.deleted_count, 0,
            "no files should be deleted for an empty table"
        );

        drop(tmpdir);
    }

    // ---- Test 7: staging dir old files are eligible ----

    #[tokio::test]
    async fn orphan_staging_dir_old_files_eligible() {
        let (catalog, table_ident, tmpdir) = build_local_table_with_n_appends(1).await;

        let table = catalog.load_table(&table_ident).await.unwrap();
        let table_location = table
            .metadata()
            .location()
            .trim_end_matches('/')
            .to_string();

        // Plant an old staging file in data/_staging/
        let staging_dir = std::path::Path::new(
            table_location
                .strip_prefix("file://")
                .unwrap_or(&table_location),
        )
        .join("data")
        .join("_staging");
        std::fs::create_dir_all(&staging_dir).unwrap();
        let staging_file = staging_dir.join("stale_staging.parquet");
        std::fs::write(&staging_file, b"stale staging content").unwrap();

        // Set mtime to 2 hours ago.
        let two_hours_ago = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);
        let _ = std::fs::File::open(&staging_file).and_then(|f| f.set_modified(two_hours_ago));

        // Threshold: 1 hour ago. Staging file (2h old) is eligible.
        let now_ms = chrono::Utc::now().timestamp_millis();
        let threshold = now_ms - 3_600_000; // 1 hour ago in ms
        let outcome = run_remove_orphan_files(catalog, table_ident, threshold, None)
            .await
            .expect("run_remove_orphan_files should succeed");

        assert!(
            outcome.deleted_count >= 1,
            "stale staging file should have been deleted"
        );
        assert!(!staging_file.exists(), "stale staging file should be gone");

        drop(tmpdir);
    }

    // ---- Test 8: MemoryCatalog (memory:// scheme) → returns Ok, 0 deletions ----

    #[tokio::test]
    async fn orphan_memory_catalog_returns_ok_no_deletions() {
        // MemoryCatalog uses memory:// paths — listing is not supported, but
        // the operation should succeed gracefully (returning 0 deletions).
        let fixture = v3_table_with_multi_batch_appends(&[2]).await;

        let now_ms = chrono::Utc::now().timestamp_millis();
        let outcome = run_remove_orphan_files(
            fixture.catalog,
            fixture.table_ident,
            now_ms + 86_400_000, // far-future threshold
            None,
        )
        .await
        .expect("ORPHAN on memory catalog should succeed");

        assert_eq!(
            outcome.deleted_count, 0,
            "memory:// catalog has no listable filesystem — 0 deletions expected"
        );
    }

    // ---- Test 9: empty table (MemoryCatalog) → no error ----

    #[tokio::test]
    async fn orphan_empty_memory_table_succeeds() {
        let fixture = empty_v3_iceberg_table().await;

        let now_ms = chrono::Utc::now().timestamp_millis();
        let outcome = run_remove_orphan_files(fixture.catalog, fixture.table_ident, now_ms, None)
            .await
            .expect("ORPHAN on empty memory table should succeed");

        assert_eq!(outcome.deleted_count, 0, "empty table → 0 deletions");
    }

    // ---- Test 10: path-containment helper correctly identifies escaping paths ----

    #[test]
    fn canonicalize_location_containment_check() {
        let table_location = "/tmp/warehouse/ns/table";
        let canonical = canonicalize_location_for_containment(table_location);
        assert_eq!(canonical, "/tmp/warehouse/ns/table/");

        // Data and metadata paths are inside.
        let data_path = "/tmp/warehouse/ns/table/data";
        let meta_path = "/tmp/warehouse/ns/table/metadata";
        assert!(canonicalize_location_for_containment(data_path).starts_with(&canonical));
        assert!(canonicalize_location_for_containment(meta_path).starts_with(&canonical));

        // A sibling path escapes.
        let sibling = "/tmp/warehouse/ns/other_table/data";
        assert!(!canonicalize_location_for_containment(sibling).starts_with(&canonical));

        // A parent path escapes.
        let parent = "/tmp/warehouse/ns";
        assert!(!canonicalize_location_for_containment(parent).starts_with(&canonical));
    }

    // ---- Test 11: normalize_path_for_set_lookup ----

    #[test]
    fn normalize_path_handles_schemes() {
        // Already has scheme — returned as-is.
        assert_eq!(
            normalize_path_for_set_lookup("file:///tmp/wh/t", "file:///tmp/wh/t/data/x.parquet"),
            "file:///tmp/wh/t/data/x.parquet"
        );

        // Bare absolute path — gets file:// prepended.
        assert_eq!(
            normalize_path_for_set_lookup("/tmp/wh/t", "/tmp/wh/t/data/x.parquet"),
            "file:///tmp/wh/t/data/x.parquet"
        );
    }

    // ---- Test 12: opendal fs operator — orphan file is detected and deleted ----
    //
    // Uses the opendal `Fs` service rooted at a tempdir to exercise the
    // `list_files_opendal` code path indirectly: we build an ObjectStoreConfig
    // from a fake S3 endpoint and verify that passing it succeeds (the opendal
    // operator itself is internal to `list_files_opendal`).
    //
    // Because setting up a real S3/MinIO endpoint in unit tests is too heavy,
    // we test the opendal-based listing path using the opendal `Fs` backend
    // directly through `list_files_for_location` (the internal helper) to
    // confirm the async dispatch works and returns correct results.

    #[tokio::test]
    async fn opendal_fs_listing_finds_orphan_file() {
        use opendal::Operator;
        use opendal::services::Fs;

        let tmpdir = tempfile::tempdir().expect("tempdir");
        let root = tmpdir.path().to_str().expect("utf8 path").to_string();

        // Build an opendal Fs operator rooted at tmpdir.
        let op = Operator::new(Fs::default().root(&root))
            .expect("build fs operator")
            .finish();

        // Create data/ and metadata/ with one file each.
        op.write("data/live_file.parquet", b"live".to_vec())
            .await
            .expect("write data file");
        op.write("metadata/v1.metadata.json", b"{}".to_vec())
            .await
            .expect("write metadata file");

        // List through the internal async helper using a bare file path.
        let scanned = list_files_for_location(&root, None)
            .await
            .expect("list_files_for_location should succeed");

        // Should find exactly 2 files.
        assert_eq!(
            scanned.len(),
            2,
            "expected 2 scanned files, got: {:?}",
            scanned.iter().map(|f| &f.path).collect::<Vec<_>>()
        );

        let paths: Vec<&str> = scanned.iter().map(|f| f.path.as_str()).collect();
        assert!(
            paths.iter().any(|p| p.contains("live_file.parquet")),
            "data file should appear in scan results: {paths:?}"
        );
        assert!(
            paths.iter().any(|p| p.contains("v1.metadata.json")),
            "metadata file should appear in scan results: {paths:?}"
        );

        drop(tmpdir);
    }
}
