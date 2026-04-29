//! Errors and (in later PRs) data structures for iceberg snapshot-lineage
//! change planning under IVM Phase 2. This file is the home of the new
//! `plan_changes` entrypoint that PR-2 will introduce; PR-1 only lands the
//! error enum so that CREATE-time PRIMARY KEY validation has a stable type
//! to return.

/// All failure modes the iceberg change-planning and IVM CREATE/REFRESH
/// paths can surface. STRICT fail-fast: every variant is a hard rejection,
/// not a fallback signal. Variants not constructed in this PR are reserved
/// for PR-2 (`plan_changes` lineage walk) and PR-3/4 (runtime checks).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ChangeError {
    /// `previous_snapshot` referenced by stored MV state is no longer
    /// reachable from the current snapshot's parent chain (e.g. expired).
    LineageBroken { previous_snapshot: i64 },

    /// Snapshot operation is not understood or not in scope for this phase
    /// (e.g. `overwrite`, vendor-specific ops).
    UnsupportedOperation { snapshot_id: i64, op: String },

    /// Equality-delete file encountered; only position-deletes are in scope.
    EqualityDeleteUnsupported { snapshot_id: i64 },

    /// Iceberg v3 deletion-vector file encountered; out of scope.
    DeletionVectorUnsupported { snapshot_id: i64 },

    /// Schema evolution between `previous_snapshot` and `current_snapshot`
    /// (or any unsupported schema-related rejection at CREATE time).
    SchemaEvolutionUnsupported { detail: String },

    /// REPLACE snapshot failed the compaction-only sanity checks (records
    /// changed / schema-id changed / no added or no removed files).
    ReplaceValidationFailed { snapshot_id: i64, reason: String },

    /// CREATE-time: PRIMARY KEY column does not exist on the iceberg base
    /// table.
    PrimaryKeyMissingFromBase { pk_col: String },

    /// CREATE-time: PRIMARY KEY column is nullable on the base table.
    PrimaryKeyNullable { pk_col: String },

    /// CREATE-time: PRIMARY KEY column has a non-hashable scalar type.
    PrimaryKeyTypeUnsupported { pk_col: String, ty: String },

    /// Runtime: PRIMARY KEY column observed NULL in a base row at refresh
    /// time. Not constructed in PR-1.
    PrimaryKeyValueNull { row_info: String },

    /// CREATE-time: iceberg base table is not format-version 2.
    IcebergFormatUnsupported { format_version: i32 },

    /// Catch-all for invariant violations the codebase should never hit;
    /// constructing one is a bug, not a user error.
    InternalInconsistency(String),
}

impl std::fmt::Display for ChangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeError::LineageBroken { previous_snapshot } => write!(
                f,
                "iceberg lineage broken: previous snapshot {previous_snapshot} is unreachable from current snapshot"
            ),
            ChangeError::UnsupportedOperation { snapshot_id, op } => write!(
                f,
                "iceberg snapshot {snapshot_id} has unsupported operation `{op}`"
            ),
            ChangeError::EqualityDeleteUnsupported { snapshot_id } => write!(
                f,
                "iceberg snapshot {snapshot_id} contains equality-delete files; not supported in this phase"
            ),
            ChangeError::DeletionVectorUnsupported { snapshot_id } => write!(
                f,
                "iceberg snapshot {snapshot_id} contains v3 deletion-vector files; not supported in this phase"
            ),
            ChangeError::SchemaEvolutionUnsupported { detail } => {
                write!(f, "iceberg schema evolution not supported: {detail}")
            }
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => write!(
                f,
                "iceberg REPLACE snapshot {snapshot_id} failed compaction validation: {reason}"
            ),
            ChangeError::PrimaryKeyMissingFromBase { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` does not exist on the iceberg base table"
            ),
            ChangeError::PrimaryKeyNullable { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` must be NOT NULL on the iceberg base table"
            ),
            ChangeError::PrimaryKeyTypeUnsupported { pk_col, ty } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` has unsupported type `{ty}`; only hashable scalar types are allowed"
            ),
            ChangeError::PrimaryKeyValueNull { row_info } => {
                write!(f, "PRIMARY KEY value is NULL in base row: {row_info}")
            }
            ChangeError::IcebergFormatUnsupported { format_version } => write!(
                f,
                "iceberg base table format-version {format_version} is not supported; IVM Phase 2 requires v2"
            ),
            ChangeError::InternalInconsistency(detail) => {
                write!(f, "internal inconsistency: {detail}")
            }
        }
    }
}

impl std::error::Error for ChangeError {}

/// Reference to a single data file added to the table by an `Append`
/// snapshot. PR-2 builds these from the snapshot's data manifests; PR-3
/// will pass the path/size/record_count tuple through to the existing
/// MV-incremental-refresh executor (which currently consumes
/// `Vec<(String, i64, Option<i64>)>` directly).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
}

/// Reference to a single position-delete file added to the table by a
/// `Delete` snapshot. PR-2 only reports these on the lineage path; the
/// reverse-projection that turns each (delete_file, pos) pair back into
/// the original base row lives in PR-3.
///
/// `referenced_data_file` carries the iceberg `DataFile.referenced_data_file`
/// field — a position-delete file MAY declare a single data file that all
/// of its rows target, in which case readers can short-circuit the join.
/// When `None`, every delete row carries its own `file_path` cell and the
/// reader must read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub referenced_data_file: Option<String>,
}

/// Output of `plan_changes`: a flattened, in-order projection of every
/// data-file insert and every position-delete-file ref in the lineage
/// from `previous_snapshot_id` (exclusive) to `current_snapshot_id`
/// (inclusive). REPLACE compaction snapshots are validated and skipped;
/// they contribute to neither vector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergChangeBatch {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub inserts: Vec<DataFileRef>,
    pub deletes: Vec<PositionDeleteRef>,
}

/// Per-row Change action: this row got inserted or deleted relative to
/// the previous MV refresh state. Carried alongside the row contents
/// through the materialize-changes pipeline so the aggregate path can
/// route inserts and deletes differently (insert → positive delta;
/// delete → negative delta after sign-flip).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ChangeAction {
    Insert,
    Delete,
}

/// Output of `materialize_changes`: separate `QueryResult` streams for
/// the insert side and the delete side. Both are produced by running
/// the MV's SELECT statement against a one-shot in-memory catalog
/// whose base table has been replaced by the relevant subset of rows
/// (insert files, or deleted-rows-as-temp-parquet). Aggregate semantics
/// (WHERE, GROUP BY, projection) are honored uniformly because the SQL
/// is the same on both branches.
///
/// Either branch may be the empty `QueryResult` (no rows / no chunks)
/// if the corresponding file list was empty.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct MaterializedChanges {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub inserts: crate::engine::QueryResult,
    pub deletes: crate::engine::QueryResult,
}

/// One unit of work the file-collection phase needs to perform for a
/// single snapshot in the lineage. `Replace` snapshots are validated by
/// `classify_snapshot` itself and never produce a `LineageAction` —
/// they're silently absorbed once the validator passes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LineageAction {
    /// Walk the snapshot's data manifests, collect entries with
    /// `added_snapshot_id == this`, project to `DataFileRef`.
    CollectInserts { snapshot_id: i64 },
    /// Walk the snapshot's delete manifests, collect entries whose
    /// `DataFile.content_type()` is `PositionDeletes`. Reject equality
    /// deletes.
    CollectDeletes { snapshot_id: i64 },
}

/// Output of `classify_lineage`: a chronologically-ordered list of
/// actions to execute against snapshots from `previous_snapshot_id`
/// (exclusive) to `current_snapshot_id` (inclusive). Replace snapshots
/// validated and skipped during classification do not appear here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LineagePlan {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub actions: Vec<LineageAction>,
}

/// Pure per-snapshot decision. Returns:
/// - `Ok(Some(action))` when the snapshot contributes work to the file
///   collector,
/// - `Ok(None)` when the snapshot is a validated REPLACE compaction and
///   should be silently absorbed,
/// - `Err(ChangeError)` for OVERWRITE, REPLACE-validation failure, etc.
///
/// `parent` is required for REPLACE (the validator compares
/// `total-records` and `schema_id` against the parent). It can be
/// `None` for any other operation; passing `None` for REPLACE
/// produces a `ReplaceValidationFailed` error.
fn classify_snapshot(
    snapshot: &iceberg::spec::Snapshot,
    parent: Option<&iceberg::spec::Snapshot>,
) -> Result<Option<LineageAction>, ChangeError> {
    use iceberg::spec::Operation;
    let snapshot_id = snapshot.snapshot_id();
    match &snapshot.summary().operation {
        Operation::Append => Ok(Some(LineageAction::CollectInserts { snapshot_id })),
        Operation::Delete => Ok(Some(LineageAction::CollectDeletes { snapshot_id })),
        Operation::Replace => {
            let parent = parent.ok_or_else(|| ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason: "REPLACE snapshot has no parent reachable for compaction validation"
                    .to_string(),
            })?;
            validate_replace_snapshot(snapshot, parent)?;
            Ok(None)
        }
        Operation::Overwrite => Err(ChangeError::UnsupportedOperation {
            snapshot_id,
            op: "overwrite".to_string(),
        }),
    }
}

/// Validate that a `Replace` snapshot is a compaction (file rewrite that
/// preserves logical content). A passing REPLACE leaves `total-records`
/// unchanged, contributes both `added-data-files` and `deleted-data-files`
/// counters, and does not change the schema. Anything else is rejected.
fn validate_replace_snapshot(
    snapshot: &iceberg::spec::Snapshot,
    parent: &iceberg::spec::Snapshot,
) -> Result<(), ChangeError> {
    let snap_props = &snapshot.summary().additional_properties;
    let parent_props = &parent.summary().additional_properties;

    let snap_records = snap_props
        .get("total-records")
        .and_then(|s| s.parse::<i64>().ok());
    let parent_records = parent_props
        .get("total-records")
        .and_then(|s| s.parse::<i64>().ok());
    match (snap_records, parent_records) {
        (Some(a), Some(b)) if a == b => {}
        (Some(a), Some(b)) => {
            return Err(ChangeError::ReplaceValidationFailed {
                snapshot_id: snapshot.snapshot_id(),
                reason: format!("total-records changed across REPLACE: parent={b}, replace={a}"),
            });
        }
        _ => {
            return Err(ChangeError::ReplaceValidationFailed {
                snapshot_id: snapshot.snapshot_id(),
                reason:
                    "REPLACE snapshot summary is missing `total-records`; cannot prove compaction"
                        .to_string(),
            });
        }
    }

    let added = snap_props
        .get("added-data-files")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let removed = snap_props
        .get("deleted-data-files")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    if added <= 0 || removed <= 0 {
        return Err(ChangeError::ReplaceValidationFailed {
            snapshot_id: snapshot.snapshot_id(),
            reason: format!(
                "REPLACE snapshot must report both added-data-files (>0) and \
                 deleted-data-files (>0); got added={added}, deleted={removed}"
            ),
        });
    }

    if snapshot.schema_id() != parent.schema_id() {
        return Err(ChangeError::ReplaceValidationFailed {
            snapshot_id: snapshot.snapshot_id(),
            reason: format!(
                "REPLACE snapshot schema-id {:?} differs from parent {:?}; schema evolution \
                 across compaction is not in scope",
                snapshot.schema_id(),
                parent.schema_id(),
            ),
        });
    }
    Ok(())
}

/// Walk the parent chain from `current_snapshot` back to
/// `previous_snapshot_id`, dispatching each node through
/// `classify_snapshot`. Performs no I/O.
///
/// Errors:
/// - `LineageBroken` when `previous_snapshot_id` is not an ancestor of
///   the current snapshot (its metadata entry has been pruned, or the
///   chain runs off its root).
/// - `UnsupportedOperation` / `ReplaceValidationFailed` propagated from
///   `classify_snapshot`.
pub(crate) fn classify_lineage(
    metadata: &iceberg::spec::TableMetadata,
    previous_snapshot_id: i64,
) -> Result<LineagePlan, ChangeError> {
    let current_snapshot = metadata.current_snapshot().ok_or_else(|| {
        ChangeError::InternalInconsistency(
            "classify_lineage: table has no current snapshot".to_string(),
        )
    })?;
    let current_snapshot_id = current_snapshot.snapshot_id();

    if current_snapshot_id == previous_snapshot_id {
        return Ok(LineagePlan {
            previous_snapshot_id,
            current_snapshot_id,
            actions: Vec::new(),
        });
    }

    if metadata.snapshot_by_id(previous_snapshot_id).is_none() {
        return Err(ChangeError::LineageBroken {
            previous_snapshot: previous_snapshot_id,
        });
    }

    let mut actions_reversed: Vec<LineageAction> = Vec::new();
    let mut cursor = current_snapshot_id;
    loop {
        if cursor == previous_snapshot_id {
            break;
        }
        let snapshot_ref = metadata
            .snapshot_by_id(cursor)
            .ok_or(ChangeError::LineageBroken {
                previous_snapshot: previous_snapshot_id,
            })?;
        let snapshot = snapshot_ref.as_ref();
        let parent_id = snapshot.parent_snapshot_id();
        let parent = parent_id
            .and_then(|id| metadata.snapshot_by_id(id))
            .map(|sr| sr.as_ref());

        if let Some(action) = classify_snapshot(snapshot, parent)? {
            actions_reversed.push(action);
        }

        match parent_id {
            Some(id) => cursor = id,
            None => {
                // Walked off the root without finding previous_snapshot_id.
                return Err(ChangeError::LineageBroken {
                    previous_snapshot: previous_snapshot_id,
                });
            }
        }
    }

    actions_reversed.reverse();
    Ok(LineagePlan {
        previous_snapshot_id,
        current_snapshot_id,
        actions: actions_reversed,
    })
}

/// Public entrypoint for snapshot-lineage change planning. Walks the
/// lineage from `previous_snapshot_id` (exclusive) to the table's
/// current snapshot (inclusive), classifies each snapshot operation,
/// and assembles `IcebergChangeBatch { inserts, deletes }`.
///
/// The `_pk_columns` parameter is reserved for PR-3 (delete reverse
/// projection); it is intentionally unused in PR-2.
pub(crate) fn plan_changes(
    table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    _pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    let metadata = table.metadata();
    let current_snapshot_id = metadata
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| {
            ChangeError::InternalInconsistency(
                "plan_changes: table has no current snapshot".to_string(),
            )
        })?;

    let plan = classify_lineage(metadata, previous_snapshot_id)?;
    if plan.actions.is_empty() {
        return Ok(IcebergChangeBatch {
            previous_snapshot_id,
            current_snapshot_id,
            inserts: Vec::new(),
            deletes: Vec::new(),
        });
    }

    let file_io = table.file_io();
    let collect = collect_files(metadata, file_io, &plan.actions);
    let (inserts, deletes) = crate::connector::iceberg::catalog::registry::block_on_iceberg(
        collect,
    )
    .map_err(|e| ChangeError::InternalInconsistency(format!("plan_changes runtime: {e}")))??;

    Ok(IcebergChangeBatch {
        previous_snapshot_id,
        current_snapshot_id,
        inserts,
        deletes,
    })
}

/// Top-level PR-3 entry: take an `IcebergChangeBatch`, produce a
/// `MaterializedChanges` whose `inserts` and `deletes` branches each
/// hold the result of running the MV's SELECT statement against the
/// relevant subset of base-table rows.
///
/// The `_pk_columns` parameter is reserved for PR-4 (delete-side
/// row-id computation when AggregateApplyChanges lands).
pub(crate) fn materialize_changes(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    current_database: &str,
    sql: &str,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    base_table: &iceberg::table::Table,
    batch: IcebergChangeBatch,
    _pk_columns: &[String],
) -> Result<MaterializedChanges, String> {
    let inserts = if batch.inserts.is_empty() {
        crate::engine::QueryResult::empty()
    } else {
        let added_files: Vec<(String, i64, Option<i64>)> = batch
            .inserts
            .iter()
            .map(|f| (f.path.clone(), f.size, f.record_count))
            .collect();
        crate::engine::mv_flow::execute_query_for_mv_incremental_refresh(
            state,
            current_database,
            sql,
            base_ref,
            added_files,
        )?
    };

    let deletes = if batch.deletes.is_empty() {
        crate::engine::QueryResult::empty()
    } else {
        let factory = build_factory_for_table(base_table)?;
        let size_lookup = |path: &str| -> Option<u64> {
            // For PR-3 we don't carry the per-data-file size index across
            // the boundary; iceberg-rust's parquet reader reads metadata
            // by HEAD when we pass `None`. Best-effort optimization is
            // PR-4 territory.
            let _ = path;
            None
        };
        let deleted_rows = crate::connector::iceberg::scan_deletes::scan_deletes(
            &batch.deletes,
            &factory,
            size_lookup,
        )
        .map_err(|e| e.to_string())?;
        crate::engine::mv_flow::execute_query_for_mv_incremental_deletes(
            state,
            current_database,
            sql,
            base_ref,
            deleted_rows,
        )?
    };

    Ok(MaterializedChanges {
        previous_snapshot_id: batch.previous_snapshot_id,
        current_snapshot_id: batch.current_snapshot_id,
        inserts,
        deletes,
    })
}

/// Build a filesystem factory that can read both data files and
/// position-delete files for the given iceberg base table. We use the
/// same `OpendalRangeReaderFactory` shape as the HDFS scan path
/// (`build_fs_operator` for local FS, S3/cloud-credentialled operator
/// when the catalog has cloud properties).
fn build_factory_for_table(
    table: &iceberg::table::Table,
) -> Result<crate::fs::opendal::OpendalRangeReaderFactory, String> {
    let _ = table; // Per PR-3 scope: local-FS only — same constraint as
    // execute_query_for_mv_incremental_refresh which
    // rejects multi-file local reads. PR-4 wires cloud.
    // Build a local-FS operator rooted at "/" so absolute file paths work.
    let operator = crate::fs::opendal::build_fs_operator("/")
        .map_err(|e| format!("build local fs operator for delete reverse projection: {e}"))?;
    crate::fs::opendal::OpendalRangeReaderFactory::from_operator(operator)
        .map_err(|e| format!("build opendal range reader factory: {e}"))
}

/// Async file collection for one `LineagePlan`. Loads each snapshot's
/// manifest list, walks data manifests for `CollectInserts` actions, and
/// walks delete manifests for `CollectDeletes` actions. Order of the
/// returned `(inserts, deletes)` matches the lineage order in `actions`.
async fn collect_files(
    metadata: &iceberg::spec::TableMetadata,
    file_io: &iceberg::io::FileIO,
    actions: &[LineageAction],
) -> Result<(Vec<DataFileRef>, Vec<PositionDeleteRef>), ChangeError> {
    use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};

    let mut inserts: Vec<DataFileRef> = Vec::new();
    let mut deletes: Vec<PositionDeleteRef> = Vec::new();

    for action in actions {
        let snapshot_id = match action {
            LineageAction::CollectInserts { snapshot_id }
            | LineageAction::CollectDeletes { snapshot_id } => *snapshot_id,
        };
        let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
            ChangeError::InternalInconsistency(format!(
                "collect_files: snapshot {snapshot_id} no longer in metadata"
            ))
        })?;
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "load manifest list for snapshot {snapshot_id}: {e}"
                ))
            })?;

        match action {
            LineageAction::CollectInserts { .. } => {
                for manifest_file in manifest_list.entries() {
                    if manifest_file.content != ManifestContentType::Data {
                        continue;
                    }
                    if manifest_file.added_snapshot_id != snapshot_id {
                        continue;
                    }
                    let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
                        ChangeError::InternalInconsistency(format!(
                            "load data manifest {} for snapshot {snapshot_id}: {e}",
                            manifest_file.manifest_path
                        ))
                    })?;
                    for entry in manifest.entries() {
                        if entry.status == ManifestStatus::Deleted {
                            return Err(ChangeError::InternalInconsistency(format!(
                                "data manifest entry has DELETED status in snapshot {snapshot_id}: {}",
                                entry.data_file().file_path()
                            )));
                        }
                        if entry.status != ManifestStatus::Added {
                            continue;
                        }
                        if entry.snapshot_id() != Some(snapshot_id) {
                            continue;
                        }
                        let df = entry.data_file();
                        if df.content_type() != DataContentType::Data {
                            continue;
                        }
                        inserts.push(DataFileRef {
                            path: df.file_path().to_string(),
                            size: i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
                            record_count: Some(
                                i64::try_from(df.record_count()).unwrap_or(i64::MAX),
                            ),
                        });
                    }
                }
            }
            LineageAction::CollectDeletes { .. } => {
                for manifest_file in manifest_list.entries() {
                    if manifest_file.content != ManifestContentType::Deletes {
                        continue;
                    }
                    if manifest_file.added_snapshot_id != snapshot_id {
                        continue;
                    }
                    let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
                        ChangeError::InternalInconsistency(format!(
                            "load delete manifest {} for snapshot {snapshot_id}: {e}",
                            manifest_file.manifest_path
                        ))
                    })?;
                    for entry in manifest.entries() {
                        if entry.status != ManifestStatus::Added {
                            continue;
                        }
                        if entry.snapshot_id() != Some(snapshot_id) {
                            continue;
                        }
                        let df = entry.data_file();
                        match df.content_type() {
                            DataContentType::PositionDeletes => {
                                deletes.push(PositionDeleteRef {
                                    delete_file_path: df.file_path().to_string(),
                                    delete_file_size: i64::try_from(df.file_size_in_bytes())
                                        .unwrap_or(i64::MAX),
                                    record_count: Some(
                                        i64::try_from(df.record_count()).unwrap_or(i64::MAX),
                                    ),
                                    referenced_data_file: df.referenced_data_file(),
                                });
                            }
                            DataContentType::EqualityDeletes => {
                                return Err(ChangeError::EqualityDeleteUnsupported { snapshot_id });
                            }
                            DataContentType::Data => {
                                return Err(ChangeError::InternalInconsistency(format!(
                                    "delete manifest contains DATA file in snapshot {snapshot_id}: {}",
                                    df.file_path()
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok((inserts, deletes))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{Operation, Snapshot, Summary};

    use super::{ChangeError, LineageAction, classify_snapshot, validate_replace_snapshot};

    use crate::connector::iceberg::catalog::registry::{
        IcebergCatalogEntry, build_catalog_entry, create_namespace, create_table, insert_rows,
        load_table,
    };
    use crate::sql::{Literal, SqlType, TableColumnDef};

    use super::plan_changes;

    fn test_hadoop_catalog_entry(catalog_name: &str, warehouse_uri: &str) -> IcebergCatalogEntry {
        build_catalog_entry(
            catalog_name,
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse_uri.to_string(),
                ),
            ],
        )
        .expect("catalog entry")
    }

    /// Build a synthetic `Snapshot` whose summary carries the given
    /// operation and properties. `manifest_list` and timestamps get
    /// throwaway-but-positive values; the classifier never reads them.
    /// schema_id is encoded in the summary's `schema_id` only when the
    /// caller passes it via the builder.
    fn snap(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        operation: Operation,
        properties: &[(&str, &str)],
        schema_id: i32,
    ) -> Snapshot {
        let mut props: HashMap<String, String> = HashMap::new();
        for (k, v) in properties {
            props.insert((*k).to_string(), (*v).to_string());
        }
        // iceberg-rust 0.9 `Snapshot::with_parent_snapshot_id` is generated
        // by typed_builder without `strip_option`, so its setter takes
        // `Option<i64>` directly. We always call it (passing `None` when
        // there's no parent) because TypedBuilder's type-state means we
        // can't reassign the builder across optional setters.
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000 + snapshot_id)
            .with_manifest_list(format!("file:///tmp/manifest-list-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation,
                additional_properties: props,
            })
            .with_schema_id(schema_id)
            .build()
    }

    fn replace_props(
        total_records: i64,
        added_files: i64,
        deleted_files: i64,
    ) -> Vec<(&'static str, String)> {
        vec![
            ("total-records", total_records.to_string()),
            ("added-data-files", added_files.to_string()),
            ("deleted-data-files", deleted_files.to_string()),
        ]
    }

    #[test]
    fn display_primary_key_missing() {
        let e = ChangeError::PrimaryKeyMissingFromBase {
            pk_col: "order_id".to_string(),
        };
        let s = format!("{e}");
        assert!(s.contains("order_id"), "{s}");
        assert!(s.to_lowercase().contains("primary key"), "{s}");
    }

    #[test]
    fn display_iceberg_format_unsupported() {
        let e = ChangeError::IcebergFormatUnsupported { format_version: 1 };
        let s = format!("{e}");
        assert!(s.contains("format-version 1"), "{s}");
        assert!(s.to_lowercase().contains("v2"), "{s}");
    }

    #[test]
    fn classify_snapshot_append_emits_collect_inserts() {
        let s = snap(7, Some(1), Operation::Append, &[], 0);
        let action = classify_snapshot(&s, None).expect("ok");
        assert_eq!(
            action,
            Some(LineageAction::CollectInserts { snapshot_id: 7 })
        );
    }

    #[test]
    fn classify_snapshot_delete_emits_collect_deletes() {
        let s = snap(7, Some(1), Operation::Delete, &[], 0);
        let action = classify_snapshot(&s, None).expect("ok");
        assert_eq!(
            action,
            Some(LineageAction::CollectDeletes { snapshot_id: 7 })
        );
    }

    #[test]
    fn classify_snapshot_overwrite_is_rejected() {
        let s = snap(7, Some(1), Operation::Overwrite, &[], 0);
        let err = classify_snapshot(&s, None).expect_err("err");
        assert!(matches!(
            err,
            ChangeError::UnsupportedOperation { snapshot_id: 7, ref op } if op == "overwrite"
        ));
    }

    #[test]
    fn classify_snapshot_replace_compaction_is_skipped() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);
        let action = classify_snapshot(&s, Some(&parent)).expect("ok");
        assert_eq!(action, None);
    }

    #[test]
    fn classify_snapshot_replace_without_parent_is_rejected() {
        let owned = replace_props(100, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, None, Operation::Replace, &props, 0);
        let err = classify_snapshot(&s, None).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("parent"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_record_count_change_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(101, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("total-records"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_missing_total_records_is_rejected() {
        // Parent has total-records, REPLACE doesn't. Validator can't prove the
        // record count is unchanged → reject.
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let s = snap(
            2,
            Some(1),
            Operation::Replace,
            &[("added-data-files", "3"), ("deleted-data-files", "5")],
            0,
        );
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("total-records"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_missing_added_or_removed_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 0, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("added-data-files"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_schema_id_change_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        // schema_id 7 ≠ parent's 0.
        let s = snap(2, Some(1), Operation::Replace, &props, 7);
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("schema"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn plan_changes_collects_inserts_after_previous_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
            }],
            None,
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("first insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load first");
        let previous = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();

        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("second insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load second");
        let batch = plan_changes(&loaded.table, previous, &[]).expect("plan");
        assert_eq!(batch.previous_snapshot_id, previous);
        assert_eq!(
            batch.current_snapshot_id,
            loaded
                .table
                .metadata()
                .current_snapshot()
                .unwrap()
                .snapshot_id()
        );
        assert!(!batch.inserts.is_empty());
        assert!(batch.deletes.is_empty());
        let returned_rows: i64 = batch
            .inserts
            .iter()
            .map(|f| f.record_count.unwrap_or_default())
            .sum();
        assert_eq!(returned_rows, 1);
    }

    #[test]
    fn plan_changes_rejects_pruned_previous_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
            }],
            None,
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("first insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load first");
        let previous = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();

        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("second insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load second");

        let pruned_metadata = loaded
            .table
            .metadata()
            .clone()
            .into_builder(None)
            .remove_snapshots(&[previous])
            .build()
            .expect("pruned metadata")
            .metadata;
        let pruned_table = iceberg::table::Table::builder()
            .file_io(loaded.table.file_io().clone())
            .metadata(std::sync::Arc::new(pruned_metadata))
            .identifier(loaded.table.identifier().clone())
            .build()
            .expect("pruned table");

        let err = plan_changes(&pruned_table, previous, &[]).expect_err("should fail");
        assert!(
            matches!(err, ChangeError::LineageBroken { previous_snapshot } if previous_snapshot == previous)
        );
    }
}
