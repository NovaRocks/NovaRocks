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

//! Shared utilities for the self-implemented commit-actions
//! (`RowDeltaCommit` and `OverwriteCommit`).

use iceberg::io::FileIO;
use iceberg::spec::{FormatVersion, ManifestFile, ManifestListWriter, Summary, TableMetadata};
use std::collections::HashMap;

/// Generate an Iceberg-spec-compliant random positive snapshot id.
pub fn generate_snapshot_id() -> i64 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.r#gen::<i64>().abs()
}

/// Current wall-clock time in milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Resolve the metadata directory for a table — i.e. the directory containing
/// `metadata.json`, manifest-list, and manifest avro files.
pub fn metadata_dir(table: &iceberg::table::Table) -> String {
    format!("{}/metadata", table.metadata().location())
}

/// Read the current snapshot's `total-records` summary value.
///
/// `Ok(None)` means either the table has no current snapshot or the current
/// snapshot predates summary totals. A malformed value is an error because
/// write actions must not guess table-level metrics.
pub fn current_snapshot_total_records(
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Option<u64>, String> {
    snapshot_total_records(
        metadata,
        metadata.current_snapshot().map(|s| s.snapshot_id()),
    )
}

pub(super) fn target_ref_snapshot_id(metadata: &TableMetadata, target_ref: &str) -> Option<i64> {
    metadata
        .refs()
        .get(target_ref)
        .map(|r| r.snapshot_id)
        .or_else(|| {
            if target_ref == "main" {
                metadata.current_snapshot().map(|s| s.snapshot_id())
            } else {
                None
            }
        })
}

pub(super) fn required_target_ref_snapshot_id(
    metadata: &TableMetadata,
    target_ref: &str,
    operation: &str,
) -> Result<i64, String> {
    target_ref_snapshot_id(metadata, target_ref).ok_or_else(|| {
        format!("{operation} committed but target ref '{target_ref}' is not visible")
    })
}

pub(super) fn snapshot_summary(
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
) -> Result<Option<&Summary>, String> {
    let Some(snapshot_id) = snapshot_id else {
        return Ok(None);
    };
    metadata
        .snapshot_by_id(snapshot_id)
        .map(|snapshot| Some(snapshot.summary()))
        .ok_or_else(|| format!("snapshot {snapshot_id} not found in table metadata"))
}

pub(super) fn snapshot_total_records(
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
) -> Result<Option<u64>, String> {
    let Some(snapshot) = snapshot_summary(metadata, snapshot_id)? else {
        return Ok(None);
    };
    let Some(value) = snapshot.additional_properties.get("total-records") else {
        return Ok(None);
    };
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|e| format!("invalid current snapshot total-records `{value}`: {e}"))
}

/// Return the next unallocated Iceberg v3 row id.
///
/// Some catalog implementations do not echo the table-level `next-row-id`
/// update after custom row-lineage commits, but they do preserve each
/// snapshot's row-range. Treat the table-level value as a floor and derive the
/// effective value from the maximum `first_row_id + added_rows` in snapshots.
pub fn effective_next_row_id(metadata: &iceberg::spec::TableMetadata) -> Result<u64, String> {
    let mut next_row_id = metadata.next_row_id();
    for snapshot in metadata.snapshots() {
        if let Some((first_row_id, added_rows)) = snapshot.row_range() {
            let end = first_row_id.checked_add(added_rows).ok_or_else(|| {
                format!(
                    "row-range overflow while deriving next row id: snapshot_id={} first_row_id={} added_rows={}",
                    snapshot.snapshot_id(),
                    first_row_id,
                    added_rows
                )
            })?;
            next_row_id = next_row_id.max(end);
        }
    }
    Ok(next_row_id)
}

/// Write a manifest list (avro) to `out_path` containing the supplied entries.
/// Caller is responsible for `abort_handle.record_manifest(out_path)` before
/// invoking this function so that a later failure can clean up.
pub async fn write_manifest_list(
    file_io: &FileIO,
    out_path: &str,
    entries: Vec<ManifestFile>,
    snap_id: i64,
    parent_snap_id: Option<i64>,
    sequence_number: i64,
    format_version: FormatVersion,
    first_row_id: Option<u64>,
) -> Result<Option<u64>, String> {
    let output = file_io
        .new_output(out_path)
        .map_err(|e| format!("FileIO::new_output({out_path}) failed: {e}"))?;
    let mut writer = match format_version {
        FormatVersion::V1 => ManifestListWriter::v1(output, snap_id, parent_snap_id),
        FormatVersion::V2 => {
            ManifestListWriter::v2(output, snap_id, parent_snap_id, sequence_number)
        }
        FormatVersion::V3 => ManifestListWriter::v3(
            output,
            snap_id,
            parent_snap_id,
            sequence_number,
            first_row_id,
        ),
    };
    writer
        .add_manifests(entries.into_iter())
        .map_err(|e| format!("ManifestListWriter::add_manifests failed: {e}"))?;
    let next_row_id = writer.next_row_id();
    writer
        .close()
        .await
        .map_err(|e| format!("ManifestListWriter::close failed: {e}"))?;
    Ok(next_row_id)
}

/// Read the manifest list referenced by `current_snapshot()` and return its
/// `ManifestFile` entries. Returns an empty Vec if the table has no current
/// snapshot.
pub async fn read_base_manifest_list(
    table: &iceberg::table::Table,
    file_io: &FileIO,
) -> Result<Vec<ManifestFile>, String> {
    let m = table.metadata();
    let snapshot_id = m.current_snapshot().map(|s| s.snapshot_id());
    read_snapshot_manifest_list(m, file_io, snapshot_id).await
}

pub(super) async fn read_snapshot_manifest_list(
    metadata: &TableMetadata,
    file_io: &FileIO,
    snapshot_id: Option<i64>,
) -> Result<Vec<ManifestFile>, String> {
    let Some(snapshot_id) = snapshot_id else {
        return Ok(Vec::new());
    };
    let snap = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found in table metadata"))?;
    let bytes = file_io
        .new_input(snap.manifest_list())
        .map_err(|e| format!("FileIO::new_input({}) failed: {e}", snap.manifest_list()))?
        .read()
        .await
        .map_err(|e| format!("read manifest_list failed: {e}"))?;
    let list = iceberg::spec::ManifestList::parse_with_version(&bytes, metadata.format_version())
        .map_err(|e| format!("parse manifest_list failed: {e}"))?;
    Ok(list.entries().to_vec())
}

// ---------------------------------------------------------------------------
// Snapshot-summary `total-*` carry-forward (IV3-2).
//
// Canonical Iceberg summary key names. Mirrors the constants in
// `vendor/iceberg-0.9.0/src/spec/snapshot_summary.rs`.
// ---------------------------------------------------------------------------
const TOTAL_DATA_FILES: &str = "total-data-files";
const TOTAL_DELETE_FILES: &str = "total-delete-files";
const TOTAL_RECORDS: &str = "total-records";
const TOTAL_FILE_SIZE: &str = "total-files-size";
const TOTAL_POSITION_DELETES: &str = "total-position-deletes";
const TOTAL_EQUALITY_DELETES: &str = "total-equality-deletes";

const ADDED_DATA_FILES: &str = "added-data-files";
const DELETED_DATA_FILES: &str = "deleted-data-files";
const ADDED_DELETE_FILES: &str = "added-delete-files";
const REMOVED_DELETE_FILES: &str = "removed-delete-files";
const ADDED_RECORDS: &str = "added-records";
const DELETED_RECORDS: &str = "deleted-records";
const ADDED_FILE_SIZE: &str = "added-files-size";
const REMOVED_FILE_SIZE: &str = "removed-files-size";
const ADDED_POSITION_DELETES: &str = "added-position-deletes";
const REMOVED_POSITION_DELETES: &str = "removed-position-deletes";
const ADDED_EQUALITY_DELETES: &str = "added-equality-deletes";
const REMOVED_EQUALITY_DELETES: &str = "removed-equality-deletes";

const ENGINE_NAME_KEY: &str = "engine-name";
const ENGINE_VERSION_KEY: &str = "engine-version";
const ENGINE_NAME_VALUE: &str = "novarocks";

/// Carry forward the six Iceberg `total-*` summary fields and stamp NovaRocks
/// engine identity, returning the finalized snapshot-summary property map.
///
/// For each category, `total = previous_total + added - removed`, reading the
/// canonical `added-*` / `removed-*` / `deleted-*` keys the caller already
/// populated. Semantics mirror Iceberg-Java `SnapshotSummary` (and therefore
/// Spark), the cross-engine reference:
///
/// * First snapshot (`previous == None`): base 0, so `total == added`.
/// * `previous` present but missing a given `total-*` (legacy / foreign
///   writer): that total is OMITTED — we never fabricate a total we cannot
///   resume. (This intentionally differs from iceberg-rust 0.9.0
///   `update_totals`, which treats a missing previous total as 0.)
/// * `truncate_full_table`: every `total-*` resets to 0.
///
/// Engine identity (`engine-name`/`engine-version`) is always stamped.
pub(super) fn finalize_snapshot_summary(
    mut props: HashMap<String, String>,
    previous: Option<&Summary>,
    truncate_full_table: bool,
) -> HashMap<String, String> {
    if truncate_full_table {
        for key in [
            TOTAL_DATA_FILES,
            TOTAL_DELETE_FILES,
            TOTAL_RECORDS,
            TOTAL_FILE_SIZE,
            TOTAL_POSITION_DELETES,
            TOTAL_EQUALITY_DELETES,
        ] {
            props.insert(key.to_string(), "0".to_string());
        }
    } else {
        carry_total(
            &mut props,
            previous,
            TOTAL_DATA_FILES,
            ADDED_DATA_FILES,
            DELETED_DATA_FILES,
        );
        carry_total(
            &mut props,
            previous,
            TOTAL_DELETE_FILES,
            ADDED_DELETE_FILES,
            REMOVED_DELETE_FILES,
        );
        carry_total(
            &mut props,
            previous,
            TOTAL_RECORDS,
            ADDED_RECORDS,
            DELETED_RECORDS,
        );
        carry_total(
            &mut props,
            previous,
            TOTAL_FILE_SIZE,
            ADDED_FILE_SIZE,
            REMOVED_FILE_SIZE,
        );
        carry_total(
            &mut props,
            previous,
            TOTAL_POSITION_DELETES,
            ADDED_POSITION_DELETES,
            REMOVED_POSITION_DELETES,
        );
        carry_total(
            &mut props,
            previous,
            TOTAL_EQUALITY_DELETES,
            ADDED_EQUALITY_DELETES,
            REMOVED_EQUALITY_DELETES,
        );
    }
    props.insert(ENGINE_NAME_KEY.to_string(), ENGINE_NAME_VALUE.to_string());
    props.insert(
        ENGINE_VERSION_KEY.to_string(),
        crate::version::short_version().to_string(),
    );
    props
}

fn parse_u64_prop(props: &HashMap<String, String>, key: &str) -> u64 {
    props
        .get(key)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

fn carry_total(
    props: &mut HashMap<String, String>,
    previous: Option<&Summary>,
    total_key: &str,
    added_key: &str,
    removed_key: &str,
) {
    let base = match previous {
        None => 0u64,
        Some(prev) => match prev.additional_properties.get(total_key) {
            Some(value) => match value.parse::<u64>() {
                Ok(parsed) => parsed,
                Err(_) => return,
            },
            None => return,
        },
    };
    let added = parse_u64_prop(props, added_key);
    let removed = parse_u64_prop(props, removed_key);
    let total = base.saturating_add(added).saturating_sub(removed);
    props.insert(total_key.to_string(), total.to_string());
}

#[cfg(test)]
mod summary_tests {
    use super::*;
    use iceberg::spec::{Operation, Summary};
    use std::collections::HashMap;

    fn prev(props: &[(&str, &str)]) -> Summary {
        Summary {
            operation: Operation::Append,
            additional_properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn first_snapshot_establishes_totals_from_added() {
        let out = finalize_snapshot_summary(
            props(&[
                ("added-data-files", "3"),
                ("added-records", "30"),
                ("added-files-size", "300"),
            ]),
            None,
            false,
        );
        assert_eq!(out.get("total-data-files").unwrap(), "3");
        assert_eq!(out.get("total-records").unwrap(), "30");
        assert_eq!(out.get("total-files-size").unwrap(), "300");
        assert_eq!(out.get("total-delete-files").unwrap(), "0");
        assert_eq!(out.get("total-position-deletes").unwrap(), "0");
        assert_eq!(out.get("total-equality-deletes").unwrap(), "0");
        assert_eq!(out.get("engine-name").unwrap(), "novarocks");
        assert!(out.get("engine-version").unwrap().starts_with("novarocks-"));
    }

    #[test]
    fn carry_forward_adds_and_subtracts() {
        let previous = prev(&[
            ("total-data-files", "10"),
            ("total-records", "100"),
            ("total-files-size", "1000"),
            ("total-delete-files", "0"),
            ("total-position-deletes", "0"),
            ("total-equality-deletes", "0"),
        ]);
        let out = finalize_snapshot_summary(
            props(&[
                ("added-data-files", "2"),
                ("deleted-data-files", "1"),
                ("added-records", "20"),
                ("deleted-records", "5"),
                ("added-files-size", "200"),
                ("removed-files-size", "100"),
            ]),
            Some(&previous),
            false,
        );
        assert_eq!(out.get("total-data-files").unwrap(), "11");
        assert_eq!(out.get("total-records").unwrap(), "115");
        assert_eq!(out.get("total-files-size").unwrap(), "1100");
    }

    #[test]
    fn legacy_missing_total_is_omitted_not_fabricated() {
        let previous = prev(&[("total-records", "100")]);
        let out = finalize_snapshot_summary(
            props(&[("added-data-files", "2"), ("added-records", "20")]),
            Some(&previous),
            false,
        );
        assert!(!out.contains_key("total-data-files"));
        assert_eq!(out.get("total-records").unwrap(), "120");
    }

    #[test]
    fn truncate_resets_all_totals_to_zero() {
        let previous = prev(&[
            ("total-data-files", "10"),
            ("total-records", "100"),
            ("total-files-size", "1000"),
        ]);
        let out = finalize_snapshot_summary(
            props(&[("deleted-data-files", "10"), ("deleted-records", "100")]),
            Some(&previous),
            true,
        );
        for k in [
            "total-data-files",
            "total-delete-files",
            "total-records",
            "total-files-size",
            "total-position-deletes",
            "total-equality-deletes",
        ] {
            assert_eq!(out.get(k).map(String::as_str), Some("0"), "{k} must be 0");
        }
    }

    #[test]
    fn removed_below_zero_saturates() {
        let previous = prev(&[("total-records", "5")]);
        let out =
            finalize_snapshot_summary(props(&[("deleted-records", "9")]), Some(&previous), false);
        assert_eq!(out.get("total-records").unwrap(), "0");
    }
}
