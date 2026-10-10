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

//! Pure metadata facts and canonical snapshot summary accounting.

use crate::iceberg::spec::{Summary, TableMetadata};
use std::collections::HashMap;

pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
        env!("CARGO_PKG_VERSION").to_string(),
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
