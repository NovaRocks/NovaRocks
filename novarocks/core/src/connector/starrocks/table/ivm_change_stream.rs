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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::connector::iceberg::changes::{ChangeError, IcebergChangeBatch, plan_changes};
use crate::connector::starrocks::table::mv_refresh::load_current_iceberg_base_table;
use crate::engine::StandaloneState;
use crate::mv::refresh::pin::RefreshSnapshotPin;
use crate::runtime::query_result::QueryResult;
use novarocks_catalog::identifier::TableIdentity;

// Compatibility wrapper for the older two-branch materialized change stream.
#[allow(dead_code)]
pub(crate) struct IvmChangeStream {
    pub(crate) previous_snapshot_id: i64,
    pub(crate) current_snapshot_id: i64,
    pub(crate) inserts: QueryResult,
    pub(crate) deletes: QueryResult,
}

#[allow(dead_code)]
pub(crate) struct MaterializedChanges {
    pub(crate) previous_snapshot_id: i64,
    pub(crate) current_snapshot_id: i64,
    pub(crate) inserts: QueryResult,
    pub(crate) deletes: QueryResult,
}

#[allow(dead_code)]
impl IvmChangeStream {
    pub(crate) fn from_materialized(changes: MaterializedChanges) -> Self {
        Self {
            previous_snapshot_id: changes.previous_snapshot_id,
            current_snapshot_id: changes.current_snapshot_id,
            inserts: changes.inserts,
            deletes: changes.deletes,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inserts.row_count() == 0 && self.deletes.row_count() == 0
    }

    pub(crate) fn into_results(self) -> (QueryResult, QueryResult) {
        (self.inserts, self.deletes)
    }
}

/// Plan an `IcebergChangeBatch` for a single base table pinned to
/// `expected_current_snapshot_id`. Thin wrapper over Layer 1 `plan_changes`
/// with the to_snapshot_id set explicitly from the pin; the previous
/// current-snapshot post-check is no longer needed since plan_changes itself
/// now writes the requested-to into the batch.
pub(crate) fn plan_iceberg_change_batch_for_ivm(
    base_table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    expected_current_snapshot_id: i64,
    pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    plan_changes(
        base_table,
        previous_snapshot_id,
        Some(expected_current_snapshot_id),
        pk_columns,
    )
}

/// Plan one `IcebergChangeBatch` per base table in `pin`, using
/// `last_refresh[base.fqn()]` as `from` and `pin.get(base)` as `to`.
/// Returns batches in iteration order of the pin (sorted by fqn).
///
/// Fails fast on the first base table that:
/// - is missing from `last_refresh` (no previous refresh recorded)
/// - cannot be loaded (catalog or io error)
/// - returns any `ChangeError` from `plan_changes`
#[allow(dead_code)]
pub(crate) fn plan_change_batches_for_pin(
    state: &Arc<StandaloneState>,
    pin: &RefreshSnapshotPin,
    last_refresh: &BTreeMap<String, i64>,
    pk_columns_by_base: &HashMap<TableIdentity, Vec<String>>,
) -> Result<Vec<(TableIdentity, IcebergChangeBatch)>, String> {
    let mut out = Vec::with_capacity(pin.len());
    for (fqn, pinned_snap) in pin.iter() {
        let base_ref = parse_fqn_to_iceberg_ref(fqn)?;
        let previous = last_refresh.get(fqn).copied().ok_or_else(|| {
            format!("plan_change_batches_for_pin: base table {fqn} missing from last_refresh")
        })?;
        let loaded = load_current_iceberg_base_table(state, &base_ref)?;
        let pk_default: Vec<String> = Vec::new();
        let pk_columns = pk_columns_by_base
            .iter()
            .find_map(|(base, columns)| (base == &base_ref).then_some(columns))
            .unwrap_or(&pk_default);
        let batch = plan_changes(&loaded.table, previous, Some(pinned_snap), pk_columns)
            .map_err(|e| format!("plan_change_batches_for_pin: {fqn}: {e}"))?;
        debug_assert_eq!(batch.current_snapshot_id, pinned_snap);
        out.push((base_ref, batch));
    }
    Ok(out)
}

#[allow(dead_code)]
fn parse_fqn_to_iceberg_ref(fqn: &str) -> Result<TableIdentity, String> {
    let parts: Vec<&str> = fqn.split('.').collect();
    if parts.len() != 3 {
        return Err(format!(
            "expected 3-part fqn '<catalog>.<namespace>.<table>', got '{fqn}'"
        ));
    }
    Ok(TableIdentity {
        catalog: parts[0].to_string(),
        namespace: parts[1].to_string(),
        table: parts[2].to_string(),
    })
}
