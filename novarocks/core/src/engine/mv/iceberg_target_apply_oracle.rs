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

//! Differential oracle for Iceberg MV target-row location.
//!
//! The production apply path locates old target rows *in-plan*: the refresh
//! framework scans the target, emits `_file`/`_pos` plus the apply key, and
//! joins on the apply key (the W1/W3/W4/W5 cutover). The direct iceberg-rust
//! scan locators below are retained only as test oracles. This entire module is
//! gated by `engine::mv::mod`, so release builds cannot compile or call these
//! direct-scan helpers.

#[cfg(test)]
use crate::mv::model::TargetPartitionFilter;

#[cfg(test)]
use crate::mv::persistence::schema::{
    APPLY_KEY_COLUMN_PROPERTY, APPLY_KEY_FIELD_ID_PROPERTY, APPLY_KEY_SOURCE_PROPERTY,
    ApplyKeySource, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
};
use crate::mv::persistence::schema::{
    BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME, JOIN_APPLY_KEY_COLUMN_NAME,
};
use crate::mv::refresh::target_apply::{
    apply_key_table_column, branch_id_table_column,
    expose_physical_apply_key_for_locator_registration, iceberg_mv_physical_select_sql,
    join_apply_key_table_column,
};

#[cfg(test)]
pub(crate) fn load_target_apply_locator_inputs(
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_table: &iceberg::table::Table,
) -> Result<
    (
        crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
        crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    ),
    String,
> {
    let snapshot_id = target_table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    let existing_deletes_by_file =
        crate::connector::iceberg::delete_visibility::load_existing_delete_visibility_by_data_file_at(
            target_table,
            snapshot_id,
            target_entry.object_store_config(),
        )?;
    if existing_deletes_by_file
        .values()
        .any(|visibility| !visibility.equality_deletes.is_empty())
    {
        return Err(
            "iceberg MV target row locator cannot apply on a target snapshot with equality deletes; compact the target first"
                .to_string(),
        );
    }
    let referenced_data_file_partitions =
        crate::connector::iceberg::delete_visibility::load_referenced_data_file_partitions_at(
            target_table,
            snapshot_id,
        )?;
    Ok((existing_deletes_by_file, referenced_data_file_partitions))
}

#[cfg(test)]
pub(crate) async fn locate_target_rows_by_apply_key(
    target_table: &iceberg::table::Table,
    base_row_ids: &[i64],
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(locate_target_rows_by_apply_key_with_matches(
        target_table,
        base_row_ids,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await?
    .delete_groups)
}

#[cfg(test)]
pub(crate) async fn locate_target_rows_by_apply_key_with_matches(
    target_table: &iceberg::table::Table,
    base_row_ids: &[i64],
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        HIDDEN_APPLY_KEY_COLUMN_NAME,
        ApplyKeyRequest::Int64(base_row_ids),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn locate_target_rows_by_string_apply_key(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[String],
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(locate_target_rows_by_string_apply_key_with_matches(
        target_table,
        apply_key_column,
        requested_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await?
    .delete_groups)
}

#[cfg(test)]
pub(crate) async fn locate_target_rows_by_string_apply_key_with_matches(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[String],
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        apply_key_column,
        ApplyKeyRequest::Utf8(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BranchApplyKey {
    pub branch_id: i32,
    pub base_row_id: i64,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BranchStringApplyKey {
    pub branch_id: i32,
    pub key: String,
}

#[cfg(test)]
pub(crate) async fn locate_target_rows_by_branch_apply_key(
    target_table: &iceberg::table::Table,
    requested_keys: &[BranchApplyKey],
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(locate_target_rows_by_branch_apply_key_with_matches(
        target_table,
        requested_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await?
    .delete_groups)
}

#[cfg(test)]
pub(crate) async fn locate_target_rows_by_branch_apply_key_with_matches(
    target_table: &iceberg::table::Table,
    requested_keys: &[BranchApplyKey],
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        HIDDEN_APPLY_KEY_COLUMN_NAME,
        ApplyKeyRequest::BranchInt64(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn locate_target_rows_by_branch_string_apply_key(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[BranchStringApplyKey],
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(locate_target_rows_by_branch_string_apply_key_with_matches(
        target_table,
        apply_key_column,
        requested_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await?
    .delete_groups)
}

#[cfg(test)]
pub(crate) async fn locate_target_rows_by_branch_string_apply_key_with_matches(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[BranchStringApplyKey],
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        apply_key_column,
        ApplyKeyRequest::BranchUtf8(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum ApplyKeyRequest<'a> {
    Int64(&'a [i64]),
    Utf8(&'a [String]),
    BranchInt64(&'a [BranchApplyKey]),
    BranchUtf8(&'a [BranchStringApplyKey]),
}

#[cfg(test)]
impl ApplyKeyRequest<'_> {
    fn is_empty(&self) -> bool {
        match self {
            Self::Int64(keys) => keys.is_empty(),
            Self::Utf8(keys) => keys.is_empty(),
            Self::BranchInt64(keys) => keys.is_empty(),
            Self::BranchUtf8(keys) => keys.is_empty(),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ApplyKeyValue {
    Int64(i64),
    Utf8(String),
    BranchInt64(BranchApplyKey),
    BranchUtf8(BranchStringApplyKey),
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetRowPositionSet {
    pub(crate) referenced_data_file: String,
    pub(crate) positions: Vec<i64>,
}

#[cfg(test)]
pub(crate) struct TargetApplyLocatorResult {
    pub(crate) delete_groups: Vec<crate::connector::iceberg::commit::PositionDeleteGroup>,
    pub(crate) matched_positions: Vec<TargetRowPositionSet>,
}

#[cfg(test)]
impl std::fmt::Display for ApplyKeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int64(value) => write!(f, "{value}"),
            Self::Utf8(value) => write!(f, "{value}"),
            Self::BranchInt64(value) => {
                write!(
                    f,
                    "branch {} apply key {}",
                    value.branch_id, value.base_row_id
                )
            }
            Self::BranchUtf8(value) => {
                write!(f, "branch {} apply key {}", value.branch_id, value.key)
            }
        }
    }
}

#[cfg(test)]
fn requested_apply_key_values(
    requested_keys: ApplyKeyRequest<'_>,
) -> std::collections::HashSet<ApplyKeyValue> {
    match requested_keys {
        ApplyKeyRequest::Int64(keys) => keys
            .iter()
            .copied()
            .map(ApplyKeyValue::Int64)
            .collect::<std::collections::HashSet<_>>(),
        ApplyKeyRequest::Utf8(keys) => keys
            .iter()
            .cloned()
            .map(ApplyKeyValue::Utf8)
            .collect::<std::collections::HashSet<_>>(),
        ApplyKeyRequest::BranchInt64(keys) => keys
            .iter()
            .copied()
            .map(ApplyKeyValue::BranchInt64)
            .collect::<std::collections::HashSet<_>>(),
        ApplyKeyRequest::BranchUtf8(keys) => keys
            .iter()
            .cloned()
            .map(ApplyKeyValue::BranchUtf8)
            .collect::<std::collections::HashSet<_>>(),
    }
}

#[cfg(test)]
fn record_visible_apply_key_match(
    matches: &mut std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    requested: &std::collections::HashSet<ApplyKeyValue>,
    key: ApplyKeyValue,
    file: &str,
    pos: i64,
) -> Result<(), String> {
    if !requested.contains(&key) {
        return Ok(());
    }
    if matches
        .insert(key.clone(), (file.to_string(), pos))
        .is_some()
    {
        return Err(format!(
            "iceberg MV target has duplicate rows for apply key {key}"
        ));
    }
    Ok(())
}

#[cfg(test)]
fn ensure_all_requested_apply_keys_matched(
    requested: &std::collections::HashSet<ApplyKeyValue>,
    matches: &std::collections::HashMap<ApplyKeyValue, (String, i64)>,
) -> Result<(), String> {
    for key in requested {
        if !matches.contains_key(key) {
            return Err(format!(
                "iceberg MV target row not found for apply key {key}"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
fn process_apply_key_locator_batch(
    batch: &arrow::record_batch::RecordBatch,
    apply_key_column: &str,
    request_is_i64: bool,
    requested: &std::collections::HashSet<ApplyKeyValue>,
    matches: &mut std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
) -> Result<(), String> {
    use arrow::array::{Array, Int64Array, StringArray};

    let schema = batch.schema();
    let file_idx = schema
        .index_of("_file")
        .map_err(|e| format!("iceberg MV target locator scan missing _file: {e}"))?;
    let pos_idx = schema
        .index_of("_pos")
        .map_err(|e| format!("iceberg MV target locator scan missing _pos: {e}"))?;
    let key_idx = schema
        .index_of(apply_key_column)
        .map_err(|e| format!("iceberg MV target locator scan missing {apply_key_column}: {e}"))?;
    let file_col = arrow::compute::cast(batch.column(file_idx), &arrow::datatypes::DataType::Utf8)
        .map_err(|e| format!("cast target _file to STRING failed: {e}"))?;
    let pos_col = arrow::compute::cast(batch.column(pos_idx), &arrow::datatypes::DataType::Int64)
        .map_err(|e| format!("cast target _pos to BIGINT failed: {e}"))?;
    let files = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "target _file is not STRING after cast".to_string())?;
    let positions = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "target _pos is not BIGINT after cast".to_string())?;
    if request_is_i64 {
        let key_col =
            arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Int64)
                .map_err(|e| format!("cast target {apply_key_column} to BIGINT failed: {e}"))?;
        let keys = key_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| format!("target {apply_key_column} is not BIGINT after cast"))?;
        for row in 0..batch.num_rows() {
            if files.is_null(row) || positions.is_null(row) || keys.is_null(row) {
                continue;
            }
            let file = files.value(row);
            let pos = positions.value(row);
            if !crate::connector::iceberg::delete_visibility::data_file_row_is_visible(
                batch,
                row,
                file,
                pos,
                existing_deletes_by_file,
            )? {
                continue;
            }
            record_visible_apply_key_match(
                matches,
                requested,
                ApplyKeyValue::Int64(keys.value(row)),
                file,
                pos,
            )?;
        }
    } else {
        let key_col =
            arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Utf8)
                .map_err(|e| format!("cast target {apply_key_column} to STRING failed: {e}"))?;
        let keys = key_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| format!("target {apply_key_column} is not STRING after cast"))?;
        for row in 0..batch.num_rows() {
            if files.is_null(row) || positions.is_null(row) || keys.is_null(row) {
                continue;
            }
            let file = files.value(row);
            let pos = positions.value(row);
            if !crate::connector::iceberg::delete_visibility::data_file_row_is_visible(
                batch,
                row,
                file,
                pos,
                existing_deletes_by_file,
            )? {
                continue;
            }
            record_visible_apply_key_match(
                matches,
                requested,
                ApplyKeyValue::Utf8(keys.value(row).to_string()),
                file,
                pos,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
fn process_branch_i64_apply_key_locator_batch(
    batch: &arrow::record_batch::RecordBatch,
    requested: &std::collections::HashSet<ApplyKeyValue>,
    matches: &mut std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
) -> Result<(), String> {
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};

    let schema = batch.schema();
    let file_idx = schema
        .index_of("_file")
        .map_err(|e| format!("iceberg MV target locator scan missing _file: {e}"))?;
    let pos_idx = schema
        .index_of("_pos")
        .map_err(|e| format!("iceberg MV target locator scan missing _pos: {e}"))?;
    let branch_idx = schema.index_of(BRANCH_ID_COLUMN_NAME).map_err(|e| {
        format!("iceberg MV target locator scan missing {BRANCH_ID_COLUMN_NAME}: {e}")
    })?;
    let key_idx = schema.index_of(HIDDEN_APPLY_KEY_COLUMN_NAME).map_err(|e| {
        format!("iceberg MV target locator scan missing {HIDDEN_APPLY_KEY_COLUMN_NAME}: {e}")
    })?;
    let file_col = arrow::compute::cast(batch.column(file_idx), &arrow::datatypes::DataType::Utf8)
        .map_err(|e| format!("cast target _file to STRING failed: {e}"))?;
    let pos_col = arrow::compute::cast(batch.column(pos_idx), &arrow::datatypes::DataType::Int64)
        .map_err(|e| format!("cast target _pos to BIGINT failed: {e}"))?;
    let branch_col =
        arrow::compute::cast(batch.column(branch_idx), &arrow::datatypes::DataType::Int32)
            .map_err(|e| format!("cast target {BRANCH_ID_COLUMN_NAME} to INT failed: {e}"))?;
    let key_col = arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Int64)
        .map_err(|e| {
        format!("cast target {HIDDEN_APPLY_KEY_COLUMN_NAME} to BIGINT failed: {e}")
    })?;
    let files = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "target _file is not STRING after cast".to_string())?;
    let positions = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "target _pos is not BIGINT after cast".to_string())?;
    let branches = branch_col
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| format!("target {BRANCH_ID_COLUMN_NAME} is not INT after cast"))?;
    let keys = key_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("target {HIDDEN_APPLY_KEY_COLUMN_NAME} is not BIGINT after cast"))?;

    for row in 0..batch.num_rows() {
        if files.is_null(row)
            || positions.is_null(row)
            || branches.is_null(row)
            || keys.is_null(row)
        {
            continue;
        }
        let file = files.value(row);
        let pos = positions.value(row);
        if !crate::connector::iceberg::delete_visibility::data_file_row_is_visible(
            batch,
            row,
            file,
            pos,
            existing_deletes_by_file,
        )? {
            continue;
        }
        record_visible_apply_key_match(
            matches,
            requested,
            ApplyKeyValue::BranchInt64(BranchApplyKey {
                branch_id: branches.value(row),
                base_row_id: keys.value(row),
            }),
            file,
            pos,
        )?;
    }
    Ok(())
}

#[cfg(test)]
fn process_branch_utf8_apply_key_locator_batch(
    batch: &arrow::record_batch::RecordBatch,
    apply_key_column: &str,
    requested: &std::collections::HashSet<ApplyKeyValue>,
    matches: &mut std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
) -> Result<(), String> {
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};

    let schema = batch.schema();
    let file_idx = schema
        .index_of("_file")
        .map_err(|e| format!("iceberg MV target locator scan missing _file: {e}"))?;
    let pos_idx = schema
        .index_of("_pos")
        .map_err(|e| format!("iceberg MV target locator scan missing _pos: {e}"))?;
    let branch_idx = schema.index_of(BRANCH_ID_COLUMN_NAME).map_err(|e| {
        format!("iceberg MV target locator scan missing {BRANCH_ID_COLUMN_NAME}: {e}")
    })?;
    let key_idx = schema
        .index_of(apply_key_column)
        .map_err(|e| format!("iceberg MV target locator scan missing {apply_key_column}: {e}"))?;
    let file_col = arrow::compute::cast(batch.column(file_idx), &arrow::datatypes::DataType::Utf8)
        .map_err(|e| format!("cast target _file to STRING failed: {e}"))?;
    let pos_col = arrow::compute::cast(batch.column(pos_idx), &arrow::datatypes::DataType::Int64)
        .map_err(|e| format!("cast target _pos to BIGINT failed: {e}"))?;
    let branch_col =
        arrow::compute::cast(batch.column(branch_idx), &arrow::datatypes::DataType::Int32)
            .map_err(|e| format!("cast target {BRANCH_ID_COLUMN_NAME} to INT failed: {e}"))?;
    let key_col = arrow::compute::cast(batch.column(key_idx), &arrow::datatypes::DataType::Utf8)
        .map_err(|e| format!("cast target {apply_key_column} to STRING failed: {e}"))?;
    let files = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "target _file is not STRING after cast".to_string())?;
    let positions = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "target _pos is not BIGINT after cast".to_string())?;
    let branches = branch_col
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| format!("target {BRANCH_ID_COLUMN_NAME} is not INT after cast"))?;
    let keys = key_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("target {apply_key_column} is not STRING after cast"))?;

    for row in 0..batch.num_rows() {
        if files.is_null(row)
            || positions.is_null(row)
            || branches.is_null(row)
            || keys.is_null(row)
        {
            continue;
        }
        let file = files.value(row);
        let pos = positions.value(row);
        if !crate::connector::iceberg::delete_visibility::data_file_row_is_visible(
            batch,
            row,
            file,
            pos,
            existing_deletes_by_file,
        )? {
            continue;
        }
        record_visible_apply_key_match(
            matches,
            requested,
            ApplyKeyValue::BranchUtf8(BranchStringApplyKey {
                branch_id: branches.value(row),
                key: keys.value(row).to_string(),
            }),
            file,
            pos,
        )?;
    }
    Ok(())
}

#[cfg(test)]
fn build_position_delete_groups_from_apply_key_matches(
    matches: std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    Ok(build_target_apply_locator_result_from_apply_key_matches(
        matches,
        referenced_data_file_partitions,
    )?
    .delete_groups)
}

#[cfg(test)]
fn build_target_apply_locator_result_from_apply_key_matches(
    matches: std::collections::HashMap<ApplyKeyValue, (String, i64)>,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
) -> Result<TargetApplyLocatorResult, String> {
    let mut by_file = std::collections::BTreeMap::<String, Vec<i64>>::new();
    for (_key, (file, pos)) in matches {
        by_file.entry(file).or_default().push(pos);
    }

    let mut delete_groups = Vec::with_capacity(by_file.len());
    let mut matched_positions = Vec::with_capacity(by_file.len());
    for (referenced_data_file, mut positions) in by_file {
        positions.sort_unstable();
        let partition = referenced_data_file_partitions
            .get(&referenced_data_file)
            .ok_or_else(|| {
                format!(
                    "matched iceberg MV target data file `{referenced_data_file}` is missing partition metadata"
                )
            })?;
        matched_positions.push(TargetRowPositionSet {
            referenced_data_file: referenced_data_file.clone(),
            positions: positions.clone(),
        });
        delete_groups.push(crate::connector::iceberg::commit::PositionDeleteGroup {
            referenced_data_file,
            partition_spec_id: partition.partition_spec_id,
            partition_values: partition.partition_values.clone(),
            positions,
        });
    }

    Ok(TargetApplyLocatorResult {
        delete_groups,
        matched_positions,
    })
}

#[cfg(test)]
pub(crate) fn resolve_target_positions_via_framework(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    target_table: &iceberg::table::Table,
    target_catalog_name: &str,
    target_namespace: &str,
    target_table_name: &str,
    apply_key_column: &str,
    requested_keys: ApplyKeyRequest<'_>,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    if requested_keys.is_empty() {
        return Ok(TargetApplyLocatorResult {
            delete_groups: Vec::new(),
            matched_positions: Vec::new(),
        });
    }
    let requested = requested_apply_key_values(requested_keys);
    let request_is_i64 = matches!(requested_keys, ApplyKeyRequest::Int64(_));
    let (locator_registration, sql) = register_scoped_framework_locator_table_for_query(
        state,
        target_table,
        target_catalog_name,
        target_namespace,
        target_table_name,
        apply_key_column,
        requested_keys,
        partition_filter,
    )?;
    let session = crate::engine::StandaloneSession {
        inner: std::sync::Arc::clone(state),
    };
    let result = session.execute_in_context(&sql, None, target_namespace, None);
    let result = result?;
    locator_registration.cleanup()?;
    let result = match result {
        crate::engine::StatementResult::Query(result) => result,
        crate::engine::StatementResult::Ok => {
            return Err("framework target locator SELECT returned no rows schema".to_string());
        }
    };

    let empty_deletes_by_file = std::collections::HashMap::new();
    let mut matches = std::collections::HashMap::<ApplyKeyValue, (String, i64)>::new();
    for chunk in &result.chunks {
        if matches!(requested_keys, ApplyKeyRequest::BranchInt64(_)) {
            process_branch_i64_apply_key_locator_batch(
                &chunk.batch,
                &requested,
                &mut matches,
                &empty_deletes_by_file,
            )?;
        } else if matches!(requested_keys, ApplyKeyRequest::BranchUtf8(_)) {
            process_branch_utf8_apply_key_locator_batch(
                &chunk.batch,
                apply_key_column,
                &requested,
                &mut matches,
                &empty_deletes_by_file,
            )?;
        } else {
            process_apply_key_locator_batch(
                &chunk.batch,
                apply_key_column,
                request_is_i64,
                &requested,
                &mut matches,
                &empty_deletes_by_file,
            )?;
        }
    }

    ensure_all_requested_apply_keys_matched(&requested, &matches)?;
    build_target_apply_locator_result_from_apply_key_matches(
        matches,
        referenced_data_file_partitions,
    )
}

#[cfg(test)]
fn register_scoped_framework_locator_table_for_query(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    target_table: &iceberg::table::Table,
    target_catalog_name: &str,
    target_namespace: &str,
    target_table_name: &str,
    apply_key_column: &str,
    requested_keys: ApplyKeyRequest<'_>,
    partition_filter: &TargetPartitionFilter,
) -> Result<(ScopedFrameworkLocatorTable, String), String> {
    for _ in 0..1024 {
        let locator_table_name =
            next_framework_locator_synthetic_table_name(target_table_name, target_table);
        let table_def = build_locator_visible_target_table_def(
            state,
            target_table,
            target_catalog_name,
            target_namespace,
            &locator_table_name,
            apply_key_column,
            partition_filter,
        )?;
        let sql = framework_locator_select_sql(
            target_namespace,
            &locator_table_name,
            apply_key_column,
            requested_keys,
        )?;
        if let Some(registration) =
            try_register_scoped_framework_locator_table(state, target_namespace, table_def)?
        {
            return Ok((registration, sql));
        }
    }
    Err(
        "framework target locator could not allocate a collision-free synthetic table name"
            .to_string(),
    )
}

#[cfg(test)]
fn try_register_scoped_framework_locator_table(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    namespace: &str,
    table_def: crate::sql::planner::table::TableDef,
) -> Result<Option<ScopedFrameworkLocatorTable>, String> {
    let table_name = table_def.name.clone();
    let mut catalog = state
        .catalog_service
        .local()
        .write()
        .map_err(|e| format!("standalone catalog write lock: {e}"))?;
    catalog
        .create_database(namespace)
        .map_err(|e| format!("create framework target locator database: {e}"))?;
    match catalog.get(namespace, &table_name) {
        Ok(_) => return Ok(None),
        Err(err) if err.contains("unknown table") => {}
        Err(err) => {
            return Err(format!(
                "check framework target locator synthetic table collision: {err}"
            ));
        }
    }
    let ownership_fingerprint = FrameworkLocatorTableFingerprint::from_table_def(&table_def);
    catalog
        .register(namespace, table_def)
        .map_err(|e| format!("register framework target locator synthetic table: {e}"))?;
    Ok(Some(ScopedFrameworkLocatorTable {
        state: std::sync::Arc::clone(state),
        namespace: namespace.to_string(),
        table: table_name,
        ownership_fingerprint,
        active: true,
    }))
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
struct FrameworkLocatorTableFingerprint {
    columns: Vec<novarocks_catalog::schema::ColumnDef>,
    iceberg_row_lineage_metadata_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    source_debug: String,
}

#[cfg(test)]
impl FrameworkLocatorTableFingerprint {
    fn from_table_def(table_def: &crate::sql::planner::table::TableDef) -> Self {
        Self {
            columns: table_def.columns.clone(),
            iceberg_row_lineage_metadata_columns: table_def
                .iceberg_row_lineage_metadata_columns
                .clone(),
            source_debug: format!("{:?}", table_def.source),
        }
    }

    fn matches_table_def(&self, table_def: &crate::sql::planner::table::TableDef) -> bool {
        self.columns == table_def.columns
            && self.iceberg_row_lineage_metadata_columns
                == table_def.iceberg_row_lineage_metadata_columns
            && self.source_debug == format!("{:?}", table_def.source)
    }
}

#[cfg(test)]
struct ScopedFrameworkLocatorTable {
    state: std::sync::Arc<crate::engine::StandaloneState>,
    namespace: String,
    table: String,
    ownership_fingerprint: FrameworkLocatorTableFingerprint,
    active: bool,
}

#[cfg(test)]
impl ScopedFrameworkLocatorTable {
    fn cleanup(mut self) -> Result<(), String> {
        self.cleanup_active()
    }

    fn cleanup_active(&mut self) -> Result<(), String> {
        if !self.active {
            return Ok(());
        }

        let mut catalog = self
            .state
            .catalog_service
            .local()
            .write()
            .map_err(|e| format!("standalone catalog write lock: {e}"))?;
        let current_table = match catalog.get(&self.namespace, &self.table) {
            Ok(table_def) => table_def,
            Err(err) if err.contains("unknown") => {
                self.active = false;
                return Ok(());
            }
            Err(err) => {
                return Err(format!(
                    "check framework target locator synthetic table cleanup ownership: {err}"
                ));
            }
        };

        if !self.ownership_fingerprint.matches_table_def(&current_table) {
            self.active = false;
            return Ok(());
        }

        match catalog.drop_table(&self.namespace, &self.table) {
            Ok(()) => {
                self.active = false;
                Ok(())
            }
            Err(err) if err.contains("unknown") => {
                self.active = false;
                Ok(())
            }
            Err(err) => Err(format!(
                "drop framework target locator synthetic table: {err}"
            )),
        }
    }
}

#[cfg(test)]
impl Drop for ScopedFrameworkLocatorTable {
    fn drop(&mut self) {
        if self.active {
            let _ = self.cleanup_active();
        }
    }
}

#[cfg(test)]
fn next_framework_locator_synthetic_table_name(
    target_table_name: &str,
    target_table: &iceberg::table::Table,
) -> String {
    static NEXT_LOCATOR_TABLE_ID: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    let snapshot = target_table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id().to_string().replace('-', "m"))
        .unwrap_or_else(|| "no_snapshot".to_string());
    let target = framework_locator_identifier_token(target_table_name);
    let nonce = NEXT_LOCATOR_TABLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("__nova_mv_locator_{target}_{snapshot}_{nonce}")
}

#[cfg(test)]
fn framework_locator_identifier_token(identifier: &str) -> String {
    let mut out = String::with_capacity(identifier.len().max(1));
    for ch in identifier.chars() {
        if ch == '_' || ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "target".to_string()
    } else {
        out
    }
}

#[cfg(test)]
fn build_locator_visible_target_table_def(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    target_table: &iceberg::table::Table,
    target_catalog_name: &str,
    target_namespace: &str,
    target_table_name: &str,
    apply_key_column: &str,
    partition_filter: &TargetPartitionFilter,
) -> Result<crate::sql::planner::table::TableDef, String> {
    let entry = framework_locator_catalog_entry(state, target_catalog_name, target_table)?;
    let files = match target_table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id())
    {
        Some(snapshot_id) => {
            crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
                target_table,
                snapshot_id,
            )?
        }
        None => Vec::new(),
    };
    let files = filter_locator_data_files_by_partition(target_table, files, partition_filter)?;
    register_framework_locator_control_fixture(state, target_catalog_name, &files)?;
    let loaded =
        framework_locator_loaded_table(target_table, entry.object_store_config().cloned())?;
    let table_def = crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        &entry,
        target_catalog_name,
        target_namespace,
        target_table_name,
        loaded,
        files,
    )?;
    expose_physical_apply_key_for_locator_registration(table_def, target_table, apply_key_column)
}

#[cfg(test)]
fn register_framework_locator_control_fixture(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    catalog: &str,
    files: &[crate::connector::iceberg::catalog::registry::DataFileWithStats],
) -> Result<(), String> {
    let planned_files = files
        .iter()
        .cloned()
        .map(
            crate::connector::iceberg::catalog::backend::data_file_with_stats_to_iceberg_data_file_info,
        )
        .collect();
    crate::connector::iceberg::provider::register_planned_table_files_control_fixture(
        state.connector_control.as_ref(),
        catalog,
        std::collections::HashMap::from([("*".to_string(), planned_files)]),
    )
    .map_err(|error| format!("register framework locator connector control fixture: {error}"))
}

#[cfg(test)]
fn filter_locator_data_files_by_partition(
    target_table: &iceberg::table::Table,
    files: Vec<crate::connector::iceberg::catalog::registry::DataFileWithStats>,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::catalog::registry::DataFileWithStats>, String> {
    if !partition_filter.is_allow_list() {
        return Ok(files);
    }

    let target_metadata = target_table.metadata_ref();
    files
        .into_iter()
        .filter_map(|file| {
            match locator_data_file_matches_partition_filter(
                &target_metadata,
                &file,
                partition_filter,
            ) {
                Ok(true) => Some(Ok(file)),
                Ok(false) => None,
                Err(err) => Some(Err(err)),
            }
        })
        .collect()
}

#[cfg(test)]
fn locator_data_file_matches_partition_filter(
    target_metadata: &iceberg::spec::TableMetadata,
    file: &crate::connector::iceberg::catalog::registry::DataFileWithStats,
    partition_filter: &TargetPartitionFilter,
) -> Result<bool, String> {
    let partition_struct = file.partition_values.as_ref().ok_or_else(|| {
        format!(
            "iceberg MV target locator: data file `{}` is missing partition metadata",
            file.path
        )
    })?;
    let spec_id = file
        .partition_spec_id
        .unwrap_or_else(|| target_metadata.default_partition_spec().spec_id());
    let values = crate::connector::iceberg::changes::change_partition_field_values(
        target_metadata,
        spec_id,
        partition_struct,
    )
    .map_err(|e| {
        format!(
            "iceberg MV target locator: cannot derive partition values for `{}`: {e}",
            file.path
        )
    })?;
    let mut fields = Vec::with_capacity(values.len());
    for value in &values {
        let mv_value = crate::engine::mv::partition::mapping::change_partition_value_to_mv_value(
            &file.path,
            &value.value,
        )?;
        fields.push(crate::mv::model::MvPartitionKeyField::new(
            value.field_name.clone(),
            mv_value,
        ));
    }
    let key_spec_id = match partition_filter {
        TargetPartitionFilter::AllowList(set) => {
            set.iter().next().map(|key| key.spec_id).unwrap_or(spec_id)
        }
        TargetPartitionFilter::None => spec_id,
    };
    let key = crate::mv::model::MvPartitionKey::new(key_spec_id, fields);
    Ok(partition_filter.matches(&key))
}

#[cfg(test)]
fn framework_locator_catalog_entry(
    state: &std::sync::Arc<crate::engine::StandaloneState>,
    target_catalog_name: &str,
    target_table: &iceberg::table::Table,
) -> Result<crate::connector::iceberg::catalog::IcebergCatalogEntry, String> {
    match state
        .iceberg_catalogs
        .read()
        .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?
        .get(target_catalog_name)
    {
        Ok(entry) => Ok(entry),
        Err(_) => {
            let warehouse_uri = framework_locator_local_warehouse_uri(target_table)?;
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                target_catalog_name,
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse_uri),
                ],
            )
        }
    }
}

#[cfg(test)]
fn framework_locator_local_warehouse_uri(
    target_table: &iceberg::table::Table,
) -> Result<String, String> {
    let location = target_table.metadata().location().trim_end_matches('/');
    if !location.starts_with("file://") {
        return Err(
            "framework target locator requires the target Iceberg catalog to be registered for non-local tables"
                .to_string(),
        );
    }
    let ident = target_table.identifier();
    let mut table_path_segments = ident.namespace().as_ref().clone();
    table_path_segments.push(ident.name().to_string());
    let table_path_suffix = format!("/{}", table_path_segments.join("/"));
    Ok(location
        .strip_suffix(&table_path_suffix)
        .unwrap_or(location)
        .to_string())
}

#[cfg(test)]
fn framework_locator_loaded_table(
    target_table: &iceberg::table::Table,
    object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
) -> Result<crate::connector::iceberg::catalog::IcebergLoadedTable, String> {
    let iceberg_schema = target_table.metadata().current_schema();
    let arrow_schema = iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
        .map_err(|e| format!("convert iceberg target schema to arrow schema failed: {e}"))?;
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let nested = iceberg_schema.field_by_name(field.name()).ok_or_else(|| {
                format!(
                    "iceberg target column `{}` missing from schema",
                    field.name()
                )
            })?;
            Ok(novarocks_catalog::schema::ColumnDef {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                write_default: nested
                    .write_default
                    .as_ref()
                    .map(|literal| {
                        crate::connector::iceberg::default_value::iceberg_literal_to_column_default(
                            literal,
                            nested.field_type.as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "convert Iceberg MV target write-default for column `{}` failed: {e}",
                                field.name()
                            )
                        })
                    })
                    .transpose()?,
                logical_type: None,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(crate::connector::iceberg::catalog::IcebergLoadedTable {
        table: target_table.clone(),
        columns,
        logical_types: std::collections::HashMap::new(),
        key_desc: None,
        column_aggregations: std::collections::HashMap::new(),
        object_store_config,
    })
}

#[cfg(test)]
fn framework_locator_select_sql(
    target_namespace: &str,
    target_table_name: &str,
    apply_key_column: &str,
    requested_keys: ApplyKeyRequest<'_>,
) -> Result<String, String> {
    let table_name = format!(
        "{}.{}",
        quote_sql_identifier(target_namespace),
        quote_sql_identifier(target_table_name)
    );
    let apply_key = quote_sql_identifier(apply_key_column);
    let in_list = framework_locator_in_list(requested_keys)?;
    let mut projections = vec![quote_sql_identifier("_file"), quote_sql_identifier("_pos")];
    if matches!(
        requested_keys,
        ApplyKeyRequest::BranchInt64(_) | ApplyKeyRequest::BranchUtf8(_)
    ) {
        projections.push(quote_sql_identifier(BRANCH_ID_COLUMN_NAME));
    }
    projections.push(apply_key.clone());
    let mut predicates = vec![format!("{apply_key} IN ({in_list})")];
    if let Some(branch_in_list) = framework_locator_branch_id_in_list(requested_keys) {
        predicates.push(format!(
            "{} IN ({})",
            quote_sql_identifier(BRANCH_ID_COLUMN_NAME),
            branch_in_list
        ));
    }
    Ok(format!(
        "SELECT {} FROM {} WHERE {}",
        projections.join(", "),
        table_name,
        predicates.join(" AND ")
    ))
}

#[cfg(test)]
fn framework_locator_in_list(requested_keys: ApplyKeyRequest<'_>) -> Result<String, String> {
    match requested_keys {
        ApplyKeyRequest::Int64(keys) => Ok(join_i64_in_list(keys.iter().copied())),
        ApplyKeyRequest::Utf8(keys) => Ok(join_string_in_list(keys.iter().map(String::as_str))),
        ApplyKeyRequest::BranchInt64(keys) => {
            Ok(join_i64_in_list(keys.iter().map(|key| key.base_row_id)))
        }
        ApplyKeyRequest::BranchUtf8(keys) => {
            Ok(join_string_in_list(keys.iter().map(|key| key.key.as_str())))
        }
    }
}

#[cfg(test)]
fn framework_locator_branch_id_in_list(requested_keys: ApplyKeyRequest<'_>) -> Option<String> {
    match requested_keys {
        ApplyKeyRequest::BranchInt64(keys) => {
            Some(join_i32_in_list(keys.iter().map(|key| key.branch_id)))
        }
        ApplyKeyRequest::BranchUtf8(keys) => {
            Some(join_i32_in_list(keys.iter().map(|key| key.branch_id)))
        }
        ApplyKeyRequest::Int64(_) | ApplyKeyRequest::Utf8(_) => None,
    }
}

#[cfg(test)]
fn join_i64_in_list(values: impl Iterator<Item = i64>) -> String {
    values
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
fn join_i32_in_list(values: impl Iterator<Item = i32>) -> String {
    values
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
fn join_string_in_list<'a>(values: impl Iterator<Item = &'a str>) -> String {
    values
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(quote_sql_string_literal)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
fn quote_sql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[cfg(test)]
fn quote_sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
async fn locate_target_rows_by_apply_key_impl(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: ApplyKeyRequest<'_>,
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<TargetApplyLocatorResult, String> {
    use futures::StreamExt;
    use iceberg::arrow::ArrowReaderBuilder;

    if requested_keys.is_empty() {
        return Ok(TargetApplyLocatorResult {
            delete_groups: Vec::new(),
            matched_positions: Vec::new(),
        });
    }

    let requested = requested_apply_key_values(requested_keys);
    let request_is_i64 = matches!(requested_keys, ApplyKeyRequest::Int64(_));
    let mut select_columns = vec!["_file".to_string(), "_pos".to_string()];
    if matches!(
        requested_keys,
        ApplyKeyRequest::BranchInt64(_) | ApplyKeyRequest::BranchUtf8(_)
    ) {
        select_columns.push(BRANCH_ID_COLUMN_NAME.to_string());
    }
    select_columns.push(apply_key_column.to_string());
    let scan = target_table
        .scan()
        .select(select_columns)
        .build()
        .map_err(|e| format!("build iceberg MV target locator scan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("plan iceberg MV target locator files failed: {e}"))?;
    let target_metadata = target_table.metadata_ref();
    let filter_owned = partition_filter.clone();
    let cleaned_tasks = task_stream.map(move |task_result| {
        let mut task = task_result?;
        task.deletes.clear();
        task.predicate = None;
        if filter_owned.is_allow_list() {
            let Some(partition_struct) = task.partition.as_ref() else {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!(
                        "iceberg MV target locator: file scan task for data file `{}` is missing partition metadata",
                        task.data_file_path
                    ),
                ));
            };
            // iceberg-rust 0.9 always sets partition_spec = None in FileScanTask
            // (library TODO in scan/context.rs:139).  Fall back to the table's
            // default partition spec id so the call never errors unconditionally.
            let spec_id = task
                .partition_spec
                .as_ref()
                .map(|spec| spec.spec_id())
                .unwrap_or_else(|| target_metadata.default_partition_spec().spec_id());
            let values = crate::connector::iceberg::changes::change_partition_field_values(
                &target_metadata,
                spec_id,
                partition_struct,
            )
            .map_err(|e| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    format!(
                        "iceberg MV target locator: cannot derive partition values for `{}`: {e}",
                        task.data_file_path
                    ),
                )
            })?;
            let mut fields = Vec::with_capacity(values.len());
            for value in &values {
                let mv_value =
                    crate::engine::mv::partition::mapping::change_partition_value_to_mv_value(
                        &task.data_file_path,
                        &value.value,
                    )
                    .map_err(|e| {
                        iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e)
                    })?;
                fields.push(crate::mv::model::MvPartitionKeyField::new(
                    value.field_name.clone(),
                    mv_value,
                ));
            }
            // Use the allow-list's own spec_id as the canonical key spec_id.
            // The AllowList is built from the schema contract's target_spec_id,
            // which may differ from the table's raw default spec_id when the
            // contract was persisted before a partition spec evolution.
            // All keys in a single allow-list share the same spec_id (they come
            // from one contract refresh pass), so picking any key's spec_id is
            // safe.  For an empty allow-list, fall back to spec_id derived above;
            // filter_owned.matches will then return false (empty set has no
            // members), so the task is correctly dropped.
            let key_spec_id = match &filter_owned {
                TargetPartitionFilter::AllowList(set) => {
                    set.iter().next().map(|k| k.spec_id).unwrap_or(spec_id)
                }
                TargetPartitionFilter::None => spec_id,
            };
            let key = crate::mv::model::MvPartitionKey::new(key_spec_id, fields);
            if !filter_owned.matches(&key) {
                return Ok(None);
            }
        }
        Ok(Some(task))
    });
    let cleaned_tasks = cleaned_tasks.filter_map(|task_or_skip| async move {
        match task_or_skip {
            Ok(Some(task)) => Some(Ok(task)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    });
    let arrow_reader = ArrowReaderBuilder::new(target_table.file_io().clone())
        .with_row_group_filtering_enabled(false)
        .with_row_selection_enabled(false)
        .build();
    let mut stream = arrow_reader
        .read(Box::pin(cleaned_tasks))
        .map_err(|e| format!("read iceberg MV target locator scan failed: {e}"))?;

    let mut matches = std::collections::HashMap::<ApplyKeyValue, (String, i64)>::new();
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| format!("iceberg MV target locator scan error: {e}"))?;
        if matches!(requested_keys, ApplyKeyRequest::BranchInt64(_)) {
            process_branch_i64_apply_key_locator_batch(
                &batch,
                &requested,
                &mut matches,
                existing_deletes_by_file,
            )?;
        } else if matches!(requested_keys, ApplyKeyRequest::BranchUtf8(_)) {
            process_branch_utf8_apply_key_locator_batch(
                &batch,
                apply_key_column,
                &requested,
                &mut matches,
                existing_deletes_by_file,
            )?;
        } else {
            process_apply_key_locator_batch(
                &batch,
                apply_key_column,
                request_is_i64,
                &requested,
                &mut matches,
                existing_deletes_by_file,
            )?;
        }
    }

    ensure_all_requested_apply_keys_matched(&requested, &matches)?;
    build_target_apply_locator_result_from_apply_key_matches(
        matches,
        referenced_data_file_partitions,
    )
}

#[cfg(test)]
pub(crate) async fn locate_target_rows_by_apply_key_string(
    target_table: &iceberg::table::Table,
    join_row_keys: &[String],
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions,
    partition_filter: &TargetPartitionFilter,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    locate_target_rows_by_string_apply_key(
        target_table,
        JOIN_APPLY_KEY_COLUMN_NAME,
        join_row_keys,
        existing_deletes_by_file,
        referenced_data_file_partitions,
        partition_filter,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::commit::PositionDeleteGroup;
    use crate::mv::model::TargetPartitionFilter;
    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use iceberg::spec::Struct;
    use std::sync::Arc;

    /// Build a minimal local Hadoop-catalog iceberg table that can serve as
    /// the target for `locate_target_rows_by_apply_key` tests.  The table has
    /// a single `i64` column named `HIDDEN_APPLY_KEY_COLUMN_NAME` and no data
    /// files.  The `_row_ids` slice is accepted for future extension but
    /// currently unused (no data is written; tests that exercise the no-request
    /// path need an empty target table).
    fn build_local_iceberg_apply_key_target(_row_ids: &[i64]) -> iceberg::table::Table {
        use iceberg::Catalog;
        use iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Type,
        };
        use iceberg::{NamespaceIdent, TableCreation, TableIdent};

        let warehouse_dir = tempfile::TempDir::new()
            .expect("target warehouse tempdir")
            .keep();
        let warehouse = format!("file://{}", warehouse_dir.join("warehouse").display());
        let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "apply_key_target_test",
            &[
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), warehouse),
            ],
        )
        .expect("build hadoop catalog entry");
        let catalog = crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)
            .expect("build hadoop catalog");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, std::collections::HashMap::new())
                .await
                .expect("create_namespace");

            let schema = IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        HIDDEN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .expect("build schema");

            let table_ident = TableIdent::new(namespace.clone(), "mv_target".to_string());
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("mv_target".to_string())
                        .schema(schema)
                        .format_version(FormatVersion::V3)
                        .build(),
                )
                .await
                .expect("create_table");

            catalog.load_table(&table_ident).await.expect("load_table")
        })
    }

    #[test]
    fn apply_key_table_column_is_required_bigint() {
        let column = apply_key_table_column();

        assert_eq!(column.name, "__nova_base_row_id");
        assert_eq!(column.data_type, novarocks_catalog::schema::SqlType::BigInt);
        assert!(!column.nullable);
        assert!(column.aggregation.is_none());
        assert!(column.default.is_none());
    }

    #[test]
    fn join_apply_key_table_column_is_required_string() {
        let column = join_apply_key_table_column();

        assert_eq!(column.name, "__nova_join_row_key");
        assert_eq!(column.data_type, novarocks_catalog::schema::SqlType::String);
        assert!(!column.nullable);
        assert!(column.aggregation.is_none());
        assert!(column.default.is_none());
    }

    #[test]
    fn branch_id_table_column_is_required_int() {
        let col = branch_id_table_column();
        assert_eq!(col.name, BRANCH_ID_COLUMN_NAME);
        assert_eq!(col.name, "__branch_id__");
        assert!(!col.nullable);
        assert!(matches!(
            col.data_type,
            novarocks_catalog::schema::SqlType::Int
        ));
    }

    #[test]
    fn iceberg_mv_physical_select_appends_base_row_id() {
        let sql =
            iceberg_mv_physical_select_sql("SELECT id, amount FROM ice.ns.orders WHERE amount > 0")
                .expect("physical sql");

        assert_eq!(
            sql,
            "SELECT id, amount, _row_id AS __nova_base_row_id FROM ice.ns.orders WHERE amount > 0"
        );
    }

    #[test]
    fn iceberg_mv_physical_select_rejects_star_projection() {
        let err = iceberg_mv_physical_select_sql("SELECT * FROM ice.ns.orders")
            .expect_err("star projection must fail");

        assert!(err.contains("explicit projection columns"), "{err}");
    }

    #[test]
    fn iceberg_mv_physical_select_rejects_visible_apply_key_collision() {
        let err =
            iceberg_mv_physical_select_sql("SELECT id AS __nova_base_row_id FROM ice.ns.orders")
                .expect_err("reserved alias must fail");

        assert!(err.contains("__nova_base_row_id"), "{err}");
        assert!(err.contains("reserved"), "{err}");
    }

    #[test]
    fn apply_key_match_helper_accepts_exact_utf8_requested_key() {
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let mut matches = std::collections::HashMap::new();

        record_visible_apply_key_match(
            &mut matches,
            &requested,
            ApplyKeyValue::Utf8("group-1".to_string()),
            "file-a.parquet",
            7,
        )
        .expect("match");
        ensure_all_requested_apply_keys_matched(&requested, &matches).expect("complete");

        assert_eq!(
            matches.get(&ApplyKeyValue::Utf8("group-1".to_string())),
            Some(&("file-a.parquet".to_string(), 7))
        );
    }

    #[test]
    fn apply_key_match_helper_ignores_unrequested_utf8_key_and_reports_missing() {
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let mut matches = std::collections::HashMap::new();

        record_visible_apply_key_match(
            &mut matches,
            &requested,
            ApplyKeyValue::Utf8("group-2".to_string()),
            "file-a.parquet",
            7,
        )
        .expect("unrequested ignored");
        let err = ensure_all_requested_apply_keys_matched(&requested, &matches).unwrap_err();

        assert!(err.contains("group-1"), "err={err}");
    }

    #[test]
    fn apply_key_match_helper_rejects_duplicate_utf8_target_rows() {
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let mut matches = std::collections::HashMap::new();

        record_visible_apply_key_match(
            &mut matches,
            &requested,
            ApplyKeyValue::Utf8("group-1".to_string()),
            "file-a.parquet",
            7,
        )
        .expect("first match");
        let err = record_visible_apply_key_match(
            &mut matches,
            &requested,
            ApplyKeyValue::Utf8("group-1".to_string()),
            "file-b.parquet",
            9,
        )
        .unwrap_err();

        assert!(err.contains("duplicate"), "err={err}");
        assert!(err.contains("group-1"), "err={err}");
    }

    #[test]
    fn utf8_locator_scan_path_returns_position_delete_group_for_requested_key() {
        let batch = utf8_locator_batch(&[
            ("file-a.parquet", 7, "group-1"),
            ("file-b.parquet", 9, "group-2"),
        ]);
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        process_apply_key_locator_batch(
            &batch,
            GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
            false,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .expect("scan batch");
        ensure_all_requested_apply_keys_matched(&requested, &matches).expect("requested key");
        let groups =
            build_position_delete_groups_from_apply_key_matches(matches, &referenced_partitions())
                .expect("delete groups");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].referenced_data_file, "file-a.parquet");
        assert_eq!(groups[0].partition_spec_id, 0);
        assert_eq!(groups[0].positions, vec![7]);
    }

    #[test]
    fn locator_result_preserves_sorted_matched_positions() {
        let mut matches = std::collections::HashMap::new();
        matches.insert(
            ApplyKeyValue::Utf8("group-b".to_string()),
            ("file-b.parquet".to_string(), 9),
        );
        matches.insert(
            ApplyKeyValue::Utf8("group-a2".to_string()),
            ("file-a.parquet".to_string(), 3),
        );
        matches.insert(
            ApplyKeyValue::Utf8("group-a1".to_string()),
            ("file-a.parquet".to_string(), 7),
        );

        let result = build_target_apply_locator_result_from_apply_key_matches(
            matches,
            &referenced_partitions(),
        )
        .expect("locator result");

        assert_eq!(result.delete_groups.len(), 2);
        assert_eq!(
            result.delete_groups[0].referenced_data_file,
            "file-a.parquet"
        );
        assert_eq!(result.delete_groups[0].positions, vec![3, 7]);
        assert_eq!(
            result.delete_groups[1].referenced_data_file,
            "file-b.parquet"
        );
        assert_eq!(result.delete_groups[1].positions, vec![9]);
        assert_eq!(
            result.matched_positions,
            vec![
                TargetRowPositionSet {
                    referenced_data_file: "file-a.parquet".to_string(),
                    positions: vec![3, 7],
                },
                TargetRowPositionSet {
                    referenced_data_file: "file-b.parquet".to_string(),
                    positions: vec![9],
                },
            ]
        );
    }

    #[test]
    fn utf8_locator_scan_path_ignores_unrequested_rows_and_errors_on_missing_key() {
        let batch = utf8_locator_batch(&[("file-b.parquet", 9, "group-2")]);
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        process_apply_key_locator_batch(
            &batch,
            GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
            false,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .expect("scan batch");
        let err = ensure_all_requested_apply_keys_matched(&requested, &matches).unwrap_err();

        assert!(err.contains("group-1"), "err={err}");
        assert!(matches.is_empty());
    }

    #[test]
    fn utf8_locator_scan_path_errors_on_duplicate_visible_target_rows() {
        let batch = utf8_locator_batch(&[
            ("file-a.parquet", 7, "group-1"),
            ("file-b.parquet", 9, "group-1"),
        ]);
        let requested = requested_apply_key_values(ApplyKeyRequest::Utf8(&["group-1".to_string()]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        let err = process_apply_key_locator_batch(
            &batch,
            GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
            false,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .unwrap_err();

        assert!(err.contains("duplicate"), "err={err}");
        assert!(err.contains("group-1"), "err={err}");
    }

    #[test]
    fn branch_apply_key_locator_scan_distinguishes_same_base_row_id_across_branches() {
        let batch = branch_apply_key_locator_batch(&[
            ("file-a.parquet", 7, 0, 42),
            ("file-b.parquet", 9, 1, 42),
        ]);
        let requested =
            requested_apply_key_values(ApplyKeyRequest::BranchInt64(&[BranchApplyKey {
                branch_id: 1,
                base_row_id: 42,
            }]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        process_branch_i64_apply_key_locator_batch(
            &batch,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .expect("scan batch");
        ensure_all_requested_apply_keys_matched(&requested, &matches).expect("requested key");
        let groups =
            build_position_delete_groups_from_apply_key_matches(matches, &referenced_partitions())
                .expect("delete groups");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].referenced_data_file, "file-b.parquet");
        assert_eq!(groups[0].positions, vec![9]);
    }

    #[test]
    fn branch_scoped_string_key_matches_only_same_branch() {
        let batch = branch_string_apply_key_locator_batch(&[
            ("file-a.parquet", 7, 0, "group-1"),
            ("file-b.parquet", 9, 1, "group-1"),
        ]);
        let requested =
            requested_apply_key_values(ApplyKeyRequest::BranchUtf8(&[BranchStringApplyKey {
                branch_id: 1,
                key: "group-1".to_string(),
            }]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        process_branch_utf8_apply_key_locator_batch(
            &batch,
            GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .expect("scan batch");
        ensure_all_requested_apply_keys_matched(&requested, &matches).expect("requested key");
        let groups =
            build_position_delete_groups_from_apply_key_matches(matches, &referenced_partitions())
                .expect("delete groups");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].referenced_data_file, "file-b.parquet");
        assert_eq!(groups[0].positions, vec![9]);
    }

    #[test]
    fn branch_apply_key_locator_scan_rejects_duplicate_visible_target_rows() {
        let batch = branch_apply_key_locator_batch(&[
            ("file-a.parquet", 7, 1, 42),
            ("file-b.parquet", 9, 1, 42),
        ]);
        let requested =
            requested_apply_key_values(ApplyKeyRequest::BranchInt64(&[BranchApplyKey {
                branch_id: 1,
                base_row_id: 42,
            }]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        let err = process_branch_i64_apply_key_locator_batch(
            &batch,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .unwrap_err();

        assert!(err.contains("duplicate"), "err={err}");
        assert!(err.contains("branch 1"), "err={err}");
        assert!(err.contains("42"), "err={err}");
    }

    #[test]
    fn branch_apply_key_locator_scan_rejects_missing_branch_column() {
        let batch = utf8_locator_batch(&[("file-a.parquet", 7, "group-1")]);
        let requested =
            requested_apply_key_values(ApplyKeyRequest::BranchInt64(&[BranchApplyKey {
                branch_id: 1,
                base_row_id: 42,
            }]));
        let existing_deletes = std::collections::HashMap::new();
        let mut matches = std::collections::HashMap::new();

        let err = process_branch_i64_apply_key_locator_batch(
            &batch,
            &requested,
            &mut matches,
            &existing_deletes,
        )
        .unwrap_err();

        assert!(err.contains(BRANCH_ID_COLUMN_NAME), "err={err}");
        assert!(err.contains("missing"), "err={err}");
    }

    fn utf8_locator_batch(rows: &[(&str, i32, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int32, false),
            Field::new(GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(file, _, _)| *file),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, pos, _)| *pos),
                )) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(_, _, key)| *key),
                )) as ArrayRef,
            ],
        )
        .expect("locator batch")
    }

    fn branch_apply_key_locator_batch(rows: &[(&str, i32, i32, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int32, false),
            Field::new(BRANCH_ID_COLUMN_NAME, DataType::Int32, false),
            Field::new(HIDDEN_APPLY_KEY_COLUMN_NAME, DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(file, _, _, _)| *file),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, pos, _, _)| *pos),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, _, branch, _)| *branch),
                )) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(
                    rows.iter().map(|(_, _, _, key)| *key),
                )) as ArrayRef,
            ],
        )
        .expect("branch locator batch")
    }

    fn branch_string_apply_key_locator_batch(rows: &[(&str, i32, i32, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int32, false),
            Field::new(BRANCH_ID_COLUMN_NAME, DataType::Int32, false),
            Field::new(GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(file, _, _, _)| *file),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, pos, _, _)| *pos),
                )) as ArrayRef,
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|(_, _, branch, _)| *branch),
                )) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    rows.iter().map(|(_, _, _, key)| *key),
                )) as ArrayRef,
            ],
        )
        .expect("branch string locator batch")
    }

    fn referenced_partitions()
    -> crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions {
        let mut partitions = std::collections::HashMap::new();
        for file in ["file-a.parquet", "file-b.parquet"] {
            partitions.insert(
                file.to_string(),
                crate::connector::iceberg::delete_visibility::ReferencedDataFilePartition {
                    partition_spec_id: 0,
                    partition_values: Struct::empty(),
                },
            );
        }
        partitions
    }

    #[test]
    fn empty_request_with_filter_none_returns_empty_groups() {
        // No request → no scan → empty groups, regardless of filter shape.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let target_table = build_local_iceberg_apply_key_target(&[]);
        let existing = std::collections::HashMap::new();
        let referenced = std::collections::HashMap::new();
        let groups = rt
            .block_on(super::locate_target_rows_by_apply_key(
                &target_table,
                &[],
                &existing,
                &referenced,
                &TargetPartitionFilter::None,
            ))
            .expect("locator");
        assert!(groups.is_empty());
    }

    #[test]
    fn empty_request_with_empty_allow_list_returns_empty_groups() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let target_table = build_local_iceberg_apply_key_target(&[]);
        let existing = std::collections::HashMap::new();
        let referenced = std::collections::HashMap::new();
        let filter = TargetPartitionFilter::AllowList(std::collections::BTreeSet::new());
        let groups = rt
            .block_on(super::locate_target_rows_by_apply_key(
                &target_table,
                &[],
                &existing,
                &referenced,
                &filter,
            ))
            .expect("locator");
        assert!(groups.is_empty());
    }

    /// Build a partitioned apply-key target table with two data files:
    ///   region=a → apply_key = "key-a" at position 0
    ///   region=b → apply_key = "key-b" at position 0
    ///
    /// Schema: `JOIN_APPLY_KEY_COLUMN_NAME` (Utf8 required, field_id=1),
    ///         `region` (Utf8 optional, field_id=2).
    /// Partition spec: identity(region), bound spec_id=0.
    ///
    struct PartitionedApplyKeyTargetFixture {
        table: iceberg::table::Table,
        file_paths: Vec<String>,
        _catalog: std::sync::Arc<dyn iceberg::Catalog>,
        _warehouse_dir: tempfile::TempDir,
    }

    /// Returns a real MV-target-shaped Iceberg table fixture. The tempdir and
    /// catalog guards must stay alive while the table is scanned.
    fn build_partitioned_apply_key_target_with_rows() -> PartitionedApplyKeyTargetFixture {
        use iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type,
            UnboundPartitionSpec,
        };
        use iceberg::transaction::{ApplyTransactionAction, Transaction};
        use iceberg::{NamespaceIdent, TableCreation, TableIdent};
        use uuid::Uuid;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let warehouse_dir = tempfile::Builder::new()
            .prefix("novarocks-target-apply-")
            .tempdir()
            .expect("warehouse tempdir");
        let warehouse = format!("file://{}", warehouse_dir.path().display());
        let (table, file_paths, catalog) = rt.block_on(async {
            let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "ice",
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse),
                ],
            )
            .expect("build hadoop catalog entry");
            let catalog =
                crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
                    .expect("build hadoop catalog");

            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, std::collections::HashMap::new())
                .await
                .expect("create_namespace");

            // Schema: apply_key (String, required), region (String, optional).
            let schema = IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        JOIN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .expect("build schema");

            // Partition spec: identity(region) using source field_id=2.
            let partition_spec = UnboundPartitionSpec::builder()
                .add_partition_field(2, "region", Transform::Identity)
                .expect("add partition field")
                .build();

            let table_ident = TableIdent::new(namespace.clone(), "mv_apply_target".to_string());
            let table = catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("mv_apply_target".to_string())
                        .schema(schema)
                        .partition_spec(partition_spec)
                        .properties([
                            ("write.row-lineage".to_string(), "true".to_string()),
                            (
                                APPLY_KEY_COLUMN_PROPERTY.to_string(),
                                JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                            ),
                            (
                                APPLY_KEY_SOURCE_PROPERTY.to_string(),
                                ApplyKeySource::JoinRowKey
                                    .table_property_value()
                                    .to_string(),
                            ),
                            (APPLY_KEY_FIELD_ID_PROPERTY.to_string(), "1".to_string()),
                        ])
                        .format_version(FormatVersion::V3)
                        .build(),
                )
                .await
                .expect("create_table");

            // Two batches: region=a and region=b, each with one apply_key row.
            let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(
                    JOIN_APPLY_KEY_COLUMN_NAME,
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),
                arrow::datatypes::Field::new("region", arrow::datatypes::DataType::Utf8, true),
            ]));
            let batch_a = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["key-a"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                ],
            )
            .expect("batch_a");
            let batch_b = RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(StringArray::from(vec!["key-b"])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
                ],
            )
            .expect("batch_b");

            // Write region=a first, then region=b.  The writer produces one data
            // file per partition, so data_files[0] = region=a and
            // data_files[1] = region=b.
            let data_files =
                crate::connector::iceberg::data_writer::write_record_batches_as_data_files(
                    &table,
                    vec![batch_a, batch_b],
                )
                .await
                .expect("write data files");
            assert_eq!(data_files.len(), 2, "expected one data file per partition");

            let file_paths: Vec<String> = data_files
                .iter()
                .map(|f| f.file_path().to_string())
                .collect();

            // Commit both data files via fast_append.
            let tx = Transaction::new(&table);
            let action = tx
                .fast_append()
                .add_data_files(data_files)
                .set_commit_uuid(Uuid::new_v4());
            let tx = action.apply(tx).expect("fast_append apply");
            let _table_after: iceberg::table::Table = tx
                .commit(catalog.as_ref())
                .await
                .expect("fast_append commit");

            let refreshed = catalog
                .load_table(&table_ident)
                .await
                .expect("reload table");
            (refreshed, file_paths, catalog)
        });
        PartitionedApplyKeyTargetFixture {
            table,
            file_paths,
            _catalog: catalog,
            _warehouse_dir: warehouse_dir,
        }
    }

    struct BranchApplyKeyTargetFixture {
        table: iceberg::table::Table,
        file_paths: Vec<String>,
        _catalog: std::sync::Arc<dyn iceberg::Catalog>,
        _warehouse_dir: tempfile::TempDir,
    }

    fn build_branch_apply_key_target_with_rows() -> BranchApplyKeyTargetFixture {
        use iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema as IcebergSchema, Type,
        };
        use iceberg::transaction::{ApplyTransactionAction, Transaction};
        use iceberg::{NamespaceIdent, TableCreation, TableIdent};
        use uuid::Uuid;

        let rt = tokio::runtime::Runtime::new().unwrap();
        let warehouse_dir = tempfile::Builder::new()
            .prefix("novarocks-branch-target-apply-")
            .tempdir()
            .expect("warehouse tempdir");
        let warehouse = format!("file://{}", warehouse_dir.path().display());
        let (table, file_paths, catalog) = rt.block_on(async {
            let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "ice",
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse),
                ],
            )
            .expect("build hadoop catalog entry");
            let catalog =
                crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)
                    .expect("build hadoop catalog");

            let namespace = NamespaceIdent::new("db".to_string());
            catalog
                .create_namespace(&namespace, std::collections::HashMap::new())
                .await
                .expect("create_namespace");

            let schema = IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::required(
                        1,
                        BRANCH_ID_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        JOIN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(
                        3,
                        HIDDEN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .expect("build schema");

            let table_ident = TableIdent::new(namespace.clone(), "mv_branch_target".to_string());
            let table = catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("mv_branch_target".to_string())
                        .schema(schema)
                        .properties([
                            ("write.row-lineage".to_string(), "true".to_string()),
                            (
                                APPLY_KEY_COLUMN_PROPERTY.to_string(),
                                JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                            ),
                            (
                                APPLY_KEY_SOURCE_PROPERTY.to_string(),
                                ApplyKeySource::JoinRowKey
                                    .table_property_value()
                                    .to_string(),
                            ),
                            (APPLY_KEY_FIELD_ID_PROPERTY.to_string(), "2".to_string()),
                        ])
                        .format_version(FormatVersion::V3)
                        .build(),
                )
                .await
                .expect("create_table");

            let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(
                    BRANCH_ID_COLUMN_NAME,
                    arrow::datatypes::DataType::Int32,
                    false,
                ),
                arrow::datatypes::Field::new(
                    JOIN_APPLY_KEY_COLUMN_NAME,
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),
                arrow::datatypes::Field::new(
                    HIDDEN_APPLY_KEY_COLUMN_NAME,
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 2])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["shared", "shared", "other"])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![100, 100, 200])) as ArrayRef,
                ],
            )
            .expect("branch batch");

            let data_files =
                crate::connector::iceberg::data_writer::write_record_batches_as_data_files(
                    &table,
                    vec![batch],
                )
                .await
                .expect("write data files");
            assert_eq!(data_files.len(), 1, "expected one unpartitioned data file");
            let file_paths = data_files
                .iter()
                .map(|file| file.file_path().to_string())
                .collect::<Vec<_>>();

            let tx = Transaction::new(&table);
            let action = tx
                .fast_append()
                .add_data_files(data_files)
                .set_commit_uuid(Uuid::new_v4());
            let tx = action.apply(tx).expect("fast_append apply");
            let _table_after: iceberg::table::Table = tx
                .commit(catalog.as_ref())
                .await
                .expect("fast_append commit");

            let refreshed = catalog
                .load_table(&table_ident)
                .await
                .expect("reload table");
            (refreshed, file_paths, catalog)
        });
        BranchApplyKeyTargetFixture {
            table,
            file_paths,
            _catalog: catalog,
            _warehouse_dir: warehouse_dir,
        }
    }

    fn hadoop_catalog_entry_for_target(
        target_table: &iceberg::table::Table,
    ) -> crate::connector::iceberg::catalog::IcebergCatalogEntry {
        crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    target_table
                        .metadata()
                        .location()
                        .strip_suffix("/db/mv_apply_target")
                        .expect("target table location under warehouse")
                        .to_string(),
                ),
            ],
        )
        .expect("build iceberg catalog entry")
    }

    async fn commit_existing_position_delete_to_partitioned_target(
        fixture: &PartitionedApplyKeyTargetFixture,
        referenced_data_file: &str,
        position: i64,
    ) -> Result<iceberg::table::Table, String> {
        let snapshot_id = fixture
            .table
            .metadata()
            .current_snapshot()
            .ok_or_else(|| "target table has no snapshot".to_string())?
            .snapshot_id();
        let data_files =
            crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
                &fixture.table,
                snapshot_id,
            )?;
        let data_file = data_files
            .into_iter()
            .find(|file| file.path == referenced_data_file)
            .ok_or_else(|| format!("missing target data file `{referenced_data_file}`"))?;
        let delete_groups = vec![PositionDeleteGroup {
            referenced_data_file: referenced_data_file.to_string(),
            partition_spec_id: data_file.partition_spec_id.unwrap_or(0),
            partition_values: data_file.partition_values.unwrap_or_else(Struct::empty),
            positions: vec![position],
        }];
        let metadata = fixture.table.metadata();
        let staging_dir = format!(
            "{}/data/_staging/{}",
            metadata.location(),
            uuid::Uuid::new_v4()
        );
        let written = crate::connector::iceberg::commit::write_position_delete_files(
            &fixture.table.file_io().clone(),
            &staging_dir,
            delete_groups,
        )
        .await?;

        let table_ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new("db".to_string()),
            "mv_apply_target".to_string(),
        );
        let collector = std::sync::Arc::new(
            crate::connector::iceberg::commit::IcebergCommitCollector::new(
                crate::connector::iceberg::commit::CommitOpKind::RowDelta,
                table_ident.clone(),
                metadata
                    .current_snapshot()
                    .map(|snapshot| snapshot.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staging_dir,
                novarocks_types::UniqueId::new(0, 0),
            )
            .with_table_metadata(metadata.clone()),
        );
        for file in written {
            collector.inject_written_file(file);
        }
        let file_io = fixture.table.file_io().clone();
        let snapshot_properties = std::collections::BTreeMap::new();
        let ctx = crate::connector::iceberg::commit::CommitCtx {
            collector: &collector,
            table: &fixture.table,
            catalog: fixture._catalog.as_ref(),
            file_io: &file_io,
            commit_uuid: uuid::Uuid::new_v4(),
            abort_handle: collector.abort_log.clone(),
            target_ref: "main",
            snapshot_properties: &snapshot_properties,
        };
        crate::connector::iceberg::commit::IcebergCommitAction::commit(
            &crate::connector::iceberg::commit::RowDeltaCommit,
            ctx,
        )
        .await?;
        fixture
            ._catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| format!("reload target after position delete failed: {e}"))
    }

    fn loaded_partitioned_apply_key_target(
        target_table: &iceberg::table::Table,
    ) -> crate::connector::iceberg::catalog::IcebergLoadedTable {
        crate::connector::iceberg::catalog::IcebergLoadedTable {
            table: target_table.clone(),
            columns: vec![
                novarocks_catalog::schema::ColumnDef {
                    name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_catalog::schema::ColumnDef {
                    name: "region".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            logical_types: std::collections::HashMap::new(),
            key_desc: None,
            column_aggregations: std::collections::HashMap::new(),
            object_store_config: None,
        }
    }

    fn assert_standard_mv_target_table_def_hides_physical_apply_key(
        table_def: &crate::sql::planner::table::TableDef,
    ) {
        assert!(
            table_def
                .columns
                .iter()
                .all(|column| !column.name.eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)),
            "standard MV target registration must hide the physical apply-key column"
        );
        assert!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_file")
        );
        assert!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_pos")
        );
    }

    fn expose_physical_apply_key_for_locator_test_registration(
        mut table_def: crate::sql::planner::table::TableDef,
    ) -> crate::sql::planner::table::TableDef {
        assert_standard_mv_target_table_def_hides_physical_apply_key(&table_def);
        table_def.columns.insert(
            0,
            novarocks_catalog::schema::ColumnDef {
                name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                data_type: arrow::datatypes::DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
        );
        table_def
    }

    fn normalize_position_delete_groups(groups: &mut [PositionDeleteGroup]) {
        for group in groups.iter_mut() {
            group.positions.sort_unstable();
        }
        groups.sort_by(|left, right| {
            left.referenced_data_file
                .cmp(&right.referenced_data_file)
                .then_with(|| left.partition_spec_id.cmp(&right.partition_spec_id))
                .then_with(|| left.positions.cmp(&right.positions))
        });
    }

    fn assert_position_delete_groups_eq(
        mut expected: Vec<PositionDeleteGroup>,
        mut actual: Vec<PositionDeleteGroup>,
    ) {
        normalize_position_delete_groups(&mut expected);
        normalize_position_delete_groups(&mut actual);
        assert_eq!(
            expected.len(),
            actual.len(),
            "position-delete group count differs"
        );
        for (idx, (expected, actual)) in expected.iter().zip(actual.iter()).enumerate() {
            assert_eq!(
                expected.referenced_data_file, actual.referenced_data_file,
                "group {idx} referenced_data_file differs"
            );
            assert_eq!(
                expected.partition_spec_id, actual.partition_spec_id,
                "group {idx} partition_spec_id differs"
            );
            assert_eq!(
                expected.partition_values, actual.partition_values,
                "group {idx} partition_values differ"
            );
            assert_eq!(
                expected.positions, actual.positions,
                "group {idx} positions differ"
            );
        }
    }

    fn normalize_matched_positions(sets: &mut [TargetRowPositionSet]) {
        for set in sets.iter_mut() {
            set.positions.sort_unstable();
        }
        sets.sort_by(|left, right| {
            left.referenced_data_file
                .cmp(&right.referenced_data_file)
                .then_with(|| left.positions.cmp(&right.positions))
        });
    }

    fn assert_locator_results_eq(
        expected: TargetApplyLocatorResult,
        actual: TargetApplyLocatorResult,
    ) {
        assert_position_delete_groups_eq(expected.delete_groups, actual.delete_groups);
        let mut expected_positions = expected.matched_positions;
        let mut actual_positions = actual.matched_positions;
        normalize_matched_positions(&mut expected_positions);
        normalize_matched_positions(&mut actual_positions);
        assert_eq!(expected_positions, actual_positions);
    }

    fn referenced_partitions_for_table(
        table: &iceberg::table::Table,
    ) -> crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions {
        let snapshot_id = table
            .metadata()
            .current_snapshot()
            .expect("target snapshot")
            .snapshot_id();
        crate::connector::iceberg::delete_visibility::load_referenced_data_file_partitions_at(
            table,
            Some(snapshot_id),
        )
        .expect("load referenced data file partitions")
    }

    #[test]
    fn spike_framework_select_file_pos_on_target() {
        use arrow::array::{Int64Array, StringArray};

        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test()
            .expect("install all-in-one loopback backend");
        let state = std::sync::Arc::new(crate::engine::StandaloneState {
            exchange_port: loopback_backend.exchange_port,
            ..crate::engine::StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);

        let fixture = build_partitioned_apply_key_target_with_rows();
        let target_table = &fixture.table;
        let file_paths = &fixture.file_paths;
        assert_eq!(
            target_table.metadata().format_version(),
            iceberg::spec::FormatVersion::V3
        );
        assert_eq!(
            target_table
                .metadata()
                .properties()
                .get("write.row-lineage")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            target_table
                .metadata()
                .properties()
                .get(APPLY_KEY_COLUMN_PROPERTY)
                .map(String::as_str),
            Some(JOIN_APPLY_KEY_COLUMN_NAME)
        );
        assert_eq!(
            target_table
                .metadata()
                .properties()
                .get(APPLY_KEY_SOURCE_PROPERTY)
                .map(String::as_str),
            Some(ApplyKeySource::JoinRowKey.table_property_value())
        );
        assert_eq!(
            target_table
                .metadata()
                .properties()
                .get(APPLY_KEY_FIELD_ID_PROPERTY)
                .map(String::as_str),
            Some("1")
        );

        let snapshot_id = target_table
            .metadata()
            .current_snapshot()
            .expect("target snapshot")
            .snapshot_id();
        let data_files =
            crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
                target_table,
                snapshot_id,
            )
            .expect("extract target data files");
        assert_eq!(data_files.len(), 2, "expected one data file per partition");
        register_framework_locator_control_fixture(&state, "ice", &data_files)
            .expect("register framework locator connector control fixture");

        let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    target_table
                        .metadata()
                        .location()
                        .strip_suffix("/db/mv_apply_target")
                        .expect("target table location under warehouse")
                        .to_string(),
                ),
            ],
        )
        .expect("build iceberg catalog entry");

        let standard_table_def =
            crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
                &entry,
                "ice",
                "db",
                "mv_target",
                loaded_partitioned_apply_key_target(target_table),
                data_files,
            )
            .expect("build standard target table def");
        assert_standard_mv_target_table_def_hides_physical_apply_key(&standard_table_def);

        let table_def = expose_physical_apply_key_for_locator_test_registration(standard_table_def);
        assert!(
            table_def
                .columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)),
            "locator/test registration must expose the physical apply-key column"
        );
        assert!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_file")
        );
        assert!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_pos")
        );
        {
            let mut catalog_guard = state
                .catalog_service
                .local()
                .write()
                .expect("standalone catalog");
            catalog_guard.create_database("db").expect("create db");
            catalog_guard
                .register("db", table_def)
                .expect("register target table def");
        }

        let session = crate::engine::StandaloneSession {
            inner: std::sync::Arc::clone(&state),
        };
        let sql = format!(
            "SELECT _file, _pos, {apply_key} \
             FROM db.mv_target \
             WHERE {apply_key} IN ('key-a')",
            apply_key = JOIN_APPLY_KEY_COLUMN_NAME
        );
        let result = match session
            .execute_in_context(&sql, None, "db", None)
            .expect("framework SELECT")
        {
            crate::engine::StatementResult::Query(result) => result,
            crate::engine::StatementResult::Ok => panic!("SELECT returned Ok"),
        };

        assert_eq!(result.row_count(), 1, "result={result:?}");
        let chunk = result
            .chunks
            .iter()
            .find(|chunk| chunk.batch.num_rows() == 1)
            .expect("one-row chunk");
        let file = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("_file utf8");
        let pos = chunk
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_pos int64");
        let apply_key = chunk
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("apply-key utf8");

        assert!(
            file.value(0).contains(&file_paths[0]),
            "_file={} file_a={}",
            file.value(0),
            file_paths[0]
        );
        assert_eq!(pos.value(0), 0);
        assert_eq!(apply_key.value(0), "key-a");
        drop(loopback_backend);
    }

    #[test]
    fn framework_locate_matches_direct_locator_partitioned() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test()
            .expect("install all-in-one loopback backend");
        let state = std::sync::Arc::new(crate::engine::StandaloneState {
            exchange_port: loopback_backend.exchange_port,
            ..crate::engine::StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);

        let fixture = build_partitioned_apply_key_target_with_rows();
        let snapshot_id = fixture
            .table
            .metadata()
            .current_snapshot()
            .expect("target snapshot")
            .snapshot_id();
        let data_files =
            crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
                &fixture.table,
                snapshot_id,
            )
            .expect("extract target data files");
        let entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    fixture
                        .table
                        .metadata()
                        .location()
                        .strip_suffix("/db/mv_apply_target")
                        .expect("target table location under warehouse")
                        .to_string(),
                ),
            ],
        )
        .expect("build iceberg catalog entry");
        let standard_table_def =
            crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
                &entry,
                "ice",
                "db",
                "mv_target",
                loaded_partitioned_apply_key_target(&fixture.table),
                data_files,
            )
            .expect("build standard target table def");
        assert_standard_mv_target_table_def_hides_physical_apply_key(&standard_table_def);
        {
            let mut catalog_guard = state
                .catalog_service
                .local()
                .write()
                .expect("standalone catalog");
            catalog_guard.create_database("db").expect("create db");
            catalog_guard
                .register("db", standard_table_def)
                .expect("register standard target table def");
        }

        let requested = vec!["key-a".to_string(), "key-b".to_string()];
        let empty_deletes = std::collections::HashMap::new();
        let mut referenced: crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions =
            std::collections::HashMap::new();
        for path in &fixture.file_paths {
            referenced.insert(
                path.clone(),
                crate::connector::iceberg::delete_visibility::ReferencedDataFilePartition {
                    partition_spec_id: 0,
                    partition_values: iceberg::spec::Struct::empty(),
                },
            );
        }

        let direct_groups = rt
            .block_on(super::locate_target_rows_by_string_apply_key(
                &fixture.table,
                JOIN_APPLY_KEY_COLUMN_NAME,
                &requested,
                &empty_deletes,
                &referenced,
                &TargetPartitionFilter::None,
            ))
            .expect("direct apply-key locator");

        let framework_groups = super::resolve_target_positions_via_framework(
            &state,
            &fixture.table,
            "ice",
            "db",
            "mv_target",
            JOIN_APPLY_KEY_COLUMN_NAME,
            ApplyKeyRequest::Utf8(&requested),
            &referenced,
            &TargetPartitionFilter::None,
        )
        .expect("framework apply-key locator")
        .delete_groups;

        assert_position_delete_groups_eq(direct_groups, framework_groups);
        let target_def_after_framework = state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog")
            .get("db", "mv_target")
            .expect("registered target table");
        assert_standard_mv_target_table_def_hides_physical_apply_key(&target_def_after_framework);
        drop(loopback_backend);
    }

    #[test]
    fn framework_locate_respects_existing_position_deletes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test()
            .expect("install all-in-one loopback backend");
        let state = std::sync::Arc::new(crate::engine::StandaloneState {
            exchange_port: loopback_backend.exchange_port,
            ..crate::engine::StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);

        let mut fixture = build_partitioned_apply_key_target_with_rows();
        let deleted_key = vec!["key-a".to_string()];
        let live_key = vec!["key-b".to_string()];
        let snapshot_before_position_delete = fixture
            .table
            .metadata()
            .current_snapshot()
            .expect("target snapshot before position delete")
            .snapshot_id();
        fixture.table = rt
            .block_on(commit_existing_position_delete_to_partitioned_target(
                &fixture,
                &fixture.file_paths[0],
                0,
            ))
            .expect("commit existing position delete");
        let snapshot_after_position_delete = fixture
            .table
            .metadata()
            .current_snapshot()
            .expect("target snapshot after position delete")
            .snapshot_id();
        assert_ne!(
            snapshot_before_position_delete, snapshot_after_position_delete,
            "row-delta position-delete commit must advance the target snapshot"
        );

        let target_entry = hadoop_catalog_entry_for_target(&fixture.table);
        let (existing_deletes_by_file, referenced_data_file_partitions) =
            super::load_target_apply_locator_inputs(&target_entry, &fixture.table)
                .expect("load target apply locator inputs");
        assert!(
            existing_deletes_by_file
                .get(&fixture.file_paths[0])
                .map(|visibility| visibility.deleted_positions.contains(0))
                .unwrap_or(false),
            "position delete for key-a must be visible in locator inputs"
        );
        assert!(
            !existing_deletes_by_file
                .get(&fixture.file_paths[1])
                .map(|visibility| visibility.deleted_positions.contains(0))
                .unwrap_or(false),
            "position 0 for key-b's data file must remain visible in locator inputs"
        );

        let live_partition = referenced_data_file_partitions
            .get(&fixture.file_paths[1])
            .expect("partition metadata for key-b data file");
        let expected_live_groups = || {
            vec![PositionDeleteGroup {
                referenced_data_file: fixture.file_paths[1].clone(),
                partition_spec_id: live_partition.partition_spec_id,
                partition_values: live_partition.partition_values.clone(),
                positions: vec![0],
            }]
        };
        let direct_live_groups = rt
            .block_on(super::locate_target_rows_by_string_apply_key(
                &fixture.table,
                JOIN_APPLY_KEY_COLUMN_NAME,
                &live_key,
                &existing_deletes_by_file,
                &referenced_data_file_partitions,
                &TargetPartitionFilter::None,
            ))
            .expect("direct locator must still see key-b");
        assert_position_delete_groups_eq(expected_live_groups(), direct_live_groups);
        let framework_live_groups = super::resolve_target_positions_via_framework(
            &state,
            &fixture.table,
            "ice",
            "db",
            "mv_target",
            JOIN_APPLY_KEY_COLUMN_NAME,
            ApplyKeyRequest::Utf8(&live_key),
            &referenced_data_file_partitions,
            &TargetPartitionFilter::None,
        )
        .expect("framework locator must still see key-b")
        .delete_groups;
        assert_position_delete_groups_eq(expected_live_groups(), framework_live_groups);

        let direct_result = rt.block_on(super::locate_target_rows_by_string_apply_key(
            &fixture.table,
            JOIN_APPLY_KEY_COLUMN_NAME,
            &deleted_key,
            &existing_deletes_by_file,
            &referenced_data_file_partitions,
            &TargetPartitionFilter::None,
        ));
        let direct_err = match direct_result {
            Ok(_) => panic!("direct locator must not see a position-deleted target row"),
            Err(err) => err,
        };

        let framework_result = super::resolve_target_positions_via_framework(
            &state,
            &fixture.table,
            "ice",
            "db",
            "mv_target",
            JOIN_APPLY_KEY_COLUMN_NAME,
            ApplyKeyRequest::Utf8(&deleted_key),
            &referenced_data_file_partitions,
            &TargetPartitionFilter::None,
        );
        let framework_err = match framework_result {
            Ok(_) => panic!("framework locator must not see a position-deleted target row"),
            Err(err) => err,
        };

        assert!(
            direct_err.contains("iceberg MV target row not found for apply key"),
            "err={direct_err}"
        );
        assert!(direct_err.contains("key-a"), "err={direct_err}");
        assert!(
            framework_err.contains("iceberg MV target row not found for apply key"),
            "err={framework_err}"
        );
        assert!(framework_err.contains("key-a"), "err={framework_err}");
        assert_eq!(direct_err, framework_err);
        drop(loopback_backend);
    }

    #[test]
    fn framework_locate_branch_parity() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test()
            .expect("install all-in-one loopback backend");
        let state = std::sync::Arc::new(crate::engine::StandaloneState {
            exchange_port: loopback_backend.exchange_port,
            ..crate::engine::StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);

        let fixture = build_branch_apply_key_target_with_rows();
        let empty_deletes = std::collections::HashMap::new();
        let referenced = referenced_partitions_for_table(&fixture.table);

        let branch_string_keys = vec![BranchStringApplyKey {
            branch_id: 2,
            key: "shared".to_string(),
        }];
        let direct_string = rt
            .block_on(
                super::locate_target_rows_by_branch_string_apply_key_with_matches(
                    &fixture.table,
                    JOIN_APPLY_KEY_COLUMN_NAME,
                    &branch_string_keys,
                    &empty_deletes,
                    &referenced,
                    &TargetPartitionFilter::None,
                ),
            )
            .expect("direct branch string locator");
        assert_eq!(
            direct_string.matched_positions,
            vec![TargetRowPositionSet {
                referenced_data_file: fixture.file_paths[0].clone(),
                positions: vec![1],
            }],
            "branch-scoped string locator must choose branch=2, not branch=1"
        );
        let framework_string = super::resolve_target_positions_via_framework(
            &state,
            &fixture.table,
            "ice",
            "db",
            "mv_target",
            JOIN_APPLY_KEY_COLUMN_NAME,
            ApplyKeyRequest::BranchUtf8(&branch_string_keys),
            &referenced,
            &TargetPartitionFilter::None,
        )
        .expect("framework branch string locator");
        assert_locator_results_eq(direct_string, framework_string);

        let branch_i64_keys = vec![BranchApplyKey {
            branch_id: 2,
            base_row_id: 100,
        }];
        let direct_i64 = rt
            .block_on(super::locate_target_rows_by_branch_apply_key_with_matches(
                &fixture.table,
                &branch_i64_keys,
                &empty_deletes,
                &referenced,
                &TargetPartitionFilter::None,
            ))
            .expect("direct branch i64 locator");
        assert_eq!(
            direct_i64.matched_positions,
            vec![TargetRowPositionSet {
                referenced_data_file: fixture.file_paths[0].clone(),
                positions: vec![1],
            }],
            "branch-scoped i64 locator must choose branch=2, not branch=1"
        );
        let framework_i64 = super::resolve_target_positions_via_framework(
            &state,
            &fixture.table,
            "ice",
            "db",
            "mv_target",
            HIDDEN_APPLY_KEY_COLUMN_NAME,
            ApplyKeyRequest::BranchInt64(&branch_i64_keys),
            &referenced,
            &TargetPartitionFilter::None,
        )
        .expect("framework branch i64 locator");
        assert_locator_results_eq(direct_i64, framework_i64);
        drop(loopback_backend);
    }

    #[test]
    fn framework_locate_allow_list_parity() {
        use crate::mv::model::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};

        let rt = tokio::runtime::Runtime::new().unwrap();
        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test()
            .expect("install all-in-one loopback backend");
        let state = std::sync::Arc::new(crate::engine::StandaloneState {
            exchange_port: loopback_backend.exchange_port,
            ..crate::engine::StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);

        let fixture = build_partitioned_apply_key_target_with_rows();
        let referenced = referenced_partitions_for_table(&fixture.table);
        let empty_deletes = std::collections::HashMap::new();
        let allow_key = MvPartitionKey::new(
            0,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String("a".to_string()),
            )],
        );
        let filter = TargetPartitionFilter::AllowList(
            std::iter::once(allow_key).collect::<std::collections::BTreeSet<_>>(),
        );

        let requested_key_a = vec!["key-a".to_string()];
        let direct_key_a = rt
            .block_on(super::locate_target_rows_by_string_apply_key_with_matches(
                &fixture.table,
                JOIN_APPLY_KEY_COLUMN_NAME,
                &requested_key_a,
                &empty_deletes,
                &referenced,
                &filter,
            ))
            .expect("direct allow-list locator");
        assert_eq!(
            direct_key_a.matched_positions,
            vec![TargetRowPositionSet {
                referenced_data_file: fixture.file_paths[0].clone(),
                positions: vec![0],
            }],
            "allow-list must keep only region=a / key-a"
        );
        let framework_key_a = super::resolve_target_positions_via_framework(
            &state,
            &fixture.table,
            "ice",
            "db",
            "mv_target",
            JOIN_APPLY_KEY_COLUMN_NAME,
            ApplyKeyRequest::Utf8(&requested_key_a),
            &referenced,
            &filter,
        )
        .expect("framework allow-list locator");
        assert_locator_results_eq(direct_key_a, framework_key_a);

        let requested_key_b = vec!["key-b".to_string()];
        let direct_key_b = rt.block_on(super::locate_target_rows_by_string_apply_key_with_matches(
            &fixture.table,
            JOIN_APPLY_KEY_COLUMN_NAME,
            &requested_key_b,
            &empty_deletes,
            &referenced,
            &filter,
        ));
        let framework_key_b = super::resolve_target_positions_via_framework(
            &state,
            &fixture.table,
            "ice",
            "db",
            "mv_target",
            JOIN_APPLY_KEY_COLUMN_NAME,
            ApplyKeyRequest::Utf8(&requested_key_b),
            &referenced,
            &filter,
        );
        let direct_key_b_err = match direct_key_b {
            Ok(_) => panic!("direct locator must prune region=b"),
            Err(err) => err,
        };
        let framework_key_b_err = match framework_key_b {
            Ok(_) => panic!("framework locator must prune region=b"),
            Err(err) => err,
        };
        assert_eq!(direct_key_b_err, framework_key_b_err);
        drop(loopback_backend);
    }

    #[test]
    fn framework_locator_preserves_preexisting_synthetic_name_collision() {
        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test()
            .expect("install all-in-one loopback backend");
        let state = std::sync::Arc::new(crate::engine::StandaloneState {
            exchange_port: loopback_backend.exchange_port,
            ..crate::engine::StandaloneState::default()
        });
        crate::connector::register_standalone_backends(&state);

        let fixture = build_partitioned_apply_key_target_with_rows();
        let burned =
            super::next_framework_locator_synthetic_table_name("mv_target", &fixture.table);
        let (prefix, nonce) = burned.rsplit_once('_').expect("synthetic nonce suffix");
        let first_collision_nonce = nonce.parse::<u64>().expect("numeric synthetic nonce") + 1;
        let colliding_names = (first_collision_nonce..first_collision_nonce + 8)
            .map(|nonce| format!("{prefix}_{nonce}"))
            .collect::<Vec<_>>();
        {
            let mut catalog_guard = state
                .catalog_service
                .local()
                .write()
                .expect("standalone catalog");
            catalog_guard.create_database("db").expect("create db");
            for name in &colliding_names {
                catalog_guard
                    .register("db", sentinel_collision_table_def(name))
                    .expect("register colliding synthetic table");
            }
        }

        let requested = vec!["key-a".to_string()];
        let mut referenced: crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions =
            std::collections::HashMap::new();
        for path in &fixture.file_paths {
            referenced.insert(
                path.clone(),
                crate::connector::iceberg::delete_visibility::ReferencedDataFilePartition {
                    partition_spec_id: 0,
                    partition_values: iceberg::spec::Struct::empty(),
                },
            );
        }

        let located = super::resolve_target_positions_via_framework(
            &state,
            &fixture.table,
            "ice",
            "db",
            "mv_target",
            JOIN_APPLY_KEY_COLUMN_NAME,
            ApplyKeyRequest::Utf8(&requested),
            &referenced,
            &TargetPartitionFilter::None,
        )
        .expect("framework apply-key locator");
        assert_eq!(located.delete_groups.len(), 1);

        let catalog_guard = state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog");
        for name in &colliding_names {
            let table_def = catalog_guard.get("db", name).unwrap_or_else(|err| {
                panic!("pre-existing collision table {name} was dropped: {err}")
            });
            assert_eq!(
                table_def.columns,
                sentinel_collision_table_def(name).columns,
                "pre-existing collision table {name} was overwritten"
            );
        }
        drop(catalog_guard);
        drop(loopback_backend);
    }

    #[test]
    fn framework_locator_cleanup_preserves_overwritten_synthetic_table() {
        let state = std::sync::Arc::new(crate::engine::StandaloneState::default());
        let synthetic_name = "mv_target__nr_framework_locator_cleanup_owner";
        let original_table_def =
            sentinel_framework_locator_table_def(synthetic_name, "original_owner", 9101, 9102);
        let locator_registration =
            super::try_register_scoped_framework_locator_table(&state, "db", original_table_def)
                .expect("register scoped framework locator table")
                .expect("collision-free synthetic table");

        let overwritten_table_def =
            sentinel_framework_locator_table_def(synthetic_name, "overwritten_owner", 9201, 9202);
        {
            let mut catalog_guard = state
                .catalog_service
                .local()
                .write()
                .expect("standalone catalog");
            catalog_guard
                .register("db", overwritten_table_def.clone())
                .expect("overwrite synthetic table name");
        }

        locator_registration
            .cleanup()
            .expect("cleanup overwritten locator table");

        let current_table_def = state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog")
            .get("db", synthetic_name)
            .expect("overwritten synthetic table should remain registered");
        assert_eq!(current_table_def.columns, overwritten_table_def.columns);
        assert_eq!(
            format!("{:?}", current_table_def.source),
            format!("{:?}", overwritten_table_def.source)
        );
    }

    fn sentinel_collision_table_def(name: &str) -> crate::sql::planner::table::TableDef {
        sentinel_framework_locator_table_def(name, "sentinel_collision_column", 9001, 9002)
    }

    fn sentinel_framework_locator_table_def(
        name: &str,
        column_name: &str,
        db_id: i64,
        table_id: i64,
    ) -> crate::sql::planner::table::TableDef {
        crate::sql::planner::table::TableDef {
            name: name.to_string(),
            columns: vec![novarocks_catalog::schema::ColumnDef {
                name: column_name.to_string(),
                data_type: arrow::datatypes::DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::sql::planner::table::ScanSource::StarRocks { db_id, table_id },
        }
    }

    /// Verify that the AllowList pruning path:
    ///   (a) does not error when `task.partition_spec` is None (iceberg-rust 0.9
    ///       always sets it to None — library TODO in scan/context.rs:139),
    ///   (b) correctly uses the contract's `target_spec_id` (here: 7) rather
    ///       than the table's raw default spec_id (here: 0) when constructing
    ///       the comparison key, so that the allow-list lookup succeeds.
    ///
    /// The test builds a two-partition table (region=a, region=b) and calls the
    /// locator with an AllowList whose single key carries `spec_id=7` (the
    /// contract spec_id) and `region=a`.  Only the region=a file passes the
    /// filter; exactly one PositionDeleteGroup is produced.
    #[test]
    fn allow_list_with_contract_spec_id_keeps_matching_partition() {
        use crate::mv::model::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};

        let rt = tokio::runtime::Runtime::new().unwrap();
        let fixture = build_partitioned_apply_key_target_with_rows();
        let target_table = &fixture.table;
        let file_paths = &fixture.file_paths;

        // The contract's target_spec_id is 7 — intentionally different from the
        // table's raw default spec_id (0) to reproduce the production mismatch.
        // All keys in one AllowList share the same spec_id (single contract pass).
        const CONTRACT_SPEC_ID: i32 = 7;

        let allow_key = MvPartitionKey::new(
            CONTRACT_SPEC_ID,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String("a".to_string()),
            )],
        );
        let filter = TargetPartitionFilter::AllowList(
            std::iter::once(allow_key).collect::<std::collections::BTreeSet<_>>(),
        );

        // Populate referenced_data_file_partitions for both files so the locator
        // can build PositionDeleteGroups after finding the match.  The table was
        // just created so its only partition spec has id=0.
        let mut referenced: crate::connector::iceberg::delete_visibility::ReferencedDataFilePartitions =
            std::collections::HashMap::new();
        for path in file_paths {
            referenced.insert(
                path.clone(),
                crate::connector::iceberg::delete_visibility::ReferencedDataFilePartition {
                    partition_spec_id: 0,
                    partition_values: iceberg::spec::Struct::empty(),
                },
            );
        }

        let existing = std::collections::HashMap::new();
        let join_keys = vec!["key-a".to_string()];

        let result = rt
            .block_on(super::locate_target_rows_by_string_apply_key_with_matches(
                target_table,
                JOIN_APPLY_KEY_COLUMN_NAME,
                &join_keys,
                &existing,
                &referenced,
                &filter,
            ))
            .expect("locator must not error (old bug triggered: 'missing partition spec')");

        // The AllowList kept region=a and pruned region=b, so exactly one
        // PositionDeleteGroup must be returned.  The referenced file must be
        // one of the two data files (it will be the region=a one), and it must
        // contain exactly one row at position 0.
        assert_eq!(
            result.delete_groups.len(),
            1,
            "expected exactly one delete group (region=b must be pruned by AllowList)"
        );
        assert!(
            file_paths.contains(&result.delete_groups[0].referenced_data_file),
            "delete group references an unknown file: {}",
            result.delete_groups[0].referenced_data_file
        );
        assert_eq!(
            result.delete_groups[0].positions,
            vec![0i64],
            "one row at position 0 in the matched data file"
        );
        assert_eq!(
            result.matched_positions,
            vec![TargetRowPositionSet {
                referenced_data_file: result.delete_groups[0].referenced_data_file.clone(),
                positions: vec![0],
            }],
        );
    }
}
