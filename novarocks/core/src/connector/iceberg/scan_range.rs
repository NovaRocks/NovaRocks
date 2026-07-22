// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::datatypes::DataType;

use crate::common::min_max_predicate::MinMaxPredicate;
use crate::connector::iceberg::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
    IcebergTableInfo,
};
use crate::connector::iceberg::scan_planner::{
    IcebergScanHandle, iceberg_scan_handle, iceberg_split,
};
use crate::connector::scan_planning::{ScanHandle, Split, validate_split_connectors};
use crate::runtime::scan_range;
use novarocks_catalog::schema::ColumnDef;

const ICEBERG_SCAN_SPLIT_TARGET_BYTES: i64 = 128 * 1024 * 1024;
const ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE: usize = 1024;
const ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE: i64 = 512 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub(crate) struct IcebergScanRangeContext {
    pub(crate) min_max_predicates: Vec<MinMaxPredicate>,
    pub(crate) columns: Vec<ColumnDef>,
}

#[derive(Clone, Debug)]
pub(crate) struct PlannedIcebergScanRanges {
    pub(crate) scan_ranges: Vec<scan_range::ScanRangeParams>,
}

pub(crate) fn equality_delete_required_columns(
    table: &IcebergTableInfo,
    splits: &[Split],
) -> Result<Vec<String>, String> {
    let mut schema_by_id = BTreeMap::new();
    let mut schema_by_name = BTreeMap::new();
    for field in &table.schema.fields {
        if schema_by_id
            .insert(field.field_id, field.name.clone())
            .is_some()
        {
            return Err(format!(
                "Iceberg ScanNode table schema has duplicate field id {} for table {}",
                field.field_id, table.table
            ));
        }
        let normalized = field.name.to_ascii_lowercase();
        if schema_by_name
            .insert(normalized, field.name.clone())
            .is_some()
        {
            return Err(format!(
                "Iceberg ScanNode table schema has duplicate field name {} for table {}",
                field.name, table.table
            ));
        }
    }

    let mut required = Vec::new();
    let mut required_seen = BTreeSet::new();
    for split in splits {
        let file = iceberg_split(split)?;
        for delete in &file.data_file.delete_files {
            if delete.file_content != IcebergDeleteFileContent::Equality {
                continue;
            }

            let mut resolved_ids = Vec::new();
            let mut ids_seen = BTreeSet::new();
            for field_id in &delete.equality_field_ids {
                if !ids_seen.insert(*field_id) {
                    return Err(format!(
                        "Iceberg equality-delete file {} has duplicate equality field id {}",
                        delete.path, field_id
                    ));
                }
                let name = schema_by_id.get(field_id).ok_or_else(|| {
                    format!(
                        "Iceberg equality-delete file {} references unknown field id {} in table {}",
                        delete.path, field_id, table.table
                    )
                })?;
                resolved_ids.push(name.clone());
            }

            let mut resolved_names = Vec::new();
            let mut names_seen = BTreeSet::new();
            for name in &delete.equality_column_names {
                let normalized = name.to_ascii_lowercase();
                if !names_seen.insert(normalized.clone()) {
                    return Err(format!(
                        "Iceberg equality-delete file {} has duplicate equality column name {}",
                        delete.path, name
                    ));
                }
                let canonical = schema_by_name.get(&normalized).ok_or_else(|| {
                    format!(
                        "Iceberg equality-delete file {} references unknown equality column {} in table {}",
                        delete.path, name, table.table
                    )
                })?;
                resolved_names.push(canonical.clone());
            }

            let columns = match (resolved_ids.is_empty(), resolved_names.is_empty()) {
                (true, true) => {
                    return Err(format!(
                        "Iceberg equality-delete file {} has no equality field identity",
                        delete.path
                    ));
                }
                (false, false) => {
                    let ids = resolved_ids
                        .iter()
                        .map(|name| name.to_ascii_lowercase())
                        .collect::<BTreeSet<_>>();
                    let names = resolved_names
                        .iter()
                        .map(|name| name.to_ascii_lowercase())
                        .collect::<BTreeSet<_>>();
                    if ids != names {
                        return Err(format!(
                            "Iceberg equality-delete file {} field id/name mismatch: ids={resolved_ids:?} names={resolved_names:?}",
                            delete.path
                        ));
                    }
                    resolved_ids
                }
                (false, true) => resolved_ids,
                (true, false) => resolved_names,
            };
            for name in columns {
                if required_seen.insert(name.to_ascii_lowercase()) {
                    required.push(name);
                }
            }
        }
    }
    Ok(required)
}

pub(crate) fn plan_iceberg_scan_ranges(
    scan: &ScanHandle,
    splits: &[Split],
    ctx: IcebergScanRangeContext,
) -> Result<PlannedIcebergScanRanges, String> {
    validate_split_connectors(scan, splits)?;
    let scan = iceberg_scan_handle(scan)?;
    let scan_ranges = build_iceberg_native_scan_ranges(scan, splits, &ctx)?;
    Ok(PlannedIcebergScanRanges { scan_ranges })
}

fn build_iceberg_native_scan_ranges(
    scan: &IcebergScanHandle,
    splits: &[Split],
    ctx: &IcebergScanRangeContext,
) -> Result<Vec<scan_range::ScanRangeParams>, String> {
    let mut ranges = Vec::new();
    let scan_predicates =
        crate::connector::iceberg::file_pruning::min_max_predicates_to_scan_predicates(
            &ctx.min_max_predicates,
        );
    let mut pruning_counters =
        crate::connector::iceberg::file_pruning::IcebergFilePruningCounters::default();
    let pruning_columns = pruning_columns_for_scan(scan, &ctx.columns)?;
    for split in splits {
        let file = &iceberg_split(split)?.data_file;
        if !crate::connector::iceberg::file_pruning::file_may_satisfy_scan_predicates(
            file,
            &scan_predicates,
            &mut pruning_counters,
        ) {
            continue;
        }
        ranges.extend(build_native_file_scan_range_params_for_file(
            file,
            &pruning_columns,
        )?);
    }
    Ok(ranges)
}

#[derive(Clone, Debug)]
struct PruningColumn {
    schema_ordinal: i32,
    column: ColumnDef,
}

fn pruning_columns_for_scan(
    scan: &IcebergScanHandle,
    columns: &[ColumnDef],
) -> Result<Vec<PruningColumn>, String> {
    scan.table
        .table_info
        .schema
        .fields
        .iter()
        .enumerate()
        .filter_map(|(schema_ordinal, field)| {
            scan.table
                .column_names
                .iter()
                .any(|column_name| column_name.eq_ignore_ascii_case(&field.name))
                .then_some((schema_ordinal, field))
        })
        .map(|(schema_ordinal, field)| {
            let schema_ordinal = i32::try_from(schema_ordinal).map_err(|_| {
                format!(
                    "Iceberg table {}.{} schema field ordinal overflow for {}",
                    scan.table.namespace, scan.table.table, field.name
                )
            })?;
            let column = columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&field.name))
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "Iceberg table {}.{} scan column {} missing from resolved table columns",
                        scan.table.namespace, scan.table.table, field.name
                    )
                })?;
            Ok(PruningColumn {
                schema_ordinal,
                column,
            })
        })
        .collect()
}

fn build_native_file_scan_range_params_for_file(
    file: &IcebergDataFileInfo,
    columns: &[PruningColumn],
) -> Result<Vec<scan_range::ScanRangeParams>, String> {
    validate_iceberg_delete_apply_cost(&file.path, &file.delete_files)?;
    let splits = plan_hdfs_file_splits(file);
    let file_pruning_min_max_values = native_file_pruning_min_max_values(file, columns);
    splits
        .into_iter()
        .map(|(offset, length)| {
            build_native_file_scan_range_params(
                &file.path,
                file.size,
                offset,
                length,
                file.first_row_id,
                file.data_sequence_number,
                file.ivm_change_op,
                file.included_positions.as_ref(),
                &file.delete_files,
                file_pruning_min_max_values.clone(),
            )
        })
        .collect()
}

fn native_file_pruning_min_max_values(
    file: &IcebergDataFileInfo,
    columns: &[PruningColumn],
) -> Option<BTreeMap<i32, scan_range::FilePruningMinMaxValue>> {
    let stats = file.column_stats.as_ref()?;
    if stats.is_empty() || columns.is_empty() {
        return None;
    }

    let mut out = BTreeMap::new();
    for column in columns {
        let Some(stat) = find_column_stats(stats, &column.column.name) else {
            continue;
        };
        let Some(value) = native_min_max_value_from_stats(stat, &column.column.data_type) else {
            continue;
        };
        out.insert(column.schema_ordinal, value);
    }

    if out.is_empty() { None } else { Some(out) }
}

fn find_column_stats<'a>(
    stats: &'a HashMap<String, crate::connector::iceberg::scan_model::IcebergColumnStats>,
    column: &str,
) -> Option<&'a crate::connector::iceberg::scan_model::IcebergColumnStats> {
    stats.get(column).or_else(|| {
        stats
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, stats)| stats)
    })
}

fn native_min_max_value_from_stats(
    stats: &crate::connector::iceberg::scan_model::IcebergColumnStats,
    data_type: &DataType,
) -> Option<scan_range::FilePruningMinMaxValue> {
    let has_null = stats.null_count.unwrap_or(0) > 0;
    let all_null = stats
        .value_count
        .zip(stats.null_count)
        .is_some_and(|(value_count, null_count)| value_count > 0 && value_count == null_count);

    match data_type {
        DataType::Boolean => {
            let lower = stats.lower_bound.as_deref().and_then(decode_bool_bound)?;
            let upper = stats.upper_bound.as_deref().and_then(decode_bool_bound)?;
            Some(scan_range::FilePruningMinMaxValue {
                value_kind: scan_range::FilePruningValueKind::Bool,
                has_null,
                all_null,
                min_int_value: Some(i64::from(lower)),
                max_int_value: Some(i64::from(upper)),
                min_float_value: None,
                max_float_value: None,
            })
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let lower = stats
                .lower_bound
                .as_deref()
                .and_then(|bytes| decode_int_bound_for_type(bytes, data_type))?;
            let upper = stats
                .upper_bound
                .as_deref()
                .and_then(|bytes| decode_int_bound_for_type(bytes, data_type))?;
            Some(scan_range::FilePruningMinMaxValue {
                value_kind: scan_range::FilePruningValueKind::Int,
                has_null,
                all_null,
                min_int_value: Some(lower),
                max_int_value: Some(upper),
                min_float_value: None,
                max_float_value: None,
            })
        }
        DataType::Float32 | DataType::Float64 => {
            let lower = stats
                .lower_bound
                .as_deref()
                .and_then(|bytes| decode_float_bound_for_type(bytes, data_type))?;
            let upper = stats
                .upper_bound
                .as_deref()
                .and_then(|bytes| decode_float_bound_for_type(bytes, data_type))?;
            if lower.is_nan() || upper.is_nan() {
                return None;
            }
            Some(scan_range::FilePruningMinMaxValue {
                value_kind: scan_range::FilePruningValueKind::Float,
                has_null,
                all_null,
                min_int_value: None,
                max_int_value: None,
                min_float_value: Some(lower),
                max_float_value: Some(upper),
            })
        }
        _ => None,
    }
}

fn decode_bool_bound(bytes: &[u8]) -> Option<bool> {
    match bytes {
        [0] => Some(false),
        [1] => Some(true),
        _ => None,
    }
}

fn decode_int_bound_for_type(bytes: &[u8], data_type: &DataType) -> Option<i64> {
    match data_type {
        DataType::Int8 => {
            let arr: [u8; 1] = bytes.try_into().ok()?;
            Some(i64::from(i8::from_le_bytes(arr)))
        }
        DataType::Int16 => {
            let arr: [u8; 2] = bytes.try_into().ok()?;
            Some(i64::from(i16::from_le_bytes(arr)))
        }
        DataType::Int32 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(i64::from(i32::from_le_bytes(arr)))
        }
        DataType::Int64 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(i64::from_le_bytes(arr))
        }
        _ => None,
    }
}

fn decode_float_bound_for_type(bytes: &[u8], data_type: &DataType) -> Option<f64> {
    match data_type {
        DataType::Float32 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(f64::from(f32::from_le_bytes(arr)))
        }
        DataType::Float64 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(f64::from_le_bytes(arr))
        }
        _ => None,
    }
}

#[cfg(test)]
fn pruning_columns_from_column_order_for_test(
    columns: &[ColumnDef],
) -> Result<Vec<PruningColumn>, String> {
    columns
        .iter()
        .enumerate()
        .map(|(schema_ordinal, column)| {
            Ok(PruningColumn {
                schema_ordinal: i32::try_from(schema_ordinal)
                    .map_err(|_| "test schema ordinal overflow".to_string())?,
                column: column.clone(),
            })
        })
        .collect()
}

fn plan_hdfs_file_splits(file: &IcebergDataFileInfo) -> Vec<(i64, i64)> {
    let file_len = file.size.max(0);
    if file_len <= ICEBERG_SCAN_SPLIT_TARGET_BYTES
        || file.first_row_id.is_some()
        || !file.delete_files.is_empty()
        || file.included_positions.is_some()
    {
        return vec![(0, file_len)];
    }

    let mut out = Vec::new();
    let mut offset = 0_i64;
    while offset < file_len {
        let remaining = file_len - offset;
        let length = remaining.min(ICEBERG_SCAN_SPLIT_TARGET_BYTES);
        out.push((offset, length));
        offset += length;
    }
    if out.is_empty() {
        out.push((0, 0));
    }
    out
}

fn validate_iceberg_delete_apply_cost(
    data_path: &str,
    delete_files: &[IcebergDeleteFileInfo],
) -> Result<(), String> {
    if delete_files.len() > ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE {
        return Err(format!(
            "too many Iceberg delete files attached to data file {data_path}: count={} max={}",
            delete_files.len(),
            ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE
        ));
    }
    let total_bytes = delete_files.iter().try_fold(0_i64, |acc, delete_file| {
        let Some(length) = delete_file.length else {
            return Ok(acc);
        };
        acc.checked_add(length.max(0))
            .ok_or_else(|| format!("Iceberg delete file length overflow for data file {data_path}"))
    })?;
    if total_bytes > ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE {
        return Err(format!(
            "Iceberg delete files attached to data file {data_path} are too large: bytes={total_bytes} max={ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE}"
        ));
    }
    Ok(())
}

fn build_native_file_scan_range_params(
    full_path: &str,
    file_len: i64,
    offset: i64,
    length: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
    included_positions: Option<&Vec<i64>>,
    delete_files: &[IcebergDeleteFileInfo],
    file_pruning_min_max_values: Option<BTreeMap<i32, scan_range::FilePruningMinMaxValue>>,
) -> Result<scan_range::ScanRangeParams, String> {
    let mut parquet_delete_files = Vec::new();
    let mut deletion_vector_descriptor = None;
    for delete_file in delete_files {
        match delete_file.file_format {
            IcebergDeleteFileFormat::Parquet => {
                let file_content = match delete_file.file_content {
                    IcebergDeleteFileContent::Position => {
                        scan_range::IcebergFileContent::PositionDeletes
                    }
                    IcebergDeleteFileContent::Equality => {
                        // Equality field IDs are read from the equality-delete Parquet schema by
                        // the Rust scan runner. The scan range only needs to identify the
                        // delete file as an equality-delete file.
                        scan_range::IcebergFileContent::EqualityDeletes
                    }
                };
                parquet_delete_files.push(scan_range::IcebergDeleteFile {
                    full_path: Some(delete_file.path.clone()),
                    file_format: scan_range::IcebergFileFormat::Parquet,
                    file_content,
                    length: delete_file.length,
                });
            }
            IcebergDeleteFileFormat::Puffin => {
                if deletion_vector_descriptor.is_some() {
                    return Err(format!(
                        "multiple Puffin deletion vectors are attached to data file {}",
                        full_path
                    ));
                }
                let offset = delete_file.content_offset.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_offset",
                        delete_file.path, full_path
                    )
                })?;
                let size = delete_file.content_size_in_bytes.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_size_in_bytes",
                        delete_file.path, full_path
                    )
                })?;
                deletion_vector_descriptor = Some(scan_range::DeletionVectorDescriptor {
                    storage_type: Some("PUFFIN".to_string()),
                    path_or_inline_dv: Some(delete_file.path.clone()),
                    offset: Some(offset),
                    size_in_bytes: Some(size),
                    cardinality: None,
                });
            }
        }
    }
    if let Some(op) = ivm_change_op {
        crate::exec::change_op::validate_change_op_value(op)?;
    }
    Ok(scan_range::ScanRangeParams::file(
        scan_range::FileScanRange {
            file_format: scan_range::FileFormat::Parquet,
            full_path: Some(full_path.to_string()),
            relative_path: None,
            table_id: None,
            offset,
            length,
            file_length: file_len,
            delete_files: parquet_delete_files,
            deletion_vector_descriptor,
            first_row_id,
            data_sequence_number,
            modification_time: None,
            datacache_options: None,
            candidate_node: None,
            included_positions: included_positions.cloned().unwrap_or_default(),
            serialized_split: None,
            use_iceberg_jni_metadata_reader: false,
            ivm_change_op,
            file_pruning_min_max_values,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::connector::iceberg::scan_planner::{
        IcebergScanHandle, IcebergSplit, IcebergSplitSource, IcebergTableHandle,
    };

    fn delete_file(
        path: &str,
        file_format: IcebergDeleteFileFormat,
        file_content: IcebergDeleteFileContent,
    ) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            path: path.to_string(),
            file_format,
            file_content,
            length: Some(32),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(3),
            partition_key: Some("partition".to_string()),
            equality_column_names: Vec::new(),
            equality_field_ids: Vec::new(),
        }
    }

    fn file_range(params: &scan_range::ScanRangeParams) -> &scan_range::FileScanRange {
        let scan_range::ScanRange::File(file) = &params.range else {
            panic!("expected file scan range");
        };
        file
    }

    #[test]
    fn large_plain_file_splits_at_stable_target_boundary_in_order() {
        let file = IcebergDataFileInfo::for_test(
            "s3://bucket/large.parquet",
            ICEBERG_SCAN_SPLIT_TARGET_BYTES * 2 + 7,
            1,
        );

        let ranges =
            build_native_file_scan_range_params_for_file(&file, &[]).expect("build split ranges");
        let offsets_and_lengths = ranges
            .iter()
            .map(file_range)
            .map(|range| (range.offset, range.length))
            .collect::<Vec<_>>();

        assert_eq!(
            offsets_and_lengths,
            vec![
                (0, ICEBERG_SCAN_SPLIT_TARGET_BYTES),
                (
                    ICEBERG_SCAN_SPLIT_TARGET_BYTES,
                    ICEBERG_SCAN_SPLIT_TARGET_BYTES
                ),
                (ICEBERG_SCAN_SPLIT_TARGET_BYTES * 2, 7),
            ]
        );
    }

    #[test]
    fn deletes_dv_row_lineage_and_included_positions_map_without_loss() {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 4096, 10);
        file.first_row_id = Some(41);
        file.data_sequence_number = Some(9);
        file.ivm_change_op = Some(crate::exec::change_op::CHANGE_OP_DELETE);
        file.included_positions = Some(vec![2, 5, 8]);
        file.delete_files = vec![
            delete_file(
                "s3://bucket/position.parquet",
                IcebergDeleteFileFormat::Parquet,
                IcebergDeleteFileContent::Position,
            ),
            delete_file(
                "s3://bucket/equality.parquet",
                IcebergDeleteFileFormat::Parquet,
                IcebergDeleteFileContent::Equality,
            ),
            IcebergDeleteFileInfo {
                content_offset: Some(17),
                content_size_in_bytes: Some(23),
                ..delete_file(
                    "s3://bucket/delete.puffin",
                    IcebergDeleteFileFormat::Puffin,
                    IcebergDeleteFileContent::Position,
                )
            },
        ];

        let ranges =
            build_native_file_scan_range_params_for_file(&file, &[]).expect("build delete range");
        assert_eq!(ranges.len(), 1, "delete-bearing files must not be split");
        let range = file_range(&ranges[0]);

        assert_eq!(range.full_path.as_deref(), Some(file.path.as_str()));
        assert_eq!(range.first_row_id, Some(41));
        assert_eq!(range.data_sequence_number, Some(9));
        assert_eq!(
            range.ivm_change_op,
            Some(crate::exec::change_op::CHANGE_OP_DELETE)
        );
        assert_eq!(range.included_positions, vec![2, 5, 8]);
        assert_eq!(range.delete_files.len(), 2);
        assert_eq!(
            range.delete_files[0].file_content,
            scan_range::IcebergFileContent::PositionDeletes
        );
        assert_eq!(
            range.delete_files[1].file_content,
            scan_range::IcebergFileContent::EqualityDeletes
        );
        let deletion_vector = range
            .deletion_vector_descriptor
            .as_ref()
            .expect("deletion vector");
        assert_eq!(deletion_vector.storage_type.as_deref(), Some("PUFFIN"));
        assert_eq!(
            deletion_vector.path_or_inline_dv.as_deref(),
            Some("s3://bucket/delete.puffin")
        );
        assert_eq!(deletion_vector.offset, Some(17));
        assert_eq!(deletion_vector.size_in_bytes, Some(23));
        assert_eq!(deletion_vector.cardinality, None);
    }

    #[test]
    fn typed_adapter_preserves_split_order() {
        let table_info = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "s3://bucket/t".to_string(),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let table = IcebergTableHandle {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            snapshot_id: Some(7),
            table_info,
            split_source: IcebergSplitSource::ExplicitFiles(Vec::new()),
            column_names: Vec::new(),
        };
        let scan = ScanHandle::new("iceberg", IcebergScanHandle { table });
        let splits = ["first", "second"]
            .into_iter()
            .map(|name| {
                Split::new(
                    "iceberg",
                    IcebergSplit {
                        data_file: IcebergDataFileInfo::for_test(
                            &format!("s3://bucket/{name}.parquet"),
                            1,
                            1,
                        ),
                    },
                )
            })
            .collect::<Vec<_>>();

        let planned = plan_iceberg_scan_ranges(&scan, &splits, IcebergScanRangeContext::default())
            .expect("plan ranges");
        let paths = planned
            .scan_ranges
            .iter()
            .map(file_range)
            .map(|range| range.full_path.as_deref().expect("path"))
            .collect::<Vec<_>>();

        assert_eq!(
            paths,
            vec!["s3://bucket/first.parquet", "s3://bucket/second.parquet"]
        );
    }
}
