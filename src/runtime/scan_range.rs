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

use std::collections::BTreeMap;

#[cfg(feature = "compat")]
use crate::thrift::{data_cache, descriptors, exprs, internal_service, plan_nodes, types};
#[cfg(feature = "compat")]
use thrift::OrderedFloat;

#[derive(Clone, Debug)]
pub(crate) struct ScanRangeParams {
    pub(crate) range: ScanRange,
    pub(crate) volume_id: Option<i32>,
    pub(crate) empty: Option<bool>,
    pub(crate) has_more: Option<bool>,
}

impl ScanRangeParams {
    pub(crate) fn file(file: FileScanRange) -> Self {
        Self {
            range: ScanRange::File(file),
            volume_id: None,
            empty: Some(false),
            has_more: Some(false),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ScanRange {
    File(FileScanRange),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileFormat {
    Parquet,
    #[allow(dead_code)]
    Orc,
}

impl FileFormat {
    pub(crate) fn as_native_name(self) -> &'static str {
        match self {
            Self::Parquet => "PARQUET",
            Self::Orc => "ORC",
        }
    }

    #[cfg(feature = "compat")]
    fn to_thrift(self) -> descriptors::THdfsFileFormat {
        match self {
            Self::Parquet => descriptors::THdfsFileFormat::PARQUET,
            Self::Orc => descriptors::THdfsFileFormat::ORC,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FileScanRange {
    pub(crate) file_format: FileFormat,
    pub(crate) full_path: Option<String>,
    pub(crate) relative_path: Option<String>,
    pub(crate) table_id: Option<i64>,
    pub(crate) offset: i64,
    pub(crate) length: i64,
    pub(crate) file_length: i64,
    pub(crate) delete_files: Vec<IcebergDeleteFile>,
    pub(crate) deletion_vector_descriptor: Option<DeletionVectorDescriptor>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) modification_time: Option<i64>,
    pub(crate) datacache_options: Option<DatacacheOptions>,
    pub(crate) included_positions: Vec<i64>,
    pub(crate) serialized_split: Option<String>,
    pub(crate) use_iceberg_jni_metadata_reader: bool,
    pub(crate) ivm_change_op: Option<i8>,
    pub(crate) file_pruning_min_max_values: Option<BTreeMap<i32, FilePruningMinMaxValue>>,
    pub(crate) compat_change_op_slot_id: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergFileFormat {
    Parquet,
}

impl IcebergFileFormat {
    pub(crate) fn as_native_name(self) -> &'static str {
        match self {
            Self::Parquet => "PARQUET",
        }
    }

    #[cfg(feature = "compat")]
    fn to_thrift(self) -> descriptors::THdfsFileFormat {
        match self {
            Self::Parquet => descriptors::THdfsFileFormat::PARQUET,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IcebergFileContent {
    PositionDeletes,
    EqualityDeletes,
}

impl IcebergFileContent {
    pub(crate) fn as_native_name(self) -> &'static str {
        match self {
            Self::PositionDeletes => "POSITION_DELETES",
            Self::EqualityDeletes => "EQUALITY_DELETES",
        }
    }

    #[cfg(feature = "compat")]
    fn to_thrift(self) -> types::TIcebergFileContent {
        match self {
            Self::PositionDeletes => types::TIcebergFileContent::POSITION_DELETES,
            Self::EqualityDeletes => types::TIcebergFileContent::EQUALITY_DELETES,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergDeleteFile {
    pub(crate) full_path: Option<String>,
    pub(crate) file_format: IcebergFileFormat,
    pub(crate) file_content: IcebergFileContent,
    pub(crate) length: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct DeletionVectorDescriptor {
    pub(crate) storage_type: Option<String>,
    pub(crate) path_or_inline_dv: Option<String>,
    pub(crate) offset: Option<i64>,
    pub(crate) size_in_bytes: Option<i64>,
    pub(crate) cardinality: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct DatacacheOptions {
    pub(crate) enable_populate_datacache: Option<bool>,
    pub(crate) priority: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FilePruningValueKind {
    Bool,
    Int,
    Float,
}

#[derive(Clone, Debug)]
pub(crate) struct FilePruningMinMaxValue {
    pub(crate) value_kind: FilePruningValueKind,
    pub(crate) has_null: bool,
    pub(crate) all_null: bool,
    pub(crate) min_int_value: Option<i64>,
    pub(crate) max_int_value: Option<i64>,
    pub(crate) min_float_value: Option<f64>,
    pub(crate) max_float_value: Option<f64>,
}

#[cfg(feature = "compat")]
pub(crate) fn thrift_scan_range_params_from_native(
    src: &ScanRangeParams,
) -> Result<internal_service::TScanRangeParams, String> {
    let scan_range = match &src.range {
        ScanRange::File(file) => plan_nodes::TScanRange::new(
            None::<plan_nodes::TInternalScanRange>,
            None::<Vec<u8>>,
            None::<plan_nodes::TBrokerScanRange>,
            None::<plan_nodes::TEsScanRange>,
            Some(thrift_hdfs_scan_range_from_native(file)?),
            None::<plan_nodes::TBinlogScanRange>,
            None::<plan_nodes::TBenchmarkScanRange>,
        ),
    };
    Ok(internal_service::TScanRangeParams::new(
        scan_range,
        src.volume_id,
        src.empty,
        src.has_more,
    ))
}

#[cfg(feature = "compat")]
pub(crate) fn thrift_scan_range_map_from_native(
    src: &BTreeMap<i32, Vec<ScanRangeParams>>,
) -> Result<BTreeMap<i32, Vec<internal_service::TScanRangeParams>>, String> {
    src.iter()
        .map(|(node_id, ranges)| {
            Ok((
                *node_id,
                ranges
                    .iter()
                    .map(thrift_scan_range_params_from_native)
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        })
        .collect()
}

#[cfg(feature = "compat")]
fn thrift_hdfs_scan_range_from_native(
    src: &FileScanRange,
) -> Result<plan_nodes::THdfsScanRange, String> {
    Ok(plan_nodes::THdfsScanRange::new(
        src.relative_path.clone(),
        Some(src.offset),
        Some(src.length),
        None::<i64>,
        Some(src.file_length),
        Some(src.file_format.to_thrift()),
        None::<descriptors::TTextFileDesc>,
        src.full_path.clone(),
        None::<Vec<String>>,
        None::<bool>,
        if src.delete_files.is_empty() {
            None
        } else {
            Some(
                src.delete_files
                    .iter()
                    .map(thrift_delete_file_from_native)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        },
        None::<i64>,
        None::<bool>,
        None::<String>,
        None::<String>,
        src.modification_time,
        src.datacache_options
            .as_ref()
            .map(thrift_datacache_options_from_native),
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<BTreeMap<String, String>>,
        None::<Vec<types::TSlotId>>,
        Some(src.use_iceberg_jni_metadata_reader),
        src.serialized_split.clone().unwrap_or_default(),
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<plan_nodes::TPaimonDeletionFile>,
        thrift_extended_columns_from_native(src)?,
        None::<descriptors::THdfsPartition>,
        src.table_id,
        src.deletion_vector_descriptor
            .as_ref()
            .map(thrift_deletion_vector_from_native),
        None::<String>,
        None::<i64>,
        None::<bool>,
        src.file_pruning_min_max_values
            .as_ref()
            .map(thrift_file_pruning_min_max_values_from_native)
            .transpose()?,
        None::<i32>,
        src.first_row_id,
        src.data_sequence_number,
        if src.included_positions.is_empty() {
            None
        } else {
            Some(src.included_positions.clone())
        },
    ))
}

#[cfg(feature = "compat")]
fn thrift_delete_file_from_native(
    src: &IcebergDeleteFile,
) -> Result<plan_nodes::TIcebergDeleteFile, String> {
    Ok(plan_nodes::TIcebergDeleteFile::new(
        src.full_path.clone(),
        Some(src.file_format.to_thrift()),
        Some(src.file_content.to_thrift()),
        src.length,
    ))
}

#[cfg(feature = "compat")]
fn thrift_deletion_vector_from_native(
    src: &DeletionVectorDescriptor,
) -> plan_nodes::TDeletionVectorDescriptor {
    plan_nodes::TDeletionVectorDescriptor::new(
        src.storage_type.clone(),
        src.path_or_inline_dv.clone(),
        src.offset,
        src.size_in_bytes,
        src.cardinality,
    )
}

#[cfg(feature = "compat")]
fn thrift_datacache_options_from_native(src: &DatacacheOptions) -> data_cache::TDataCacheOptions {
    data_cache::TDataCacheOptions::new(src.enable_populate_datacache, src.priority)
}

#[cfg(feature = "compat")]
fn thrift_file_pruning_min_max_values_from_native(
    src: &BTreeMap<i32, FilePruningMinMaxValue>,
) -> Result<BTreeMap<i32, exprs::TExprMinMaxValue>, String> {
    src.iter()
        .map(|(ordinal, value)| {
            Ok((
                *ordinal,
                exprs::TExprMinMaxValue::new(
                    thrift_file_pruning_value_kind(value.value_kind),
                    value.has_null,
                    value.all_null,
                    value.min_int_value,
                    value.max_int_value,
                    value.min_float_value.map(OrderedFloat),
                    value.max_float_value.map(OrderedFloat),
                ),
            ))
        })
        .collect()
}

#[cfg(feature = "compat")]
fn thrift_file_pruning_value_kind(kind: FilePruningValueKind) -> exprs::TExprNodeType {
    match kind {
        FilePruningValueKind::Bool => exprs::TExprNodeType::BOOL_LITERAL,
        FilePruningValueKind::Int => exprs::TExprNodeType::INT_LITERAL,
        FilePruningValueKind::Float => exprs::TExprNodeType::FLOAT_LITERAL,
    }
}

#[cfg(feature = "compat")]
fn thrift_int_literal_expr(value: i64) -> exprs::TExpr {
    exprs::TExpr::new(vec![crate::sql::codegen::expr_compiler::int_literal_node(
        value,
    )])
}

#[cfg(feature = "compat")]
fn thrift_extended_columns_from_native(
    src: &FileScanRange,
) -> Result<Option<BTreeMap<types::TSlotId, exprs::TExpr>>, String> {
    match (src.ivm_change_op, src.compat_change_op_slot_id) {
        (Some(op), Some(slot_id)) => {
            crate::exec::change_op::validate_change_op_value(op)?;
            Ok(Some(BTreeMap::from([(
                slot_id,
                thrift_int_literal_expr(op as i64),
            )])))
        }
        _ => Ok(None),
    }
}

#[cfg(all(test, feature = "compat"))]
mod compat_tests {
    use super::*;

    fn native_file_range() -> ScanRangeParams {
        ScanRangeParams {
            range: ScanRange::File(FileScanRange {
                file_format: FileFormat::Parquet,
                full_path: Some("s3://bucket/data.parquet".to_string()),
                relative_path: Some("data.parquet".to_string()),
                table_id: Some(7),
                offset: 8,
                length: 16,
                file_length: 128,
                delete_files: vec![IcebergDeleteFile {
                    full_path: Some("s3://bucket/delete.parquet".to_string()),
                    file_format: IcebergFileFormat::Parquet,
                    file_content: IcebergFileContent::PositionDeletes,
                    length: Some(64),
                }],
                deletion_vector_descriptor: Some(DeletionVectorDescriptor {
                    storage_type: Some("PUFFIN".to_string()),
                    path_or_inline_dv: Some("s3://bucket/dv.puffin".to_string()),
                    offset: Some(12),
                    size_in_bytes: Some(34),
                    cardinality: Some(5),
                }),
                first_row_id: Some(1000),
                data_sequence_number: Some(44),
                modification_time: Some(123_456),
                datacache_options: Some(DatacacheOptions {
                    enable_populate_datacache: Some(true),
                    priority: Some(3),
                }),
                included_positions: vec![3, 5, 8],
                serialized_split: Some("{\"split\":1}".to_string()),
                use_iceberg_jni_metadata_reader: true,
                ivm_change_op: Some(-1),
                file_pruning_min_max_values: Some(BTreeMap::from([
                    (
                        0,
                        FilePruningMinMaxValue {
                            value_kind: FilePruningValueKind::Int,
                            has_null: true,
                            all_null: false,
                            min_int_value: Some(10),
                            max_int_value: Some(20),
                            min_float_value: None,
                            max_float_value: None,
                        },
                    ),
                    (
                        1,
                        FilePruningMinMaxValue {
                            value_kind: FilePruningValueKind::Float,
                            has_null: false,
                            all_null: false,
                            min_int_value: None,
                            max_int_value: None,
                            min_float_value: Some(1.5),
                            max_float_value: Some(9.25),
                        },
                    ),
                ])),
                compat_change_op_slot_id: None,
            }),
            volume_id: Some(13),
            empty: Some(false),
            has_more: Some(false),
        }
    }

    #[test]
    fn compat_projection_preserves_native_file_scan_range_fields() {
        let native = native_file_range();

        let thrift = thrift_scan_range_params_from_native(&native).expect("project native range");
        assert_eq!(thrift.volume_id, Some(13));
        assert_eq!(thrift.empty, Some(false));
        assert_eq!(thrift.has_more, Some(false));
        let hdfs = thrift
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs projection");

        assert_eq!(
            hdfs.file_format,
            Some(descriptors::THdfsFileFormat::PARQUET)
        );
        assert_eq!(hdfs.full_path.as_deref(), Some("s3://bucket/data.parquet"));
        assert_eq!(hdfs.relative_path.as_deref(), Some("data.parquet"));
        assert_eq!(hdfs.table_id, Some(7));
        assert_eq!(hdfs.offset, Some(8));
        assert_eq!(hdfs.length, Some(16));
        assert_eq!(hdfs.file_length, Some(128));
        assert_eq!(hdfs.first_row_id, Some(1000));
        assert_eq!(hdfs.data_sequence_number, Some(44));
        assert_eq!(hdfs.modification_time, Some(123_456));
        assert_eq!(hdfs.included_positions, Some(vec![3, 5, 8]));
        assert_eq!(hdfs.serialized_split, Some("{\"split\":1}".to_string()));
        assert_eq!(hdfs.use_iceberg_jni_metadata_reader, Some(true));
        let pruning = hdfs.min_max_values.as_ref().expect("file pruning stats");
        assert_eq!(pruning[&0].type_, exprs::TExprNodeType::INT_LITERAL);
        assert_eq!(pruning[&0].has_null, true);
        assert_eq!(pruning[&0].min_int_value, Some(10));
        assert_eq!(pruning[&0].max_int_value, Some(20));
        assert_eq!(pruning[&1].type_, exprs::TExprNodeType::FLOAT_LITERAL);
        assert_eq!(pruning[&1].min_float_value.map(|v| v.0), Some(1.5));
        assert_eq!(pruning[&1].max_float_value.map(|v| v.0), Some(9.25));

        let delete = &hdfs.delete_files.as_ref().expect("delete files")[0];
        assert_eq!(
            delete.full_path.as_deref(),
            Some("s3://bucket/delete.parquet")
        );
        assert_eq!(
            delete.file_format,
            Some(descriptors::THdfsFileFormat::PARQUET)
        );
        assert_eq!(
            delete.file_content,
            Some(types::TIcebergFileContent::POSITION_DELETES)
        );
        assert_eq!(delete.length, Some(64));

        let dv = hdfs
            .deletion_vector_descriptor
            .as_ref()
            .expect("deletion vector");
        assert_eq!(dv.storage_type.as_deref(), Some("PUFFIN"));
        assert_eq!(
            dv.path_or_inline_dv.as_deref(),
            Some("s3://bucket/dv.puffin")
        );
        assert_eq!(dv.offset, Some(12));
        assert_eq!(dv.size_in_bytes, Some(34));
        assert_eq!(dv.cardinality, Some(5));

        let cache = hdfs.datacache_options.as_ref().expect("data cache options");
        assert_eq!(cache.enable_populate_datacache, Some(true));
        assert_eq!(cache.priority, Some(3));
    }
}
