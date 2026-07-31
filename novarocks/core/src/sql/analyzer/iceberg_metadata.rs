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

//! Resolve the trailing `__nr_meta_<type>__` suffix that the parser-level
//! pre-parse rewrites `<tbl>$<metatype>` into.
//!
//! Mirrors `iceberg_ref::split_ref_suffix` for branch/tag.

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use iceberg::spec::{PrimitiveType, TableMetadata, Type};
use std::sync::Arc;

use crate::connector::iceberg::IcebergMetadataTableType;
use crate::connector::iceberg::scan_model::IcebergTableInfo;
use crate::sql::planner::table::ScanSource;

/// Inspect the trailing identifier part of a qualified name and, if it
/// matches `__nr_meta_<type>__`, return the parts with the suffix stripped
/// plus the parsed metadata-table type.
pub fn split_metadata_suffix(parts: &[String]) -> (Vec<String>, Option<IcebergMetadataTableType>) {
    if let Some(last) = parts.last()
        && let Some(inner) = last
            .strip_prefix("__nr_meta_")
            .and_then(|s| s.strip_suffix("__"))
        && let Ok(ty) = IcebergMetadataTableType::parse(inner)
    {
        return (parts[..parts.len() - 1].to_vec(), Some(ty));
    }
    (parts.to_vec(), None)
}

#[derive(Clone, Debug)]
pub struct MetadataColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl MetadataColumn {
    fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable,
        }
    }
}

/// Build an Arrow `Map<Int32, value>` matching the metadata-scan MapBuilder
/// output (keys non-nullable).
///
/// Field names (`entries` / `key` / `value`) MUST mirror
/// `metadata.rs::iceberg_map_field_names()` exactly: the scan-op builders
/// produce their `Map` columns through that `MapFieldNames`, and
/// `RecordBatch::try_new` compares `Field` names structurally, so any
/// divergence here would fail the column-type check.
fn map_int_to(value: DataType) -> DataType {
    let entries = DataType::Struct(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", value, true)),
        ]
        .into(),
    );
    DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
}

fn list_of(value: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", value, true)))
}

fn files_columns_with_partition(partition_type: DataType) -> Vec<MetadataColumn> {
    vec![
        MetadataColumn::new("content", DataType::Int32, false),
        MetadataColumn::new("file_path", DataType::Utf8, false),
        MetadataColumn::new("file_format", DataType::Utf8, false),
        MetadataColumn::new("spec_id", DataType::Int32, false),
        MetadataColumn::new("record_count", DataType::Int64, false),
        MetadataColumn::new("file_size_in_bytes", DataType::Int64, false),
        MetadataColumn::new("column_sizes", map_int_to(DataType::Int64), true),
        MetadataColumn::new("value_counts", map_int_to(DataType::Int64), true),
        MetadataColumn::new("null_value_counts", map_int_to(DataType::Int64), true),
        MetadataColumn::new("nan_value_counts", map_int_to(DataType::Int64), true),
        MetadataColumn::new("lower_bounds", map_int_to(DataType::Binary), true),
        MetadataColumn::new("upper_bounds", map_int_to(DataType::Binary), true),
        MetadataColumn::new("split_offsets", list_of(DataType::Int64), true),
        MetadataColumn::new("equality_ids", list_of(DataType::Int32), true),
        MetadataColumn::new("sort_order_id", DataType::Int32, true),
        MetadataColumn::new("key_metadata", DataType::Binary, true),
        MetadataColumn::new("first_row_id", DataType::Int64, true),
        MetadataColumn::new("partition", partition_type, true),
    ]
}

fn files_columns() -> Vec<MetadataColumn> {
    files_columns_with_partition(DataType::Utf8)
}

fn iceberg_type_to_arrow_type(ty: &Type) -> Result<DataType, String> {
    match ty {
        Type::Primitive(primitive) => Ok(match primitive {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { precision, scale } => DataType::Decimal128(
                u8::try_from(*precision)
                    .map_err(|_| format!("iceberg decimal precision out of range: {precision}"))?,
                i8::try_from(*scale)
                    .map_err(|_| format!("iceberg decimal scale out of range: {scale}"))?,
            ),
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            PrimitiveType::String | PrimitiveType::Uuid => DataType::Utf8,
            PrimitiveType::Fixed(width) => DataType::FixedSizeBinary(
                i32::try_from(*width)
                    .map_err(|_| format!("iceberg fixed width out of range: {width}"))?,
            ),
            PrimitiveType::Binary | PrimitiveType::Variant => DataType::Binary,
        }),
        other => Err(format!(
            "iceberg metadata partition field must be primitive, got {other:?}"
        )),
    }
}

fn partition_source_type<'a>(metadata: &'a TableMetadata, source_id: i32) -> Option<&'a Type> {
    metadata
        .current_schema()
        .field_by_id(source_id)
        .map(|field| field.field_type.as_ref())
        .or_else(|| {
            metadata.schemas_iter().find_map(|schema| {
                schema
                    .field_by_id(source_id)
                    .map(|field| field.field_type.as_ref())
            })
        })
}

fn partition_struct_type(table: &IcebergTableInfo) -> Result<DataType, String> {
    let Some(serialized) = table.serialized_metadata.as_deref() else {
        return Err(format!(
            "iceberg metadata table {}.{} requires serialized metadata to type partition struct",
            table.namespace, table.table
        ));
    };
    let metadata: TableMetadata = serde_json::from_str(serialized)
        .map_err(|e| format!("parse iceberg table metadata for partition schema failed: {e}"))?;
    let mut specs = metadata.partition_specs_iter().cloned().collect::<Vec<_>>();
    specs.sort_by_key(|spec| spec.spec_id());

    let mut fields: Vec<Arc<Field>> = Vec::new();
    for spec in specs {
        for part_field in spec.fields() {
            let source_type =
                partition_source_type(&metadata, part_field.source_id).ok_or_else(|| {
                    format!(
                        "iceberg partition field {} references missing source field id {}",
                        part_field.name, part_field.source_id
                    )
                })?;
            let result_type = part_field.transform.result_type(source_type).map_err(|e| {
                format!(
                    "infer iceberg partition field {} type: {e}",
                    part_field.name
                )
            })?;
            let arrow_type = iceberg_type_to_arrow_type(&result_type)?;
            if let Some(existing) = fields
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(&part_field.name))
            {
                if existing.data_type() != &arrow_type {
                    return Err(format!(
                        "iceberg partition field {} has incompatible types across specs: {:?} vs {:?}",
                        part_field.name,
                        existing.data_type(),
                        arrow_type
                    ));
                }
                continue;
            }
            fields.push(Arc::new(Field::new(
                part_field.name.clone(),
                arrow_type,
                true,
            )));
        }
    }
    Ok(DataType::Struct(Fields::from(fields)))
}

fn table_info_from_source(source: &ScanSource) -> Option<&IcebergTableInfo> {
    match source {
        ScanSource::IcebergDataFiles { table, .. }
        | ScanSource::IcebergMetadataTable { table, .. }
        | ScanSource::IcebergDeltaTable { table, .. }
        | ScanSource::IcebergVersionTable { table, .. } => Some(table),
        ScanSource::ConnectorPinned
        | ScanSource::StarRocks { .. }
        | ScanSource::IcebergMvTargetState { .. }
        | ScanSource::IcebergMvTargetLocator(_) => None,
    }
}

pub fn metadata_table_schema_for_source(
    ty: IcebergMetadataTableType,
    source: &ScanSource,
) -> Result<Vec<MetadataColumn>, String> {
    let Some(table) = table_info_from_source(source) else {
        return Err("iceberg metadata table requires iceberg table identity".to_string());
    };
    metadata_table_schema_for_table(ty, table)
}

pub fn metadata_table_schema_for_table(
    ty: IcebergMetadataTableType,
    table: &IcebergTableInfo,
) -> Result<Vec<MetadataColumn>, String> {
    use IcebergMetadataTableType as T;
    match ty {
        T::Files => Ok(files_columns_with_partition(partition_struct_type(table)?)),
        T::LogicalIcebergMetadata => {
            let mut cols = vec![
                MetadataColumn::new("status", DataType::Int32, false),
                MetadataColumn::new("snapshot_id", DataType::Int64, true),
                MetadataColumn::new("sequence_number", DataType::Int64, true),
                MetadataColumn::new("file_sequence_number", DataType::Int64, true),
            ];
            cols.extend(files_columns_with_partition(partition_struct_type(table)?));
            Ok(cols)
        }
        other => Ok(metadata_table_schema(other)),
    }
}

/// Fixed analyzer-level column schema for each Iceberg metadata table.
///
/// Wire types match the Iceberg metadata SPI reader's `build_*_array` helpers in
/// `src/connector/iceberg/metadata.rs`. Logical formatting (e.g. converting
/// the `committed_at` Int64-of-micros into a TIMESTAMPTZ display) is FE's
/// responsibility; the analyzer surfaces the underlying Arrow type.
pub fn metadata_table_schema(ty: IcebergMetadataTableType) -> Vec<MetadataColumn> {
    use IcebergMetadataTableType as T;
    match ty {
        T::Snapshots => vec![
            MetadataColumn::new("committed_at", DataType::Int64, false),
            MetadataColumn::new("snapshot_id", DataType::Int64, false),
            MetadataColumn::new("parent_id", DataType::Int64, true),
            MetadataColumn::new("operation", DataType::Utf8, true),
            MetadataColumn::new("manifest_list", DataType::Utf8, false),
            // TODO: BE actually emits this column as Map<Utf8,Utf8>; the analyzer
            // surfaces it as Utf8 for now so users can SELECT it without the
            // analyzer rejecting unknown Map operations. Refining this requires
            // teaching the scope/expr layer about Map-typed metadata columns
            // and is tracked alongside the broader metadata-table follow-ups.
            MetadataColumn::new("summary", DataType::Utf8, false),
        ],
        T::History => vec![
            MetadataColumn::new("made_current_at", DataType::Int64, false),
            MetadataColumn::new("snapshot_id", DataType::Int64, false),
            MetadataColumn::new("parent_id", DataType::Int64, true),
            MetadataColumn::new("is_current_ancestor", DataType::Boolean, false),
        ],
        T::Refs => vec![
            MetadataColumn::new("name", DataType::Utf8, false),
            MetadataColumn::new("type", DataType::Utf8, false),
            MetadataColumn::new("snapshot_id", DataType::Int64, false),
            MetadataColumn::new("max_reference_age_in_ms", DataType::Int64, true),
            MetadataColumn::new("min_snapshots_to_keep", DataType::Int32, true),
            MetadataColumn::new("max_snapshot_age_in_ms", DataType::Int64, true),
        ],
        T::Partitions => vec![
            // The dynamic `partition` struct column is appended at lowering
            // time when the table's partition spec is available (Task A4).
            // Analyzer surfaces only the count columns.
            MetadataColumn::new("record_count", DataType::Int64, false),
            MetadataColumn::new("file_count", DataType::Int64, false),
            MetadataColumn::new("position_delete_file_count", DataType::Int64, true),
            MetadataColumn::new("equality_delete_file_count", DataType::Int64, true),
        ],
        T::Files => files_columns(),
        T::Manifests => vec![
            MetadataColumn::new("content", DataType::Int32, false),
            MetadataColumn::new("path", DataType::Utf8, false),
            MetadataColumn::new("length", DataType::Int64, false),
            MetadataColumn::new("partition_spec_id", DataType::Int32, false),
            MetadataColumn::new("added_snapshot_id", DataType::Int64, true),
            MetadataColumn::new("added_data_files_count", DataType::Int32, false),
            MetadataColumn::new("existing_data_files_count", DataType::Int32, false),
            MetadataColumn::new("deleted_data_files_count", DataType::Int32, false),
            MetadataColumn::new("added_rows_count", DataType::Int64, false),
            MetadataColumn::new("existing_rows_count", DataType::Int64, false),
            MetadataColumn::new("deleted_rows_count", DataType::Int64, false),
            MetadataColumn::new(
                "partition_summaries",
                list_of(DataType::Struct(
                    vec![
                        Arc::new(Field::new("contains_null", DataType::Boolean, true)),
                        Arc::new(Field::new("contains_nan", DataType::Boolean, true)),
                        Arc::new(Field::new("lower_bound", DataType::Utf8, true)),
                        Arc::new(Field::new("upper_bound", DataType::Utf8, true)),
                    ]
                    .into(),
                )),
                true,
            ),
        ],
        T::LogicalIcebergMetadata => {
            // `first_row_id` is intentionally NOT prepended here: it is a file
            // property already provided by `files_columns()`. Listing it twice
            // would produce two same-named columns (invalid RecordBatch).
            let mut cols = vec![
                MetadataColumn::new("status", DataType::Int32, false),
                MetadataColumn::new("snapshot_id", DataType::Int64, true),
                MetadataColumn::new("sequence_number", DataType::Int64, true),
                MetadataColumn::new("file_sequence_number", DataType::Int64, true),
            ];
            cols.extend(files_columns());
            cols
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_suffix_is_stripped() {
        let parts = vec![
            "db".to_string(),
            "t".to_string(),
            "__nr_meta_snapshots__".to_string(),
        ];
        let (stripped, ty) = split_metadata_suffix(&parts);
        assert_eq!(stripped, vec!["db".to_string(), "t".to_string()]);
        assert_eq!(ty, Some(IcebergMetadataTableType::Snapshots));
    }

    #[test]
    fn three_part_qualified_name_works() {
        let parts = vec![
            "ice".to_string(),
            "db".to_string(),
            "t".to_string(),
            "__nr_meta_history__".to_string(),
        ];
        let (stripped, ty) = split_metadata_suffix(&parts);
        assert_eq!(
            stripped,
            vec!["ice".to_string(), "db".to_string(), "t".to_string()]
        );
        assert_eq!(ty, Some(IcebergMetadataTableType::History));
    }

    #[test]
    fn refs_and_partitions_round_trip() {
        for (suffix, expected) in [
            ("__nr_meta_refs__", IcebergMetadataTableType::Refs),
            (
                "__nr_meta_partitions__",
                IcebergMetadataTableType::Partitions,
            ),
        ] {
            let parts = vec!["t".to_string(), suffix.to_string()];
            let (_, ty) = split_metadata_suffix(&parts);
            assert_eq!(ty, Some(expected));
        }
    }

    #[test]
    fn unrecognised_metatype_returns_none() {
        let parts = vec!["t".to_string(), "__nr_meta_xyz__".to_string()];
        let (out_parts, ty) = split_metadata_suffix(&parts);
        assert_eq!(out_parts, parts);
        assert_eq!(ty, None);
    }

    #[test]
    fn no_suffix_passthrough() {
        let parts = vec!["db".to_string(), "t".to_string()];
        let (out_parts, ty) = split_metadata_suffix(&parts);
        assert_eq!(out_parts, parts);
        assert_eq!(ty, None);
    }

    #[test]
    fn metadata_table_schema_snapshots_has_expected_columns() {
        let cols = metadata_table_schema(IcebergMetadataTableType::Snapshots);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "committed_at",
                "snapshot_id",
                "parent_id",
                "operation",
                "manifest_list",
                "summary",
            ]
        );
    }

    #[test]
    fn metadata_table_schema_history_has_expected_columns() {
        let cols = metadata_table_schema(IcebergMetadataTableType::History);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "made_current_at",
                "snapshot_id",
                "parent_id",
                "is_current_ancestor",
            ]
        );
    }

    #[test]
    fn metadata_table_schema_refs_has_expected_columns() {
        let cols = metadata_table_schema(IcebergMetadataTableType::Refs);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "name",
                "type",
                "snapshot_id",
                "max_reference_age_in_ms",
                "min_snapshots_to_keep",
                "max_snapshot_age_in_ms",
            ]
        );
    }

    #[test]
    fn metadata_table_schema_partitions_has_expected_columns() {
        let cols = metadata_table_schema(IcebergMetadataTableType::Partitions);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"record_count"));
        assert!(names.contains(&"file_count"));
        assert!(names.contains(&"position_delete_file_count"));
        assert!(names.contains(&"equality_delete_file_count"));
    }

    #[test]
    fn files_schema_has_expected_columns() {
        let names: Vec<String> = metadata_table_schema(IcebergMetadataTableType::Files)
            .iter()
            .map(|c| c.name.clone())
            .collect();
        for col in [
            "content",
            "file_path",
            "file_format",
            "spec_id",
            "record_count",
            "file_size_in_bytes",
            "column_sizes",
            "lower_bounds",
            "split_offsets",
            "equality_ids",
            "first_row_id",
            "partition",
        ] {
            assert!(names.contains(&col.to_string()), "missing {col}");
        }
    }
    #[test]
    fn manifests_schema_has_partition_summaries() {
        let names: Vec<String> = metadata_table_schema(IcebergMetadataTableType::Manifests)
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert!(names.contains(&"partition_summaries".to_string()));
        assert!(names.contains(&"added_snapshot_id".to_string()));
    }
    #[test]
    fn entries_schema_superset_of_files() {
        let names: Vec<String> =
            metadata_table_schema(IcebergMetadataTableType::LogicalIcebergMetadata)
                .iter()
                .map(|c| c.name.clone())
                .collect();
        for col in [
            "status",
            "snapshot_id",
            "sequence_number",
            "file_sequence_number",
            "file_path",
            "record_count",
        ] {
            assert!(names.contains(&col.to_string()), "missing {col}");
        }
    }
}
