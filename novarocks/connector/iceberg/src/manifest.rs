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

//! Provider-owned manifest facts used by statistics and mutation planning.

use std::collections::HashMap;

use crate::iceberg::spec::{
    DataContentType, DataFileFormat, Literal as IcebergLiteral, ManifestContentType,
    ManifestStatus, PrimitiveLiteral, TableMetadata, Transform,
};
use crate::iceberg::table::Table;
use crate::read_model::{IcebergReadDeleteFormat, IcebergReadDeleteKind};
use crate::scan_model::{
    IcebergColumnStats, IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat,
    IcebergDeleteFileInfo, IcebergPartitionFieldValue, IcebergPartitionValue,
};

/// Data-file facts extracted from one pinned Iceberg manifest walk.
#[derive(Clone)]
pub struct DataFileWithStats {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Option<crate::iceberg::spec::Struct>,
    pub manifest_path: Option<String>,
    pub partition_field_values: Vec<IcebergPartitionFieldValue>,
    /// Iceberg v3 row-lineage: first row id assigned to this data file.
    pub first_row_id: Option<i64>,
    /// Iceberg v3 row-lineage: data sequence number of the manifest entry.
    pub data_sequence_number: Option<i64>,
    pub delete_files: Vec<IcebergDeleteFileInfo>,
}

pub fn data_file_with_stats_to_iceberg_data_file_info(
    file: DataFileWithStats,
) -> IcebergDataFileInfo {
    IcebergDataFileInfo {
        path: file.path,
        size: file.size,
        row_count: file.record_count,
        column_stats: file.column_stats,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: None,
        included_positions: None,
        delete_files: file.delete_files,
        manifest_path: file.manifest_path,
        partition_values: file.partition_field_values,
    }
}

fn partition_field_values(
    metadata: &TableMetadata,
    schema: &crate::iceberg::spec::Schema,
    spec_id: i32,
    partition: &crate::iceberg::spec::Struct,
) -> Result<Vec<IcebergPartitionFieldValue>, String> {
    let Some(spec) = metadata.partition_spec_by_id(spec_id) else {
        return Err(format!(
            "iceberg table metadata missing partition spec id {spec_id}"
        ));
    };
    let mut values = Vec::with_capacity(spec.fields().len());
    for (idx, field) in spec.fields().iter().enumerate() {
        let source_column = schema
            .field_by_id(field.source_id)
            .map(|source| source.name.clone())
            .unwrap_or_else(|| format!("#{}", field.source_id));
        let value = partition
            .fields()
            .get(idx)
            .and_then(|literal| literal.as_ref())
            .and_then(partition_value_from_literal);
        values.push(IcebergPartitionFieldValue {
            source_column,
            field_name: field.name.clone(),
            transform: partition_transform_name(&field.transform),
            value,
        });
    }
    Ok(values)
}

fn partition_transform_name(transform: &Transform) -> String {
    match transform {
        Transform::Identity => "identity".to_string(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

fn partition_value_from_literal(literal: &IcebergLiteral) -> Option<IcebergPartitionValue> {
    let IcebergLiteral::Primitive(value) = literal else {
        return None;
    };
    match value {
        PrimitiveLiteral::Boolean(value) => Some(IcebergPartitionValue::Boolean(*value)),
        PrimitiveLiteral::Int(value) => Some(IcebergPartitionValue::Int32(*value)),
        PrimitiveLiteral::Long(value) => Some(IcebergPartitionValue::Int64(*value)),
        PrimitiveLiteral::Float(value) => Some(IcebergPartitionValue::Float(value.0)),
        PrimitiveLiteral::Double(value) => Some(IcebergPartitionValue::Double(value.0)),
        PrimitiveLiteral::String(value) => Some(IcebergPartitionValue::String(value.clone())),
        PrimitiveLiteral::Binary(value) => Some(IcebergPartitionValue::Binary(value.clone())),
        PrimitiveLiteral::Int128(_)
        | PrimitiveLiteral::UInt128(_)
        | PrimitiveLiteral::AboveMax
        | PrimitiveLiteral::BelowMin => None,
    }
}

fn equality_delete_column_names_for_field_ids(
    file_path: &str,
    equality_ids: Option<Vec<i32>>,
    field_id_to_name: &HashMap<i32, String>,
) -> Result<Vec<String>, String> {
    let equality_ids = equality_ids
        .ok_or_else(|| format!("iceberg equality-delete file {file_path} missing equality_ids"))?;
    if equality_ids.is_empty() {
        return Err(format!(
            "iceberg equality-delete file {file_path} has empty equality_ids"
        ));
    }
    equality_ids
        .iter()
        .map(|id| {
            field_id_to_name.get(id).cloned().ok_or_else(|| {
                format!("iceberg equality-delete file {file_path} references unknown field id {id}")
            })
        })
        .collect()
}

pub async fn current_equality_delete_column_names(table: &Table) -> Result<Vec<String>, String> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(Vec::new());
    };
    let schema = metadata.current_schema();
    let field_id_to_name: HashMap<i32, String> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.id, field.name.clone()))
        .collect();
    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(|error| format!("load manifest list: {error}"))?;
    let mut columns = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|error| format!("load manifest: {error}"))?;
        for entry in manifest.entries() {
            if entry.status == ManifestStatus::Deleted {
                continue;
            }
            let data_file = entry.data_file();
            if data_file.content_type() != DataContentType::EqualityDeletes {
                continue;
            }
            if data_file.file_format() != DataFileFormat::Parquet {
                return Err(format!(
                    "unsupported iceberg equality-delete file format {:?}: {}",
                    data_file.file_format(),
                    data_file.file_path()
                ));
            }
            columns.extend(equality_delete_column_names_for_field_ids(
                data_file.file_path(),
                data_file.equality_ids(),
                &field_id_to_name,
            )?);
        }
    }
    Ok(columns)
}

fn read_delete_to_catalog_delete(
    delete_file: crate::read_model::IcebergReadDeleteFile,
) -> Result<IcebergDeleteFileInfo, String> {
    let file_format = match delete_file.file_format {
        IcebergReadDeleteFormat::Parquet => IcebergDeleteFileFormat::Parquet,
        IcebergReadDeleteFormat::Puffin => IcebergDeleteFileFormat::Puffin,
    };
    let (file_content, equality_column_names, equality_field_ids) = match delete_file.kind {
        IcebergReadDeleteKind::Position => {
            (IcebergDeleteFileContent::Position, Vec::new(), Vec::new())
        }
        IcebergReadDeleteKind::Equality { equality_field_ids } => {
            if file_format != IcebergDeleteFileFormat::Parquet {
                return Err(format!(
                    "iceberg equality-delete file {} must use Parquet format",
                    delete_file.path
                ));
            }
            (
                IcebergDeleteFileContent::Equality,
                Vec::new(),
                equality_field_ids,
            )
        }
    };
    Ok(IcebergDeleteFileInfo {
        path: delete_file.path,
        file_format,
        file_content,
        length: delete_file.length,
        content_offset: delete_file.content_offset,
        content_size_in_bytes: delete_file.content_size_in_bytes,
        sequence_number: delete_file.sequence_number,
        partition_spec_id: delete_file.partition_spec_id,
        partition_key: delete_file.partition_key,
        equality_column_names,
        equality_field_ids,
    })
}

pub async fn extract_data_files_with_stats_at(
    table: &Table,
    snapshot_id: i64,
) -> Result<Vec<DataFileWithStats>, String> {
    let metadata = table.metadata();
    let snapshot_schema = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("Iceberg snapshot {snapshot_id} is absent from table metadata"))?
        .schema(metadata)
        .map_err(|error| format!("resolve Iceberg snapshot {snapshot_id} schema: {error}"))?;
    let read_snapshot = crate::read_snapshot::build_read_snapshot_at(table, snapshot_id).await?;
    read_snapshot
        .files
        .into_iter()
        .map(|file| {
            let partition_field_values =
                match (file.partition_spec_id, file.partition_values.as_ref()) {
                    (Some(spec_id), Some(partition_values)) => partition_field_values(
                        metadata,
                        snapshot_schema.as_ref(),
                        spec_id,
                        partition_values,
                    )?,
                    _ => Vec::new(),
                };
            let delete_files = file
                .deletes
                .into_iter()
                .map(read_delete_to_catalog_delete)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataFileWithStats {
                path: file.path,
                size: file.size,
                record_count: file.record_count,
                column_stats: file.column_stats,
                partition_spec_id: file.partition_spec_id,
                partition_key: file.partition_key,
                partition_values: file.partition_values,
                manifest_path: file.manifest_path,
                partition_field_values,
                first_row_id: file.first_row_id,
                data_sequence_number: file.data_sequence_number,
                delete_files,
            })
        })
        .collect()
}

pub async fn extract_data_files_with_stats(
    table: &Table,
) -> Result<Vec<DataFileWithStats>, String> {
    match table.metadata().current_snapshot() {
        Some(snapshot) => extract_data_files_with_stats_at(table, snapshot.snapshot_id()).await,
        None => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::{equality_delete_column_names_for_field_ids, read_delete_to_catalog_delete};
    use std::collections::HashMap;

    use crate::read_model::{
        IcebergReadDeleteFile, IcebergReadDeleteFormat, IcebergReadDeleteKind,
    };
    use crate::scan_model::{IcebergDeleteFileContent, IcebergDeleteFileFormat};

    fn read_delete(
        file_format: IcebergReadDeleteFormat,
        kind: IcebergReadDeleteKind,
    ) -> IcebergReadDeleteFile {
        IcebergReadDeleteFile {
            path: "s3://bucket/table/delete-file".to_string(),
            file_format,
            kind,
            length: Some(128),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(7),
            partition_spec_id: Some(1),
            partition_key: Some("city=A".to_string()),
            referenced_data_file: None,
        }
    }

    #[test]
    fn parquet_equality_delete_carries_explicit_field_ids() {
        let delete_file = read_delete(
            IcebergReadDeleteFormat::Parquet,
            IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![3, 1],
            },
        );
        let catalog_delete = read_delete_to_catalog_delete(delete_file).expect("convert");

        assert_eq!(catalog_delete.file_format, IcebergDeleteFileFormat::Parquet);
        assert_eq!(
            catalog_delete.file_content,
            IcebergDeleteFileContent::Equality
        );
        assert_eq!(catalog_delete.equality_field_ids, vec![3, 1]);
        assert!(catalog_delete.equality_column_names.is_empty());
    }

    #[test]
    fn puffin_position_delete_preserves_content_range() {
        let mut delete_file = read_delete(
            IcebergReadDeleteFormat::Puffin,
            IcebergReadDeleteKind::Position,
        );
        delete_file.content_offset = Some(64);
        delete_file.content_size_in_bytes = Some(512);
        let catalog_delete = read_delete_to_catalog_delete(delete_file).expect("convert");

        assert_eq!(catalog_delete.file_format, IcebergDeleteFileFormat::Puffin);
        assert_eq!(
            catalog_delete.file_content,
            IcebergDeleteFileContent::Position
        );
        assert_eq!(catalog_delete.content_offset, Some(64));
        assert_eq!(catalog_delete.content_size_in_bytes, Some(512));
        assert!(catalog_delete.equality_field_ids.is_empty());
    }

    #[test]
    fn puffin_equality_delete_is_rejected() {
        let delete_file = read_delete(
            IcebergReadDeleteFormat::Puffin,
            IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![3],
            },
        );
        let err = read_delete_to_catalog_delete(delete_file).expect_err("reject puffin equality");

        assert!(err.contains("must use Parquet format"));
    }

    #[test]
    fn equality_delete_column_names_follow_current_schema_field_ids() {
        let fields = HashMap::from([(1, "id".to_string()), (2, "category".to_string())]);
        let names =
            equality_delete_column_names_for_field_ids("delete.parquet", Some(vec![2, 1]), &fields)
                .expect("column names");

        assert_eq!(names, vec!["category".to_string(), "id".to_string()]);
    }

    #[test]
    fn equality_delete_column_names_reject_unknown_field_id() {
        let fields = HashMap::from([(1, "id".to_string())]);
        let err =
            equality_delete_column_names_for_field_ids("delete.parquet", Some(vec![7]), &fields)
                .expect_err("unknown field id");

        assert!(err.contains("unknown field id 7"));
    }
}
