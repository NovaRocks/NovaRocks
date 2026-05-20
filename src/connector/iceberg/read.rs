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

#![allow(dead_code)]

// The shared Iceberg read-view contract is now used by catalog extraction.
// Follow-up tasks will migrate MV change planning to the same read semantics.

use std::collections::HashMap;

use crate::sql::catalog::IcebergColumnStats;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergReadDeleteFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergReadDeleteKind {
    Position,
    Equality { equality_field_ids: Vec<i32> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergReadDeleteFile {
    pub(crate) path: String,
    pub(crate) file_format: IcebergReadDeleteFormat,
    pub(crate) kind: IcebergReadDeleteKind,
    pub(crate) length: Option<i64>,
    pub(crate) content_offset: Option<i64>,
    pub(crate) content_size_in_bytes: Option<i64>,
    pub(crate) sequence_number: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) referenced_data_file: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergReadFile {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) record_count: Option<i64>,
    pub(crate) column_stats: Option<HashMap<String, IcebergColumnStats>>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) partition_values: Option<iceberg::spec::Struct>,
    pub(crate) manifest_path: Option<String>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) deletes: Vec<IcebergReadDeleteFile>,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergReadSnapshot {
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) files: Vec<IcebergReadFile>,
}

pub(crate) fn delete_applies_to_data_file(
    delete_file: &IcebergReadDeleteFile,
    data_file: &IcebergReadFile,
) -> bool {
    if let (Some(delete_sequence), Some(data_sequence)) =
        (delete_file.sequence_number, data_file.data_sequence_number)
        && delete_sequence <= data_sequence
    {
        return false;
    }

    if let Some(referenced) = delete_file.referenced_data_file.as_deref()
        && referenced != data_file.path
    {
        return false;
    }

    if let Some(delete_partition) = delete_file.partition_key.as_deref() {
        let Some(delete_spec_id) = delete_file.partition_spec_id else {
            return false;
        };
        let Some(data_spec_id) = data_file.partition_spec_id else {
            return false;
        };
        if delete_spec_id != data_spec_id {
            return false;
        }
        if data_file.partition_key.as_deref() != Some(delete_partition) {
            return false;
        }
    }

    true
}

pub(crate) fn attach_applicable_deletes(
    data_file: &mut IcebergReadFile,
    delete_files: &[IcebergReadDeleteFile],
) {
    let applicable = delete_files
        .iter()
        .filter(|delete_file| delete_applies_to_data_file(delete_file, data_file))
        .cloned()
        .collect::<Vec<_>>();
    data_file.deletes.extend(applicable);
}

pub(crate) fn data_files_matching_delete<'a>(
    snapshot: &'a IcebergReadSnapshot,
    delete_file: &IcebergReadDeleteFile,
) -> Vec<&'a IcebergReadFile> {
    snapshot
        .files
        .iter()
        .filter(|data_file| delete_applies_to_data_file(delete_file, data_file))
        .collect()
}

#[derive(Default)]
struct DeleteApplicabilityIndex {
    by_referenced_data_path: HashMap<String, Vec<IcebergReadDeleteFile>>,
    global: Vec<IcebergReadDeleteFile>,
}

impl DeleteApplicabilityIndex {
    fn push(&mut self, delete_file: IcebergReadDeleteFile) {
        if let Some(referenced_data_file) = delete_file.referenced_data_file.clone() {
            self.by_referenced_data_path
                .entry(referenced_data_file)
                .or_default()
                .push(delete_file);
        } else {
            self.global.push(delete_file);
        }
    }

    fn attach_to(&self, data_file: &mut IcebergReadFile) {
        if let Some(delete_files) = self.by_referenced_data_path.get(&data_file.path) {
            attach_applicable_deletes(data_file, delete_files);
        }
        attach_applicable_deletes(data_file, &self.global);
    }
}

pub(crate) fn iceberg_partition_key(partition: &iceberg::spec::Struct) -> Option<String> {
    if partition.fields().is_empty() {
        None
    } else {
        Some(format!("{partition:?}"))
    }
}

pub(crate) fn build_read_snapshot_at(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<IcebergReadSnapshot, String> {
    use crate::connector::iceberg::catalog::registry::block_on_iceberg;
    use iceberg::spec::{DataContentType, DataFileFormat, ManifestContentType, ManifestStatus};

    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found"))?;

    // Use the schema associated with this snapshot for correct schema-evolution semantics.
    // Snapshot::schema() resolves the schema via schema_id if set, falling back to current.
    let schema = snapshot
        .schema(metadata)
        .map_err(|e| format!("resolve snapshot schema: {e}"))?;
    let field_id_to_name: HashMap<i32, String> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.id, field.name.clone()))
        .collect();

    let file_io = table.file_io();

    block_on_iceberg(async {
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|e| format!("load manifest list: {e}"))?;

        let mut delete_index = DeleteApplicabilityIndex::default();

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|e| format!("load manifest: {e}"))?;

            let partition_spec_id = manifest_file.partition_spec_id;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let df = entry.data_file();
                let sequence_number = Some(
                    entry
                        .sequence_number()
                        .unwrap_or(manifest_file.sequence_number),
                );

                match df.content_type() {
                    DataContentType::PositionDeletes => {
                        let (file_format, content_offset, content_size_in_bytes) = match df
                            .file_format()
                        {
                            DataFileFormat::Parquet => {
                                (IcebergReadDeleteFormat::Parquet, None, None)
                            }
                            DataFileFormat::Puffin => {
                                let offset = df.content_offset().ok_or_else(|| {
                                    format!("Puffin DV {} missing content_offset", df.file_path())
                                })?;
                                let length = df.content_size_in_bytes().ok_or_else(|| {
                                    format!(
                                        "Puffin DV {} missing content_size_in_bytes",
                                        df.file_path()
                                    )
                                })?;
                                (IcebergReadDeleteFormat::Puffin, Some(offset), Some(length))
                            }
                            other => {
                                return Err(format!(
                                    "unsupported iceberg delete file format {:?}: {}",
                                    other,
                                    df.file_path()
                                ));
                            }
                        };

                        delete_index.push(IcebergReadDeleteFile {
                            path: df.file_path().to_string(),
                            file_format,
                            kind: IcebergReadDeleteKind::Position,
                            length: Some(i64::try_from(df.file_size_in_bytes()).map_err(|_| {
                                format!("delete file too large: {}", df.file_path())
                            })?),
                            content_offset,
                            content_size_in_bytes,
                            sequence_number,
                            partition_spec_id: Some(partition_spec_id),
                            partition_key: iceberg_partition_key(df.partition()),
                            referenced_data_file: df.referenced_data_file(),
                        });
                    }
                    DataContentType::EqualityDeletes => {
                        if df.file_format() != DataFileFormat::Parquet {
                            return Err(format!(
                                "unsupported iceberg equality-delete file format {:?}: {}",
                                df.file_format(),
                                df.file_path()
                            ));
                        }
                        let equality_field_ids = df.equality_ids().ok_or_else(|| {
                            format!(
                                "iceberg equality-delete file {} missing equality_ids",
                                df.file_path()
                            )
                        })?;
                        if equality_field_ids.is_empty() {
                            return Err(format!(
                                "iceberg equality-delete file {} has empty equality_ids",
                                df.file_path()
                            ));
                        }

                        delete_index.push(IcebergReadDeleteFile {
                            path: df.file_path().to_string(),
                            file_format: IcebergReadDeleteFormat::Parquet,
                            kind: IcebergReadDeleteKind::Equality { equality_field_ids },
                            length: Some(i64::try_from(df.file_size_in_bytes()).map_err(|_| {
                                format!("delete file too large: {}", df.file_path())
                            })?),
                            content_offset: None,
                            content_size_in_bytes: None,
                            sequence_number,
                            partition_spec_id: Some(partition_spec_id),
                            partition_key: iceberg_partition_key(df.partition()),
                            referenced_data_file: None,
                        });
                    }
                    DataContentType::Data => {}
                }
            }
        }

        let mut files = Vec::new();

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .map_err(|e| format!("load manifest: {e}"))?;

            let mut next_manifest_first_row_id = manifest_file
                .first_row_id
                .map(|v| {
                    i64::try_from(v).map_err(|_| format!("manifest first_row_id too large: {v}"))
                })
                .transpose()?;

            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }

                let df = entry.data_file();
                if df.content_type() != DataContentType::Data {
                    continue;
                }

                let record_count_i64 = i64::try_from(df.record_count())
                    .map_err(|_| format!("record_count too large for {}", df.file_path()))?;
                let first_row_id = df.first_row_id().or(next_manifest_first_row_id);
                if let Some(next) = next_manifest_first_row_id.as_mut() {
                    *next = next.checked_add(record_count_i64).ok_or_else(|| {
                        format!(
                            "first_row_id overflow for manifest {}",
                            manifest_file.manifest_path
                        )
                    })?;
                }

                let null_counts = df.null_value_counts();
                let value_counts = df.value_counts();
                let col_sizes = df.column_sizes();
                let lower = df.lower_bounds();
                let upper = df.upper_bounds();

                let has_any_stats = !null_counts.is_empty()
                    || !value_counts.is_empty()
                    || !col_sizes.is_empty()
                    || !lower.is_empty()
                    || !upper.is_empty();

                let column_stats = if has_any_stats {
                    let mut all_ids = std::collections::HashSet::new();
                    all_ids.extend(null_counts.keys());
                    all_ids.extend(value_counts.keys());
                    all_ids.extend(col_sizes.keys());
                    all_ids.extend(lower.keys());
                    all_ids.extend(upper.keys());

                    let mut stats_map = HashMap::new();
                    for &fid in &all_ids {
                        if let Some(col_name) = field_id_to_name.get(&fid) {
                            let lb = lower
                                .get(&fid)
                                .and_then(|d| d.to_bytes().ok())
                                .map(|b| b.to_vec());
                            let ub = upper
                                .get(&fid)
                                .and_then(|d| d.to_bytes().ok())
                                .map(|b| b.to_vec());
                            stats_map.insert(
                                col_name.clone(),
                                IcebergColumnStats {
                                    null_count: null_counts
                                        .get(&fid)
                                        .map(|&v| i64::try_from(v).unwrap_or(i64::MAX)),
                                    value_count: value_counts
                                        .get(&fid)
                                        .map(|&v| i64::try_from(v).unwrap_or(i64::MAX)),
                                    column_size: col_sizes
                                        .get(&fid)
                                        .map(|&v| i64::try_from(v).unwrap_or(i64::MAX)),
                                    lower_bound: lb,
                                    upper_bound: ub,
                                },
                            );
                        }
                    }
                    Some(stats_map)
                } else {
                    None
                };

                let data_sequence_number = Some(
                    entry
                        .sequence_number()
                        .unwrap_or(manifest_file.sequence_number),
                );
                let mut read_file = IcebergReadFile {
                    path: df.file_path().to_string(),
                    size: i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
                    record_count: Some(record_count_i64),
                    column_stats,
                    partition_spec_id: Some(manifest_file.partition_spec_id),
                    partition_key: iceberg_partition_key(df.partition()),
                    partition_values: Some(df.partition().clone()),
                    manifest_path: Some(manifest_file.manifest_path.clone()),
                    first_row_id,
                    data_sequence_number,
                    deletes: Vec::new(),
                };
                delete_index.attach_to(&mut read_file);
                files.push(read_file);
            }
        }

        Ok(IcebergReadSnapshot {
            snapshot_id: Some(snapshot_id),
            files,
        })
    })
    .map_err(|e| format!("build iceberg read snapshot runtime: {e}"))?
}

pub(crate) fn build_read_snapshot(
    table: &iceberg::table::Table,
) -> Result<IcebergReadSnapshot, String> {
    let metadata = table.metadata();
    match metadata.current_snapshot() {
        Some(s) => build_read_snapshot_at(table, s.snapshot_id()),
        None => Ok(IcebergReadSnapshot {
            snapshot_id: None,
            files: Vec::new(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_file(
        seq: Option<i64>,
        spec_id: Option<i32>,
        partition_key: Option<&str>,
    ) -> IcebergReadFile {
        IcebergReadFile {
            path: "s3://bucket/table/data-1.parquet".to_string(),
            size: 10,
            record_count: Some(1),
            column_stats: None,
            partition_spec_id: spec_id,
            partition_key: partition_key.map(str::to_string),
            partition_values: None,
            manifest_path: None,
            first_row_id: Some(0),
            data_sequence_number: seq,
            deletes: Vec::new(),
        }
    }

    fn equality_delete(
        seq: Option<i64>,
        spec_id: Option<i32>,
        partition_key: Option<&str>,
    ) -> IcebergReadDeleteFile {
        IcebergReadDeleteFile {
            path: "s3://bucket/table/delete-1.parquet".to_string(),
            file_format: IcebergReadDeleteFormat::Parquet,
            kind: IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![3],
            },
            length: Some(10),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: seq,
            partition_spec_id: spec_id,
            partition_key: partition_key.map(str::to_string),
            referenced_data_file: None,
        }
    }

    #[test]
    fn delete_with_older_or_equal_sequence_does_not_apply() {
        let data = data_file(Some(7), None, None);
        let older = equality_delete(Some(6), None, None);
        let equal = equality_delete(Some(7), None, None);

        assert!(!delete_applies_to_data_file(&older, &data));
        assert!(!delete_applies_to_data_file(&equal, &data));
    }

    #[test]
    fn unpartitioned_newer_equality_delete_applies_globally() {
        let data = data_file(Some(7), Some(2), Some("city=A"));
        let delete = equality_delete(Some(8), None, None);

        assert!(delete_applies_to_data_file(&delete, &data));
    }

    #[test]
    fn partitioned_equality_delete_requires_matching_spec_and_partition() {
        let data = data_file(Some(7), Some(2), Some("city=A"));
        let same = equality_delete(Some(8), Some(2), Some("city=A"));
        let different_spec = equality_delete(Some(8), Some(3), Some("city=A"));
        let different_partition = equality_delete(Some(8), Some(2), Some("city=B"));

        assert!(delete_applies_to_data_file(&same, &data));
        assert!(!delete_applies_to_data_file(&different_spec, &data));
        assert!(!delete_applies_to_data_file(&different_partition, &data));
    }

    #[test]
    fn partitioned_equality_delete_requires_spec_id_on_both_sides() {
        let data_without_spec = data_file(Some(7), None, Some("city=A"));
        let data_with_spec = data_file(Some(7), Some(2), Some("city=A"));
        let delete_without_spec = equality_delete(Some(8), None, Some("city=A"));
        let delete_with_spec = equality_delete(Some(8), Some(2), Some("city=A"));

        assert!(!delete_applies_to_data_file(
            &delete_with_spec,
            &data_without_spec
        ));
        assert!(!delete_applies_to_data_file(
            &delete_without_spec,
            &data_with_spec
        ));
    }

    #[test]
    fn referenced_position_delete_requires_matching_data_file() {
        let data = data_file(Some(7), None, None);
        let delete = IcebergReadDeleteFile {
            referenced_data_file: Some(data.path.clone()),
            kind: IcebergReadDeleteKind::Position,
            sequence_number: Some(8),
            ..equality_delete(Some(8), None, None)
        };
        let other = IcebergReadDeleteFile {
            referenced_data_file: Some("s3://bucket/table/other.parquet".to_string()),
            ..delete.clone()
        };

        assert!(delete_applies_to_data_file(&delete, &data));
        assert!(!delete_applies_to_data_file(&other, &data));
    }

    #[test]
    fn read_view_attaches_only_applicable_deletes() {
        let mut data = data_file(Some(5), Some(1), Some("city=A"));
        let applicable = equality_delete(Some(6), Some(1), Some("city=A"));
        let too_old = equality_delete(Some(5), Some(1), Some("city=A"));
        let wrong_partition = equality_delete(Some(6), Some(1), Some("city=B"));

        attach_applicable_deletes(&mut data, &[applicable.clone(), too_old, wrong_partition]);

        assert_eq!(data.deletes, vec![applicable]);
    }

    #[test]
    fn data_files_matching_delete_returns_only_applicable_files() {
        let a = data_file(Some(1), Some(1), Some("city=A"));
        let b = data_file(Some(1), Some(1), Some("city=B"));
        let snapshot = IcebergReadSnapshot {
            snapshot_id: Some(10),
            files: vec![a.clone(), b],
        };
        let delete = equality_delete(Some(2), Some(1), Some("city=A"));

        let files = data_files_matching_delete(&snapshot, &delete);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, a.path);
    }

    #[test]
    fn build_read_snapshot_at_returns_error_for_unknown_snapshot_id() {
        use iceberg::io::FileIO;
        use iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Type,
        };
        use iceberg::table::Table;
        use iceberg::{NamespaceIdent, TableIdent};
        use std::collections::HashMap;

        // Build a minimal TableMetadata with no snapshots.
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "memory://test/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;

        let file_io = FileIO::new_with_memory();
        let ident = TableIdent::new(NamespaceIdent::new("db".to_string()), "table".to_string());

        let table = Table::builder()
            .file_io(file_io)
            .metadata(metadata)
            .identifier(ident)
            .build()
            .unwrap();

        let result = build_read_snapshot_at(&table, 999_i64);
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("snapshot 999 not found"),
            "unexpected error: {msg}"
        );
    }
}
