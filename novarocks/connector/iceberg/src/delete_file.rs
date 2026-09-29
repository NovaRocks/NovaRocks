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

use std::collections::HashMap;

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::spec::Struct;
use crate::scan_model::{IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergFileFormat {
    Parquet,
    Puffin,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergFileContent {
    Data,
    PositionDeletes,
    EqualityDeletes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergDeleteFileSpec {
    pub path: String,
    pub file_format: IcebergFileFormat,
    pub file_content: IcebergFileContent,
    pub length: Option<u64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub referenced_data_file: Option<String>,
}

impl IcebergDeleteFileSpec {
    pub fn parquet_position_delete(path: String, length: Option<u64>) -> Self {
        Self {
            path,
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::PositionDeletes,
            length,
            content_offset: None,
            content_size_in_bytes: None,
            referenced_data_file: None,
        }
    }

    pub fn puffin_position_delete(
        path: String,
        length: Option<u64>,
        content_offset: i64,
        content_size_in_bytes: i64,
    ) -> Self {
        Self {
            path,
            file_format: IcebergFileFormat::Puffin,
            file_content: IcebergFileContent::PositionDeletes,
            length,
            content_offset: Some(content_offset),
            content_size_in_bytes: Some(content_size_in_bytes),
            referenced_data_file: None,
        }
    }
}

/// Validate the bounded physical delete work attached to one Iceberg data
/// file before any reader opens external files.
pub fn validate_delete_apply_cost(file: &IcebergDataFileInfo) -> Result<(), ConnectorError> {
    const MAX_DELETE_FILES: usize = 1024;
    const MAX_DELETE_BYTES: i64 = 512 * 1024 * 1024;
    if file.delete_files.len() > MAX_DELETE_FILES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!(
                "too many Iceberg delete files attached to {}: count={} max={MAX_DELETE_FILES}",
                file.path,
                file.delete_files.len()
            ),
        ));
    }
    let total_bytes = file
        .delete_files
        .iter()
        .try_fold(0_i64, |total, delete| {
            total.checked_add(delete.length.unwrap_or_default().max(0))
        })
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                format!("Iceberg delete byte total overflows for {}", file.path),
            )
        })?;
    if total_bytes > MAX_DELETE_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!(
                "Iceberg delete files attached to {} exceed byte limit: bytes={total_bytes} max={MAX_DELETE_BYTES}",
                file.path
            ),
        ));
    }
    Ok(())
}

/// Turn sealed Iceberg delete facts into the provider's physical I/O specs.
pub fn delete_specs_for_data_file(
    file: &IcebergDataFileInfo,
) -> Result<Vec<IcebergDeleteFileSpec>, ConnectorError> {
    validate_delete_apply_cost(file)?;
    file.delete_files
        .iter()
        .map(|delete| {
            let file_format = match delete.file_format {
                IcebergDeleteFileFormat::Parquet => IcebergFileFormat::Parquet,
                IcebergDeleteFileFormat::Puffin => IcebergFileFormat::Puffin,
            };
            let file_content = match delete.file_content {
                IcebergDeleteFileContent::Position => IcebergFileContent::PositionDeletes,
                IcebergDeleteFileContent::Equality => IcebergFileContent::EqualityDeletes,
            };
            Ok(IcebergDeleteFileSpec {
                path: delete.path.clone(),
                file_format,
                file_content,
                length: delete.length.and_then(|length| u64::try_from(length).ok()),
                content_offset: delete.content_offset,
                content_size_in_bytes: delete.content_size_in_bytes,
                referenced_data_file: delete.referenced_data_file.clone(),
            })
        })
        .collect()
}

/// Validate and normalize an optional exact row-position inclusion set.
pub fn included_positions_for_data_file(
    file: &IcebergDataFileInfo,
) -> Result<Option<roaring::RoaringTreemap>, ConnectorError> {
    let Some(positions) = &file.included_positions else {
        return Ok(None);
    };
    let mut included = roaring::RoaringTreemap::new();
    for position in positions {
        let position = u64::try_from(*position).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("Iceberg included position is negative for {}", file.path),
            )
        })?;
        included.insert(position);
    }
    Ok(Some(included))
}

/// The partition facts needed to write a position delete against one data file.
///
/// These are extracted from the Iceberg manifest and deliberately retain the
/// provider's native partition value representation.
pub struct ReferencedDataFilePartition {
    pub partition_spec_id: i32,
    pub partition_values: Struct,
}

pub type ReferencedDataFilePartitions = HashMap<String, ReferencedDataFilePartition>;

pub fn insert_referenced_data_file_partition(
    partitions: &mut ReferencedDataFilePartitions,
    path: String,
    partition: ReferencedDataFilePartition,
) -> Result<(), String> {
    match partitions.entry(path) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(partition);
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            let existing = entry.get();
            if existing.partition_spec_id == partition.partition_spec_id
                && existing.partition_values == partition.partition_values
            {
                return Ok(());
            }
            return Err(format!(
                "iceberg data file `{}` has conflicting partition metadata: old partition spec id {}, new partition spec id {}",
                entry.key(),
                existing.partition_spec_id,
                partition.partition_spec_id
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::Struct;
    use crate::scan_model::IcebergDeleteFileInfo;

    #[test]
    fn parquet_position_delete_sets_required_content() {
        let spec = IcebergDeleteFileSpec::parquet_position_delete(
            "/tmp/delete.parquet".to_string(),
            Some(99),
        );

        assert_eq!(spec.file_format, IcebergFileFormat::Parquet);
        assert_eq!(spec.file_content, IcebergFileContent::PositionDeletes);
        assert_eq!(spec.length, Some(99));
        assert_eq!(spec.content_offset, None);
        assert_eq!(spec.content_size_in_bytes, None);
    }

    #[test]
    fn puffin_position_delete_carries_byte_range() {
        let spec = IcebergDeleteFileSpec::puffin_position_delete(
            "/tmp/delete.puffin".to_string(),
            Some(512),
            12,
            34,
        );

        assert_eq!(spec.file_format, IcebergFileFormat::Puffin);
        assert_eq!(spec.file_content, IcebergFileContent::PositionDeletes);
        assert_eq!(spec.length, Some(512));
        assert_eq!(spec.content_offset, Some(12));
        assert_eq!(spec.content_size_in_bytes, Some(34));
    }

    #[test]
    fn converts_sealed_delete_and_position_facts_for_physical_io() {
        let mut file = IcebergDataFileInfo::for_test("data.parquet", 10, 1);
        file.included_positions = Some(vec![3, 7]);
        file.delete_files.push(IcebergDeleteFileInfo {
            record_count: None,
            partition_data_json: None,
            path: "delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Position,
            length: Some(42),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: None,
            partition_spec_id: None,
            partition_key: None,
            referenced_data_file: None,
            equality_column_names: Vec::new(),
            equality_field_ids: Vec::new(),
        });

        let specs = delete_specs_for_data_file(&file).expect("delete specs");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].file_format, IcebergFileFormat::Parquet);
        assert_eq!(specs[0].file_content, IcebergFileContent::PositionDeletes);
        assert_eq!(
            included_positions_for_data_file(&file)
                .expect("positions")
                .expect("present")
                .iter()
                .collect::<Vec<_>>(),
            vec![3, 7]
        );
    }

    #[test]
    fn rejects_negative_included_position_as_corrupt_data() {
        let mut file = IcebergDataFileInfo::for_test("data.parquet", 10, 1);
        file.included_positions = Some(vec![-1]);

        let error = included_positions_for_data_file(&file).expect_err("negative position");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn referenced_data_file_partition_rejects_conflicting_metadata() {
        let mut partitions = ReferencedDataFilePartitions::new();
        let partition = || ReferencedDataFilePartition {
            partition_spec_id: 7,
            partition_values: Struct::empty(),
        };
        insert_referenced_data_file_partition(
            &mut partitions,
            "s3://warehouse/data.parquet".to_string(),
            partition(),
        )
        .expect("insert partition facts");
        insert_referenced_data_file_partition(
            &mut partitions,
            "s3://warehouse/data.parquet".to_string(),
            partition(),
        )
        .expect("identical partition facts are idempotent");

        let error = insert_referenced_data_file_partition(
            &mut partitions,
            "s3://warehouse/data.parquet".to_string(),
            ReferencedDataFilePartition {
                partition_spec_id: 8,
                partition_values: Struct::empty(),
            },
        )
        .expect_err("conflicting partition facts must fail");
        assert!(error.contains("conflicting partition metadata"));
    }
}
