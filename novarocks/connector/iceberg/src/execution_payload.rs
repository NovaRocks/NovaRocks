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

//! Provider-private frozen Iceberg scan payloads.

use arrow::datatypes::DataType;
use bytes::Bytes;
use novarocks_fs::{
    FileCancellation, FileError, FileErrorKind, FileIdentity, inspect_parquet_metadata,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorPrepareSplitRequest};
use serde::{Deserialize, Serialize};

use crate::access_binding::IcebergReadBinding;
use crate::delta::IcebergDeltaSplitPayload;
use crate::metadata_batch_reader::MetadataTableType;
use crate::scan_model::{IcebergDataFileInfo, IcebergPhysicalPredicate};

pub const ICEBERG_SPLIT_V5: u16 = 5;

#[derive(Deserialize, Serialize)]
pub struct SplitPayload {
    pub version: u16,
    pub owner_instance_id: String,
    pub incarnation: [u8; 16],
    pub namespace: String,
    pub table: String,
    pub snapshot_id: Option<i64>,
    #[serde(default)]
    pub table_uuid: Option<String>,
    #[serde(default)]
    pub schema_id: Option<i32>,
    pub units: Vec<IcebergFrozenScanUnitPayload>,
    pub projection: Vec<usize>,
    pub limit: Option<u64>,
    #[serde(default)]
    pub physical_predicates: Vec<IcebergPhysicalPredicate>,
    #[serde(default)]
    pub fact_columns: Vec<IcebergScanFactColumnV1>,
    #[serde(default)]
    pub name_mapping: Option<String>,
    #[serde(default)]
    pub delta: Option<IcebergDeltaSplitPayload>,
    #[serde(default)]
    pub metadata: Option<IcebergMetadataSplitPayloadV1>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct IcebergMetadataSplitPayloadV1 {
    pub metadata_table_type: MetadataTableType,
    pub serialized_table: String,
    pub serialized_payload: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct IcebergFrozenScanUnitPayload {
    pub data_file: IcebergDataFileInfo,
    pub row_groups: Option<Vec<usize>>,
    pub estimated_bytes: Option<u64>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct IcebergScanFactColumnV1 {
    pub field_ordinal: u32,
    pub field_id: i32,
    pub canonical_name: String,
    pub scalar_type: IcebergScanFactScalarTypeV1,
    pub nullable: bool,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
pub enum IcebergScanFactScalarTypeV1 {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Date32,
    TimestampMicros,
    TimestampNanos,
    Utf8,
    Binary,
    Unsupported,
}

pub fn encode_payload(
    payload: &impl Serialize,
    subject: &str,
    max_payload_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(payload)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("serialize Iceberg {subject}: {error}"),
            )
        })
        .and_then(|payload| {
            if payload.len() > max_payload_bytes {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    format!("Iceberg {subject} exceeds the request payload budget"),
                ));
            }
            Ok(Bytes::from(payload))
        })
}

pub fn decode_payload<T: for<'de> Deserialize<'de>>(
    payload: &Bytes,
    subject: &str,
) -> Result<T, ConnectorError> {
    serde_json::from_slice(payload).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode Iceberg {subject}: {error}"),
        )
    })
}

pub fn canonical_split_name_mapping(mapping: &str) -> Result<String, ConnectorError> {
    if mapping.len() > novarocks_spi::connector::MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg name mapping exceeds the provider-private split bound",
        ));
    }
    crate::schema_mapping::canonical_name_mapping(mapping)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))
}

/// Refine one already-authorized file into sealed local row-group leaves.
pub fn materialize_local_scan_units(
    binding: &IcebergReadBinding,
    frozen_units: Vec<IcebergFrozenScanUnitPayload>,
    special_unit: bool,
    request: &ConnectorPrepareSplitRequest,
) -> Result<Vec<IcebergFrozenScanUnitPayload>, ConnectorError> {
    if special_unit {
        return Ok(frozen_units);
    }
    let mut result = Vec::with_capacity(frozen_units.len());
    for unit in frozen_units {
        request.check_active()?;
        if unit.row_groups.is_some() || !is_parquet_path(&unit.data_file.path) {
            result.push(unit);
            continue;
        }
        let file_size = u64::try_from(unit.data_file.size).map_err(|_| {
            corrupt(format!(
                "Iceberg data file {} has a negative size",
                unit.data_file.path
            ))
        })?;
        let access = binding.resolve_access(&unit.data_file.path)?;
        let file = access
            .bind_location(
                &unit.data_file.path,
                FileIdentity::new(&unit.data_file.path, file_size, None),
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })?;
        let context = binding.file_read_context(
            FileCancellation::from_connector_request(&request.context),
            request.context.deadline(),
        )?;
        let inspection = inspect_parquet_metadata(file, None, context).map_err(map_footer_error)?;
        let layout = inspection.row_groups();
        request.check_active()?;
        if layout.len() <= 1 {
            result.push(unit);
            continue;
        }
        let total = unit
            .estimated_bytes
            .ok_or_else(|| corrupt("Iceberg Parquet split unit must carry a known frozen cost"))?;
        let costs = distribute_unit_cost(total, layout)?;
        for (group, estimated_bytes) in layout.iter().zip(costs) {
            result.push(IcebergFrozenScanUnitPayload {
                data_file: unit.data_file.clone(),
                row_groups: Some(vec![group.ordinal as usize]),
                estimated_bytes: Some(estimated_bytes),
            });
        }
    }
    Ok(result)
}

fn distribute_unit_cost(
    total: u64,
    layout: &[novarocks_fs::ParquetRowGroupLayout],
) -> Result<Vec<u64>, ConnectorError> {
    let weight_total = layout.iter().try_fold(0_u64, |sum, row_group| {
        sum.checked_add(row_group.compressed_bytes)
    });
    let mut costs = Vec::with_capacity(layout.len());
    if let Some(weight_total) = weight_total.filter(|total| *total > 0) {
        let mut assigned = 0_u64;
        for (index, row_group) in layout.iter().enumerate() {
            let cost = if index + 1 == layout.len() {
                total.checked_sub(assigned).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "Iceberg row-group cost accounting underflowed",
                    )
                })?
            } else {
                total
                    .checked_mul(row_group.compressed_bytes)
                    .and_then(|value| value.checked_div(weight_total))
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "Iceberg row-group cost accounting overflowed",
                        )
                    })?
            };
            assigned = assigned.checked_add(cost).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg row-group cost accounting overflowed",
                )
            })?;
            costs.push(cost);
        }
    } else {
        let count = u64::try_from(layout.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg row-group count overflows u64",
            )
        })?;
        if count == 0 {
            return Ok(costs);
        }
        let base = total / count;
        let mut remainder = total % count;
        for _ in layout {
            let extra = u64::from(remainder > 0);
            remainder = remainder.saturating_sub(extra);
            costs.push(base + extra);
        }
    }
    Ok(costs)
}

pub fn scan_fact_scalar_type(data_type: &DataType) -> IcebergScanFactScalarTypeV1 {
    match data_type {
        DataType::Boolean => IcebergScanFactScalarTypeV1::Boolean,
        DataType::Int8 => IcebergScanFactScalarTypeV1::Int8,
        DataType::Int16 => IcebergScanFactScalarTypeV1::Int16,
        DataType::Int32 => IcebergScanFactScalarTypeV1::Int32,
        DataType::Int64 => IcebergScanFactScalarTypeV1::Int64,
        DataType::Date32 => IcebergScanFactScalarTypeV1::Date32,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            IcebergScanFactScalarTypeV1::TimestampMicros
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            IcebergScanFactScalarTypeV1::TimestampNanos
        }
        DataType::Utf8 => IcebergScanFactScalarTypeV1::Utf8,
        DataType::Binary => IcebergScanFactScalarTypeV1::Binary,
        _ => IcebergScanFactScalarTypeV1::Unsupported,
    }
}

fn is_parquet_path(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path).to_ascii_lowercase();
    path.ends_with(".parquet") || path.ends_with(".parq")
}

fn map_footer_error(error: FileError) -> ConnectorError {
    let kind = match error.kind() {
        FileErrorKind::Invalid => ConnectorErrorKind::InvalidRequest,
        FileErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
        FileErrorKind::NotFound | FileErrorKind::Corrupt => ConnectorErrorKind::CorruptData,
        FileErrorKind::Permission => ConnectorErrorKind::PermissionDenied,
        FileErrorKind::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        FileErrorKind::Transient => ConnectorErrorKind::Unavailable,
        FileErrorKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        FileErrorKind::Cancelled => ConnectorErrorKind::Cancelled,
        FileErrorKind::AlreadyExists | FileErrorKind::Internal => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, error.to_string())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

#[cfg(test)]
mod tests {
    use super::distribute_unit_cost;

    #[test]
    fn row_group_cost_distribution_is_deterministic() {
        let weighted = vec![
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 0,
                compressed_bytes: 1,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 1,
                compressed_bytes: 3,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 2,
                compressed_bytes: 6,
                row_count: 1,
            },
        ];
        assert_eq!(
            distribute_unit_cost(101, &weighted).expect("weighted costs"),
            [10, 30, 61]
        );
        assert_eq!(
            distribute_unit_cost(101, &weighted).expect("repeated weighted costs"),
            [10, 30, 61]
        );

        let zero_weight = vec![
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 0,
                compressed_bytes: 0,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 1,
                compressed_bytes: 0,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 2,
                compressed_bytes: 0,
                row_count: 1,
            },
        ];
        assert_eq!(
            distribute_unit_cost(8, &zero_weight).expect("zero-weight costs"),
            [3, 3, 2]
        );
    }
}
