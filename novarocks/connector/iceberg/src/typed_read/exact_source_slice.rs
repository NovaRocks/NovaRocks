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

//! Provider proof and evaluation for an exact replacement source slice.

use std::collections::BTreeMap;

use novarocks_spi::connector::read_stack::{
    ConnectorExactPartitionSliceVolume, ConnectorExactSourcePartitionSlice,
};
use novarocks_spi::connector::{
    ConnectorChangePartitionTransform, ConnectorChangePartitionValue, ConnectorError,
    ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
};

use crate::iceberg::spec::{PartitionSpec, TableMetadata, Transform};
use crate::read_model::IcebergReadFile;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IcebergExactSourcePartitionSlice {
    predicate: ConnectorExactSourcePartitionSlice,
    positions: BTreeMap<i32, Vec<usize>>,
    volume: ConnectorExactPartitionSliceVolume,
}

impl IcebergExactSourcePartitionSlice {
    pub(crate) fn prove(
        predicate: ConnectorExactSourcePartitionSlice,
        metadata: &TableMetadata,
        files: &[IcebergReadFile],
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        let mut positions = BTreeMap::new();
        let mut volume = ConnectorExactPartitionSliceVolume::default();
        for file in files {
            context.check_active()?;
            let spec_id = file.partition_spec_id.ok_or_else(unsupported_spec)?;
            if let std::collections::btree_map::Entry::Vacant(entry) = positions.entry(spec_id) {
                let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "source slice To file names an unknown Iceberg partition spec",
                    )
                })?;
                entry.insert(source_positions(&predicate, spec)?);
            }
            // Validate the actual partition values before the strategy can
            // publish. The execution matcher applies the same typed comparison.
            let key = source_key(file, &positions[&spec_id])?;
            if predicate.keys().binary_search(&key).is_ok() {
                let records = file
                    .record_count
                    .and_then(|count| u128::try_from(count).ok())
                    .ok_or_else(invalid_volume)?;
                let bytes = u128::try_from(file.size).map_err(|_| invalid_volume())?;
                volume.record_count = volume
                    .record_count
                    .checked_add(records)
                    .ok_or_else(invalid_volume)?;
                volume.file_size_in_bytes = volume
                    .file_size_in_bytes
                    .checked_add(bytes)
                    .ok_or_else(invalid_volume)?;
            }
        }
        Ok(Self {
            predicate,
            positions,
            volume,
        })
    }

    pub(crate) const fn volume(&self) -> ConnectorExactPartitionSliceVolume {
        self.volume
    }

    pub(crate) fn includes(&self, file: &IcebergReadFile) -> Result<bool, ConnectorError> {
        let positions = file
            .partition_spec_id
            .and_then(|id| self.positions.get(&id))
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "source slice enumeration differs from its frozen To partition proof",
                )
            })?;
        let key = source_key(file, positions)?;
        Ok(self.predicate.keys().binary_search(&key).is_ok())
    }
}

fn source_positions(
    predicate: &ConnectorExactSourcePartitionSlice,
    spec: &PartitionSpec,
) -> Result<Vec<usize>, ConnectorError> {
    predicate
        .fields()
        .iter()
        .map(|field| {
            spec.fields()
                .iter()
                .position(|candidate| {
                    field.source_field_id.as_ref() == candidate.source_id.to_be_bytes()
                        && matches_transform(field.transform, candidate.transform)
                })
                .ok_or_else(unsupported_spec)
        })
        .collect()
}

fn source_key(
    file: &IcebergReadFile,
    positions: &[usize],
) -> Result<Vec<ConnectorChangePartitionValue>, ConnectorError> {
    let fields = file
        .partition_values
        .as_ref()
        .ok_or_else(unsupported_value)?
        .fields();
    positions
        .iter()
        .map(|&position| {
            let value = fields.get(position).ok_or_else(unsupported_value)?;
            crate::change_planning::exact_partition_value(value.as_ref())
                .ok_or_else(unsupported_value)
        })
        .collect()
}

fn invalid_volume() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "exact source partition slice has invalid data-file volume",
    )
}

fn unsupported_spec() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "source slice is not aligned in every live Iceberg To file partition spec",
    )
}

fn unsupported_value() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "source slice To file partition value cannot be compared exactly",
    )
}

fn matches_transform(expected: ConnectorChangePartitionTransform, actual: Transform) -> bool {
    use ConnectorChangePartitionTransform as Expected;
    match (expected, actual) {
        (Expected::Identity, Transform::Identity)
        | (Expected::Year, Transform::Year)
        | (Expected::Month, Transform::Month)
        | (Expected::Day, Transform::Day)
        | (Expected::Hour, Transform::Hour) => true,
        (Expected::Bucket { buckets }, Transform::Bucket(actual)) => buckets.get() == actual,
        (Expected::Truncate { width }, Transform::Truncate(actual)) => width.get() == actual,
        _ => false,
    }
}
