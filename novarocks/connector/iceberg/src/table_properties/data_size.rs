// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The two declared Iceberg DataSize properties, using Airlift units 1.13.
//!
//! This normalizes a property value; it does not execute a property mutation.

use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, DdlPropertyValue,
};
use std::{error::Error, fmt};

pub use novarocks_spi::connector::DataSizeParseFailure;
pub(crate) use novarocks_spi::connector::parse_airlift_data_size;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataSizeProperty {
    ParquetWriterRowGroupSize,
    TargetMaxFileSize,
}
impl DataSizeProperty {
    pub const fn name(self) -> &'static str {
        match self {
            Self::ParquetWriterRowGroupSize => "parquet_writer_row_group_size",
            Self::TargetMaxFileSize => "target_max_file_size",
        }
    }
    pub const fn raw_key(self) -> &'static str {
        match self {
            Self::ParquetWriterRowGroupSize => "write.parquet.row-group-size-bytes",
            Self::TargetMaxFileSize => "write.target-file-size-bytes",
        }
    }
}

/// A parsed and property-validated count, not a write capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NormalizedDataSize {
    property: DataSizeProperty,
    bytes: i64,
}
impl NormalizedDataSize {
    pub const fn property(self) -> DataSizeProperty {
        self.property
    }
    pub const fn bytes(self) -> i64 {
        self.bytes
    }
    pub const fn raw_key(self) -> &'static str {
        self.property.raw_key()
    }
    pub fn raw_value(self) -> String {
        self.bytes.to_string()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataSizeRangeFailure {
    ZeroParquetRowGroup,
    ParquetRowGroupAboveTwoGiB,
    ParquetRowGroupOutsideUsize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataSizeFailureReason {
    ExpectedVarchar,
    Parse(DataSizeParseFailure),
    Range(DataSizeRangeFailure),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataSizePropertyFailure {
    property: DataSizeProperty,
    reason: DataSizeFailureReason,
}
impl DataSizePropertyFailure {
    pub const fn property(self) -> DataSizeProperty {
        self.property
    }
    pub const fn reason(self) -> DataSizeFailureReason {
        self.reason
    }
}
impl fmt::Display for DataSizePropertyFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid Iceberg {}: {:?}",
            self.property.name(),
            self.reason
        )
    }
}
impl Error for DataSizePropertyFailure {}
impl FailureProjection for DataSizePropertyFailure {
    fn failure_fact(&self) -> FailureFact {
        ConnectorErrorKind::InvalidRequest.failure_fact()
    }
}
fn invalid(property: DataSizeProperty, reason: DataSizeFailureReason) -> ConnectorError {
    ConnectorError::from_domain(
        ConnectorErrorKind::InvalidRequest,
        DataSizePropertyFailure { property, reason },
    )
}

/// SQL NULL remains NULL. DEFAULT/absence are separate application facts.
/// The original caller supplies its operation control; it is never replaced.
pub fn normalize_data_size_property(
    property: DataSizeProperty,
    value: &DdlPropertyValue,
    control: &dyn ConnectorOperationControl,
) -> Result<Option<NormalizedDataSize>, ConnectorError> {
    control.check_active()?;
    let text = match value {
        DdlPropertyValue::Null => return Ok(None),
        DdlPropertyValue::Varchar(text) => text,
        _ => return Err(invalid(property, DataSizeFailureReason::ExpectedVarchar)),
    };
    let bytes = parse_airlift_data_size(
        text,
        || control.check_active(),
        |reason, _| invalid(property, DataSizeFailureReason::Parse(reason)),
    )?;
    if let Some(reason) = data_size_range_failure(property, bytes) {
        return Err(invalid(property, DataSizeFailureReason::Range(reason)));
    }
    control.check_active()?;
    Ok(Some(NormalizedDataSize { property, bytes }))
}

/// Width bounds after parsing the declared data size into nonnegative bytes.
fn data_size_range_failure(property: DataSizeProperty, bytes: i64) -> Option<DataSizeRangeFailure> {
    if property != DataSizeProperty::ParquetWriterRowGroupSize {
        return None;
    }
    if bytes == 0 {
        Some(DataSizeRangeFailure::ZeroParquetRowGroup)
    } else if bytes > 2 * (1_i64 << 30) {
        Some(DataSizeRangeFailure::ParquetRowGroupAboveTwoGiB)
    } else if usize::try_from(bytes).is_err() {
        Some(DataSizeRangeFailure::ParquetRowGroupOutsideUsize)
    } else {
        None
    }
}
