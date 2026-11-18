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

//! Partial projection for already evaluated, raw-backed table properties.
//! Dedicated format-version, partition-spec and location authors are separate.

use super::{DataSizeProperty, ExtraPropertyWrite};
use crate::iceberg::spec::{DataFileFormat, TableProperties};
use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorPropertyChange,
    DdlPropertyChange, DdlPropertyValue,
};
use std::{error::Error, fmt, sync::Arc};

/// Project only the finite raw-backed subset, without applying a mutation.
///
/// The caller first resolves dedicated format-version, partitioning and
/// location facts with their original authors. CREATE defaults contain such
/// facts, so this function is not a complete table-property normalizer.
/// `canonical_name` is supplied by the original scope owner. Returned changes
/// retain their order; callers must not collect them into a last-wins map.
pub fn project_partial_raw_table_property(
    canonical_name: &str,
    change: &DdlPropertyChange,
    operation: ExtraPropertyWrite,
    control: &dyn ConnectorOperationControl,
) -> Result<Vec<ConnectorPropertyChange>, ConnectorError> {
    let change = match change {
        DdlPropertyChange::Set(value) => BorrowedChange::Set(value),
        DdlPropertyChange::Reset(value) => BorrowedChange::Reset(value),
    };
    project_raw_table_property(canonical_name, change, operation, control)
}

/// CREATE already owns the actual evaluated value; borrow its Set intention
/// without cloning the whole Map or introducing a second projection author.
pub(crate) fn project_raw_table_property_set(
    canonical_name: &str,
    value: &DdlPropertyValue,
    operation: ExtraPropertyWrite,
    control: &dyn ConnectorOperationControl,
) -> Result<Vec<ConnectorPropertyChange>, ConnectorError> {
    project_raw_table_property(
        canonical_name,
        BorrowedChange::Set(value),
        operation,
        control,
    )
}

enum BorrowedChange<'a> {
    Set(&'a DdlPropertyValue),
    Reset(&'a DdlPropertyValue),
}

fn project_raw_table_property(
    canonical_name: &str,
    change: BorrowedChange<'_>,
    operation: ExtraPropertyWrite,
    control: &dyn ConnectorOperationControl,
) -> Result<Vec<ConnectorPropertyChange>, ConnectorError> {
    project_raw_table_property_inner(canonical_name, change, operation, control)
        .map_err(novarocks_spi::connector::InvalidTableProperty::from_property_error)
}

fn project_raw_table_property_inner(
    canonical_name: &str,
    change: BorrowedChange<'_>,
    operation: ExtraPropertyWrite,
    control: &dyn ConnectorOperationControl,
) -> Result<Vec<ConnectorPropertyChange>, ConnectorError> {
    control.check_active()?;
    if super::is_recognized_unsupported_table_property(canonical_name) {
        return Err(unsupported(canonical_name));
    }
    let changes = match canonical_name {
        "format_version" | "partitioning" | "location" => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!("Iceberg {canonical_name} requires its dedicated property setter"),
            ));
        }
        "extra_properties" => match change {
            BorrowedChange::Reset(DdlPropertyValue::Null) => {
                // The reference DEFAULT(NULL) reaches the provider's empty
                // Optional rejection. This is a compatibility refusal, not a
                // corrupted request or an instruction to remove stored keys.
                return Err(ConnectorError::from_domain(
                    ConnectorErrorKind::Internal,
                    ReferenceExtraPropertiesDefaultNull,
                ));
            }
            BorrowedChange::Set(value) | BorrowedChange::Reset(value) => {
                super::extra_property_sets(value, operation)?
            }
        },
        _ => {
            let key = raw_key(canonical_name).ok_or_else(|| {
                invalid(format!("Unknown Iceberg table property '{canonical_name}'"))
            })?;
            match change {
                BorrowedChange::Reset(DdlPropertyValue::Null) => {
                    vec![ConnectorPropertyChange::Unset {
                        key: Arc::from(key),
                        if_exists: true,
                    }]
                }
                BorrowedChange::Set(DdlPropertyValue::Null) => {
                    return Err(invalid(format!("Iceberg {canonical_name} cannot be NULL")));
                }
                BorrowedChange::Set(value) | BorrowedChange::Reset(value) => {
                    let value = raw_value(canonical_name, value, control)?;
                    vec![ConnectorPropertyChange::Set {
                        key: Arc::from(key),
                        value: Arc::from(value),
                    }]
                }
            }
        }
    };
    control.check_active()?;
    Ok(changes)
}

fn raw_key(name: &str) -> Option<&'static str> {
    Some(match name {
        "format" => TableProperties::PROPERTY_DEFAULT_FILE_FORMAT,
        "max_commit_retry" => TableProperties::PROPERTY_COMMIT_NUM_RETRIES,
        "gc_enabled" => TableProperties::PROPERTY_GC_ENABLED,
        "data_location" => TableProperties::PROPERTY_WRITE_DATA_LOCATION,
        "statistics_collect_on_write" => crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY,
        "maintenance_enabled" => "novarocks.maintenance.enabled",
        "parquet_writer_row_group_size" => DataSizeProperty::ParquetWriterRowGroupSize.raw_key(),
        "target_max_file_size" => DataSizeProperty::TargetMaxFileSize.raw_key(),
        _ => return None,
    })
}

pub(super) fn normalize_declared_format(format: &str) -> Result<String, ConnectorError> {
    if !format.eq_ignore_ascii_case("PARQUET") {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!("Iceberg format '{format}' is not supported"),
        ));
    }
    Ok(DataFileFormat::Parquet.to_string())
}

fn raw_value(
    name: &str,
    value: &DdlPropertyValue,
    control: &dyn ConnectorOperationControl,
) -> Result<String, ConnectorError> {
    match (name, value) {
        ("format", DdlPropertyValue::Varchar(format)) => normalize_declared_format(format),
        ("max_commit_retry", DdlPropertyValue::Integer(value)) if *value >= 0 => {
            Ok(value.to_string())
        }
        ("max_commit_retry", DdlPropertyValue::Integer(_)) => {
            Err(invalid("Iceberg max_commit_retry must be nonnegative"))
        }
        (
            "gc_enabled" | "statistics_collect_on_write" | "maintenance_enabled",
            DdlPropertyValue::Boolean(value),
        ) => Ok(value.to_string()),
        ("data_location", DdlPropertyValue::Varchar(value)) => Ok(value.clone()),
        ("parquet_writer_row_group_size" | "target_max_file_size", _) => {
            let property = if name == "parquet_writer_row_group_size" {
                DataSizeProperty::ParquetWriterRowGroupSize
            } else {
                DataSizeProperty::TargetMaxFileSize
            };
            let normalized = super::normalize_data_size_property(property, value, control)?
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "non-NULL DataSize property lost its actual value",
                    )
                })?;
            Ok(normalized.raw_value())
        }
        _ => Err(invalid(format!("Invalid value type for Iceberg {name}"))),
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn unsupported(name: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        format!("Iceberg table property '{name}' is not supported"),
    )
}

#[derive(Debug)]
struct ReferenceExtraPropertiesDefaultNull;
impl fmt::Display for ReferenceExtraPropertiesDefaultNull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("The extra_properties property cannot be empty")
    }
}
impl Error for ReferenceExtraPropertiesDefaultNull {}
impl FailureProjection for ReferenceExtraPropertiesDefaultNull {
    fn failure_fact(&self) -> FailureFact {
        ConnectorErrorKind::Internal.failure_fact()
    }
}
