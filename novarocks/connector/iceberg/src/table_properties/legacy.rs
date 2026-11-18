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

//! Temporary preparation of actual rendered legacy CREATE property slots.
//! Source ranges stay with the caller; final semantic commands have no origin.

use super::ExtraPropertyWrite;
use crate::catalog_control::catalog_mutation::{
    parse_create_format_version, prepare_create_raw_table_properties,
};
use crate::iceberg::spec::{FormatVersion, TableProperties};
use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorPropertyChange,
    DdlPropertyChange, DdlPropertyValue,
};
use std::{collections::HashSet, error::Error, fmt, sync::Arc};

// Preserve the provider's public path while SPI owns the neutral preparation facts.
// Remove these legacy DTOs and the preparation entry at P12.
pub use novarocks_spi::connector::{
    LegacyPreparedTableProperty, LegacyPreparedTablePropertyValue,
    LegacyTablePropertyPreparationFailure,
};

#[derive(Debug)]
enum Unrepresentable {
    RetryCountOutsideInteger,
    RowGroupOutsideDeclaredSize,
    TargetSizeNotParsed,
    TargetSizeNegative,
}
impl fmt::Display for Unrepresentable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RetryCountOutsideInteger => "Legacy commit.retry.num-retries exceeds the declared INTEGER domain",
            Self::RowGroupOutsideDeclaredSize => "Legacy write.parquet.row-group-size-bytes exceeds the declared DataSize domain",
            Self::TargetSizeNotParsed => "Legacy write.target-file-size-bytes has no parsed i64 byte count",
            Self::TargetSizeNegative => "Legacy write.target-file-size-bytes is negative and cannot be represented as DataSize",
        })
    }
}
impl Error for Unrepresentable {}
impl FailureProjection for Unrepresentable {
    fn failure_fact(&self) -> FailureFact {
        ConnectorErrorKind::Unsupported.failure_fact()
    }
}
fn unsupported(cause: Unrepresentable) -> ConnectorError {
    ConnectorError::from_domain(ConnectorErrorKind::Unsupported, cause)
}

/// The original scalar leaves below can only produce their own DataInvalid
/// parsing errors. Keep that exact Iceberg error as the typed original source.
#[derive(Debug)]
struct ScalarParseFailure(crate::iceberg::Error);
impl fmt::Display for ScalarParseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
impl Error for ScalarParseFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for ScalarParseFailure {
    fn failure_fact(&self) -> FailureFact {
        ConnectorErrorKind::InvalidRequest.failure_fact()
    }
}
fn scalar(error: crate::iceberg::Error) -> ConnectorError {
    ConnectorError::from_domain(
        ConnectorErrorKind::InvalidRequest,
        ScalarParseFailure(error),
    )
}
fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn check(
    control: &dyn ConnectorOperationControl,
) -> Result<(), LegacyTablePropertyPreparationFailure> {
    control
        .check_active()
        .map_err(LegacyTablePropertyPreparationFailure::whole)
}
fn copy_text(
    value: &str,
    control: &dyn ConnectorOperationControl,
) -> Result<String, LegacyTablePropertyPreparationFailure> {
    for _ in value.as_bytes().chunks(256) {
        check(control)?;
    }
    let value = value.to_owned();
    check(control)?;
    Ok(value)
}

fn legacy_declared_raw_name(key: &str) -> Option<&'static str> {
    match key {
        TableProperties::PROPERTY_DEFAULT_FILE_FORMAT => Some("format"),
        TableProperties::PROPERTY_COMMIT_NUM_RETRIES => Some("max_commit_retry"),
        crate::commit::data_writer::PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY => {
            Some("parquet_writer_row_group_size")
        }
        "write.target-file-size-bytes" => Some("target_max_file_size"),
        TableProperties::PROPERTY_GC_ENABLED => Some("gc_enabled"),
        "write.data.path" => Some("data_location"),
        crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY => Some("statistics_collect_on_write"),
        "novarocks.maintenance.enabled" => Some("maintenance_enabled"),
        _ => None,
    }
}

fn decode_legacy_declared_value(
    name: &str,
    value: &str,
    input_index: usize,
    control: &dyn ConnectorOperationControl,
) -> Result<DdlPropertyValue, LegacyTablePropertyPreparationFailure> {
    let at = |error: ConnectorError| LegacyTablePropertyPreparationFailure::at(input_index, error);
    Ok(match name {
        "format_version" => {
            let version = parse_create_format_version(value).map_err(at)?;
            DdlPropertyValue::Integer(match version {
                FormatVersion::V1 => 1,
                FormatVersion::V2 => 2,
                FormatVersion::V3 => 3,
            })
        }
        "format" => {
            super::projection::normalize_declared_format(value).map_err(at)?;
            DdlPropertyValue::Varchar(copy_text(value, control)?)
        }
        "max_commit_retry" => {
            let number = crate::commit::attempt::parse_retry_count_property_value(value)
                .map_err(|error| at(scalar(error)))?;
            let number = i32::try_from(number)
                .map_err(|_| at(unsupported(Unrepresentable::RetryCountOutsideInteger)))?;
            DdlPropertyValue::Integer(number)
        }
        "parquet_writer_row_group_size" => {
            let number = crate::commit::data_writer::parse_parquet_row_group_size_property(value)
                .map_err(|error| {
                at(ConnectorError::from_domain(
                    ConnectorErrorKind::InvalidRequest,
                    error,
                ))
            })?;
            let number = i64::try_from(number)
                .ok()
                .filter(|number| {
                    super::data_size::data_size_range_failure(
                        super::DataSizeProperty::ParquetWriterRowGroupSize,
                        *number,
                    )
                    .is_none()
                })
                .ok_or_else(|| at(unsupported(Unrepresentable::RowGroupOutsideDeclaredSize)))?;
            DdlPropertyValue::Varchar(format!("{number}B"))
        }
        "target_max_file_size" => {
            let number = crate::storage_inspector::target_file_size_property_value(value)
                .ok_or_else(|| at(unsupported(Unrepresentable::TargetSizeNotParsed)))?;
            if number < 0 {
                return Err(at(unsupported(Unrepresentable::TargetSizeNegative)));
            }
            DdlPropertyValue::Varchar(format!("{number}B"))
        }
        "gc_enabled" => DdlPropertyValue::Boolean(
            crate::commit::snapshot_lifecycle_helpers::retention::parse_gc_enabled_property_value(
                value,
            )
            .map_err(|error| at(scalar(error)))?,
        ),
        "statistics_collect_on_write" => DdlPropertyValue::Boolean(
            crate::commit::write_stack::control::collect_on_write_property_enabled(value),
        ),
        "maintenance_enabled" => DdlPropertyValue::Boolean(
            crate::storage_inspector::maintenance_property_enabled(value),
        ),
        "data_location" => DdlPropertyValue::Varchar(copy_text(value, control)?),
        _ => unreachable!("closed declared-name classification above"),
    })
}

/// Each success belongs to the same real input ordinal. Extra grouping happens
/// only after this complete ordered preparation has succeeded.
pub fn prepare_legacy_create_table_property_inputs(
    input: &[(Arc<str>, Arc<str>)],
    control: &dyn ConnectorOperationControl,
) -> Result<Vec<LegacyPreparedTableProperty>, LegacyTablePropertyPreparationFailure> {
    check(control)?;
    // Complete the shared original raw phase before any new typed decoding.
    // Its actual result is not used to invent defaults or final input values.
    let _ = prepare_create_raw_table_properties(input).map_err(|failure| {
        LegacyTablePropertyPreparationFailure {
            input_index: failure.input_index,
            error: failure.error,
        }
    })?;
    check(control)?;
    let mut declared_names = HashSet::new();
    let mut extra_names = HashSet::new();
    let mut prepared = Vec::with_capacity(input.len());
    for (input_index, (key, value)) in input.iter().enumerate() {
        check(control)?;
        let version = key.eq_ignore_ascii_case("format-version")
            || key.eq_ignore_ascii_case("format_version");
        let declared = if version {
            Some("format_version")
        } else {
            legacy_declared_raw_name(key)
        };
        let value = if let Some(name) = declared {
            if !declared_names.insert(name) {
                return Err(LegacyTablePropertyPreparationFailure::at(
                    input_index,
                    invalid(format!("duplicate declared Iceberg table property: {name}")),
                ));
            }
            let parsed = decode_legacy_declared_value(name, value, input_index, control)?;
            LegacyPreparedTablePropertyValue::Declared {
                name,
                value: parsed,
            }
        } else {
            let canonical = super::normalized_extra_property_key(key);
            if extra_names.contains(&canonical) {
                return Err(LegacyTablePropertyPreparationFailure::at(
                    input_index,
                    super::duplicate_extra_property(&canonical),
                ));
            }
            super::reject_variant_shredding_property(&canonical)
                .map_err(|error| LegacyTablePropertyPreparationFailure::at(input_index, error))?;
            super::reject_extra_property_key(&canonical, ExtraPropertyWrite::Create)
                .map_err(|error| LegacyTablePropertyPreparationFailure::at(input_index, error))?;
            extra_names.insert(canonical);
            LegacyPreparedTablePropertyValue::Extra {
                key: copy_text(key, control)?,
                value: copy_text(value, control)?,
            }
        };
        prepared.push(LegacyPreparedTableProperty { input_index, value });
    }
    check(control)?;
    Ok(prepared)
}

#[derive(Debug, PartialEq)]
pub(crate) enum LegacyAlterSetPreparation {
    Typed(Vec<(Arc<str>, DdlPropertyChange)>),
    KeepRaw { input_index: usize },
}

/// Only for a legacy user SET batch after the original property mutation
/// author accepts the raw changes against the loaded table. The caller keeps
/// that already prepared raw change when these inputs cannot round-trip.
pub(crate) fn prepare_legacy_alter_set_table_property_inputs(
    changes: &[ConnectorPropertyChange],
    control: &dyn ConnectorOperationControl,
) -> Result<LegacyAlterSetPreparation, ConnectorError> {
    control.check_active()?;
    let mut properties = Vec::with_capacity(changes.len());
    let mut extra = Vec::new();
    let mut extra_position = None;
    let keep_raw = |input_index| {
        control.check_active()?;
        Ok(LegacyAlterSetPreparation::KeepRaw { input_index })
    };
    for (input_index, change) in changes.iter().enumerate() {
        control.check_active()?;
        let ConnectorPropertyChange::Set { key, value } = change else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Legacy SET property preparation requires an already checked SET batch",
            ));
        };
        let (name, typed) = if let Some(name) = legacy_declared_raw_name(key) {
            match decode_legacy_declared_value(name, value, input_index, control) {
                Ok(value) => (name, value),
                // An indexed refusal comes from a known legacy reader or its
                // representable range. The original ALTER writer accepted bytes.
                Err(failure) if failure.input_index.is_some() => return keep_raw(input_index),
                Err(failure) => return Err(failure.error),
            }
        } else {
            let key_copy = copy_text(key, control).map_err(|failure| failure.error)?;
            // Alternate format-version spellings were never an ALTER upgrade.
            // They also cannot pass the declared-key protection for extras.
            if super::declared_raw_property(key)
                || super::normalized_extra_property_key(&key_copy) != key_copy
            {
                return keep_raw(input_index);
            }
            let value_copy = copy_text(value, control).map_err(|failure| failure.error)?;
            (
                "extra_properties",
                DdlPropertyValue::MapVarcharVarchar(vec![(key_copy, Some(value_copy))]),
            )
        };
        let projected = super::projection::project_raw_table_property_set(
            name,
            &typed,
            ExtraPropertyWrite::Alter,
            control,
        )?;
        if !matches!(
            projected.as_slice(),
            [ConnectorPropertyChange::Set { key: actual_key, value: actual_value }]
                if actual_key == key && actual_value == value
        ) {
            return keep_raw(input_index);
        }
        if let DdlPropertyValue::MapVarcharVarchar(entries) = typed {
            extra_position.get_or_insert(properties.len());
            extra.extend(entries);
        } else {
            properties.push((Arc::from(name), DdlPropertyChange::Set(typed)));
        }
    }
    if let Some(position) = extra_position {
        properties.insert(
            position,
            (
                Arc::from("extra_properties"),
                DdlPropertyChange::Set(DdlPropertyValue::MapVarcharVarchar(extra)),
            ),
        );
    }
    control.check_active()?;
    Ok(LegacyAlterSetPreparation::Typed(properties))
}

/// Temporary inverse of actual old CREATE transform inputs. Remove with the
/// legacy entry; this is not a general SHOW CREATE or external-schema renderer.
pub fn prepare_legacy_create_table_partitioning_property(
    fields: &[novarocks_spi::connector::ConnectorPartitionTransform],
    control: &dyn ConnectorOperationControl,
) -> Result<DdlPropertyValue, ConnectorError> {
    use novarocks_spi::connector::ConnectorPartitionTransform as T;
    control.check_active()?;
    let mut values = Vec::with_capacity(fields.len());
    for field in fields {
        control.check_active()?;
        let source = novarocks_types::naming::normalize_identifier(
            crate::catalog_control::catalog_mutation::partition_source(field),
        )
        .map_err(|detail| ConnectorError::new(ConnectorErrorKind::InvalidRequest, detail))?;
        let text = match field {
            T::Identity { .. } => source,
            T::Year { .. } => format!("year({source})"),
            T::Month { .. } => format!("month({source})"),
            T::Day { .. } => format!("day({source})"),
            T::Hour { .. } => format!("hour({source})"),
            T::Void { .. } => format!("void({source})"),
            T::Bucket { num_buckets, .. }
            | T::Truncate {
                width: num_buckets, ..
            } => {
                if *num_buckets == 0 || i32::try_from(*num_buckets).is_err() {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "legacy partition width cannot be represented as a positive INTEGER",
                    ));
                }
                if matches!(field, T::Bucket { .. }) {
                    format!("bucket({source}, {num_buckets})")
                } else {
                    format!("truncate({source}, {num_buckets})")
                }
            }
        };
        values.push(Some(text));
    }
    control.check_active()?;
    Ok(DdlPropertyValue::ArrayVarchar(values))
}
