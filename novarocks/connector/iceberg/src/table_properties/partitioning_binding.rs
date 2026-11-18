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

//! Bind the existing simple-name CREATE partition profile to its real schema.
//! Property output-name admission does not author stored partition names/IDs.

use super::partitioning::parse_optional_partitioning_element;
use crate::catalog_control::catalog_mutation::{
    initial_partition_field, partition_source, validate_partition_transform,
};
use crate::iceberg::spec::{PartitionSpec, Schema, SchemaRef, UnboundPartitionSpec};
use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorPartitionTransform,
};
use std::{collections::HashSet, error::Error, fmt};

pub(crate) fn bind_create_table_partitioning(
    schema: SchemaRef,
    fields: &[Option<String>],
    control: &dyn ConnectorOperationControl,
) -> Result<PartitionSpec, ConnectorError> {
    bind_create_table_partitioning_inner(schema, fields, None, control)
        .map_err(novarocks_spi::connector::InvalidTableProperty::from_property_error)
}

pub(crate) fn bind_mv_create_table_partitioning(
    schema: SchemaRef,
    fields: &[Option<String>],
    visible_sources: &HashSet<String>,
    control: &dyn ConnectorOperationControl,
) -> Result<PartitionSpec, ConnectorError> {
    bind_create_table_partitioning_inner(schema, fields, Some(visible_sources), control)
        .map_err(novarocks_spi::connector::InvalidTableProperty::from_property_error)
}

fn bind_create_table_partitioning_inner(
    schema: SchemaRef,
    fields: &[Option<String>],
    visible_sources: Option<&HashSet<String>>,
    control: &dyn ConnectorOperationControl,
) -> Result<PartitionSpec, ConnectorError> {
    control.check_active()?;
    let mut builder = UnboundPartitionSpec::builder().with_spec_id(0);
    let mut resolved = Vec::with_capacity(fields.len());
    let mut domain = PropertyOutputNames::new(&schema, fields.len())?;
    for (index, field) in fields.iter().enumerate() {
        control.check_active()?;
        let (source_id, field) =
            resolve_property_partition_source(&schema, field.as_deref(), index, "CREATE", control)?;
        let source = partition_source(&field);
        if visible_sources.is_some_and(|visible| !visible.contains(source)) {
            return Err(invalid(format!(
                "materialized view partition source column `{source}` must be an output column"
            )));
        }
        domain.admit(&schema, source_id, &field, false, control)?;
        let unbound = initial_partition_field(&field, source_id, index).map_err(invalid)?;
        builder = builder
            .add_partition_fields([unbound])
            .map_err(native_spec_error)?;
        resolved.push((source_id, field));
    }
    // Java completes field syntax/lookup/name admission before final type
    // compatibility. Reuse the original native validator in that final order.
    for (source_id, field) in &resolved {
        control.check_active()?;
        validate_partition_transform(&schema, *source_id, field)
            .map_err(|failure| failure.into_connector_error())?;
    }
    control.check_active()?;
    let spec = builder.build().bind(schema).map_err(native_spec_error)?;
    control.check_active()?;
    Ok(spec)
}

pub(super) fn resolve_property_partition_source(
    schema: &Schema,
    field: Option<&str>,
    index: usize,
    operation: &'static str,
    control: &dyn ConnectorOperationControl,
) -> Result<(i32, ConnectorPartitionTransform), ConnectorError> {
    let field = parse_optional_partitioning_element(field, index, control)?;
    let name = partition_source(&field);
    let source = schema
        .field_by_name(name)
        .ok_or_else(|| invalid(format!("partition source column `{name}` does not exist")))?;
    let source_id = source.id;
    if !schema
        .as_struct()
        .fields()
        .iter()
        .any(|field| field.id == source_id)
        || !novarocks_types::naming::normalize_identifier(name)
            .is_ok_and(|normalized| normalized == name)
    {
        return Err(unsupported(&format!(
            "nested or non-simple partition source is not supported by the existing {operation} profile"
        )));
    }
    Ok((source_id, field))
}

/// The property grammar's output-name checks do not assign physical IDs. Source
/// resolution stays with Schema; stored names/IDs stay with the native author.
pub(super) struct PropertyOutputNames {
    names: HashSet<String>,
    dedup: HashSet<(i32, &'static str)>,
    attempts_end: usize,
}
impl PropertyOutputNames {
    pub(super) fn new(schema: &Schema, field_count: usize) -> Result<Self, ConnectorError> {
        let attempts_end = schema
            .as_struct()
            .fields()
            .len()
            .checked_add(field_count)
            .ok_or_else(|| overflow("partition output-name search size overflow"))?;
        Ok(Self {
            names: HashSet::new(),
            dedup: HashSet::new(),
            attempts_end,
        })
    }
    pub(super) fn admit(
        &mut self,
        schema: &Schema,
        source_id: i32,
        field: &ConnectorPartitionTransform,
        include_width: bool,
        control: &dyn ConnectorOperationControl,
    ) -> Result<String, ConnectorError> {
        use ConnectorPartitionTransform as T;
        let source = partition_source(field);
        let base = match field {
            T::Identity { .. } => schema
                .name_by_field_id(source_id)
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "resolved partition source has no canonical name",
                    )
                })?
                .to_owned(),
            T::Year { .. } => format!("{source}_year"),
            T::Month { .. } => format!("{source}_month"),
            T::Day { .. } => format!("{source}_day"),
            T::Hour { .. } => format!("{source}_hour"),
            T::Bucket { num_buckets, .. } if include_width => {
                format!("{source}_bucket_{num_buckets}")
            }
            T::Truncate { width, .. } if include_width => format!("{source}_trunc_{width}"),
            T::Bucket { .. } => format!("{source}_bucket"),
            T::Truncate { .. } => format!("{source}_trunc"),
            T::Void { .. } => format!("{source}_null"),
        };
        let mut attempt = 1_usize;
        while attempt < self.attempts_end {
            control.check_active()?;
            let candidate = if attempt == 1 || matches!(field, T::Identity { .. }) {
                base.clone()
            } else {
                format!("{base}_{attempt}")
            };
            let schema_conflict = schema.field_by_name(&candidate).is_some_and(|actual| {
                !matches!(field, T::Identity { .. } | T::Void { .. }) || actual.id != source_id
            });
            if !schema_conflict {
                if !self.names.insert(candidate.clone()) {
                    return Err(invalid(format!(
                        "partition output name is repeated: {candidate}"
                    )));
                }
                let dedup = match field {
                    T::Identity { .. } => Some("identity"),
                    T::Year { .. } | T::Month { .. } | T::Day { .. } | T::Hour { .. } => {
                        Some("time")
                    }
                    _ => None,
                };
                if let Some(kind) = dedup {
                    if !self.dedup.insert((source_id, kind)) {
                        return Err(invalid(format!(
                            "redundant partition transform for source ID {source_id}: {kind}"
                        )));
                    }
                }
                return Ok(candidate);
            }
            attempt = attempt
                .checked_add(1)
                .ok_or_else(|| overflow("partition output-name suffix overflow"))?;
        }
        Err(invalid(format!(
            "cannot resolve partition output name for source `{source}`"
        )))
    }
}

#[derive(Debug)]
struct NativeSpecFailure {
    kind: ConnectorErrorKind,
    cause: crate::iceberg::Error,
}
impl fmt::Display for NativeSpecFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.cause, f)
    }
}
impl Error for NativeSpecFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}
impl FailureProjection for NativeSpecFailure {
    fn failure_fact(&self) -> FailureFact {
        self.kind.failure_fact()
    }
}
fn native_spec_error(cause: crate::iceberg::Error) -> ConnectorError {
    let kind = match cause.kind() {
        crate::iceberg::ErrorKind::DataInvalid | crate::iceberg::ErrorKind::FeatureUnsupported => {
            ConnectorErrorKind::Unsupported
        }
        _ => ConnectorErrorKind::Internal,
    };
    ConnectorError::from_domain(kind, NativeSpecFailure { kind, cause })
}
fn invalid(detail: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, detail)
}
fn unsupported(detail: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, detail)
}
fn overflow(detail: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, detail)
}
