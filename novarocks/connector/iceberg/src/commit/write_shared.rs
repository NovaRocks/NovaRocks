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

//! Helpers shared by the write-control preparation paths.
//!
//! `prepare_write` and `prepare_row_mutation` resolve the same ref-scoped
//! facts before they diverge into their own field-signing rules. Keeping the
//! shared resolution here stops the two paths from drifting apart.

use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorWriteFieldRequest};

use crate::iceberg::spec::{PrimitiveType, Schema, TableMetadata, Type};

/// Resolve the snapshot a write against `target_ref` will be based on.
///
/// `main` resolves to the table's current snapshot; any other ref resolves to
/// that branch's head. `Ok(None)` means the ref exists but has no snapshot yet.
pub(crate) fn write_target_snapshot_id(
    metadata: &TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, ConnectorError> {
    if target_ref == "main" {
        return Ok(metadata.current_snapshot_id());
    }
    crate::ref_snapshot::resolve_branch_head_snapshot_id(metadata, target_ref)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))
}

/// Render a resolved base snapshot for the `base_version` / preparation
/// payload strings. Kept next to the resolver so both preparation paths spell
/// a missing snapshot the same way.
pub(crate) fn snapshot_token(target_snapshot_id: Option<i64>) -> String {
    target_snapshot_id.map_or_else(|| "none".to_string(), |id| id.to_string())
}

/// Resolve each requested write column against the frozen target schema and
/// restate it with the Arrow type the Iceberg writers actually consume.
///
/// The Variant/Binary/Timestamptz overrides exist because
/// `schema_to_arrow_schema` widens those Iceberg types beyond what the data
/// writers accept; keeping the override here stops each write path from
/// re-deciding it.
pub(crate) fn exact_requested_write_fields(
    metadata: &TableMetadata,
    requested: &[ConnectorWriteFieldRequest],
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    exact_requested_write_fields_at_schema(metadata.current_schema(), requested)
}

/// Resolve write fields against an already-frozen Iceberg schema.
pub(crate) fn exact_requested_write_fields_at_schema(
    iceberg_schema: &Schema,
    requested: &[ConnectorWriteFieldRequest],
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let arrow_schema =
        crate::iceberg::arrow::schema_to_arrow_schema(iceberg_schema).map_err(|error| {
            invalid_write_activation(format!(
                "convert frozen Iceberg write schema to Arrow: {error}"
            ))
        })?;
    requested
        .iter()
        .map(|request| {
            let requested_name = request.field().name();
            let (ordinal, iceberg_field) = iceberg_schema
                .as_struct()
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.name.eq_ignore_ascii_case(requested_name))
                .ok_or_else(|| {
                    invalid_write_activation(format!(
                        "Iceberg write input column `{requested_name}` is absent from the frozen target schema"
                    ))
                })?;
            let arrow_field = arrow_schema.field(ordinal);
            let data_type = match iceberg_field.field_type.as_ref() {
                Type::Primitive(PrimitiveType::Variant) => DataType::LargeBinary,
                Type::Primitive(PrimitiveType::Binary) => DataType::Binary,
                Type::Primitive(PrimitiveType::Timestamptz) => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
                Type::Primitive(PrimitiveType::TimestamptzNs) => {
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                }
                _ => arrow_field.data_type().clone(),
            };
            Ok(ConnectorWriteFieldRequest::new(Field::new(
                &iceberg_field.name,
                data_type,
                !iceberg_field.required,
            )))
        })
        .collect()
}

/// Resolve the exact schema owned by a previously resolved write base.
///
/// A missing snapshot denotes an empty table/ref and deliberately retains the
/// admitted metadata's current schema. A concrete snapshot always owns the
/// schema, including when that snapshot is an older branch head.
pub(crate) fn write_target_schema(
    metadata: &TableMetadata,
    target_snapshot_id: Option<i64>,
) -> Result<std::sync::Arc<Schema>, ConnectorError> {
    match target_snapshot_id {
        Some(snapshot_id) => metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "resolved Iceberg write base snapshot is absent from admitted metadata",
                )
            })?
            .schema(metadata)
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("resolve admitted Iceberg write base schema: {error}"),
                )
            }),
        None => Ok(metadata.current_schema().clone()),
    }
}

pub(crate) fn invalid_write_activation(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}
