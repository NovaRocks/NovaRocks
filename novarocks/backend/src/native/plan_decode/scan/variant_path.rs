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

use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, Field};

use super::super::error::NativeFragmentLeafDecodeError;
use super::common::{column_def_data_type, output_column_data_type};
use novarocks::common::ids::SlotId;
use novarocks::formats::parquet::VariantPathSpec;
use novarocks::protocol::ProtocolErrorKind;
use novarocks_protocol::{common, plan};

#[derive(Clone, Debug, Default)]
pub(super) struct NativeVariantPathPlan {
    pub(super) specs: Vec<VariantPathSpec>,
    pub(super) output_slot_ids: HashSet<SlotId>,
}

pub(super) fn parse_native_scan_variant_path_columns(
    scan: &plan::ScanNode,
    table: &plan::TableDef,
    output_columns: &[common::OutputColumn],
) -> Result<NativeVariantPathPlan, NativeFragmentLeafDecodeError> {
    if scan.variant_columns.is_empty() {
        return Ok(NativeVariantPathPlan::default());
    }
    let output_by_slot = output_columns
        .iter()
        .map(|col| (SlotId::new(col.column_id), col))
        .collect::<HashMap<_, _>>();
    let scan_by_slot = scan
        .columns
        .iter()
        .map(|col| (SlotId::new(col.column_id), col))
        .collect::<HashMap<_, _>>();
    let mut plan = NativeVariantPathPlan::default();

    for (idx, column) in scan.variant_columns.iter().enumerate() {
        let source_slot_id = SlotId::new(column.source_column_id);
        let output_slot_id = SlotId::new(column.synthetic_column_id);
        if source_slot_id == output_slot_id {
            return Err(variant_error(
                idx,
                "source_column_id",
                ProtocolErrorKind::InconsistentFields,
                "source_column_id must differ from synthetic_column_id",
            ));
        }

        let source_name =
            required_native_variant_path_string(idx, "source_column", &column.source_column)?;
        let output_name =
            required_native_variant_path_string(idx, "synthetic_column", &column.synthetic_column)?;
        let canonical_path =
            required_native_variant_path_string(idx, "canonical_path", &column.canonical_path)?;
        validate_native_variant_path_column_path(idx, &canonical_path)?;

        let source_scan_column = scan_by_slot.get(&source_slot_id).ok_or_else(|| {
            variant_error(
                idx,
                "source_column_id",
                ProtocolErrorKind::InvalidValue,
                format!("source_column_id={source_slot_id} is not a scan column"),
            )
        })?;
        if source_scan_column.name != source_name {
            return Err(variant_error(
                idx,
                "source_column",
                ProtocolErrorKind::InconsistentFields,
                format!(
                    "source_column={source_name:?} does not match source_column_id={source_slot_id} name {:?}",
                    source_scan_column.name
                ),
            ));
        }
        let source_table_column = table
            .columns
            .iter()
            .find(|col| col.name == source_name)
            .ok_or_else(|| {
                variant_error(
                    idx,
                    "source_column",
                    ProtocolErrorKind::InvalidValue,
                    format!("source_column={source_name:?} is not in table column definitions"),
                )
            })?;
        let source_type = column_def_data_type(source_table_column).map_err(|err| {
            err.prepend_field("source_column")
                .prepend_index(idx)
                .prepend_field("variant_columns")
        })?;
        if !matches!(source_type, DataType::LargeBinary) {
            return Err(variant_error(
                idx,
                "source_column",
                ProtocolErrorKind::InvalidValue,
                format!(
                    "source_column={source_name:?} expects VARIANT/LargeBinary, got {:?}",
                    source_type
                ),
            ));
        }

        let output_column = output_by_slot.get(&output_slot_id).ok_or_else(|| {
            variant_error(
                idx,
                "synthetic_column_id",
                ProtocolErrorKind::InvalidValue,
                format!("synthetic_column_id={output_slot_id} is not an output column"),
            )
        })?;
        if output_column.name != output_name {
            return Err(variant_error(
                idx,
                "synthetic_column",
                ProtocolErrorKind::InconsistentFields,
                format!(
                    "synthetic_column={output_name:?} does not match synthetic_column_id={output_slot_id} name {:?}",
                    output_column.name
                ),
            ));
        }
        let output_type = output_column_data_type(output_column).map_err(|err| {
            err.prepend_field("synthetic_column")
                .prepend_index(idx)
                .prepend_field("variant_columns")
        })?;
        let requested_type_desc = column.requested_type.as_ref().ok_or_else(|| {
            variant_error(
                idx,
                "requested_type",
                ProtocolErrorKind::MissingField,
                "requested_type missing",
            )
        })?;
        let requested_type =
            crate::native::type_decode::decode_type(requested_type_desc).map_err(|err| {
                variant_error(idx, "requested_type", ProtocolErrorKind::InvalidValue, err)
            })?;
        if !is_supported_native_variant_path_requested_type(&requested_type) {
            return Err(variant_error(
                idx,
                "requested_type",
                ProtocolErrorKind::Unsupported,
                format!(
                    "unsupported requested_type {:?} for synthetic_column_id={output_slot_id}",
                    requested_type
                ),
            ));
        }
        if requested_type != output_type {
            return Err(variant_error(
                idx,
                "requested_type",
                ProtocolErrorKind::InconsistentFields,
                format!(
                    "requested_type {:?} does not match synthetic_column_id={output_slot_id} type {:?}",
                    requested_type, output_type
                ),
            ));
        }
        if !plan.output_slot_ids.insert(output_slot_id) {
            return Err(variant_error(
                idx,
                "synthetic_column_id",
                ProtocolErrorKind::InconsistentFields,
                format!("duplicate synthetic_column_id={output_slot_id}"),
            ));
        }

        plan.specs.push(VariantPathSpec {
            source_slot_id,
            source_read_slot_id: source_slot_id,
            output_slot_id,
            source_field_id: None,
            source_name: source_name.clone(),
            output_name: output_name.clone(),
            source_field: Field::new(source_name, source_type, source_table_column.nullable),
            output_field: Field::new(output_name, output_type, output_column.nullable),
            canonical_path,
            requested_type,
            strict: column.strict,
        });
    }

    Ok(plan)
}

fn required_native_variant_path_string(
    idx: usize,
    field_name: &'static str,
    value: &str,
) -> Result<String, NativeFragmentLeafDecodeError> {
    if value.trim().is_empty() {
        return Err(variant_error(
            idx,
            field_name,
            ProtocolErrorKind::MissingField,
            format!("{field_name} missing"),
        ));
    }
    Ok(value.trim().to_string())
}

fn validate_native_variant_path_column_path(
    idx: usize,
    canonical_path: &str,
) -> Result<(), NativeFragmentLeafDecodeError> {
    let parsed = novarocks::exec::variant::parse_variant_path(canonical_path).map_err(|err| {
        variant_error(
            idx,
            "canonical_path",
            ProtocolErrorKind::InvalidValue,
            format!("invalid canonical_path={canonical_path:?}: {err}"),
        )
    })?;
    if parsed.segments.is_empty() {
        return Err(variant_error(
            idx,
            "canonical_path",
            ProtocolErrorKind::InvalidValue,
            format!("canonical_path={canonical_path:?} must reference at least one object key"),
        ));
    }
    if parsed.segments.iter().any(|segment| {
        !matches!(
            segment,
            novarocks::exec::variant::VariantPathSegment::ObjectKey(_)
        )
    }) {
        return Err(variant_error(
            idx,
            "canonical_path",
            ProtocolErrorKind::Unsupported,
            format!("canonical_path={canonical_path:?} only supports object-key path segments"),
        ));
    }
    Ok(())
}

fn variant_error(
    idx: usize,
    field: &'static str,
    kind: ProtocolErrorKind,
    detail: impl std::fmt::Display,
) -> NativeFragmentLeafDecodeError {
    NativeFragmentLeafDecodeError::at_field(kind, "variant_columns", detail)
        .append_index(idx)
        .append_field(field)
}

fn is_supported_native_variant_path_requested_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean | DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Date32
    )
}
