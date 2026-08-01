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
use std::sync::Arc;

use arrow::datatypes::DataType;

use super::super::context::NativePlanDecodeContext;
use super::super::error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};
use super::super::layout::Layout;
use crate::native::type_decode::{decode_field_type, decode_type};
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks::protocol::{FieldPath, ProtocolErrorKind};
use novarocks_protocol::{common, plan};

#[derive(Clone, Debug)]
pub(super) struct ProvenancedOutputColumn {
    column: common::OutputColumn,
    source_path: FieldPath,
    name_path: FieldPath,
    type_path: Option<FieldPath>,
    slot_schema: ChunkSlotSchema,
}

impl ProvenancedOutputColumn {
    pub(super) fn decode(
        column: common::OutputColumn,
        source_path: FieldPath,
        name_path: FieldPath,
        type_path: FieldPath,
    ) -> Result<Self, NativeFragmentDecodeError> {
        let type_desc = column.r#type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                type_path.clone(),
                format!("ScanNode column {} type missing", column.name),
            )
        })?;
        let field = decode_field_type(&column.name, column.nullable, type_desc)
            .map_err(|error| NativeFragmentDecodeError::invalid_value(type_path.clone(), error))?;
        let slot_schema = ChunkSlotSchema::from_field(SlotId::new(column.column_id), &field, None)
            .map_err(|error| NativeFragmentDecodeError::invalid_value(type_path.clone(), error))?;
        Ok(Self {
            column,
            source_path,
            name_path,
            type_path: Some(type_path),
            slot_schema,
        })
    }

    pub(super) fn trusted_internal(
        column: common::OutputColumn,
        source_path: FieldPath,
        name_path: FieldPath,
    ) -> Self {
        let type_desc = column
            .r#type
            .as_ref()
            .expect("trusted internal scan column type");
        let field = decode_field_type(&column.name, column.nullable, type_desc)
            .expect("trusted internal scan column field");
        let slot_schema = ChunkSlotSchema::from_field(SlotId::new(column.column_id), &field, None)
            .expect("trusted internal scan column schema");
        Self {
            column,
            source_path,
            name_path,
            type_path: None,
            slot_schema,
        }
    }

    pub(super) fn column(&self) -> &common::OutputColumn {
        &self.column
    }

    pub(super) fn source_path(&self) -> FieldPath {
        self.source_path.clone()
    }

    pub(super) fn type_path(&self) -> Option<FieldPath> {
        self.type_path.clone()
    }

    pub(super) fn name_path(&self) -> FieldPath {
        self.name_path.clone()
    }

    pub(super) fn slot_schema(&self) -> &ChunkSlotSchema {
        &self.slot_schema
    }
}

#[derive(Clone, Debug)]
pub(super) struct DecodedScanOutputColumns {
    columns: Vec<common::OutputColumn>,
    provenanced: Vec<ProvenancedOutputColumn>,
    layout: Layout,
    output_schema: ChunkSchemaRef,
}

impl DecodedScanOutputColumns {
    pub(super) fn columns(&self) -> &[common::OutputColumn] {
        &self.columns
    }

    pub(super) fn source_path(&self, selected_index: usize) -> FieldPath {
        self.provenanced[selected_index].source_path()
    }

    pub(super) fn provenanced(&self) -> &[ProvenancedOutputColumn] {
        &self.provenanced
    }

    pub(super) fn layout(&self) -> Layout {
        self.layout.clone()
    }

    pub(super) fn output_schema(&self) -> ChunkSchemaRef {
        Arc::clone(&self.output_schema)
    }
}

pub(super) fn decode_scan_output_columns(
    scan: &plan::ScanNode,
    scan_path: FieldPath,
) -> Result<DecodedScanOutputColumns, NativeFragmentDecodeError> {
    if scan.columns.is_empty() {
        return Err(NativeFragmentDecodeError::missing(
            scan_path.field("columns"),
            "ScanNode columns are empty",
        ));
    }
    let required = (!scan.required_columns.is_empty()).then(|| {
        scan.required_columns
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<HashSet<_>>()
    });
    let selected = scan
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            required
                .as_ref()
                .is_none_or(|required| required.contains(&column.name.to_ascii_lowercase()))
        })
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Err(NativeFragmentDecodeError::invalid_value(
            scan_path.field("required_columns"),
            format!(
                "ScanNode required_columns {:?} do not match any scan columns",
                scan.required_columns
            ),
        ));
    }
    let columns_path = scan_path.field("columns");
    let mut columns = Vec::with_capacity(selected.len());
    let mut provenanced = Vec::with_capacity(selected.len());
    let mut seen = HashMap::with_capacity(selected.len());
    for (wire_index, column) in selected {
        let source_path = columns_path.clone().index(wire_index);
        let slot_id = SlotId::new(column.column_id);
        if let Some(first_wire_index) = seen.insert(slot_id, wire_index) {
            return Err(NativeFragmentDecodeError::inconsistent(
                source_path.field("column_id"),
                format!(
                    "duplicate ScanNode column_id {} at wire index {} (first seen at wire index {})",
                    column.column_id, wire_index, first_wire_index
                ),
            ));
        }
        let decoded = ProvenancedOutputColumn::decode(
            column.clone(),
            source_path.clone(),
            source_path.clone().field("name"),
            source_path.field("type"),
        )?;
        columns.push(column.clone());
        provenanced.push(decoded);
    }
    let slot_schemas = provenanced
        .iter()
        .map(|column| column.slot_schema().clone())
        .collect::<Vec<_>>();
    let layout = Layout::for_slots(slot_schemas.iter().map(ChunkSlotSchema::slot_id));
    let output_schema = ChunkSchema::try_new(slot_schemas)
        .map(Arc::new)
        .map_err(|error| NativeFragmentDecodeError::inconsistent(columns_path.clone(), error))?;
    Ok(DecodedScanOutputColumns {
        columns,
        provenanced,
        layout,
        output_schema,
    })
}

pub(super) fn column_def_data_type(
    column: &plan::ColumnDef,
) -> Result<DataType, NativeFragmentLeafDecodeError> {
    let desc = column
        .logical_type
        .as_ref()
        .or(column.data_type.as_ref())
        .ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "data_type",
                format!("column {} type missing", column.name),
            )
        })?;
    decode_type(desc).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "data_type", error)
    })
}

pub(super) fn output_column_data_type(
    column: &common::OutputColumn,
) -> Result<DataType, NativeFragmentLeafDecodeError> {
    let desc = column.r#type.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "type",
            format!("output column {} type missing", column.name),
        )
    })?;
    decode_type(desc).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "type", error)
    })
}

pub(super) fn scan_batch_size(
    query_options: Option<&novarocks::runtime::query_options::QueryOptions>,
) -> Result<usize, NativeFragmentLeafDecodeError> {
    let Some(value) = query_options.and_then(|opts| opts.batch_size()) else {
        return Ok(4096);
    };
    let batch_size = usize::try_from(value).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "batch_size",
            format!("native ScanNode query_options.batch_size must be positive, got {value}"),
        )
    })?;
    if batch_size == 0 {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "batch_size",
            "native ScanNode query_options.batch_size must be positive",
        ));
    }
    Ok(batch_size)
}

pub(super) fn lower_scan_predicate(
    scan: &plan::ScanNode,
    arena: &mut ExprArena,
    layout: &Layout,
    ctx: &NativePlanDecodeContext,
) -> Result<Option<ExprId>, NativeFragmentLeafDecodeError> {
    let mut predicate = None;
    for (idx, expr) in scan.predicates.iter().enumerate() {
        let expr_id = ctx
            .decode_expression(
                expr,
                FieldPath::root("scan").field("predicates").index(idx),
                arena,
                layout,
            )
            .map_err(|err| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "predicates",
                    format!("ScanNode predicate {idx}: {err}"),
                )
                .append_index(idx)
            })?;
        predicate = Some(match predicate {
            Some(prev) => arena.push_typed(ExprNode::And(prev, expr_id), DataType::Boolean),
            None => expr_id,
        });
    }
    Ok(predicate)
}

pub(super) fn parse_scan_limit(limit: i64) -> Result<Option<usize>, NativeFragmentLeafDecodeError> {
    if limit == -1 {
        Ok(None)
    } else if limit < 0 {
        Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "limit",
            format!("ScanNode limit must be -1 or >= 0, got {limit}"),
        ))
    } else {
        Ok(Some(limit as usize))
    }
}

/// Loads the object-store configuration that was installed on this BE at
/// startup. Native fragment payloads must not carry credentials or endpoint
/// configuration because every BE receives the same deployment configuration.
pub(super) fn reject_native_connector_cloud_properties(
    cloud_properties: &HashMap<String, String>,
) -> Result<(), NativeFragmentLeafDecodeError> {
    if cloud_properties.is_empty() {
        Ok(())
    } else {
        Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "cloud_properties",
            "native connector scans must use the BE startup connector configuration; cloud_properties are not accepted",
        ))
    }
}

pub(super) fn table_location_map(table: &plan::IcebergTableInfo) -> HashMap<i64, String> {
    let mut locations = HashMap::new();
    if !table.location.is_empty() {
        locations.insert(i64::from(table.schema_id), table.location.clone());
    }
    locations
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::reject_native_connector_cloud_properties;

    #[test]
    fn native_connector_scan_rejects_plan_object_store_properties() {
        let error = reject_native_connector_cloud_properties(&HashMap::from([(
            "aws.s3.access_key".to_string(),
            "not-for-the-plan".to_string(),
        )]))
        .expect_err("native connector scan must reject plan-side configuration");

        assert!(error.contains("startup connector configuration"));
    }
}
