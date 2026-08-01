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

//! Lowering for `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE` (IVM-A1).
//!
//! The Thrift node carries identity, snapshot range, and a NovaRocks-private
//! typed plan produced at refresh/codegen time. Lowering converts that plan
//! into typed table descriptors, change files, object-store config, and
//! delete-side descriptors; it does not read connector catalog state or
//! reconstruct Iceberg table metadata.
//! It normalizes the wire facts into provider-owned opaque splits; the stable
//! compat Iceberg instance owns all physical reads and delete correctness.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::protocol::starrocks::decode::layout::{Layout, chunk_schema_for_layout};
use crate::protocol::starrocks::decode::node::{Lowered, ScanRangeCarrier};
use crate::thrift::descriptors;
use crate::thrift::plan_nodes;
use novarocks::connector::iceberg::build_compat_delta_read_splits;
use novarocks::connector::iceberg::delta::{
    BaseDataFileLineage, DeletedFileVisibility, DeltaDataColumn, DeltaScanDeleteSide,
    DeltaSourceFile, DeltaSourceRole, EqualityDeleteTargetData, PositionDeleteFileFormat,
    PositionDeleteSourceData,
};
use novarocks::connector::runtime::{ConnectorReadScanSource, ConnectorScheduledSplit};
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use novarocks::exec::node::scan::BoundScanRanges;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::runtime::query_options::{QueryOptions, query_expire_durations};
use novarocks::runtime::starrocks_fragment_query::StarRocksFragmentQueryRuntime;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorCancellation, ConnectorExecutionBinding,
    ConnectorOpenReaderRequest, ConnectorRequestContext, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use novarocks_types::QueryId;

/// Lower an `ICEBERG_DELTA_SCAN_NODE` into an `ExecNode` of kind
/// `IcebergDeltaScan`. The node must carry a typed refresh/codegen-time
/// plan; this boundary does not read connector catalog state.
pub(crate) fn lower_iceberg_delta_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    out_layout: Layout,
    iceberg_execution: Option<Arc<ConnectorExecutionBinding>>,
    query_id: Option<QueryId>,
    scan_ranges: Option<ScanRangeCarrier<'_>>,
    query_options: &QueryOptions,
) -> Result<Lowered, String> {
    let payload = node.iceberg_delta_scan_node.as_ref().ok_or_else(|| {
        format!(
            "ICEBERG_DELTA_SCAN_NODE node_id={} missing iceberg_delta_scan_node payload",
            node.node_id
        )
    })?;

    // Defense in depth: revalidate snapshot ids are non-negative even though
    // the standalone analyzer already rejects negative values. A Thrift node
    // from a non-analyzer producer (e.g. direct Thrift, future IVM planner
    // path) would bypass that guard and silently misinterpret the ids.
    let node_id = node.node_id;
    if payload.from_snapshot_id < 0 {
        return Err(format!(
            "ivm-a1 lower delta-scan (node_id={node_id}, {}.{}.{}): from_snapshot_id must be non-negative, got {}",
            payload.catalog, payload.iceberg_namespace, payload.table, payload.from_snapshot_id,
        ));
    }
    if payload.to_snapshot_id < 0 {
        return Err(format!(
            "ivm-a1 lower delta-scan (node_id={node_id}, {}.{}.{}): to_snapshot_id must be non-negative, got {}",
            payload.catalog, payload.iceberg_namespace, payload.table, payload.to_snapshot_id,
        ));
    }

    let plan = &payload.delta_plan;
    let data_columns = lower_data_columns(plan);
    let change_files = lower_delta_source_files(&plan.change_files)?;
    let delete_side_payload = lower_delete_side_payload(plan.delete_side.as_ref())?;

    let output_chunk_schema: ChunkSchemaRef = if out_layout.order.is_empty() {
        Arc::new(novarocks::exec::chunk::ChunkSchema::empty())
    } else {
        let desc_tbl = desc_tbl.ok_or_else(|| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE node_id={} requires descriptor table to build chunk schema",
                node.node_id
            )
        })?;
        chunk_schema_for_layout(desc_tbl, &out_layout)?
    };

    let query_id = query_id.ok_or_else(|| {
        "ICEBERG_DELTA_SCAN_NODE requires a query identity for connector cancellation".to_string()
    })?;
    let scan_ranges = scan_ranges.ok_or_else(|| {
        "ICEBERG_DELTA_SCAN_NODE requires scan-range carrier for connector binding".to_string()
    })?;
    let iceberg_execution = iceberg_execution
        .ok_or_else(|| "compat Iceberg execution binding is unavailable".to_string())?;
    let rows = query_options
        .batch_size()
        .and_then(|value| usize::try_from(value).ok())
        .and_then(NonZeroUsize::new)
        .unwrap_or_else(|| NonZeroUsize::new(4096).expect("default batch size is nonzero"));
    let (_, query_expire) = query_expire_durations(Some(query_options));
    let context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        StarRocksFragmentQueryRuntime::new().connector_cancellation(query_id),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())?;
    let provider_schema = provider_schema_for_delta(&output_chunk_schema, &data_columns)?;
    let splits = build_compat_delta_read_splits(change_files, delete_side_payload)
        .map_err(|error| error.to_string())?;
    scan_ranges.capture(node.node_id, BoundScanRanges::None);
    let scheduled = splits
        .into_iter()
        .map(ConnectorScheduledSplit::plain)
        .collect();
    let scan = novarocks::exec::node::scan::ScanNode::new(Arc::new(
        ConnectorReadScanSource::new_scheduled_execution(
            iceberg_execution,
            scheduled,
            ConnectorOpenReaderRequest {
                expected_schema: provider_schema.arrow_schema_ref(),
                batch: ConnectorBatchBudget {
                    max_rows: rows,
                    max_bytes: NonZeroUsize::new(MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES)
                        .expect("SPI handle maximum is nonzero"),
                },
                context,
            },
            provider_schema,
        ),
    ))
    .with_node_id(node.node_id)
    .with_output_chunk_schema(output_chunk_schema)
    .with_accept_empty_scan_ranges(true);

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: out_layout,
    })
}

fn lower_data_columns(plan: &plan_nodes::TIcebergDeltaScanPlan) -> Vec<DeltaDataColumn> {
    plan.data_columns
        .iter()
        .map(|column| DeltaDataColumn {
            name: column.name.clone(),
            field_id: column.field_id,
        })
        .collect()
}

fn provider_schema_for_delta(
    output: &ChunkSchemaRef,
    data_columns: &[DeltaDataColumn],
) -> Result<ChunkSchemaRef, String> {
    let slots = output
        .slots()
        .iter()
        .map(|slot| {
            let virtual_column = matches!(
                slot.name().to_ascii_lowercase().as_str(),
                "_file" | "_pos" | "_row_id" | "_last_updated_sequence_number" | "__change_op"
            );
            let Some(column) = data_columns.iter().find(|column| {
                column.name == slot.name() || column.name.eq_ignore_ascii_case(slot.name())
            }) else {
                if virtual_column {
                    return Ok(slot.clone());
                }
                return Err(format!(
                    "ICEBERG_DELTA_SCAN_NODE output column {} has no Iceberg field-ID descriptor",
                    slot.name()
                ));
            };
            let mut metadata = slot.field().metadata().clone();
            metadata.insert(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                column.field_id.to_string(),
            );
            slot.with_field(slot.field().clone().with_metadata(metadata))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(ChunkSchema::try_new(slots)?))
}

fn lower_delta_source_files(
    files: &[plan_nodes::TIcebergDeltaSourceFile],
) -> Result<Vec<DeltaSourceFile>, String> {
    files.iter().map(lower_delta_source_file).collect()
}

fn lower_delta_source_file(
    file: &plan_nodes::TIcebergDeltaSourceFile,
) -> Result<DeltaSourceFile, String> {
    let role = if file.role == plan_nodes::TIcebergDeltaSourceRole::DATA_FILE {
        reject_role_payload(
            file,
            "DATA_FILE",
            &[
                "position_deletes",
                "equality_field_ids",
                "equality_targets",
                "deleted_file_visibility",
            ],
        )?;
        DeltaSourceRole::DataFile
    } else if file.role == plan_nodes::TIcebergDeltaSourceRole::POSITION_DELETE {
        reject_role_payload(
            file,
            "POSITION_DELETE",
            &[
                "equality_field_ids",
                "equality_targets",
                "deleted_file_visibility",
            ],
        )?;
        let deletes = file.position_deletes.as_ref().ok_or_else(|| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE source file {} role POSITION_DELETE requires position_deletes",
                file.path
            )
        })?;
        DeltaSourceRole::PositionDelete {
            deletes: deletes
                .iter()
                .map(lower_position_delete_source)
                .collect::<Result<Vec<_>, _>>()?,
        }
    } else if file.role == plan_nodes::TIcebergDeltaSourceRole::EQUALITY_DELETE {
        reject_role_payload(
            file,
            "EQUALITY_DELETE",
            &["position_deletes", "deleted_file_visibility"],
        )?;
        let equality_field_ids = file.equality_field_ids.clone().ok_or_else(|| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE source file {} role EQUALITY_DELETE requires equality_field_ids",
                file.path
            )
        })?;
        let targets = file.equality_targets.as_ref().ok_or_else(|| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE source file {} role EQUALITY_DELETE requires equality_targets",
                file.path
            )
        })?;
        DeltaSourceRole::EqualityDelete {
            equality_field_ids,
            targets: targets.iter().map(lower_equality_delete_target).collect(),
        }
    } else if file.role == plan_nodes::TIcebergDeltaSourceRole::DELETED_DATA_FILE {
        reject_role_payload(
            file,
            "DELETED_DATA_FILE",
            &["position_deletes", "equality_field_ids", "equality_targets"],
        )?;
        DeltaSourceRole::DeletedDataFile {
            previous_data_file_visibility: file.deleted_file_visibility.as_ref().map(
                |visibility| DeletedFileVisibility {
                    already_deleted_positions: visibility.already_deleted_positions.clone(),
                },
            ),
        }
    } else {
        return Err(format!(
            "ICEBERG_DELTA_SCAN_NODE source file {} has unknown delta role {:?}",
            file.path, file.role
        ));
    };

    Ok(DeltaSourceFile {
        path: file.path.clone(),
        size: file.size,
        role,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        row_id_allow_list: file.row_id_allow_list.clone(),
    })
}

fn reject_role_payload(
    file: &plan_nodes::TIcebergDeltaSourceFile,
    role_name: &str,
    fields: &[&str],
) -> Result<(), String> {
    for field in fields {
        let present = match *field {
            "position_deletes" => file.position_deletes.is_some(),
            "equality_field_ids" => file.equality_field_ids.is_some(),
            "equality_targets" => file.equality_targets.is_some(),
            "deleted_file_visibility" => file.deleted_file_visibility.is_some(),
            _ => false,
        };
        if present {
            return Err(format!(
                "ICEBERG_DELTA_SCAN_NODE source file {} role {} must not carry {}",
                file.path, role_name, field
            ));
        }
    }
    Ok(())
}

fn lower_position_delete_source(
    delete: &plan_nodes::TIcebergDeltaPositionDeleteSource,
) -> Result<PositionDeleteSourceData, String> {
    Ok(PositionDeleteSourceData {
        delete_file_path: delete.delete_file_path.clone(),
        delete_file_size: delete.delete_file_size,
        referenced_data_file: delete.referenced_data_file.clone(),
        file_format: lower_position_delete_format(delete.file_format)?,
        content_offset: delete.content_offset,
        content_size_in_bytes: delete.content_size_in_bytes,
    })
}

fn lower_position_delete_format(
    format: plan_nodes::TIcebergDeltaPositionDeleteFileFormat,
) -> Result<PositionDeleteFileFormat, String> {
    match format {
        f if f == plan_nodes::TIcebergDeltaPositionDeleteFileFormat::PARQUET => {
            Ok(PositionDeleteFileFormat::Parquet)
        }
        f if f == plan_nodes::TIcebergDeltaPositionDeleteFileFormat::PUFFIN => {
            Ok(PositionDeleteFileFormat::Puffin)
        }
        other => Err(format!(
            "ICEBERG_DELTA_SCAN_NODE unsupported position-delete file format {:?}",
            other
        )),
    }
}

fn lower_equality_delete_target(
    target: &plan_nodes::TIcebergDeltaEqualityDeleteTarget,
) -> EqualityDeleteTargetData {
    EqualityDeleteTargetData {
        data_file_path: target.data_file_path.clone(),
        data_file_size: target.data_file_size,
        data_file_first_row_id: target.data_file_first_row_id,
        data_file_sequence_number: target.data_file_sequence_number,
    }
}

fn lower_delete_side_payload(
    payload: Option<&plan_nodes::TIcebergDeltaDeleteSidePlan>,
) -> Result<Option<DeltaScanDeleteSide>, String> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    Ok(Some(DeltaScanDeleteSide {
        base_data_file_lineage: lower_lineage_map(&payload.base_data_file_lineage),
        previous_data_file_lineage: lower_lineage_map(&payload.previous_data_file_lineage),
        previous_delete_visibility_data_files: payload
            .previous_delete_visibility_data_files
            .iter()
            .map(lower_delete_visibility_data_file)
            .collect::<Result<Vec<_>, _>>()?,
        previously_deleted_positions_per_file: payload
            .previously_deleted_positions_per_file
            .iter()
            .map(|(path, positions)| {
                let converted = positions
                    .iter()
                    .map(|position| {
                        u64::try_from(*position).map_err(|_| {
                            format!(
                                "ICEBERG_DELTA_SCAN_NODE previous deleted position is negative for {}: {}",
                                path, position
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((path.clone(), converted))
            })
            .collect::<Result<HashMap<_, _>, String>>()?,
        deleted_data_file_paths: payload.deleted_data_file_paths.iter().cloned().collect(),
    }))
}

fn lower_lineage_map(
    input: &BTreeMap<String, plan_nodes::TIcebergDeltaBaseDataFileLineage>,
) -> HashMap<String, BaseDataFileLineage> {
    input
        .iter()
        .map(|(path, lineage)| {
            (
                path.clone(),
                BaseDataFileLineage {
                    first_row_id: lineage.first_row_id,
                    data_sequence_number: lineage.data_sequence_number,
                },
            )
        })
        .collect()
}

fn lower_delete_visibility_data_file(
    file: &plan_nodes::TIcebergDeltaDeleteVisibilityDataFile,
) -> Result<novarocks::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor, String> {
    Ok(
        novarocks::connector::iceberg::changes::DeleteVisibilityDataFileDescriptor {
            path: file.path.clone(),
            size: file.size,
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            delete_files: file
                .delete_files
                .iter()
                .map(lower_delete_visibility_delete_file)
                .collect::<Result<Vec<_>, _>>()?,
        },
    )
}

fn lower_delete_visibility_delete_file(
    file: &plan_nodes::TIcebergDeltaDeleteVisibilityDeleteFile,
) -> Result<novarocks::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor, String> {
    Ok(
        novarocks::connector::iceberg::changes::DeleteVisibilityDeleteFileDescriptor {
            path: file.path.clone(),
            file_format: lower_delete_visibility_format(file.file_format)?,
            file_content: lower_delete_visibility_content(file.file_content)?,
            length: file.length,
            content_offset: file.content_offset,
            content_size_in_bytes: file.content_size_in_bytes,
        },
    )
}

fn lower_delete_visibility_format(
    format: plan_nodes::TIcebergDeltaDeleteFileFormat,
) -> Result<novarocks::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat, String> {
    match format {
        f if f == plan_nodes::TIcebergDeltaDeleteFileFormat::PARQUET => {
            Ok(novarocks::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Parquet)
        }
        f if f == plan_nodes::TIcebergDeltaDeleteFileFormat::PUFFIN => {
            Ok(novarocks::connector::iceberg::changes::DeleteVisibilityDeleteFileFormat::Puffin)
        }
        other => Err(format!(
            "ICEBERG_DELTA_SCAN_NODE unsupported delete visibility file format {:?}",
            other
        )),
    }
}

fn lower_delete_visibility_content(
    content: plan_nodes::TIcebergDeltaDeleteFileContent,
) -> Result<novarocks::connector::iceberg::changes::DeleteVisibilityDeleteFileContent, String> {
    match content {
        c if c == plan_nodes::TIcebergDeltaDeleteFileContent::POSITION => {
            Ok(novarocks::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Position)
        }
        c if c == plan_nodes::TIcebergDeltaDeleteFileContent::EQUALITY => {
            Ok(novarocks::connector::iceberg::changes::DeleteVisibilityDeleteFileContent::Equality)
        }
        other => Err(format!(
            "ICEBERG_DELTA_SCAN_NODE unsupported delete visibility file content {:?}",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::provider_schema_for_delta;
    use novarocks::common::ids::SlotId;
    use novarocks::connector::iceberg::delta::DeltaDataColumn;
    use novarocks::exec::chunk::{ChunkSchema, ChunkSlotSchema};

    #[test]
    fn delta_provider_schema_uses_planned_field_ids_and_keeps_virtual_columns() {
        let output = Arc::new(
            ChunkSchema::try_new(vec![
                ChunkSlotSchema::from_field(
                    SlotId::new(1),
                    &Field::new("renamed", DataType::Int64, false),
                    None,
                )
                .expect("data slot"),
                ChunkSlotSchema::from_field(
                    SlotId::new(2),
                    &Field::new("__change_op", DataType::Int8, false),
                    None,
                )
                .expect("virtual slot"),
            ])
            .expect("output schema"),
        );

        let provider = provider_schema_for_delta(
            &output,
            &[DeltaDataColumn {
                name: "renamed".to_string(),
                field_id: 17,
            }],
        )
        .expect("provider schema");

        assert_eq!(
            provider.slots()[0]
                .field()
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY),
            Some(&"17".to_string())
        );
        assert!(provider.slots()[1].field().metadata().is_empty());
    }
}
