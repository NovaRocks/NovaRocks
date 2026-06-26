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
//! explicit payload produced at refresh/codegen time. Lowering parses that
//! payload into change files, table metadata, object-store config, and
//! delete-side descriptors; it does not read connector catalog state.
//! Delete-side runtime state is captured into `IcebergRuntimeHandles` so
//! per-file operator code can borrow it instead of rebuilding it per file.

use std::sync::Arc;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::node::iceberg_delta_scan::{
    ApplyKeySource, BaseTableIdent, DeltaScanDeleteSide, DeltaScanDeleteSidePayload,
    ICEBERG_DELTA_EXPLICIT_PAYLOAD_VERSION, IcebergDeltaExplicitPayload, IcebergDeltaScanNode,
    IcebergRuntimeHandles,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::layout::{Layout, chunk_schema_for_layout};
use crate::lower::node::Lowered;
use crate::thrift::descriptors;
use crate::thrift::plan_nodes;

/// Lower an `ICEBERG_DELTA_SCAN_NODE` into an `ExecNode` of kind
/// `IcebergDeltaScan`. The node must carry an explicit refresh/codegen-time
/// payload; this boundary does not read connector catalog state.
pub(crate) fn lower_iceberg_delta_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    out_layout: Layout,
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

    let explicit_payload = payload.explicit_payload_json.as_deref().ok_or_else(|| {
        format!(
            "ICEBERG_DELTA_SCAN_NODE node_id={} requires explicit_payload_json; \
             lower does not read connector catalog state",
            node.node_id
        )
    })?;
    let explicit: IcebergDeltaExplicitPayload =
        serde_json::from_str(explicit_payload).map_err(|e| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE node_id={} parse explicit_payload_json failed: {e}",
                node.node_id
            )
        })?;
    if explicit.version != ICEBERG_DELTA_EXPLICIT_PAYLOAD_VERSION {
        return Err(format!(
            "ICEBERG_DELTA_SCAN_NODE node_id={} unsupported explicit payload version {}; expected {}",
            node.node_id, explicit.version, ICEBERG_DELTA_EXPLICIT_PAYLOAD_VERSION
        ));
    }
    let metadata: iceberg::spec::TableMetadata =
        serde_json::from_str(&explicit.serialized_table_metadata).map_err(|e| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE node_id={} parse serialized_table_metadata failed: {e}",
                node.node_id
            )
        })?;
    let change_files = explicit.change_files;
    let delete_side_payload = explicit.delete_side;
    let base_table = build_base_table_from_explicit_metadata(payload, metadata)?;
    let object_store_config = explicit.object_store_config;
    let object_store_factory =
        Arc::new(crate::connector::iceberg::changes::build_factory_for_table(
            &base_table,
            object_store_config.as_ref(),
        )?);
    let delete_side =
        build_delete_side_from_payload(delete_side_payload, object_store_config.as_ref())?;

    let output_chunk_schema: ChunkSchemaRef = if out_layout.order.is_empty() {
        Arc::new(crate::exec::chunk::ChunkSchema::empty())
    } else {
        let desc_tbl = desc_tbl.ok_or_else(|| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE node_id={} requires descriptor table to build chunk schema",
                node.node_id
            )
        })?;
        chunk_schema_for_layout(desc_tbl, &out_layout)?
    };

    let exec_node = IcebergDeltaScanNode {
        base_table_ident: BaseTableIdent {
            catalog: payload.catalog.clone(),
            namespace: payload.iceberg_namespace.clone(),
            table: payload.table.clone(),
        },
        from_snapshot_id: payload.from_snapshot_id,
        to_snapshot_id: payload.to_snapshot_id,
        output_chunk_schema,
        apply_key_source: ApplyKeySource::BaseRowId,
        change_files,
        object_store_config,
        iceberg_runtime: Arc::new(IcebergRuntimeHandles {
            base_table,
            object_store_factory,
            delete_side,
        }),
        node_id: node.node_id,
    };

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::IcebergDeltaScan(exec_node),
        },
        layout: out_layout,
    })
}

fn build_base_table_from_explicit_metadata(
    payload: &plan_nodes::TIcebergDeltaScanNode,
    metadata: iceberg::spec::TableMetadata,
) -> Result<iceberg::table::Table, String> {
    let file_io = if metadata.location().starts_with("memory://") {
        iceberg::io::FileIO::new_with_memory()
    } else {
        iceberg::io::FileIO::new_with_fs()
    };
    iceberg::table::Table::builder()
        .file_io(file_io)
        .metadata(metadata)
        .identifier(iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(payload.iceberg_namespace.clone()),
            payload.table.clone(),
        ))
        .build()
        .map_err(|e| {
            format!(
                "ICEBERG_DELTA_SCAN_NODE build base table handle for {}.{}.{} from explicit metadata failed: {e}",
                payload.catalog, payload.iceberg_namespace, payload.table
            )
        })
}

fn build_delete_side_from_payload(
    payload: Option<DeltaScanDeleteSidePayload>,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Option<DeltaScanDeleteSide>, String> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let mut previously_deleted_positions_per_file = std::collections::HashMap::new();
    for (path, positions) in payload.previously_deleted_positions_per_file {
        let mut bitmap = roaring::RoaringTreemap::new();
        for pos in positions {
            bitmap.insert(pos);
        }
        previously_deleted_positions_per_file.insert(path, bitmap);
    }
    let previous_delete_visibility =
        crate::engine::delete_flow::load_existing_delete_visibility_from_descriptors(
            &payload.previous_delete_visibility_data_files,
            object_store_config,
        )?;
    Ok(Some(DeltaScanDeleteSide {
        base_data_file_lineage: payload.base_data_file_lineage,
        previous_delete_visibility,
        previously_deleted_positions_per_file,
        previous_data_file_lineage: payload.previous_data_file_lineage,
        deleted_data_file_paths: payload.deleted_data_file_paths,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };

    use super::*;

    fn serialized_test_table_metadata() -> String {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///tmp/iceberg_delta_payload_table".to_string(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        serde_json::to_string(&metadata).expect("serialize metadata")
    }

    #[test]
    fn delta_scan_lowers_from_explicit_payload_without_catalog_registry() {
        let payload = serde_json::json!({
            "version": 1,
            "serialized_table_metadata": serialized_test_table_metadata(),
            "object_store_config": null,
            "change_files": [
                {
                    "path": "file:///tmp/added.parquet",
                    "size": 123,
                    "role": "DataFile",
                    "partition_spec_id": null,
                    "partition_key": null,
                    "first_row_id": 1000,
                    "data_sequence_number": 7,
                    "row_id_allow_list": null
                }
            ],
            "delete_side": null
        });
        let mut node = crate::sql::codegen::nodes::default_plan_node();
        node.node_id = 42;
        node.node_type = plan_nodes::TPlanNodeType::ICEBERG_DELTA_SCAN_NODE;
        node.num_children = 0;
        node.row_tuples = vec![];
        node.iceberg_delta_scan_node = Some(plan_nodes::TIcebergDeltaScanNode {
            catalog: "ice".to_string(),
            iceberg_namespace: "db".to_string(),
            table: "orders".to_string(),
            from_snapshot_id: 10,
            to_snapshot_id: 11,
            explicit_payload_json: Some(payload.to_string()),
        });

        let lowered = lower_iceberg_delta_scan_node(
            &node,
            None,
            Layout {
                order: Vec::new(),
                index: HashMap::new(),
            },
        )
        .expect("lower from explicit payload without registry");
        let ExecNodeKind::IcebergDeltaScan(scan) = lowered.node.kind else {
            panic!("expected iceberg delta scan");
        };
        assert_eq!(scan.change_files.len(), 1);
        assert_eq!(scan.change_files[0].path, "file:///tmp/added.parquet");
        assert_eq!(scan.base_table_ident.catalog, "ice");
    }
}
