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
//! The Thrift node carries only the lightweight identity
//! (`catalog/namespace/table`) plus the snapshot range. The full change
//! batch (insert data files, position-delete, equality-delete, deleted
//! data files) is computed here at lower_plan time via
//! `connector::iceberg::changes::plan_changes`. The delete-side
//! preloads (`base_data_file_lineage`, `previous_delete_visibility`) are
//! captured into `IcebergRuntimeHandles` so per-file operator code can
//! borrow them instead of rebuilding them per file.

use std::sync::Arc;

use crate::connector::iceberg::catalog::IcebergCatalogRegistry;
use crate::descriptors;
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::node::iceberg_delta_scan::{
    ApplyKeySource, BaseTableIdent, DeltaScanDeleteSide, DeltaSourceFile, DeltaSourceRole,
    EqualityDeleteTargetData, IcebergDeltaScanNode, IcebergRuntimeHandles,
    PositionDeleteFileFormat, PositionDeleteTargetData,
};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::layout::{Layout, chunk_schema_for_layout};
use crate::lower::node::Lowered;
use crate::plan_nodes;

/// Lower an `ICEBERG_DELTA_SCAN_NODE` into an `ExecNode` of kind
/// `IcebergDeltaScan`. Requires an `IcebergCatalogRegistry` so the base
/// table can be re-loaded; standard FE-compatible paths do not provide
/// one and reject the node before reaching this function.
pub(crate) fn lower_iceberg_delta_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    out_layout: Layout,
    iceberg_catalogs: Option<&IcebergCatalogRegistry>,
) -> Result<Lowered, String> {
    let payload = node.iceberg_delta_scan_node.as_ref().ok_or_else(|| {
        format!(
            "ICEBERG_DELTA_SCAN_NODE node_id={} missing iceberg_delta_scan_node payload",
            node.node_id
        )
    })?;
    let iceberg_catalogs = iceberg_catalogs.ok_or_else(|| {
        format!(
            "ICEBERG_DELTA_SCAN_NODE node_id={} requires an iceberg_catalogs registry; \
             this entrypoint is IVM-only and the lower_plan caller did not provide one",
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

    let entry = iceberg_catalogs.get(&payload.catalog)?;
    let loaded = crate::connector::iceberg::catalog::load_table(
        &entry,
        &payload.iceberg_namespace,
        &payload.table,
    )?;

    // The snapshot interval is (from_snapshot_id, to_snapshot_id] semantically.
    // Lineage validation (to in metadata, from is a descendant ancestor of to)
    // is enforced by plan_changes / classify_lineage.
    let batch = crate::connector::iceberg::changes::plan_changes(
        &loaded.table,
        payload.from_snapshot_id,
        Some(payload.to_snapshot_id),
        &[],
    )
    .map_err(|e| {
        format!(
            "ivm-a1 lower delta-scan: plan_changes failed for {}.{}.{} from_snapshot={} to_snapshot={}: {e}",
            payload.catalog,
            payload.iceberg_namespace,
            payload.table,
            payload.from_snapshot_id,
            payload.to_snapshot_id
        )
    })?;

    let change_files = build_delta_source_files_from_batch(&batch)?;

    let object_store_factory =
        Arc::new(crate::connector::iceberg::changes::build_factory_for_table(
            &loaded.table,
            entry.object_store_config(),
        )?);

    // Only preload the delete-side full-table indices when the change
    // batch actually contains delete-side roles. Empty preloads waste I/O
    // on the common insert-only path.
    let has_delete = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    let delete_side = if has_delete {
        let base_data_file_lineage =
            crate::connector::iceberg::changes::base_data_file_lineage_index_at(
                &loaded.table,
                batch.current_snapshot_id,
            )?;
        // For OVERWRITE-deleted files we need the original `first_row_id`
        // from the previous-snapshot view (the file is no longer alive in
        // the delta-scan upper snapshot, so `base_data_file_lineage` doesn't
        // index it). Only build this when the change batch actually contains
        // `deleted_data_files`, since the lookup walks every manifest in
        // the previous snapshot.
        let previous_data_file_lineage = if !batch.deleted_data_files.is_empty() {
            crate::connector::iceberg::changes::previous_snapshot_data_file_lineage_index(
                &loaded.table,
                batch.previous_snapshot_id,
            )?
        } else {
            std::collections::HashMap::new()
        };
        let deleted_data_file_paths = batch
            .deleted_data_files
            .iter()
            .map(|file| file.path.clone())
            .collect();
        let previous_delete_visibility =
            crate::engine::delete_flow::load_existing_delete_visibility_by_data_file_at(
                &loaded.table,
                Some(batch.previous_snapshot_id),
                entry.object_store_config(),
            )?;
        Some(DeltaScanDeleteSide {
            base_data_file_lineage,
            previous_delete_visibility,
            previous_data_file_lineage,
            deleted_data_file_paths,
        })
    } else {
        None
    };

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
        object_store_config: entry.object_store_config().cloned(),
        iceberg_runtime: Arc::new(IcebergRuntimeHandles {
            base_table: loaded.table,
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

/// Flatten an `IcebergChangeBatch` into the operator's per-file work list.
/// Each delta source file is annotated with its semantic role
/// (DataFile / PositionDelete / EqualityDelete / DeletedDataFile) so the
/// downstream `IcebergDeltaScanOperator` can dispatch on it.
///
/// **Why `targets` / `previous_data_file_visibility` are empty here:**
/// `PositionDelete::targets` and `EqualityDelete::targets` are populated with
/// `Vec::new()`, and `DeletedDataFile::previous_data_file_visibility` is `None`.
/// This is intentional — operator scanners read from `runtime.delete_side`
/// (the preloaded base-table row-id index and previous delete-visibility map)
/// rather than these per-role fields. Populating per-role fields would duplicate
/// what the runtime already holds and create a second source of truth.
/// See: `src/exec/operators/iceberg_delta_scan.rs::open_position_delete_scanner`,
///      `open_equality_delete_scanner`, `open_deleted_data_file_scanner`.
fn build_delta_source_files_from_batch(
    batch: &crate::connector::iceberg::changes::IcebergChangeBatch,
) -> Result<Vec<DeltaSourceFile>, String> {
    let mut out = Vec::with_capacity(
        batch.inserts.len()
            + batch.deletes.len()
            + batch.equality_deletes.len()
            + batch.deleted_data_files.len(),
    );
    for ins in &batch.inserts {
        out.push(DeltaSourceFile {
            path: ins.path.clone(),
            size: ins.size,
            role: DeltaSourceRole::DataFile,
            partition_spec_id: ins.partition_spec_id,
            partition_key: ins.partition_key.clone(),
            first_row_id: ins.first_row_id,
            data_sequence_number: ins.data_sequence_number,
        });
    }
    for del in &batch.deletes {
        // `referenced_data_file` is the only target identity present on the
        // PositionDeleteRef. The operator scanner re-derives `data_file_first_row_id`
        // through `runtime.delete_side.base_data_file_lineage` (preloaded above).
        let targets = del
            .referenced_data_file
            .as_ref()
            .map(|p| {
                vec![PositionDeleteTargetData {
                    data_file_path: p.clone(),
                    data_file_first_row_id: None,
                }]
            })
            .unwrap_or_default();
        let file_format = match del.file_format {
            iceberg::spec::DataFileFormat::Parquet => PositionDeleteFileFormat::Parquet,
            iceberg::spec::DataFileFormat::Puffin => PositionDeleteFileFormat::Puffin,
            other => {
                return Err(format!(
                    "ivm-a1 lower delta-scan: position-delete file {} has unsupported \
                     file_format {:?}; only Parquet and Puffin are supported",
                    del.delete_file_path, other
                ));
            }
        };
        out.push(DeltaSourceFile {
            path: del.delete_file_path.clone(),
            size: del.delete_file_size,
            role: DeltaSourceRole::PositionDelete {
                targets,
                file_format,
                content_offset: del.content_offset,
                content_size_in_bytes: del.content_size_in_bytes,
            },
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
        });
    }
    for eq in &batch.equality_deletes {
        // Equality deletes don't carry per-target row-ids; the operator
        // scans older data files in the same partition through the iceberg
        // reader, again leveraging the preloaded `base_data_file_lineage`.
        let targets: Vec<EqualityDeleteTargetData> = Vec::new();
        out.push(DeltaSourceFile {
            path: eq.delete_file_path.clone(),
            size: eq.delete_file_size,
            role: DeltaSourceRole::EqualityDelete {
                equality_field_ids: eq.equality_ids.clone(),
                targets,
            },
            partition_spec_id: eq.partition_spec_id,
            partition_key: eq.partition_key.clone(),
            first_row_id: None,
            data_sequence_number: eq.sequence_number,
        });
    }
    for d in &batch.deleted_data_files {
        out.push(DeltaSourceFile {
            path: d.path.clone(),
            size: d.size,
            role: DeltaSourceRole::DeletedDataFile {
                previous_data_file_visibility: None,
            },
            partition_spec_id: d.partition_spec_id,
            partition_key: d.partition_key.clone(),
            first_row_id: d.first_row_id,
            data_sequence_number: d.data_sequence_number,
        });
    }
    Ok(out)
}
