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

#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::connector::iceberg::scan_model::IcebergTableInfo;
use crate::coordinator::prepare::scan::{
    IcebergDeltaScanRuntimePlan, ResolvedIcebergDeltaScan, ResolvedIcebergFileScan,
    ResolvedScanExecution, ScanBindingResolver,
};
use crate::exec::node::iceberg_delta_scan::{
    DeltaScanDeleteSidePayload, IcebergDeltaDataColumnPayload,
};
use crate::mv::refresh::execution_context::IcebergMvRefreshContext;
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

impl ScanBindingResolver for IcebergMvRefreshContext {
    fn resolve_scan(
        &self,
        node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        resolve_scan_source(
            node_id,
            &scan.table.source,
            |table, snapshot_id| self.version_scan_source(table, snapshot_id),
            |target_scan| self.target_state_scan_source(target_scan),
            |target_scan| {
                self.target_bindings
                    .target_apply()
                    .resolve_locator_scan(target_scan)
            },
            |table, from_snapshot_id, to_snapshot_id| {
                build_iceberg_delta_scan_runtime_plan(table, from_snapshot_id, to_snapshot_id, self)
            },
        )
    }
}

fn resolve_scan_source<V, S, L, D>(
    node_id: i32,
    source: &ScanSource,
    version: V,
    target_state: S,
    target_locator: L,
    delta: D,
) -> Result<Option<ResolvedScanExecution>, String>
where
    V: FnOnce(&IcebergTableInfo, i64) -> Result<ScanSource, String>,
    S: FnOnce(&crate::sql::planner::table::IcebergMvTargetStateScan) -> Result<ScanSource, String>,
    L: FnOnce(
        &crate::sql::planner::table::IcebergMvTargetLocatorScan,
    ) -> Result<ScanSource, String>,
    D: FnOnce(&IcebergTableInfo, i64, i64) -> Result<IcebergDeltaScanRuntimePlan, String>,
{
    let (kind, resolved) = match source {
        ScanSource::IcebergVersionTable { table, snapshot_id } => {
            let resolved = version(table, *snapshot_id).and_then(|source| {
                resolve_file_scan(node_id, "IcebergVersionTable", source)
                    .map(ResolvedScanExecution::IcebergFiles)
                    .map(Some)
            });
            ("IcebergVersionTable", resolved)
        }
        ScanSource::IcebergMvTargetState(scan) => {
            let resolved = target_state(scan).and_then(|source| {
                resolve_file_scan(node_id, "IcebergMvTargetState", source)
                    .map(ResolvedScanExecution::IcebergFiles)
                    .map(Some)
            });
            ("IcebergMvTargetState", resolved)
        }
        ScanSource::IcebergMvTargetLocator(scan) => {
            let resolved = target_locator(scan).and_then(|source| {
                resolve_file_scan(node_id, "IcebergMvTargetLocator", source)
                    .map(ResolvedScanExecution::IcebergFiles)
                    .map(Some)
            });
            ("IcebergMvTargetLocator", resolved)
        }
        ScanSource::IcebergDeltaTable {
            table,
            from_snapshot_id,
            to_snapshot_id,
        } => {
            let resolved = delta(table, *from_snapshot_id, *to_snapshot_id).map(|runtime_plan| {
                Some(ResolvedScanExecution::IcebergDelta(
                    ResolvedIcebergDeltaScan { runtime_plan },
                ))
            });
            ("IcebergDeltaTable", resolved)
        }
        _ => return Ok(None),
    };
    resolved.map_err(|err| format!("resolve scan binding node_id={node_id} source={kind}: {err}"))
}

fn resolve_file_scan(
    node_id: i32,
    source_kind: &str,
    source: ScanSource,
) -> Result<ResolvedIcebergFileScan, String> {
    let ScanSource::IcebergDataFiles {
        table,
        files,
        cloud_properties,
        binding,
    } = source
    else {
        return Err(format!(
            "internal scan binding contract violation: node_id={node_id} source={source_kind} resolver must return IcebergDataFiles"
        ));
    };
    Ok(ResolvedIcebergFileScan {
        table,
        files,
        cloud_properties,
        binding,
    })
}

pub(crate) fn build_iceberg_delta_scan_runtime_plan(
    table: &IcebergTableInfo,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    refresh_ctx: &IcebergMvRefreshContext,
) -> Result<IcebergDeltaScanRuntimePlan, String> {
    let catalog_key = novarocks_catalog::identifier::normalize_identifier(&table.catalog)?;
    let entry = refresh_ctx
        .base_catalog_entries
        .get(&catalog_key)
        .ok_or_else(|| {
            format!(
                "Iceberg delta scan requires base catalog {} in MV refresh context",
                table.catalog
            )
        })?;
    let ident = iceberg::TableIdent::from_strs([table.namespace.as_str(), table.table.as_str()])
        .map_err(|e| {
            format!(
                "build iceberg table ident for delta scan {}.{}.{}: {e}",
                table.catalog, table.namespace, table.table
            )
        })?;
    let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(entry)
        .map_err(|e| {
            format!(
                "build iceberg catalog for delta scan {}.{}.{}: {e}",
                table.catalog, table.namespace, table.table
            )
        })?;
    let loaded = crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        catalog.load_table(&ident).await
    })
    .map_err(|e| format!("load iceberg table for delta scan runtime failed: {e}"))?
    .map_err(|e| {
        format!(
            "load iceberg table for delta scan {}.{}.{}: {e}",
            table.catalog, table.namespace, table.table
        )
    })?;

    let batch = crate::connector::iceberg::changes::plan_changes(
        &loaded,
        from_snapshot_id,
        Some(to_snapshot_id),
        &[],
    )
    .map_err(|e| {
        format!(
            "ivm-a1 scan binding delta-scan: plan_changes failed for {}.{}.{} from_snapshot={} to_snapshot={}: {e}",
            table.catalog, table.namespace, table.table, from_snapshot_id, to_snapshot_id
        )
    })?;
    let equality_targets_by_delete_file =
        crate::connector::iceberg::changes::equality_delete_targets_at(
            &loaded,
            batch.current_snapshot_id,
            &batch.equality_deletes,
        )
        .map_err(|e| {
            format!(
                "ivm-a1 scan binding delta-scan: plan equality-delete targets failed for {}.{}.{} at snapshot {}: {e}",
                table.catalog, table.namespace, table.table, batch.current_snapshot_id
            )
        })?;
    let change_files =
        crate::connector::iceberg::changes::delta_source_files_from_change_batch_with_equality_targets(
            &batch,
            &equality_targets_by_delete_file,
        )?;
    let has_delete = !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty();
    let delete_side = if has_delete {
        let object_store_factory = crate::connector::iceberg::changes::build_factory_for_table(
            &loaded,
            entry.object_store_config(),
        )?;
        let object_store_factory = std::sync::Arc::new(object_store_factory);
        let expected_object_store_bucket =
            crate::connector::iceberg::changes::expected_object_store_bucket_for_table(&loaded)?;
        let base_data_file_lineage =
            crate::connector::iceberg::changes::base_data_file_lineage_index_at(
                &loaded,
                batch.current_snapshot_id,
            )?;
        let previous_data_file_lineage = if !batch.deleted_data_files.is_empty() {
            crate::connector::iceberg::changes::previous_snapshot_data_file_lineage_index(
                &loaded,
                batch.previous_snapshot_id,
            )?
        } else {
            HashMap::new()
        };
        let deleted_data_file_paths = batch
            .deleted_data_files
            .iter()
            .map(|file| file.path.clone())
            .collect();
        let touched_referenced_data_files: std::collections::HashSet<String> = batch
            .deletes
            .iter()
            .filter_map(|delete| delete.referenced_data_file.clone())
            .collect();
        let previously_deleted_positions_per_file = if !touched_referenced_data_files.is_empty() {
            crate::connector::iceberg::scan_deletes::previously_deleted_positions_at_snapshot(
                &loaded,
                batch.previous_snapshot_id,
                object_store_factory.as_ref(),
                &|path: &str| {
                    crate::connector::iceberg::changes::normalize_delete_projection_path(
                        path,
                        entry.object_store_config(),
                        expected_object_store_bucket.as_deref(),
                    )
                },
                |data_file_path: &str| touched_referenced_data_files.contains(data_file_path),
            )
            .map_err(|e| {
                format!(
                    "ivm-a1 scan binding delta-scan: preload previous deleted positions failed for {}.{}.{} at snapshot {}: {e}",
                    table.catalog, table.namespace, table.table, batch.previous_snapshot_id
                )
            })?
            .into_iter()
            .map(|(path, bitmap)| (path, bitmap.iter().collect::<Vec<_>>()))
            .collect()
        } else {
            HashMap::new()
        };
        let previous_delete_visibility_data_files =
            crate::connector::iceberg::changes::delete_visibility_data_files_at(
                &loaded,
                batch.previous_snapshot_id,
            )?;
        Some(DeltaScanDeleteSidePayload {
            base_data_file_lineage,
            previous_data_file_lineage,
            previous_delete_visibility_data_files,
            previously_deleted_positions_per_file,
            deleted_data_file_paths,
        })
    } else {
        None
    };
    let current_schema = loaded.metadata().current_schema();
    let data_columns = current_schema
        .as_ref()
        .as_struct()
        .fields()
        .iter()
        .map(|field| IcebergDeltaDataColumnPayload {
            name: field.name.clone(),
            field_id: field.id,
        })
        .collect();
    Ok(IcebergDeltaScanRuntimePlan {
        table_location: loaded.metadata().location().to_string(),
        data_columns,
        cloud_properties: entry.cloud_properties_map(),
        change_files,
        delete_side,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::connector::iceberg::scan_model::{
        IcebergDataFileBinding, IcebergDataFileInfo, IcebergSchemaDef,
    };
    use crate::mv::refresh::execution_context::tests_support::{
        TargetLocatorRefreshFixture, aggregate_target_state_refresh_fixture,
        refresh_context_for_target_fixture, target_fixture_table_info,
        target_locator_refresh_fixture,
    };
    use crate::sql::planner::table::{
        BranchScope, IcebergMvTargetLocatorScan, IcebergMvTargetStatePartitionConstraint,
        IcebergMvTargetStateRowFilter, IcebergMvTargetStateScan, TableDef,
    };

    fn table_info(catalog: &str, table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: catalog.to_string(),
            namespace: "db".to_string(),
            table: table.to_string(),
            table_uuid: Some(format!("uuid-{table}")),
            current_snapshot_id: Some(99),
            schema_id: 1,
            location: format!("s3://bucket/{table}"),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn resolved_files(table: IcebergTableInfo) -> ScanSource {
        ScanSource::IcebergDataFiles {
            table,
            files: vec![IcebergDataFileInfo {
                path: "s3://bucket/data.parquet".to_string(),
                size: 10,
                row_count: Some(2),
                column_stats: None,
                partition_spec_id: None,
                partition_key: None,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                delete_files: Vec::new(),
                manifest_path: None,
                partition_values: Vec::new(),
            }],
            cloud_properties: BTreeMap::from([("endpoint".to_string(), "minio".to_string())]),
            binding: IcebergDataFileBinding::ExplicitFiles,
        }
    }

    fn plan_scan(source: ScanSource) -> PlanScanNode {
        PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "scan".to_string(),
                columns: Vec::new(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source,
            },
            alias: None,
            columns: Vec::new(),
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
    }

    fn data_file_to_written_file_for_test(
        data_file: &iceberg::spec::DataFile,
        partition_spec_id: i32,
    ) -> crate::connector::iceberg::commit::WrittenFile {
        crate::connector::iceberg::commit::WrittenFile {
            path: data_file.file_path().to_string(),
            format: data_file.file_format(),
            content: data_file.content_type(),
            partition_values: data_file.partition().clone(),
            partition_spec_id,
            record_count: data_file.record_count(),
            file_size_in_bytes: data_file.file_size_in_bytes(),
            split_offsets: data_file
                .split_offsets()
                .map(|offsets| offsets.to_vec())
                .unwrap_or_default(),
            column_sizes: data_file.column_sizes().clone(),
            value_counts: data_file.value_counts().clone(),
            null_value_counts: data_file.null_value_counts().clone(),
            nan_value_counts: data_file.nan_value_counts().clone(),
            lower_bounds: data_file.lower_bounds().clone(),
            upper_bounds: data_file.upper_bounds().clone(),
            key_metadata: data_file.key_metadata().map(|value| value.to_vec()),
            referenced_data_file: data_file
                .referenced_data_file()
                .map(|value| value.to_string()),
            equality_ids: data_file.equality_ids(),
            first_row_id: data_file.first_row_id(),
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        }
    }

    struct DeltaOverwriteRefreshFixture {
        _base_warehouse: tempfile::TempDir,
        _target_fixture: TargetLocatorRefreshFixture,
        ctx: IcebergMvRefreshContext,
        table: IcebergTableInfo,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    }

    fn delta_overwrite_refresh_fixture(test_name: &str) -> DeltaOverwriteRefreshFixture {
        use std::sync::Arc;

        use crate::connector::iceberg::catalog::registry::{
            block_on_iceberg, build_catalog_entry, build_iceberg_catalog, create_namespace,
            create_table, insert_rows, load_table,
        };
        use crate::connector::iceberg::commit::{
            CommitCtx, CommitOpKind, IcebergCommitAction, IcebergCommitCollector, OverwriteCommit,
        };
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let target_fixture = target_locator_refresh_fixture(&format!("{test_name}_target"));
        let mut ctx = refresh_context_for_target_fixture(&target_fixture);
        let warehouse = tempfile::Builder::new()
            .prefix(&format!("novarocks_delta_binding_{test_name}_"))
            .tempdir()
            .expect("delta warehouse");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let entry = build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), warehouse_uri),
                (
                    "aws.s3.endpoint_url".to_string(),
                    "http://127.0.0.1:9000".to_string(),
                ),
            ],
        )
        .expect("delta catalog entry");
        create_namespace(&entry, "db").expect("delta namespace");
        create_table(
            &entry,
            "db",
            "base",
            &[crate::sql::TableColumnDef {
                name: "k".to_string(),
                data_type: novarocks_catalog::schema::SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("delta table");
        insert_rows(
            &entry,
            "db",
            "base",
            &[
                vec![crate::sql::Literal::Int(1)],
                vec![crate::sql::Literal::Int(2)],
            ],
        )
        .expect("seed delta table");
        let loaded = load_table(&entry, "db", "base").expect("load seeded delta table");
        let metadata = loaded.table.metadata();
        let from_snapshot_id = metadata.current_snapshot_id().expect("seed snapshot id");
        let replacement = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![3]))],
        )
        .expect("replacement batch");
        let replacement_files = block_on_iceberg(async {
            crate::connector::iceberg::data_writer::write_record_batches_as_data_files(
                &loaded.table,
                [replacement],
            )
            .await
        })
        .expect("replacement write runtime")
        .expect("replacement data file");
        let table_ident = iceberg::TableIdent::from_strs(["db", "base"]).expect("table ident");
        let catalog = build_iceberg_catalog(&entry).expect("delta iceberg catalog");
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::Overwrite,
                table_ident,
                Some(from_snapshot_id),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!("{}/data/_staging/test-overwrite", metadata.location()),
                crate::common::types::UniqueId { hi: 0, lo: 107 },
            )
            .with_table_metadata(metadata.clone()),
        );
        for data_file in replacement_files {
            collector.inject_written_file(data_file_to_written_file_for_test(
                &data_file,
                metadata.default_partition_spec_id(),
            ));
        }
        block_on_iceberg(async {
            let file_io = loaded.table.file_io().clone();
            let snapshot_properties = BTreeMap::new();
            let commit_ctx = CommitCtx {
                collector: &collector,
                table: &loaded.table,
                catalog: catalog.as_ref(),
                file_io: &file_io,
                commit_uuid: uuid::Uuid::new_v4(),
                abort_handle: collector.abort_log.clone(),
                target_ref: "main",
                snapshot_properties: &snapshot_properties,
            };
            OverwriteCommit.commit(commit_ctx).await
        })
        .expect("overwrite runtime")
        .expect("overwrite commit");
        entry.invalidate_table_cache("db", "base");
        let loaded = load_table(&entry, "db", "base").expect("load overwritten delta table");
        let to_snapshot_id = loaded
            .table
            .metadata()
            .current_snapshot_id()
            .expect("overwrite snapshot id");
        let table = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "base".to_string(),
            table_uuid: Some(loaded.table.metadata().uuid().to_string()),
            current_snapshot_id: Some(to_snapshot_id),
            schema_id: loaded.table.metadata().current_schema_id(),
            location: loaded.table.metadata().location().to_string(),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        ctx.base_catalog_entries.insert("ice".to_string(), entry);

        DeltaOverwriteRefreshFixture {
            _base_warehouse: warehouse,
            _target_fixture: target_fixture,
            ctx,
            table,
            from_snapshot_id,
            to_snapshot_id,
        }
    }

    fn panic_version(_: &IcebergTableInfo, _: i64) -> Result<ScanSource, String> {
        panic!("unexpected version resolver call")
    }

    fn panic_state(_: &IcebergMvTargetStateScan) -> Result<ScanSource, String> {
        panic!("unexpected target-state resolver call")
    }

    fn panic_locator(_: &IcebergMvTargetLocatorScan) -> Result<ScanSource, String> {
        panic!("unexpected target-locator resolver call")
    }

    fn panic_delta(
        _: &IcebergTableInfo,
        _: i64,
        _: i64,
    ) -> Result<IcebergDeltaScanRuntimePlan, String> {
        panic!("unexpected delta resolver call")
    }

    #[test]
    fn iceberg_mv_refresh_context_implements_scan_binding_resolver() {
        fn assert_impl<T: ScanBindingResolver>() {}
        assert_impl::<IcebergMvRefreshContext>();
    }

    #[test]
    fn real_context_resolves_version_at_exact_pinned_snapshot() {
        let fixture = target_locator_refresh_fixture("scan_binding_version");
        let mut ctx = refresh_context_for_target_fixture(&fixture);
        ctx.base_catalog_entries
            .insert("tgt".to_string(), fixture.target_entry.as_ref().clone());
        let table = target_fixture_table_info(&ctx);
        let resolved = ctx
            .resolve_scan(
                101,
                &plan_scan(ScanSource::IcebergVersionTable {
                    table,
                    snapshot_id: fixture.target_snapshot_id,
                }),
            )
            .expect("real version adapter")
            .expect("version binding");

        let ResolvedScanExecution::IcebergFiles(files) = resolved else {
            panic!("expected file binding");
        };
        assert_eq!(files.binding, IcebergDataFileBinding::ExplicitFiles);
        assert_eq!(files.files.len(), 1);
        assert_eq!(files.files[0].row_count, Some(2));
    }

    #[test]
    fn real_context_version_errors_preserve_catalog_table_and_pinned_snapshot() {
        let fixture = target_locator_refresh_fixture("scan_binding_version_errors");
        let mut ctx = refresh_context_for_target_fixture(&fixture);
        let table = target_fixture_table_info(&ctx);

        let mut missing_catalog = table.clone();
        missing_catalog.catalog = "missing_catalog".to_string();
        let err = ctx
            .resolve_scan(
                102,
                &plan_scan(ScanSource::IcebergVersionTable {
                    table: missing_catalog,
                    snapshot_id: fixture.target_snapshot_id,
                }),
            )
            .expect_err("missing catalog must fail");
        assert!(err.contains("missing_catalog"), "{err}");
        assert!(err.contains("node_id=102"), "{err}");

        ctx.base_catalog_entries
            .insert("tgt".to_string(), fixture.target_entry.as_ref().clone());
        let mut missing_table = table.clone();
        missing_table.table = "missing_table".to_string();
        let err = ctx
            .resolve_scan(
                103,
                &plan_scan(ScanSource::IcebergVersionTable {
                    table: missing_table,
                    snapshot_id: fixture.target_snapshot_id,
                }),
            )
            .expect_err("missing table must fail");
        assert!(err.contains("tgt.db.missing_table"), "{err}");
        assert!(err.contains("node_id=103"), "{err}");

        let missing_snapshot_id = fixture.target_snapshot_id.wrapping_add(1_000_000);
        let err = ctx
            .resolve_scan(
                104,
                &plan_scan(ScanSource::IcebergVersionTable {
                    table,
                    snapshot_id: missing_snapshot_id,
                }),
            )
            .expect_err("missing pinned snapshot must not use current snapshot");
        assert!(err.contains(&missing_snapshot_id.to_string()), "{err}");
        assert!(err.contains("node_id=104"), "{err}");
    }

    #[test]
    fn real_context_resolves_target_locator_through_trait_adapter() {
        let fixture = target_locator_refresh_fixture("scan_binding_locator");
        let ctx = refresh_context_for_target_fixture(&fixture);
        let resolved = ctx
            .resolve_scan(
                105,
                &plan_scan(ScanSource::IcebergMvTargetLocator(
                    IcebergMvTargetLocatorScan {
                        catalog: "tgt".to_string(),
                        database: "db".to_string(),
                        table: "mv".to_string(),
                        target_table_uuid: ctx.rewrite.target_table_uuid.clone(),
                        target_snapshot_id: Some(fixture.target_snapshot_id),
                        apply_key_column: "k".to_string(),
                        branch_id_column: None,
                    },
                )),
            )
            .expect("real target locator adapter")
            .expect("locator binding");
        let ResolvedScanExecution::IcebergFiles(files) = resolved else {
            panic!("expected file binding");
        };
        assert_eq!(
            files.table.current_snapshot_id,
            Some(fixture.target_snapshot_id)
        );
        assert_eq!(files.files.len(), 1);
    }

    #[test]
    fn real_context_resolves_target_state_through_trait_adapter() {
        let (ctx, target_scan) = aggregate_target_state_refresh_fixture();
        let resolved = ctx
            .resolve_scan(
                106,
                &plan_scan(ScanSource::IcebergMvTargetState(target_scan)),
            )
            .expect("real target-state adapter")
            .expect("target-state binding");
        let ResolvedScanExecution::IcebergFiles(files) = resolved else {
            panic!("expected file binding");
        };
        assert_eq!(files.binding, IcebergDataFileBinding::ExplicitFiles);
        assert!(
            files.files.is_empty(),
            "no target snapshot should bind no files"
        );
        assert!(
            files
                .table
                .schema
                .fields
                .iter()
                .any(|field| field.name == "__agg_state_v"),
            "target-state binding must preserve aggregate physical projection"
        );
    }

    fn resolve_target_state_error(
        ctx: &IcebergMvRefreshContext,
        scan: IcebergMvTargetStateScan,
    ) -> String {
        ctx.resolve_scan(206, &plan_scan(ScanSource::IcebergMvTargetState(scan)))
            .expect_err("invalid target-state binding must fail")
    }

    #[test]
    fn target_state_scan_rejects_target_identity_mismatch() {
        let (ctx, mut scan) = aggregate_target_state_refresh_fixture();
        scan.table = "other_mv".to_string();
        let err = resolve_target_state_error(&ctx, scan);
        assert!(err.contains("tgt.db.other_mv"), "{err}");
        assert!(
            err.contains("does not match MV refresh target tgt.db.mv"),
            "{err}"
        );
    }

    #[test]
    fn target_state_scan_rejects_target_uuid_mismatch() {
        let (ctx, mut scan) = aggregate_target_state_refresh_fixture();
        let expected = scan.target_table_uuid.clone();
        scan.target_table_uuid = "unexpected-uuid".to_string();
        let err = resolve_target_state_error(&ctx, scan);
        assert!(err.contains("tgt.db.mv target uuid mismatch"), "{err}");
        assert!(err.contains("scan=unexpected-uuid"), "{err}");
        assert!(err.contains(&format!("context={expected}")), "{err}");
    }

    #[test]
    fn target_state_scan_rejects_target_snapshot_mismatch() {
        let (ctx, mut scan) = aggregate_target_state_refresh_fixture();
        scan.target_snapshot_id = Some(909);
        let err = resolve_target_state_error(&ctx, scan);
        assert!(err.contains("tgt.db.mv target snapshot mismatch"), "{err}");
        assert!(err.contains("scan=Some(909)"), "{err}");
        assert!(err.contains("context=None"), "{err}");
    }

    #[test]
    fn target_state_scan_rejects_aggregate_layout_mismatch() {
        let (ctx, mut scan) = aggregate_target_state_refresh_fixture();
        scan.aggregate_state_layout_version = 99;
        let err = resolve_target_state_error(&ctx, scan);
        assert!(
            err.contains("tgt.db.mv aggregate layout version mismatch"),
            "{err}"
        );
        assert!(err.contains("scan=99 contract=1"), "{err}");
    }

    #[test]
    fn target_state_scan_rejects_physical_columns_mismatch() {
        let (ctx, mut scan) = aggregate_target_state_refresh_fixture();
        scan.physical_column_names.push("unexpected".to_string());
        let err = resolve_target_state_error(&ctx, scan);
        assert!(err.contains("tgt.db.mv physical column mismatch"), "{err}");
        assert!(err.contains("unexpected"), "{err}");
        assert!(err.contains("expected="), "{err}");
    }

    #[test]
    fn target_state_scan_rejects_row_filter_column_mismatch() {
        let (ctx, mut scan) = aggregate_target_state_refresh_fixture();
        scan.row_filter = IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: "unexpected_row_id".to_string(),
            branch_scope: None,
        };
        let err = resolve_target_state_error(&ctx, scan);
        assert!(
            err.contains("tgt.db.mv row filter column mismatch"),
            "{err}"
        );
        assert!(
            err.contains("filter=unexpected_row_id scan=__row_id__"),
            "{err}"
        );
    }

    #[test]
    fn target_state_scan_rejects_branch_scope_mismatch() {
        let (ctx, mut scan) = aggregate_target_state_refresh_fixture();
        scan.row_filter = IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: scan.row_id_column_name.clone(),
            branch_scope: Some(BranchScope {
                branch_id_column_name: "__branch_id__".to_string(),
                branch_id: 0,
            }),
        };
        let err = resolve_target_state_error(&ctx, scan);
        assert!(err.contains("tgt.db.mv has branch scope"), "{err}");
        assert!(
            err.contains("schema contract has no branch contract"),
            "{err}"
        );
    }

    #[test]
    fn real_delta_builder_materializes_changes_delete_visibility_and_cloud_properties() {
        let fixture = delta_overwrite_refresh_fixture("scan_binding_delta");
        let resolved = fixture
            .ctx
            .resolve_scan(
                107,
                &plan_scan(ScanSource::IcebergDeltaTable {
                    table: fixture.table,
                    from_snapshot_id: fixture.from_snapshot_id,
                    to_snapshot_id: fixture.to_snapshot_id,
                }),
            )
            .expect("real delta adapter")
            .expect("delta binding");
        let ResolvedScanExecution::IcebergDelta(delta) = resolved else {
            panic!("expected delta binding");
        };
        assert!(!delta.runtime_plan.change_files.is_empty());
        assert!(!delta.runtime_plan.cloud_properties.is_empty());
        let delete_side = delta
            .runtime_plan
            .delete_side
            .expect("overwrite delta must materialize delete side");
        assert!(!delete_side.previous_data_file_lineage.is_empty());
        assert!(!delete_side.previous_delete_visibility_data_files.is_empty());
        assert!(!delete_side.deleted_data_file_paths.is_empty());
    }

    #[test]
    fn delta_binding_rejects_missing_requested_endpoint_snapshot() {
        let fixture = delta_overwrite_refresh_fixture("missing_delta_endpoint");
        let missing_snapshot_id = fixture.to_snapshot_id + 10_000;
        let err = fixture
            .ctx
            .resolve_scan(
                207,
                &plan_scan(ScanSource::IcebergDeltaTable {
                    table: fixture.table,
                    from_snapshot_id: fixture.from_snapshot_id,
                    to_snapshot_id: missing_snapshot_id,
                }),
            )
            .expect_err("missing requested delta endpoint must fail");
        assert!(
            err.contains(&format!("from_snapshot={}", fixture.from_snapshot_id)),
            "{err}"
        );
        assert!(
            err.contains(&format!("to_snapshot={missing_snapshot_id}")),
            "{err}"
        );
    }

    #[test]
    fn version_dispatch_preserves_explicit_snapshot_and_narrows_files() {
        let source = ScanSource::IcebergVersionTable {
            table: table_info("ice", "base"),
            snapshot_id: 42,
        };
        let resolved = resolve_scan_source(
            7,
            &source,
            |table, snapshot_id| {
                assert_eq!(table.catalog, "ice");
                assert_eq!(snapshot_id, 42);
                Ok(resolved_files(table.clone()))
            },
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect("version binding")
        .expect("binding required");
        let ResolvedScanExecution::IcebergFiles(files) = resolved else {
            panic!("expected file binding");
        };
        assert_eq!(files.table.current_snapshot_id, Some(99));
        assert_eq!(files.files.len(), 1);
        assert_eq!(files.binding, IcebergDataFileBinding::ExplicitFiles);
    }

    #[test]
    fn target_state_dispatch_preserves_projection_contract() {
        let scan = IcebergMvTargetStateScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: "uuid-mv".to_string(),
            target_snapshot_id: Some(77),
            aggregate_state_layout_version: 1,
            columns: Vec::new(),
            group_key_names: vec!["k".to_string()],
            aggregate_state_names: vec!["sum_state".to_string()],
            physical_column_names: vec!["k".to_string(), "sum_state".to_string()],
            row_id_column_name: "k".to_string(),
            row_filter: IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "k".to_string(),
                branch_scope: None,
            },
            partition_constraint: IcebergMvTargetStatePartitionConstraint::Unpartitioned,
        };
        let source = ScanSource::IcebergMvTargetState(scan);
        let resolved = resolve_scan_source(
            8,
            &source,
            panic_version,
            |scan| {
                assert_eq!(scan.target_snapshot_id, Some(77));
                assert_eq!(scan.physical_column_names, ["k", "sum_state"]);
                Ok(resolved_files(table_info("tgt", "mv")))
            },
            panic_locator,
            panic_delta,
        )
        .expect("target-state binding");
        assert!(matches!(
            resolved,
            Some(ResolvedScanExecution::IcebergFiles(_))
        ));
    }

    #[test]
    fn target_locator_dispatch_preserves_apply_key_projection() {
        let source = ScanSource::IcebergMvTargetLocator(IcebergMvTargetLocatorScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: "uuid-mv".to_string(),
            target_snapshot_id: Some(77),
            apply_key_column: "__apply_key".to_string(),
            branch_id_column: Some("__branch_id".to_string()),
        });
        let resolved = resolve_scan_source(
            9,
            &source,
            panic_version,
            panic_state,
            |scan| {
                assert_eq!(scan.apply_key_column, "__apply_key");
                assert_eq!(scan.branch_id_column.as_deref(), Some("__branch_id"));
                Ok(resolved_files(table_info("tgt", "mv")))
            },
            panic_delta,
        )
        .expect("target-locator binding");
        assert!(matches!(
            resolved,
            Some(ResolvedScanExecution::IcebergFiles(_))
        ));
    }

    #[test]
    fn delta_dispatch_returns_fully_materialized_neutral_payload() {
        let source = ScanSource::IcebergDeltaTable {
            table: table_info("ice", "base"),
            from_snapshot_id: 10,
            to_snapshot_id: 20,
        };
        let resolved = resolve_scan_source(
            10,
            &source,
            panic_version,
            panic_state,
            panic_locator,
            |table, from, to| {
                assert_eq!(table.table, "base");
                assert_eq!((from, to), (10, 20));
                Ok(IcebergDeltaScanRuntimePlan {
                    table_location: "s3://bucket/base".to_string(),
                    data_columns: vec![IcebergDeltaDataColumnPayload {
                        name: "k".to_string(),
                        field_id: 1,
                    }],
                    cloud_properties: BTreeMap::new(),
                    change_files: Vec::new(),
                    delete_side: None,
                })
            },
        )
        .expect("delta binding")
        .expect("binding required");
        let ResolvedScanExecution::IcebergDelta(delta) = resolved else {
            panic!("expected delta binding");
        };
        assert_eq!(delta.runtime_plan.table_location, "s3://bucket/base");
        assert_eq!(delta.runtime_plan.data_columns[0].field_id, 1);
    }

    #[test]
    fn ordinary_source_does_not_require_refresh_binding() {
        let source = resolved_files(table_info("ice", "ordinary"));
        let resolved = resolve_scan_source(
            11,
            &source,
            panic_version,
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect("ordinary source");
        assert!(resolved.is_none());
    }

    #[test]
    fn resolver_errors_retain_node_source_and_catalog_table_context() {
        let source = ScanSource::IcebergVersionTable {
            table: table_info("missing_catalog", "missing_table"),
            snapshot_id: 42,
        };
        let err = resolve_scan_source(
            12,
            &source,
            |table, snapshot_id| {
                Err(format!(
                    "load {}.{}.{} snapshot {}: table not found",
                    table.catalog, table.namespace, table.table, snapshot_id
                ))
            },
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect_err("missing context must fail");
        assert!(err.contains("node_id=12"), "{err}");
        assert!(err.contains("IcebergVersionTable"), "{err}");
        assert!(
            err.contains("missing_catalog.db.missing_table snapshot 42"),
            "{err}"
        );
    }

    #[test]
    fn missing_pinned_snapshot_never_falls_back_to_current_snapshot() {
        let calls = Cell::new(0);
        let source = ScanSource::IcebergVersionTable {
            table: table_info("ice", "base"),
            snapshot_id: 42,
        };
        let err = resolve_scan_source(
            13,
            &source,
            |_, snapshot_id| {
                calls.set(calls.get() + 1);
                assert_eq!(snapshot_id, 42);
                Err("snapshot 42 not found".to_string())
            },
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect_err("missing pinned snapshot must fail");
        assert_eq!(calls.get(), 1);
        assert!(err.contains("snapshot 42 not found"), "{err}");
    }

    #[test]
    fn file_resolver_rejects_non_file_variant_as_internal_contract_error() {
        let source = ScanSource::IcebergVersionTable {
            table: table_info("ice", "base"),
            snapshot_id: 42,
        };
        let err = resolve_scan_source(
            14,
            &source,
            |table, snapshot_id| {
                Ok(ScanSource::IcebergVersionTable {
                    table: table.clone(),
                    snapshot_id,
                })
            },
            panic_state,
            panic_locator,
            panic_delta,
        )
        .expect_err("semantic source must not escape adapter");
        assert!(
            err.contains("internal scan binding contract violation"),
            "{err}"
        );
        assert!(err.contains("node_id=14"), "{err}");
    }
}
