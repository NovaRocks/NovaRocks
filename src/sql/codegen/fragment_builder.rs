//! PlanFragmentBuilder — lowers planner-owned DistributedPlan requests into
//! Thrift TPlan fragments.
//!
//! Optimizer physical trees are consumed by planner::optimizer_bridge before
//! this module runs. Write sinks and change-stream write DAGs are represented
//! as planner-owned DistributedPlan sink/topology semantics before codegen.

use std::collections::HashMap;

use crate::thrift::data_sinks;

use crate::sql::codegen::FragmentId;
use crate::sql::codegen::boundary_schema::{
    BoundaryKind, BoundarySchemaReport, output_columns_to_boundary_columns,
};
use crate::sql::codegen::{FragmentBuildRequest, MultiFragmentBuildResult, OutputColumn};

pub(in crate::sql::codegen) fn output_columns_for_boundary(
    columns: &[crate::sql::analysis::OutputColumn],
) -> Vec<OutputColumn> {
    columns
        .iter()
        .map(|c| OutputColumn {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        })
        .collect()
}

pub(in crate::sql::codegen) fn result_root_boundary_schema_report(
    fragment_id: FragmentId,
    root_node_id: i32,
    output_columns: &[OutputColumn],
) -> BoundarySchemaReport {
    BoundarySchemaReport {
        fragment_id: Some(fragment_id as i32),
        node_id: root_node_id,
        boundary_kind: BoundaryKind::ResultRoot,
        columns: output_columns_to_boundary_columns(output_columns),
    }
}

pub(in crate::sql::codegen) fn iceberg_table_info(
    source: &crate::sql::catalog::ScanSource,
) -> Option<&crate::sql::catalog::IcebergTableInfo> {
    match source {
        crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::catalog::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::catalog::ScanSource::StarRocks { .. }
        | crate::sql::catalog::ScanSource::IcebergMvTargetState { .. }
        | crate::sql::catalog::ScanSource::IcebergMvTargetLocator { .. } => None,
    }
}

pub(in crate::sql::codegen) fn add_iceberg_equality_delete_required_columns(
    required: &mut std::collections::HashSet<String>,
    table: &crate::sql::catalog::TableDef,
    planned_scan: Option<&crate::sql::codegen::resolve::PlannedConnectorScan>,
) -> Result<(), String> {
    let crate::sql::catalog::ScanSource::IcebergDataFiles {
        table: iceberg,
        files,
        ..
    } = &table.source
    else {
        return Ok(());
    };
    let field_id_to_name: HashMap<i32, String> = iceberg
        .schema
        .fields
        .iter()
        .map(|field| (field.field_id, field.name.clone()))
        .collect();
    let mut add_required_columns_for_file =
        |file: &crate::sql::catalog::IcebergDataFileInfo| -> Result<(), String> {
            for delete_file in &file.delete_files {
                if delete_file.file_content
                    != crate::sql::catalog::IcebergDeleteFileContent::Equality
                {
                    continue;
                }
                if delete_file.equality_field_ids.is_empty() {
                    for column in &delete_file.equality_column_names {
                        required.insert(column.to_lowercase());
                    }
                    continue;
                }
                for field_id in &delete_file.equality_field_ids {
                    let Some(column) = field_id_to_name.get(field_id) else {
                        return Err(format!(
                            "iceberg equality-delete file {} references unknown field id {} in table {}",
                            delete_file.path, field_id, table.name
                        ));
                    };
                    required.insert(column.to_lowercase());
                }
            }
            Ok(())
        };

    if let Some(planned_scan) = planned_scan {
        for split in &planned_scan.splits {
            let split = crate::connector::iceberg::scan_planner::iceberg_split(split)?;
            add_required_columns_for_file(&split.data_file)?;
        }
        return Ok(());
    }

    for file in files {
        add_required_columns_for_file(file)?;
    }
    Ok(())
}

pub(in crate::sql::codegen) fn effective_iceberg_scan_column_names(
    table: &crate::sql::catalog::TableDef,
) -> Vec<String> {
    table.columns.iter().map(|c| c.name.clone()).collect()
}

pub(in crate::sql::codegen) fn iceberg_scan_table_handle_for_codegen(
    original_source: &crate::sql::catalog::ScanSource,
    iceberg_table: &crate::sql::catalog::IcebergTableInfo,
    files: Vec<crate::sql::catalog::IcebergDataFileInfo>,
    column_names: Vec<String>,
) -> crate::connector::scan_planning::TableHandle {
    match original_source {
        crate::sql::catalog::ScanSource::IcebergDataFiles {
            binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
            ..
        }
        | crate::sql::catalog::ScanSource::IcebergVersionTable { .. }
        | crate::sql::catalog::ScanSource::IcebergMvTargetState(_)
        | crate::sql::catalog::ScanSource::IcebergMvTargetLocator(_) => {
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
                &iceberg_table.catalog,
                &iceberg_table.namespace,
                &iceberg_table.table,
                iceberg_table.current_snapshot_id,
                iceberg_table.clone(),
                files,
                column_names,
            )
        }
        _ => crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_for_current_snapshot(
            &iceberg_table.catalog,
            &iceberg_table.namespace,
            &iceberg_table.table,
            iceberg_table.clone(),
            column_names,
        ),
    }
}

// ---------------------------------------------------------------------------
// PlanFragmentBuilder
// ---------------------------------------------------------------------------

pub(crate) struct PlanFragmentBuilder;

impl PlanFragmentBuilder {
    pub(crate) fn build(
        request: FragmentBuildRequest<'_>,
    ) -> Result<MultiFragmentBuildResult, String> {
        crate::sql::codegen::ir::lower_distributed_plan(
            request.distributed_plan,
            request.catalog,
            request.connectors,
            request.mv_refresh_ctx,
        )
    }
}

pub(in crate::sql::codegen) fn synthetic_iceberg_table_id(scan_node_id: i32) -> i64 {
    -(scan_node_id as i64)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(in crate::sql::codegen) fn build_result_sink() -> data_sinks::TDataSink {
    data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::RESULT_SINK,
        None::<data_sinks::TDataStreamSink>,
        Some(data_sinks::TResultSink::default()),
        None::<data_sinks::TMysqlTableSink>,
        None::<data_sinks::TExportSink>,
        None::<data_sinks::TOlapTableSink>,
        None::<data_sinks::TMemoryScratchSink>,
        None::<data_sinks::TMultiCastDataStreamSink>,
        None::<data_sinks::TSchemaTableSink>,
        None::<data_sinks::TIcebergTableSink>,
        None::<data_sinks::THiveTableSink>,
        None::<data_sinks::TTableFunctionTableSink>,
        None::<data_sinks::TDictionaryCacheSink>,
        None::<Vec<Box<data_sinks::TDataSink>>>,
        None::<i64>,
        None::<data_sinks::TSplitDataStreamSink>,
        None::<data_sinks::TIcebergChangeStreamRouterSink>,
    )
}

/// Placeholder sink for child / CTE fragments.  The coordinator replaces
/// this with the real DataStreamSink or MultiCastDataStreamSink after
/// fragment instance IDs are assigned.
pub(in crate::sql::codegen) fn build_noop_sink() -> data_sinks::TDataSink {
    data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::NOOP_SINK,
        None::<data_sinks::TDataStreamSink>,
        None::<data_sinks::TResultSink>,
        None::<data_sinks::TMysqlTableSink>,
        None::<data_sinks::TExportSink>,
        None::<data_sinks::TOlapTableSink>,
        None::<data_sinks::TMemoryScratchSink>,
        None::<data_sinks::TMultiCastDataStreamSink>,
        None::<data_sinks::TSchemaTableSink>,
        None::<data_sinks::TIcebergTableSink>,
        None::<data_sinks::THiveTableSink>,
        None::<data_sinks::TTableFunctionTableSink>,
        None::<data_sinks::TDictionaryCacheSink>,
        None::<Vec<Box<data_sinks::TDataSink>>>,
        None::<i64>,
        None::<data_sinks::TSplitDataStreamSink>,
        None::<data_sinks::TIcebergChangeStreamRouterSink>,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
        WindowBound, WindowFrame, WindowFrameType,
    };
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergColumnStats, IcebergDataFileInfo,
        IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
        IcebergPartitionFieldValue, IcebergPartitionValue, IcebergSchemaDef, IcebergSchemaFieldDef,
        IcebergTableInfo, PhysicalTableLayout, ScanSource, StarRocksTabletRef, TableDef,
    };
    use crate::sql::codegen::runtime_filter_lowering::remap_rf_expr_order;
    use crate::sql::codegen::{FragmentEdgeKind, fallback_audit, nodes};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer;
    use crate::sql::optimizer::operator::{
        GenerateSeriesOp, JoinDistribution, LimitOp, Operator, PhysicalDistributionOp,
        PhysicalHashJoinEqCondition, PhysicalHashJoinOp, ProjectOp, ScanDictionaryColumn, ScanOp,
        ScanVariantColumn, SortOp, TopNOp, TopNPhase, UnionOp, ValuesOp, WindowOp,
    };
    use crate::sql::optimizer::physical_tree::{
        JoinExecutionDistribution, OptimizerPhysicalNode, PlanExecutionProps, attach_scalar_arena,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use crate::sql::planner::optimizer_bridge::scalar::{
        intern_exprs, intern_project_items, intern_sort_items, intern_window_exprs,
    };
    use crate::sql::planner::plan::WindowExpr;
    use crate::sql::planner::{
        ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec, IcebergWriteFragmentSink,
        IcebergWriteInputBinding, IcebergWriteSinkSpec, with_iceberg_change_stream_write,
        with_iceberg_write_sink,
    };
    use crate::thrift::{exprs, plan_nodes};

    fn build_fragments_from_optimizer_for_test(
        plan: &OptimizerPhysicalNode,
        catalog: &dyn CatalogProvider,
        connectors: &crate::connector::ConnectorRegistry,
    ) -> Result<MultiFragmentBuildResult, String> {
        let dp =
            crate::sql::planner::optimizer_bridge::distributed::optimizer_physical_to_distributed_plan(
                plan,
            )?;
        PlanFragmentBuilder::build(crate::sql::codegen::FragmentBuildRequest::result(
            &dp, catalog, connectors, None,
        ))
    }

    fn build_fragments_from_optimizer_for_database_for_test(
        plan: &OptimizerPhysicalNode,
        catalog: &dyn CatalogProvider,
        connectors: &crate::connector::ConnectorRegistry,
        _current_database: &str,
    ) -> Result<MultiFragmentBuildResult, String> {
        build_fragments_from_optimizer_for_test(plan, catalog, connectors)
    }

    fn build_fragments_with_iceberg_sink_from_optimizer_for_database_for_test(
        plan: &OptimizerPhysicalNode,
        catalog: &dyn CatalogProvider,
        connectors: &crate::connector::ConnectorRegistry,
        current_database: &str,
        mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
        sink_spec: &IcebergWriteSinkSpec,
    ) -> Result<MultiFragmentBuildResult, String> {
        let dp =
            crate::sql::planner::optimizer_bridge::distributed::optimizer_physical_to_distributed_plan(
                plan,
            )?;
        let dp = with_iceberg_write_sink(
            dp,
            IcebergWriteFragmentSink {
                descriptor_database: current_database.to_string(),
                spec: sink_spec.clone(),
                input: IcebergWriteInputBinding::RootOutputByOrdinal,
            },
        )?;
        PlanFragmentBuilder::build(crate::sql::codegen::FragmentBuildRequest::result(
            &dp,
            catalog,
            connectors,
            mv_refresh_ctx,
        ))
    }

    fn build_fragments_with_change_stream_write_from_optimizer_for_database_for_test(
        plan: &OptimizerPhysicalNode,
        catalog: &dyn CatalogProvider,
        connectors: &crate::connector::ConnectorRegistry,
        current_database: &str,
        mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
        dag: &ChangeStreamWriteDagSpec,
    ) -> Result<MultiFragmentBuildResult, String> {
        let dp =
            crate::sql::planner::optimizer_bridge::distributed::optimizer_physical_to_distributed_plan(
                plan,
            )?;
        let planned_dp = with_iceberg_change_stream_write(dp, current_database, dag.clone())?;
        PlanFragmentBuilder::build(crate::sql::codegen::FragmentBuildRequest::result(
            &planned_dp.distributed_plan,
            catalog,
            connectors,
            mv_refresh_ctx,
        ))
    }

    /// OQ-5 B1: `remap_rf_expr_order` must translate a runtime filter's
    /// pre-demote `op.eq_conditions` index into the post-demote
    /// `eq_join_conjuncts` index that BE lowering uses, and drop (return
    /// `None`) any RF whose source conjunct was demoted to
    /// `other_join_conjuncts`.
    #[test]
    fn rf_expr_order_remap_handles_demote() {
        // No demotion: surviving conjuncts cover every source index in order,
        // so each pre-demote index maps to itself.
        let identity = [0usize, 1, 2];
        assert_eq!(remap_rf_expr_order(&identity, 0), Some(0));
        assert_eq!(remap_rf_expr_order(&identity, 1), Some(1));
        assert_eq!(remap_rf_expr_order(&identity, 2), Some(2));

        // Earlier conjunct (source index 0) demoted: surviving conjuncts are
        // source indices [1, 2]. An RF on the demoted index 0 must be dropped;
        // indices 1 and 2 shift down to post-demote positions 0 and 1.
        let first_demoted = [1usize, 2];
        assert_eq!(
            remap_rf_expr_order(&first_demoted, 0),
            None,
            "RF on a demoted conjunct must be dropped"
        );
        assert_eq!(remap_rf_expr_order(&first_demoted, 1), Some(0));
        assert_eq!(remap_rf_expr_order(&first_demoted, 2), Some(1));

        // Middle conjunct (source index 1) demoted: surviving = [0, 2]. The
        // surviving RF on source index 2 lands at post-demote position 1 —
        // exactly the index BE uses into build_keys/probe_keys/eq_null_safe.
        let middle_demoted = [0usize, 2];
        assert_eq!(remap_rf_expr_order(&middle_demoted, 0), Some(0));
        assert_eq!(remap_rf_expr_order(&middle_demoted, 1), None);
        assert_eq!(remap_rf_expr_order(&middle_demoted, 2), Some(1));

        // Every remapped index is in range for the post-demote conjunct list
        // (whose length equals `surviving_eq_origin.len()`), which is the
        // invariant the defensive guard in `build_rf_descriptors` relies on.
        for origin in [&identity[..], &first_demoted[..], &middle_demoted[..]] {
            for src in 0..3usize {
                if let Some(j) = remap_rf_expr_order(origin, src) {
                    assert!(
                        j < origin.len(),
                        "post-demote index {j} out of range for {origin:?}"
                    );
                }
            }
        }

        // An out-of-range source index (no matching surviving conjunct) is
        // dropped rather than mis-mapped.
        assert_eq!(remap_rf_expr_order(&identity, 7), None);
        assert_eq!(remap_rf_expr_order(&[], 0), None);
    }

    fn test_iceberg_table_info_with_schema(fields: Vec<IcebergSchemaFieldDef>) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema: IcebergSchemaDef { fields },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn test_iceberg_table_info() -> IcebergTableInfo {
        test_iceberg_table_info_with_schema(vec![])
    }

    fn test_iceberg_table_info_with_id_schema() -> IcebergTableInfo {
        test_iceberg_table_info_with_schema(vec![IcebergSchemaFieldDef {
            field_id: 1,
            name: "id".to_string(),
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            children: vec![],
        }])
    }

    struct DummyCatalog;

    impl CatalogProvider for DummyCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in scan-only builder tests".to_string())
        }

        fn get_physical_layout(
            &self,
            _database: &str,
            _table: &str,
        ) -> Result<Option<PhysicalTableLayout>, String> {
            Ok(None)
        }
    }

    struct FallbackGateCatalog;

    impl FallbackGateCatalog {
        fn test_table() -> TableDef {
            TableDef {
                name: "t".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "a".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                    ColumnDef {
                        name: "b".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                ],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::IcebergDataFiles {
                    table: test_iceberg_table_info_with_schema(vec![
                        IcebergSchemaFieldDef {
                            field_id: 1,
                            name: "a".to_string(),
                            initial_default: None,
                            write_default: None,
                            initial_default_json: None,
                            children: vec![],
                        },
                        IcebergSchemaFieldDef {
                            field_id: 2,
                            name: "b".to_string(),
                            initial_default: None,
                            write_default: None,
                            initial_default_json: None,
                            children: vec![],
                        },
                    ]),
                    files: vec![],
                    cloud_properties: BTreeMap::new(),
                    binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                },
            }
        }
    }

    impl CatalogProvider for FallbackGateCatalog {
        fn get_table(&self, _database: &str, table: &str) -> Result<TableDef, String> {
            if table.eq_ignore_ascii_case("t") {
                Ok(Self::test_table())
            } else {
                Err(format!("unknown test table `{table}`"))
            }
        }
    }

    struct StarRocksCatalog {
        layout: PhysicalTableLayout,
    }

    impl CatalogProvider for StarRocksCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in StarRocks scan builder tests".to_string())
        }

        fn get_physical_layout(
            &self,
            _database: &str,
            _table: &str,
        ) -> Result<Option<PhysicalTableLayout>, String> {
            Ok(Some(self.layout.clone()))
        }
    }

    struct MixedCatalog {
        starrocks_layout: PhysicalTableLayout,
    }

    impl CatalogProvider for MixedCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in mixed scan builder tests".to_string())
        }

        fn get_physical_layout(
            &self,
            _database: &str,
            table: &str,
        ) -> Result<Option<PhysicalTableLayout>, String> {
            if table == "starrocks_t" {
                Ok(Some(self.starrocks_layout.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn stats_for_test() -> Statistics {
        Statistics {
            output_row_count: 0.0,
            column_statistics: HashMap::new(),
            ..Default::default()
        }
    }

    fn output_col_for_test(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: crate::sql::column_id::ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn values_plan_for_test(columns: Vec<OutputColumn>) -> OptimizerPhysicalNode {
        physical_node_for_test(
            Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: columns.clone(),
            }),
            Vec::new(),
            columns,
        )
    }

    fn physical_node_for_test(
        op: Operator,
        children: Vec<OptimizerPhysicalNode>,
        output_columns: Vec<OutputColumn>,
    ) -> OptimizerPhysicalNode {
        attach_test_scalar_arena(OptimizerPhysicalNode {
            op,
            children,
            stats: stats_for_test(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        })
    }

    fn attach_test_scalar_arena(mut plan: OptimizerPhysicalNode) -> OptimizerPhysicalNode {
        let scalars = plan
            .children
            .iter()
            .find_map(|child| child.execution_props.scalar_arena.as_deref().cloned())
            .unwrap_or_else(ScalarArena::new);
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn two_value_join_for_sort_test() -> (
        OptimizerPhysicalNode,
        Vec<OutputColumn>,
        crate::sql::optimizer::scalar::SortKey,
        ScalarArena,
    ) {
        let mut scalars = ScalarArena::new();
        let left_col = output_col_for_test(9101, "left_id", DataType::Int32, false);
        let right_col = output_col_for_test(9102, "right_id", DataType::Int32, false);
        let left_expr =
            column_ref_expr_for_test(left_col.column_id, "left_id", DataType::Int32, false);
        let right_expr =
            column_ref_expr_for_test(right_col.column_id, "right_id", DataType::Int32, false);
        let left_key = intern_typed(&mut scalars, &left_expr);
        let right_key = intern_typed(&mut scalars, &right_expr);
        let output_columns = vec![left_col.clone(), right_col.clone()];
        let join = physical_node_for_test(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Colocate,
            }),
            vec![
                values_plan_for_test(vec![left_col]),
                values_plan_for_test(vec![right_col]),
            ],
            output_columns.clone(),
        );
        let sort_item = intern_sort_items(
            &mut scalars,
            &[SortItem {
                expr: left_expr,
                asc: true,
                nulls_first: false,
            }],
        )
        .remove(0);
        (join, output_columns, sort_item, scalars)
    }

    fn column_ref_expr_for_test(
        column_id: ColumnId,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type,
            nullable,
        }
    }

    fn project_passthrough_plan_for_test(
        child: OptimizerPhysicalNode,
        source_column: &OutputColumn,
        output_column_id: ColumnId,
    ) -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let output_column = OutputColumn {
            column_id: output_column_id,
            name: source_column.name.clone(),
            data_type: source_column.data_type.clone(),
            nullable: source_column.nullable,
            is_internal: source_column.is_internal,
        };
        let items = vec![ProjectItem {
            expr: column_ref_expr_for_test(
                source_column.column_id,
                &source_column.name,
                source_column.data_type.clone(),
                source_column.nullable,
            ),
            output_name: source_column.name.clone(),
            output_column_id,
        }];
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &items),
                output_qualifier: None,
            }),
            children: vec![child],
            stats: stats_for_test(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![output_column],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    #[test]
    fn project_codegen_uses_project_item_output_ids_not_stale_node_outputs() {
        let mut scalars = ScalarArena::new();
        let source = output_col_for_test(1, "__change_op_source", DataType::Int8, false);
        let action_id = ColumnId::new_for_test(14);
        let stale_id = ColumnId::new_for_test(13);
        let parent_id = ColumnId::new_for_test(20);
        let values = OptimizerPhysicalNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: vec![source.clone()],
            }),
            children: Vec::new(),
            stats: stats_for_test(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![source.clone()],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        let child_item = ProjectItem {
            expr: column_ref_expr_for_test(source.column_id, &source.name, DataType::Int8, false),
            output_name: "__change_op".to_string(),
            output_column_id: action_id,
        };
        let stale_child_output = OutputColumn {
            column_id: stale_id,
            name: "__change_op".to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        };
        let child_project = OptimizerPhysicalNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &[child_item]),
                output_qualifier: None,
            }),
            children: vec![values],
            stats: stats_for_test(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![stale_child_output],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        let parent_item = ProjectItem {
            expr: column_ref_expr_for_test(action_id, "__change_op", DataType::Int8, false),
            output_name: "__change_op".to_string(),
            output_column_id: parent_id,
        };
        let parent_output = OutputColumn {
            column_id: parent_id,
            name: "__change_op".to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        };
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &[parent_item]),
                output_qualifier: None,
            }),
            children: vec![child_project],
            stats: stats_for_test(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![parent_output],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &crate::connector::ConnectorRegistry::new(),
            "default",
        )
        .expect("Project codegen must keep ProjectOp item output ids as the scope contract");
    }

    #[derive(Debug)]
    struct MockScanPlanner {
        schema_id: i64,
        splits: Vec<crate::connector::starrocks::table::scan_planner::StarRocksSplit>,
    }

    impl crate::connector::scan_planning::ConnectorScanPlanner for MockScanPlanner {
        fn name(&self) -> &'static str {
            "starrocks"
        }

        fn begin_scan(
            &self,
            table: crate::connector::scan_planning::TableHandle,
            _ctx: crate::connector::scan_planning::BeginScanContext,
        ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
            let inner = table
                .downcast_ref::<crate::connector::starrocks::table::scan_planner::StarRocksTableHandle>()
                .ok_or_else(|| "MockScanPlanner expected StarRocksTableHandle".to_string())?
                .clone();
            Ok(crate::connector::scan_planning::ScanHandle::new(
                "starrocks",
                crate::connector::starrocks::table::scan_planner::StarRocksScanHandle {
                    table: inner,
                    schema_id: self.schema_id,
                },
            ))
        }

        fn plan_splits(
            &self,
            _scan: &crate::connector::scan_planning::ScanHandle,
            _ctx: crate::connector::scan_planning::SplitPlanningContext,
        ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
            Ok(self
                .splits
                .iter()
                .map(|split| {
                    crate::connector::scan_planning::Split::new("starrocks", split.clone())
                })
                .collect())
        }
    }

    #[derive(Debug, Default)]
    struct ScanPlannerCallCounts {
        begin_scan: std::sync::atomic::AtomicUsize,
        plan_splits: std::sync::atomic::AtomicUsize,
    }

    #[derive(Debug)]
    struct CountingScanPlanner {
        inner: MockScanPlanner,
        counts: std::sync::Arc<ScanPlannerCallCounts>,
    }

    impl crate::connector::scan_planning::ConnectorScanPlanner for CountingScanPlanner {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn begin_scan(
            &self,
            table: crate::connector::scan_planning::TableHandle,
            ctx: crate::connector::scan_planning::BeginScanContext,
        ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
            self.counts
                .begin_scan
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.begin_scan(table, ctx)
        }

        fn plan_splits(
            &self,
            scan: &crate::connector::scan_planning::ScanHandle,
            ctx: crate::connector::scan_planning::SplitPlanningContext,
        ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
            self.counts
                .plan_splits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.plan_splits(scan, ctx)
        }
    }

    #[derive(Debug)]
    struct CurrentSnapshotAssertingIcebergPlanner {
        counts: std::sync::Arc<ScanPlannerCallCounts>,
        files: Vec<IcebergDataFileInfo>,
    }

    impl crate::connector::scan_planning::ConnectorScanPlanner
        for CurrentSnapshotAssertingIcebergPlanner
    {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn begin_scan(
            &self,
            table: crate::connector::scan_planning::TableHandle,
            _ctx: crate::connector::scan_planning::BeginScanContext,
        ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
            self.counts
                .begin_scan
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let inner = table
                .downcast_ref::<crate::connector::iceberg::scan_planner::IcebergTableHandle>()
                .ok_or_else(|| "expected IcebergTableHandle".to_string())?
                .clone();
            assert!(
                matches!(
                    inner.split_source,
                    crate::connector::iceberg::scan_planner::IcebergSplitSource::CurrentSnapshot
                ),
                "ordinary Iceberg scans must not embed registered files"
            );
            Ok(crate::connector::scan_planning::ScanHandle::new(
                "iceberg",
                crate::connector::iceberg::scan_planner::IcebergScanHandle { table: inner },
            ))
        }

        fn plan_splits(
            &self,
            scan: &crate::connector::scan_planning::ScanHandle,
            _ctx: crate::connector::scan_planning::SplitPlanningContext,
        ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
            self.counts
                .plan_splits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _scan = crate::connector::iceberg::scan_planner::iceberg_scan_handle(scan)?;
            Ok(self
                .files
                .iter()
                .cloned()
                .map(|data_file| {
                    crate::connector::scan_planning::Split::new(
                        "iceberg",
                        crate::connector::iceberg::scan_planner::IcebergSplit { data_file },
                    )
                })
                .collect())
        }
    }

    fn mock_starrocks_registry(
        layout: &crate::sql::catalog::PhysicalTableLayout,
    ) -> crate::connector::ConnectorRegistry {
        use crate::connector::starrocks::table::scan_planner::StarRocksSplit;
        let splits = layout
            .tablets
            .iter()
            .map(|tablet| StarRocksSplit {
                tablet_id: tablet.tablet_id,
                partition_id: tablet.partition_id,
                version: tablet.version,
            })
            .collect();
        let planner = std::sync::Arc::new(MockScanPlanner {
            schema_id: layout.schema_id,
            splits,
        });
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(planner);
        registry
    }

    fn register_current_snapshot_iceberg_planner(
        registry: &mut crate::connector::ConnectorRegistry,
        files: Vec<IcebergDataFileInfo>,
    ) {
        let counts = std::sync::Arc::new(ScanPlannerCallCounts::default());
        let planner = std::sync::Arc::new(CurrentSnapshotAssertingIcebergPlanner { counts, files });
        registry.register_scan_planner(planner);
    }

    fn mock_iceberg_registry() -> crate::connector::ConnectorRegistry {
        mock_current_snapshot_iceberg_registry_with_files(vec![iceberg_i32_file(
            "s3://bucket/current.parquet",
            1,
            1,
        )])
    }

    fn build_query_for_fallback_gate(sql: &str) -> Result<(), String> {
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let mut stmts =
            sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|err| err.to_string())?;
        let stmt = stmts
            .pop()
            .ok_or_else(|| "expected one query statement".to_string())?;
        let query = match stmt {
            sqlparser::ast::Statement::Query(query) => query,
            other => return Err(format!("expected query statement, got {other:?}")),
        };

        let catalog = FallbackGateCatalog;
        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &catalog, "default")?;
        let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
        let mut scalar_arena = optimizer::scalar::ScalarArena::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            &logical,
            &mut scalar_arena,
        )?;
        let physical = optimizer::optimize_with_legacy_table_stats_for_migration(
            opt_expr,
            scalar_arena,
            &HashMap::new(),
            factory,
            None,
            Vec::new(),
        )?;
        let registry = mock_iceberg_registry();
        build_fragments_from_optimizer_for_database_for_test(
            &physical, &catalog, &registry, "default",
        )?;
        Ok(())
    }

    fn build_raw_query_for_fallback_gate(sql: &str) -> Result<(), String> {
        let stmt = crate::sql::parser::parse_sql_raw(sql)?;
        let query = match stmt {
            sqlparser::ast::Statement::Query(query) => query,
            other => return Err(format!("expected query statement, got {other:?}")),
        };

        let catalog = FallbackGateCatalog;
        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &catalog, "default")?;
        let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
        let mut scalar_arena = optimizer::scalar::ScalarArena::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            &logical,
            &mut scalar_arena,
        )?;
        let physical = optimizer::optimize_with_legacy_table_stats_for_migration(
            opt_expr,
            scalar_arena,
            &HashMap::new(),
            factory,
            None,
            Vec::new(),
        )?;
        let registry = mock_iceberg_registry();
        build_fragments_from_optimizer_for_database_for_test(
            &physical, &catalog, &registry, "default",
        )?;
        Ok(())
    }

    #[test]
    fn p2_targeted_surfaces_do_not_use_name_fallback() {
        let cases = [
            ("aggregate", "SELECT sum(b) + 1 FROM t"),
            (
                "window",
                "SELECT row_number() OVER (PARTITION BY a ORDER BY b) + 1 FROM t",
            ),
            ("select_alias_order", "SELECT a AS x FROM t ORDER BY x"),
            ("values", "SELECT * FROM (VALUES (1, 2)) v(a, b)"),
            (
                "generate_series",
                "SELECT * FROM TABLE(generate_series(1, 3, 1)) AS gs(v)",
            ),
            ("rollup", "SELECT grouping(a), a FROM t GROUP BY ROLLUP(a)"),
            ("using", "SELECT a FROM t l JOIN t r USING(a)"),
            (
                "subquery_alias",
                "SELECT x FROM (SELECT a AS x FROM t) s WHERE x > 0",
            ),
        ];

        for (name, sql) in cases {
            let (result, audit) =
                fallback_audit::run_with_isolated_audit(|| build_query_for_fallback_gate(sql));
            result.unwrap_or_else(|err| {
                panic!("{name} should build without fallback gate error: {err}")
            });
            assert_eq!(
                audit,
                fallback_audit::FallbackAuditSnapshot::default(),
                "{name} triggered codegen name fallback audit: {audit:?}"
            );
        }
    }

    #[test]
    fn cte_produce_declared_output_ids_match_fragment_child_after_recursive_unroll() {
        build_raw_query_for_fallback_gate(
            "WITH RECURSIVE const_cte AS ( \
                 SELECT CAST(1 AS INT) AS a \
             ), \
             r AS ( \
                 SELECT t.a, CAST(0 AS BIGINT) AS step \
                 FROM t, const_cte \
                 WHERE t.a = const_cte.a \
                 UNION ALL \
                 SELECT t.a, r.step + 1 \
                 FROM t INNER JOIN r ON r.a = t.a \
             ) \
             SELECT /*+ SET_VAR(enable_recursive_cte=true, recursive_cte_max_depth=2)*/ \
                    r.a, step, const_cte.a \
             FROM r, const_cte",
        )
        .expect("recursive CTE fragment build should keep CTE produce ids aligned");
    }

    fn mock_current_snapshot_iceberg_registry_with_files(
        files: Vec<IcebergDataFileInfo>,
    ) -> crate::connector::ConnectorRegistry {
        let mut registry = crate::connector::ConnectorRegistry::new();
        register_current_snapshot_iceberg_planner(&mut registry, files);
        registry
    }

    fn iceberg_scan_files(plan: &OptimizerPhysicalNode) -> Vec<IcebergDataFileInfo> {
        let Operator::PhysicalScan(scan) = &plan.op else {
            panic!("expected scan plan");
        };
        let ScanSource::IcebergDataFiles { files, .. } = &scan.table.source else {
            panic!("expected iceberg source");
        };
        files.clone()
    }

    fn mock_starrocks_and_iceberg_registry(
        layout: &crate::sql::catalog::PhysicalTableLayout,
    ) -> crate::connector::ConnectorRegistry {
        let mut registry = mock_starrocks_registry(layout);
        register_current_snapshot_iceberg_planner(
            &mut registry,
            vec![iceberg_i32_file("s3://bucket/current.parquet", 1, 1)],
        );
        registry
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id: crate::sql::column_id::ColumnId::new_for_test(1),
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }]
    }

    fn id_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: crate::sql::column_id::ColumnId::new_for_test(1),
                qualifier: None,
                column: "id".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn id_expr_with_column_id(column_id: crate::sql::column_id::ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: "id".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn id_eq_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(id_expr()),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(value)),
                    data_type: DataType::Int32,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn with_id_predicate(mut plan: OptimizerPhysicalNode, value: i64) -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let Operator::PhysicalScan(scan) = &mut plan.op else {
            panic!("expected scan plan");
        };
        scan.predicates = vec![intern_typed(&mut scalars, &id_eq_literal(value))];
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn iceberg_i32_file(path: &str, min: i32, max: i32) -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: Some(HashMap::from([(
                "id".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: None,
                    column_size: None,
                    lower_bound: Some(min.to_le_bytes().to_vec()),
                    upper_bound: Some(max.to_le_bytes().to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }
    }

    fn iceberg_i32_partition_file(path: &str, id: i32) -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: Some(format!("Struct([{id}])")),
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: vec![],
            manifest_path: Some(format!("manifest-{id}.avro")),
            partition_values: vec![IcebergPartitionFieldValue {
                source_column: "id".to_string(),
                field_name: "id".to_string(),
                transform: "identity".to_string(),
                value: Some(IcebergPartitionValue::Int32(id)),
            }],
        }
    }

    fn iceberg_delete_file(path: &str, length: i64) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            path: path.to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Position,
            length: Some(length),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(0),
            partition_key: None,
            equality_column_names: vec![],
            equality_field_ids: vec![],
        }
    }

    fn stats() -> Statistics {
        Statistics {
            output_row_count: 3.0,
            column_statistics: HashMap::new(),
            ..Default::default()
        }
    }

    fn table_with_delete_files(
        delete_files: Vec<IcebergDeleteFileInfo>,
        iceberg_schema_fields: Vec<IcebergSchemaFieldDef>,
    ) -> TableDef {
        TableDef {
            name: "ice_t".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "category".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info_with_schema(iceberg_schema_fields),
                files: vec![crate::sql::catalog::IcebergDataFileInfo {
                    path: "s3://bucket/data.parquet".to_string(),
                    size: 1,
                    row_count: Some(1),
                    column_stats: None,
                    partition_spec_id: Some(0),
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: Some(1),
                    ivm_change_op: None,
                    included_positions: None,
                    delete_files,
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: BTreeMap::new(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        }
    }

    fn equality_delete_file(
        equality_column_names: Vec<String>,
        equality_field_ids: Vec<i32>,
    ) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            path: "s3://bucket/eq-delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Equality,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(0),
            partition_key: Some("Struct([])".to_string()),
            equality_column_names,
            equality_field_ids,
        }
    }

    #[test]
    fn equality_delete_field_ids_are_resolved_to_required_scan_columns() {
        let mut required = std::collections::HashSet::from(["id".to_string()]);
        let table = table_with_delete_files(
            vec![equality_delete_file(Vec::new(), vec![3])],
            vec![
                IcebergSchemaFieldDef {
                    field_id: 1,
                    name: "id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    children: vec![],
                },
                IcebergSchemaFieldDef {
                    field_id: 3,
                    name: "category".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    children: vec![],
                },
            ],
        );

        add_iceberg_equality_delete_required_columns(&mut required, &table, None)
            .expect("resolve ids");

        assert!(required.contains("id"));
        assert!(required.contains("category"));
    }

    #[test]
    fn equality_delete_column_names_are_legacy_fallback_for_required_scan_columns() {
        let mut required = std::collections::HashSet::from(["id".to_string()]);
        let table = table_with_delete_files(
            vec![equality_delete_file(
                vec!["category".to_string()],
                Vec::new(),
            )],
            vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                children: vec![],
            }],
        );

        add_iceberg_equality_delete_required_columns(&mut required, &table, None)
            .expect("legacy names");

        assert!(required.contains("id"));
        assert!(required.contains("category"));
    }

    #[test]
    fn equality_delete_unknown_field_id_is_planning_error() {
        let mut required = std::collections::HashSet::from(["id".to_string()]);
        let table = table_with_delete_files(
            vec![equality_delete_file(Vec::new(), vec![99])],
            vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                children: vec![],
            }],
        );

        let err = add_iceberg_equality_delete_required_columns(&mut required, &table, None)
            .expect_err("unknown field id");

        assert!(err.contains("unknown field id 99"), "{err}");
    }

    #[test]
    fn equality_delete_required_columns_are_added_from_planned_iceberg_splits() {
        let iceberg_schema_fields = vec![
            IcebergSchemaFieldDef {
                field_id: 1,
                name: "id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                children: vec![],
            },
            IcebergSchemaFieldDef {
                field_id: 3,
                name: "category".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                children: vec![],
            },
        ];
        let mut planned_file = iceberg_i32_file("s3://bucket/current.parquet", 1, 1);
        planned_file.delete_files = vec![equality_delete_file(Vec::new(), vec![3])];
        let table = TableDef {
            name: "ice_t".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "category".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info_with_schema(iceberg_schema_fields),
                files: vec![],
                cloud_properties: BTreeMap::new(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        let plan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table,
                alias: None,
                stats_ref: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: Some(vec!["id".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_current_snapshot_iceberg_registry_with_files(vec![planned_file]),
            "default",
        )
        .expect("build Iceberg fragment");
        let root = build.fragment_results.first().expect("root fragment");

        assert!(
            slot_id_by_name_opt(&root.desc_tbl, "category").is_some(),
            "equality-delete column must be scanned even when registered Iceberg files are empty"
        );
    }

    #[test]
    fn effective_iceberg_scan_columns_use_converted_table_projection() {
        let table = TableDef {
            name: "mv_target".to_string(),
            columns: vec![
                ColumnDef {
                    name: "region".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "sum_v".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info(),
                files: vec![],
                cloud_properties: BTreeMap::new(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };

        assert_eq!(
            effective_iceberg_scan_column_names(&table),
            vec!["region".to_string(), "sum_v".to_string()]
        );
    }

    #[test]
    fn target_state_scan_rejects_equality_delete_files() {
        let source = ScanSource::IcebergDataFiles {
            table: test_iceberg_table_info_with_id_schema(),
            files: vec![IcebergDataFileInfo {
                path: "s3://bucket/data.parquet".to_string(),
                size: 1,
                row_count: Some(1),
                column_stats: None,
                partition_spec_id: Some(0),
                partition_key: None,
                first_row_id: None,
                data_sequence_number: Some(1),
                ivm_change_op: None,
                included_positions: None,
                delete_files: vec![equality_delete_file(
                    vec!["category".to_string()],
                    Vec::new(),
                )],
                manifest_path: None,
                partition_values: vec![],
            }],
            cloud_properties: BTreeMap::new(),
            binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
        };

        let err = nodes::reject_target_state_equality_deletes(&source)
            .expect_err("target-state scan must reject equality deletes");

        assert!(
            err.contains("Iceberg target-state scan does not support equality deletes yet"),
            "{err}"
        );
    }

    fn scan_plan(path: PathBuf) -> OptimizerPhysicalNode {
        attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info(),
                        files: vec![crate::sql::catalog::IcebergDataFileInfo {
                            path: path.display().to_string(),
                            size: 0,
                            row_count: None,
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
                        cloud_properties: Default::default(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        })
    }

    fn starrocks_scan_plan() -> OptimizerPhysicalNode {
        attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "starrocks_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 11,
                        table_id: 22,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        })
    }

    fn iceberg_scan_plan() -> OptimizerPhysicalNode {
        attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![],
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        })
    }

    fn iceberg_scan_plan_with_file_stats() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![
                            iceberg_i32_file("s3://bucket/file-1-5.parquet", 1, 5),
                            iceberg_i32_file("s3://bucket/file-10-20.parquet", 10, 20),
                        ],
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: output_columns(),
                predicates: vec![intern_typed(&mut scalars, &id_eq_literal(12))],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn iceberg_scan_plan_with_partition_values() -> OptimizerPhysicalNode {
        let mut scalars = ScalarArena::new();
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![
                            iceberg_i32_partition_file("s3://bucket/id-1.parquet", 1),
                            iceberg_i32_partition_file("s3://bucket/id-12.parquet", 12),
                        ],
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: output_columns(),
                predicates: vec![intern_typed(&mut scalars, &id_eq_literal(12))],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn iceberg_scan_plan_with_large_file(size: i64) -> OptimizerPhysicalNode {
        let mut file = iceberg_i32_file("s3://bucket/large.parquet", 1, 100);
        file.size = size;
        attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![file],
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        })
    }

    fn iceberg_scan_plan_with_many_delete_files(delete_count: usize) -> OptimizerPhysicalNode {
        let mut file = iceberg_i32_file("s3://bucket/delete-heavy.parquet", 1, 100);
        file.delete_files = (0..delete_count)
            .map(|idx| iceberg_delete_file(&format!("s3://bucket/delete-{idx}.parquet"), 1))
            .collect();
        attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![file],
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        })
    }

    #[test]
    fn iceberg_scan_predicates_feed_min_max_and_file_stats_pruning() {
        let plan = iceberg_scan_plan_with_file_stats();

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_current_snapshot_iceberg_registry_with_files(iceberg_scan_files(&plan)),
            "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let hdfs = scan.hdfs_scan_node.as_ref().expect("hdfs scan payload");

        assert_eq!(
            hdfs.min_max_conjuncts.as_ref().map(Vec::len),
            Some(1),
            "standalone scan predicates should be available to HDFS min/max pruning"
        );
        assert_eq!(hdfs.min_max_tuple_id, hdfs.tuple_id);

        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges");
        assert_eq!(
            ranges.len(),
            1,
            "file-level Iceberg stats should prune the file whose id range cannot contain 12"
        );
        let kept_path = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .and_then(|range| range.full_path.as_deref());
        assert_eq!(kept_path, Some("s3://bucket/file-10-20.parquet"));
    }

    #[test]
    fn iceberg_identity_partition_values_prune_scan_ranges() {
        let plan = iceberg_scan_plan_with_partition_values();

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_current_snapshot_iceberg_registry_with_files(iceberg_scan_files(&plan)),
            "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges");

        assert_eq!(
            ranges.len(),
            1,
            "identity partition values should prune files before scan range planning"
        );
        let kept_path = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .and_then(|range| range.full_path.as_deref());
        assert_eq!(kept_path, Some("s3://bucket/id-12.parquet"));
    }

    #[test]
    fn iceberg_large_plain_files_are_split_into_parallel_scan_ranges() {
        let plan = iceberg_scan_plan_with_large_file(300 * 1024 * 1024);

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_current_snapshot_iceberg_registry_with_files(iceberg_scan_files(&plan)),
            "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges");

        assert_eq!(ranges.len(), 3);
        let first = ranges[0].scan_range.hdfs_scan_range.as_ref().unwrap();
        let second = ranges[1].scan_range.hdfs_scan_range.as_ref().unwrap();
        let third = ranges[2].scan_range.hdfs_scan_range.as_ref().unwrap();
        assert_eq!(first.offset, Some(0));
        assert_eq!(first.length, Some(128 * 1024 * 1024));
        assert_eq!(first.file_length, Some(300 * 1024 * 1024));
        assert_eq!(second.offset, Some(128 * 1024 * 1024));
        assert_eq!(second.length, Some(128 * 1024 * 1024));
        assert_eq!(third.offset, Some(256 * 1024 * 1024));
        assert_eq!(third.length, Some(44 * 1024 * 1024));
    }

    #[test]
    fn iceberg_delete_apply_cost_rejects_too_many_delete_files() {
        let plan = iceberg_scan_plan_with_many_delete_files(1025);

        let err = match build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_current_snapshot_iceberg_registry_with_files(iceberg_scan_files(&plan)),
            "default",
        ) {
            Ok(_) => panic!("delete-heavy scan should fail fast"),
            Err(err) => err,
        };

        assert!(
            err.contains("too many Iceberg delete files"),
            "unexpected error: {err}"
        );
    }

    fn mixed_starrocks_iceberg_join_plan() -> OptimizerPhysicalNode {
        let id_col = crate::sql::column_id::ColumnId::new_for_test(1);
        let mut scalars = ScalarArena::new();
        let left_expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id_col,
                qualifier: Some("ice_t".to_string()),
                column: "id".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        };
        let right_expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id_col,
                qualifier: Some("starrocks_t".to_string()),
                column: "id".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        };
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: intern_typed(&mut scalars, &left_expr),
                    right: intern_typed(&mut scalars, &right_expr),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Colocate,
            }),
            children: vec![iceberg_scan_plan(), starrocks_scan_plan()],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    #[test]
    fn build_splits_gather_distribution_into_stream_edge() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let mut scalars = ScalarArena::new();
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalSort(SortOp {
                items: intern_sort_items(
                    &mut scalars,
                    &[SortItem {
                        expr: id_expr(),
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                analytic_partition_exprs: Vec::new(),
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![OptimizerPhysicalNode {
                op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                    spec: DistributionSpec::Gather,
                }),
                children: vec![scan_plan(file.path().to_path_buf())],
                stats: stats(),
                explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(
                ),
                output_columns: output_columns(),
                execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(
                ),
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
            }],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");

        assert_eq!(build.fragment_results.len(), 2);
        assert_eq!(build.edges.len(), 1);
        assert!(matches!(
            build.edges[0].edge_kind,
            crate::sql::codegen::FragmentEdgeKind::Stream
        ));
        assert_eq!(
            build.edges[0].stream_kind,
            crate::sql::codegen::FragmentStreamKind::Gather
        );

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert!(
            root.plan
                .nodes
                .iter()
                .any(|node| { node.node_type == plan_nodes::TPlanNodeType::EXCHANGE_NODE })
        );
    }

    #[test]
    fn gather_over_limit_applies_limit_on_exchange_receiver() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let output = output_columns();
        let mut scalars = ScalarArena::new();
        let mut plan = physical_node_for_test(
            Operator::PhysicalSort(SortOp {
                items: intern_sort_items(
                    &mut scalars,
                    &[SortItem {
                        expr: id_expr(),
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                analytic_partition_exprs: Vec::new(),
                partition_limit: None,
                topn_type: None,
            }),
            vec![physical_node_for_test(
                Operator::PhysicalLimit(LimitOp {
                    limit: Some(1),
                    offset: None,
                }),
                vec![physical_node_for_test(
                    Operator::PhysicalDistribution(PhysicalDistributionOp {
                        spec: DistributionSpec::Gather,
                    }),
                    vec![scan_plan(file.path().to_path_buf())],
                    output.clone(),
                )],
                output.clone(),
            )],
            output,
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");

        assert_eq!(build.fragment_results.len(), 2);
        assert_eq!(build.edges.len(), 1);
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let exchange = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::EXCHANGE_NODE)
            .expect("root fragment should receive gathered rows");
        assert_eq!(exchange.limit, 1);

        let child = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id != build.root_fragment_id)
            .expect("child fragment");
        let scan = child
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("child fragment should scan");
        assert_eq!(
            scan.limit, -1,
            "global LIMIT must not be pushed into the per-BE sender fragment"
        );
    }

    #[test]
    fn sort_over_multi_tuple_child_emits_projection_exprs() {
        let (join, output_columns, sort_item, scalars) = two_value_join_for_sort_test();
        let mut plan = physical_node_for_test(
            Operator::PhysicalSort(SortOp {
                items: vec![sort_item],
                analytic_partition_exprs: Vec::new(),
                partition_limit: None,
                topn_type: None,
            }),
            vec![join],
            output_columns,
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let sort_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::SORT_NODE)
            .expect("sort node");
        let sort = sort_node.sort_node.as_ref().expect("sort payload");

        assert_eq!(sort_node.row_tuples.len(), 2);
        assert_eq!(
            sort.sort_info.sort_tuple_slot_exprs.as_ref().map(Vec::len),
            Some(2),
            "Sort over a multi-tuple child must describe its visible output projection"
        );
    }

    #[test]
    fn topn_over_multi_tuple_child_preserves_tuple_contract() {
        let (join, output_columns, sort_item, scalars) = two_value_join_for_sort_test();
        let mut plan = physical_node_for_test(
            Operator::PhysicalTopN(TopNOp {
                items: vec![sort_item],
                limit: Some(10),
                offset: None,
                phase: TopNPhase::Final,
                is_split: false,
            }),
            vec![join],
            output_columns,
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let sort_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::SORT_NODE)
            .expect("sort node");
        let sort = sort_node.sort_node.as_ref().expect("sort payload");

        assert_eq!(
            sort_node.row_tuples.len(),
            2,
            "TopN preserves its child's relational output, including both join tuples"
        );
        assert_eq!(
            sort.sort_info.sort_tuple_slot_exprs.as_ref().map(Vec::len),
            None,
            "TopN should preserve child tuple layout directly instead of remapping by plan output ids"
        );
    }

    #[test]
    fn build_broadcast_distribution_edge_uses_unpartitioned_stream_partition() {
        let columns = output_columns();
        let plan = physical_node_for_test(
            Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Broadcast,
            }),
            vec![values_plan_for_test(columns.clone())],
            columns,
        );

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");

        assert_eq!(build.edges.len(), 1);
        assert!(matches!(
            build.edges[0].edge_kind,
            crate::sql::codegen::FragmentEdgeKind::Stream
        ));
        assert_eq!(
            build.edges[0].stream_kind,
            crate::sql::codegen::FragmentStreamKind::Broadcast
        );
        assert_eq!(
            build.edges[0].output_partition.type_,
            crate::thrift::partitions::TPartitionType::UNPARTITIONED
        );
    }

    #[test]
    fn hash_join_thrift_distribution_uses_execution_metadata_partitioned() {
        let mut plan = mixed_starrocks_iceberg_join_plan();
        let Operator::PhysicalHashJoin(op) = &mut plan.op else {
            panic!("expected hash join");
        };
        op.distribution = JoinDistribution::Unknown;
        plan.execution_props.join_distribution = Some(JoinExecutionDistribution::Partitioned);

        let starrocks_layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let registry = mock_starrocks_and_iceberg_registry(&starrocks_layout);
        let catalog = MixedCatalog { starrocks_layout };

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let hash_join_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HASH_JOIN_NODE)
            .expect("hash join node");

        assert_eq!(
            hash_join_node
                .hash_join_node
                .as_ref()
                .and_then(|join| join.distribution_mode),
            Some(plan_nodes::TJoinDistributionMode::PARTITIONED)
        );
    }

    #[test]
    fn runtime_filter_uses_execution_distribution_metadata() {
        let mut plan = mixed_starrocks_iceberg_join_plan();
        let Operator::PhysicalHashJoin(op) = &plan.op else {
            panic!("expected hash join");
        };
        let eq = op.eq_conditions[0].clone();
        plan.execution_props.join_distribution = Some(JoinExecutionDistribution::Partitioned);
        plan.build_runtime_filters = vec![RuntimeFilterDesc {
            filter_id: 7,
            build_expr: eq.right,
            probe_expr: eq.left,
            expr_order: 0,
            distribution: JoinDistribution::Broadcast,
        }];

        let starrocks_layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let registry = mock_starrocks_and_iceberg_registry(&starrocks_layout);
        let catalog = MixedCatalog { starrocks_layout };

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let hash_join_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HASH_JOIN_NODE)
            .expect("hash join node");
        let rf = hash_join_node
            .hash_join_node
            .as_ref()
            .and_then(|join| join.build_runtime_filters.as_ref())
            .and_then(|filters| filters.first())
            .expect("runtime filter");

        assert_eq!(
            rf.build_join_mode,
            Some(crate::thrift::runtime_filter::TRuntimeFilterBuildJoinMode::PARTITIONED)
        );
        assert_eq!(
            rf.layout.as_ref().and_then(|layout| layout.global_layout),
            Some(crate::thrift::runtime_filter::TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L)
        );
    }

    #[test]
    fn runtime_filter_invalid_build_binding_is_skipped() {
        let mut plan = mixed_starrocks_iceberg_join_plan();
        let Operator::PhysicalHashJoin(op) = &plan.op else {
            panic!("expected hash join");
        };
        let eq = op.eq_conditions[0].clone();
        let mut scalars = plan
            .execution_props
            .scalar_arena
            .as_deref()
            .unwrap()
            .clone();
        let build_expr = column_ref_expr_for_test(
            ColumnId::new_for_test(9999),
            "wrong_build_id",
            DataType::Int32,
            false,
        );
        let build_expr = intern_exprs(&mut scalars, &[build_expr])[0];
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan.execution_props.join_distribution = Some(JoinExecutionDistribution::Broadcast);
        plan.build_runtime_filters = vec![RuntimeFilterDesc {
            filter_id: 99,
            build_expr,
            probe_expr: eq.left,
            expr_order: 0,
            distribution: JoinDistribution::Broadcast,
        }];

        let starrocks_layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let registry = mock_starrocks_and_iceberg_registry(&starrocks_layout);
        let catalog = MixedCatalog { starrocks_layout };

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let hash_join_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HASH_JOIN_NODE)
            .expect("hash join node");
        let build_filters = hash_join_node
            .hash_join_node
            .as_ref()
            .and_then(|join| join.build_runtime_filters.as_ref());

        assert!(
            build_filters.is_none_or(Vec::is_empty),
            "invalid runtime filter descriptor should be skipped"
        );
    }

    #[test]
    fn runtime_filter_unknown_uses_execution_metadata_broadcast() {
        let mut plan = mixed_starrocks_iceberg_join_plan();
        let Operator::PhysicalHashJoin(op) = &plan.op else {
            panic!("expected hash join");
        };
        let eq = op.eq_conditions[0].clone();
        plan.execution_props.join_distribution = Some(JoinExecutionDistribution::Broadcast);
        plan.build_runtime_filters = vec![RuntimeFilterDesc {
            filter_id: 7,
            build_expr: eq.right,
            probe_expr: eq.left,
            expr_order: 0,
            distribution: JoinDistribution::Unknown,
        }];

        let starrocks_layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let registry = mock_starrocks_and_iceberg_registry(&starrocks_layout);
        let catalog = MixedCatalog { starrocks_layout };

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let hash_join_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HASH_JOIN_NODE)
            .expect("hash join node");
        let rf = hash_join_node
            .hash_join_node
            .as_ref()
            .and_then(|join| join.build_runtime_filters.as_ref())
            .and_then(|filters| filters.first())
            .expect("runtime filter");

        assert_eq!(
            rf.build_join_mode,
            Some(crate::thrift::runtime_filter::TRuntimeFilterBuildJoinMode::BORADCAST)
        );
        assert_eq!(
            rf.layout.as_ref().and_then(|layout| layout.global_layout),
            Some(crate::thrift::runtime_filter::TRuntimeFilterLayoutMode::SINGLETON)
        );
    }

    #[test]
    fn multi_group_window_reuses_child_ordering_without_redundant_sorts() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let id = id_expr_with_column_id(crate::sql::column_id::ColumnId(1));
        let order_by = vec![SortItem {
            expr: id.clone(),
            asc: true,
            nulls_first: true,
        }];
        let mut scalars = ScalarArena::new();
        let win_rows = WindowExpr {
            name: "sum".to_string(),
            args: vec![id.clone()],
            distinct: false,
            partition_by: vec![],
            order_by: order_by.clone(),
            window_frame: Some(WindowFrame {
                frame_type: WindowFrameType::Rows,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
            }),
            result_type: DataType::Int64,
            output_name: "sum_rows".to_string(),
            output_column_id: crate::sql::column_id::ColumnId::new_for_test(7101),
            ignore_nulls: false,
        };
        let win_range = WindowExpr {
            window_frame: Some(WindowFrame {
                frame_type: WindowFrameType::Range,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
            }),
            output_name: "sum_range".to_string(),
            output_column_id: crate::sql::column_id::ColumnId::new_for_test(7102),
            ..win_rows.clone()
        };
        let window_output_columns = vec![
            output_col_for_test(7101, "sum_rows", DataType::Int64, true),
            output_col_for_test(7102, "sum_range", DataType::Int64, true),
        ];
        let window_exprs = intern_window_exprs(&mut scalars, &[win_rows, win_range]);
        let sort_items = intern_sort_items(&mut scalars, &order_by);
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalWindow(WindowOp {
                window_exprs,
                output_columns: window_output_columns,
            }),
            children: vec![OptimizerPhysicalNode {
                op: Operator::PhysicalSort(SortOp {
                    items: sort_items,
                    analytic_partition_exprs: Vec::new(),
                    partition_limit: None,
                    topn_type: None,
                }),
                children: vec![scan_plan(file.path().to_path_buf())],
                stats: stats(),
                explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(
                ),
                output_columns: output_columns(),
                execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(
                ),
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
            }],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let sort_count = root
            .plan
            .nodes
            .iter()
            .filter(|node| node.node_type == plan_nodes::TPlanNodeType::SORT_NODE)
            .count();

        assert_eq!(
            sort_count, 1,
            "child ordering already satisfies both window groups"
        );
    }

    #[test]
    fn result_sink_projects_declared_window_output_columns() {
        let input_col = output_col_for_test(1, "k1", DataType::Int64, true);
        let window_col = output_col_for_test(2, "idx", DataType::Int64, false);
        let window_expr = WindowExpr {
            name: "row_number".to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: "idx".to_string(),
            output_column_id: window_col.column_id,
            ignore_nulls: false,
        };
        let mut scalars = ScalarArena::new();
        let mut window_plan = physical_node_for_test(
            Operator::PhysicalWindow(WindowOp {
                window_exprs: intern_window_exprs(&mut scalars, &[window_expr]),
                output_columns: vec![input_col.clone(), window_col.clone()],
            }),
            vec![values_plan_for_test(vec![input_col])],
            vec![window_col],
        );
        attach_scalar_arena(&mut window_plan, Arc::new(scalars));

        let build = build_fragments_from_optimizer_for_database_for_test(
            &window_plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("window build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let output_exprs = root
            .output_exprs
            .as_ref()
            .expect("result sink must project logical output columns");

        assert_eq!(output_exprs.len(), 1);
        assert_eq!(output_exprs[0].nodes.len(), 1);
        assert_eq!(
            output_exprs[0].nodes[0].node_type,
            exprs::TExprNodeType::SLOT_REF
        );
    }

    #[test]
    fn build_nested_gather_distribution_targets_immediate_parent_fragment() {
        // Wrap the nested gathers inside a Sort so the root is NOT a Gather
        // (root-level Gather is elided).
        let file = NamedTempFile::new().expect("temp parquet path");
        let mut scalars = ScalarArena::new();
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalSort(SortOp {
                items: intern_sort_items(
                    &mut scalars,
                    &[SortItem {
                        expr: id_expr(),
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                analytic_partition_exprs: Vec::new(),
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![OptimizerPhysicalNode {
                op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                    spec: DistributionSpec::Gather,
                }),
                children: vec![OptimizerPhysicalNode {
                    op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                        spec: DistributionSpec::Gather,
                    }),
                    children: vec![scan_plan(file.path().to_path_buf())],
                    stats: stats(),
                    explain_stats:
                        crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
                    output_columns: output_columns(),
                    execution_props:
                        crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
                    build_runtime_filters: Vec::new(),
                    probe_runtime_filters: Vec::new(),
                }],
                stats: stats(),
                explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(
                ),
                output_columns: output_columns(),
                execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(
                ),
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
            }],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: output_columns(),
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");
        assert_eq!(build.fragment_results.len(), 3);
        assert_eq!(build.edges.len(), 2);

        // The inner gather targets its immediate parent (the outer gather fragment),
        // not the root fragment directly.
        let outer_gather_frag_id = build
            .edges
            .iter()
            .find(|e| e.target_fragment_id == build.root_fragment_id)
            .expect("edge to root")
            .source_fragment_id;
        assert!(build.edges.iter().any(|e| {
            e.target_fragment_id == outer_gather_frag_id
                && e.source_fragment_id != outer_gather_frag_id
                && matches!(e.edge_kind, crate::sql::codegen::FragmentEdgeKind::Stream)
        }));
    }

    #[test]
    fn build_maps_hash_distribution_to_hash_partitioned_edge() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let hash_col = crate::sql::column_id::ColumnId(1);
        let mut scan = scan_plan(file.path().to_path_buf());
        scan.output_columns[0].column_id = hash_col;
        let Operator::PhysicalScan(scan_op) = &mut scan.op else {
            panic!("expected scan child");
        };
        scan_op.columns[0].column_id = hash_col;
        let plan = physical_node_for_test(
            Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::shuffle_agg([hash_col]),
            }),
            vec![scan],
            output_columns(),
        );

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");
        let edge = build.edges.first().expect("stream edge");
        assert_eq!(
            edge.stream_kind,
            crate::sql::codegen::FragmentStreamKind::Partitioned
        );
        assert_eq!(
            edge.output_partition.type_,
            crate::thrift::partitions::TPartitionType::HASH_PARTITIONED
        );
        assert_eq!(
            edge.output_partition
                .partition_exprs
                .as_ref()
                .map(|v| v.len()),
            Some(1)
        );
    }

    #[test]
    fn build_rejects_any_distribution_in_fragment_builder() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let plan = physical_node_for_test(
            Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Any,
            }),
            vec![scan_plan(file.path().to_path_buf())],
            output_columns(),
        );

        let result = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        );
        let err = result.err().expect("distribution any must fail");
        assert!(err.contains("DistributionSpec::Any"));
    }

    #[test]
    fn build_elides_root_gather_distribution() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let plan = physical_node_for_test(
            Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            }),
            vec![scan_plan(file.path().to_path_buf())],
            output_columns(),
        );

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");
        assert_eq!(build.fragment_results.len(), 1);
        assert!(build.edges.is_empty());
    }

    #[test]
    fn p2_values_registers_output_columns_by_id_without_name_fallback() {
        let source_column = output_col_for_test(9101, "v", DataType::Int32, true);
        let plan = project_passthrough_plan_for_test(
            values_plan_for_test(vec![source_column.clone()]),
            &source_column,
            ColumnId::new_for_test(9102),
        );

        let (result, audit) = fallback_audit::run_with_isolated_audit(|| {
            build_fragments_from_optimizer_for_database_for_test(
                &plan,
                &DummyCatalog,
                &crate::connector::ConnectorRegistry::new(),
                "default",
            )
        });
        result.expect("build");
        assert_eq!(
            audit,
            fallback_audit::FallbackAuditSnapshot::default(),
            "Project over Values must resolve child output by ColumnId"
        );
    }

    #[test]
    fn p2_generate_series_registers_output_column_by_id_without_name_fallback() {
        let source_column = output_col_for_test(9201, "x", DataType::Int64, false);
        let generate_series = OptimizerPhysicalNode {
            op: Operator::PhysicalGenerateSeries(GenerateSeriesOp {
                start: 1,
                end: 3,
                step: 1,
                column_name: source_column.name.clone(),
                alias: Some("gs".to_string()),
                output_column_id: source_column.column_id,
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 3.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![source_column.clone()],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        let plan = project_passthrough_plan_for_test(
            generate_series,
            &source_column,
            ColumnId::new_for_test(9202),
        );

        let (result, audit) = fallback_audit::run_with_isolated_audit(|| {
            build_fragments_from_optimizer_for_database_for_test(
                &plan,
                &DummyCatalog,
                &crate::connector::ConnectorRegistry::new(),
                "default",
            )
        });
        result.expect("build");
        assert_eq!(
            audit,
            fallback_audit::FallbackAuditSnapshot::default(),
            "Project over GenerateSeries must resolve child output by ColumnId"
        );
    }

    #[test]
    fn residual_union_distinct_is_rejected_before_fragment_codegen() {
        let union_output = output_col_for_test(9351, "u", DataType::Int32, true);
        let union_left = output_col_for_test(9352, "u", DataType::Int32, true);
        let union_right = output_col_for_test(9353, "u", DataType::Int32, true);
        let plan = physical_node_for_test(
            Operator::PhysicalUnion(UnionOp {
                all: false,
                output_columns: vec![union_output.clone()],
                child_output_columns: vec![vec![union_left.clone()], vec![union_right.clone()]],
            }),
            vec![
                values_plan_for_test(vec![union_left]),
                values_plan_for_test(vec![union_right]),
            ],
            vec![union_output],
        );

        let err = match build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &crate::connector::ConnectorRegistry::new(),
            "default",
        ) {
            Ok(_) => panic!("residual UNION DISTINCT should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.contains("UNION DISTINCT must be rewritten by UnionDistinctToAggregate"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_generate_series_emits_table_function_without_scan_source() {
        let plan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalGenerateSeries(GenerateSeriesOp {
                start: 1,
                end: 3_000_000,
                step: 1,
                column_name: "generate_series".to_string(),
                alias: Some("gs".to_string()),
                output_column_id: crate::sql::column_id::ColumnId::new_for_test(9001),
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 3_000_000.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![OutputColumn {
                column_id: crate::sql::column_id::ColumnId::new_for_test(9001),
                name: "generate_series".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &crate::connector::ConnectorRegistry::new(),
            "default",
        )
        .expect("build");
        let root = build.fragment_results.first().expect("root fragment");
        assert!(root.exec_params.per_node_scan_ranges.is_empty());
        assert!(
            root.plan.nodes.iter().all(|node| {
                !matches!(
                    node.node_type,
                    plan_nodes::TPlanNodeType::HDFS_SCAN_NODE
                        | plan_nodes::TPlanNodeType::LAKE_SCAN_NODE
                )
            }),
            "generate_series must not be emitted as a scan: {:?}",
            root.plan.nodes
        );
        let table_function = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::TABLE_FUNCTION_NODE)
            .and_then(|node| node.table_function_node.as_ref())
            .expect("table function node");
        assert_eq!(
            table_function.param_columns.as_ref().expect("params").len(),
            3
        );
        assert!(
            table_function
                .outer_columns
                .as_ref()
                .expect("outer columns")
                .is_empty()
        );
        assert_eq!(
            table_function
                .fn_result_columns
                .as_ref()
                .expect("result columns")
                .len(),
            1
        );
        let union = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::UNION_NODE)
            .and_then(|node| node.union_node.as_ref())
            .expect("parameter values node");
        assert_eq!(union.const_expr_lists.len(), 1);
        assert_eq!(union.const_expr_lists[0].len(), 3);
    }

    #[test]
    fn build_starrocks_scan_emits_lake_scan_with_internal_ranges() {
        let layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let plan = starrocks_scan_plan();
        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build");
        assert_eq!(build.fragment_results.len(), 1);
        let root = build.fragment_results.first().expect("root fragment");
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
            .expect("lake scan node");
        let lake = scan_node
            .lake_scan_node
            .as_ref()
            .expect("lake scan payload");
        let schema_key = lake.schema_key.as_ref().expect("schema_key");
        assert_eq!(schema_key.db_id, Some(11));
        assert_eq!(schema_key.table_id, Some(22));
        assert_eq!(schema_key.schema_id, Some(33));

        let tuple_desc = root
            .desc_tbl
            .tuple_descriptors
            .iter()
            .find(|tuple| tuple.id == Some(1))
            .expect("StarRocks scan tuple descriptor");
        assert_eq!(tuple_desc.table_id, Some(22));

        let table_descs = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors");
        let table_desc = table_descs
            .iter()
            .find(|table| table.id == 22)
            .expect("StarRocks table descriptor");
        assert_eq!(table_desc.db_name, "default");
        assert_eq!(table_desc.table_name, "starrocks_t");

        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&1)
            .expect("scan ranges");
        assert_eq!(ranges.len(), 1);
        let internal = ranges[0]
            .scan_range
            .internal_scan_range
            .as_ref()
            .expect("internal scan range");
        assert_eq!(internal.tablet_id, 101);
        assert_eq!(internal.partition_id, Some(201));
        assert_eq!(internal.version, "7");
        assert_eq!(internal.db_name, "default");
        assert_eq!(internal.table_name.as_deref(), Some("starrocks_t"));
    }

    #[test]
    fn iceberg_scan_without_starrocks_layout_uses_synthetic_descriptor_table_id() {
        let build = build_fragments_from_optimizer_for_database_for_test(
            &iceberg_scan_plan(),
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");
        assert_eq!(build.fragment_results.len(), 1);
        let root = build.fragment_results.first().expect("root fragment");
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let synthetic_table_id = synthetic_iceberg_table_id(scan_node.node_id);
        let tuple_desc = root
            .desc_tbl
            .tuple_descriptors
            .iter()
            .find(|tuple| tuple.id == Some(1))
            .expect("scan tuple descriptor");
        assert_eq!(tuple_desc.table_id, Some(synthetic_table_id));

        let table_desc = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors")
            .iter()
            .find(|table| table.id == synthetic_table_id)
            .expect("synthetic iceberg table descriptor");
        assert_eq!(
            table_desc.table_type,
            crate::thrift::types::TTableType::ICEBERG_TABLE
        );
        assert_eq!(
            table_desc
                .iceberg_table
                .as_ref()
                .and_then(|table| table.iceberg_schema.as_ref())
                .and_then(|schema| schema.fields.as_ref())
                .and_then(|fields| fields.first())
                .and_then(|field| field.field_id),
            Some(1)
        );
    }

    #[test]
    fn fragment_build_request_with_iceberg_sink_attaches_partition_metadata() {
        let plan = values_plan_for_test(vec![output_col_for_test(1, "id", DataType::Int32, false)]);
        let connectors = crate::connector::ConnectorRegistry::new();
        let mut spec = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::write_sink::test_support::single_bucket_partition_metadata_json(),
        );

        let build = build_fragments_with_iceberg_sink_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &connectors,
            "default",
            None,
            &spec,
        )
        .expect("build with iceberg sink");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert_eq!(
            root.output_sink.type_,
            data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
        );
        let iceberg_sink = root
            .output_sink
            .iceberg_table_sink
            .as_ref()
            .expect("iceberg sink payload");
        assert_eq!(iceberg_sink.target_table_id, Some(spec.target_table_id));

        let root_tuple_id = root
            .plan
            .nodes
            .first()
            .and_then(|node| node.row_tuples.first())
            .copied()
            .expect("root output tuple");
        assert_eq!(iceberg_sink.tuple_id, Some(root_tuple_id));
        let output_exprs = root.output_exprs.as_ref().expect("sink output exprs");
        assert_eq!(output_exprs.len(), spec.target_columns.len());
        assert_eq!(output_exprs[0].nodes.len(), 1);
        let expr_node = &output_exprs[0].nodes[0];
        assert_eq!(expr_node.node_type, exprs::TExprNodeType::SLOT_REF);
        let slot_ref = expr_node.slot_ref.as_ref().expect("slot ref");
        assert_eq!(slot_ref.tuple_id, root_tuple_id);

        let target_desc = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors")
            .iter()
            .find(|table| table.id == spec.target_table_id)
            .expect("target iceberg table descriptor");
        assert_eq!(
            target_desc.table_type,
            crate::thrift::types::TTableType::ICEBERG_TABLE
        );
        let partition_info = target_desc
            .iceberg_table
            .as_ref()
            .and_then(|table| table.partition_info.as_ref())
            .expect("target iceberg partition info");
        assert_eq!(partition_info.len(), 1);
        assert_eq!(partition_info[0].source_column_name.as_deref(), Some("id"));
        assert_eq!(
            partition_info[0].transform_expr.as_deref(),
            Some("bucket[16]")
        );
    }

    #[test]
    fn fragment_build_request_with_iceberg_sink_sets_root_output_sink() {
        let plan = values_plan_for_test(vec![output_col_for_test(1, "id", DataType::Int32, false)]);
        let connectors = crate::connector::ConnectorRegistry::new();
        let mut spec = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::write_sink::test_support::single_bucket_partition_metadata_json(),
        );

        let build = build_fragments_with_iceberg_sink_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &connectors,
            "default",
            None,
            &spec,
        )
        .expect("build via distributed plan with iceberg sink");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert_eq!(
            root.output_sink.type_,
            data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
        );
        let iceberg_sink = root
            .output_sink
            .iceberg_table_sink
            .as_ref()
            .expect("iceberg sink payload");
        assert_eq!(iceberg_sink.target_table_id, Some(spec.target_table_id));

        let root_tuple_id = root
            .plan
            .nodes
            .first()
            .and_then(|node| node.row_tuples.first())
            .copied()
            .expect("root output tuple");
        assert_eq!(iceberg_sink.tuple_id, Some(root_tuple_id));
        let output_exprs = root.output_exprs.as_ref().expect("sink output exprs");
        assert_eq!(output_exprs.len(), spec.target_columns.len());
        assert_eq!(output_exprs[0].nodes.len(), 1);
        let expr_node = &output_exprs[0].nodes[0];
        assert_eq!(expr_node.node_type, exprs::TExprNodeType::SLOT_REF);
        let slot_ref = expr_node.slot_ref.as_ref().expect("slot ref");
        assert_eq!(slot_ref.tuple_id, root_tuple_id);

        let target_desc = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors")
            .iter()
            .find(|table| table.id == spec.target_table_id)
            .expect("target iceberg table descriptor");
        assert_eq!(
            target_desc.table_type,
            crate::thrift::types::TTableType::ICEBERG_TABLE
        );
        let partition_info = target_desc
            .iceberg_table
            .as_ref()
            .and_then(|table| table.partition_info.as_ref())
            .expect("target iceberg partition info");
        assert_eq!(partition_info.len(), 1);
        assert_eq!(partition_info[0].source_column_name.as_deref(), Some("id"));
        assert_eq!(
            partition_info[0].transform_expr.as_deref(),
            Some("bucket[16]")
        );
    }

    #[test]
    fn fragment_build_request_with_iceberg_sink_preserves_delete_sink_mode() {
        let plan = values_plan_for_test(vec![output_col_for_test(1, "id", DataType::Int32, false)]);
        let connectors = crate::connector::ConnectorRegistry::new();
        let mut spec = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        spec.mode = crate::sql::planner::write_sink::IcebergWriteSinkMode::PositionDeletes;
        spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::write_sink::test_support::single_bucket_partition_metadata_json(),
        );

        let build = build_fragments_with_iceberg_sink_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &connectors,
            "default",
            None,
            &spec,
        )
        .expect("build with iceberg delete sink");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert_eq!(
            root.output_sink.type_,
            data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
        );
        assert!(root.output_sink.iceberg_table_sink.is_some());
    }

    #[test]
    fn fragment_build_request_with_iceberg_sink_preserves_equality_delete_schema() {
        let plan = values_plan_for_test(vec![output_col_for_test(1, "id", DataType::Int32, false)]);
        let connectors = crate::connector::ConnectorRegistry::new();
        let mut spec = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        spec.mode = crate::sql::planner::write_sink::IcebergWriteSinkMode::EqualityDeletes;
        spec.iceberg.serialized_metadata =
            Some(crate::sql::planner::write_sink::test_support::unpartitioned_metadata_json());

        let build = build_fragments_with_iceberg_sink_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &connectors,
            "default",
            None,
            &spec,
        )
        .expect("build with iceberg equality-delete sink");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert_eq!(
            root.output_sink.type_,
            data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK
        );
        let target_desc = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors")
            .iter()
            .find(|table| table.id == spec.target_table_id)
            .expect("target iceberg table descriptor");
        let equality_schema = target_desc
            .iceberg_table
            .as_ref()
            .and_then(|table| table.iceberg_equal_delete_schema.as_ref())
            .expect("equality-delete schema");
        let fields = equality_schema.fields.as_ref().expect("schema fields");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].field_id, Some(1));
        assert_eq!(fields[0].name.as_deref(), Some("id"));
    }

    #[test]
    fn fragment_build_request_with_iceberg_sink_maps_file_hash_distribution_to_partitioned_edge() {
        let file_col_id = ColumnId::new_for_test(2);
        let output_columns = vec![
            output_col_for_test(1, "id", DataType::Int32, false),
            output_col_for_test(2, "_file", DataType::Utf8, false),
        ];
        let values = values_plan_for_test(output_columns.clone());
        let plan = physical_node_for_test(
            Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::shuffle_agg([file_col_id]),
            }),
            vec![values],
            output_columns,
        );

        let connectors = crate::connector::ConnectorRegistry::new();
        let mut spec = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        spec.mode = crate::sql::planner::write_sink::IcebergWriteSinkMode::DeletionVectors;
        spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::write_sink::test_support::single_bucket_partition_metadata_json(),
        );
        let target_columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "_file".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
        ];
        spec.target_columns = target_columns.clone();
        spec.target_table.columns = target_columns;

        let build = build_fragments_with_iceberg_sink_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &connectors,
            "default",
            None,
            &spec,
        )
        .expect("build with iceberg DV sink");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert_eq!(
            root.output_sink.type_,
            data_sinks::TDataSinkType::ICEBERG_DV_SINK
        );
        let file_slot = slot_id_by_name(&root.desc_tbl, "_file");
        let edge = build.edges.first().expect("hash stream edge");
        assert_eq!(
            edge.stream_kind,
            crate::sql::codegen::FragmentStreamKind::Partitioned
        );
        assert_eq!(
            edge.output_partition.type_,
            crate::thrift::partitions::TPartitionType::HASH_PARTITIONED
        );
        let partition_exprs = edge
            .output_partition
            .partition_exprs
            .as_ref()
            .expect("partition exprs");
        assert_eq!(partition_exprs.len(), 1);
        let expr_node = partition_exprs[0]
            .nodes
            .first()
            .expect("partition expr node");
        assert_eq!(expr_node.node_type, exprs::TExprNodeType::SLOT_REF);
        let slot_ref = expr_node.slot_ref.as_ref().expect("slot ref");
        assert_eq!(slot_ref.slot_id, file_slot);
    }

    #[test]
    fn change_stream_single_branch_builds_router_and_writer_leg() {
        let plan = values_plan_for_test(vec![output_col_for_test(
            1,
            "delete_id",
            DataType::Int32,
            false,
        )]);
        let connectors = crate::connector::ConnectorRegistry::new();
        let mut dag = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            None,
            vec![ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![0])],
        );

        let build = build_fragments_with_change_stream_write_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &connectors,
            "default",
            None,
            &mut dag,
        )
        .expect("build change stream single branch");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert_eq!(
            root.output_sink.type_,
            data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK
        );
        let router = root
            .output_sink
            .iceberg_change_stream_router_sink
            .as_ref()
            .expect("router sink");
        assert_eq!(router.branches.len(), 1);
        assert_eq!(
            router.branches[0].stream_sink.output_columns.as_deref(),
            Some(&[1][..])
        );

        let router_edges = build
            .edges
            .iter()
            .filter(|edge| {
                matches!(
                    edge.edge_kind,
                    FragmentEdgeKind::IcebergChangeStreamRouter { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(router_edges.len(), 1);
        let writer = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == router_edges[0].target_fragment_id)
            .expect("writer fragment");
        assert_eq!(
            writer.output_sink.type_,
            data_sinks::TDataSinkType::ICEBERG_DV_SINK
        );
    }

    #[test]
    fn change_stream_multi_branch_builds_router_and_writer_legs() {
        let output_columns = vec![
            output_col_for_test(1, "__change_op", DataType::Int32, false),
            output_col_for_test(2, "data_route", DataType::Int32, true),
            output_col_for_test(3, "delete_id", DataType::Int32, false),
            output_col_for_test(4, "reuse_id", DataType::Int32, false),
            output_col_for_test(5, "fresh_id", DataType::Int32, false),
        ];
        let plan = values_plan_for_test(output_columns);
        let connectors = crate::connector::ConnectorRegistry::new();
        let mut dag = ChangeStreamWriteDagSpec::for_test(
            Some(0),
            Some(1),
            vec![
                ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![2]),
                ChangeStreamWriteBranchSpec::reuse_data_for_test(vec![3]),
                ChangeStreamWriteBranchSpec::fresh_data_for_test(vec![4]),
            ],
        );

        let build = build_fragments_with_change_stream_write_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &connectors,
            "default",
            None,
            &mut dag,
        )
        .expect("build change stream multi branch");

        let source = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("source fragment");
        assert_eq!(
            source.output_sink.type_,
            data_sinks::TDataSinkType::ICEBERG_CHANGE_STREAM_ROUTER_SINK
        );
        assert!(source.output_exprs.is_none());
        let source_root_tuple_id = source
            .plan
            .nodes
            .first()
            .and_then(|node| node.row_tuples.first())
            .copied()
            .expect("source root tuple");
        let router = source
            .output_sink
            .iceberg_change_stream_router_sink
            .as_ref()
            .expect("router sink");
        assert_eq!(router.change_op_slot_id, 1);
        assert_eq!(router.data_route_slot_id, Some(2));
        assert_eq!(router.branches.len(), 3);
        assert_eq!(
            router.branches[0].stream_sink.output_columns.as_deref(),
            Some(&[3][..])
        );
        assert_eq!(
            router.branches[1].stream_sink.output_columns.as_deref(),
            Some(&[4][..])
        );
        assert_eq!(
            router.branches[2].stream_sink.output_columns.as_deref(),
            Some(&[5][..])
        );
        assert!(
            router
                .branches
                .iter()
                .all(|branch| branch.destinations.is_empty())
        );

        let router_edges = build
            .edges
            .iter()
            .filter(|edge| {
                matches!(
                    edge.edge_kind,
                    FragmentEdgeKind::IcebergChangeStreamRouter { .. }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(router_edges.len(), 3);
        let mut sink_table_ids = build
            .fragment_results
            .iter()
            .filter_map(|fragment| {
                fragment
                    .output_sink
                    .iceberg_table_sink
                    .as_ref()
                    .and_then(|sink| sink.target_table_id)
            })
            .collect::<Vec<_>>();
        sink_table_ids.sort_unstable();
        sink_table_ids.dedup();
        assert_eq!(
            sink_table_ids.len(),
            dag.branches.len(),
            "each change-stream write branch needs an independent table descriptor"
        );

        for branch in &dag.branches {
            let edge = router_edges
                .iter()
                .find(|edge| {
                    matches!(
                        edge.edge_kind,
                        FragmentEdgeKind::IcebergChangeStreamRouter { branch_id, .. }
                            if branch_id == branch.branch_id
                    )
                })
                .expect("router edge for branch");
            let writer_fragment_id = edge.target_fragment_id;
            let writer = build
                .fragment_results
                .iter()
                .find(|fragment| fragment.fragment_id == writer_fragment_id)
                .expect("writer fragment");
            assert_eq!(edge.source_fragment_id, build.root_fragment_id);
            assert_eq!(
                edge.output_partition.type_,
                crate::thrift::partitions::TPartitionType::UNPARTITIONED
            );
            assert_eq!(writer.plan.nodes.len(), 1);
            let exchange = writer.plan.nodes.first().expect("writer exchange");
            assert_eq!(exchange.node_type, plan_nodes::TPlanNodeType::EXCHANGE_NODE);
            assert_eq!(exchange.node_id, edge.target_exchange_node_id);
            let writer_table_id = writer
                .output_sink
                .iceberg_table_sink
                .as_ref()
                .expect("writer iceberg sink")
                .target_table_id
                .expect("writer target table id");
            assert!(sink_table_ids.contains(&writer_table_id));
            assert_eq!(exchange.row_tuples.len(), 1);
            let writer_tuple_id = exchange.row_tuples[0];
            assert_ne!(
                writer_tuple_id, source_root_tuple_id,
                "writer leg must use its own narrowed tuple"
            );
            assert_eq!(
                writer.output_columns.len(),
                branch.stream_output_ordinals.len()
            );
            let output_exprs = writer.output_exprs.as_ref().expect("writer output exprs");
            assert_eq!(output_exprs.len(), branch.stream_output_ordinals.len());
            let router_branch = router
                .branches
                .iter()
                .find(|router_branch| router_branch.branch_id == branch.branch_id)
                .expect("router branch");
            let source_output_slots = router_branch
                .stream_sink
                .output_columns
                .as_deref()
                .unwrap_or(&[]);
            for expr in output_exprs {
                let node = expr.nodes.first().expect("slot ref node");
                assert_eq!(node.node_type, exprs::TExprNodeType::SLOT_REF);
                let slot_ref = node.slot_ref.as_ref().expect("slot ref");
                assert_eq!(slot_ref.tuple_id, writer_tuple_id);
                assert!(
                    !source_output_slots.contains(&slot_ref.slot_id),
                    "writer output must reference writer slots, not source slots"
                );
            }
        }
    }

    #[test]
    fn mixed_starrocks_and_iceberg_scan_table_ids_do_not_collide() {
        let starrocks_layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let registry = mock_starrocks_and_iceberg_registry(&starrocks_layout);
        let catalog = MixedCatalog { starrocks_layout };

        let build = build_fragments_from_optimizer_for_database_for_test(
            &mixed_starrocks_iceberg_join_plan(),
            &catalog,
            &registry,
            "default",
        )
        .expect("build");
        let root = build.fragment_results.first().expect("root fragment");
        let tuple_descs = &root.desc_tbl.tuple_descriptors;
        let iceberg_table_id = tuple_descs
            .iter()
            .find(|tuple| tuple.id == Some(1))
            .and_then(|tuple| tuple.table_id)
            .expect("iceberg tuple table id");
        let starrocks_table_id = tuple_descs
            .iter()
            .find(|tuple| tuple.id == Some(2))
            .and_then(|tuple| tuple.table_id)
            .expect("StarRocks tuple table id");
        assert_ne!(iceberg_table_id, starrocks_table_id);
        assert_eq!(starrocks_table_id, 22);

        let table_descs = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors");
        let iceberg_desc = table_descs
            .iter()
            .find(|table| table.id == iceberg_table_id)
            .expect("iceberg table descriptor");
        assert_eq!(
            iceberg_desc.table_type,
            crate::thrift::types::TTableType::ICEBERG_TABLE
        );
        let starrocks_desc = table_descs
            .iter()
            .find(|table| table.id == starrocks_table_id)
            .expect("StarRocks table descriptor");
        assert_eq!(
            starrocks_desc.table_type,
            crate::thrift::types::TTableType::OLAP_TABLE
        );
    }

    // -------------------------------------------------------------------
    // Task 6: codegen dictionary plan interface
    // -------------------------------------------------------------------

    fn dict_snapshot_a_b() -> std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot> {
        use crate::engine::dictionary::model::{
            DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue,
            DictionaryWatermark,
        };
        std::sync::Arc::new(DictionarySnapshot {
            dictionary_id: 1,
            owner: DictionaryOwner::StarRocksTable {
                database: "default".to_string(),
                table: "starrocks_t".to_string(),
                db_id: 11,
                table_id: 22,
            },
            column_id: None,
            column_name: "id".to_string(),
            data_type: DataType::Int32,
            version: 1,
            watermark: DictionaryWatermark::StarRocks {
                schema_id: 33,
                tablets: vec![],
            },
            values: vec![
                DictionaryValue {
                    id: 1,
                    bytes: b"a".to_vec(),
                },
                DictionaryValue {
                    id: 2,
                    bytes: b"b".to_vec(),
                },
            ],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        })
    }

    fn dict_snapshot_x_y_z() -> std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot>
    {
        use crate::engine::dictionary::model::{
            DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue,
            DictionaryWatermark,
        };
        std::sync::Arc::new(DictionarySnapshot {
            dictionary_id: 2,
            owner: DictionaryOwner::StarRocksTable {
                database: "default".to_string(),
                table: "starrocks_t".to_string(),
                db_id: 11,
                table_id: 22,
            },
            column_id: None,
            column_name: "name".to_string(),
            data_type: DataType::Int32,
            version: 3,
            watermark: DictionaryWatermark::StarRocks {
                schema_id: 33,
                tablets: vec![],
            },
            values: vec![
                DictionaryValue {
                    id: 10,
                    bytes: b"x".to_vec(),
                },
                DictionaryValue {
                    id: 11,
                    bytes: b"y".to_vec(),
                },
                DictionaryValue {
                    id: 12,
                    bytes: b"z".to_vec(),
                },
            ],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        })
    }

    /// Look up the slot id of a slot by its column name in `desc_tbl`.
    /// Panics if no such slot exists — the caller is asserting that the
    /// builder produced a slot with the expected name.
    fn slot_id_by_name(
        desc_tbl: &crate::thrift::descriptors::TDescriptorTable,
        column_name: &str,
    ) -> i32 {
        slot_id_by_name_opt(desc_tbl, column_name)
            .unwrap_or_else(|| panic!("no slot named `{}` in desc_tbl", column_name))
    }

    /// Optional variant for tests that need to assert ABSENCE of a slot
    /// (e.g. Bug B regression: a dict-rewritten scan must NOT emit a
    /// separate source-string slot alongside its dict slot).
    fn slot_id_by_name_opt(
        desc_tbl: &crate::thrift::descriptors::TDescriptorTable,
        column_name: &str,
    ) -> Option<i32> {
        let slots = desc_tbl.slot_descriptors.as_ref()?;
        for slot in slots {
            if slot.col_name.as_deref() == Some(column_name) {
                return slot.id;
            }
        }
        None
    }

    fn starrocks_layout() -> PhysicalTableLayout {
        PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        }
    }

    #[test]
    fn physical_decode_emits_decode_node() {
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::operator::DecodeOp;
        use crate::sql::planner::plan::DecodeMapping;

        let id_col = ColumnId::new_for_test(7001);
        // Build a StarRocks scan that exposes one dict column ("id" string
        // column gets a sibling "id_dict" INT slot via dict_columns).
        let layout = starrocks_layout();
        let scan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "starrocks_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 11,
                        table_id: 22,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: vec![OutputColumn {
                    column_id: id_col,
                    name: "id".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                }],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![ScanDictionaryColumn {
                    source_column: "id".to_string(),
                    dict_column: "id_dict".to_string(),
                    dictionary: dict_snapshot_a_b(),
                }],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![OutputColumn {
                column_id: id_col,
                name: "id".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });

        let decode_plan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalDecode(DecodeOp {
                mappings: vec![DecodeMapping {
                    source_column_id: id_col,
                    output_column_id: id_col,
                    dict_column: "id_dict".to_string(),
                    string_column: "id".to_string(),
                }],
                output_columns: vec![OutputColumn {
                    column_id: id_col,
                    name: "id".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            children: vec![scan],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![OutputColumn {
                column_id: id_col,
                name: "id".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });

        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };
        let build = build_fragments_from_optimizer_for_database_for_test(
            &decode_plan,
            &catalog,
            &registry,
            "default",
        )
        .expect("build");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");

        // The decode allocates a NEW tuple with a NEW Utf8 slot named
        // `id` (the decoded output). Under the Bug B fix the scan tuple
        // also holds a slot named `id` (single-slot-per-column contract)
        // but typed Int32 — the dict-encoded payload. The
        // `dict_id_to_string_ids` mapping pairs the scan's `id` slot id
        // with the decode tuple's new string slot id.
        let desc_tbl = &root.desc_tbl;
        let slots = desc_tbl
            .slot_descriptors
            .as_ref()
            .expect("slot_descriptors");
        let tuples = &desc_tbl.tuple_descriptors;
        assert_eq!(tuples.len(), 2, "expected scan tuple + decode tuple");
        let scan_tuple_id = tuples[0].id.expect("scan tuple id");
        let decode_tuple_id = tuples[1].id.expect("decode tuple id");
        let scan_dict_slot = slots
            .iter()
            .find(|s| s.parent == Some(scan_tuple_id) && s.col_name.as_deref() == Some("id"))
            .and_then(|s| s.id)
            .expect("scan dict slot named after source `id`");
        let decode_string_slot = slots
            .iter()
            .find(|s| s.parent == Some(decode_tuple_id) && s.col_name.as_deref() == Some("id"))
            .and_then(|s| s.id)
            .expect("decode id slot");
        assert_ne!(
            scan_dict_slot, decode_string_slot,
            "scan dict slot (Int32) and decode string slot (Utf8) must be distinct"
        );

        // First plan node is the decode node (pre-order).
        let first = root.plan.nodes.first().expect("decode plan node");
        assert_eq!(first.node_type, plan_nodes::TPlanNodeType::DECODE_NODE);
        assert_eq!(
            first.row_tuples,
            vec![decode_tuple_id],
            "decode row_tuples must reference the new decode tuple"
        );
        let decode = first.decode_node.as_ref().expect("decode payload");
        let mapping = decode
            .dict_id_to_string_ids
            .as_ref()
            .expect("dict_id_to_string_ids");
        assert_eq!(mapping.len(), 1);
        let (dict_slot, string_slot) = mapping.iter().next().expect("one entry");
        assert_eq!(
            *dict_slot, scan_dict_slot,
            "decode mapping key must be the scan's dict slot id"
        );
        assert_eq!(
            *string_slot, decode_string_slot,
            "decode mapping value must be the decode tuple's new string slot id"
        );
    }

    #[test]
    fn scan_dict_column_emits_query_global_dict() {
        let layout = starrocks_layout();
        let plan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "starrocks_t".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Utf8,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "name".to_string(),
                            data_type: DataType::Utf8,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 11,
                        table_id: 22,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: vec![
                    output_col_for_test(8101, "id", DataType::Utf8, false),
                    output_col_for_test(8102, "name", DataType::Utf8, false),
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![
                    ScanDictionaryColumn {
                        source_column: "id".to_string(),
                        dict_column: "id_dict".to_string(),
                        dictionary: dict_snapshot_a_b(),
                    },
                    ScanDictionaryColumn {
                        source_column: "name".to_string(),
                        dict_column: "name_dict".to_string(),
                        dictionary: dict_snapshot_x_y_z(),
                    },
                ],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![
                output_col_for_test(8101, "id", DataType::Utf8, false),
                output_col_for_test(8102, "name", DataType::Utf8, false),
            ],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });

        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };
        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");

        // Bug B regression: the scan must emit exactly ONE slot per dict
        // column. The slot keeps the SOURCE column's name (so the lake
        // scan finds the storage column by name) but its declared type
        // is Int32 (the BE encodes string -> dict id at read time using
        // the per-slot TGlobalDict). Emitting BOTH a source string slot
        // AND a separate dict int slot would let the BE's
        // `lake_scan.rs::dict_int_to_string` swap collapse them onto the
        // same storage slot id, producing `duplicate slot id <N> in
        // chunk schema contract` at runtime.
        let id_slot = slot_id_by_name(&root.desc_tbl, "id");
        let name_slot = slot_id_by_name(&root.desc_tbl, "name");
        assert_ne!(id_slot, name_slot, "scan slots must be distinct");
        // The dict_column NAMES (`id_dict`, `name_dict`) must NOT appear
        // as slot descriptor `col_name`s. The dict_column lives only in
        // the FE codegen scope as an alias for the source slot — the BE
        // never sees a column named `id_dict` in the tablet schema.
        assert!(
            slot_id_by_name_opt(&root.desc_tbl, "id_dict").is_none(),
            "dict_column name must not surface as a slot descriptor col_name"
        );
        assert!(
            slot_id_by_name_opt(&root.desc_tbl, "name_dict").is_none(),
            "dict_column name must not surface as a slot descriptor col_name"
        );
        // The dict slot type is Int32 (so the BE knows to encode).
        let id_slot_desc = root
            .desc_tbl
            .slot_descriptors
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .find(|s| s.id == Some(id_slot))
            .expect("id slot desc");
        assert_eq!(
            id_slot_desc
                .slot_type
                .as_ref()
                .and_then(|t| t.types.as_ref())
                .and_then(|tys| tys.first())
                .and_then(|tn| tn.scalar_type.as_ref())
                .map(|st| st.type_),
            Some(crate::thrift::types::TPrimitiveType::INT),
            "dict slot type must be INT (Int32) — see build_scan_schema_for_global_dict_encoding"
        );
        // The tuple itself should contain exactly the two dict slots.
        let scan_tuple_id = root
            .desc_tbl
            .tuple_descriptors
            .first()
            .and_then(|t| t.id)
            .expect("scan tuple id");
        let scan_slots: Vec<i32> = root
            .desc_tbl
            .slot_descriptors
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|s| s.parent == Some(scan_tuple_id))
            .filter_map(|s| s.id)
            .collect();
        assert_eq!(
            scan_slots.len(),
            2,
            "scan tuple must contain exactly two slots (one per dict column), got {scan_slots:?}"
        );

        // The fragment should carry two TGlobalDicts, one per source column.
        let dicts = root
            .query_global_dicts
            .as_ref()
            .expect("query_global_dicts populated");
        assert!(
            dicts.len() >= 2,
            "at least one TGlobalDict per source column; got {}",
            dicts.len()
        );

        // Match each TGlobalDict back to its slot id and check payload.
        let id_dict = dicts
            .iter()
            .find(|d| d.column_id == Some(id_slot))
            .expect("TGlobalDict for id slot");
        assert_eq!(id_dict.ids.as_deref(), Some(&[1, 2][..]));
        assert_eq!(
            id_dict.strings.as_deref(),
            Some(&[b"a".to_vec(), b"b".to_vec()][..])
        );
        let name_dict = dicts
            .iter()
            .find(|d| d.column_id == Some(name_slot))
            .expect("TGlobalDict for name slot");
        assert_eq!(name_dict.ids.as_deref(), Some(&[10, 11, 12][..]));
        assert_eq!(
            name_dict.strings.as_deref(),
            Some(&[b"x".to_vec(), b"y".to_vec(), b"z".to_vec()][..])
        );
        // Distinct column_ids on the two TGlobalDicts.
        assert_ne!(id_dict.column_id, name_dict.column_id);

        // StarRocks scan's TLakeScanNode carries the
        // `dict_string_id_to_int_ids` map. Under the Bug B fix this is a
        // SELF-map (slot -> same slot): the BE's layout rewrite at
        // `src/lower/node/lake_scan.rs` replaces every dict int slot
        // with its mapped string slot, and with the dict slot now
        // occupying the storage column's tuple position the self-map
        // keeps the layout swap a no-op — which is exactly what avoids
        // the duplicate-slot-id error.
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
            .expect("lake scan node");
        let lake = scan_node
            .lake_scan_node
            .as_ref()
            .expect("lake scan payload");
        let mapping = lake
            .dict_string_id_to_int_ids
            .as_ref()
            .expect("dict_string_id_to_int_ids populated");
        assert_eq!(mapping.len(), 2);
        assert_eq!(
            mapping.get(&id_slot).copied(),
            Some(id_slot),
            "id slot must self-map"
        );
        assert_eq!(
            mapping.get(&name_slot).copied(),
            Some(name_slot),
            "name slot must self-map"
        );
    }

    #[test]
    fn scan_emits_single_slot_per_dict_column() {
        // Direct Bug B regression: build a single-column StarRocks scan
        // where the rewriter has produced a `ScanDictionaryColumn` for
        // `s` and renamed the OutputColumn to `__nr_dict_t_s` (Int32).
        // Mirrors the post-rewriter shape that the FE actually emits
        // after `rewrite_scan`. The scan tuple must contain exactly one
        // slot: a single Int32 slot named after the SOURCE column `s`
        // (so the BE lake scan finds the storage column by name) with
        // the dict_column name (`__nr_dict_t_s`) registered as a scope
        // alias for the same slot. The LakeScanNode's
        // `dict_string_id_to_int_ids` must self-map that slot id, so
        // the BE layout swap is a no-op rather than collapsing two
        // distinct slots onto one storage slot id.
        let layout = starrocks_layout();
        let plan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![ColumnDef {
                        name: "s".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 11,
                        table_id: 22,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: vec![output_col_for_test(
                    8301,
                    "__nr_dict_t_s",
                    DataType::Int32,
                    false,
                )],
                predicates: vec![],
                required_columns: Some(vec!["__nr_dict_t_s".to_string()]),
                dict_columns: vec![ScanDictionaryColumn {
                    source_column: "s".to_string(),
                    dict_column: "__nr_dict_t_s".to_string(),
                    dictionary: dict_snapshot_a_b(),
                }],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![output_col_for_test(
                8301,
                "__nr_dict_t_s",
                DataType::Int32,
                false,
            )],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });

        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };
        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");

        // The scan tuple must contain exactly one slot: the dict slot.
        let scan_tuple_id = root
            .desc_tbl
            .tuple_descriptors
            .first()
            .and_then(|t| t.id)
            .expect("scan tuple id");
        let scan_slots: Vec<&crate::thrift::descriptors::TSlotDescriptor> = root
            .desc_tbl
            .slot_descriptors
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|s| s.parent == Some(scan_tuple_id))
            .collect();
        assert_eq!(
            scan_slots.len(),
            1,
            "scan tuple must contain exactly the dict slot, got {} slots",
            scan_slots.len()
        );
        let dict_slot = &scan_slots[0];
        // The slot keeps the SOURCE column's name `s` so the BE finds
        // the storage column in the tablet schema by name. The dict
        // column name lives only in the FE codegen scope.
        assert_eq!(dict_slot.col_name.as_deref(), Some("s"));
        assert_eq!(
            dict_slot
                .slot_type
                .as_ref()
                .and_then(|t| t.types.as_ref())
                .and_then(|tys| tys.first())
                .and_then(|tn| tn.scalar_type.as_ref())
                .map(|st| st.type_),
            Some(crate::thrift::types::TPrimitiveType::INT),
            "dict slot type must be INT (Int32)"
        );
        let dict_slot_id = dict_slot.id.expect("dict slot id");

        // LakeScanNode's dict_string_id_to_int_ids must self-map
        // (dict_slot -> dict_slot). The BE swaps each int slot with its
        // mapped string slot in the layout; with the dict slot at the
        // source column's storage position, the self-map keeps the
        // layout one slot wide, avoiding the duplicate-slot-id error.
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
            .expect("lake scan node");
        let lake = scan_node
            .lake_scan_node
            .as_ref()
            .expect("lake scan payload");
        let mapping = lake
            .dict_string_id_to_int_ids
            .as_ref()
            .expect("dict_string_id_to_int_ids populated");
        assert_eq!(mapping.len(), 1, "exactly one dict slot mapping");
        assert_eq!(
            mapping.get(&dict_slot_id).copied(),
            Some(dict_slot_id),
            "dict slot must self-map (FE emits single slot per dict column)"
        );
    }

    #[test]
    fn scan_dict_column_on_iceberg_scan_is_supported() {
        use crate::sql::catalog::{IcebergSchemaDef, IcebergTableInfo};

        // Build an Iceberg scan (non-StarRocks ScanSource) carrying a
        // dict_columns entry. With Option A landed, iceberg/HDFS scans now
        // support dict_columns: the dicts flow via query_global_dicts in
        // lowering rather than via TLakeScanNode.dict_string_id_to_int_ids.
        // visit_scan must succeed (the thrift node is left untouched).
        let iceberg_table_info = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 0,
            location: "s3://b/t".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let plan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: iceberg_table_info,
                        files: vec![],
                        cloud_properties: std::collections::BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: vec![output_col_for_test(8401, "id", DataType::Utf8, false)],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![ScanDictionaryColumn {
                    source_column: "id".to_string(),
                    dict_column: "id_dict".to_string(),
                    dictionary: dict_snapshot_a_b(),
                }],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![output_col_for_test(8401, "id", DataType::Utf8, false)],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });

        // Use an iceberg-only catalog (returns None for physical_layout) so
        // codegen routes the scan through the HDFS-style scan node instead of
        // the StarRocks lake scan path. visit_scan must now succeed: the HDFS
        // node is left untouched and the dict flows via query_global_dicts.
        struct IcebergCatalog;
        impl CatalogProvider for IcebergCatalog {
            fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
                Err("not used".to_string())
            }
            fn get_physical_layout(
                &self,
                _database: &str,
                _table: &str,
            ) -> Result<Option<PhysicalTableLayout>, String> {
                Ok(None)
            }
        }
        let catalog = IcebergCatalog;
        build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &catalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("iceberg scan with dict_columns must now succeed (Option A)");
    }

    #[test]
    fn iceberg_scan_slots_carry_schema_field_ids() {
        let plan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_schema(vec![IcebergSchemaFieldDef {
                            field_id: 10,
                            name: "id".to_string(),
                            initial_default: None,
                            write_default: None,
                            initial_default_json: None,
                            children: vec![],
                        }]),
                        files: vec![],
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: vec![output_col_for_test(8402, "id", DataType::Int32, false)],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![output_col_for_test(8402, "id", DataType::Int32, false)],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build Iceberg scan");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let slot = root
            .desc_tbl
            .slot_descriptors
            .as_ref()
            .expect("slot descriptors")
            .iter()
            .find(|slot| slot.col_name.as_deref() == Some("id"))
            .expect("id slot descriptor");

        assert_eq!(slot.col_unique_id, Some(10));
    }

    #[test]
    fn starrocks_fragment_exec_params_are_generated_from_planned_connector_scan() {
        let layout = starrocks_layout();
        let plan = starrocks_scan_plan();
        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build StarRocks fragment");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let exec_params = &root.exec_params;
        let per_node = &exec_params.per_node_scan_ranges;
        let ranges = per_node
            .values()
            .next()
            .expect("one scan node should have ranges");

        assert_eq!(ranges.len(), 1);
        let tablet_ids = ranges
            .iter()
            .map(|range| {
                range
                    .scan_range
                    .internal_scan_range
                    .as_ref()
                    .map(|internal| internal.tablet_id)
                    .expect("internal scan range")
            })
            .collect::<Vec<_>>();
        assert_eq!(tablet_ids, vec![101]);
    }

    #[test]
    fn ir_scan_lowering_calls_connector_begin_scan_and_plan_splits_for_starrocks() {
        use crate::connector::starrocks::table::scan_planner::StarRocksSplit;
        let layout = starrocks_layout();
        let plan = with_id_predicate(starrocks_scan_plan(), 7);
        let catalog = StarRocksCatalog {
            layout: layout.clone(),
        };

        let splits: Vec<StarRocksSplit> = layout
            .tablets
            .iter()
            .map(|tablet| StarRocksSplit {
                tablet_id: tablet.tablet_id,
                partition_id: tablet.partition_id,
                version: tablet.version,
            })
            .collect();
        let counts = std::sync::Arc::new(ScanPlannerCallCounts::default());
        let planner = std::sync::Arc::new(CountingScanPlanner {
            inner: MockScanPlanner {
                schema_id: layout.schema_id,
                splits,
            },
            counts: counts.clone(),
        });
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(planner);

        let built = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build StarRocks fragment");

        assert_eq!(
            counts.begin_scan.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "begin_scan must be invoked exactly once for the StarRocks scan"
        );
        assert_eq!(
            counts.plan_splits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "plan_splits must be invoked exactly once for the StarRocks scan"
        );
        let root = built
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == built.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
            .expect("lake scan node");
        assert_eq!(scan.row_tuples.len(), 1, "scan node must carry one tuple");
        assert!(
            scan.conjuncts
                .as_ref()
                .is_some_and(|exprs| !exprs.is_empty()),
            "scan node emission must carry pushed predicates"
        );
        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges must be keyed by the emitted scan node id");
        assert_eq!(
            ranges.len(),
            layout.tablets.len(),
            "range emission must use the planned connector splits"
        );
    }

    #[test]
    fn ir_scan_lowering_calls_connector_begin_scan_and_plan_splits_for_iceberg() {
        let plan = with_id_predicate(iceberg_scan_plan(), 7);
        let catalog = DummyCatalog;

        let counts = std::sync::Arc::new(ScanPlannerCallCounts::default());
        let planner = std::sync::Arc::new(CurrentSnapshotAssertingIcebergPlanner {
            counts: counts.clone(),
            files: vec![iceberg_i32_file("s3://bucket/current.parquet", 1, 10)],
        });
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(planner);

        let built = build_fragments_from_optimizer_for_database_for_test(
            &plan, &catalog, &registry, "default",
        )
        .expect("build Iceberg fragment");

        assert_eq!(
            counts.begin_scan.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "begin_scan must be invoked exactly once for the Iceberg scan"
        );
        assert_eq!(
            counts.plan_splits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "plan_splits must be invoked exactly once for the Iceberg scan"
        );
        let root = built
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == built.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        assert_eq!(scan.row_tuples.len(), 1, "scan node must carry one tuple");
        assert!(
            scan.conjuncts
                .as_ref()
                .is_some_and(|exprs| !exprs.is_empty()),
            "scan node emission must carry pushed predicates"
        );
        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges must be keyed by the emitted scan node id");
        assert_eq!(
            ranges.len(),
            1,
            "range emission must use the planned connector splits"
        );
    }

    #[test]
    fn ir_scan_lowering_emits_variant_path_columns_for_iceberg() {
        let source_column_id = ColumnId::new_for_test(9301);
        let synthetic_column_id = ColumnId::new_for_test(9302);
        let source_column = OutputColumn {
            column_id: source_column_id,
            name: "v".to_string(),
            data_type: DataType::LargeBinary,
            nullable: true,
            is_internal: false,
        };
        let synthetic_column = OutputColumn {
            column_id: synthetic_column_id,
            name: "__nr_var_v_0".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: true,
        };
        let plan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalScan(ScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Int32,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "v".to_string(),
                            data_type: DataType::LargeBinary,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_schema(vec![
                            IcebergSchemaFieldDef {
                                field_id: 1,
                                name: "id".to_string(),
                                initial_default: None,
                                write_default: None,
                                initial_default_json: None,
                                children: vec![],
                            },
                            IcebergSchemaFieldDef {
                                field_id: 2,
                                name: "v".to_string(),
                                initial_default: None,
                                write_default: None,
                                initial_default_json: None,
                                children: vec![],
                            },
                        ]),
                        files: vec![],
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: vec![source_column.clone(), synthetic_column.clone()],
                predicates: vec![],
                required_columns: Some(vec![synthetic_column.name.clone()]),
                dict_columns: vec![],
                variant_columns: vec![ScanVariantColumn {
                    source_column_id,
                    source_column: source_column.name.clone(),
                    synthetic_column_id,
                    synthetic_column: synthetic_column.name.clone(),
                    canonical_path: "$.a.b".to_string(),
                    requested_type: DataType::Int64,
                    strict: true,
                }],
                mv_rewritten_from: None,
            }),
            children: vec![],
            stats: stats(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![synthetic_column.clone()],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });
        let registry = mock_iceberg_registry();

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &registry,
            "default",
        )
        .expect("build Iceberg scan");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let hdfs = scan.hdfs_scan_node.as_ref().expect("hdfs scan payload");

        let source_slot_id = slot_id_by_name(&root.desc_tbl, "v");
        let synthetic_slot_id = slot_id_by_name(&root.desc_tbl, &synthetic_column.name);
        let variant_columns = hdfs
            .variant_path_columns
            .as_ref()
            .expect("variant path columns");
        assert_eq!(variant_columns.len(), 1);
        let variant_column = &variant_columns[0];
        assert_eq!(variant_column.source_slot_id, Some(source_slot_id));
        assert_eq!(variant_column.output_slot_id, Some(synthetic_slot_id));
        assert_eq!(variant_column.source_column.as_deref(), Some("v"));
        assert_eq!(
            variant_column.output_column.as_deref(),
            Some(synthetic_column.name.as_str())
        );
        assert_eq!(variant_column.canonical_path.as_deref(), Some("$.a.b"));
        assert_eq!(variant_column.strict, Some(true));
        assert_eq!(
            variant_column
                .requested_type
                .as_ref()
                .and_then(|desc| desc.types.as_ref())
                .and_then(|nodes| nodes.first())
                .and_then(|node| node.scalar_type.as_ref())
                .map(|scalar| scalar.type_),
            Some(crate::thrift::types::TPrimitiveType::BIGINT)
        );

        let output_exprs = root.output_exprs.as_ref().expect("root output exprs");
        assert_eq!(output_exprs.len(), 1);
        let slot_ref = output_exprs[0]
            .nodes
            .first()
            .and_then(|node| node.slot_ref.as_ref())
            .expect("synthetic root output slot ref");
        assert_eq!(slot_ref.slot_id, synthetic_slot_id);

        let hive_column_names = hdfs.hive_column_names.as_deref().unwrap_or(&[]);
        assert!(
            !hive_column_names
                .iter()
                .any(|name| name == &synthetic_column.name),
            "synthetic column must not be part of physical hive_column_names"
        );
    }

    #[test]
    fn ir_scan_lowering_uses_current_snapshot_handle_for_ordinary_iceberg_scan() {
        let mut plan = iceberg_scan_plan();
        if let Operator::PhysicalScan(scan) = &mut plan.op {
            let ScanSource::IcebergDataFiles { files, .. } = &mut scan.table.source else {
                panic!("expected iceberg source");
            };
            files.push(iceberg_i32_file(
                "s3://bucket/stale-registered.parquet",
                1,
                1,
            ));
        }
        let catalog = DummyCatalog;
        let counts = std::sync::Arc::new(ScanPlannerCallCounts::default());
        let planner = std::sync::Arc::new(CurrentSnapshotAssertingIcebergPlanner {
            counts: counts.clone(),
            files: vec![iceberg_i32_file("s3://bucket/current.parquet", 1, 1)],
        });
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(planner);

        build_fragments_from_optimizer_for_database_for_test(&plan, &catalog, &registry, "default")
            .expect("build Iceberg fragment");

        assert_eq!(
            counts.begin_scan.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[test]
    fn refresh_derived_iceberg_scan_handle_uses_explicit_files() {
        fn assert_explicit_file_path(
            table_handle: crate::connector::scan_planning::TableHandle,
            expected_path: &str,
        ) {
            let table_handle = table_handle
                .downcast_ref::<crate::connector::iceberg::scan_planner::IcebergTableHandle>()
                .expect("IcebergTableHandle");
            let crate::connector::iceberg::scan_planner::IcebergSplitSource::ExplicitFiles(files) =
                &table_handle.split_source
            else {
                panic!("refresh-derived Iceberg scans must preserve explicit files");
            };
            assert_eq!(files.len(), 1);
            assert_eq!(files[0].path, expected_path);
        }

        let iceberg_table = test_iceberg_table_info_with_id_schema();
        let original_source = ScanSource::IcebergVersionTable {
            table: iceberg_table.clone(),
            snapshot_id: 42,
        };

        let table_handle = iceberg_scan_table_handle_for_codegen(
            &original_source,
            &iceberg_table,
            vec![iceberg_i32_file("s3://bucket/pinned-refresh.parquet", 1, 1)],
            vec!["id".to_string()],
        );
        assert_explicit_file_path(table_handle, "s3://bucket/pinned-refresh.parquet");

        let target_state_source =
            ScanSource::IcebergMvTargetState(crate::sql::catalog::IcebergMvTargetStateScan {
                catalog: iceberg_table.catalog.clone(),
                database: iceberg_table.namespace.clone(),
                table: iceberg_table.table.clone(),
                target_table_uuid: iceberg_table.table_uuid.clone().expect("test table uuid"),
                target_snapshot_id: iceberg_table.current_snapshot_id,
                aggregate_state_layout_version: 1,
                columns: vec![],
                group_key_names: vec![],
                aggregate_state_names: vec![],
                physical_column_names: vec![],
                row_id_column_name: "_row_id".to_string(),
                row_filter: crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                    row_id_column_name: "_row_id".to_string(),
                    branch_scope: None,
                },
                partition_constraint:
                    crate::sql::catalog::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
            });
        let table_handle = iceberg_scan_table_handle_for_codegen(
            &target_state_source,
            &iceberg_table,
            vec![iceberg_i32_file(
                "s3://bucket/target-state-refresh.parquet",
                1,
                1,
            )],
            vec!["id".to_string()],
        );
        assert_explicit_file_path(table_handle, "s3://bucket/target-state-refresh.parquet");
    }

    #[test]
    fn explicit_iceberg_data_file_binding_uses_explicit_files() {
        let iceberg_table = test_iceberg_table_info_with_id_schema();
        let file = iceberg_i32_file("s3://bucket/explicit-snapshot.parquet", 1, 1);
        let original_source = ScanSource::IcebergDataFiles {
            table: iceberg_table.clone(),
            files: vec![file.clone()],
            cloud_properties: BTreeMap::new(),
            binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
        };

        let table_handle = iceberg_scan_table_handle_for_codegen(
            &original_source,
            &iceberg_table,
            vec![file],
            vec!["id".to_string()],
        );
        let table_handle = table_handle
            .downcast_ref::<crate::connector::iceberg::scan_planner::IcebergTableHandle>()
            .expect("IcebergTableHandle");
        let crate::connector::iceberg::scan_planner::IcebergSplitSource::ExplicitFiles(files) =
            &table_handle.split_source
        else {
            panic!("explicit Iceberg data-file scans must preserve explicit files");
        };

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "s3://bucket/explicit-snapshot.parquet");
    }

    #[test]
    fn assert_one_row_emits_assert_num_rows_node() {
        let child = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalGenerateSeries(GenerateSeriesOp {
                start: 1,
                end: 3,
                step: 1,
                column_name: "generate_series".to_string(),
                alias: Some("gs".to_string()),
                output_column_id: crate::sql::column_id::ColumnId::new_for_test(9001),
            }),
            children: vec![],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![OutputColumn {
                column_id: crate::sql::column_id::ColumnId::new_for_test(9001),
                name: "generate_series".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });
        let output_columns = child.output_columns.clone();
        let plan = attach_test_scalar_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalAssertOneRow(crate::sql::optimizer::operator::AssertOneRowOp {
                subquery_text: "select 1".to_string(),
            }),
            children: vec![child],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns,
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &crate::connector::ConnectorRegistry::new(),
            "default",
        )
        .expect("build");
        let root = build.fragment_results.first().expect("root fragment");
        let assert_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::ASSERT_NUM_ROWS_NODE)
            .expect("assert num rows node");
        let payload = assert_node
            .assert_num_rows_node
            .as_ref()
            .expect("assert payload");
        assert_eq!(payload.desired_num_rows, Some(1));
        assert_eq!(payload.assertion, Some(plan_nodes::TAssertion::LE));
        assert_eq!(payload.subquery_string.as_deref(), Some("select 1"));
    }

    /// Codegen unit test: visit_sort with partition_limit set must emit
    /// TSortNode with partition_exprs, partition_limit, topn_type, use_top_n=true,
    /// and NO global limit (node.limit == -1).
    ///
    /// This validates Change A of the partition-topn end-to-end contract: the
    /// codegen layer correctly serializes all three partition-topn fields and
    /// leaves the global limit untouched.
    #[test]
    fn ir_sort_partition_topn_emits_correct_tsortnode_fields() {
        use tempfile::NamedTempFile;

        let file = NamedTempFile::new().expect("temp parquet path");
        let output = output_columns();
        let mut scalars = ScalarArena::new();

        // Build a PhysicalSort with partition_limit=Some(2) + Rank topn_type
        // + analytic_partition_exprs = [id_expr] (the partition key).
        let mut plan = physical_node_for_test(
            Operator::PhysicalSort(SortOp {
                items: intern_sort_items(
                    &mut scalars,
                    &[SortItem {
                        expr: id_expr(),
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                analytic_partition_exprs: intern_exprs(&mut scalars, &[id_expr()]),
                partition_limit: Some(2),
                topn_type: Some(crate::exec::node::sort::SortTopNType::Rank),
            }),
            vec![scan_plan(file.path().to_path_buf())],
            output,
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        let build = build_fragments_from_optimizer_for_database_for_test(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");

        let sort_plan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::SORT_NODE)
            .expect("sort node in root fragment");

        // Global limit must remain -1 (not set) — partition-topn is decoupled
        // from the global limit mechanism.
        assert_eq!(
            sort_plan_node.limit, -1,
            "partition-topn must NOT set a global limit"
        );

        let sort = sort_plan_node.sort_node.as_ref().expect("sort payload");

        // use_top_n must be true for partition-topn routing in lowering/exec.
        assert!(
            sort.use_top_n,
            "visit_sort with partition_limit must set use_top_n=true"
        );

        // partition_limit must be emitted as the per-partition row cap.
        assert_eq!(
            sort.partition_limit,
            Some(2),
            "partition_limit must be emitted"
        );

        // topn_type must be RANK.
        assert_eq!(
            sort.topn_type,
            Some(plan_nodes::TTopNType::RANK),
            "topn_type must be RANK"
        );

        // partition_exprs must be non-empty (compiled from analytic_partition_exprs).
        assert!(
            sort.partition_exprs
                .as_ref()
                .map(|v| !v.is_empty())
                .unwrap_or(false),
            "partition_exprs must be emitted from analytic_partition_exprs"
        );
    }
}
