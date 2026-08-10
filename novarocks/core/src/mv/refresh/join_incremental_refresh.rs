// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Canonical planning and execution adapter for incremental join MV refreshes.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use novarocks_connector_iceberg::iceberg::TableIdent;

use crate::connector::iceberg::commit::CommitOpKind;
use crate::mv::refresh::change_stream_write::{
    ChangeStreamWriteError, ExecutedChangeStreamWrite, PopulatedChangeStreamWrite,
    execute_and_collect_change_stream_write,
};
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::compiler::mv_rewrite::SqlImvRewriteSnapshot;
use crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor;
use crate::sql::planner::logical::LogicalPlanNode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinIncrementalRefreshMode {
    AppendOnly,
    Coalesce,
}

pub(crate) fn select_join_incremental_refresh_mode(
    left_has_delete_changes: bool,
    right_has_delete_changes: bool,
) -> JoinIncrementalRefreshMode {
    if left_has_delete_changes || right_has_delete_changes {
        JoinIncrementalRefreshMode::Coalesce
    } else {
        JoinIncrementalRefreshMode::AppendOnly
    }
}

fn commit_op_kind(mode: JoinIncrementalRefreshMode) -> CommitOpKind {
    match mode {
        JoinIncrementalRefreshMode::AppendOnly => CommitOpKind::FastAppend,
        JoinIncrementalRefreshMode::Coalesce => CommitOpKind::RowDeltaDvFromFiles,
    }
}

pub(crate) struct JoinIncrementalLogicalInput {
    pub(crate) plan: LogicalPlanNode,
    pub(crate) factory: ColumnRefFactory,
}

pub(crate) struct JoinIncrementalLogicalPlan {
    pub(crate) plan: LogicalPlanNode,
    pub(crate) factory: ColumnRefFactory,
    pub(crate) change_stream_override: Option<ImvChangeStreamDescriptor>,
}

pub(crate) fn build_join_incremental_refresh_logical_plan(
    snapshot: &Arc<SqlImvRewriteSnapshot>,
    mode: JoinIncrementalRefreshMode,
    input: JoinIncrementalLogicalInput,
) -> Result<JoinIncrementalLogicalPlan, String> {
    let JoinIncrementalLogicalInput { plan, factory } = input;
    let is_aggregate_refresh = snapshot.schema_contract.aggregate.is_some();
    let factory_cell = Rc::new(RefCell::new(factory));
    let outcome = crate::sql::planner::imv_rewrite::entrypoint::run_imv_rewrite(
        crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteInput {
            plan,
            snapshot: Arc::clone(snapshot),
            disabled_rules: logical_execution_disabled_rules(
                is_aggregate_refresh,
                &crate::sql::optimizer::options::SessionOptimizerSettings::default(),
            ),
            deadline: None,
            column_ref_factory: Rc::clone(&factory_cell),
        },
    )
    .map_err(|e| format!("join refresh logical rewrite: {e}"))?;
    let mut factory = Rc::try_unwrap(factory_cell)
        .map_err(|_| "IMV rewrite leaked ColumnRefFactory references".to_string())?
        .into_inner();
    let mut change_stream_override = None;
    let plan = match mode {
        JoinIncrementalRefreshMode::AppendOnly => outcome.plan,
        JoinIncrementalRefreshMode::Coalesce if is_aggregate_refresh => outcome.plan,
        JoinIncrementalRefreshMode::Coalesce => {
            let descriptor = outcome
                .annotation
                .change_stream
                .join_refresh
                .clone()
                .ok_or_else(|| {
                    format!(
                        "iceberg join MV {} incremental refresh rewrite did not produce join refresh descriptor",
                        snapshot.target.fqn()
                    )
                })?;
            descriptor.validate().map_err(|e| {
                format!(
                    "iceberg join MV {} incremental refresh descriptor is invalid: {e}",
                    snapshot.target.fqn()
                )
            })?;
            change_stream_override = Some(ImvChangeStreamDescriptor {
                aggregate: None,
                join_refresh: Some(descriptor.clone()),
            });
            let locator_columns =
                allocate_join_coalesce_locator_column_ids(&mut factory, &outcome.plan)?;
            crate::sql::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                outcome.plan,
                &descriptor,
                &crate::sql::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding::from_snapshot(snapshot),
                &mut factory,
                locator_columns.net,
                locator_columns.file,
                locator_columns.pos,
                locator_columns.row_id,
                locator_columns.last_updated_sequence_number,
            )
            .map_err(|e| format!("build join refresh coalesce logical plan: {e}"))?
        }
    };
    reserve_factory_for_logical_plan(&mut factory, &plan)?;
    Ok(JoinIncrementalLogicalPlan {
        plan,
        factory,
        change_stream_override,
    })
}

pub(crate) fn execute_join_incremental_refresh_write<F>(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    ident: &TableIdent,
    target_ref: &str,
    mode: JoinIncrementalRefreshMode,
    logical: JoinIncrementalLogicalPlan,
    execute: F,
) -> Result<PopulatedChangeStreamWrite, ChangeStreamWriteError>
where
    F: FnOnce(JoinIncrementalLogicalPlan) -> Result<ExecutedChangeStreamWrite, String>,
{
    execute_and_collect_change_stream_write(table, ident, target_ref, commit_op_kind(mode), || {
        execute(logical)
    })
}

fn logical_execution_disabled_rules(
    is_aggregate_refresh: bool,
    optimizer_settings: &crate::sql::optimizer::options::SessionOptimizerSettings,
) -> Vec<String> {
    let mut disabled_rules = optimizer_settings.disabled_rules.clone();
    if !disabled_rules
        .iter()
        .any(|rule| rule == "InjectTargetLocatorJoin")
    {
        disabled_rules.push("InjectTargetLocatorJoin".to_string());
    }
    if is_aggregate_refresh
        && !disabled_rules
            .iter()
            .any(|rule| rule == "RecordJoinRefreshDescriptor")
    {
        disabled_rules.push("RecordJoinRefreshDescriptor".to_string());
    }
    disabled_rules
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JoinCoalesceLocatorColumnIds {
    pub(crate) net: u32,
    pub(crate) file: u32,
    pub(crate) pos: u32,
    pub(crate) row_id: u32,
    pub(crate) last_updated_sequence_number: u32,
}

pub(crate) fn allocate_join_coalesce_locator_column_ids(
    factory: &mut ColumnRefFactory,
    plan: &LogicalPlanNode,
) -> Result<JoinCoalesceLocatorColumnIds, String> {
    reserve_factory_for_logical_plan(factory, plan)?;
    Ok(JoinCoalesceLocatorColumnIds {
        net: factory
            .create(
                None,
                "net".to_string(),
                arrow::datatypes::DataType::Int64,
                false,
            )
            .0,
        file: factory
            .create(
                None,
                novarocks_execution::exec::row_position::ICEBERG_FILE_PATH_COL.to_string(),
                arrow::datatypes::DataType::Utf8,
                true,
            )
            .0,
        pos: factory
            .create(
                None,
                novarocks_execution::exec::row_position::ICEBERG_ROW_POS_COL.to_string(),
                arrow::datatypes::DataType::Int64,
                true,
            )
            .0,
        row_id: factory
            .create(
                None,
                novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
                arrow::datatypes::DataType::Int64,
                true,
            )
            .0,
        last_updated_sequence_number: factory
            .create(
                None,
                novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
                arrow::datatypes::DataType::Int64,
                true,
            )
            .0,
    })
}

pub(crate) fn reserve_factory_for_logical_plan(
    factory: &mut ColumnRefFactory,
    plan: &LogicalPlanNode,
) -> Result<(), String> {
    let max_id = max_logical_plan_output_column_id(plan)?;
    factory.reserve_until(max_id.saturating_add(1));
    Ok(())
}

fn max_logical_plan_output_column_id(plan: &LogicalPlanNode) -> Result<u32, String> {
    let mut max_id = crate::sql::planner::plan_output_columns(plan)?
        .iter()
        .map(|column| column.column_id.0)
        .max()
        .unwrap_or(0);
    for child in &plan.children {
        max_id = max_id.max(max_logical_plan_output_column_id(child)?);
    }
    Ok(max_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell;

    use novarocks_connector_iceberg::iceberg::NamespaceIdent;
    use novarocks_connector_iceberg::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };

    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::logical::LogicalPlanKind;
    use crate::sql::planner::payload::PlanValuesNode;

    fn empty_input() -> JoinIncrementalLogicalInput {
        JoinIncrementalLogicalInput {
            plan: LogicalPlanNode::new(
                LogicalPlanKind::Values(PlanValuesNode {
                    rows: Vec::new(),
                    columns: Vec::new(),
                }),
                Vec::new(),
                None,
            ),
            factory: ColumnRefFactory::new(),
        }
    }

    fn test_ident() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("db".to_string()), "mv".to_string())
    }

    fn test_table() -> novarocks_connector_iceberg::iceberg::table::Table {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/mv".to_string(),
            FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("table metadata builder")
        .build()
        .expect("table metadata")
        .metadata;
        novarocks_connector_iceberg::iceberg::table::Table::builder()
            .identifier(test_ident())
            .file_io(novarocks_connector_iceberg::iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("table")
    }

    #[test]
    fn selects_mode_from_delete_presence() {
        assert_eq!(
            select_join_incremental_refresh_mode(false, false),
            JoinIncrementalRefreshMode::AppendOnly
        );
        assert_eq!(
            select_join_incremental_refresh_mode(true, false),
            JoinIncrementalRefreshMode::Coalesce
        );
        assert_eq!(
            select_join_incremental_refresh_mode(false, true),
            JoinIncrementalRefreshMode::Coalesce
        );
    }

    #[test]
    fn mode_owns_commit_kind() {
        assert_eq!(
            commit_op_kind(JoinIncrementalRefreshMode::AppendOnly),
            CommitOpKind::FastAppend
        );
        assert_eq!(
            commit_op_kind(JoinIncrementalRefreshMode::Coalesce),
            CommitOpKind::RowDeltaDvFromFiles
        );
    }

    #[test]
    fn locator_ids_reserve_all_nested_outputs() {
        let child_output = OutputColumn {
            column_id: ColumnId(42),
            name: "child_k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let child = LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![child_output],
            }),
            Vec::new(),
            None,
        );
        let root_output = OutputColumn {
            column_id: ColumnId(6),
            name: "root_k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![root_output],
            }),
            vec![child],
            None,
        );
        let mut factory = ColumnRefFactory::new();

        let ids =
            allocate_join_coalesce_locator_column_ids(&mut factory, &plan).expect("locator ids");
        let allocated = [
            ids.net,
            ids.file,
            ids.pos,
            ids.row_id,
            ids.last_updated_sequence_number,
        ];
        assert!(allocated.iter().all(|id| *id > 42));
        assert_eq!(
            allocated
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            allocated.len()
        );
    }

    #[test]
    fn execution_adapter_invokes_callback_once() {
        let calls = Cell::new(0);
        let input = empty_input();
        let logical = JoinIncrementalLogicalPlan {
            plan: input.plan,
            factory: input.factory,
            change_stream_override: None,
        };

        let error = match execute_join_incremental_refresh_write(
            &test_table(),
            &test_ident(),
            "staging",
            JoinIncrementalRefreshMode::AppendOnly,
            logical,
            |_| {
                calls.set(calls.get() + 1);
                Err("sentinel execution failure".to_string())
            },
        ) {
            Ok(_) => panic!("callback failure must cross the canonical execution seam"),
            Err(error) => error,
        };

        assert_eq!(calls.get(), 1);
        assert_eq!(error.into_message(), "sentinel execution failure");
    }
}
