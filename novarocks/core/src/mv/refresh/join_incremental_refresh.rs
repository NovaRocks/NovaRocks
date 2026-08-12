// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Canonical planning and execution adapter for incremental join MV refreshes.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

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
