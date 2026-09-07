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

use crate::analysis::ExprKind;
use crate::column_id::ColumnId;
use crate::planner::distributed::DistributedPlan;
use crate::planner::payload::PlanAssertOneRowNode;
use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode, PreExpandKeyedAssertSpec};

#[allow(
    dead_code,
    reason = "The pipeline entrypoint is retained for SQL compilation surfaces enabled outside this target."
)]
pub(crate) fn build_distributed_plan(
    physical: PhysicalPlanNode,
) -> Result<DistributedPlan, String> {
    build_distributed_plan_with_settings(
        physical,
        &crate::optimizer::options::SessionOptimizerSettings::default(),
    )
}

pub(crate) fn build_distributed_plan_with_settings(
    mut physical: PhysicalPlanNode,
    settings: &crate::optimizer::options::SessionOptimizerSettings,
) -> Result<DistributedPlan, String> {
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        settings,
    );
    crate::planner::distributed::build::build_distributed_plan(&physical)
}

pub(crate) fn build_connector_write_dataflow_plan_with_auxiliary_settings(
    mut physical: PhysicalPlanNode,
    sink: crate::planner::distributed::write::sink::ConnectorWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    auxiliary: &crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan,
    settings: &crate::optimizer::options::SessionOptimizerSettings,
) -> Result<DistributedPlan, String> {
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        settings,
    );
    crate::planner::distributed::write::plan::build_table_writer_finish_distributed_plan_with_auxiliary(
        &physical, sink, write_target_ordinal, auxiliary,
    )
}

/// Compile a SQL-owned write contract into the dataflow writer/finish shape.
/// Provider metadata and writer handles are intentionally absent; application
/// code resolves those from the request-local binding token after this plan has
/// been sealed.
#[cfg(test)]
pub(crate) fn build_sql_write_dataflow_plan_with_settings(
    physical: PhysicalPlanNode,
    sink: crate::planner::distributed::write::contract::SqlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    settings: &crate::optimizer::options::SessionOptimizerSettings,
) -> Result<DistributedPlan, String> {
    let auxiliary =
        crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan::without_requirements(
            [write_target_ordinal],
        )?;
    build_sql_write_dataflow_plan_with_auxiliary_settings(
        physical,
        sink,
        write_target_ordinal,
        &auxiliary,
        settings,
    )
}

/// Build the same write dataflow with an already-resolved ordinary aggregate
/// auxiliary plan. Connector descriptors have been erased before this point.
pub(crate) fn build_sql_write_dataflow_plan_with_auxiliary_settings(
    mut physical: PhysicalPlanNode,
    sink: crate::planner::distributed::write::contract::SqlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    auxiliary: &crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan,
    settings: &crate::optimizer::options::SessionOptimizerSettings,
) -> Result<DistributedPlan, String> {
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        settings,
    );
    crate::planner::distributed::write::plan::build_sql_table_writer_finish_distributed_plan_with_auxiliary(
        &physical, sink, write_target_ordinal, auxiliary,
    )
}

/// Compile a change-stream write into the dataflow writer/finish shape. Every
/// route's writer gathers into the same single Root finish fragment.
#[cfg(test)]
pub(crate) fn build_sql_change_stream_dataflow_plan_with_settings(
    physical: PhysicalPlanNode,
    dag: crate::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    keyed_assert: Option<PreExpandKeyedAssertSpec>,
    settings: &crate::optimizer::options::SessionOptimizerSettings,
) -> Result<crate::planner::distributed::write::plan::PlannedSqlChangeStreamDistributedPlan, String>
{
    let auxiliary =
        crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan::without_requirements(
            dag.routes.iter().map(|route| route.write_target_ordinal),
        )?;
    build_sql_change_stream_dataflow_plan_with_auxiliary_settings(
        physical,
        dag,
        keyed_assert,
        &auxiliary,
        settings,
    )
}

pub(crate) fn build_sql_change_stream_dataflow_plan_with_auxiliary_settings(
    mut physical: PhysicalPlanNode,
    dag: crate::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    keyed_assert: Option<PreExpandKeyedAssertSpec>,
    auxiliary: &crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan,
    settings: &crate::optimizer::options::SessionOptimizerSettings,
) -> Result<crate::planner::distributed::write::plan::PlannedSqlChangeStreamDistributedPlan, String>
{
    if let Some(keyed_assert) = keyed_assert {
        insert_pre_expand_keyed_assert(&mut physical, &keyed_assert)?;
    }
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        settings,
    );
    crate::planner::distributed::write::plan::build_sql_change_stream_table_writer_finish_distributed_plan_with_auxiliary(
        &physical, dag, auxiliary,
    )
}

fn insert_pre_expand_keyed_assert(
    physical: &mut PhysicalPlanNode,
    keyed_assert: &PreExpandKeyedAssertSpec,
) -> Result<(), String> {
    let mut expand_count = 0usize;
    let mut key_column_id = None;
    validate_pre_expand_keyed_assert_in_node(
        physical,
        keyed_assert,
        &mut expand_count,
        &mut key_column_id,
    )?;
    if expand_count != 1 {
        return Err(format!(
            "DML change-stream keyed assert requires exactly one native ChangeEventExpand node, found {expand_count}"
        ));
    }
    let key_column_id = key_column_id.expect("one validated ChangeEventExpand has one key");
    apply_pre_expand_keyed_assert_in_node(physical, keyed_assert, key_column_id);
    Ok(())
}

fn validate_pre_expand_keyed_assert_in_node(
    node: &PhysicalPlanNode,
    keyed_assert: &PreExpandKeyedAssertSpec,
    expand_count: &mut usize,
    key_column_id: &mut Option<ColumnId>,
) -> Result<(), String> {
    for child in &node.children {
        validate_pre_expand_keyed_assert_in_node(child, keyed_assert, expand_count, key_column_id)?;
    }

    let PhysicalPlanKind::ChangeEventExpand(expand) = &node.kind else {
        return Ok(());
    };
    *expand_count += 1;
    let [child] = node.children.as_slice() else {
        return Err(format!(
            "DML change-stream native ChangeEventExpand expected one child, got {}",
            node.children.len()
        ));
    };
    *key_column_id = Some(find_pre_expand_key_column_id(child, expand, keyed_assert)?);
    Ok(())
}

fn apply_pre_expand_keyed_assert_in_node(
    node: &mut PhysicalPlanNode,
    keyed_assert: &PreExpandKeyedAssertSpec,
    key_column_id: ColumnId,
) -> bool {
    for child in &mut node.children {
        if apply_pre_expand_keyed_assert_in_node(child, keyed_assert, key_column_id) {
            return true;
        }
    }

    if !matches!(node.kind, PhysicalPlanKind::ChangeEventExpand(_)) {
        return false;
    }
    let child = node.children.pop().expect("validated single child");
    let output_columns = child.output_columns.clone();
    node.children.push(PhysicalPlanNode {
        kind: PhysicalPlanKind::AssertOneRow(PlanAssertOneRowNode::per_key_at_most_one(
            "DML change-stream matched row uniqueness",
            vec![key_column_id],
            vec![keyed_assert.key_label.clone()],
            keyed_assert.message_prefix.clone(),
        )),
        children: vec![child],
        output_columns,
        stats: node.stats.clone(),
        probe_runtime_filters: vec![],
    });
    true
}

fn find_pre_expand_key_column_id(
    child: &PhysicalPlanNode,
    expand: &crate::planner::physical::DistributedChangeEventExpandNode,
    keyed_assert: &PreExpandKeyedAssertSpec,
) -> Result<ColumnId, String> {
    match unique_output_column_id_by_name(
        effective_physical_output_columns(child),
        &keyed_assert.key_column_name,
    ) {
        Ok(column_id) => Ok(column_id),
        Err(name_err) if can_derive_key_from_row_id_assignment(keyed_assert) => {
            find_pre_expand_key_column_id_from_assignment(child, expand, keyed_assert)?
                .ok_or(name_err)
        }
        Err(err) => Err(err),
    }
}

fn unique_output_column_id_by_name(
    columns: &[crate::analysis::OutputColumn],
    column_name: &str,
) -> Result<ColumnId, String> {
    let mut matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(column_name));
    let column = matches.next().ok_or_else(|| {
        format!("DML change-stream keyed assert column `{column_name}` not found in native child")
    })?;
    if matches.next().is_some() {
        return Err(format!(
            "DML change-stream keyed assert column `{column_name}` is ambiguous in native child"
        ));
    }
    Ok(column.column_id)
}

fn can_derive_key_from_row_id_assignment(keyed_assert: &PreExpandKeyedAssertSpec) -> bool {
    keyed_assert
        .key_column_name
        .eq_ignore_ascii_case("__nr_row_id")
        && keyed_assert
            .key_label
            .eq_ignore_ascii_case(crate::common::ICEBERG_ROW_ID_COL)
}

fn find_pre_expand_key_column_id_from_assignment(
    child: &PhysicalPlanNode,
    expand: &crate::planner::physical::DistributedChangeEventExpandNode,
    keyed_assert: &PreExpandKeyedAssertSpec,
) -> Result<Option<ColumnId>, String> {
    let mut output_columns = expand
        .output_columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(&keyed_assert.key_label));
    let Some(output_column) = output_columns.next() else {
        return Ok(None);
    };
    if output_columns.next().is_some() {
        return Err(format!(
            "DML change-stream native keyed assert output column `{}` is ambiguous",
            keyed_assert.key_label
        ));
    }

    let mut key_column_id = None;
    for event in &expand.events {
        for assignment in &event.assignments {
            if assignment.output_column_id != output_column.column_id {
                continue;
            }
            let Some(expr) = assignment.expr.as_ref() else {
                continue;
            };
            let ExprKind::ColumnRef { column_id, .. } = expr.kind else {
                continue;
            };
            validate_unique_column_in_output_scope(child, column_id)?;
            if let Some(previous) = key_column_id {
                if previous != column_id {
                    return Err(format!(
                        "DML change-stream native keyed assert output `{}` is assigned from multiple child columns: {:?} and {:?}",
                        keyed_assert.key_label, previous, column_id
                    ));
                }
            } else {
                key_column_id = Some(column_id);
            }
        }
    }
    Ok(key_column_id)
}

fn validate_unique_column_in_output_scope(
    child: &PhysicalPlanNode,
    column_id: ColumnId,
) -> Result<(), String> {
    match effective_physical_output_columns(child)
        .iter()
        .filter(|column| column.column_id == column_id)
        .count()
    {
        1 => Ok(()),
        0 => Err(format!(
            "DML change-stream native keyed assert assignment ColumnId({}) is not in direct child output scope",
            column_id.0
        )),
        count => Err(format!(
            "DML change-stream native keyed assert assignment ColumnId({}) is ambiguous in direct child output scope ({count} bindings)",
            column_id.0
        )),
    }
}

fn effective_physical_output_columns(node: &PhysicalPlanNode) -> &[crate::analysis::OutputColumn] {
    // Bridge 2a intentionally keeps Sort's materialized child contract on the
    // generic node while the payload carries its visible reordered/pruned
    // output. The native Sort lowering follows the payload when it is non-empty
    // and treats an empty payload as passthrough. Other physical payload output
    // fields currently mirror the generic node contract at this boundary.
    match &node.kind {
        PhysicalPlanKind::Sort(sort) if !sort.output_columns.is_empty() => &sort.output_columns,
        _ => &node.output_columns,
    }
}

#[cfg(test)]
mod tests;
