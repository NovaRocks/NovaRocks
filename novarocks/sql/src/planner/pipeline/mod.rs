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
use crate::planner::payload::PlanAssertOneRowNode;
use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode, PreExpandKeyedAssertSpec};

pub(crate) fn insert_pre_expand_keyed_assert(
    physical: &mut PhysicalPlanNode,
    keyed_assert: &PreExpandKeyedAssertSpec,
) -> Result<(), String> {
    if keyed_assert.key_columns.is_empty() {
        return Err("DML change-stream keyed assert requires at least one key".to_string());
    }
    let mut expand_count = 0usize;
    let mut key_column_ids = None;
    let mut applicability_column_id = None;
    validate_pre_expand_keyed_assert_in_node(
        physical,
        keyed_assert,
        &mut expand_count,
        &mut key_column_ids,
        &mut applicability_column_id,
    )?;
    if expand_count != 1 {
        return Err(format!(
            "DML change-stream keyed assert requires exactly one native ChangeEventExpand node, found {expand_count}"
        ));
    }
    let key_column_ids = key_column_ids.expect("one validated ChangeEventExpand has keys");
    apply_pre_expand_keyed_assert_in_node(
        physical,
        keyed_assert,
        &key_column_ids,
        applicability_column_id,
    );
    Ok(())
}

fn validate_pre_expand_keyed_assert_in_node(
    node: &PhysicalPlanNode,
    keyed_assert: &PreExpandKeyedAssertSpec,
    expand_count: &mut usize,
    key_column_ids: &mut Option<Vec<ColumnId>>,
    applicability_column_id: &mut Option<ColumnId>,
) -> Result<(), String> {
    for child in &node.children {
        validate_pre_expand_keyed_assert_in_node(
            child,
            keyed_assert,
            expand_count,
            key_column_ids,
            applicability_column_id,
        )?;
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
    *key_column_ids = Some(
        keyed_assert
            .key_columns
            .iter()
            .map(|(name, label)| find_pre_expand_key_column_id(child, expand, name, label))
            .collect::<Result<Vec<_>, _>>()?,
    );
    *applicability_column_id = keyed_assert.applicability_output_ordinal.map(|ordinal| {
        let column = effective_physical_output_columns(child).get(ordinal).ok_or_else(|| {
            format!("DML change-stream applicability output ordinal {ordinal} is outside the native child")
        })?;
        if column.value_type.data_type() != &arrow::datatypes::DataType::Boolean {
            return Err("DML change-stream applicability output must be Boolean".to_string());
        }
        Ok(column.column_id)
    }).transpose()?;
    Ok(())
}

fn apply_pre_expand_keyed_assert_in_node(
    node: &mut PhysicalPlanNode,
    keyed_assert: &PreExpandKeyedAssertSpec,
    key_column_ids: &[ColumnId],
    applicability_column_id: Option<ColumnId>,
) -> bool {
    for child in &mut node.children {
        if apply_pre_expand_keyed_assert_in_node(
            child,
            keyed_assert,
            key_column_ids,
            applicability_column_id,
        ) {
            return true;
        }
    }

    if !matches!(node.kind, PhysicalPlanKind::ChangeEventExpand(_)) {
        return false;
    }
    let child = node.children.pop().expect("validated single child");
    let output_columns = child.output_columns.clone();
    node.children.push(PhysicalPlanNode {
        kind: PhysicalPlanKind::AssertOneRow(
            PlanAssertOneRowNode::per_key_at_most_one(
                "DML change-stream matched row uniqueness",
                key_column_ids.to_vec(),
                keyed_assert
                    .key_columns
                    .iter()
                    .map(|(_, label)| label.clone())
                    .collect(),
                keyed_assert.message_prefix.clone(),
                applicability_column_id,
            )
            .with_declared_violation(keyed_assert.declared_violation),
        ),
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
    key_column_name: &str,
    key_label: &str,
) -> Result<ColumnId, String> {
    match unique_output_column_id_by_name(effective_physical_output_columns(child), key_column_name)
    {
        Ok(column_id) => Ok(column_id),
        Err(name_err) if can_derive_key_from_assignment(key_column_name, key_label) => {
            find_pre_expand_key_column_id_from_assignment(child, expand, key_label)?.ok_or(name_err)
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

fn can_derive_key_from_assignment(key_column_name: &str, key_label: &str) -> bool {
    [
        ("__nr_file", crate::common::ICEBERG_FILE_PATH_COL),
        ("__nr_pos", crate::common::ICEBERG_ROW_POS_COL),
        ("__nr_row_id", crate::common::ICEBERG_ROW_ID_COL),
    ]
    .iter()
    .any(|(name, label)| {
        key_column_name.eq_ignore_ascii_case(name) && key_label.eq_ignore_ascii_case(label)
    })
}

fn find_pre_expand_key_column_id_from_assignment(
    child: &PhysicalPlanNode,
    expand: &crate::planner::physical::DistributedChangeEventExpandNode,
    key_label: &str,
) -> Result<Option<ColumnId>, String> {
    let mut output_columns = expand
        .output_columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(key_label));
    let Some(output_column) = output_columns.next() else {
        return Ok(None);
    };
    if output_columns.next().is_some() {
        return Err(format!(
            "DML change-stream native keyed assert output column `{key_label}` is ambiguous"
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
                        "DML change-stream native keyed assert output `{key_label}` is assigned from multiple child columns: {previous:?} and {column_id:?}"
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
