//! Extract the best physical plan from the Memo after top-down search.
//!
//! Walks the winner map starting from the root group with the required
//! physical properties, recursively building a `PhysicalPlanNode` tree.

use std::collections::HashMap;

use super::memo::{GroupId, Memo};
use super::operator::{Operator, PhysicalDistributionOp, PhysicalSortOp};
use super::physical_plan::PhysicalPlanNode;
use super::property::{OrderingSpec, PhysicalPropertySet};
use super::search::{required_input_properties, EnforcerKind, Winner};
use crate::sql::ir::{ExprKind, SortItem, TypedExpr};
use crate::sql::statistics::Statistics;

/// Extract the best physical plan tree from the Memo.
///
/// Walks the winner map starting from `root_group` with `required` properties.
/// For each winner, if it has an enforcer, an enforcer PhysicalPlanNode is
/// created wrapping the recursive extraction with the enforcer's child props.
/// Otherwise, the winner's physical expression is used directly with children
/// extracted according to `required_input_properties`.
pub(crate) fn extract_best(
    memo: &Memo,
    root_group: GroupId,
    required: &PhysicalPropertySet,
    winners: &HashMap<(GroupId, PhysicalPropertySet), Winner>,
) -> Result<PhysicalPlanNode, String> {
    let cache_key = (root_group, required.clone());
    let winner = winners
        .get(&cache_key)
        .ok_or_else(|| format!("no winner for group {} with props {:?}", root_group, required))?;

    if winner.cost.is_infinite() {
        return Err(format!(
            "no feasible plan for group {} with props {:?}",
            root_group, required
        ));
    }

    let group = &memo.groups[root_group];
    let group_stats = group_statistics(group);
    let output_columns = group
        .logical_props
        .as_ref()
        .map(|lp| lp.output_columns.clone())
        .unwrap_or_default();

    // If the winner has an enforcer, create the enforcer node on top.
    if let Some(ref enforcer_info) = winner.enforcer {
        // Recursively extract the child plan with the enforcer's child properties.
        let child_node = extract_best(memo, root_group, &enforcer_info.child_props, winners)?;

        let enforcer_op = match &enforcer_info.kind {
            EnforcerKind::Distribution(spec) => {
                Operator::PhysicalDistribution(PhysicalDistributionOp { spec: spec.clone() })
            }
            EnforcerKind::Sort(ordering) => {
                let items = ordering_spec_to_sort_items(ordering);
                Operator::PhysicalSort(PhysicalSortOp { items })
            }
        };

        return Ok(PhysicalPlanNode {
            op: enforcer_op,
            children: vec![child_node],
            stats: group_stats,
            output_columns,
        });
    }

    // No enforcer: extract the physical expression directly.
    let expr = group
        .physical_exprs
        .get(winner.expr_index)
        .ok_or_else(|| {
            format!(
                "winner expr_index {} out of bounds for group {} (has {} physical exprs)",
                winner.expr_index,
                root_group,
                group.physical_exprs.len()
            )
        })?;

    // Determine child required properties.
    let child_reqs = required_input_properties(&expr.op, required);

    // Recursively extract children.
    let mut children = Vec::with_capacity(expr.children.len());
    for (i, &child_group_id) in expr.children.iter().enumerate() {
        let child_req = child_reqs
            .get(i)
            .cloned()
            .unwrap_or_else(PhysicalPropertySet::any);
        let child_node = extract_best(memo, child_group_id, &child_req, winners)?;
        children.push(child_node);
    }

    Ok(PhysicalPlanNode {
        op: expr.op.clone(),
        children,
        stats: group_stats,
        output_columns,
    })
}

/// Build a `Statistics` from a group's logical properties.
fn group_statistics(group: &super::memo::Group) -> Statistics {
    if let Some(ref lp) = group.logical_props {
        Statistics {
            output_row_count: lp.row_count,
            column_statistics: HashMap::new(),
        }
    } else {
        Statistics {
            output_row_count: 1.0,
            column_statistics: HashMap::new(),
        }
    }
}

/// Convert an `OrderingSpec` to `Vec<SortItem>` for the enforcer PhysicalSort node.
fn ordering_spec_to_sort_items(ordering: &OrderingSpec) -> Vec<SortItem> {
    match ordering {
        OrderingSpec::Any => vec![],
        OrderingSpec::Required(sort_keys) => sort_keys
            .iter()
            .map(|sk| SortItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: sk.column.qualifier.clone(),
                        column: sk.column.column.clone(),
                    },
                    data_type: arrow::datatypes::DataType::Null,
                    nullable: true,
                },
                asc: sk.asc,
                nulls_first: sk.nulls_first,
            })
            .collect(),
    }
}
