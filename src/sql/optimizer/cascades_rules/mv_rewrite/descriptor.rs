//! SPJG (select-project-join-group-by, single-table subset) decomposition.
//!
//! Both sides of MV rewrite matching are normalized into this shape:
//! the MV defining plan (built at candidate-prep time from the planner
//! LogicalPlanNode) and the query subtree (rebuilt from memo MExprs by the rule).

use std::collections::HashMap;

use crate::sql::catalog::TableDef;
use crate::sql::column_id::ColumnId;
use crate::sql::common::OutputColumn;
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{
    AggStage, LogicalAggregateOp, Operator, ProjectOp, ScalarAggregateSpec,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode, SortKey};
use crate::sql::optimizer::scalar_expr;

/// What the alternative must reproduce at the matched group's top.
///
/// Carries the original operator shape so the rule can reuse the matched
/// group's output `ColumnId`s when constructing the rewritten alternative
/// (memo-group equivalence requires equivalent expressions to share their
/// output columns).
#[derive(Clone, Debug)]
pub(crate) enum MatchedShape {
    /// Top is the scan itself or `Filter(Scan)`: outputs are scan columns.
    Spj,
    /// Top is `LogicalAggregate`: the original op is cloned so the rule can
    /// reuse its `output_columns` ids.
    Spjg { original_agg: LogicalAggregateOp },
}

/// One visible output of the SPJG subtree, in output order.
#[derive(Clone, Debug)]
pub(crate) struct SpjgOutput {
    pub name: String,
    /// The ColumnId this output is addressed by at the subtree top.
    pub column_id: ColumnId,
    pub expr: SpjgOutputExpr,
}

#[derive(Clone, Debug)]
pub(crate) enum SpjgOutputExpr {
    /// Expression over base-table columns (projection item or group key).
    Dimension(ScalarId),
    /// Aggregate call over base-table columns.
    Aggregate(ScalarAggregateSpec),
}

#[derive(Clone, Debug)]
pub(crate) struct SpjgAggregate {
    /// Group keys, composed down to base-table column expressions.
    /// Rollup matching only needs the group-by set; aggregate calls are matched
    /// through the descriptor's `outputs` (the `Aggregate` variant), so they are
    /// not duplicated here.
    pub group_by: Vec<ScalarId>,
}

#[derive(Clone, Debug)]
pub(crate) struct SpjgDescriptor {
    pub table: TableDef,
    /// Scan output columns: ColumnId -> base column binding.
    pub scan_columns: Vec<OutputColumn>,
    /// All conjuncts below the aggregate (scan predicates + filter, CNF-split).
    pub predicates: Vec<ScalarId>,
    pub aggregate: Option<SpjgAggregate>,
    /// Visible outputs in order (the subtree's output schema).
    pub outputs: Vec<SpjgOutput>,
}

impl SpjgDescriptor {
    /// Map from scan ColumnId to base column name (for cross-side matching:
    /// the two sides see the same physical table through different ids).
    pub(crate) fn base_name_of(&self) -> HashMap<ColumnId, String> {
        self.scan_columns
            .iter()
            .map(|c| (c.column_id, c.name.clone()))
            .collect()
    }

    pub(crate) fn from_opt_expr(
        expr: &OptExpr,
        arena: &mut ScalarArena,
    ) -> Result<SpjgDescriptor, String> {
        // Accepted normal form, peeled top-down:
        //   [Project] -> [Aggregate] -> [Project] -> [Filter]* -> Scan
        // Anything else (Join/Sort/Limit/Window/Union/CTE/...) is rejected.
        let mut node = expr;

        // Optional top project (rebinding of aggregate/scan outputs).
        let top_project = match &node.op {
            Operator::LogicalProject(p) => {
                node = node
                    .children
                    .first()
                    .ok_or_else(|| "project without child in MV rewrite shape".to_string())?;
                Some(p)
            }
            _ => None,
        };

        let aggregate = match &node.op {
            Operator::LogicalAggregate(a) => {
                if a.stage != AggStage::Single || a.is_split || aggregate_has_order_by(a) {
                    return Err("unsupported aggregate shape for MV rewrite".to_string());
                }
                node = node
                    .children
                    .first()
                    .ok_or_else(|| "aggregate without child in MV rewrite shape".to_string())?;
                Some(a)
            }
            _ => None,
        };

        // Optional pre-aggregate project (planner may compute group-key /
        // agg-arg expressions in a project below the aggregate).
        let mid_project = match &node.op {
            Operator::LogicalProject(p) => {
                node = node
                    .children
                    .first()
                    .ok_or_else(|| "project without child in MV rewrite shape".to_string())?;
                Some(p)
            }
            _ => None,
        };

        let mut predicates: Vec<ScalarId> = Vec::new();
        while let Operator::LogicalFilter(f) = &node.op {
            scalar_expr::split_conjuncts(arena, f.predicate, &mut predicates);
            node = node
                .children
                .first()
                .ok_or_else(|| "filter without child in MV rewrite shape".to_string())?;
        }

        let Operator::LogicalScan(scan) = &node.op else {
            return Err(format!(
                "not a single-table SPJG shape: unexpected node {:?}",
                std::mem::discriminant(&node.op)
            ));
        };
        predicates.extend(scan.predicates.iter().copied());

        // Composition map: ColumnId -> defining expr over scan columns
        // (from the mid project). Identity for scan columns themselves.
        let mut defs: HashMap<ColumnId, ScalarId> = HashMap::new();
        if let Some(p) = mid_project {
            for item in &p.items {
                let composed = substitute_scalar(arena, item.expr, &defs);
                defs.insert(item.output_column_id, composed);
            }
        }

        let (agg, outputs) = match aggregate {
            Some(a) => {
                let group_by: Vec<ScalarId> = a
                    .group_by
                    .iter()
                    .map(|expr| substitute_scalar(arena, *expr, &defs))
                    .collect();
                let aggregates: Vec<ScalarAggregateSpec> = a
                    .aggregates
                    .iter()
                    .map(|c| substitute_aggregate(arena, c, &defs))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| "unsupported aggregate order_by in MV rewrite".to_string())?;
                // Aggregate output convention: [group keys..., agg results...]
                if a.output_columns.len() != a.group_by.len() + a.aggregates.len() {
                    return Err(format!(
                        "aggregate output layout {} != group_by {} + aggs {}",
                        a.output_columns.len(),
                        a.group_by.len(),
                        a.aggregates.len()
                    ));
                }
                // Binding map at the aggregate's outputs.
                let mut agg_outputs: Vec<SpjgOutput> = Vec::new();
                for (i, oc) in a.output_columns.iter().enumerate() {
                    let expr = if i < a.group_by.len() {
                        SpjgOutputExpr::Dimension(group_by[i].clone())
                    } else {
                        SpjgOutputExpr::Aggregate(aggregates[i - a.group_by.len()].clone())
                    };
                    agg_outputs.push(SpjgOutput {
                        name: oc.name.clone(),
                        column_id: oc.column_id,
                        expr,
                    });
                }
                let outputs = apply_top_project(arena, top_project, agg_outputs)?;
                (Some(SpjgAggregate { group_by }), outputs)
            }
            None => {
                let scan_outputs: Vec<SpjgOutput> = scan
                    .columns
                    .iter()
                    .map(|c| SpjgOutput {
                        name: c.name.clone(),
                        column_id: c.column_id,
                        expr: SpjgOutputExpr::Dimension(column_ref(arena, c)),
                    })
                    .collect();
                // mid_project without aggregate is just "the" project.
                let scan_outputs = match mid_project {
                    Some(p) => p
                        .items
                        .iter()
                        .map(|item| SpjgOutput {
                            name: item.output_name.clone(),
                            column_id: item.output_column_id,
                            expr: SpjgOutputExpr::Dimension(substitute_scalar(
                                arena, item.expr, &defs,
                            )),
                        })
                        .collect(),
                    None => scan_outputs,
                };
                let outputs = apply_top_project(arena, top_project, scan_outputs)?;
                (None, outputs)
            }
        };

        Ok(SpjgDescriptor {
            table: scan.table.clone(),
            scan_columns: scan.columns.clone(),
            predicates,
            aggregate: agg,
            outputs,
        })
    }

    /// Rebuild the SPJG view of the subtree rooted at `expr`, walking the
    /// memo by following the FIRST logical expression of each child group.
    ///
    /// The first logical expr of a group is always its original (unsplit)
    /// shape — `memo_copy` seeds each group with the planner node, and
    /// transformation rules only ever append alternatives. MvRewrite runs in
    /// the same explore round as `SplitAggregateRule`, so an aggregate group
    /// may already carry a split Local/Global alternative; the first expr is
    /// still the original Single aggregate, which is the only form this
    /// accepts.
    ///
    /// Mirrors [`SpjgDescriptor::from_opt_expr`] arm-by-arm but over
    /// `Operator` variants. Returns `None` for any non-SPJG operator in the
    /// chain (the same fail-closed contract). Unlike `from_opt_expr` there
    /// is no top-project arm: the rule only matches on Aggregate/Filter/Scan,
    /// so the matched node is the subtree top.
    pub(crate) fn from_memo(
        expr: &MExpr,
        memo: &mut Memo,
    ) -> Option<(SpjgDescriptor, MatchedShape)> {
        // Peel an optional top aggregate.
        let (aggregate, mut node) = match &expr.op {
            Operator::LogicalAggregate(a) => {
                // Only the original, unsplit Single aggregate is accepted.
                if a.stage != AggStage::Single || a.is_split || aggregate_has_order_by(a) {
                    return None;
                }
                let child = first_logical_expr(memo, *expr.children.first()?)?;
                (Some(a.clone()), child)
            }
            _ => (None, expr.clone()),
        };

        // Optional pre-aggregate (or sole) project below the current node.
        let mid_project = match &node.op {
            Operator::LogicalProject(p) => {
                let child = first_logical_expr(memo, *node.children.first()?)?;
                let saved = p.clone();
                node = child;
                Some(saved)
            }
            _ => None,
        };

        // Filter chain down to the scan.
        let mut predicates: Vec<ScalarId> = Vec::new();
        while let Operator::LogicalFilter(f) = &node.op {
            scalar_expr::split_conjuncts(&memo.scalars, f.predicate, &mut predicates);
            node = first_logical_expr(memo, *node.children.first()?)?;
        }

        let Operator::LogicalScan(scan) = &node.op else {
            return None;
        };
        // Reject scans already injected by a prior MV rewrite (MV-on-MV).
        if scan.mv_rewritten_from.is_some() {
            return None;
        }
        predicates.extend(scan.predicates.iter().copied());

        // Composition map from the mid project (ColumnId -> expr over scan cols).
        let mut defs: HashMap<ColumnId, ScalarId> = HashMap::new();
        if let Some(p) = &mid_project {
            for item in &p.items {
                let composed = substitute_scalar(&mut memo.scalars, item.expr, &defs);
                defs.insert(item.output_column_id, composed);
            }
        }

        let (agg, outputs, shape) = match aggregate {
            Some(a) => {
                let group_by: Vec<ScalarId> = a
                    .group_by
                    .iter()
                    .map(|expr| substitute_scalar(&mut memo.scalars, *expr, &defs))
                    .collect();
                let aggregates: Vec<ScalarAggregateSpec> = a
                    .aggregates
                    .iter()
                    .map(|c| substitute_aggregate(&mut memo.scalars, c, &defs))
                    .collect::<Option<Vec<_>>>()?;
                if a.output_columns.len() != a.group_by.len() + a.aggregates.len() {
                    return None;
                }
                let mut agg_outputs: Vec<SpjgOutput> = Vec::new();
                for (i, oc) in a.output_columns.iter().enumerate() {
                    let out_expr = if i < a.group_by.len() {
                        SpjgOutputExpr::Dimension(group_by[i])
                    } else {
                        SpjgOutputExpr::Aggregate(aggregates[i - a.group_by.len()].clone())
                    };
                    agg_outputs.push(SpjgOutput {
                        name: oc.name.clone(),
                        column_id: oc.column_id,
                        expr: out_expr,
                    });
                }
                (
                    Some(SpjgAggregate { group_by }),
                    agg_outputs,
                    MatchedShape::Spjg {
                        original_agg: a.clone(),
                    },
                )
            }
            None => {
                let outputs: Vec<SpjgOutput> = match mid_project {
                    Some(p) => p
                        .items
                        .iter()
                        .map(|item| SpjgOutput {
                            name: item.output_name.clone(),
                            column_id: item.output_column_id,
                            expr: SpjgOutputExpr::Dimension(substitute_scalar(
                                &mut memo.scalars,
                                item.expr,
                                &defs,
                            )),
                        })
                        .collect(),
                    // No surviving Project: the scan's output IS the subtree
                    // output. Honor the scan's pruned `required_columns` when
                    // present so the descriptor reflects the columns the plan
                    // actually produces. Without this, columns the optimizer
                    // prunes away — the row-lineage metadata columns (`_file`,
                    // `_pos`, `_row_id`, `_last_updated_sequence_number`) that
                    // are mandatory on format-v3 row-lineage Iceberg base
                    // tables, plus any unreferenced data column — leak into the
                    // SPJ output set and never map onto the MV's narrower
                    // output list, so every SPJ rewrite is rejected. The
                    // physical scan is pruned to exactly `required_columns`, so
                    // matching on that set is sound. `None` keeps the full
                    // column list (the planner did not record a pruned set).
                    None => {
                        let pruned: Option<&Vec<String>> = scan.required_columns.as_ref();
                        scan.columns
                            .iter()
                            .filter(|c| pruned.is_none_or(|req| req.contains(&c.name)))
                            .map(|c| SpjgOutput {
                                name: c.name.clone(),
                                column_id: c.column_id,
                                expr: SpjgOutputExpr::Dimension(column_ref(&mut memo.scalars, c)),
                            })
                            .collect()
                    }
                };
                (None, outputs, MatchedShape::Spj)
            }
        };

        Some((
            SpjgDescriptor {
                table: scan.table.clone(),
                scan_columns: scan.columns.clone(),
                predicates,
                aggregate: agg,
                outputs,
            },
            shape,
        ))
    }
}

/// Rebind outputs through an optional top project. MVP: top project items
/// must be bare ColumnRefs into the inputs (renames only); complex exprs
/// over aggregate results reject the shape.
fn apply_top_project(
    arena: &mut ScalarArena,
    project: Option<&ProjectOp>,
    inputs: Vec<SpjgOutput>,
) -> Result<Vec<SpjgOutput>, String> {
    let Some(p) = project else {
        return Ok(inputs);
    };
    let by_id: HashMap<ColumnId, &SpjgOutput> = inputs.iter().map(|o| (o.column_id, o)).collect();
    p.items
        .iter()
        .map(|item| match arena.node(item.expr) {
            ScalarNode::ColumnRef(column_id) => by_id
                .get(column_id)
                .map(|o| SpjgOutput {
                    name: item.output_name.clone(),
                    column_id: item.output_column_id,
                    expr: o.expr.clone(),
                })
                .ok_or_else(|| "top project references unknown column".to_string()),
            // A computed top-project item over a pure-dimension input can be
            // composed; over aggregate outputs it is rejected (MVP).
            _ => {
                let mut defs: HashMap<ColumnId, ScalarId> = HashMap::new();
                for o in &inputs {
                    match &o.expr {
                        SpjgOutputExpr::Dimension(e) => {
                            defs.insert(o.column_id, *e);
                        }
                        SpjgOutputExpr::Aggregate(_) => {}
                    }
                }
                let composed = substitute_scalar(arena, item.expr, &defs);
                if references_any(arena, composed, &inputs_agg_ids(&inputs)) {
                    Err("computed top-project over aggregate output (unsupported)".to_string())
                } else {
                    Ok(SpjgOutput {
                        name: item.output_name.clone(),
                        column_id: item.output_column_id,
                        expr: SpjgOutputExpr::Dimension(composed),
                    })
                }
            }
        })
        .collect()
}

fn first_logical_expr(memo: &Memo, gid: usize) -> Option<MExpr> {
    memo.groups[gid].logical_exprs.first().cloned()
}

fn inputs_agg_ids(inputs: &[SpjgOutput]) -> Vec<ColumnId> {
    inputs
        .iter()
        .filter(|o| matches!(o.expr, SpjgOutputExpr::Aggregate(_)))
        .map(|o| o.column_id)
        .collect()
}

fn references_any(arena: &ScalarArena, expr: ScalarId, ids: &[ColumnId]) -> bool {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => ids.contains(column_id),
        node => scalar_children(node)
            .into_iter()
            .any(|child| references_any(arena, child, ids)),
    }
}

fn aggregate_has_order_by(agg: &LogicalAggregateOp) -> bool {
    agg.aggregates.iter().any(|call| !call.order_by.is_empty())
}

fn substitute_aggregate(
    arena: &mut ScalarArena,
    call: &ScalarAggregateSpec,
    defs: &HashMap<ColumnId, ScalarId>,
) -> Option<ScalarAggregateSpec> {
    if !call.order_by.is_empty() {
        return None;
    }
    Some(ScalarAggregateSpec {
        name: call.name.clone(),
        args: call
            .args
            .iter()
            .map(|arg| substitute_scalar(arena, *arg, defs))
            .collect(),
        distinct: call.distinct,
        order_by: vec![],
    })
}

fn substitute_sort_key(
    arena: &mut ScalarArena,
    key: &SortKey,
    defs: &HashMap<ColumnId, ScalarId>,
) -> SortKey {
    SortKey {
        expr: substitute_scalar(arena, key.expr, defs),
        asc: key.asc,
        nulls_first: key.nulls_first,
        display: key.display.clone(),
    }
}

pub(crate) fn substitute_scalar(
    arena: &mut ScalarArena,
    expr: ScalarId,
    defs: &HashMap<ColumnId, ScalarId>,
) -> ScalarId {
    if let ScalarNode::ColumnRef(column_id) = arena.node(expr)
        && let Some(replacement) = defs.get(column_id)
    {
        return *replacement;
    }
    let original = arena.node(expr).clone();
    let rewritten = match original {
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            return expr;
        }
        ScalarNode::BinaryOp { op, left, right } => ScalarNode::BinaryOp {
            op,
            left: substitute_scalar(arena, left, defs),
            right: substitute_scalar(arena, right, defs),
        },
        ScalarNode::UnaryOp { op, child } => ScalarNode::UnaryOp {
            op,
            child: substitute_scalar(arena, child, defs),
        },
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
        } => ScalarNode::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute_scalar(arena, arg, defs))
                .collect(),
            distinct,
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => ScalarNode::AggregateCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute_scalar(arena, arg, defs))
                .collect(),
            distinct,
            order_by: order_by
                .iter()
                .map(|key| substitute_sort_key(arena, key, defs))
                .collect(),
        },
        ScalarNode::Cast { child, target } => ScalarNode::Cast {
            child: substitute_scalar(arena, child, defs),
            target,
        },
        ScalarNode::IsNull { child, negated } => ScalarNode::IsNull {
            child: substitute_scalar(arena, child, defs),
            negated,
        },
        ScalarNode::InList {
            child,
            list,
            negated,
        } => ScalarNode::InList {
            child: substitute_scalar(arena, child, defs),
            list: list
                .into_iter()
                .map(|item| substitute_scalar(arena, item, defs))
                .collect(),
            negated,
        },
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => ScalarNode::Between {
            child: substitute_scalar(arena, child, defs),
            low: substitute_scalar(arena, low, defs),
            high: substitute_scalar(arena, high, defs),
            negated,
        },
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => ScalarNode::Like {
            child: substitute_scalar(arena, child, defs),
            pattern: substitute_scalar(arena, pattern, defs),
            negated,
        },
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => ScalarNode::Case {
            operand: operand.map(|item| substitute_scalar(arena, item, defs)),
            when_then: when_then
                .into_iter()
                .map(|(when, then)| {
                    (
                        substitute_scalar(arena, when, defs),
                        substitute_scalar(arena, then, defs),
                    )
                })
                .collect(),
            else_expr: else_expr.map(|item| substitute_scalar(arena, item, defs)),
        },
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => ScalarNode::IsTruthValue {
            child: substitute_scalar(arena, child, defs),
            value,
            negated,
        },
        ScalarNode::Nested(inner) => ScalarNode::Nested(substitute_scalar(arena, inner, defs)),
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ScalarNode::WindowCall {
            name,
            args: args
                .into_iter()
                .map(|arg| substitute_scalar(arena, arg, defs))
                .collect(),
            distinct,
            partition_by: partition_by
                .into_iter()
                .map(|item| substitute_scalar(arena, item, defs))
                .collect(),
            order_by: order_by
                .iter()
                .map(|key| substitute_sort_key(arena, key, defs))
                .collect(),
            window_frame,
            ignore_nulls,
        },
        ScalarNode::LambdaFunction { params, body } => ScalarNode::LambdaFunction {
            params,
            body: substitute_scalar(arena, body, defs),
        },
        ScalarNode::Lambda { params, body } => ScalarNode::Lambda {
            params,
            body: substitute_scalar(arena, body, defs),
        },
    };
    arena.intern(
        rewritten,
        arena.data_type(expr).clone(),
        arena.nullable(expr),
    )
}

fn scalar_children(node: &ScalarNode) -> Vec<ScalarId> {
    match node {
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            vec![]
        }
        ScalarNode::BinaryOp { left, right, .. } => vec![*left, *right],
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::Nested(child)
        | ScalarNode::IsTruthValue { child, .. } => vec![*child],
        ScalarNode::FunctionCall { args, .. } | ScalarNode::AggregateCall { args, .. } => {
            args.clone()
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => vec![*body],
        ScalarNode::InList { child, list, .. } => {
            let mut out = Vec::with_capacity(1 + list.len());
            out.push(*child);
            out.extend(list.iter().copied());
            out
        }
        ScalarNode::Between {
            child, low, high, ..
        } => vec![*child, *low, *high],
        ScalarNode::Like { child, pattern, .. } => vec![*child, *pattern],
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut out = Vec::with_capacity(operand.iter().count() + when_then.len() * 2 + 1);
            out.extend(operand.iter().copied());
            for (when, then) in when_then {
                out.push(*when);
                out.push(*then);
            }
            out.extend(else_expr.iter().copied());
            out
        }
        ScalarNode::WindowCall {
            args, partition_by, ..
        } => {
            let mut out = Vec::with_capacity(args.len() + partition_by.len());
            out.extend(args.iter().copied());
            out.extend(partition_by.iter().copied());
            out
        }
    }
}

pub(crate) fn column_ref(arena: &mut ScalarArena, c: &OutputColumn) -> ScalarId {
    arena.remember_project_output_display(c.column_id, None, c.name.clone());
    arena.intern(
        ScalarNode::ColumnRef(c.column_id),
        c.data_type.clone(),
        c.nullable,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::plan::{
        AggregateCall, LogicalAggregateNode, LogicalFilterNode, LogicalPlanNode, LogicalScanNode,
        LogicalSortNode, PlanNodeKind,
    };
    use arrow::datatypes::DataType;

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn col_ref(c: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: c.column_id,
                qualifier: None,
                column: c.name.clone(),
            },
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn cmp(left: TypedExpr, op: crate::sql::analysis::BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    /// Simplest `ScanSource` to construct in unit tests. The descriptor logic
    /// never inspects `ScanSource`, so the cheap `StarRocks` variant (two i64s)
    /// is sufficient and avoids building a heavy `IcebergTableInfo`.
    fn test_scan_source() -> ScanSource {
        ScanSource::StarRocks {
            db_id: 0,
            table_id: 0,
        }
    }

    fn scan(cols: &[OutputColumn]) -> LogicalScanNode {
        LogicalScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: cols
                    .iter()
                    .map(|c| ColumnDef {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: test_scan_source(),
            },
            alias: None,
            columns: cols.to_vec(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    fn scan_plan(cols: &[OutputColumn]) -> LogicalPlanNode {
        LogicalPlanNode::new(PlanNodeKind::Scan(scan(cols)), vec![], None)
    }

    fn descriptor_from_plan(
        plan: &LogicalPlanNode,
    ) -> Result<(SpjgDescriptor, ScalarArena), String> {
        let mut arena = ScalarArena::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            plan, &mut arena,
        )?;
        let descriptor = SpjgDescriptor::from_opt_expr(&opt_expr, &mut arena)?;
        Ok((descriptor, arena))
    }

    #[test]
    fn extracts_filter_scan_shape() {
        let a = col(1, "a");
        let b = col(2, "b");
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: cmp(col_ref(&a), crate::sql::analysis::BinOp::Ge, int_lit(5)),
            }),
            vec![scan_plan(&[a.clone(), b.clone()])],
            None,
        );
        let (d, _arena) = descriptor_from_plan(&plan).expect("spjg");
        assert_eq!(d.table.name, "t");
        assert_eq!(d.predicates.len(), 1);
        assert!(d.aggregate.is_none());
        assert_eq!(d.outputs.len(), 2); // pass-through scan columns
    }

    #[test]
    fn extracts_aggregate_shape_and_rejects_join() {
        let a = col(1, "a");
        let v = col(2, "v");
        let sum_out = col(3, "s");
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref(&v)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: sum_out.column_id,
                }],
                output_columns: vec![col(1, "a"), sum_out.clone()],
                already_pushed: false,
            }),
            vec![scan_plan(&[a.clone(), v.clone()])],
            None,
        );
        let (d, _arena) = descriptor_from_plan(&plan).expect("spjg");
        let agg = d.aggregate.as_ref().expect("aggregate present");
        assert_eq!(agg.group_by.len(), 1);
        // outputs: Dimension(a) then Aggregate(sum(v)); the aggregate call is
        // captured in `outputs`, not duplicated on `SpjgAggregate`.
        assert_eq!(d.outputs.len(), 2);
        assert!(matches!(d.outputs[0].expr, SpjgOutputExpr::Dimension(_)));
        assert!(matches!(d.outputs[1].expr, SpjgOutputExpr::Aggregate(_)));
    }

    #[test]
    fn rejects_sort_and_window() {
        // Any node outside {Scan, Filter, Project, Aggregate} must yield Err.
        let a = col(1, "a");
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Sort(LogicalSortNode {
                items: vec![],
                analytic_partition_by: vec![],
                output_columns: vec![],
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            vec![scan_plan(std::slice::from_ref(&a))],
            None,
        );
        assert!(descriptor_from_plan(&plan).is_err());
    }

    /// Build a memo from a plan and return the first logical expr of the root
    /// group (cloned) plus the memo, ready for `from_memo`.
    fn memo_root(plan: &LogicalPlanNode) -> (crate::sql::optimizer::memo::Memo, MExpr) {
        let mut memo = crate::sql::optimizer::memo::Memo::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            plan,
            &mut memo.scalars,
        )
        .expect("logical plan to opt expr");
        let root = crate::sql::optimizer::memo_copy::opt_expr_to_memo(&opt_expr, &mut memo);
        let root_expr = memo.groups[root].logical_exprs[0].clone();
        (memo, root_expr)
    }

    #[test]
    fn from_memo_matches_filter_scan() {
        let a = col(1, "a");
        let b = col(2, "b");
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: cmp(col_ref(&a), crate::sql::analysis::BinOp::Ge, int_lit(5)),
            }),
            vec![scan_plan(&[a.clone(), b.clone()])],
            None,
        );
        let (logical, _arena) = descriptor_from_plan(&plan).expect("spjg");
        let (mut memo, root_expr) = memo_root(&plan);
        let (mem, shape) = SpjgDescriptor::from_memo(&root_expr, &mut memo).expect("from_memo");
        assert!(matches!(shape, MatchedShape::Spj));
        assert_eq!(mem.table.name, logical.table.name);
        assert_eq!(mem.predicates.len(), logical.predicates.len());
        assert!(mem.aggregate.is_none());
        assert_eq!(mem.outputs.len(), logical.outputs.len());
    }

    #[test]
    fn from_memo_matches_aggregate() {
        let a = col(1, "a");
        let v = col(2, "v");
        let sum_out = col(3, "s");
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col_ref(&v)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: sum_out.column_id,
                }],
                output_columns: vec![col(1, "a"), sum_out.clone()],
                already_pushed: false,
            }),
            vec![scan_plan(&[a.clone(), v.clone()])],
            None,
        );
        let (logical, _arena) = descriptor_from_plan(&plan).expect("spjg");
        let (mut memo, root_expr) = memo_root(&plan);
        let (mem, shape) = SpjgDescriptor::from_memo(&root_expr, &mut memo).expect("from_memo");
        // Shape carries the original aggregate op for output-id reuse.
        let MatchedShape::Spjg { original_agg } = &shape else {
            panic!("expected Spjg shape");
        };
        assert_eq!(original_agg.output_columns.len(), 2);
        assert_eq!(original_agg.stage, AggStage::Single);
        let agg = mem.aggregate.as_ref().expect("aggregate present");
        let logical_agg = logical.aggregate.as_ref().expect("aggregate present");
        assert_eq!(agg.group_by.len(), logical_agg.group_by.len());
        // Aggregate-call parity is covered by output-length equality
        // (outputs = group keys + aggregates) below.
        assert_eq!(mem.outputs.len(), logical.outputs.len());
    }

    #[test]
    fn from_memo_rejects_split_aggregate() {
        use crate::sql::optimizer::operator::{LogicalAggregateOp, Operator};
        use crate::sql::planner::optimizer_bridge::scalar::{intern_aggregate_calls, intern_exprs};
        // A split (Local) aggregate is not the original Single shape and must
        // be rejected even when it sits at the matched position.
        let a = col(1, "a");
        let v = col(2, "v");
        let sum_out = col(3, "s");
        let scan_op = scan(&[a.clone(), v.clone()]);
        let plan = LogicalPlanNode::new(PlanNodeKind::Scan(scan_op), vec![], None);
        let mut memo = crate::sql::optimizer::memo::Memo::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            &plan,
            &mut memo.scalars,
        )
        .expect("logical plan to opt expr");
        let scan_gid = crate::sql::optimizer::memo_copy::opt_expr_to_memo(&opt_expr, &mut memo);
        let group_by = intern_exprs(&mut memo.scalars, &[col_ref(&a)]);
        let aggregates = intern_aggregate_calls(
            &mut memo.scalars,
            &[AggregateCall {
                name: "sum".to_string(),
                args: vec![col_ref(&v)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: sum_out.column_id,
            }],
        );
        let split = LogicalAggregateOp::staged(
            AggStage::Local,
            group_by,
            aggregates,
            vec![col(1, "a"), sum_out.clone()],
            vec![false],
            true,
        );
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(split),
            children: vec![scan_gid],
        };
        assert!(SpjgDescriptor::from_memo(&expr, &mut memo).is_none());
    }
}
