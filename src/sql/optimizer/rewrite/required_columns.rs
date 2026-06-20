//! Phase 1 of column pruning: top-down tagging pass.
//!
//! Walks the logical plan tree and writes `required_output_columns:
//! Option<HashSet<ColumnId>>` on every operator node based on what the
//! *parent* operator needs.
//!
//! Semantics:
//! - `parent_needed = None` at the root means "all outputs required".
//! - `Some(set)` means "downstream needs exactly this ColumnId set".
//! - After this pass every node has `Some(_)` so Phase-2 pruning rules
//!   can read a local tag without recursing.
//!
//! This module does **not** prune anything.  Pruning (removing items /
//! output_columns entries) is done in Phase-2 `Prune*Columns` rules.
//!
//! Spec: `docs/design/specs/2026-05-28-oq-1-column-pruning-arch-refactor-design.md` §5.

use std::collections::{HashMap, HashSet};

use crate::sql::column_id::ColumnId;
use crate::sql::common::CteId;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_output_ids_opt, collect_output_ids_ordered_opt,
};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Walk `expr` top-down and stamp `required_output_columns` on every operator.
///
/// `parent_needed = None` means the root has no caller restriction (all outputs
/// required).  Each operator type computes its own child's needed set and
/// recurses.
pub(crate) fn tag_required_columns(
    expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    match &expr.op {
        Operator::LogicalScan(_) => tag_scan(expr, parent_needed),
        Operator::LogicalValues(_) => tag_values(expr, parent_needed),
        Operator::LogicalGenerateSeries(_) => tag_generate_series(expr, parent_needed),
        Operator::LogicalProject(_) => tag_project(expr, arena, parent_needed),
        Operator::LogicalFilter(_) => tag_filter(expr, arena, parent_needed),
        Operator::LogicalSort(_) => tag_sort(expr, arena, parent_needed),
        Operator::LogicalLimit(_) => tag_limit(expr, arena, parent_needed),
        Operator::LogicalAggregate(_) => tag_aggregate(expr, arena, parent_needed),
        Operator::LogicalJoin(_) => tag_join(expr, arena, parent_needed),
        Operator::LogicalUnion(_) => tag_union(expr, arena, parent_needed),
        Operator::LogicalIntersect(_) => tag_intersect(expr, arena, parent_needed),
        Operator::LogicalExcept(_) => tag_except(expr, arena, parent_needed),
        Operator::LogicalCTEAnchor(_) => tag_cte_anchor(expr, arena, parent_needed),
        Operator::LogicalCTEConsume(_) => tag_cte_consume(expr, parent_needed),
        Operator::LogicalCTEProduce(_) => tag_cte_produce(expr, arena, parent_needed),
        Operator::LogicalWindow(_) => tag_window(expr, arena, parent_needed),
        Operator::LogicalRepeat(_) => tag_repeat(expr, arena, parent_needed),
        Operator::LogicalDecode(_) => tag_decode(expr, arena, parent_needed),
        Operator::LogicalAggregateStateMerge(_) => {
            tag_aggregate_state_merge(expr, arena, parent_needed)
        }
        Operator::LogicalTableFunction(_) => tag_table_function(expr, arena, parent_needed),
        Operator::LogicalAssertOneRow(_) => tag_assert_one_row(expr, arena, parent_needed),
        _ => {
            // Conservative: require all below (Apply, IMV markers, physical ops).
            let children = expr.children;
            OptExpr {
                op: expr.op,
                children: children
                    .into_iter()
                    .map(|child| tag_required_columns(child, arena, None))
                    .collect(),
                required_output_columns: None,
            }
        }
    }
}

fn tag_aggregate_state_merge(
    expr: OptExpr,
    arena: &ScalarArena,
    _parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalAggregateStateMerge(_)));
    let mut children = expr.children;
    let delta_input = children.pop().unwrap();
    let old_input = children.pop().unwrap();
    OptExpr {
        op: expr.op,
        children: vec![
            tag_required_columns(old_input, arena, None),
            tag_required_columns(delta_input, arena, None),
        ],
        required_output_columns: expr.required_output_columns,
    }
}

/// Conservative: require everything below, prune nothing.
fn tag_assert_one_row(
    expr: OptExpr,
    arena: &ScalarArena,
    _parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalAssertOneRow(_)));
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, None)],
        required_output_columns: None,
    }
}

// ---------------------------------------------------------------------------
// Leaf handlers
// ---------------------------------------------------------------------------

fn tag_scan(mut expr: OptExpr, parent_needed: Option<HashSet<ColumnId>>) -> OptExpr {
    let Operator::LogicalScan(scan) = &expr.op else {
        unreachable!()
    };
    let needed =
        parent_needed.unwrap_or_else(|| scan.columns.iter().map(|c| c.column_id).collect());
    expr.required_output_columns = Some(needed);
    expr
}

fn tag_values(mut expr: OptExpr, parent_needed: Option<HashSet<ColumnId>>) -> OptExpr {
    let Operator::LogicalValues(node) = &expr.op else {
        unreachable!()
    };
    let needed =
        parent_needed.unwrap_or_else(|| node.columns.iter().map(|c| c.column_id).collect());
    expr.required_output_columns = Some(needed);
    expr
}

/// GenerateSeries is a leaf with one output ColumnId.  Like Scan/Values, a
/// `None` parent means all leaf outputs are required.
fn tag_generate_series(mut expr: OptExpr, parent_needed: Option<HashSet<ColumnId>>) -> OptExpr {
    let Operator::LogicalGenerateSeries(node) = &expr.op else {
        unreachable!()
    };
    let needed = parent_needed.unwrap_or_else(|| {
        if node.output_column_id == ColumnId::UNSET {
            HashSet::new()
        } else {
            HashSet::from([node.output_column_id])
        }
    });
    expr.required_output_columns = Some(needed);
    expr
}

// ---------------------------------------------------------------------------
// Unary handlers
// ---------------------------------------------------------------------------

fn collect_scalar_column_id_refs(arena: &ScalarArena, expr: ScalarId) -> HashSet<ColumnId> {
    let mut out = HashSet::new();
    collect_scalar_column_id_refs_inner(arena, expr, &mut out);
    out
}

fn collect_scalar_column_id_refs_inner(
    arena: &ScalarArena,
    expr: ScalarId,
    out: &mut HashSet<ColumnId>,
) {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => {
            if *column_id != ColumnId::UNSET {
                out.insert(*column_id);
            }
        }
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_scalar_column_id_refs_inner(arena, *left, out);
            collect_scalar_column_id_refs_inner(arena, *right, out);
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => collect_scalar_column_id_refs_inner(arena, *child, out),
        ScalarNode::FunctionCall { args, .. } => {
            for arg in args {
                collect_scalar_column_id_refs_inner(arena, *arg, out);
            }
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            collect_scalar_column_id_refs_inner(arena, *body, out);
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_scalar_column_id_refs_inner(arena, *arg, out);
            }
            for item in order_by {
                collect_scalar_column_id_refs_inner(arena, item.expr, out);
            }
        }
        ScalarNode::InList { child, list, .. } => {
            collect_scalar_column_id_refs_inner(arena, *child, out);
            for item in list {
                collect_scalar_column_id_refs_inner(arena, *item, out);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_scalar_column_id_refs_inner(arena, *child, out);
            collect_scalar_column_id_refs_inner(arena, *low, out);
            collect_scalar_column_id_refs_inner(arena, *high, out);
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_scalar_column_id_refs_inner(arena, *child, out);
            collect_scalar_column_id_refs_inner(arena, *pattern, out);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_scalar_column_id_refs_inner(arena, *operand, out);
            }
            for (when, then) in when_then {
                collect_scalar_column_id_refs_inner(arena, *when, out);
                collect_scalar_column_id_refs_inner(arena, *then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_scalar_column_id_refs_inner(arena, *else_expr, out);
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_scalar_column_id_refs_inner(arena, *arg, out);
            }
            for item in partition_by {
                collect_scalar_column_id_refs_inner(arena, *item, out);
            }
            for item in order_by {
                collect_scalar_column_id_refs_inner(arena, item.expr, out);
            }
        }
    }
}

fn scalar_is_assert_true(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(
        arena.node(expr),
        ScalarNode::FunctionCall { name, .. } if name == "assert_true"
    )
}

fn tag_project(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    let Operator::LogicalProject(node) = &expr.op else {
        unreachable!()
    };
    expr.required_output_columns = parent_needed.clone();
    // child_needed = union of ColumnRefs of items whose output_column_id is in
    // parent_needed (or all items when parent_needed is None).
    //
    // assert_true items are ALWAYS included in child_needed regardless of
    // parent_needed: they carry runtime correctness checks (e.g. the per-group
    // row-check from ScalarApplyToJoin) whose column refs (e.g. the count
    // column from the grouping aggregate) must remain available to the child.
    // This mirrors the StarRocks PruneProjectColumnsRule carve-out.
    let child_needed: HashSet<ColumnId> = node
        .items
        .iter()
        .filter(|item| match &parent_needed {
            None => true,
            Some(n) => {
                let is_needed = n.contains(&item.output_column_id);
                let is_assert_true = scalar_is_assert_true(arena, item.expr);
                is_needed || is_assert_true
            }
        })
        .flat_map(|item| collect_scalar_column_id_refs(arena, item.expr))
        .collect();
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, Some(child_needed))],
        required_output_columns: parent_needed,
    }
}

fn tag_filter(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    let Operator::LogicalFilter(node) = &expr.op else {
        unreachable!()
    };
    expr.required_output_columns = parent_needed.clone();
    // Child needs everything the parent needs PLUS all columns referenced in
    // the predicate.  When parent_needed is None (keep all), propagate None so
    // the child also keeps all columns instead of collapsing to just the
    // predicate refs.
    let child_needed = parent_needed.as_ref().map(|needed| {
        let mut child = needed.clone();
        child.extend(collect_scalar_column_id_refs(arena, node.predicate));
        child
    });
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, child_needed)],
        required_output_columns: parent_needed,
    }
}

fn tag_sort(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    let Operator::LogicalSort(node) = &expr.op else {
        unreachable!()
    };
    expr.required_output_columns = parent_needed.clone();
    // When parent_needed is None (keep all), propagate None so the child also
    // keeps all columns instead of collapsing to just the sort-key refs.
    let child_needed = parent_needed.as_ref().map(|needed| {
        let mut child = needed.clone();
        for item in &node.items {
            child.extend(collect_scalar_column_id_refs(arena, item.expr));
        }
        for &sid in &node.analytic_partition_exprs {
            child.extend(collect_scalar_column_id_refs(arena, sid));
        }
        child
    });
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, child_needed)],
        required_output_columns: parent_needed,
    }
}

fn tag_limit(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalLimit(_)));
    expr.required_output_columns = parent_needed.clone();
    // Limit is transparent: passes parent_needed straight through.
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, parent_needed.clone())],
        required_output_columns: parent_needed,
    }
}

fn tag_aggregate(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    let Operator::LogicalAggregate(node) = &expr.op else {
        unreachable!()
    };
    expr.required_output_columns = parent_needed.clone();

    // Conservative keep-all-aggregate-inputs strategy.
    //
    // Aggregate output metadata starts with the group-by output prefix used by
    // the physical layout, while aggregate function identity lives on
    // ScalarAggregateSpec (no per-call output_column_id in the optimizer IR).
    // Required input derivation should not infer liveness from output positions;
    // if the aggregate node is live at all, every expression it consumes remains
    // needed.
    //
    // Conservative fix: child always needs ALL group-by column refs PLUS ALL
    // aggregate args and order-by column refs, regardless of parent_needed.
    // This matches the semantics of the old name-based PruneColumns pass and
    // is correct: if the aggregate node is live at all, every input column it
    // consumes is required.  Per-aggregate output pruning (Gap 5) is a
    // follow-up.
    //
    // None-propagation discipline: when parent_needed is None (root / keep-all),
    // pass None to the child so it also keeps all its columns.
    let child_needed: Option<HashSet<ColumnId>> = parent_needed.as_ref().map(|_| {
        let mut needed: HashSet<ColumnId> = HashSet::new();
        for &gb in &node.group_by {
            needed.extend(collect_scalar_column_id_refs(arena, gb));
        }
        for agg in &node.aggregates {
            for &arg in &agg.args {
                needed.extend(collect_scalar_column_id_refs(arena, arg));
            }
            for item in &agg.order_by {
                needed.extend(collect_scalar_column_id_refs(arena, item.expr));
            }
        }
        needed
    });

    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, child_needed)],
        required_output_columns: parent_needed,
    }
}

fn tag_window(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalWindow(_)));
    expr.required_output_columns = parent_needed.clone();
    // Window output columns carry fresh ColumnIds (allocated by the planner)
    // that are distinct from the child's ids, so we cannot reliably map
    // parent_needed back to child column ids.  Pass None to the child so all
    // input columns are preserved and no column is spuriously dropped.
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, None)],
        required_output_columns: parent_needed,
    }
}

fn tag_repeat(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    let Operator::LogicalRepeat(node) = &expr.op else {
        unreachable!()
    };
    expr.required_output_columns = parent_needed.clone();
    let child_needed = if parent_needed.is_none() {
        None
    } else if node.all_rollup_column_ids.len() == node.all_rollup_columns.len() {
        let grouping_output_ids: HashSet<ColumnId> = node
            .grouping_fn_ids
            .iter()
            .map(|(_, column_id)| *column_id)
            .collect();
        let mut needed = parent_needed.clone().unwrap_or_default();
        needed.retain(|column_id| !grouping_output_ids.contains(column_id));
        needed.extend(node.all_rollup_column_ids.iter().copied());
        Some(needed)
    } else {
        None
    };
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, child_needed)],
        required_output_columns: parent_needed,
    }
}

/// Decode node: ColumnIds are pass-through transparent (same ids on both sides
/// of the decode boundary per the rewriter invariant). Pass parent_needed to
/// the child unchanged.
fn tag_decode(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalDecode(_)));
    expr.required_output_columns = parent_needed.clone();
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, parent_needed.clone())],
        required_output_columns: parent_needed,
    }
}

fn tag_table_function(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalTableFunction(_)));
    expr.required_output_columns = parent_needed.clone();
    // The function's args reference INPUT columns that may not appear in
    // parent_needed (e.g. UNNEST(t.arr) where parent only sees the exploded
    // output).  Pass None to the child so no input column is spuriously dropped.
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, None)],
        required_output_columns: parent_needed,
    }
}

// ---------------------------------------------------------------------------
// Binary / n-ary handlers
// ---------------------------------------------------------------------------

fn tag_join(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    let Operator::LogicalJoin(node) = &expr.op else {
        unreachable!()
    };
    let result_needed = parent_needed.clone();
    expr.required_output_columns = result_needed.clone();

    // When parent_needed is None (keep all), propagate None to both children so
    // they also keep all columns.  When Some, compute combined = parent_needed ∪
    // condition refs, then split by which child produces each id.
    let (left_needed, right_needed) = match parent_needed {
        None => (None, None),
        Some(mut combined) => {
            if let Some(cond_id) = node.condition {
                combined.extend(collect_scalar_column_id_refs(arena, cond_id));
            }
            let left_outputs = collect_output_ids_opt(expr.left());
            let right_outputs = collect_output_ids_opt(expr.right());
            let left: HashSet<ColumnId> = combined
                .iter()
                .filter(|id| left_outputs.contains(id))
                .copied()
                .collect();
            let right: HashSet<ColumnId> = combined
                .iter()
                .filter(|id| right_outputs.contains(id))
                .copied()
                .collect();
            (Some(left), Some(right))
        }
    };

    let mut children = expr.children;
    let right = children.pop().unwrap();
    let left = children.pop().unwrap();
    OptExpr {
        op: expr.op,
        children: vec![
            tag_required_columns(left, arena, left_needed),
            tag_required_columns(right, arena, right_needed),
        ],
        required_output_columns: result_needed,
    }
}

// ---------------------------------------------------------------------------
// Set operation handlers (Gap 4)
// ---------------------------------------------------------------------------

fn tag_union(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    let Operator::LogicalUnion(node) = &expr.op else {
        unreachable!()
    };

    if !node.all {
        let children = expr.children;
        return OptExpr {
            op: expr.op,
            children: children
                .into_iter()
                .map(|child| tag_required_columns(child, arena, None))
                .collect(),
            required_output_columns: parent_needed,
        };
    }

    // Resolve which positions in the output schema are needed.
    let outputs: Vec<ColumnId> = node.output_columns.iter().map(|c| c.column_id).collect();
    let needed_positions: Vec<usize> = match &parent_needed {
        None => (0..outputs.len()).collect(),
        Some(n) => outputs
            .iter()
            .enumerate()
            .filter_map(|(i, id)| n.contains(id).then_some(i))
            .collect(),
    };

    expr.required_output_columns = parent_needed.clone();
    let children = expr.children;
    OptExpr {
        op: expr.op,
        children: children
            .into_iter()
            .map(|child| {
                let child_outputs = collect_output_ids_ordered_opt(&child);
                let child_needed: HashSet<ColumnId> = needed_positions
                    .iter()
                    .filter_map(|&i| child_outputs.get(i).copied())
                    .collect();
                tag_required_columns(child, arena, Some(child_needed))
            })
            .collect(),
        required_output_columns: parent_needed,
    }
}

fn tag_intersect(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalIntersect(_)));
    expr.required_output_columns = parent_needed.clone();
    let children = expr.children;
    OptExpr {
        op: expr.op,
        children: children
            .into_iter()
            .map(|child| tag_required_columns(child, arena, None))
            .collect(),
        required_output_columns: parent_needed,
    }
}

fn tag_except(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalExcept(_)));
    expr.required_output_columns = parent_needed.clone();
    let children = expr.children;
    OptExpr {
        op: expr.op,
        children: children
            .into_iter()
            .map(|child| tag_required_columns(child, arena, None))
            .collect(),
        required_output_columns: parent_needed,
    }
}

// ---------------------------------------------------------------------------
// CTE handlers (Gap 3 — two-walk pattern)
// ---------------------------------------------------------------------------

fn tag_cte_consume(mut expr: OptExpr, parent_needed: Option<HashSet<ColumnId>>) -> OptExpr {
    let Operator::LogicalCTEConsume(node) = &expr.op else {
        unreachable!()
    };
    // Leaf in this walk — always store Some(_) so that subtree_untagged
    // returns false after tagging.  When parent_needed is None (no restriction
    // from above), default to keeping all of this node's own output ids, which
    // is the correct "keep-all" signal for the CTE two-walk.
    expr.required_output_columns = Some(
        parent_needed.unwrap_or_else(|| node.output_columns.iter().map(|c| c.column_id).collect()),
    );
    expr
}

fn tag_cte_produce(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalCTEProduce(_)));
    expr.required_output_columns = parent_needed.clone();
    // The produce-side needed ids are already in the producer's output id
    // space (translate_consume_to_produce_ids mapped them).  Pass them
    // straight through to the CTE body.
    let mut children = expr.children;
    let input = children.remove(0);
    OptExpr {
        op: expr.op,
        children: vec![tag_required_columns(input, arena, parent_needed.clone())],
        required_output_columns: parent_needed,
    }
}

fn tag_cte_anchor(
    mut expr: OptExpr,
    arena: &ScalarArena,
    parent_needed: Option<HashSet<ColumnId>>,
) -> OptExpr {
    debug_assert!(matches!(expr.op, Operator::LogicalCTEAnchor(_)));

    // children[0] = produce, children[1] = consumer subtree
    let mut children = expr.children;
    let consumer = children.pop().unwrap();
    let produce = children.pop().unwrap();

    // --- Walk 1: tag the consumer subtree with parent_needed. ---
    // This stamps required_output_columns on every CTEConsume for this cte_id.
    let consumer = tag_required_columns(consumer, arena, parent_needed.clone());

    // --- Tag the producer subtree with None (keep all). ---
    //
    // Conservative choice: pass None to the CTEProduce body so all produce
    // columns survive, rather than computing a narrowed produce_needed set.
    //
    // Why conservative: PruneCTEProduceColumns and PruneCTEConsumeColumns are
    // no-ops (Gap-3 deferred).  The CTE multicast protocol sends ALL produce
    // columns to every consumer exchange node; each consumer reads them by
    // positional index.  A narrowed produce_needed set could prune leaf nodes
    // (e.g. Scan) below the produce correctly in isolation, but since the
    // produce's output_columns list is not pruned (no-op rule), the scan must
    // still produce ALL columns that the produce's output_columns list names.
    // Passing None ensures the scan's required_output_columns == all columns,
    // which is the safe invariant until Gap-3 is implemented.
    let produce = tag_required_columns(produce, arena, None);

    expr.children = vec![produce, consumer];
    expr.required_output_columns = parent_needed;
    expr
}

// ---------------------------------------------------------------------------
// CTE helpers
// ---------------------------------------------------------------------------

/// Recursively traverse `expr` and union all `required_output_columns` sets
/// from `CTEConsume` nodes whose `cte_id` matches `target_id` into `acc`.
fn collect_cte_consumer_needs(expr: &OptExpr, target_id: CteId, acc: &mut HashSet<ColumnId>) {
    match &expr.op {
        Operator::LogicalCTEConsume(c) if c.cte_id == target_id => {
            if let Some(req) = &expr.required_output_columns {
                acc.extend(req.iter().copied());
            }
            // A CTEConsume is a leaf; do not recurse further.
        }
        Operator::LogicalCTEConsume(_) => {
            // Different cte_id — skip.
        }
        _ => {
            for child in &expr.children {
                collect_cte_consumer_needs(child, target_id, acc);
            }
        }
    }
}

/// Build a map from `consume_side_column_id` → `position` for the first
/// matching `CTEConsume(target_id)` found in the subtree.
///
/// All consumers with the same `cte_id` share the same positional schema, so
/// we stop at the first match.  The position is the index into
/// `CTEConsume.output_columns`, which aligns with `CTEProduce.output_columns`.
fn find_consume_position_map(expr: &OptExpr, target_id: CteId) -> HashMap<ColumnId, usize> {
    let mut map = HashMap::new();
    walk_consume_position_map(expr, target_id, &mut map);
    map
}

fn walk_consume_position_map(expr: &OptExpr, target_id: CteId, map: &mut HashMap<ColumnId, usize>) {
    match &expr.op {
        Operator::LogicalCTEConsume(c) if c.cte_id == target_id => {
            // Record consume_side_column_id -> position for each output column.
            // Use `or_insert` so that if multiple consumers exist (multi-consumer
            // case), the first one wins — positions are identical across all
            // consumers with the same cte_id.
            for (i, col) in c.output_columns.iter().enumerate() {
                map.entry(col.column_id).or_insert(i);
            }
        }
        Operator::LogicalCTEConsume(_) => {
            // Different cte_id — skip.
        }
        _ => {
            for child in &expr.children {
                walk_consume_position_map(child, target_id, map);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TagRequiredColumns rewrite rule
// ---------------------------------------------------------------------------

/// Returns `true` when the plan tree rooted at `expr` has not yet been tagged
/// by the Phase-1 tagging pass.
///
/// **Why we check first-child rather than the root node itself**:
/// `tag_required_columns(root, arena, None)` stores `parent_needed = None` on the
/// root operator (semantics: "all outputs required, no restriction from the
/// parent"), but it ALWAYS stores `Some(_)` on every *leaf* node (Scan,
/// Values, GenerateSeries, CTEConsume).  Non-leaf nodes at the root that
/// received `parent_needed = None` therefore still carry
/// `required_output_columns = None` after being tagged.  Using the root's own
/// field as the guard would cause the rule to re-fire on every fixed-point
/// iteration.
///
/// The fix: for leaf nodes, check the node's own field (leaves always get
/// `Some(_)` after tagging).  For non-leaf nodes, check the first child's
/// field recursively — after tagging, the deepest leaf will have `Some(_)`.
fn subtree_untagged(expr: &OptExpr) -> bool {
    match &expr.op {
        // Leaves: always get `Some(_)` after tagging.
        Operator::LogicalScan(_)
        | Operator::LogicalValues(_)
        | Operator::LogicalGenerateSeries(_)
        | Operator::LogicalCTEConsume(_) => expr.required_output_columns.is_none(),
        // Non-leaves: check the first child (which will itself be a leaf or
        // recurse further until a leaf is reached).
        _ => expr
            .children
            .first()
            .map_or(false, |child| subtree_untagged(child)),
    }
}

/// Phase-1 tagging rule: walks the plan top-down via [`tag_required_columns`]
/// and stamps `required_output_columns` on every operator node.
///
/// The rule fires once per subtree: `matches` uses [`subtree_untagged`] which
/// checks the first reachable leaf rather than the root node itself.  This is
/// necessary because `tag_required_columns(root, arena, None)` stores `None` on
/// the root (semantics: "no parent restriction"), but always stores `Some(_)`
/// on leaf nodes.  After `apply` returns, all leaves carry `Some(_)`, so
/// `subtree_untagged` returns `false` and the rule does not re-fire.
///
/// TopDown driver post-`apply` child walk: after the root fires and tags the
/// whole tree, `rewrite_children` recurses into already-tagged children.
/// `matches` returns `false` for each (their leaves are `Some(_)`), so no
/// re-tagging occurs.
///
/// The pipeline's fixed-point loop re-runs the stage; on the second pass
/// `subtree_untagged == false` everywhere, `phase_changed == false`, and the
/// loop exits cleanly.
///
/// **No behavior change**: this pass only writes metadata.  Nothing reads
/// `required_output_columns` until the per-operator prune rules are registered
/// in a later task.
pub(crate) struct TagRequiredColumns;

impl LogicalRewriteRule for TagRequiredColumns {
    fn name(&self) -> &'static str {
        "TagRequiredColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        subtree_untagged(expr)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let arena_rc = ctx.scalar_arena();
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(expr, &arena, None);
        Ok(RewriteResult::Changed(tagged))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::cte::CteId;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::optimizer::operator::{
        CTEAnchorOp, CTEConsumeOp, CTEProduceOp, ExceptOp, FilterOp, GenerateSeriesOp, IntersectOp,
        LimitOp, LogicalAggregateOp, LogicalJoinOp, ProjectOp, RepeatOp, ScalarAggregateSpec,
        ScalarProjectItem, ScalarWindowSpec, ScanOp, SortOp, TableFunctionOp, UnionOp, ValuesOp,
        WindowOp,
    };
    use crate::sql::optimizer::scalar::{self, ScalarArena, SortKey};
    use arrow::datatypes::DataType;
    use std::cell::RefCell;
    use std::rc::Rc;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn make_arena() -> Rc<RefCell<ScalarArena>> {
        Rc::new(RefCell::new(ScalarArena::new()))
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref_scalar(
        arena: &mut ScalarArena,
        id: ColumnId,
    ) -> crate::sql::optimizer::scalar::ScalarId {
        let expr = crate::sql::analysis::TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: format!("c{}", id.0),
            },
            data_type: DataType::Int32,
            nullable: false,
        };
        crate::sql::planner::optimizer_bridge::scalar::intern_typed(arena, &expr)
    }

    fn int_literal_scalar(
        arena: &mut ScalarArena,
        v: i64,
    ) -> crate::sql::optimizer::scalar::ScalarId {
        let expr = crate::sql::analysis::TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        };
        crate::sql::planner::optimizer_bridge::scalar::intern_typed(arena, &expr)
    }

    fn binop_scalar(
        arena: &mut ScalarArena,
        left: crate::sql::optimizer::scalar::ScalarId,
        op: BinOp,
        right: crate::sql::optimizer::scalar::ScalarId,
    ) -> crate::sql::optimizer::scalar::ScalarId {
        let left_typed = crate::sql::planner::optimizer_bridge::scalar::materialize(arena, left);
        let right_typed = crate::sql::planner::optimizer_bridge::scalar::materialize(arena, right);
        let expr = crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left_typed),
                op,
                right: Box::new(right_typed),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        crate::sql::planner::optimizer_bridge::scalar::intern_typed(arena, &expr)
    }

    fn make_scan_with_ids(
        arena_rc: &Rc<RefCell<ScalarArena>>,
        id_a: u32,
        id_b: u32,
        id_c: u32,
    ) -> OptExpr {
        let _ = arena_rc; // scan predicates empty; arena not needed here
        let table = TableDef {
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
                ColumnDef {
                    name: "c".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "d".to_string(),
            table,
            alias: None,
            columns: vec![
                make_output_column(ColumnId::new_for_test(id_a), "a"),
                make_output_column(ColumnId::new_for_test(id_b), "b"),
                make_output_column(ColumnId::new_for_test(id_c), "c"),
            ],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn scan_with_3_cols(arena_rc: &Rc<RefCell<ScalarArena>>) -> OptExpr {
        make_scan_with_ids(arena_rc, 1, 2, 3)
    }

    fn needed_set(ids: &[u32]) -> HashSet<ColumnId> {
        ids.iter().map(|&id| ColumnId::new_for_test(id)).collect()
    }

    fn required_columns(expr: &OptExpr) -> &HashSet<ColumnId> {
        expr.required_output_columns
            .as_ref()
            .expect("expected required_output_columns to be tagged")
    }

    fn scan_required_columns(expr: &OptExpr) -> &HashSet<ColumnId> {
        assert!(
            matches!(&expr.op, Operator::LogicalScan(_)),
            "expected Scan node"
        );
        required_columns(expr)
    }

    // -----------------------------------------------------------------------
    // Scan tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_scan_with_none_keeps_all_cols() {
        let arena_rc = make_arena();
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(scan_with_3_cols(&arena_rc), &arena, None);
        assert!(matches!(&tagged.op, Operator::LogicalScan(_)));
        let req = required_columns(&tagged);
        assert_eq!(req.len(), 3);
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_scan_with_subset_keeps_only_those() {
        let arena_rc = make_arena();
        let arena = arena_rc.borrow();
        let subset = needed_set(&[2]);
        let tagged =
            tag_required_columns(scan_with_3_cols(&arena_rc), &arena, Some(subset.clone()));
        assert!(matches!(&tagged.op, Operator::LogicalScan(_)));
        assert_eq!(required_columns(&tagged), &subset);
    }

    // -----------------------------------------------------------------------
    // Project tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_project_filters_child_needed_by_output_column_id() {
        // Project[a→101, b→102] <- Scan[a@1, b@2, c@3]
        // parent_needed = {102 (b)}
        // Expected: scan.required_output_columns = {2}  (only b from scan)
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col1 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(1));
        let col2 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(2));
        drop(arena_mut);

        let project = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![
                    ScalarProjectItem {
                        output_column_id: ColumnId::new_for_test(101),
                        output_name: "a".to_string(),
                        expr: col1,
                        expr_display: None,
                    },
                    ScalarProjectItem {
                        output_column_id: ColumnId::new_for_test(102),
                        output_name: "b".to_string(),
                        expr: col2,
                        expr_display: None,
                    },
                ],
                output_qualifier: None,
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let needed = needed_set(&[102]);
        let tagged = tag_required_columns(project, &arena, Some(needed.clone()));

        assert!(matches!(&tagged.op, Operator::LogicalProject(_)));
        assert_eq!(tagged.required_output_columns.as_ref().unwrap(), &needed);

        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        let scan_req = required_columns(input);
        assert!(
            scan_req.contains(&ColumnId::new_for_test(2)),
            "scan should keep b"
        );
        assert!(
            !scan_req.contains(&ColumnId::new_for_test(1)),
            "scan should NOT keep a"
        );
    }

    #[test]
    fn tag_project_with_none_parent_includes_all_item_refs() {
        // parent_needed=None: child_needed = union of all items' column refs
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col1 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(1));
        let col2 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(2));
        drop(arena_mut);

        let project = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![
                    ScalarProjectItem {
                        output_column_id: ColumnId::new_for_test(101),
                        output_name: "a".to_string(),
                        expr: col1,
                        expr_display: None,
                    },
                    ScalarProjectItem {
                        output_column_id: ColumnId::new_for_test(102),
                        output_name: "b".to_string(),
                        expr: col2,
                        expr_display: None,
                    },
                ],
                output_qualifier: None,
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(project, &arena, None);
        assert!(matches!(&tagged.op, Operator::LogicalProject(_)));
        // required_output_columns should be None (transparent)
        assert!(tagged.required_output_columns.is_none());
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        let scan_req = required_columns(input);
        // Both a(1) and b(2) referenced; c(3) not in any item expr
        assert!(scan_req.contains(&ColumnId::new_for_test(1)));
        assert!(scan_req.contains(&ColumnId::new_for_test(2)));
        assert!(!scan_req.contains(&ColumnId::new_for_test(3)));
    }

    // -----------------------------------------------------------------------
    // Filter test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_filter_adds_predicate_cols_to_child_needed() {
        // Filter(c@3 > 0) <- Scan[a@1, b@2, c@3]
        // parent_needed = {1}
        // Expected: child_needed = {1, 3}
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col3 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(3));
        let zero = int_literal_scalar(&mut arena_mut, 0);
        let pred = binop_scalar(&mut arena_mut, col3, BinOp::Gt, zero);
        drop(arena_mut);

        let filter = OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(filter, &arena, Some(needed_set(&[1])));
        assert!(matches!(&tagged.op, Operator::LogicalFilter(_)));
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        let req = required_columns(input);
        assert!(
            req.contains(&ColumnId::new_for_test(1)),
            "a needed by parent"
        );
        assert!(
            req.contains(&ColumnId::new_for_test(3)),
            "c needed by predicate"
        );
        assert!(!req.contains(&ColumnId::new_for_test(2)), "b not needed");
    }

    // -----------------------------------------------------------------------
    // Aggregate test
    // -----------------------------------------------------------------------

    /// Bug A regression: tag_aggregate must use the conservative keep-all
    /// strategy for aggregate args.
    #[test]
    fn tag_aggregate_conservative_keeps_all_aggregate_args_in_child_needed() {
        // Aggregate[group_by=[y@1], count(*)→301, sum(x@2)→302]
        // parent_needed = {301}  (only count needed)
        //
        // Expected (conservative fix):
        //   child_needed = {1, 2}  (group_by y@1 + ALL aggregate args: x@2)
        //   c@3 (not referenced by any group_by or agg arg) is NOT needed.
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col1 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(1));
        let col2 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(2));
        drop(arena_mut);

        let agg = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![col1],
                vec![
                    ScalarAggregateSpec {
                        name: "count".to_string(),
                        args: vec![],
                        distinct: false,
                        order_by: vec![],
                    },
                    ScalarAggregateSpec {
                        name: "sum".to_string(),
                        args: vec![col2],
                        distinct: false,
                        order_by: vec![],
                    },
                ],
                vec![
                    make_output_column(ColumnId::new_for_test(1), "y"),
                    make_output_column(ColumnId::new_for_test(301), "count"),
                    make_output_column(ColumnId::new_for_test(302), "sum_x"),
                ],
            )),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(agg, &arena, Some(needed_set(&[301])));
        assert!(matches!(&tagged.op, Operator::LogicalAggregate(_)));
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        let req = required_columns(input);
        // group_by y@1 must always be in child_needed.
        assert!(req.contains(&ColumnId::new_for_test(1)), "group_by y@1");
        // sum(x) arg x@2 must be in child_needed even though parent only needs count.
        assert!(
            req.contains(&ColumnId::new_for_test(2)),
            "sum(x@2) arg must be kept (conservative keep-all)"
        );
        // c@3 is not referenced by group_by or any agg arg — may be absent.
        // (We do not assert it is absent; correctness only requires the above.)
    }

    /// tag_aggregate with parent_needed=None propagates None to the child
    /// (None-propagation discipline — child keeps all its columns).
    #[test]
    fn tag_aggregate_none_parent_propagates_none_to_child() {
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col1 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(1));
        let col2 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(2));
        drop(arena_mut);

        let agg = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![col1],
                vec![ScalarAggregateSpec {
                    name: "sum".to_string(),
                    args: vec![col2],
                    distinct: false,
                    order_by: vec![],
                }],
                vec![make_output_column(ColumnId::new_for_test(301), "sum_x")],
            )),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(agg, &arena, None);
        assert!(matches!(&tagged.op, Operator::LogicalAggregate(_)));
        // Aggregate receives None → keeps None on itself.
        assert!(tagged.required_output_columns.is_none());
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        // Child got None → Scan expands to all columns.
        let req = required_columns(input);
        assert_eq!(req.len(), 3, "scan keeps all 3 columns");
    }

    // -----------------------------------------------------------------------
    // Join test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_join_splits_needed_by_child_outputs_and_adds_condition_cols() {
        // Join[INNER, on a@1=d@4] <- {Scan_l[a@1,b@2,c@3], Scan_r[d@4,e@5,f@6]}
        // parent_needed = {2, 6}
        // Expected:
        //   left_needed  = {1, 2}  (join cond a + parent b)
        //   right_needed = {4, 6}  (join cond d + parent f)
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col1 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(1));
        let col4 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(4));
        let cond = binop_scalar(&mut arena_mut, col1, BinOp::Eq, col4);
        drop(arena_mut);

        let join = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(cond),
            }),
            vec![
                make_scan_with_ids(&arena_rc, 1, 2, 3),
                make_scan_with_ids(&arena_rc, 4, 5, 6),
            ],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(join, &arena, Some(needed_set(&[2, 6])));
        assert!(matches!(&tagged.op, Operator::LogicalJoin(_)));
        let left = tagged.left();
        assert!(matches!(&left.op, Operator::LogicalScan(_)));
        let right = tagged.right();
        assert!(matches!(&right.op, Operator::LogicalScan(_)));
        let lreq = required_columns(left);
        let rreq = required_columns(right);
        assert_eq!(lreq.len(), 2);
        assert!(lreq.contains(&ColumnId::new_for_test(1)));
        assert!(lreq.contains(&ColumnId::new_for_test(2)));
        assert_eq!(rreq.len(), 2);
        assert!(rreq.contains(&ColumnId::new_for_test(4)));
        assert!(rreq.contains(&ColumnId::new_for_test(6)));
    }

    // -----------------------------------------------------------------------
    // Union position-aligned test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_union_position_aligned_propagation() {
        // Union[output: x@1001, y@1002, z@1003]
        //   <- Scan_a[a@1, b@2, c@3]
        //   <- Scan_b[d@4, e@5, f@6]
        // parent_needed = {1002}  (position 1 = y)
        // Expected:
        //   Scan_a: {2}  (position 1 = b@2)
        //   Scan_b: {5}  (position 1 = e@5)
        let arena_rc = make_arena();
        let union = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1001), "x"),
                    make_output_column(ColumnId::new_for_test(1002), "y"),
                    make_output_column(ColumnId::new_for_test(1003), "z"),
                ],
                child_output_columns: vec![
                    vec![
                        make_output_column(ColumnId::new_for_test(1), "a"),
                        make_output_column(ColumnId::new_for_test(2), "b"),
                        make_output_column(ColumnId::new_for_test(3), "c"),
                    ],
                    vec![
                        make_output_column(ColumnId::new_for_test(4), "d"),
                        make_output_column(ColumnId::new_for_test(5), "e"),
                        make_output_column(ColumnId::new_for_test(6), "f"),
                    ],
                ],
            }),
            vec![
                make_scan_with_ids(&arena_rc, 1, 2, 3),
                make_scan_with_ids(&arena_rc, 4, 5, 6),
            ],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(union, &arena, Some(needed_set(&[1002])));
        assert!(matches!(&tagged.op, Operator::LogicalUnion(_)));
        let a_req = scan_required_columns(tagged.child(0));
        let b_req = scan_required_columns(tagged.child(1));
        assert_eq!(a_req.len(), 1);
        assert!(
            a_req.contains(&ColumnId::new_for_test(2)),
            "position 1 = b@2"
        );
        assert_eq!(b_req.len(), 1);
        assert!(
            b_req.contains(&ColumnId::new_for_test(5)),
            "position 1 = e@5"
        );
    }

    #[test]
    fn tag_union_distinct_preserves_all_child_columns() {
        let arena_rc = make_arena();
        let union = OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: false,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1001), "x"),
                    make_output_column(ColumnId::new_for_test(1002), "y"),
                    make_output_column(ColumnId::new_for_test(1003), "z"),
                ],
                child_output_columns: vec![
                    vec![
                        make_output_column(ColumnId::new_for_test(1), "a"),
                        make_output_column(ColumnId::new_for_test(2), "b"),
                        make_output_column(ColumnId::new_for_test(3), "c"),
                    ],
                    vec![
                        make_output_column(ColumnId::new_for_test(4), "d"),
                        make_output_column(ColumnId::new_for_test(5), "e"),
                        make_output_column(ColumnId::new_for_test(6), "f"),
                    ],
                ],
            }),
            vec![
                make_scan_with_ids(&arena_rc, 1, 2, 3),
                make_scan_with_ids(&arena_rc, 4, 5, 6),
            ],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(union, &arena, Some(needed_set(&[1002])));
        assert!(matches!(&tagged.op, Operator::LogicalUnion(_)));
        let a_req = scan_required_columns(tagged.child(0));
        let b_req = scan_required_columns(tagged.child(1));
        assert_eq!(a_req.len(), 3);
        assert_eq!(b_req.len(), 3);
        for id in [1, 2, 3] {
            assert!(a_req.contains(&ColumnId::new_for_test(id)));
        }
        for id in [4, 5, 6] {
            assert!(b_req.contains(&ColumnId::new_for_test(id)));
        }
    }

    #[test]
    fn tag_intersect_preserves_all_child_columns() {
        let arena_rc = make_arena();
        let intersect = OptExpr::new(
            Operator::LogicalIntersect(IntersectOp {
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1001), "x"),
                    make_output_column(ColumnId::new_for_test(1002), "y"),
                    make_output_column(ColumnId::new_for_test(1003), "z"),
                ],
                child_output_columns: vec![],
            }),
            vec![
                make_scan_with_ids(&arena_rc, 1, 2, 3),
                make_scan_with_ids(&arena_rc, 4, 5, 6),
            ],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(intersect, &arena, Some(needed_set(&[1002])));
        assert!(matches!(&tagged.op, Operator::LogicalIntersect(_)));
        assert_eq!(scan_required_columns(tagged.child(0)).len(), 3);
        assert_eq!(scan_required_columns(tagged.child(1)).len(), 3);
    }

    #[test]
    fn tag_except_preserves_all_child_columns() {
        let arena_rc = make_arena();
        let except = OptExpr::new(
            Operator::LogicalExcept(ExceptOp {
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1001), "x"),
                    make_output_column(ColumnId::new_for_test(1002), "y"),
                    make_output_column(ColumnId::new_for_test(1003), "z"),
                ],
                child_output_columns: vec![],
            }),
            vec![
                make_scan_with_ids(&arena_rc, 1, 2, 3),
                make_scan_with_ids(&arena_rc, 4, 5, 6),
            ],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(except, &arena, Some(needed_set(&[1002])));
        assert!(matches!(&tagged.op, Operator::LogicalExcept(_)));
        assert_eq!(scan_required_columns(tagged.child(0)).len(), 3);
        assert_eq!(scan_required_columns(tagged.child(1)).len(), 3);
    }

    // -----------------------------------------------------------------------
    // CTEAnchor two-walk test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_cte_anchor_produce_body_gets_keep_all_none() {
        let cte_id: CteId = 7;
        let arena_rc = make_arena();

        let scan = make_scan_with_ids(&arena_rc, 10, 20, 30);

        let produce = OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(10), "c0"),
                    make_output_column(ColumnId::new_for_test(20), "c1"),
                    make_output_column(ColumnId::new_for_test(30), "c2"),
                ],
            }),
            vec![scan],
        );

        let consume = OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id,
            alias: "u1".to_string(),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(101), "k0"),
                make_output_column(ColumnId::new_for_test(102), "k1"),
                make_output_column(ColumnId::new_for_test(103), "k2"),
            ],
        }));

        let anchor = OptExpr::new(
            Operator::LogicalCTEAnchor(CTEAnchorOp { cte_id }),
            vec![produce, consume],
        );

        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(anchor, &arena, Some(needed_set(&[102])));

        assert!(matches!(&tagged.op, Operator::LogicalCTEAnchor(_)));
        let produce = tagged.child(0);
        assert!(matches!(&produce.op, Operator::LogicalCTEProduce(_)));
        let produce_input = produce.unary_input();
        assert!(matches!(&produce_input.op, Operator::LogicalScan(_)));
        let req = required_columns(produce_input);
        // Conservative keep-all: produce body scan keeps all 3 columns.
        assert_eq!(
            req.len(),
            3,
            "scan must keep all columns (keep-all for CTE produce body)"
        );
        assert!(req.contains(&ColumnId::new_for_test(10)), "a@10 kept");
        assert!(req.contains(&ColumnId::new_for_test(20)), "b@20 kept");
        assert!(req.contains(&ColumnId::new_for_test(30)), "c@30 kept");
    }

    #[test]
    fn tag_cte_anchor_multi_consumer_produce_body_gets_keep_all_none() {
        let cte_id: CteId = 42;
        let arena_rc = make_arena();

        let scan = make_scan_with_ids(&arena_rc, 10, 20, 30);
        let produce = OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(10), "c0"),
                    make_output_column(ColumnId::new_for_test(20), "c1"),
                    make_output_column(ColumnId::new_for_test(30), "c2"),
                ],
            }),
            vec![scan],
        );

        let consume1 = OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id,
            alias: "u1".to_string(),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(101), "k0"),
                make_output_column(ColumnId::new_for_test(102), "k1"),
                make_output_column(ColumnId::new_for_test(103), "k2"),
            ],
        }));
        let consume2 = OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id,
            alias: "u2".to_string(),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(201), "m0"),
                make_output_column(ColumnId::new_for_test(202), "m1"),
                make_output_column(ColumnId::new_for_test(203), "m2"),
            ],
        }));

        // Consumer subtree: Join of consume1 and consume2.
        let consumer = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![consume1, consume2],
        );

        let anchor = OptExpr::new(
            Operator::LogicalCTEAnchor(CTEAnchorOp { cte_id }),
            vec![produce, consumer],
        );

        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(anchor, &arena, Some(needed_set(&[102, 203])));

        assert!(matches!(&tagged.op, Operator::LogicalCTEAnchor(_)));
        let produce = tagged.child(0);
        assert!(matches!(&produce.op, Operator::LogicalCTEProduce(_)));
        let produce_input = produce.unary_input();
        assert!(matches!(&produce_input.op, Operator::LogicalScan(_)));
        let req = required_columns(produce_input);
        // Conservative keep-all: produce body scan keeps all 3 columns.
        assert_eq!(
            req.len(),
            3,
            "scan must keep all columns (keep-all for CTE produce body)"
        );
        assert!(req.contains(&ColumnId::new_for_test(10)), "a@10 kept");
        assert!(req.contains(&ColumnId::new_for_test(20)), "b@20 kept");
        assert!(req.contains(&ColumnId::new_for_test(30)), "c@30 kept");
    }

    // -----------------------------------------------------------------------
    // Window test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_window_passes_none_to_child_keeps_all_input_cols() {
        // Window node must pass None to its child because window output_columns
        // carry fresh ColumnIds distinct from the child's ids — any attempt to
        // remap them risks under-tagging.  The safe fallback is None (keep all).
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let part_by = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(2));
        let order_by = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(3));
        drop(arena_mut);

        let window = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![ScalarWindowSpec {
                    name: "row_number".to_string(),
                    args: vec![],
                    distinct: false,
                    partition_by: vec![part_by],
                    order_by: vec![SortKey {
                        expr: order_by,
                        asc: true,
                        nulls_first: false,
                        display: None,
                    }],
                    window_frame: None,
                    ignore_nulls: false,
                }],
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1), "a"),
                    make_output_column(ColumnId::new_for_test(2), "b"),
                    make_output_column(ColumnId::new_for_test(301), "row_number"),
                ],
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(window, &arena, Some(needed_set(&[1])));
        assert!(matches!(&tagged.op, Operator::LogicalWindow(_)));
        // The window node itself records the parent's request.
        assert_eq!(
            tagged.required_output_columns.as_ref().unwrap(),
            &needed_set(&[1])
        );
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        // Child got None → Scan expands to all its columns.
        let req = required_columns(input);
        assert_eq!(req.len(), 3, "scan keeps all 3 input columns");
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_window_with_none_parent_child_also_keeps_all() {
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let part_by = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(2));
        let order_by = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(3));
        drop(arena_mut);

        let window = OptExpr::new(
            Operator::LogicalWindow(WindowOp {
                window_exprs: vec![ScalarWindowSpec {
                    name: "row_number".to_string(),
                    args: vec![],
                    distinct: false,
                    partition_by: vec![part_by],
                    order_by: vec![SortKey {
                        expr: order_by,
                        asc: true,
                        nulls_first: false,
                        display: None,
                    }],
                    window_frame: None,
                    ignore_nulls: false,
                }],
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1), "a"),
                    make_output_column(ColumnId::new_for_test(2), "b"),
                    make_output_column(ColumnId::new_for_test(301), "row_number"),
                ],
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(window, &arena, None);
        assert!(matches!(&tagged.op, Operator::LogicalWindow(_)));
        assert!(tagged.required_output_columns.is_none());
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        // None propagated → Scan keeps all columns.
        let req = required_columns(input);
        assert_eq!(req.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Sort / Limit passthrough tests
    // -----------------------------------------------------------------------

    #[test]
    fn tag_sort_adds_key_cols_to_child_needed() {
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col3 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(3));
        drop(arena_mut);

        let sort = OptExpr::new(
            Operator::LogicalSort(SortOp {
                items: vec![SortKey {
                    expr: col3,
                    asc: true,
                    nulls_first: false,
                    display: None,
                }],
                analytic_partition_exprs: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(sort, &arena, Some(needed_set(&[1])));
        assert!(matches!(&tagged.op, Operator::LogicalSort(_)));
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        let req = required_columns(input);
        assert!(req.contains(&ColumnId::new_for_test(1)), "parent needed a");
        assert!(
            req.contains(&ColumnId::new_for_test(3)),
            "sort key c needed"
        );
        assert!(!req.contains(&ColumnId::new_for_test(2)));
    }

    #[test]
    fn tag_limit_passes_needed_through() {
        let arena_rc = make_arena();
        let limit = OptExpr::new(
            Operator::LogicalLimit(LimitOp {
                limit: Some(10),
                offset: None,
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let needed = needed_set(&[2]);
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(limit, &arena, Some(needed.clone()));
        assert!(matches!(&tagged.op, Operator::LogicalLimit(_)));
        assert_eq!(tagged.required_output_columns.as_ref().unwrap(), &needed);
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        // Exactly the parent needed set passed through.
        assert_eq!(required_columns(input), &needed_set(&[2]));
    }

    // -----------------------------------------------------------------------
    // Values leaf test
    // -----------------------------------------------------------------------

    #[test]
    fn tag_values_with_none_stamps_all_ids() {
        let arena_rc = make_arena();
        let arena = arena_rc.borrow();
        let values = OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![
                make_output_column(ColumnId::new_for_test(5), "x"),
                make_output_column(ColumnId::new_for_test(6), "y"),
            ],
        }));
        let tagged = tag_required_columns(values, &arena, None);
        assert!(matches!(&tagged.op, Operator::LogicalValues(_)));
        let req = required_columns(&tagged);
        assert_eq!(req.len(), 2);
        assert!(req.contains(&ColumnId::new_for_test(5)));
        assert!(req.contains(&ColumnId::new_for_test(6)));
    }

    // -----------------------------------------------------------------------
    // None-propagation tests (Fix 4/5/6: Filter/Sort/Join must not collapse
    // None to an empty set — they must pass None through to children).
    // -----------------------------------------------------------------------

    #[test]
    fn tag_filter_none_parent_propagates_none_to_child() {
        // Filter(pred on c@3) <- Scan[a@1, b@2, c@3]
        // parent_needed = None
        // Correct: child gets None → Scan keeps all {1,2,3}.
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col3 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(3));
        let zero = int_literal_scalar(&mut arena_mut, 0);
        let pred = binop_scalar(&mut arena_mut, col3, BinOp::Gt, zero);
        drop(arena_mut);

        let filter = OptExpr::new(
            Operator::LogicalFilter(FilterOp { predicate: pred }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(filter, &arena, None);
        assert!(matches!(&tagged.op, Operator::LogicalFilter(_)));
        assert!(
            tagged.required_output_columns.is_none(),
            "filter keeps None on itself"
        );
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        // None propagated → Scan expands to all columns.
        let req = required_columns(input);
        assert_eq!(
            req.len(),
            3,
            "scan must keep all 3 columns, not just predicate ref c"
        );
        assert!(req.contains(&ColumnId::new_for_test(1)), "a@1 kept");
        assert!(req.contains(&ColumnId::new_for_test(2)), "b@2 kept");
        assert!(req.contains(&ColumnId::new_for_test(3)), "c@3 kept");
    }

    #[test]
    fn tag_sort_none_parent_propagates_none_to_child() {
        // Sort(order by c@3) <- Scan[a@1, b@2, c@3]
        // parent_needed = None
        // Correct: child gets None → Scan keeps all {1,2,3}.
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col3 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(3));
        let col2 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(2));
        drop(arena_mut);

        let sort = OptExpr::new(
            Operator::LogicalSort(SortOp {
                items: vec![SortKey {
                    expr: col3,
                    asc: true,
                    nulls_first: false,
                    display: None,
                }],
                analytic_partition_exprs: vec![col2],
                partition_limit: None,
                topn_type: None,
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(sort, &arena, None);
        assert!(matches!(&tagged.op, Operator::LogicalSort(_)));
        assert!(
            tagged.required_output_columns.is_none(),
            "sort keeps None on itself"
        );
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        // None propagated → Scan expands to all columns.
        let req = required_columns(input);
        assert_eq!(
            req.len(),
            3,
            "scan must keep all 3 columns, not just sort/partition refs"
        );
        assert!(req.contains(&ColumnId::new_for_test(1)), "a@1 kept");
        assert!(req.contains(&ColumnId::new_for_test(2)), "b@2 kept");
        assert!(req.contains(&ColumnId::new_for_test(3)), "c@3 kept");
    }

    #[test]
    fn tag_join_none_parent_propagates_none_to_both_children() {
        // Join[INNER, on a@1=d@4] <- {Scan_l[a@1,b@2,c@3], Scan_r[d@4,e@5,f@6]}
        // parent_needed = None
        // Correct: both children get None → each Scan keeps all its columns.
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col1 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(1));
        let col4 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(4));
        let cond = binop_scalar(&mut arena_mut, col1, BinOp::Eq, col4);
        drop(arena_mut);

        let join = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(cond),
            }),
            vec![
                make_scan_with_ids(&arena_rc, 1, 2, 3),
                make_scan_with_ids(&arena_rc, 4, 5, 6),
            ],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(join, &arena, None);
        assert!(matches!(&tagged.op, Operator::LogicalJoin(_)));
        assert!(
            tagged.required_output_columns.is_none(),
            "join keeps None on itself"
        );
        let left = tagged.left();
        assert!(matches!(&left.op, Operator::LogicalScan(_)));
        let right = tagged.right();
        assert!(matches!(&right.op, Operator::LogicalScan(_)));
        // None propagated → each Scan expands to all its columns.
        let lreq = required_columns(left);
        let rreq = required_columns(right);
        assert_eq!(lreq.len(), 3, "left scan keeps all 3 columns");
        assert!(lreq.contains(&ColumnId::new_for_test(1)));
        assert!(lreq.contains(&ColumnId::new_for_test(2)));
        assert!(lreq.contains(&ColumnId::new_for_test(3)));
        assert_eq!(rreq.len(), 3, "right scan keeps all 3 columns");
        assert!(rreq.contains(&ColumnId::new_for_test(4)));
        assert!(rreq.contains(&ColumnId::new_for_test(5)));
        assert!(rreq.contains(&ColumnId::new_for_test(6)));
    }

    // -----------------------------------------------------------------------
    // Keep-all tests for Repeat and TableFunction (Fix 1/2):
    // even when parent_needed omits a column the operator needs from its input,
    // the child retains all columns because the handler passes None.
    // -----------------------------------------------------------------------

    #[test]
    fn tag_repeat_maps_parent_needed_and_rollup_keys_to_child_ids() {
        // Repeat node referencing rollup columns b@2 by ColumnId.
        // parent_needed = {1}  (only a — does NOT include the rollup column b@2).
        // The handler sends {1,2} to the child: parent output a@1 plus
        // rollup key b@2 needed by Repeat's nulling/grouping logic.
        let arena_rc = make_arena();
        let repeat = OptExpr::new(
            Operator::LogicalRepeat(RepeatOp {
                repeat_column_ref_list: vec![vec!["b".to_string()]],
                repeat_column_ref_ids: vec![vec![ColumnId::new_for_test(2)]],
                grouping_ids: vec![1],
                all_rollup_columns: vec!["b".to_string()],
                all_rollup_column_ids: vec![ColumnId::new_for_test(2)],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![],
                grouping_fn_arg_ids: vec![],
                grouping_fn_ids: vec![],
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(repeat, &arena, Some(needed_set(&[1])));
        assert!(matches!(&tagged.op, Operator::LogicalRepeat(_)));
        // Repeat records parent_needed on itself.
        assert_eq!(
            tagged.required_output_columns.as_ref().unwrap(),
            &needed_set(&[1])
        );
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        let req = required_columns(input);
        assert_eq!(req.len(), 2, "scan keeps parent-needed and rollup key ids");
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(!req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_repeat_parent_none_preserves_all_child_outputs() {
        let arena_rc = make_arena();
        let repeat = OptExpr::new(
            Operator::LogicalRepeat(RepeatOp {
                repeat_column_ref_list: vec![vec!["b".to_string()]],
                repeat_column_ref_ids: vec![vec![ColumnId::new_for_test(2)]],
                grouping_ids: vec![1],
                all_rollup_columns: vec!["b".to_string()],
                all_rollup_column_ids: vec![ColumnId::new_for_test(2)],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![],
                grouping_fn_arg_ids: vec![],
                grouping_fn_ids: vec![],
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(repeat, &arena, None);
        assert!(matches!(&tagged.op, Operator::LogicalRepeat(_)));
        assert!(
            tagged.required_output_columns.is_none(),
            "Repeat root should keep the all-required None marker"
        );
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        let req = required_columns(input);
        assert_eq!(req.len(), 3, "child scan must keep all outputs");
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(req.contains(&ColumnId::new_for_test(2)));
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    #[test]
    fn tag_generate_series_parent_none_requires_output_id() {
        let output_id = ColumnId::new_for_test(301);
        let arena_rc = make_arena();
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(
            OptExpr::leaf(Operator::LogicalGenerateSeries(GenerateSeriesOp {
                start: 1,
                end: 3,
                step: 1,
                column_name: "x".to_string(),
                alias: Some("gs".to_string()),
                output_column_id: output_id,
            })),
            &arena,
            None,
        );
        assert!(matches!(&tagged.op, Operator::LogicalGenerateSeries(_)));
        let req = required_columns(&tagged);
        assert_eq!(req.len(), 1);
        assert!(req.contains(&output_id));
    }

    #[test]
    fn tag_table_function_passes_none_to_child_even_when_parent_needed_is_narrow() {
        // TableFunction: UNNEST(arr@2) → exploded_col@401
        // parent_needed = {401}  (only the function output — does NOT include arr@2).
        // The handler must pass None to the child so arr@2 (the arg) is not dropped.
        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col2 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(2));
        drop(arena_mut);

        let tf = OptExpr::new(
            Operator::LogicalTableFunction(TableFunctionOp {
                function_name: "unnest".to_string(),
                args: vec![col2],
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(1), "a"),
                    make_output_column(ColumnId::new_for_test(401), "unnested"),
                ],
                alias: None,
                is_left_join: false,
            }),
            vec![scan_with_3_cols(&arena_rc)],
        );
        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(tf, &arena, Some(needed_set(&[401])));
        assert!(matches!(&tagged.op, Operator::LogicalTableFunction(_)));
        // TableFunction records parent_needed on itself.
        assert_eq!(
            tagged.required_output_columns.as_ref().unwrap(),
            &needed_set(&[401])
        );
        let input = tagged.unary_input();
        assert!(matches!(&input.op, Operator::LogicalScan(_)));
        // Child got None → Scan expands to all columns, including arr@2.
        let req = required_columns(input);
        assert_eq!(
            req.len(),
            3,
            "scan keeps all 3 columns, including arr@2 needed by function arg"
        );
        assert!(req.contains(&ColumnId::new_for_test(1)));
        assert!(
            req.contains(&ColumnId::new_for_test(2)),
            "arr@2 must be kept for UNNEST arg"
        );
        assert!(req.contains(&ColumnId::new_for_test(3)));
    }

    // -----------------------------------------------------------------------
    // TagRequiredColumns rule end-to-end pipeline test
    // -----------------------------------------------------------------------

    /// Verify that `TagRequiredColumns` runs through the full
    /// `query_rewrite_pipeline` and stamps `required_output_columns = Some(_)`
    /// on both nodes of a Project → Scan plan.
    #[test]
    fn tag_required_columns_rule_runs_through_pipeline_and_stamps_nodes() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
        use std::collections::HashMap;

        let arena_rc = make_arena();
        let mut arena_mut = arena_rc.borrow_mut();
        let col1 = col_ref_scalar(&mut arena_mut, ColumnId::new_for_test(1));
        drop(arena_mut);

        let plan = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    output_column_id: ColumnId::new_for_test(101),
                    output_name: "a".to_string(),
                    expr: col1,
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            vec![OptExpr::leaf(Operator::LogicalScan(ScanOp {
                database: "db".to_string(),
                table: crate::sql::catalog::TableDef {
                    name: "t".to_string(),
                    columns: vec![crate::sql::catalog::ColumnDef {
                        name: "a".to_string(),
                        data_type: arrow::datatypes::DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::catalog::ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![make_output_column(ColumnId::new_for_test(1), "a")],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }))],
        );

        let table_stats = HashMap::new();
        let pipeline = query_rewrite_pipeline(&table_stats);
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_scalar_arena(arena_rc.clone());
        let result = pipeline.rewrite(plan, &mut ctx).unwrap();

        // After the pipeline, the Scan leaf must have Some(_) on
        // required_output_columns — proof that TagRequiredColumns ran and
        // stamped the leaf.
        //
        // Note: the root Project carries `required_output_columns = None`
        // because it was called as the tree root (parent_needed = None), which
        // is the correct metadata: "no parent restriction on the root".
        // Only leaf nodes are guaranteed to hold `Some(_)` after tagging.
        assert!(
            matches!(&result.op, Operator::LogicalProject(_)),
            "expected Project at root after pipeline rewrite"
        );

        let input = result.unary_input();
        assert!(
            matches!(&input.op, Operator::LogicalScan(_)),
            "expected Scan child after pipeline rewrite"
        );
        assert!(
            input.required_output_columns.is_some(),
            "Scan.required_output_columns must be Some(_) after TagRequiredColumns stage ran"
        );
    }

    // -----------------------------------------------------------------------
    // tag_cte_consume with None parent — must store Some(all output ids)
    // -----------------------------------------------------------------------

    #[test]
    fn tag_cte_consume_with_none_parent_stores_some_all_output_ids() {
        let cte_id: CteId = 99;
        let consume = OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id,
            alias: "c".to_string(),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(10), "x"),
                make_output_column(ColumnId::new_for_test(20), "y"),
                make_output_column(ColumnId::new_for_test(30), "z"),
            ],
        }));

        let tagged = tag_cte_consume(consume, None);

        assert!(matches!(&tagged.op, Operator::LogicalCTEConsume(_)));
        let req = tagged
            .required_output_columns
            .as_ref()
            .expect("required_output_columns must be Some(_) after tagging with None parent");
        assert!(
            req.contains(&ColumnId::new_for_test(10)),
            "x@10 must be kept"
        );
        assert!(
            req.contains(&ColumnId::new_for_test(20)),
            "y@20 must be kept"
        );
        assert!(
            req.contains(&ColumnId::new_for_test(30)),
            "z@30 must be kept"
        );
        assert_eq!(req.len(), 3, "all 3 output ids kept");
    }

    #[test]
    fn tag_cte_anchor_with_none_parent_consume_leaf_is_some() {
        let cte_id: CteId = 88;
        let arena_rc = make_arena();

        let scan = make_scan_with_ids(&arena_rc, 10, 20, 30);
        let produce = OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id,
                output_columns: vec![
                    make_output_column(ColumnId::new_for_test(10), "a"),
                    make_output_column(ColumnId::new_for_test(20), "b"),
                    make_output_column(ColumnId::new_for_test(30), "c"),
                ],
            }),
            vec![scan],
        );

        let consume = OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id,
            alias: "u".to_string(),
            output_columns: vec![
                make_output_column(ColumnId::new_for_test(101), "p"),
                make_output_column(ColumnId::new_for_test(102), "q"),
            ],
        }));

        let anchor = OptExpr::new(
            Operator::LogicalCTEAnchor(CTEAnchorOp { cte_id }),
            vec![produce, consume],
        );

        let arena = arena_rc.borrow();
        let tagged = tag_required_columns(anchor, &arena, None);

        assert!(matches!(&tagged.op, Operator::LogicalCTEAnchor(_)));
        let consumer = tagged.child(1);
        assert!(matches!(&consumer.op, Operator::LogicalCTEConsume(_)));
        // The leaf must be Some(_) — not None — so subtree_untagged is false.
        assert!(
            consumer.required_output_columns.is_some(),
            "CTEConsume.required_output_columns must be Some(_) after tagging with None parent"
        );
        // All output ids must be present (keep-all semantics).
        let req = required_columns(consumer);
        assert!(req.contains(&ColumnId::new_for_test(101)), "p@101 kept");
        assert!(req.contains(&ColumnId::new_for_test(102)), "q@102 kept");
        assert_eq!(req.len(), 2, "both output ids kept");
    }
}
