//! `ApplyToWindow` — ports StarRocks `ScalarApply2AnalyticRule` ("WinMagic").
//!
//! Rewrites `Filter( ... lhs op APPLY_OUT ... )` over a decorrelated correlated
//! scalar-aggregate `Apply` into a `Window` (analytic) over the OUTER relation,
//! discarding the subquery subtree. Runs BEFORE `ScalarApplyToJoin`; on any
//! precondition failure returns `Unchanged` so `ScalarApplyToJoin` produces the
//! M1 join form. Never errors (the join form is always a valid fallback).

use std::collections::{HashMap, HashSet};

use super::scalar_utils;
use super::win_magic_util::{
    TableIdentity, collect_scan_column_map, collect_table_ids, expr_phys_eq,
};
use crate::sql::column_id::ColumnId;
use crate::sql::common::ApplyKind;
use crate::sql::common::{JoinKind, OutputColumn};
use crate::sql::optimizer::operator::{
    ApplyOp, LogicalAggregateOp, Operator, ScalarAggregateSpec, SortOp, WindowOp,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId};

const WHITELIST: &[&str] = &["count", "sum", "avg", "min", "max"];

pub(crate) struct ApplyToWindow;

/// Everything Task 3's transform needs, validated by `check_preconditions`.
#[allow(dead_code)]
pub(super) struct WinMagicMatch {
    /// All conjuncts of the matched WHERE Filter (already AND-split).
    pub outer_conjuncts: Vec<ScalarId>,
    /// The single outer conjunct that references `APPLY_OUT`.
    pub subquery_conjunct: ScalarId,
    /// Outer-side ColumnRef of each correlation conjunct — the window PARTITION BY keys.
    pub partition_by: Vec<ScalarId>,
    /// The inner single aggregate call (name in WHITELIST, non-distinct).
    pub inner_agg: ScalarAggregateSpec,
    /// Output column for the inner aggregate call.
    pub inner_agg_output: OutputColumn,
}

impl LogicalRewriteRule for ApplyToWindow {
    fn name(&self) -> &'static str {
        "ApplyToWindow"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let _ = ctx;
        matches_plan(expr)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let arena = ctx.scalar_arena();
        let mut arena = arena.borrow_mut();
        match apply_plan_inner(expr, ctx, &mut arena)? {
            Some(new_expr) => Ok(RewriteResult::Changed(new_expr)),
            None => Ok(RewriteResult::Unchanged),
        }
    }
}

fn matches_plan(plan: &OptExpr) -> bool {
    let Operator::LogicalFilter(_) = &plan.op else {
        return false;
    };
    let apply = plan.unary_input();
    let Operator::LogicalApply(a) = &apply.op else {
        return false;
    };
    a.kind == ApplyKind::Scalar && !a.need_check_max_rows && !a.correlation_conjuncts.is_empty()
}

fn apply_plan_inner(
    plan: OptExpr,
    ctx: &mut RewriteContext,
    arena: &mut ScalarArena,
) -> Result<Option<OptExpr>, String> {
    let Some(m) = ({
        let Operator::LogicalFilter(f) = &plan.op else {
            return Ok(None);
        };
        let apply = plan.unary_input();
        let Operator::LogicalApply(a) = &apply.op else {
            return Ok(None);
        };
        check_preconditions_opt(f.predicate, a, apply.left(), apply.right(), arena)
    }) else {
        return Ok(None);
    };

    let mut filter_children = plan.children;
    let apply_expr = filter_children.remove(0);
    let OptExpr {
        op,
        mut children,
        required_output_columns: _,
    } = apply_expr;
    let Operator::LogicalApply(a) = op else {
        unreachable!()
    };
    let apply_right = children.remove(1);
    let outer_subtree = children.remove(0);

    // --- 1. Remap the inner aggregate's args to the outer instance of the same physical column. ---
    let outer_map = collect_scan_column_map(&outer_subtree);
    let inner_map = collect_scan_column_map(&apply_right);
    // Collect the set of inner-scan ColumnIds upfront; used by post-condition guards.
    let inner_ids: HashSet<ColumnId> = inner_map.keys().copied().collect();
    let outer_cols = scalar_utils::opt_output_columns(&outer_subtree, arena)?;
    let mut phys_to_outer: HashMap<(TableIdentity, String), OutputColumn> = HashMap::new();
    for oc in &outer_cols {
        if let Some((tab, name)) = outer_map.get(&oc.column_id) {
            phys_to_outer.insert((tab.clone(), name.clone()), oc.clone());
        }
    }
    let mut agg_args = m.inner_agg.args.clone();
    for arg in &mut agg_args {
        let Some(remapped) = remap_inner_to_outer(arena, *arg, &inner_map, &phys_to_outer) else {
            // Required column unavailable on outer side → fall back to join form.
            return Ok(None);
        };
        *arg = remapped;
    }
    // Guard 1: post-condition — verify no inner-scan column survived the remap.
    // This catches any ExprKind variant that remap_inner_to_outer might have missed.
    for arg in &agg_args {
        if !scalar_utils::collect_column_ids(arena, *arg).is_disjoint(&inner_ids) {
            return Ok(None); // an inner column survived the remap
        }
    }

    // --- 2. Mint the window output column; build the WindowExpr. ---
    let factory = ctx
        .column_ref_factory()
        .ok_or_else(|| "ApplyToWindow requires ColumnRefFactory".to_string())?;
    let win_id = factory.borrow_mut().create(
        None,
        format!("{}_window", m.inner_agg.name),
        m.inner_agg_output.data_type.clone(),
        true,
    );
    let win_expr = crate::sql::optimizer::operator::ScalarWindowSpec {
        name: m.inner_agg.name.clone(),
        args: agg_args,
        distinct: false,
        partition_by: m.partition_by.clone(),
        order_by: vec![],
        window_frame: None,
        ignore_nulls: false,
    };
    let win_output = OutputColumn {
        column_id: win_id,
        name: format!("{}_window", m.inner_agg.name),
        data_type: m.inner_agg_output.data_type.clone(),
        nullable: true,
        is_internal: true,
    };

    // --- 3. before-window Filter = all outer conjuncts except the subquery one. ---
    let before: Vec<ScalarId> = m
        .outer_conjuncts
        .iter()
        .filter(|oc| **oc != m.subquery_conjunct)
        .copied()
        .collect();
    let before_filtered = if before.is_empty() {
        outer_subtree
    } else {
        let Some(predicate) = scalar_utils::combine_and(arena, before) else {
            return Ok(None);
        };
        scalar_utils::filter(outer_subtree, predicate)
    };

    // --- 4. Sort(partition keys) under the Window. ---
    let sort_items = m
        .partition_by
        .iter()
        .copied()
        .map(scalar_utils::sort_key)
        .collect();
    let sorted = OptExpr::new(
        Operator::LogicalSort(SortOp {
            items: sort_items,
            analytic_partition_exprs: m.partition_by.clone(),
            partition_limit: None,
            topn_type: None,
        }),
        vec![before_filtered],
    );

    // --- 5. Window node: output = base outer columns + the window column. ---
    let mut window_output = scalar_utils::opt_output_columns(&sorted, arena)?;
    window_output.push(win_output.clone());
    let window = OptExpr::new(
        Operator::LogicalWindow(WindowOp {
            window_exprs: vec![win_expr],
            output_columns: window_output,
        }),
        vec![sorted],
    );

    // --- 6. after-window Filter = subquery comparison with APPLY_OUT replaced by the value expr. ---
    let value_expr = build_value_expr(
        &apply_right,
        a.inner_output_column_id,
        m.inner_agg_output.column_id,
        &win_output,
        arena,
    );
    // Guard 2: value_expr must not reference the inner agg output or any inner-scan column.
    {
        let vrefs = scalar_utils::collect_column_ids(arena, value_expr);
        if vrefs.contains(&m.inner_agg_output.column_id) || !vrefs.is_disjoint(&inner_ids) {
            return Ok(None); // value expr still references a disappearing inner column
        }
    }
    let after_pred = scalar_utils::replace_column_ref(
        arena,
        m.subquery_conjunct,
        a.output_column.column_id,
        value_expr,
    );
    // Guard 3: APPLY_OUT must be gone from after_pred — it has been fully replaced.
    if scalar_utils::collect_column_ids(arena, after_pred).contains(&a.output_column.column_id) {
        return Ok(None); // APPLY_OUT survived (comparison used an unhandled node)
    }
    let after = scalar_utils::filter(window, after_pred);

    Ok(Some(after))
}

/// Recursively remap each `ColumnRef` in `expr` from an inner-scan column id to
/// the corresponding outer-scan column id, using physical (table, column-name)
/// identity.
///
/// Returns `true` if all ColumnRefs that appear in `inner_map` were successfully
/// remapped to an outer twin. Returns `false` if a ColumnRef IS in `inner_map`
/// but has NO outer twin in `phys_to_outer` (the column is not present on the
/// outer side → caller should fall back to the join form).
///
/// ColumnRefs whose ids are NOT in `inner_map` (e.g. already-outer refs or
/// literal-derived ids) are left unchanged and contribute `true`.
fn remap_inner_to_outer(
    arena: &mut ScalarArena,
    expr: ScalarId,
    inner_map: &HashMap<ColumnId, (TableIdentity, String)>,
    phys_to_outer: &HashMap<(TableIdentity, String), OutputColumn>,
) -> Option<ScalarId> {
    scalar_utils::remap_column_refs(arena, expr, &mut |arena, column_id| {
        let Some((tab, name)) = inner_map.get(&column_id) else {
            return Some(None);
        };
        let outer_col = phys_to_outer.get(&(tab.clone(), name.clone()))?;
        Some(Some(scalar_utils::column_ref(arena, outer_col)))
    })
}

/// Build the "value expression" that replaces `APPLY_OUT` in the after-window
/// filter predicate.
///
/// Post-`PushDownApplyAggFilter`, `apply_right` has at most ONE leading `Project`
/// that computes the arithmetic on top of the aggregate result (e.g. `0.2 * avg`).
/// If such a Project item with `output_column_id == inner_output_col_id` exists,
/// clone that item's expression and substitute the aggregate output column with
/// `WIN_ID`. If there is no leading Project (bare aggregate, `inner_output_col_id
/// == agg_out_col_id`), return a bare `ColumnRef(WIN_ID)`.
fn build_value_expr(
    apply_right: &OptExpr,
    inner_output_col_id: ColumnId,
    agg_out_col_id: ColumnId,
    win_output: &OutputColumn,
    arena: &mut ScalarArena,
) -> ScalarId {
    let win_ref = scalar_utils::column_ref(arena, win_output);
    // Peel exactly one optional leading Project (single-leading-Project assumption:
    // PushDownApplyAggFilter inserts at most one Project above the Aggregate).
    if let Operator::LogicalProject(proj) = &apply_right.op {
        // Look for the Project item whose output id matches inner_output_col_id.
        for item in &proj.items {
            if item.output_column_id == inner_output_col_id {
                // Replace the aggregate output column reference with win_id.
                return scalar_utils::replace_column_ref(arena, item.expr, agg_out_col_id, win_ref);
            }
        }
        // No matching item found in the Project (unusual shape) → safe fallback.
    }
    // No leading Project OR no matching item: inner_output_col_id IS the aggregate
    // output column (bare agg case, e.g. q2 min/max with no arithmetic).
    win_ref
}

fn check_preconditions_opt(
    where_pred: ScalarId,
    a: &ApplyOp,
    apply_left: &OptExpr,
    apply_right: &OptExpr,
    arena: &ScalarArena,
) -> Option<WinMagicMatch> {
    // (0) Inner: peel optional leading Project, require a vector Aggregate with a
    // single non-DISTINCT whitelisted aggregate.
    let agg = peel_to_aggregate(apply_right)?;
    if agg.aggregates.len() != 1 {
        return None;
    }
    let inner_agg = agg.aggregates[0].clone();
    if inner_agg.distinct {
        return None;
    }
    if !WHITELIST.contains(&inner_agg.name.as_str()) {
        return None;
    }
    let inner_agg_output = agg.output_columns.get(agg.group_by.len())?.clone();

    // (1) No LIMIT and only whitelisted operators in either subtree.
    if !operator_whitelist_ok(apply_left, false) {
        return None;
    }
    if !operator_whitelist_ok(apply_right, true) {
        return None;
    }

    // (2) Table-set identity: outerTables == subqueryTables + exactly 1 extra;
    // no duplicate physical table on either side (rejects self-joins).
    let outer_tabs = collect_table_ids(apply_left);
    let sub_tabs = collect_table_ids(apply_right);
    let outer_set: HashSet<TableIdentity> = outer_tabs.iter().cloned().collect();
    let sub_set: HashSet<TableIdentity> = sub_tabs.iter().cloned().collect();
    if outer_tabs.len() != outer_set.len() || sub_tabs.len() != sub_set.len() {
        return None;
    }
    if outer_set.len() != sub_set.len() + 1 {
        return None;
    }
    if !sub_set.is_subset(&outer_set) {
        return None;
    }
    let extra: Vec<&TableIdentity> = outer_set.difference(&sub_set).collect();
    if extra.len() != 1 {
        return None;
    }
    let correlated_outer_table = extra[0].clone();

    // (3) Partition-by keys = outer side of each correlation conjunct. Verify each
    // outer side is a ColumnRef of `correlated_outer_table`.
    let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();
    let col_map = collect_scan_column_map(apply_left);
    let mut partition_by = Vec::new();
    for conj in &a.correlation_conjuncts {
        let (outer_side, _inner) = scalar_utils::orient_eq(arena, *conj, &corr_ids)?;
        let column_id = scalar_utils::is_column_ref(arena, outer_side)?;
        match col_map.get(&column_id) {
            Some((tab, _)) if *tab == correlated_outer_table => {}
            _ => return None,
        }
        partition_by.push(outer_side);
    }

    // (4) Predicate identity (StarRocks checkPredicate, 4 steps). Use a phys map
    // spanning BOTH subtrees so inner/outer instances unify.
    let full_map = {
        let mut m = collect_scan_column_map(apply_left);
        m.extend(collect_scan_column_map(apply_right));
        m
    };
    let mut outer_conjuncts = scalar_utils::split_and(arena, where_pred);

    // 4a. Each correlation conjunct must have a phys-identical twin among outer conjuncts.
    let mut unmatched_corr = a.correlation_conjuncts.clone();
    unmatched_corr.retain(|cc| {
        if let Some(pos) = outer_conjuncts
            .iter()
            .position(|oc| expr_phys_eq(arena, *cc, *oc, &full_map))
        {
            outer_conjuncts.remove(pos);
            false
        } else {
            true
        }
    });
    if !unmatched_corr.is_empty() {
        return None;
    }

    // 4b. Exactly the subquery-comparison conjunct references APPLY_OUT; remove it.
    let apply_out = a.output_column.column_id;
    let sub_pos = outer_conjuncts
        .iter()
        .position(|oc| scalar_utils::collect_column_ids(arena, *oc).contains(&apply_out))?;
    let subquery_conjunct = outer_conjuncts.remove(sub_pos);
    if outer_conjuncts
        .iter()
        .any(|oc| scalar_utils::collect_column_ids(arena, *oc).contains(&apply_out))
    {
        return None;
    }

    // 4c. Drop outer conjuncts that reference ONLY `correlated_outer_table`.
    outer_conjuncts.retain(|oc| {
        let refs = scalar_utils::collect_column_ids(arena, *oc);
        let only_extra = !refs.is_empty()
            && refs
                .iter()
                .all(|id| matches!(col_map.get(id), Some((t, _)) if *t == correlated_outer_table));
        !only_extra
    });

    // 4d. Remaining outer conjuncts must 1:1 phys-match the subquery's residual Filter conjuncts.
    let mut sub_residual = subquery_residual_conjuncts(apply_right, arena);
    if outer_conjuncts.len() != sub_residual.len() {
        return None;
    }
    for oc in &outer_conjuncts {
        match sub_residual
            .iter()
            .position(|sc| expr_phys_eq(arena, *oc, *sc, &full_map))
        {
            Some(pos) => {
                sub_residual.remove(pos);
            }
            None => return None,
        }
    }

    Some(WinMagicMatch {
        outer_conjuncts: scalar_utils::split_and(arena, where_pred),
        subquery_conjunct,
        partition_by,
        inner_agg,
        inner_agg_output,
    })
}

/// Peel optional leading Project and return the underlying aggregate payload, if any.
fn peel_to_aggregate(plan: &OptExpr) -> Option<&LogicalAggregateOp> {
    match &plan.op {
        Operator::LogicalAggregate(agg) => Some(agg),
        Operator::LogicalProject(_) => peel_to_aggregate(plan.unary_input()),
        _ => None,
    }
}

/// Walk `plan` and confirm it contains only whitelisted operators.
///
/// For `is_subquery = false` (outer subtree): allow Scan, Cross-only Join,
/// Filter, Project.
/// For `is_subquery = true` (inner/subquery subtree): additionally allow
/// Aggregate.
///
/// Any other node (Limit, Sort, Window, Union, Apply, …) returns `false`.
fn operator_whitelist_ok(plan: &OptExpr, is_subquery: bool) -> bool {
    match &plan.op {
        Operator::LogicalScan(_) => true,
        Operator::LogicalFilter(_) | Operator::LogicalProject(_) => {
            operator_whitelist_ok(plan.unary_input(), is_subquery)
        }
        Operator::LogicalJoin(j) => {
            if j.join_type != JoinKind::Cross {
                return false;
            }
            operator_whitelist_ok(plan.left(), is_subquery)
                && operator_whitelist_ok(plan.right(), is_subquery)
        }
        Operator::LogicalAggregate(_) if is_subquery => {
            operator_whitelist_ok(plan.unary_input(), is_subquery)
        }
        _ => false,
    }
}

/// Collect the residual (non-correlation) Filter conjuncts from the subquery's
/// aggregate input, if a Filter is present.
fn subquery_residual_conjuncts(apply_right: &OptExpr, arena: &ScalarArena) -> Vec<ScalarId> {
    // Peel optional leading Project, then the Aggregate.
    let aggregate_plan = match &apply_right.op {
        Operator::LogicalAggregate(_) => apply_right,
        Operator::LogicalProject(_) => {
            let input = apply_right.unary_input();
            match &input.op {
                Operator::LogicalAggregate(_) => input,
                _ => return vec![],
            }
        }
        _ => return vec![],
    };
    // If the aggregate's input is a Filter, split its predicate into conjuncts.
    match &aggregate_plan.unary_input().op {
        Operator::LogicalFilter(f) => scalar_utils::split_and(arena, f.predicate),
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::optimizer::operator::Operator;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::rewrite::rules::subquery::bridge::opt_expr_to_plan;
    use crate::sql::optimizer::rewrite::rules::utils::{collect_column_id_refs, split_and};
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::optimizer_bridge::plan::logical_plan_to_opt_expr;
    use crate::sql::planner::plan::{
        AggregateCall, ApplyKind, LogicalAggregateNode, LogicalApplyNode, LogicalFilterNode,
        LogicalJoinNode, LogicalLimitNode, LogicalPlanNode, LogicalProjectNode, LogicalScanNode,
        PlanNodeKind,
    };

    fn check_preconditions(
        where_pred: &TypedExpr,
        apply: &LogicalApplyNode,
        apply_left: &LogicalPlanNode,
        apply_right: &LogicalPlanNode,
    ) -> Option<()> {
        let apply_plan = LogicalPlanNode::new(
            PlanNodeKind::Apply(apply.clone()),
            vec![apply_left.clone(), apply_right.clone()],
            None,
        );
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: where_pred.clone(),
            }),
            vec![apply_plan],
            None,
        );
        let mut arena = ScalarArena::new();
        let opt = logical_plan_to_opt_expr(&plan, &mut arena);
        let Operator::LogicalFilter(filter) = &opt.op else {
            return None;
        };
        let apply_expr = opt.unary_input();
        let Operator::LogicalApply(apply_op) = &apply_expr.op else {
            return None;
        };
        super::check_preconditions_opt(
            filter.predicate,
            apply_op,
            apply_expr.left(),
            apply_expr.right(),
            &arena,
        )
        .map(|_| ())
    }

    // ---- Column ID constants -------------------------------------------------
    // Outer lineitem scan (table_id=1, first instance)
    const L_ORDERKEY: ColumnId = ColumnId(1);
    const L_PARTKEY: ColumnId = ColumnId(2);
    const L_QUANTITY: ColumnId = ColumnId(3);
    // part scan (table_id=2)
    const P_PARTKEY: ColumnId = ColumnId(10);
    const P_BRAND: ColumnId = ColumnId(11);
    // Inner lineitem scan (table_id=1, second instance — same physical table, different ColumnIds)
    const INNER_L_PARTKEY: ColumnId = ColumnId(20);
    const INNER_L_QUANTITY: ColumnId = ColumnId(21);
    // AVG result
    const AVG_RESULT: ColumnId = ColumnId(30);
    // Apply output
    const APPLY_OUT: ColumnId = ColumnId(50);

    fn col_ref(id: ColumnId, name: &str, dt: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: dt,
            nullable: false,
        }
    }

    fn col_ref_nullable(id: ColumnId, name: &str, dt: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: dt,
            nullable: true,
        }
    }

    fn eq_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn lt_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Lt,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn mul_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Mul,
                right: Box::new(right),
            },
            data_type: DataType::Float64,
            nullable: true,
        }
    }

    fn str_lit(s: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(s.to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn float_lit(v: f64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Float(v)),
            data_type: DataType::Float64,
            nullable: false,
        }
    }

    /// Build `Scan(lineitem, table_id=1)` for the outer left side (first instance).
    fn make_outer_lineitem_scan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: "lineitem".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 1,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: L_ORDERKEY,
                        name: "l_orderkey".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: L_PARTKEY,
                        name: "l_partkey".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: L_QUANTITY,
                        name: "l_quantity".to_string(),
                        data_type: DataType::Float64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    /// Build `Scan(part, table_id=2)`.
    fn make_part_scan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: "part".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 2,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: P_PARTKEY,
                        name: "p_partkey".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: P_BRAND,
                        name: "p_brand".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    /// Build inner `Scan(lineitem, table_id=1)` — second instance with INNER_ ColumnIds.
    fn make_inner_lineitem_scan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: "lineitem".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 1,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: INNER_L_PARTKEY,
                        name: "l_partkey".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: INNER_L_QUANTITY,
                        name: "l_quantity".to_string(),
                        data_type: DataType::Float64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    /// Build outer left plan: `CrossJoin(lineitem_scan, part_scan)`.
    fn make_outer_join() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Cross,
                condition: None,
            }),
            vec![make_outer_lineitem_scan(), make_part_scan()],
            None,
        )
    }

    /// Build inner aggregate: `Agg{group_by:[inner_l_partkey], avg(l_quantity)}(inner_scan)`.
    /// This is the post-PushDownApplyAggFilter shape.
    fn make_inner_avg_agg() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64)],
                aggregates: vec![AggregateCall {
                    name: "avg".to_string(),
                    args: vec![col_ref(INNER_L_QUANTITY, "l_quantity", DataType::Float64)],
                    distinct: false,
                    result_type: DataType::Float64,
                    order_by: vec![],
                    output_column_id: AVG_RESULT,
                }],
                output_columns: vec![
                    OutputColumn {
                        column_id: INNER_L_PARTKEY,
                        name: "l_partkey".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: AVG_RESULT,
                        name: "avg(l_quantity)".to_string(),
                        data_type: DataType::Float64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
                already_pushed: false,
            }),
            vec![make_inner_lineitem_scan()],
            None,
        )
    }

    /// Build the q17-shaped `Filter(Apply(...))` plan.
    ///
    /// WHERE predicate:
    ///   (part.p_partkey == lineitem.l_partkey)
    ///   AND (part.p_brand == 'x')
    ///   AND (lineitem.l_quantity < APPLY_OUT)
    ///
    /// Apply: left = CrossJoin(lineitem, part), right = avg_agg(inner_lineitem),
    ///        correlation_conjuncts = [part.p_partkey == inner.l_partkey],
    ///        need_check_max_rows = false.
    fn winmagic_filter_apply() -> LogicalPlanNode {
        // Correlation conjunct: part.p_partkey == inner.l_partkey
        let corr_conj = eq_expr(
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
            col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
        );

        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "avg_subq".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: AVG_RESULT,
                correlation_column_ids: vec![P_PARTKEY],
                correlation_conjuncts: vec![corr_conj],
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_outer_join(), make_inner_avg_agg()],
            None,
        );

        // WHERE: (p_partkey == l_partkey) AND (p_brand == 'x') AND (l_quantity < APPLY_OUT)
        let pred = and_expr(
            and_expr(
                // corr twin: outer p_partkey == outer l_partkey
                eq_expr(
                    col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
                    col_ref(L_PARTKEY, "l_partkey", DataType::Int64),
                ),
                // extra: p_brand == 'x'  (references only correlated_outer_table=part)
                eq_expr(col_ref(P_BRAND, "p_brand", DataType::Utf8), str_lit("x")),
            ),
            // subquery comparison: l_quantity < APPLY_OUT
            lt_expr(
                col_ref(L_QUANTITY, "l_quantity", DataType::Float64),
                col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64),
            ),
        );

        LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode { predicate: pred }),
            vec![apply],
            None,
        )
    }

    /// Variant of the base fixture but with a leading Project above the agg:
    /// `Project[inner_output_col_id := 2.0 * AVG_RESULT](Agg)`.
    /// Here `inner_output_column_id` is VAL_ID (100), not AVG_RESULT.
    fn winmagic_filter_apply_with_project() -> (LogicalPlanNode, ColumnId) {
        const VAL_ID: ColumnId = ColumnId(100);
        let corr_conj = eq_expr(
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
            col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
        );
        // Project: VAL_ID := 2.0 * AVG_RESULT
        let project_expr = mul_expr(
            float_lit(2.0),
            col_ref_nullable(AVG_RESULT, "avg(l_quantity)", DataType::Float64),
        );
        let projected_right = LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    // passthrough: l_partkey
                    ProjectItem {
                        expr: col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
                        output_name: "l_partkey".to_string(),
                        output_column_id: INNER_L_PARTKEY,
                    },
                    // computed: 2.0 * avg -> VAL_ID
                    ProjectItem {
                        expr: project_expr,
                        output_name: "val".to_string(),
                        output_column_id: VAL_ID,
                    },
                ],
                output_qualifier: None,
            }),
            vec![make_inner_avg_agg()],
            None,
        );
        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "val_subq", DataType::Float64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "val_subq".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: VAL_ID,
                correlation_column_ids: vec![P_PARTKEY],
                correlation_conjuncts: vec![corr_conj],
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_outer_join(), projected_right],
            None,
        );
        let pred = and_expr(
            and_expr(
                eq_expr(
                    col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
                    col_ref(L_PARTKEY, "l_partkey", DataType::Int64),
                ),
                eq_expr(col_ref(P_BRAND, "p_brand", DataType::Utf8), str_lit("x")),
            ),
            lt_expr(
                col_ref(L_QUANTITY, "l_quantity", DataType::Float64),
                col_ref_nullable(APPLY_OUT, "val_subq", DataType::Float64),
            ),
        );
        (
            LogicalPlanNode::new(
                PlanNodeKind::Filter(LogicalFilterNode { predicate: pred }),
                vec![apply],
                None,
            ),
            VAL_ID,
        )
    }

    fn ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx
    }

    fn ctx_with_factory() -> RewriteContext {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_column_ref_factory(Rc::new(RefCell::new(ColumnRefFactory::new())));
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx
    }

    fn to_opt_expr(plan: &LogicalPlanNode, ctx: &mut RewriteContext) -> OptExpr {
        logical_plan_to_opt_expr(plan, &mut ctx.scalar_arena().borrow_mut())
    }

    fn mat_plan(expr: &OptExpr, ctx: &RewriteContext) -> LogicalPlanNode {
        opt_expr_to_plan(expr, &ctx.scalar_arena().borrow())
    }

    // ---- matches() tests --------------------------------------------------------

    #[test]
    fn matches_returns_true_for_q17_shape() {
        let rule = ApplyToWindow;
        let plan = winmagic_filter_apply();
        let mut ctx = ctx();
        let expr = to_opt_expr(&plan, &mut ctx);
        assert!(rule.matches(&expr, &ctx));
    }

    #[test]
    fn matches_returns_false_for_bare_apply() {
        let rule = ApplyToWindow;
        // Apply without Filter wrapper → should not match
        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Float64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: AVG_RESULT,
                correlation_column_ids: vec![P_PARTKEY],
                correlation_conjuncts: vec![eq_expr(
                    col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
                    col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
                )],
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_outer_join(), make_inner_avg_agg()],
            None,
        );
        let mut ctx = ctx();
        let expr = to_opt_expr(&apply, &mut ctx);
        assert!(!rule.matches(&expr, &ctx));
    }

    // ---- transform tests --------------------------------------------------------

    /// The transform must emit a Window with exactly one WindowExpr (avg),
    /// with partition_by pointing to the OUTER p_partkey, args[0] pointing
    /// to the OUTER l_quantity (NOT inner INNER_L_QUANTITY), and no Apply
    /// anywhere in the result tree.
    #[test]
    fn transform_emits_window_over_outer() {
        let rule = ApplyToWindow;
        let plan = winmagic_filter_apply();
        let mut ctx = ctx_with_factory();
        let expr = to_opt_expr(&plan, &mut ctx);

        let result = rule.apply(expr, &mut ctx).expect("apply must not error");
        let RewriteResult::Changed(result_expr) = result else {
            panic!("expected Changed, got Unchanged");
        };
        let result = mat_plan(&result_expr, &ctx);

        // No Apply anywhere.
        assert!(
            super::super::find_residual_apply(&result_expr).is_none(),
            "Apply must be gone after transform"
        );

        // after-window Filter at the top.
        let PlanNodeKind::Filter(_after_filter) = &result.kind else {
            panic!("expected after-window Filter at root, got: {:?}", result);
        };

        // Window inside.
        let window_plan = result.unary_input();
        let PlanNodeKind::Window(window_node) = &window_plan.kind else {
            panic!("expected Window under after-filter");
        };
        assert_eq!(window_node.window_exprs.len(), 1);
        let win_expr = &window_node.window_exprs[0];
        assert_eq!(win_expr.name, "avg");

        // Partition-by: one ColumnRef pointing to OUTER p_partkey (P_PARTKEY = 10).
        assert_eq!(win_expr.partition_by.len(), 1);
        let ExprKind::ColumnRef {
            column_id: pb_id, ..
        } = &win_expr.partition_by[0].kind
        else {
            panic!("expected ColumnRef in partition_by");
        };
        assert_eq!(
            *pb_id, P_PARTKEY,
            "partition_by must reference outer p_partkey"
        );

        // args[0] must reference OUTER l_quantity (L_QUANTITY = 3, NOT INNER_L_QUANTITY = 21).
        assert_eq!(win_expr.args.len(), 1);
        let ExprKind::ColumnRef {
            column_id: arg_id, ..
        } = &win_expr.args[0].kind
        else {
            panic!("expected ColumnRef in window args");
        };
        assert_eq!(
            *arg_id, L_QUANTITY,
            "window arg must reference outer lineitem.l_quantity (id=3), not inner (id=21)"
        );

        // Sort under Window.
        let sort_plan = window_plan.unary_input();
        let PlanNodeKind::Sort(sort_node) = &sort_plan.kind else {
            panic!("expected Sort under Window");
        };
        assert_eq!(sort_node.items.len(), 1);
        assert_eq!(sort_node.analytic_partition_by.len(), 1);

        // before-window Filter under Sort.
        let before_filter_plan = sort_plan.unary_input();
        let PlanNodeKind::Filter(before_filter) = &before_filter_plan.kind else {
            panic!("expected before-window Filter under Sort");
        };
        // before-filter predicate must contain p_partkey == l_partkey AND p_brand == 'x'
        // (NOT the l_quantity < APPLY_OUT conjunct).
        let before_conjuncts = split_and(before_filter.predicate.clone());
        // Should not contain any reference to APPLY_OUT.
        for c in &before_conjuncts {
            let ids = collect_column_id_refs(c);
            assert!(
                !ids.contains(&APPLY_OUT),
                "before-window filter must not reference APPLY_OUT"
            );
        }
        // before-filter input is the original CrossJoin.
        assert!(
            matches!(
                &before_filter_plan.unary_input().kind,
                PlanNodeKind::Join(_)
            ),
            "before-window filter input must be the original Join"
        );
    }

    /// The after-window Filter predicate must reference WIN_ID (the minted window
    /// output column), not APPLY_OUT and not the inner AVG_RESULT.
    ///
    /// Two sub-cases:
    /// (a) bare aggregate (no leading Project): value_expr == ColumnRef(WIN_ID).
    /// (b) with leading Project (`2 * avg`): value_expr == `2.0 * ColumnRef(WIN_ID)`.
    #[test]
    fn transform_rewrites_subquery_comparison_to_window_col() {
        let rule = ApplyToWindow;

        // --- (a) Bare aggregate (no leading Project) ---
        {
            let plan = winmagic_filter_apply();
            let mut ctx = ctx_with_factory();
            let expr = to_opt_expr(&plan, &mut ctx);
            let result = rule.apply(expr, &mut ctx).unwrap();
            let RewriteResult::Changed(result_expr) = result else {
                panic!("expected Changed")
            };
            let result = mat_plan(&result_expr, &ctx);
            let PlanNodeKind::Filter(after) = &result.kind else {
                panic!("expected Filter")
            };
            let PlanNodeKind::Window(win) = &result.unary_input().kind else {
                panic!("expected Window")
            };
            let win_id = win.window_exprs[0].output_column_id;

            // The after-filter predicate must reference win_id, not APPLY_OUT, not AVG_RESULT.
            let refs = collect_column_id_refs(&after.predicate);
            assert!(
                refs.contains(&win_id),
                "after-window predicate must reference WIN_ID (bare agg case)"
            );
            assert!(
                !refs.contains(&APPLY_OUT),
                "after-window predicate must NOT reference APPLY_OUT"
            );
            assert!(
                !refs.contains(&AVG_RESULT),
                "after-window predicate must NOT reference inner AVG_RESULT"
            );
        }

        // --- (b) With leading Project (2 * avg) ---
        {
            let (plan, _val_id) = winmagic_filter_apply_with_project();
            let mut ctx = ctx_with_factory();
            let expr = to_opt_expr(&plan, &mut ctx);
            let result = rule.apply(expr, &mut ctx).unwrap();
            let RewriteResult::Changed(result_expr) = result else {
                panic!("expected Changed")
            };
            let result = mat_plan(&result_expr, &ctx);
            let PlanNodeKind::Filter(after) = &result.kind else {
                panic!("expected Filter")
            };
            let PlanNodeKind::Window(win) = &result.unary_input().kind else {
                panic!("expected Window")
            };
            let win_id = win.window_exprs[0].output_column_id;

            let refs = collect_column_id_refs(&after.predicate);
            assert!(
                refs.contains(&win_id),
                "after-window predicate must reference WIN_ID (project case)"
            );
            assert!(
                !refs.contains(&APPLY_OUT),
                "after-window predicate must NOT reference APPLY_OUT (project case)"
            );
            assert!(
                !refs.contains(&AVG_RESULT),
                "after-window predicate must NOT reference inner AVG_RESULT (project case)"
            );
            // The value expression must be `2.0 * WIN_ID`, i.e. the BinaryOp must be Mul.
            // Walk the predicate: find the BinaryOp whose right (or left) is ColumnRef(win_id).
            fn contains_mul_with_win(expr: &TypedExpr, win_id: ColumnId) -> bool {
                match &expr.kind {
                    ExprKind::BinaryOp {
                        left,
                        op: BinOp::Mul,
                        right,
                    } => {
                        let lr = collect_column_id_refs(right);
                        let ll = collect_column_id_refs(left);
                        if lr.contains(&win_id) || ll.contains(&win_id) {
                            return true;
                        }
                        contains_mul_with_win(left, win_id) || contains_mul_with_win(right, win_id)
                    }
                    ExprKind::BinaryOp { left, right, .. } => {
                        contains_mul_with_win(left, win_id) || contains_mul_with_win(right, win_id)
                    }
                    _ => false,
                }
            }
            assert!(
                contains_mul_with_win(&after.predicate, win_id),
                "after-window predicate for project case must contain (2.0 * WIN_ID)"
            );
        }
    }

    /// When preconditions fail (e.g. self-join on outer), the rule must return Unchanged.
    #[test]
    fn transform_unchanged_when_precondition_fails() {
        let rule = ApplyToWindow;
        // Self-join outer: CrossJoin(lineitem(table_id=1), lineitem(table_id=1))
        let plan = winmagic_filter_apply();
        let (pred, a_orig, _, right) = extract_filter_apply(&plan);
        let bad_left = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Cross,
                condition: None,
            }),
            vec![make_outer_lineitem_scan(), make_outer_lineitem_scan()],
            None,
        );
        let bad_plan = make_filter_apply(pred.clone(), a_orig.clone(), bad_left, right.clone());

        let mut ctx = ctx_with_factory();
        let expr = to_opt_expr(&bad_plan, &mut ctx);
        let result = rule.apply(expr, &mut ctx).expect("must not error");
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "self-join fixture must produce Unchanged"
        );
    }

    // ---- precondition tests -----------------------------------------------------

    /// Helper: extract the LogicalApplyNode and predicate from the canonical Filter(Apply) fixture.
    fn extract_filter_apply(
        plan: &LogicalPlanNode,
    ) -> (
        &TypedExpr,
        &LogicalApplyNode,
        &LogicalPlanNode,
        &LogicalPlanNode,
    ) {
        let PlanNodeKind::Filter(f) = &plan.kind else {
            panic!("expected Filter")
        };
        let apply_plan = plan.unary_input();
        let PlanNodeKind::Apply(a) = &apply_plan.kind else {
            panic!("expected Apply")
        };
        (&f.predicate, a, apply_plan.left(), apply_plan.right())
    }

    fn make_apply_plan(
        apply: LogicalApplyNode,
        left: LogicalPlanNode,
        right: LogicalPlanNode,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(PlanNodeKind::Apply(apply), vec![left, right], None)
    }

    fn make_filter_apply(
        predicate: TypedExpr,
        apply: LogicalApplyNode,
        left: LogicalPlanNode,
        right: LogicalPlanNode,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode { predicate }),
            vec![make_apply_plan(apply, left, right)],
            None,
        )
    }

    fn with_kind_like(original: &LogicalPlanNode, kind: PlanNodeKind) -> LogicalPlanNode {
        LogicalPlanNode::new(
            kind,
            original.children.clone(),
            original.required_output_columns.clone(),
        )
    }

    #[test]
    fn precond_accepts_q17_shape() {
        let plan = winmagic_filter_apply();
        let (pred, a, left, right) = extract_filter_apply(&plan);
        assert!(
            check_preconditions(pred, a, left, right).is_some(),
            "q17-shaped Filter(Apply) must pass all preconditions"
        );
    }

    #[test]
    fn precond_rejects_non_whitelist_agg() {
        let plan = winmagic_filter_apply();
        let (pred, a_orig, left, right) = extract_filter_apply(&plan);
        // Replace avg with array_agg (not in whitelist)
        let a = a_orig.clone();
        let orig_agg = right.as_aggregate().unwrap();
        let bad_agg = LogicalAggregateNode {
            aggregates: vec![AggregateCall {
                name: "array_agg".to_string(),
                ..orig_agg.aggregates[0].clone()
            }],
            ..orig_agg.clone()
        };
        let bad_right = with_kind_like(right, PlanNodeKind::Aggregate(bad_agg));
        assert!(
            check_preconditions(pred, &a, left, &bad_right).is_none(),
            "non-whitelist agg must reject"
        );
    }

    #[test]
    fn precond_rejects_distinct_agg() {
        let plan = winmagic_filter_apply();
        let (pred, a_orig, left, right) = extract_filter_apply(&plan);
        let a = a_orig.clone();
        let orig_agg = right.as_aggregate().unwrap();
        let bad_agg = LogicalAggregateNode {
            aggregates: vec![AggregateCall {
                distinct: true,
                ..orig_agg.aggregates[0].clone()
            }],
            ..orig_agg.clone()
        };
        let bad_right = with_kind_like(right, PlanNodeKind::Aggregate(bad_agg));
        assert!(
            check_preconditions(pred, &a, left, &bad_right).is_none(),
            "distinct agg must reject"
        );
    }

    #[test]
    fn precond_rejects_two_aggregates() {
        let plan = winmagic_filter_apply();
        let (pred, a_orig, left, right) = extract_filter_apply(&plan);
        let a = a_orig.clone();
        let orig_agg = right.as_aggregate().unwrap();
        let two_agg = LogicalAggregateNode {
            aggregates: vec![
                orig_agg.aggregates[0].clone(),
                AggregateCall {
                    name: "min".to_string(),
                    output_column_id: ColumnId(99),
                    ..orig_agg.aggregates[0].clone()
                },
            ],
            ..orig_agg.clone()
        };
        let bad_right = with_kind_like(right, PlanNodeKind::Aggregate(two_agg));
        assert!(
            check_preconditions(pred, &a, left, &bad_right).is_none(),
            "two aggregates must reject"
        );
    }

    #[test]
    fn precond_rejects_self_join_outer() {
        // Outer: CrossJoin(lineitem(table_id=1), lineitem(table_id=1)) — two same-table scans
        let plan = winmagic_filter_apply();
        let (pred, a_orig, _, right) = extract_filter_apply(&plan);
        let a = a_orig.clone();
        // Replace part scan with another lineitem scan (same table_id=1)
        let dup_lineitem = make_outer_lineitem_scan();
        let bad_left = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Cross,
                condition: None,
            }),
            vec![make_outer_lineitem_scan(), dup_lineitem],
            None,
        );
        assert!(
            check_preconditions(pred, &a, &bad_left, right).is_none(),
            "self-join (duplicate table) on outer must reject"
        );
    }

    #[test]
    fn precond_rejects_table_set_mismatch() {
        // Subquery scans a table_id=99 absent from outer (outer has table_id=1 and 2)
        let plan = winmagic_filter_apply();
        let (pred, a_orig, left, right) = extract_filter_apply(&plan);
        let a = a_orig.clone();
        // Replace inner scan with one from table_id=99
        let foreign_scan = LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: "other".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 99,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: INNER_L_PARTKEY,
                        name: "l_partkey".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: INNER_L_QUANTITY,
                        name: "l_quantity".to_string(),
                        data_type: DataType::Float64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let orig_agg = right.as_aggregate().unwrap();
        let foreign_agg = LogicalAggregateNode { ..orig_agg.clone() };
        let bad_right = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(foreign_agg),
            vec![foreign_scan],
            right.required_output_columns.clone(),
        );
        assert!(
            check_preconditions(pred, &a, left, &bad_right).is_none(),
            "subquery with foreign table must reject"
        );
    }

    #[test]
    fn precond_rejects_limit_in_subtree() {
        // Wrap outer left in a Limit node (not whitelisted)
        let plan = winmagic_filter_apply();
        let (pred, a_orig, left, right) = extract_filter_apply(&plan);
        let a = a_orig.clone();
        let bad_left = LogicalPlanNode::new(
            PlanNodeKind::Limit(LogicalLimitNode {
                limit: Some(10),
                offset: None,
            }),
            vec![left.clone()],
            None,
        );
        assert!(
            check_preconditions(pred, &a, &bad_left, right).is_none(),
            "Limit in outer subtree must reject"
        );
    }

    #[test]
    fn precond_rejects_predicate_mismatch() {
        // Add a residual Filter inside the subquery aggregate's input that
        // has no twin in the outer WHERE predicate.
        let plan = winmagic_filter_apply();
        let (pred, a_orig, left, right) = extract_filter_apply(&plan);
        let a = a_orig.clone();
        // Add a Filter below the aggregate input: l_quantity > 0
        let residual_pred = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(INNER_L_QUANTITY, "l_quantity", DataType::Float64)),
                op: BinOp::Gt,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(0)),
                    data_type: DataType::Float64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let inner_with_filter = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: residual_pred,
            }),
            vec![make_inner_lineitem_scan()],
            None,
        );
        let orig_agg = right.as_aggregate().unwrap();
        let agg_with_residual = LogicalAggregateNode { ..orig_agg.clone() };
        let bad_right = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(agg_with_residual),
            vec![inner_with_filter],
            right.required_output_columns.clone(),
        );
        assert!(
            check_preconditions(pred, &a, left, &bad_right).is_none(),
            "residual Filter conjunct without outer twin must reject"
        );
    }

    #[test]
    fn precond_rejects_no_subquery_conjunct() {
        // WHERE predicate doesn't reference APPLY_OUT at all
        let plan = winmagic_filter_apply();
        let (_, a, left, right) = extract_filter_apply(&plan);
        // Build a predicate that has no APPLY_OUT reference
        let pred_without_apply = eq_expr(
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
            col_ref(L_PARTKEY, "l_partkey", DataType::Int64),
        );
        assert!(
            check_preconditions(&pred_without_apply, a, left, right).is_none(),
            "WHERE predicate without APPLY_OUT reference must reject"
        );
    }

    /// Step 3(b): exercises the 4d per-conjunct mismatch branch directly.
    /// A subquery with a residual Filter that has the SAME number of conjuncts
    /// as the outer-side remainders but a DIFFERENT conjunct (non-phys-eq) must
    /// reject.
    #[test]
    fn precond_rejects_4d_differing_residual_same_count() {
        // Build a plan where:
        // - outer WHERE has one "extra" conjunct beyond corr-twin and subquery-comparison:
        //   l_orderkey > 100  (references outer lineitem, not only part)
        // - subquery has a residual Filter with one conjunct, but different:
        //   inner l_quantity < 999.0  (does NOT phys-match l_orderkey > 100)
        let corr_conj = eq_expr(
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
            col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
        );
        // Residual inside subquery: l_quantity < 999
        let inner_residual = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(INNER_L_QUANTITY, "l_quantity", DataType::Float64)),
                op: BinOp::Lt,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Float(999.0)),
                    data_type: DataType::Float64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let inner_with_filter = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: inner_residual,
            }),
            vec![make_inner_lineitem_scan()],
            None,
        );
        let agg_with_residual = LogicalAggregateNode {
            group_by: vec![col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64)],
            aggregates: vec![AggregateCall {
                name: "avg".to_string(),
                args: vec![col_ref(INNER_L_QUANTITY, "l_quantity", DataType::Float64)],
                distinct: false,
                result_type: DataType::Float64,
                order_by: vec![],
                output_column_id: AVG_RESULT,
            }],
            output_columns: vec![
                OutputColumn {
                    column_id: INNER_L_PARTKEY,
                    name: "l_partkey".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: AVG_RESULT,
                    name: "avg(l_quantity)".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            already_pushed: false,
        };
        let right = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(agg_with_residual),
            vec![inner_with_filter],
            None,
        );
        let apply = LogicalApplyNode {
            kind: ApplyKind::Scalar,
            subquery_expr: col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64),
            output_column: OutputColumn {
                column_id: APPLY_OUT,
                name: "avg_subq".to_string(),
                data_type: DataType::Float64,
                nullable: true,
                is_internal: true,
            },
            inner_output_column_id: AVG_RESULT,
            correlation_column_ids: vec![P_PARTKEY],
            correlation_conjuncts: vec![corr_conj],
            residual_predicate: None,
            need_check_max_rows: false,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
        };
        let left = make_outer_join();
        // Outer WHERE: (p_partkey == l_partkey) AND (l_orderkey > 100) AND (l_quantity < APPLY_OUT)
        // l_orderkey > 100 references outer lineitem (not only "part"), so 4c does NOT drop it.
        // After 4a (removes corr-twin) and 4b (removes subquery-comparison), outer_conjuncts = [l_orderkey > 100].
        // sub_residual = [inner l_quantity < 999].  Same count (1), but non-phys-eq → None.
        let outer_extra = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(L_ORDERKEY, "l_orderkey", DataType::Int64)),
                op: BinOp::Gt,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(100)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let pred = and_expr(
            and_expr(
                eq_expr(
                    col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
                    col_ref(L_PARTKEY, "l_partkey", DataType::Int64),
                ),
                outer_extra,
            ),
            lt_expr(
                col_ref(L_QUANTITY, "l_quantity", DataType::Float64),
                col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64),
            ),
        );
        assert!(
            check_preconditions(&pred, &apply, &left, &right).is_none(),
            "differing residual conjunct (same count, non-phys-eq) must reject (4d branch)"
        );
    }

    /// Regression test for the Between-wrapped subquery comparison bug.
    ///
    /// Builds a q17-shaped fixture where the subquery comparison conjunct is:
    ///   `l_quantity BETWEEN (APPLY_OUT - 1.0) AND (APPLY_OUT + 1.0)`
    ///
    /// Before the fix, `replace_column_ref` did not recurse into `Between` children,
    /// so APPLY_OUT survived as a dangling reference. After the fix the transform must
    /// SUCCEED: result has a Window, no Apply, and the after-window predicate no longer
    /// references APPLY_OUT (it references win_id instead).
    #[test]
    fn transform_rewrites_subquery_comparison_inside_between() {
        // The subquery conjunct: l_quantity BETWEEN (APPLY_OUT - 1.0) AND (APPLY_OUT + 1.0)
        // low  = APPLY_OUT - 1.0
        let low_expr = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64)),
                op: BinOp::Sub,
                right: Box::new(float_lit(1.0)),
            },
            data_type: DataType::Float64,
            nullable: true,
        };
        // high = APPLY_OUT + 1.0
        let high_expr = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64)),
                op: BinOp::Add,
                right: Box::new(float_lit(1.0)),
            },
            data_type: DataType::Float64,
            nullable: true,
        };
        let between_conjunct = TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(col_ref(L_QUANTITY, "l_quantity", DataType::Float64)),
                low: Box::new(low_expr),
                high: Box::new(high_expr),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };

        // Build the corr conjunct and Apply/Filter plan mirroring winmagic_filter_apply().
        let corr_conj = eq_expr(
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
            col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
        );
        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "avg_subq".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: AVG_RESULT,
                correlation_column_ids: vec![P_PARTKEY],
                correlation_conjuncts: vec![corr_conj],
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_outer_join(), make_inner_avg_agg()],
            None,
        );

        // WHERE: (p_partkey == l_partkey) AND (p_brand == 'x') AND BETWEEN(...)
        let pred = and_expr(
            and_expr(
                eq_expr(
                    col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
                    col_ref(L_PARTKEY, "l_partkey", DataType::Int64),
                ),
                eq_expr(col_ref(P_BRAND, "p_brand", DataType::Utf8), str_lit("x")),
            ),
            between_conjunct.clone(),
        );
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode { predicate: pred }),
            vec![apply],
            None,
        );

        let rule = ApplyToWindow;
        let mut ctx = ctx_with_factory();
        let expr = to_opt_expr(&plan, &mut ctx);
        let result = rule.apply(expr, &mut ctx).expect("apply must not error");
        let RewriteResult::Changed(result_expr) = result else {
            panic!("expected Changed — Between-wrapped APPLY_OUT should now be handled");
        };
        let result = mat_plan(&result_expr, &ctx);

        // Structural assertions.
        assert!(
            super::super::find_residual_apply(&result_expr).is_none(),
            "Apply must be gone after transform"
        );
        let PlanNodeKind::Filter(after_filter) = &result.kind else {
            panic!("expected after-window Filter at root");
        };
        assert!(
            matches!(&result.unary_input().kind, PlanNodeKind::Window(_)),
            "expected Window under after-filter"
        );

        // Key correctness check: APPLY_OUT is gone from the after-window predicate.
        let refs = collect_column_id_refs(&after_filter.predicate);
        assert!(
            !refs.contains(&APPLY_OUT),
            "after-window predicate must NOT reference APPLY_OUT after Between rewrite"
        );
        // win_id is present instead.
        let PlanNodeKind::Window(win) = &result.unary_input().kind else {
            unreachable!()
        };
        let win_id = win.window_exprs[0].output_column_id;
        assert!(
            refs.contains(&win_id),
            "after-window predicate must reference the minted WIN_ID"
        );
    }

    // ---- full-pipeline integration tests ----------------------------------------

    /// Build the PRE-pushdown shape: Apply{need_check_max_rows=true,
    /// correlation_column_ids=[P_PARTKEY], correlation_conjuncts=[]} where the inner
    /// is Agg{group_by:[]}(Filter(inner.l_partkey==outer.p_partkey)(inner_scan)).
    /// This is the shape the planner emits before PushDownApplyAggFilter fires.
    fn winmagic_pre_pushdown_filter_apply() -> LogicalPlanNode {
        // Correlation predicate inside the inner filter: inner.l_partkey == outer.p_partkey
        let inner_corr_pred = eq_expr(
            col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
        );
        // Inner: Agg{group_by:[]}(Filter(corr_pred)(inner_scan))
        let inner_filter = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: inner_corr_pred,
            }),
            vec![make_inner_lineitem_scan()],
            None,
        );
        let inner_agg = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "avg".to_string(),
                    args: vec![col_ref(INNER_L_QUANTITY, "l_quantity", DataType::Float64)],
                    distinct: false,
                    result_type: DataType::Float64,
                    order_by: vec![],
                    output_column_id: AVG_RESULT,
                }],
                output_columns: vec![OutputColumn {
                    column_id: AVG_RESULT,
                    name: "avg(l_quantity)".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![inner_filter],
            None,
        );

        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "avg_subq".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    is_internal: true,
                },
                // inner_output_column_id == AVG_RESULT (no leading Project)
                inner_output_column_id: AVG_RESULT,
                correlation_column_ids: vec![P_PARTKEY],
                // correlation_conjuncts is EMPTY — PushDownApplyAggFilter has not run yet
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                // pre-pushdown flag
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_outer_join(), inner_agg],
            None,
        );

        // WHERE: (p_partkey == l_partkey) AND (p_brand == 'x') AND (l_quantity < APPLY_OUT)
        let pred = and_expr(
            and_expr(
                eq_expr(
                    col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
                    col_ref(L_PARTKEY, "l_partkey", DataType::Int64),
                ),
                eq_expr(col_ref(P_BRAND, "p_brand", DataType::Utf8), str_lit("x")),
            ),
            lt_expr(
                col_ref(L_QUANTITY, "l_quantity", DataType::Float64),
                col_ref_nullable(APPLY_OUT, "avg_subq", DataType::Float64),
            ),
        );

        LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode { predicate: pred }),
            vec![apply],
            None,
        )
    }

    /// Recursively find any Window node in the plan tree.
    ///
    /// Test-only walker. Covers only the plan shapes the SubqueryRewrite stage can
    /// emit (Filter/Sort/Project/Aggregate/Join/Limit inputs). The Apply case is
    /// intentionally omitted: every caller asserts `find_residual_apply(..).is_none()`
    /// first, so no Apply is present when this runs. Extend if a future rule emits a
    /// Window under a variant not traversed here.
    fn find_window(plan: &LogicalPlanNode) -> bool {
        match &plan.kind {
            PlanNodeKind::Window(_) => true,
            PlanNodeKind::Filter(_)
            | PlanNodeKind::Sort(_)
            | PlanNodeKind::Project(_)
            | PlanNodeKind::Aggregate(_)
            | PlanNodeKind::Limit(_) => find_window(plan.unary_input()),
            PlanNodeKind::Join(_) => find_window(plan.left()) || find_window(plan.right()),
            _ => false,
        }
    }

    /// Recursively find any LeftOuter Join node in the plan tree.
    ///
    /// Test-only walker. Covers only the plan shapes the SubqueryRewrite stage can
    /// emit (Join/Filter/Sort/Project/Aggregate/Limit/Window inputs). The Apply case
    /// is intentionally omitted: every caller asserts `find_residual_apply(..).is_none()`
    /// first, so no Apply is present when this runs. Extend if a future rule emits a
    /// LeftOuter join under a variant not traversed here.
    fn find_left_outer_join(plan: &LogicalPlanNode) -> bool {
        match &plan.kind {
            PlanNodeKind::Join(n) if n.join_type == JoinKind::LeftOuter => true,
            PlanNodeKind::Join(_) => {
                find_left_outer_join(plan.left()) || find_left_outer_join(plan.right())
            }
            PlanNodeKind::Filter(_)
            | PlanNodeKind::Sort(_)
            | PlanNodeKind::Project(_)
            | PlanNodeKind::Aggregate(_)
            | PlanNodeKind::Limit(_)
            | PlanNodeKind::Window(_) => find_left_outer_join(plan.unary_input()),
            _ => false,
        }
    }

    /// Full-pipeline integration test (SubqueryRewrite stage only — the full query
    /// pipeline's later stages such as ColumnPruning require `required_output_columns`
    /// wiring that synthetic fixtures lack).
    ///
    /// Variant 1: ApplyToWindow enabled.
    ///   A PRE-pushdown correlated scalar-aggregate Apply (need_check_max_rows=true,
    ///   correlation_conjuncts=[]) fed through the SubqueryRewrite stage must emerge
    ///   as a Window node with no Apply remaining in the tree.
    #[test]
    fn full_pipeline_pre_pushdown_becomes_window() {
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::rules::subquery::subquery_rewrite_rules;

        let plan = winmagic_pre_pushdown_filter_apply();

        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_column_ref_factory(Rc::new(RefCell::new(ColumnRefFactory::new())));
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        let expr = to_opt_expr(&plan, &mut ctx);

        // Run only the SubqueryRewrite stage so the fixture doesn't need full column wiring.
        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "SubqueryRewrite",
            RewritePhase::StructuralRewrite,
            subquery_rewrite_rules(),
        )]);
        let result_expr = pipeline
            .rewrite(expr, &mut ctx)
            .expect("SubqueryRewrite stage must succeed for decorrelatable Apply");
        let result = mat_plan(&result_expr, &ctx);

        // No Apply must survive.
        assert!(
            super::super::find_residual_apply(&result_expr).is_none(),
            "no Apply must survive after SubqueryRewrite stage"
        );

        // A Window node must be present (ApplyToWindow fired).
        assert!(
            find_window(&result),
            "expected a Window node in the result — ApplyToWindow must have fired"
        );
    }

    /// Full-pipeline integration test (SubqueryRewrite stage only).
    ///
    /// Variant 2: ApplyToWindow disabled via `disable_optimizer_rules='ApplyToWindow'`.
    ///   The same PRE-pushdown fixture must fall back to the M1 LEFT OUTER JOIN form
    ///   produced by ScalarApplyToJoin, with no Window node in the tree.
    #[test]
    fn full_pipeline_pre_pushdown_disabled_falls_back_to_join() {
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::rules::subquery::subquery_rewrite_rules;

        let plan = winmagic_pre_pushdown_filter_apply();

        // Disable ApplyToWindow → should fall back to ScalarApplyToJoin (LEFT OUTER JOIN form).
        let mut ctx = RewriteContext::for_query(vec!["ApplyToWindow".to_string()]);
        ctx.set_column_ref_factory(Rc::new(RefCell::new(ColumnRefFactory::new())));
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        let expr = to_opt_expr(&plan, &mut ctx);

        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "SubqueryRewrite",
            RewritePhase::StructuralRewrite,
            subquery_rewrite_rules(),
        )]);
        let result_expr = pipeline
            .rewrite(expr, &mut ctx)
            .expect("SubqueryRewrite stage must succeed when ApplyToWindow is disabled");
        let result = mat_plan(&result_expr, &ctx);

        // No Apply must survive.
        assert!(
            super::super::find_residual_apply(&result_expr).is_none(),
            "no Apply must survive after SubqueryRewrite stage (disabled-ApplyToWindow path)"
        );

        // No Window — ApplyToWindow was disabled.
        assert!(
            !find_window(&result),
            "no Window must be present when ApplyToWindow is disabled"
        );

        // ScalarApplyToJoin must have produced a LEFT OUTER JOIN form.
        assert!(
            find_left_outer_join(&result),
            "expected a LEFT OUTER JOIN when ApplyToWindow is disabled (M1 join form)"
        );
    }

    // Helper trait to make test code more readable — extracts LogicalAggregateNode from plan.
    trait AsAggregate {
        fn as_aggregate(&self) -> Option<&LogicalAggregateNode>;
    }

    impl AsAggregate for LogicalPlanNode {
        fn as_aggregate(&self) -> Option<&LogicalAggregateNode> {
            match &self.kind {
                PlanNodeKind::Aggregate(a) => Some(a),
                _ => None,
            }
        }
    }
}
