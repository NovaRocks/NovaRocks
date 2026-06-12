//! `ApplyToWindow` — ports StarRocks `ScalarApply2AnalyticRule` ("WinMagic").
//!
//! Rewrites `Filter( ... lhs op APPLY_OUT ... )` over a decorrelated correlated
//! scalar-aggregate `Apply` into a `Window` (analytic) over the OUTER relation,
//! discarding the subquery subtree. Runs BEFORE `ScalarApplyToJoin`; on any
//! precondition failure returns `Unchanged` so `ScalarApplyToJoin` produces the
//! M1 join form. Never errors (the join form is always a valid fallback).

use std::collections::HashMap;
use std::collections::HashSet;

use arrow::datatypes::DataType;

use super::win_magic_util::{
    TableIdentity, collect_scan_column_map, collect_table_ids, expr_phys_eq,
};
use crate::sql::analysis::{ExprKind, OutputColumn, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{
    collect_column_id_refs, combine_and, split_and,
};
use crate::sql::planner::plan::{
    AggregateCall, AggregateNode, ApplyKind, ApplyNode, FilterNode, LogicalPlan, SortNode,
    WindowExpr, WindowNode,
};
use crate::sql::planner::plan_output_columns;

const WHITELIST: &[&str] = &["count", "sum", "avg", "min", "max"];

pub(crate) struct ApplyToWindow;

/// Everything Task 3's transform needs, validated by `check_preconditions`.
#[allow(dead_code)]
pub(super) struct WinMagicMatch {
    /// All conjuncts of the matched WHERE Filter (already AND-split).
    pub outer_conjuncts: Vec<TypedExpr>,
    /// The single outer conjunct that references `APPLY_OUT`.
    pub subquery_conjunct: TypedExpr,
    /// Outer-side ColumnRef of each correlation conjunct — the window PARTITION BY keys.
    pub partition_by: Vec<TypedExpr>,
    /// The inner single aggregate call (name in WHITELIST, non-distinct).
    pub inner_agg: AggregateCall,
}

impl LogicalRewriteRule for ApplyToWindow {
    fn name(&self) -> &'static str {
        "ApplyToWindow"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        let LogicalPlan::Filter(f) = plan else {
            return false;
        };
        let LogicalPlan::Apply(a) = f.input.as_ref() else {
            return false;
        };
        a.kind == ApplyKind::Scalar && !a.need_check_max_rows && !a.correlation_conjuncts.is_empty()
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Filter(f) = &plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let LogicalPlan::Apply(a) = f.input.as_ref() else {
            return Ok(RewriteResult::Unchanged);
        };
        let Some(m) = check_preconditions(&f.predicate, a) else {
            return Ok(RewriteResult::Unchanged);
        };

        // Re-own the pieces. (matches() guaranteed Filter(Apply).)
        let LogicalPlan::Filter(f) = plan else {
            unreachable!()
        };
        let LogicalPlan::Apply(a) = *f.input else {
            unreachable!()
        };

        // --- 1. Remap the inner aggregate's args to the outer instance of the same physical column. ---
        let outer_map = collect_scan_column_map(&a.left);
        let inner_map = collect_scan_column_map(&a.right);
        // Collect the set of inner-scan ColumnIds upfront; used by post-condition guards.
        let inner_ids: HashSet<ColumnId> = inner_map.keys().copied().collect();
        let outer_cols = plan_output_columns(&a.left)?;
        let mut phys_to_outer: HashMap<(TableIdentity, String), OutputColumn> = HashMap::new();
        for oc in &outer_cols {
            if let Some((tab, name)) = outer_map.get(&oc.column_id) {
                phys_to_outer.insert((tab.clone(), name.clone()), oc.clone());
            }
        }
        let mut agg_args = m.inner_agg.args.clone();
        for arg in &mut agg_args {
            if !remap_inner_to_outer(arg, &inner_map, &phys_to_outer) {
                // Required column unavailable on outer side → fall back to join form.
                return Ok(RewriteResult::Unchanged);
            }
        }
        // Guard 1: post-condition — verify no inner-scan column survived the remap.
        // This catches any ExprKind variant that remap_inner_to_outer might have missed.
        for arg in &agg_args {
            if !collect_column_id_refs(arg).is_disjoint(&inner_ids) {
                return Ok(RewriteResult::Unchanged); // an inner column survived the remap
            }
        }

        // --- 2. Mint the window output column; build the WindowExpr. ---
        let factory = ctx
            .column_ref_factory()
            .ok_or_else(|| "ApplyToWindow requires ColumnRefFactory".to_string())?;
        let win_id = factory.borrow_mut().create(
            None,
            format!("{}_window", m.inner_agg.name),
            m.inner_agg.result_type.clone(),
            true,
        );
        let win_expr = WindowExpr {
            name: m.inner_agg.name.clone(),
            args: agg_args,
            distinct: false,
            partition_by: m.partition_by.clone(),
            order_by: vec![],
            window_frame: None,
            result_type: m.inner_agg.result_type.clone(),
            output_name: format!("{}_window", m.inner_agg.name),
            output_column_id: win_id,
            ignore_nulls: false,
        };

        // --- 3. before-window Filter = all outer conjuncts except the subquery one. ---
        let before: Vec<TypedExpr> = m
            .outer_conjuncts
            .iter()
            .filter(|oc| !expr_struct_eq(oc, &m.subquery_conjunct))
            .cloned()
            .collect();
        let outer_subtree = *a.left;
        let before_filtered = if before.is_empty() {
            outer_subtree
        } else {
            LogicalPlan::Filter(FilterNode {
                predicate: combine_and(before),
                input: Box::new(outer_subtree),
                required_output_columns: None,
            })
        };

        // --- 4. Sort(partition keys) under the Window. ---
        let sort_items: Vec<SortItem> = m
            .partition_by
            .iter()
            .map(|e| SortItem {
                expr: e.clone(),
                asc: true,
                nulls_first: true,
            })
            .collect();
        let sorted = LogicalPlan::Sort(SortNode {
            input: Box::new(before_filtered),
            items: sort_items,
            analytic_partition_by: m.partition_by.clone(),
            required_output_columns: None,
        });

        // --- 5. Window node: output = base outer columns + the window column. ---
        let mut window_output = plan_output_columns(&sorted)?;
        window_output.push(OutputColumn {
            column_id: win_id,
            name: format!("{}_window", m.inner_agg.name),
            data_type: m.inner_agg.result_type.clone(),
            nullable: true,
            is_internal: true,
        });
        let window = LogicalPlan::Window(WindowNode {
            input: Box::new(sorted),
            window_exprs: vec![win_expr],
            output_columns: window_output,
            required_output_columns: None,
        });

        // --- 6. after-window Filter = subquery comparison with APPLY_OUT replaced by the value expr. ---
        let value_expr = build_value_expr(
            &a.right,
            a.inner_output_column_id,
            m.inner_agg.output_column_id,
            win_id,
            &m.inner_agg.result_type,
        )?;
        // Guard 2: value_expr must not reference the inner agg output or any inner-scan column.
        {
            let vrefs = collect_column_id_refs(&value_expr);
            if vrefs.contains(&m.inner_agg.output_column_id) || !vrefs.is_disjoint(&inner_ids) {
                return Ok(RewriteResult::Unchanged); // value expr still references a disappearing inner column
            }
        }
        let mut after_pred = m.subquery_conjunct.clone();
        replace_column_ref(&mut after_pred, a.output_column.column_id, &value_expr);
        // Guard 3: APPLY_OUT must be gone from after_pred — it has been fully replaced.
        if collect_column_id_refs(&after_pred).contains(&a.output_column.column_id) {
            return Ok(RewriteResult::Unchanged); // APPLY_OUT survived (comparison used an unhandled node)
        }
        let after = LogicalPlan::Filter(FilterNode {
            predicate: after_pred,
            input: Box::new(window),
            required_output_columns: None,
        });

        Ok(RewriteResult::Changed(after))
    }
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
    expr: &mut TypedExpr,
    inner_map: &HashMap<ColumnId, (TableIdentity, String)>,
    phys_to_outer: &HashMap<(TableIdentity, String), OutputColumn>,
) -> bool {
    match &mut expr.kind {
        ExprKind::ColumnRef {
            column_id, column, ..
        } => {
            if let Some((tab, name)) = inner_map.get(column_id) {
                // This ColumnRef is from an inner scan; find its outer twin.
                match phys_to_outer.get(&(tab.clone(), name.clone())) {
                    Some(outer_col) => {
                        *column_id = outer_col.column_id;
                        *column = outer_col.name.clone();
                        expr.data_type = outer_col.data_type.clone();
                        expr.nullable = outer_col.nullable;
                        true
                    }
                    None => false, // inner column has no outer counterpart
                }
            } else {
                // Not an inner-scan column; leave unchanged.
                true
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            // Clone to avoid simultaneous mutable borrows via reborrow trick.
            let l_ok = remap_inner_to_outer(left, inner_map, phys_to_outer);
            let r_ok = remap_inner_to_outer(right, inner_map, phys_to_outer);
            l_ok && r_ok
        }
        ExprKind::FunctionCall { args, .. } => {
            let mut ok = true;
            for arg in args.iter_mut() {
                if !remap_inner_to_outer(arg, inner_map, phys_to_outer) {
                    ok = false;
                }
            }
            ok
        }
        ExprKind::IsNull {
            expr: inner_expr, ..
        } => remap_inner_to_outer(inner_expr, inner_map, phys_to_outer),
        ExprKind::UnaryOp {
            expr: inner_expr, ..
        } => remap_inner_to_outer(inner_expr, inner_map, phys_to_outer),
        ExprKind::Cast {
            expr: inner_expr, ..
        } => remap_inner_to_outer(inner_expr, inner_map, phys_to_outer),
        ExprKind::InList {
            expr: inner_expr,
            list,
            ..
        } => {
            let e_ok = remap_inner_to_outer(inner_expr, inner_map, phys_to_outer);
            let l_ok = list
                .iter_mut()
                .all(|item| remap_inner_to_outer(item, inner_map, phys_to_outer));
            e_ok && l_ok
        }
        ExprKind::Between {
            expr: inner_expr,
            low,
            high,
            ..
        } => {
            let e_ok = remap_inner_to_outer(inner_expr, inner_map, phys_to_outer);
            let lo_ok = remap_inner_to_outer(low, inner_map, phys_to_outer);
            let hi_ok = remap_inner_to_outer(high, inner_map, phys_to_outer);
            e_ok && lo_ok && hi_ok
        }
        ExprKind::Like {
            expr: inner_expr,
            pattern,
            ..
        } => {
            let e_ok = remap_inner_to_outer(inner_expr, inner_map, phys_to_outer);
            let p_ok = remap_inner_to_outer(pattern, inner_map, phys_to_outer);
            e_ok && p_ok
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut ok = true;
            if let Some(op) = operand {
                ok &= remap_inner_to_outer(op, inner_map, phys_to_outer);
            }
            for (when, then) in when_then.iter_mut() {
                ok &= remap_inner_to_outer(when, inner_map, phys_to_outer);
                ok &= remap_inner_to_outer(then, inner_map, phys_to_outer);
            }
            if let Some(els) = else_expr {
                ok &= remap_inner_to_outer(els, inner_map, phys_to_outer);
            }
            ok
        }
        ExprKind::IsTruthValue {
            expr: inner_expr, ..
        } => remap_inner_to_outer(inner_expr, inner_map, phys_to_outer),
        ExprKind::Nested(inner_expr) => remap_inner_to_outer(inner_expr, inner_map, phys_to_outer),
        ExprKind::AggregateCall { args, order_by, .. } => {
            let mut ok = true;
            for arg in args.iter_mut() {
                ok &= remap_inner_to_outer(arg, inner_map, phys_to_outer);
            }
            for ob in order_by.iter_mut() {
                ok &= remap_inner_to_outer(&mut ob.expr, inner_map, phys_to_outer);
            }
            ok
        }
        ExprKind::LambdaFunction { body, .. } => {
            remap_inner_to_outer(body, inner_map, phys_to_outer)
        }
        ExprKind::Lambda { body, .. } => remap_inner_to_outer(body, inner_map, phys_to_outer),
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            let mut ok = true;
            for arg in args.iter_mut() {
                ok &= remap_inner_to_outer(arg, inner_map, phys_to_outer);
            }
            for pb in partition_by.iter_mut() {
                ok &= remap_inner_to_outer(pb, inner_map, phys_to_outer);
            }
            for ob in order_by.iter_mut() {
                ok &= remap_inner_to_outer(&mut ob.expr, inner_map, phys_to_outer);
            }
            ok
        }
        // True leaf variants: no child TypedExprs to recurse into.
        // ColumnRef is handled above; LambdaParamRef, Literal, SubqueryPlaceholder
        // carry no child expressions.
        ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => true,
    }
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
    apply_right: &LogicalPlan,
    inner_output_col_id: ColumnId,
    agg_out_col_id: ColumnId,
    win_id: ColumnId,
    win_type: &DataType,
) -> Result<TypedExpr, String> {
    let win_ref = col_ref_expr(win_id, win_type);
    // Peel exactly one optional leading Project (single-leading-Project assumption:
    // PushDownApplyAggFilter inserts at most one Project above the Aggregate).
    if let LogicalPlan::Project(proj) = apply_right {
        // Look for the Project item whose output id matches inner_output_col_id.
        for item in &proj.items {
            if item.output_column_id == inner_output_col_id {
                let mut value_expr = item.expr.clone();
                // Replace the aggregate output column reference with win_id.
                replace_column_ref(&mut value_expr, agg_out_col_id, &win_ref);
                return Ok(value_expr);
            }
        }
        // No matching item found in the Project (unusual shape) → safe fallback.
    }
    // No leading Project OR no matching item: inner_output_col_id IS the aggregate
    // output column (bare agg case, e.g. q2 min/max with no arithmetic).
    Ok(win_ref)
}

/// Build a nullable `ColumnRef` TypedExpr for `id` with the given type.
fn col_ref_expr(id: ColumnId, dt: &DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: id,
            qualifier: None,
            column: format!("col_{}", id.0),
        },
        data_type: dt.clone(),
        nullable: true,
    }
}

/// Recursively replace every `ColumnRef { column_id == target }` in `expr` with
/// `replacement.clone()`.
fn replace_column_ref(expr: &mut TypedExpr, target: ColumnId, replacement: &TypedExpr) {
    // Base case: this node IS the target column ref — replace in place and return.
    if let ExprKind::ColumnRef { column_id, .. } = &expr.kind
        && *column_id == target
    {
        *expr = replacement.clone();
        return;
    }
    // Recurse into all compound variants.
    match &mut expr.kind {
        ExprKind::BinaryOp { left, right, .. } => {
            replace_column_ref(left, target, replacement);
            replace_column_ref(right, target, replacement);
        }
        ExprKind::FunctionCall { args, .. } => {
            for arg in args.iter_mut() {
                replace_column_ref(arg, target, replacement);
            }
        }
        ExprKind::IsNull {
            expr: inner_expr, ..
        } => {
            replace_column_ref(inner_expr, target, replacement);
        }
        ExprKind::UnaryOp {
            expr: inner_expr, ..
        } => {
            replace_column_ref(inner_expr, target, replacement);
        }
        ExprKind::Cast {
            expr: inner_expr, ..
        } => {
            replace_column_ref(inner_expr, target, replacement);
        }
        ExprKind::InList {
            expr: inner_expr,
            list,
            ..
        } => {
            replace_column_ref(inner_expr, target, replacement);
            for item in list.iter_mut() {
                replace_column_ref(item, target, replacement);
            }
        }
        ExprKind::Between {
            expr: inner_expr,
            low,
            high,
            ..
        } => {
            replace_column_ref(inner_expr, target, replacement);
            replace_column_ref(low, target, replacement);
            replace_column_ref(high, target, replacement);
        }
        ExprKind::Like {
            expr: inner_expr,
            pattern,
            ..
        } => {
            replace_column_ref(inner_expr, target, replacement);
            replace_column_ref(pattern, target, replacement);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                replace_column_ref(op, target, replacement);
            }
            for (when, then) in when_then.iter_mut() {
                replace_column_ref(when, target, replacement);
                replace_column_ref(then, target, replacement);
            }
            if let Some(els) = else_expr {
                replace_column_ref(els, target, replacement);
            }
        }
        ExprKind::IsTruthValue {
            expr: inner_expr, ..
        } => {
            replace_column_ref(inner_expr, target, replacement);
        }
        ExprKind::Nested(inner_expr) => {
            replace_column_ref(inner_expr, target, replacement);
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for arg in args.iter_mut() {
                replace_column_ref(arg, target, replacement);
            }
            for ob in order_by.iter_mut() {
                replace_column_ref(&mut ob.expr, target, replacement);
            }
        }
        ExprKind::LambdaFunction { body, .. } => {
            replace_column_ref(body, target, replacement);
        }
        ExprKind::Lambda { body, .. } => {
            replace_column_ref(body, target, replacement);
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args.iter_mut() {
                replace_column_ref(arg, target, replacement);
            }
            for pb in partition_by.iter_mut() {
                replace_column_ref(pb, target, replacement);
            }
            for ob in order_by.iter_mut() {
                replace_column_ref(&mut ob.expr, target, replacement);
            }
        }
        // True leaf variants: ColumnRef already handled as base case above;
        // LambdaParamRef, Literal, SubqueryPlaceholder carry no child exprs.
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

/// Structural equality used ONLY to identify the exact `subquery_conjunct` within
/// the `outer_conjuncts` set. Both expressions come from the same `where_pred`
/// via `split_and`, so debug-format equality is exact and correct here.
fn expr_struct_eq(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a.kind) == format!("{:?}", b.kind)
}

/// Port of StarRocks ScalarApply2AnalyticRule's check() family. Returns the
/// validated match data, or None if any precondition fails (-> caller Unchanged).
pub(super) fn check_preconditions(where_pred: &TypedExpr, a: &ApplyNode) -> Option<WinMagicMatch> {
    // (0) Inner: peel optional leading Project, require a vector Aggregate with a
    // single non-DISTINCT whitelisted aggregate.
    let agg = peel_to_aggregate(&a.right)?;
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

    // (1) No LIMIT and only whitelisted operators in either subtree.
    if !operator_whitelist_ok(&a.left, false) {
        return None;
    }
    if !operator_whitelist_ok(&a.right, true) {
        return None;
    }

    // (2) Table-set identity: outerTables == subqueryTables + exactly 1 extra;
    // no duplicate physical table on either side (rejects self-joins).
    let outer_tabs = collect_table_ids(&a.left);
    let sub_tabs = collect_table_ids(&a.right);
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
    let col_map = collect_scan_column_map(&a.left);
    let mut partition_by = Vec::new();
    for conj in &a.correlation_conjuncts {
        let (outer_side, _inner) = super::decorrelate_util::orient_eq(conj, &corr_ids)?;
        let ExprKind::ColumnRef { column_id, .. } = &outer_side.kind else {
            return None;
        };
        match col_map.get(column_id) {
            Some((tab, _)) if *tab == correlated_outer_table => {}
            _ => return None,
        }
        partition_by.push(outer_side.clone());
    }

    // (4) Predicate identity (StarRocks checkPredicate, 4 steps). Use a phys map
    // spanning BOTH subtrees so inner/outer instances unify.
    let full_map = {
        let mut m = collect_scan_column_map(&a.left);
        m.extend(collect_scan_column_map(&a.right));
        m
    };
    let mut outer_conjuncts = split_and(where_pred.clone());

    // 4a. Each correlation conjunct must have a phys-identical twin among outer conjuncts.
    let mut unmatched_corr = a.correlation_conjuncts.clone();
    unmatched_corr.retain(|cc| {
        if let Some(pos) = outer_conjuncts
            .iter()
            .position(|oc| expr_phys_eq(cc, oc, &full_map))
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
        .position(|oc| collect_column_id_refs(oc).contains(&apply_out))?;
    let subquery_conjunct = outer_conjuncts.remove(sub_pos);
    if outer_conjuncts
        .iter()
        .any(|oc| collect_column_id_refs(oc).contains(&apply_out))
    {
        return None;
    }

    // 4c. Drop outer conjuncts that reference ONLY `correlated_outer_table`.
    outer_conjuncts.retain(|oc| {
        let refs = collect_column_id_refs(oc);
        let only_extra = !refs.is_empty()
            && refs
                .iter()
                .all(|id| matches!(col_map.get(id), Some((t, _)) if *t == correlated_outer_table));
        !only_extra
    });

    // 4d. Remaining outer conjuncts must 1:1 phys-match the subquery's residual Filter conjuncts.
    let mut sub_residual = subquery_residual_conjuncts(&a.right);
    if outer_conjuncts.len() != sub_residual.len() {
        return None;
    }
    for oc in &outer_conjuncts {
        match sub_residual
            .iter()
            .position(|sc| expr_phys_eq(oc, sc, &full_map))
        {
            Some(pos) => {
                sub_residual.remove(pos);
            }
            None => return None,
        }
    }

    Some(WinMagicMatch {
        outer_conjuncts: split_and(where_pred.clone()),
        subquery_conjunct,
        partition_by,
        inner_agg,
    })
}

/// Peel optional leading Project and return the underlying AggregateNode, if any.
fn peel_to_aggregate(plan: &LogicalPlan) -> Option<&AggregateNode> {
    match plan {
        LogicalPlan::Aggregate(agg) => Some(agg),
        LogicalPlan::Project(p) => peel_to_aggregate(&p.input),
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
fn operator_whitelist_ok(plan: &LogicalPlan, is_subquery: bool) -> bool {
    match plan {
        LogicalPlan::Scan(_) => true,
        LogicalPlan::Filter(f) => operator_whitelist_ok(&f.input, is_subquery),
        LogicalPlan::Project(p) => operator_whitelist_ok(&p.input, is_subquery),
        LogicalPlan::Join(j) => {
            if j.join_type != crate::sql::analysis::JoinKind::Cross {
                return false;
            }
            operator_whitelist_ok(&j.left, is_subquery)
                && operator_whitelist_ok(&j.right, is_subquery)
        }
        LogicalPlan::Aggregate(agg) if is_subquery => {
            operator_whitelist_ok(&agg.input, is_subquery)
        }
        _ => false,
    }
}

/// Collect the residual (non-correlation) Filter conjuncts from the subquery's
/// aggregate input, if a Filter is present.
fn subquery_residual_conjuncts(apply_right: &LogicalPlan) -> Vec<TypedExpr> {
    // Peel optional leading Project, then the Aggregate.
    let agg = match apply_right {
        LogicalPlan::Aggregate(a) => a,
        LogicalPlan::Project(p) => match p.input.as_ref() {
            LogicalPlan::Aggregate(a) => a,
            _ => return vec![],
        },
        _ => return vec![],
    };
    // If the aggregate's input is a Filter, split its predicate into conjuncts.
    match agg.input.as_ref() {
        LogicalPlan::Filter(f) => split_and(f.predicate.clone()),
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
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, ApplyKind, ApplyNode, FilterNode, JoinNode, LimitNode,
        LogicalPlan, ProjectNode, ScanNode,
    };

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
    fn make_outer_lineitem_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
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
            required_output_columns: None,
        })
    }

    /// Build `Scan(part, table_id=2)`.
    fn make_part_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
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
            required_output_columns: None,
        })
    }

    /// Build inner `Scan(lineitem, table_id=1)` — second instance with INNER_ ColumnIds.
    fn make_inner_lineitem_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
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
            required_output_columns: None,
        })
    }

    /// Build outer left plan: `CrossJoin(lineitem_scan, part_scan)`.
    fn make_outer_join() -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(make_outer_lineitem_scan()),
            right: Box::new(make_part_scan()),
            join_type: JoinKind::Cross,
            condition: None,
            required_output_columns: None,
        })
    }

    /// Build inner aggregate: `Agg{group_by:[inner_l_partkey], avg(l_quantity)}(inner_scan)`.
    /// This is the post-PushDownApplyAggFilter shape.
    fn make_inner_avg_agg() -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(make_inner_lineitem_scan()),
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
            required_output_columns: None,
        })
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
    fn winmagic_filter_apply() -> LogicalPlan {
        // Correlation conjunct: part.p_partkey == inner.l_partkey
        let corr_conj = eq_expr(
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
            col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
        );

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_outer_join()),
            right: Box::new(make_inner_avg_agg()),
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
            required_output_columns: None,
        });

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

        LogicalPlan::Filter(FilterNode {
            input: Box::new(apply),
            predicate: pred,
            required_output_columns: None,
        })
    }

    /// Variant of the base fixture but with a leading Project above the agg:
    /// `Project[inner_output_col_id := 2.0 * AVG_RESULT](Agg)`.
    /// Here `inner_output_column_id` is VAL_ID (100), not AVG_RESULT.
    fn winmagic_filter_apply_with_project() -> (LogicalPlan, ColumnId) {
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
        let projected_right = LogicalPlan::Project(ProjectNode {
            input: Box::new(make_inner_avg_agg()),
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
            required_output_columns: None,
        });
        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_outer_join()),
            right: Box::new(projected_right),
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
            required_output_columns: None,
        });
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
            LogicalPlan::Filter(FilterNode {
                input: Box::new(apply),
                predicate: pred,
                required_output_columns: None,
            }),
            VAL_ID,
        )
    }

    fn ctx() -> RewriteContext {
        RewriteContext::for_query(Vec::<String>::new())
    }

    fn ctx_with_factory() -> RewriteContext {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_column_ref_factory(Rc::new(RefCell::new(ColumnRefFactory::new())));
        ctx
    }

    // ---- matches() tests --------------------------------------------------------

    #[test]
    fn matches_returns_true_for_q17_shape() {
        let rule = ApplyToWindow;
        let plan = winmagic_filter_apply();
        assert!(rule.matches(&plan, &ctx()));
    }

    #[test]
    fn matches_returns_false_for_bare_apply() {
        let rule = ApplyToWindow;
        // Apply without Filter wrapper → should not match
        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_outer_join()),
            right: Box::new(make_inner_avg_agg()),
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
            required_output_columns: None,
        });
        assert!(!rule.matches(&apply, &ctx()));
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

        let result = rule.apply(plan, &mut ctx).expect("apply must not error");
        let RewriteResult::Changed(result) = result else {
            panic!("expected Changed, got Unchanged");
        };

        // No Apply anywhere.
        assert!(
            super::super::find_residual_apply(&result).is_none(),
            "Apply must be gone after transform"
        );

        // after-window Filter at the top.
        let LogicalPlan::Filter(after_filter) = &result else {
            panic!("expected after-window Filter at root, got: {:?}", result);
        };

        // Window inside.
        let LogicalPlan::Window(window_node) = after_filter.input.as_ref() else {
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
        let LogicalPlan::Sort(sort_node) = window_node.input.as_ref() else {
            panic!("expected Sort under Window");
        };
        assert_eq!(sort_node.items.len(), 1);
        assert_eq!(sort_node.analytic_partition_by.len(), 1);

        // before-window Filter under Sort.
        let LogicalPlan::Filter(before_filter) = sort_node.input.as_ref() else {
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
            matches!(before_filter.input.as_ref(), LogicalPlan::Join(_)),
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
            let result = rule.apply(plan, &mut ctx).unwrap();
            let RewriteResult::Changed(result) = result else {
                panic!("expected Changed")
            };
            let LogicalPlan::Filter(after) = &result else {
                panic!("expected Filter")
            };
            let LogicalPlan::Window(win) = after.input.as_ref() else {
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
            let result = rule.apply(plan, &mut ctx).unwrap();
            let RewriteResult::Changed(result) = result else {
                panic!("expected Changed")
            };
            let LogicalPlan::Filter(after) = &result else {
                panic!("expected Filter")
            };
            let LogicalPlan::Window(win) = after.input.as_ref() else {
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
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        a.left = Box::new(LogicalPlan::Join(JoinNode {
            left: Box::new(make_outer_lineitem_scan()),
            right: Box::new(make_outer_lineitem_scan()),
            join_type: JoinKind::Cross,
            condition: None,
            required_output_columns: None,
        }));
        let bad_plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(LogicalPlan::Apply(a)),
            predicate: pred.clone(),
            required_output_columns: None,
        });

        let mut ctx = ctx_with_factory();
        let result = rule.apply(bad_plan, &mut ctx).expect("must not error");
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "self-join fixture must produce Unchanged"
        );
    }

    // ---- precondition tests -----------------------------------------------------

    /// Helper: extract the ApplyNode and predicate from the canonical Filter(Apply) fixture.
    fn extract_filter_apply(plan: &LogicalPlan) -> (&TypedExpr, &ApplyNode) {
        let LogicalPlan::Filter(f) = plan else {
            panic!("expected Filter")
        };
        let LogicalPlan::Apply(a) = f.input.as_ref() else {
            panic!("expected Apply")
        };
        (&f.predicate, a)
    }

    #[test]
    fn precond_accepts_q17_shape() {
        let plan = winmagic_filter_apply();
        let (pred, a) = extract_filter_apply(&plan);
        assert!(
            check_preconditions(pred, a).is_some(),
            "q17-shaped Filter(Apply) must pass all preconditions"
        );
    }

    #[test]
    fn precond_rejects_non_whitelist_agg() {
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        // Replace avg with array_agg (not in whitelist)
        let mut a = a_orig.clone();
        let bad_agg = AggregateNode {
            aggregates: vec![AggregateCall {
                name: "array_agg".to_string(),
                ..a_orig.right.as_ref().as_aggregate().unwrap().aggregates[0].clone()
            }],
            ..a_orig.right.as_ref().as_aggregate().unwrap().clone()
        };
        a.right = Box::new(LogicalPlan::Aggregate(bad_agg));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "non-whitelist agg must reject"
        );
    }

    #[test]
    fn precond_rejects_distinct_agg() {
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        let bad_agg = AggregateNode {
            aggregates: vec![AggregateCall {
                distinct: true,
                ..a_orig.right.as_ref().as_aggregate().unwrap().aggregates[0].clone()
            }],
            ..a_orig.right.as_ref().as_aggregate().unwrap().clone()
        };
        a.right = Box::new(LogicalPlan::Aggregate(bad_agg));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "distinct agg must reject"
        );
    }

    #[test]
    fn precond_rejects_two_aggregates() {
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        let orig_agg = a_orig.right.as_ref().as_aggregate().unwrap();
        let two_agg = AggregateNode {
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
        a.right = Box::new(LogicalPlan::Aggregate(two_agg));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "two aggregates must reject"
        );
    }

    #[test]
    fn precond_rejects_self_join_outer() {
        // Outer: CrossJoin(lineitem(table_id=1), lineitem(table_id=1)) — two same-table scans
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        // Replace part scan with another lineitem scan (same table_id=1)
        let dup_lineitem = make_outer_lineitem_scan();
        a.left = Box::new(LogicalPlan::Join(JoinNode {
            left: Box::new(make_outer_lineitem_scan()),
            right: Box::new(dup_lineitem),
            join_type: JoinKind::Cross,
            condition: None,
            required_output_columns: None,
        }));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "self-join (duplicate table) on outer must reject"
        );
    }

    #[test]
    fn precond_rejects_table_set_mismatch() {
        // Subquery scans a table_id=99 absent from outer (outer has table_id=1 and 2)
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        // Replace inner scan with one from table_id=99
        let foreign_scan = LogicalPlan::Scan(ScanNode {
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
            required_output_columns: None,
        });
        let orig_agg = a_orig.right.as_ref().as_aggregate().unwrap();
        let foreign_agg = AggregateNode {
            input: Box::new(foreign_scan),
            ..orig_agg.clone()
        };
        a.right = Box::new(LogicalPlan::Aggregate(foreign_agg));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "subquery with foreign table must reject"
        );
    }

    #[test]
    fn precond_rejects_limit_in_subtree() {
        // Wrap outer left in a Limit node (not whitelisted)
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
        a.left = Box::new(LogicalPlan::Limit(LimitNode {
            input: a_orig.left.clone(),
            limit: Some(10),
            offset: None,
            required_output_columns: None,
        }));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "Limit in outer subtree must reject"
        );
    }

    #[test]
    fn precond_rejects_predicate_mismatch() {
        // Add a residual Filter inside the subquery aggregate's input that
        // has no twin in the outer WHERE predicate.
        let plan = winmagic_filter_apply();
        let (pred, a_orig) = extract_filter_apply(&plan);
        let mut a = a_orig.clone();
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
        let inner_with_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(make_inner_lineitem_scan()),
            predicate: residual_pred,
            required_output_columns: None,
        });
        let orig_agg = a_orig.right.as_ref().as_aggregate().unwrap();
        let agg_with_residual = AggregateNode {
            input: Box::new(inner_with_filter),
            ..orig_agg.clone()
        };
        a.right = Box::new(LogicalPlan::Aggregate(agg_with_residual));
        assert!(
            check_preconditions(pred, &a).is_none(),
            "residual Filter conjunct without outer twin must reject"
        );
    }

    #[test]
    fn precond_rejects_no_subquery_conjunct() {
        // WHERE predicate doesn't reference APPLY_OUT at all
        let plan = winmagic_filter_apply();
        let (_, a) = extract_filter_apply(&plan);
        // Build a predicate that has no APPLY_OUT reference
        let pred_without_apply = eq_expr(
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
            col_ref(L_PARTKEY, "l_partkey", DataType::Int64),
        );
        assert!(
            check_preconditions(&pred_without_apply, a).is_none(),
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
        let inner_with_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(make_inner_lineitem_scan()),
            predicate: inner_residual,
            required_output_columns: None,
        });
        let agg_with_residual = AggregateNode {
            input: Box::new(inner_with_filter),
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
            required_output_columns: None,
        };
        let apply = ApplyNode {
            left: Box::new(make_outer_join()),
            right: Box::new(LogicalPlan::Aggregate(agg_with_residual)),
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
            required_output_columns: None,
        };
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
            check_preconditions(&pred, &apply).is_none(),
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
        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_outer_join()),
            right: Box::new(make_inner_avg_agg()),
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
            required_output_columns: None,
        });

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
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(apply),
            predicate: pred,
            required_output_columns: None,
        });

        let rule = ApplyToWindow;
        let mut ctx = ctx_with_factory();
        let result = rule.apply(plan, &mut ctx).expect("apply must not error");
        let RewriteResult::Changed(result) = result else {
            panic!("expected Changed — Between-wrapped APPLY_OUT should now be handled");
        };

        // Structural assertions.
        assert!(
            super::super::find_residual_apply(&result).is_none(),
            "Apply must be gone after transform"
        );
        let LogicalPlan::Filter(after_filter) = &result else {
            panic!("expected after-window Filter at root");
        };
        assert!(
            matches!(after_filter.input.as_ref(), LogicalPlan::Window(_)),
            "expected Window under after-filter"
        );

        // Key correctness check: APPLY_OUT is gone from the after-window predicate.
        let refs = collect_column_id_refs(&after_filter.predicate);
        assert!(
            !refs.contains(&APPLY_OUT),
            "after-window predicate must NOT reference APPLY_OUT after Between rewrite"
        );
        // win_id is present instead.
        let LogicalPlan::Window(win) = after_filter.input.as_ref() else {
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
    fn winmagic_pre_pushdown_filter_apply() -> LogicalPlan {
        // Correlation predicate inside the inner filter: inner.l_partkey == outer.p_partkey
        let inner_corr_pred = eq_expr(
            col_ref(INNER_L_PARTKEY, "l_partkey", DataType::Int64),
            col_ref(P_PARTKEY, "p_partkey", DataType::Int64),
        );
        // Inner: Agg{group_by:[]}(Filter(corr_pred)(inner_scan))
        let inner_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(make_inner_lineitem_scan()),
            predicate: inner_corr_pred,
            required_output_columns: None,
        });
        let inner_agg = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(inner_filter),
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
            required_output_columns: None,
        });

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_outer_join()),
            right: Box::new(inner_agg),
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
            need_check_max_rows: true, // pre-pushdown flag
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

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

        LogicalPlan::Filter(FilterNode {
            input: Box::new(apply),
            predicate: pred,
            required_output_columns: None,
        })
    }

    /// Recursively find any Window node in the plan tree.
    ///
    /// Test-only walker. Covers only the plan shapes the SubqueryRewrite stage can
    /// emit (Filter/Sort/Project/Aggregate/Join/Limit inputs). The Apply case is
    /// intentionally omitted: every caller asserts `find_residual_apply(..).is_none()`
    /// first, so no Apply is present when this runs. Extend if a future rule emits a
    /// Window under a variant not traversed here.
    fn find_window(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Window(_) => true,
            LogicalPlan::Filter(n) => find_window(&n.input),
            LogicalPlan::Sort(n) => find_window(&n.input),
            LogicalPlan::Project(n) => find_window(&n.input),
            LogicalPlan::Aggregate(n) => find_window(&n.input),
            LogicalPlan::Join(n) => find_window(&n.left) || find_window(&n.right),
            LogicalPlan::Limit(n) => find_window(&n.input),
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
    fn find_left_outer_join(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join(n) if n.join_type == JoinKind::LeftOuter => true,
            LogicalPlan::Join(n) => find_left_outer_join(&n.left) || find_left_outer_join(&n.right),
            LogicalPlan::Filter(n) => find_left_outer_join(&n.input),
            LogicalPlan::Sort(n) => find_left_outer_join(&n.input),
            LogicalPlan::Project(n) => find_left_outer_join(&n.input),
            LogicalPlan::Aggregate(n) => find_left_outer_join(&n.input),
            LogicalPlan::Limit(n) => find_left_outer_join(&n.input),
            LogicalPlan::Window(n) => find_left_outer_join(&n.input),
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

        // Run only the SubqueryRewrite stage so the fixture doesn't need full column wiring.
        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "SubqueryRewrite",
            RewritePhase::StructuralRewrite,
            subquery_rewrite_rules(),
        )]);
        let result = pipeline
            .rewrite(plan, &mut ctx)
            .expect("SubqueryRewrite stage must succeed for decorrelatable Apply");

        // No Apply must survive.
        assert!(
            super::super::find_residual_apply(&result).is_none(),
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

        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "SubqueryRewrite",
            RewritePhase::StructuralRewrite,
            subquery_rewrite_rules(),
        )]);
        let result = pipeline
            .rewrite(plan, &mut ctx)
            .expect("SubqueryRewrite stage must succeed when ApplyToWindow is disabled");

        // No Apply must survive.
        assert!(
            super::super::find_residual_apply(&result).is_none(),
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

    // Helper trait to make test code more readable — extracts AggregateNode from plan.
    trait AsAggregate {
        fn as_aggregate(&self) -> Option<&AggregateNode>;
    }

    impl AsAggregate for LogicalPlan {
        fn as_aggregate(&self) -> Option<&AggregateNode> {
            match self {
                LogicalPlan::Aggregate(a) => Some(a),
                _ => None,
            }
        }
    }
}
