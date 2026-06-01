//! Collector pass for `LowCardinalityDictionaryRewrite`.
//!
//! Walks the logical plan once, top-to-bottom, and asks the
//! `QueryDictionaryProvider` for an active dictionary snapshot on every
//! string-typed column produced by a `Scan`. Eligible mappings get
//! registered into the rule-local `DictionaryRewriteContext` which the
//! subsequent rewriter pass consumes.
//!
//! The collector deliberately does not look at non-scan plumbing
//! (Aggregate / Sort / Project / Join etc.): the rewriter consults the
//! same `DictionaryRewriteContext` and applies node-specific behavior
//! there. That keeps the collector focused on snapshot discovery.

use std::collections::BTreeSet;

use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::planner::plan::{LogicalPlan, ScanNode};

use super::context::{DictionaryRewriteContext, ScanColumnKey};
use super::expr::{
    DICT_AGG_FUNCTIONS, collect_all_columns, collect_nested_columns, is_string_like,
};

pub(crate) fn collect(
    plan: &LogicalPlan,
    rewrite_ctx: &mut RewriteContext,
) -> Result<DictionaryRewriteContext, String> {
    let mut dict_ctx = DictionaryRewriteContext::default();
    if rewrite_ctx.dictionary_provider().is_none() {
        return Ok(dict_ctx);
    }
    // Compute the blocklist for THIS subtree: the set of (lowercased)
    // column names consumed in a position the rewriter does NOT retarget
    // to the dict slot (e.g. a filter predicate, a non-allowlisted
    // aggregate, a function call). Encoding such a column would silently
    // feed Int32 dict ids into an operator expecting the string — a
    // wrong-result bug. Columns used only in safe positions (group-by /
    // DISTINCT / allowlisted agg / equi-join keys / bare project items /
    // decoded boundaries) are NOT blocked.
    //
    // Merge into the persistent per-query blocklist. The `TopDown` driver
    // runs the rule on the FULL plan first (where the unsafe consumer is
    // visible), then re-fires on every subtree down to the bare `Scan`
    // (where it is not). Persisting the union across invocations is what
    // keeps a blocklisted column blocked on the bare-scan re-fire.
    let mut blocklist: BTreeSet<String> = BTreeSet::new();
    collect_blocklist(plan, &mut blocklist);
    rewrite_ctx.extend_dict_rewrite_blocklist(blocklist);
    let blocklist = rewrite_ctx.dict_rewrite_blocklist().clone();

    // Re-borrow the provider after the mutable blocklist update above.
    let provider = rewrite_ctx
        .dictionary_provider()
        .expect("provider presence checked above")
        .clone();
    walk(plan, provider.as_ref(), &blocklist, &mut dict_ctx)?;
    Ok(dict_ctx)
}

/// Walk the whole plan and collect the names of columns consumed in
/// positions the rewriter does not retarget to the dict slot. Mirrors the
/// structure of `walk`; per-node behavior matches the rewriter's
/// retargeting capability.
fn collect_blocklist(plan: &LogicalPlan, out: &mut BTreeSet<String>) {
    match plan {
        LogicalPlan::Scan(_) => {}
        LogicalPlan::Filter(node) => {
            // A filter predicate is never retargeted — even a bare
            // `s = 'x'` evaluates against the string value.
            collect_all_columns(&node.predicate, out);
            collect_blocklist(&node.input, out);
        }
        LogicalPlan::Project(node) => {
            // Bare ColumnRef items merely propagate the dict slot (safe);
            // any compound item consumes the string.
            for item in &node.items {
                collect_nested_columns(&item.expr, out);
            }
            collect_blocklist(&node.input, out);
        }
        LogicalPlan::Aggregate(node) => {
            // Bare ColumnRef group keys are safe (rewritten to the dict
            // slot + post-aggregate Decode); compound group keys consume
            // the string.
            for expr in &node.group_by {
                collect_nested_columns(expr, out);
            }
            for agg in &node.aggregates {
                let lower = agg.name.to_ascii_lowercase();
                if DICT_AGG_FUNCTIONS.iter().any(|f| *f == lower) {
                    // Allowlisted aggregate: a bare ColumnRef arg may
                    // consume the dict id directly (safe). Only compound
                    // args / order_by exprs consume the string.
                    for arg in &agg.args {
                        collect_nested_columns(arg, out);
                    }
                    for item in &agg.order_by {
                        collect_nested_columns(&item.expr, out);
                    }
                } else {
                    // Non-allowlisted aggregate (e.g. min/max/sum/...):
                    // the string is consumed regardless of arg shape.
                    for arg in &agg.args {
                        collect_all_columns(arg, out);
                    }
                    for item in &agg.order_by {
                        collect_all_columns(&item.expr, out);
                    }
                }
            }
            collect_blocklist(&node.input, out);
        }
        LogicalPlan::Sort(node) => {
            // Bare ColumnRef sort keys are safe — the rewriter decodes
            // non-order-preserving ones itself. Compound keys consume the
            // string.
            for item in &node.items {
                collect_nested_columns(&item.expr, out);
            }
            collect_blocklist(&node.input, out);
        }
        LogicalPlan::Limit(node) => collect_blocklist(&node.input, out),
        LogicalPlan::Join(node) => {
            if let Some(cond) = node.condition.as_ref() {
                // Spare equi-join keys the rewriter would dict-join;
                // collect every other column the condition consumes.
                collect_join_condition(cond, out);
            }
            collect_blocklist(&node.left, out);
            collect_blocklist(&node.right, out);
        }
        LogicalPlan::Window(node) => collect_blocklist(&node.input, out),
        LogicalPlan::TableFunction(node) => collect_blocklist(&node.input, out),
        LogicalPlan::Repeat(node) => collect_blocklist(&node.input, out),
        LogicalPlan::CTEProduce(node) => collect_blocklist(&node.input, out),
        LogicalPlan::Decode(node) => collect_blocklist(&node.input, out),
        LogicalPlan::CTEAnchor(node) => {
            collect_blocklist(&node.produce, out);
            collect_blocklist(&node.consumer, out);
        }
        LogicalPlan::Union(node) => {
            for input in &node.inputs {
                collect_blocklist(input, out);
            }
        }
        LogicalPlan::Intersect(node) => {
            for input in &node.inputs {
                collect_blocklist(input, out);
            }
        }
        LogicalPlan::Except(node) => {
            for input in &node.inputs {
                collect_blocklist(input, out);
            }
        }
        LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) | LogicalPlan::CTEConsume(_) => {}
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

/// Collect columns consumed by a join condition, sparing the equi-join
/// keys the rewriter (`rewrite_join` / `collect_equi_pairs`) would
/// dict-join. Mirrors that classification:
///
/// * `And` → recurse into both sides with the same equi-aware logic.
/// * `Eq` / `EqForNull` with BOTH operands a bare `ColumnRef` → contribute
///   nothing (the rewriter compares these on the dict id slot).
/// * `Nested(inner)` → unwrap and recurse (matches the rewriter, which
///   unwraps `Nested` when collecting equi pairs).
/// * anything else → `collect_all_columns` (decoded by the join, consumes
///   the string).
fn collect_join_condition(expr: &TypedExpr, out: &mut BTreeSet<String>) {
    match &expr.kind {
        ExprKind::BinaryOp { left, op, right } if matches!(op, BinOp::And) => {
            collect_join_condition(left, out);
            collect_join_condition(right, out);
        }
        ExprKind::BinaryOp { left, op, right } if matches!(op, BinOp::Eq | BinOp::EqForNull) => {
            let both_bare = matches!(left.kind, ExprKind::ColumnRef { .. })
                && matches!(right.kind, ExprKind::ColumnRef { .. });
            if both_bare {
                // Safe equi key — the rewriter dict-joins it. Contribute
                // nothing. (Whether the two sides' snapshots are actually
                // compatible is decided by the rewriter; if not, it
                // decodes the columns itself, which is still correct.)
            } else {
                collect_all_columns(expr, out);
            }
        }
        ExprKind::Nested(inner) => collect_join_condition(inner, out),
        _ => collect_all_columns(expr, out),
    }
}

fn walk(
    plan: &LogicalPlan,
    provider: &dyn crate::sql::optimizer::rewrite::context::QueryDictionaryProvider,
    blocklist: &BTreeSet<String>,
    dict_ctx: &mut DictionaryRewriteContext,
) -> Result<(), String> {
    match plan {
        LogicalPlan::Scan(scan) => {
            visit_scan(scan, provider, blocklist, dict_ctx)?;
        }
        LogicalPlan::Filter(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::Project(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::Aggregate(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::Sort(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::Limit(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::Window(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::TableFunction(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::Repeat(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::CTEProduce(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::Decode(node) => walk(&node.input, provider, blocklist, dict_ctx)?,
        LogicalPlan::Join(node) => {
            // TODO(task-8): joins with matching dict snapshots on both
            // sides could keep dict ids through the equi-join; today
            // the rewriter inserts a Decode boundary instead.
            walk(&node.left, provider, blocklist, dict_ctx)?;
            walk(&node.right, provider, blocklist, dict_ctx)?;
        }
        LogicalPlan::CTEAnchor(node) => {
            walk(&node.produce, provider, blocklist, dict_ctx)?;
            walk(&node.consumer, provider, blocklist, dict_ctx)?;
        }
        LogicalPlan::Union(node) => {
            // TODO(task-8): UNION ALL with matching dicts on every leg
            // can propagate dict columns upward; for Task 7 we treat
            // every set op as a decode boundary.
            for input in &node.inputs {
                walk(input, provider, blocklist, dict_ctx)?;
            }
        }
        LogicalPlan::Intersect(node) => {
            for input in &node.inputs {
                walk(input, provider, blocklist, dict_ctx)?;
            }
        }
        LogicalPlan::Except(node) => {
            for input in &node.inputs {
                walk(input, provider, blocklist, dict_ctx)?;
            }
        }
        LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) | LogicalPlan::CTEConsume(_) => {}
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
    Ok(())
}

fn visit_scan(
    scan: &ScanNode,
    provider: &dyn crate::sql::optimizer::rewrite::context::QueryDictionaryProvider,
    blocklist: &BTreeSet<String>,
    dict_ctx: &mut DictionaryRewriteContext,
) -> Result<(), String> {
    for col in &scan.columns {
        if !is_string_like(&col.data_type) {
            continue;
        }
        // Respect pruning: if the scan has been pruned and this column
        // is not in the required set, skip it.
        if let Some(required) = &scan.required_columns {
            let lower = col.name.to_ascii_lowercase();
            if !required.iter().any(|r| r.to_ascii_lowercase() == lower) {
                continue;
            }
        }
        // Blocklist: this column is consumed somewhere in the plan in a
        // position the rewriter does not retarget to the dict slot.
        // Skip dict-encoding it so the scan keeps emitting the plain
        // string; nothing downstream needs a Decode and correctness is
        // preserved.
        if blocklist.contains(&col.name.to_ascii_lowercase()) {
            continue;
        }
        let snapshot = provider.load_active_snapshot(&scan.table, &scan.database, &col.name)?;
        if let Some(snapshot) = snapshot {
            let key = ScanColumnKey::new(&scan.database, &scan.table.name, &col.name);
            dict_ctx.register_scan_column(key, snapshot);
        }
    }
    Ok(())
}
