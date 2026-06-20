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

use crate::sql::analysis::BinOp;
use crate::sql::optimizer::operator::{Operator, ScanOp};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

use super::context::{DictionaryRewriteContext, ScanColumnKey};
use super::expr::{
    DICT_AGG_FUNCTIONS, collect_all_columns, collect_nested_columns, is_string_like,
};

pub(crate) fn collect(
    expr: &OptExpr,
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
    {
        let arena_rc = rewrite_ctx.scalar_arena();
        let arena = arena_rc.borrow();
        collect_blocklist(expr, &arena, &mut blocklist);
    }
    rewrite_ctx.extend_dict_rewrite_blocklist(blocklist);
    let blocklist = rewrite_ctx.dict_rewrite_blocklist().clone();

    // Re-borrow the provider after the mutable blocklist update above.
    let provider = rewrite_ctx
        .dictionary_provider()
        .expect("provider presence checked above")
        .clone();
    walk(expr, provider.as_ref(), &blocklist, &mut dict_ctx)?;
    Ok(dict_ctx)
}

/// Walk the whole plan and collect the names of columns consumed in
/// positions the rewriter does not retarget to the dict slot. Mirrors the
/// structure of `walk`; per-node behavior matches the rewriter's
/// retargeting capability.
fn collect_blocklist(expr: &OptExpr, arena: &ScalarArena, out: &mut BTreeSet<String>) {
    match &expr.op {
        Operator::LogicalScan(_) => {}
        Operator::LogicalFilter(node) => {
            // A filter predicate is never retargeted — even a bare
            // `s = 'x'` evaluates against the string value.
            collect_all_columns(arena, node.predicate, out);
            collect_blocklist(expr.unary_input(), arena, out);
        }
        Operator::LogicalProject(node) => {
            // Bare ColumnRef items merely propagate the dict slot (safe);
            // any compound item consumes the string.
            for item in &node.items {
                collect_nested_columns(arena, item.expr, out);
            }
            collect_blocklist(expr.unary_input(), arena, out);
        }
        Operator::LogicalAggregate(node) => {
            // Bare ColumnRef group keys are safe (rewritten to the dict
            // slot + post-aggregate Decode); compound group keys consume
            // the string.
            for group_expr in &node.group_by {
                collect_nested_columns(arena, *group_expr, out);
            }
            for agg in &node.aggregates {
                let lower = agg.name.to_ascii_lowercase();
                if DICT_AGG_FUNCTIONS.iter().any(|f| *f == lower) {
                    // Allowlisted aggregate: a bare ColumnRef arg may
                    // consume the dict id directly (safe). Only compound
                    // args / order_by exprs consume the string.
                    for arg in &agg.args {
                        collect_nested_columns(arena, *arg, out);
                    }
                    for item in &agg.order_by {
                        collect_nested_columns(arena, item.expr, out);
                    }
                } else {
                    // Non-allowlisted aggregate (e.g. min/max/sum/...):
                    // the string is consumed regardless of arg shape.
                    for arg in &agg.args {
                        collect_all_columns(arena, *arg, out);
                    }
                    for item in &agg.order_by {
                        collect_all_columns(arena, item.expr, out);
                    }
                }
            }
            collect_blocklist(expr.unary_input(), arena, out);
        }
        Operator::LogicalSort(node) => {
            // Bare ColumnRef sort keys are safe — the rewriter decodes
            // non-order-preserving ones itself. Compound keys consume the
            // string.
            for item in &node.items {
                collect_nested_columns(arena, item.expr, out);
            }
            collect_blocklist(expr.unary_input(), arena, out);
        }
        Operator::LogicalTopN(node) => {
            for item in &node.items {
                collect_nested_columns(arena, item.expr, out);
            }
            collect_blocklist(expr.unary_input(), arena, out);
        }
        Operator::LogicalJoin(node) => {
            if let Some(cond) = node.condition {
                // Spare equi-join keys the rewriter would dict-join;
                // collect every other column the condition consumes.
                collect_join_condition(arena, cond, out);
            }
            for child in &expr.children {
                collect_blocklist(child, arena, out);
            }
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
        _ => {
            for child in &expr.children {
                collect_blocklist(child, arena, out);
            }
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
fn collect_join_condition(arena: &ScalarArena, expr: ScalarId, out: &mut BTreeSet<String>) {
    match arena.node(expr) {
        ScalarNode::BinaryOp { left, op, right } if matches!(op, BinOp::And) => {
            collect_join_condition(arena, *left, out);
            collect_join_condition(arena, *right, out);
        }
        ScalarNode::BinaryOp { left, op, right } if matches!(op, BinOp::Eq | BinOp::EqForNull) => {
            let both_bare = matches!(arena.node(*left), ScalarNode::ColumnRef(_))
                && matches!(arena.node(*right), ScalarNode::ColumnRef(_));
            if both_bare {
                // Safe equi key — the rewriter dict-joins it. Contribute
                // nothing. (Whether the two sides' snapshots are actually
                // compatible is decided by the rewriter; if not, it
                // decodes the columns itself, which is still correct.)
            } else {
                collect_all_columns(arena, expr, out);
            }
        }
        ScalarNode::Nested(inner) => collect_join_condition(arena, *inner, out),
        _ => collect_all_columns(arena, expr, out),
    }
}

fn walk(
    expr: &OptExpr,
    provider: &dyn crate::sql::optimizer::rewrite::context::QueryDictionaryProvider,
    blocklist: &BTreeSet<String>,
    dict_ctx: &mut DictionaryRewriteContext,
) -> Result<(), String> {
    match &expr.op {
        Operator::LogicalScan(scan) => {
            visit_scan(scan, provider, blocklist, dict_ctx)?;
        }
        Operator::LogicalJoin(_) => {
            // TODO(task-8): joins with matching dict snapshots on both
            // sides could keep dict ids through the equi-join; today
            // the rewriter inserts a Decode boundary instead.
            for child in &expr.children {
                walk(child, provider, blocklist, dict_ctx)?;
            }
        }
        Operator::LogicalUnion(_) => {
            // TODO(task-8): UNION ALL with matching dicts on every leg
            // can propagate dict columns upward; for Task 7 we treat
            // every set op as a decode boundary.
            for child in &expr.children {
                walk(child, provider, blocklist, dict_ctx)?;
            }
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
        _ => {
            for child in &expr.children {
                walk(child, provider, blocklist, dict_ctx)?;
            }
        }
    }
    Ok(())
}

fn visit_scan(
    scan: &ScanOp,
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
