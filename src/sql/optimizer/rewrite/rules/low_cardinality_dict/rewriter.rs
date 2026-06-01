//! Rewrite pass for `LowCardinalityDictionaryRewrite`.
//!
//! Walks the plan top-down. Per-node behavior:
//!
//! * `Scan`: attach `ScanDictionaryColumn` hints + an extra hidden
//!   `OutputColumn` so codegen materializes the dict-id slot. The
//!   string column itself is kept on the scan output — callers that
//!   only need the dict id read it from the dict slot, and any
//!   downstream `Decode` consumes the dict slot back to the original
//!   string slot.
//! * `Aggregate`: rewrite group-by string column refs to point at the
//!   dict slot; if the aggregate exposes a string group-by column
//!   upward, insert a `Decode` above the aggregate so consumers still
//!   see the string value. Aggregate function arguments are routed
//!   through `DICT_AGG_FUNCTIONS`: `count` and `approx_count_distinct`
//!   may consume the dict id directly (their result type — BIGINT — is
//!   independent of the input encoding); anything else keeps its
//!   string-column argument, which the scan still emits alongside the
//!   dict slot. `min` / `max` / `any_value` / `array_agg` are
//!   intentionally NOT rewritten today — they would emit Int32 dict ids
//!   under a still-Utf8 result column. See the TODO(task-8-min-max) note
//!   on `DICT_AGG_FUNCTIONS` in `expr.rs`.
//! * `Sort` / `TopN-via-Sort`: when a sort key has an order-preserving
//!   snapshot, rewrite the key to the dict slot; otherwise insert a
//!   `Decode` between the sort and its input so the sort still sees
//!   strings.
//! * `Project`: passthrough at Task 7 scope, but plain column-alias
//!   items propagate the dict binding under the new name so a
//!   downstream Join / boundary still finds it.
//! * `Limit`: passthrough.
//! * `Join` (Task 8 item 1): hash-join equality predicates over two
//!   dict-encoded sides whose snapshots are `dict_keys_compatible`
//!   keep dict columns on BOTH sides and compare on the dict id slot.
//!   Otherwise each side is wrapped in a Decode below the join.
//!   Non-equality conjuncts (e.g. `<`, `>`, `LIKE`, function calls) are
//!   not rewritten, and any column they reference is decoded below the
//!   join so the predicate sees strings. This applies uniformly to
//!   `Inner` and `Cross` joins today; outer / semi / anti join variants
//!   are pinned by tests and follow the same rewrite (the join itself
//!   generates the NULL on unmatched outer rows — the equi-key
//!   comparison is unchanged regardless of `JoinKind`).
//! * `Union` (Task 8 item 2): UNION ALL preserves dict columns only
//!   when *every* input exposes the same `dict_keys_compatible`
//!   snapshot for that output column; otherwise every input is
//!   decoded before the union. UNION DISTINCT / INTERSECT / EXCEPT
//!   always decode (set-distinct semantics hash on the user-facing
//!   string value, so dict ids would diverge across snapshots).
//! * `Window` / `TableFunction` / `Repeat` /
//!   `CTEAnchor` / `CTEProduce`: conservative decode boundary — every
//!   dict column flowing through is decoded back to its string before
//!   the node. For multi-consumer CTEs, see the `TODO(task-8-cte)`
//!   marker on `CTEAnchor`.
//!
//! The rewriter is idempotent: a `Scan` whose `dict_columns` is
//! already populated is skipped on a second pass.
//!
//! Per-subtree dict visibility lives in `DictScope`, returned alongside
//! the rewritten plan. The rule-global `DictionaryRewriteContext` does
//! NOT carry an output-name -> dict-column map: that map collides when
//! two scans share a column name. See `context.rs`.

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::{
    AggregateCall, AggregateNode, DecodeMapping, DecodeNode, FilterNode, JoinNode, LimitNode,
    LogicalPlan, ProjectNode, ScanDictionaryColumn, ScanNode, SortNode, UnionNode,
};

use super::context::{DictBinding, DictScope, DictionaryRewriteContext};
use super::expr::{DICT_AGG_FUNCTIONS, dict_keys_compatible, rewrite_column_ref_with_scope};

pub(crate) fn rewrite(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    let (plan, _scope) = rewrite_node(plan, ctx)?;
    Ok(plan)
}

fn rewrite_node(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let (scan, scope) = rewrite_scan(scan, ctx);
            Ok((LogicalPlan::Scan(scan), scope))
        }
        LogicalPlan::Filter(node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            Ok((
                LogicalPlan::Filter(FilterNode {
                    input: Box::new(input),
                    predicate: node.predicate,
                    required_output_columns: node.required_output_columns,
                }),
                scope,
            ))
        }
        LogicalPlan::Project(node) => rewrite_project(node, ctx),
        LogicalPlan::Aggregate(node) => rewrite_aggregate(node, ctx),
        LogicalPlan::Sort(node) => rewrite_sort(node, ctx),
        LogicalPlan::Limit(node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            Ok((
                LogicalPlan::Limit(LimitNode {
                    input: Box::new(input),
                    limit: node.limit,
                    offset: node.offset,
                    required_output_columns: node.required_output_columns,
                }),
                scope,
            ))
        }
        LogicalPlan::Join(node) => rewrite_join(node, ctx),
        LogicalPlan::Union(node) => rewrite_union(node, ctx),
        // UNION DISTINCT / INTERSECT / EXCEPT semantics require hashing
        // on the user-facing string value — dict ids from different
        // snapshots cannot be compared directly. Always decode here.
        LogicalPlan::Intersect(_)
        | LogicalPlan::Except(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::TableFunction(_)
        | LogicalPlan::Repeat(_)
        // TODO(post-Task-9): multi-consumer CTEs with matching dict
        // snapshots across every consumer could keep the dict column
        // on the producer output. Doing so requires a fix-up pass over
        // every consumer of a `CTEProduce` (the current top-down
        // rewrite cannot see all consumers while rewriting the
        // producer). Task 8 keeps the conservative behaviour (decode
        // at the producer / consumer boundary) and pins it in
        // `cte_anchor_always_decodes_at_boundary`. Single-use CTEs are
        // already inlined before this rule runs, so the observable
        // surface here is narrow. Deferred until a Task 9+ query case
        // demands it.
        | LogicalPlan::CTEAnchor(_)
        | LogicalPlan::CTEProduce(_) => decode_boundary(plan, ctx),
        // Leaves that produce no dict columns of their own.
        LogicalPlan::CTEConsume(_) | LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) => {
            Ok((plan, DictScope::new()))
        }
        // Decode is the rewrite's own output; do not recurse into it
        // again. The decoded output is all strings — no dict scope.
        LogicalPlan::Decode(_) => Ok((plan, DictScope::new())),

        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

fn rewrite_scan(mut scan: ScanNode, ctx: &mut DictionaryRewriteContext) -> (ScanNode, DictScope) {
    let mut scope = DictScope::new();
    // Idempotency guard: an already-populated `dict_columns` means a
    // previous application of this rule already rewrote the scan.
    // Rebuild the scope from the existing hints so callers above still
    // see the bindings.
    if !scan.dict_columns.is_empty() {
        for hint in &scan.dict_columns {
            scope.insert(
                hint.source_column.clone(),
                DictBinding {
                    dict_column: hint.dict_column.clone(),
                    snapshot: hint.dictionary.clone(),
                },
            );
        }
        return (scan, scope);
    }
    let eligible = ctx.dict_eligible_columns_for_scan(&scan.database, &scan.table.name);
    if eligible.is_empty() {
        return (scan, scope);
    }
    for (col_name, snapshot) in eligible {
        // Locate the source column descriptor to preserve nullability and
        // its column_id so the rewritten OutputColumn keeps a stable id
        // for downstream resolution.
        let (source_name, nullable, source_column_id) = match scan
            .columns
            .iter()
            .find(|c| c.name.to_ascii_lowercase() == col_name.to_ascii_lowercase())
        {
            Some(c) => (c.name.clone(), c.nullable, c.column_id),
            None => continue,
        };
        let dict_column = DictionaryRewriteContext::dict_column_name(&scan.table.name, &col_name);
        // Bug B fix: REPLACE the source string OutputColumn with the dict
        // INT slot rather than adding a sibling. The codegen `visit_scan`
        // walks `scan.columns` (filtered by `required_columns`) to decide
        // which slots end up in the scan tuple; if we kept the original
        // string column here, the BE would receive BOTH the source string
        // slot AND the dict slot in the lake-scan layout and the
        // `dict_int_to_string` rewrite at `src/lower/node/lake_scan.rs`
        // would collapse them onto the same storage slot id (duplicate
        // slot id in chunk schema contract). Every downstream reference
        // to the source column is already retargeted to the dict column
        // by `rewrite_project` / `rewrite_aggregate` / `rewrite_sort` /
        // `rewrite_join`; the final user-facing string materialization
        // happens at a `Decode` boundary inserted by those arms (or by
        // `wrap_with_decode` for conservative parents).
        for col in scan.columns.iter_mut() {
            if col.name.eq_ignore_ascii_case(&source_name) {
                col.name = dict_column.clone();
                col.data_type = DataType::Int32;
                col.nullable = nullable;
                col.column_id = source_column_id;
            }
        }
        if let Some(required) = scan.required_columns.as_mut() {
            for entry in required.iter_mut() {
                if entry.eq_ignore_ascii_case(&source_name) {
                    *entry = dict_column.clone();
                }
            }
        }
        scan.dict_columns.push(ScanDictionaryColumn {
            source_column: source_name.clone(),
            dict_column: dict_column.clone(),
            dictionary: snapshot.clone(),
        });
        scope.insert(
            source_name,
            DictBinding {
                dict_column,
                snapshot,
            },
        );
        ctx.mark_changed();
    }
    (scan, scope)
}

fn rewrite_project(
    node: ProjectNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    let (input, input_scope) = rewrite_node(*node.input, ctx)?;
    // Task 7 scope: do not rewrite arbitrary project item expressions
    // (derived dict expressions like `upper(s)` are TODO(task-8)). But
    // plain ColumnRef items MUST be retargeted to the dict slot now —
    // after the Bug B fix the scan publishes only `__nr_dict_<t>_<c>`,
    // not the source string column, so a ColumnRef("s") in the project
    // body cannot resolve at codegen. The output_name stays the same;
    // only the expression's inner column ref + data type change.
    //
    // For plain column-alias items (`SELECT s AS t FROM ...`), propagate
    // the dict binding under the alias name so a downstream boundary
    // can still find the dict column to decode.
    let mut output_scope = DictScope::new();
    let mut items: Vec<crate::sql::analysis::ProjectItem> = Vec::with_capacity(node.items.len());
    for item in node.items.into_iter() {
        if let ExprKind::ColumnRef { column, .. } = &item.expr.kind
            && let Some(binding) = input_scope.get(column)
        {
            output_scope.insert(item.output_name.clone(), binding.clone());
            // Idempotency: don't double-rewrite when the column is
            // already the dict slot (post-iteration-1 of the pipeline's
            // fixed-point loop).
            let already_dict = column.eq_ignore_ascii_case(&binding.dict_column);
            let rewritten = if already_dict {
                item.expr.clone()
            } else {
                rewrite_column_ref_with_scope(&item.expr, &input_scope)
            };
            items.push(crate::sql::analysis::ProjectItem {
                expr: rewritten,
                output_name: item.output_name,
                output_column_id: item.output_column_id,
            });
            continue;
        }
        items.push(item);
    }
    // Bug A fix: ColumnPruning leaves residual Project nodes between Scan
    // and Aggregate/Sort/etc. Without propagating the hidden dict slot
    // through those Projects, the codegen ExprScope built from project
    // items does not contain `__nr_dict_<table>_<col>`, so downstream
    // operators that the dict rewriter has retargeted to the dict slot
    // (e.g. an Aggregate whose group-by was rewritten to `__nr_dict_t_s`)
    // fail to resolve the column. Append a sibling pass-through
    // ProjectItem for every dict binding visible at the input whose dict
    // column name is not already produced by an existing item. The
    // user-facing root projection does not name `__nr_dict_*` columns in
    // its items, so the extra slot is hidden from query results. Parents
    // that decide to decode at the boundary (Join/Union/CTE →
    // wrap_with_decode) see the dict slot in plan_output_columns and
    // pair it with the source binding via the alias-aware DictScope
    // lookup; `wrap_with_decode` dedupes the dict_column so a Project
    // that already exposes the source name does not produce two
    // DecodeMappings pointing at the same slot.
    let existing_names: std::collections::BTreeSet<String> = items
        .iter()
        .map(|i| i.output_name.to_ascii_lowercase())
        .collect();
    // Look up the dict slot's nullability from the input plan's output
    // columns so the inserted pass-through item matches the scan's
    // declared shape. Defaults to `true` if not found (no production
    // path hits the fallback today, but it stays defensive).
    let input_cols = plan_output_columns(&input);
    for (_source, binding) in input_scope.iter() {
        let dict_name = binding.dict_column.clone();
        if existing_names.contains(&dict_name.to_ascii_lowercase()) {
            continue;
        }
        let nullable = input_cols
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&dict_name))
            .map(|c| c.nullable)
            .unwrap_or(true);
        items.push(crate::sql::analysis::ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::UNSET,
                    qualifier: None,
                    column: dict_name.clone(),
                },
                data_type: DataType::Int32,
                nullable,
            },
            output_name: dict_name,
            // Synthetic dict-slot pass-through; not addressed by the pruning pass.
            output_column_id: ColumnId::UNSET,
        });
    }
    Ok((
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items,
            required_output_columns: node.required_output_columns,
        }),
        output_scope,
    ))
}

fn rewrite_aggregate(
    node: AggregateNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    let (input, input_scope) = rewrite_node(*node.input, ctx)?;
    let mut group_by = Vec::with_capacity(node.group_by.len());
    let mut decoded_group_keys: Vec<(
        String,
        String,
        std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot>,
    )> = Vec::new();
    for expr in &node.group_by {
        if let crate::sql::analysis::ExprKind::ColumnRef { column, .. } = &expr.kind
            && let Some(binding) = input_scope.get(column)
        {
            group_by.push(rewrite_column_ref_with_scope(expr, &input_scope));
            // Skip the decode-wrap bookkeeping when this group key is
            // ALREADY pointing at the dict slot (a previous iteration of
            // this rule under the pipeline's fixed-point loop already
            // rewrote it). The scope's dict-name binding lets the
            // expression rewriter resolve the column either way, but
            // here we must distinguish so we don't double-wrap with
            // Decode every iteration.
            if column.eq_ignore_ascii_case(&binding.dict_column) {
                continue;
            }
            // The aggregate node was emitting the original string
            // column name to consumers; we must surface that name
            // through a Decode boundary above the aggregate.
            decoded_group_keys.push((
                binding.dict_column.clone(),
                column.clone(),
                binding.snapshot.clone(),
            ));
            continue;
        }
        group_by.push(expr.clone());
    }

    // Output columns: dict-encoded group-by columns are renamed to the
    // dict slot for the immediate aggregate scope; the decode above
    // restores the original string name for callers. Output columns
    // that ALREADY carry the dict-column name (post-iteration-1) pass
    // through unchanged — they are the dict slot already.
    let mut output_columns: Vec<OutputColumn> = node
        .output_columns
        .iter()
        .map(|out| {
            if let Some(binding) = input_scope.get(&out.name) {
                if out.name.eq_ignore_ascii_case(&binding.dict_column) {
                    return out.clone();
                }
                OutputColumn {
                    column_id: out.column_id,
                    name: binding.dict_column.clone(),
                    data_type: DataType::Int32,
                    nullable: out.nullable,
                    is_internal: false,
                }
            } else {
                out.clone()
            }
        })
        .collect();
    // Task 8 item 5: rewrite individual aggregate-call arguments to the
    // dict slot when the call is on the `DICT_AGG_FUNCTIONS` allowlist
    // (and additionally requires `order_preserving` for `min` / `max`).
    // Other calls keep their string-column argument — the scan still
    // exposes the original string column alongside the dict slot, so no
    // extra Decode is needed for these argument paths. Group-by keys
    // are still handled above and are responsible for the post-aggregate
    // Decode boundary that restores the user-facing string name.
    let aggregates = node
        .aggregates
        .into_iter()
        .map(|agg| rewrite_aggregate_call(agg, &input_scope, ctx))
        .collect::<Vec<_>>();

    let aggregate = LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(input),
        group_by,
        aggregates,
        output_columns: output_columns.clone(),
        already_pushed: node.already_pushed,
        required_output_columns: node.required_output_columns,
    });
    if decoded_group_keys.is_empty() {
        // Aggregate did not consume any dict columns from its input;
        // the aggregate's own output is all strings, so nothing
        // dict-typed is exposed upward.
        return Ok((aggregate, DictScope::new()));
    }

    // Build dict_column -> (string_column, snapshot) so we can restore
    // names and types on the decode's output_columns.
    let mut decoded_index: std::collections::BTreeMap<
        String,
        (
            String,
            std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot>,
        ),
    > = std::collections::BTreeMap::new();
    for (dict, string, snap) in &decoded_group_keys {
        decoded_index.insert(dict.clone(), (string.clone(), snap.clone()));
    }
    let mappings: Vec<DecodeMapping> = decoded_group_keys
        .iter()
        .map(|(dict, string, _)| DecodeMapping {
            dict_column: dict.clone(),
            string_column: string.clone(),
        })
        .collect();
    // Restore the original string-column names on the post-decode
    // output_columns so consumers continue to bind to the string.
    for out in output_columns.iter_mut() {
        if let Some((original, snap)) = decoded_index.get(&out.name) {
            out.name = original.clone();
            out.data_type = snap.data_type.clone();
        }
    }
    ctx.mark_changed();
    // Decode is a terminator for dict bindings — its output is all
    // strings, so the returned scope is empty.
    Ok((
        LogicalPlan::Decode(DecodeNode {
            input: Box::new(aggregate),
            mappings,
            output_columns,
            required_output_columns: None,
        }),
        DictScope::new(),
    ))
}

fn rewrite_sort(
    node: SortNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    let (input, input_scope) = rewrite_node(*node.input, ctx)?;
    // Determine whether all sort keys with dict snapshots are
    // order-preserving. Otherwise insert a Decode before the sort so
    // the sort still operates on strings.
    let mut needs_decode = false;
    let mut sort_items = Vec::with_capacity(node.items.len());
    for item in &node.items {
        if let crate::sql::analysis::ExprKind::ColumnRef { column, .. } = &item.expr.kind
            && let Some(binding) = input_scope.get(column)
        {
            if binding.snapshot.order_preserving {
                let already_dict = column.eq_ignore_ascii_case(&binding.dict_column);
                let mut rewritten = item.clone();
                rewritten.expr = rewrite_column_ref_with_scope(&item.expr, &input_scope);
                sort_items.push(rewritten);
                if !already_dict {
                    ctx.mark_changed();
                }
                continue;
            } else {
                needs_decode = true;
            }
        }
        sort_items.push(item.clone());
    }
    let (input, output_scope) = if needs_decode {
        // Decode below the sort: the sort itself now sees strings and
        // surfaces strings; no dict columns leak upward.
        (wrap_with_decode(input, &input_scope, ctx), DictScope::new())
    } else {
        (input, input_scope)
    };
    Ok((
        LogicalPlan::Sort(SortNode {
            input: Box::new(input),
            items: sort_items,
            analytic_partition_by: node.analytic_partition_by,
            required_output_columns: node.required_output_columns,
        }),
        output_scope,
    ))
}

/// Per-aggregate-call argument rewrite (Task 8 item 5).
///
/// * `count(*)` — unchanged (no args to rewrite).
/// * `count(col)` / `count(DISTINCT col)` — may consume dict id; arg
///   rewritten to the dict slot. `DISTINCT` is safe because dict ids
///   are 1:1 with source strings.
/// * `approx_count_distinct(col)` — same as `count(DISTINCT col)`; the
///   distinct count over dict ids equals the distinct count over
///   strings.
/// * Aggregates outside the allowlist — unchanged. Their string-column
///   argument remains a string ref; the scan still emits the original
///   string column alongside the dict slot, so no decode is required
///   solely for the agg-arg path. (Group-by keys are handled separately
///   in `rewrite_aggregate` and ARE wrapped in a post-aggregate decode.)
///
/// `agg.distinct` is not inspected here. After narrowing the allowlist
/// to `count` and `approx_count_distinct` (see `DICT_AGG_FUNCTIONS`),
/// the only rewrites possible are over those two functions, and
/// `DISTINCT` is safe for both.
fn rewrite_aggregate_call(
    mut agg: AggregateCall,
    input_scope: &DictScope,
    ctx: &mut DictionaryRewriteContext,
) -> AggregateCall {
    let lower = agg.name.to_ascii_lowercase();
    if !DICT_AGG_FUNCTIONS.iter().any(|f| *f == lower) {
        return agg;
    }
    // count(*) has no args; leave it alone.
    if lower == "count" && agg.args.is_empty() {
        return agg;
    }

    // Neither `count` nor `approx_count_distinct` accepts an ORDER BY
    // clause in SQL — but the AST node carries the slot anyway. Keep a
    // defensive check: if the agg ever carries an ORDER BY over a
    // non-order-preserving dict column, fall back to the string arg.
    let order_by_dict_compatible = agg.order_by.iter().all(|item| {
        match &item.expr.kind {
            ExprKind::ColumnRef { column, .. } => match input_scope.get(column) {
                Some(binding) => binding.snapshot.order_preserving,
                // No dict binding on this order_by column — irrelevant,
                // it stays as-is and does not block the agg-arg rewrite.
                None => true,
            },
            // Non-column-ref ORDER BY expressions cannot be reasoned
            // about cheaply; play it safe and skip the dict-id rewrite.
            _ => false,
        }
    });

    let mut rewrote_any_arg = false;
    let mut new_args = Vec::with_capacity(agg.args.len());
    for arg in agg.args.drain(..) {
        if let ExprKind::ColumnRef { column, .. } = &arg.kind
            && let Some(binding) = input_scope.get(column)
            && order_by_dict_compatible
        {
            // Idempotency: an already-dict-rewritten arg (post first
            // pipeline iteration) keeps the same ColumnRef without
            // marking the rewrite changed; otherwise the pipeline's
            // fixed-point loop would never terminate.
            let already_dict = column.eq_ignore_ascii_case(&binding.dict_column);
            new_args.push(rewrite_column_ref_with_scope(&arg, input_scope));
            if !already_dict {
                rewrote_any_arg = true;
            }
            continue;
        }
        new_args.push(arg);
    }
    agg.args = new_args;
    if rewrote_any_arg {
        ctx.mark_changed();
    }
    agg
}

/// Equi-join two dict-encoded sides directly when their snapshots are
/// `dict_keys_compatible` (Task 8 item 1). Otherwise wrap each side in a
/// Decode below the join.
///
/// Non-equality conditions always decode both sides — comparing dict ids
/// for `<`, `>`, etc. is only meaningful when the encoding is order-
/// preserving, and even then mixing dict ids and strings inside arbitrary
/// expressions is more fragile than the win is worth. Cross joins (no
/// condition) skip the rewrite and rely on each side's own scope; columns
/// that cross unchanged are still surfaceable as dict ids by downstream
/// boundaries via the returned scope.
fn rewrite_join(
    node: JoinNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    let (left, left_scope) = rewrite_node(*node.left, ctx)?;
    let (right, right_scope) = rewrite_node(*node.right, ctx)?;

    // No condition → CROSS JOIN. There is no opportunity to compare on
    // dict ids; keep the conservative boundary by decoding both sides.
    let Some(condition) = node.condition.as_ref() else {
        let left = wrap_with_decode(left, &left_scope, ctx);
        let right = wrap_with_decode(right, &right_scope, ctx);
        return Ok((
            LogicalPlan::Join(JoinNode {
                left: Box::new(left),
                right: Box::new(right),
                join_type: node.join_type,
                condition: node.condition,
                required_output_columns: node.required_output_columns,
            }),
            DictScope::new(),
        ));
    };

    // Collect the equality pairs that align two dict-compatible columns.
    // For each such pair we keep both sides' dict columns and rewrite the
    // ColumnRef nodes inside the condition. Pairs that don't align
    // (different snapshots, only one side has a dict binding, or non-
    // equality predicates) fall through to per-side Decode.
    let aligned = aligned_dict_join_pairs(condition, &left_scope, &right_scope);

    // Build the sets of output column names we are KEEPING dict-encoded
    // on each input side. Everything else gets decoded below the join.
    //
    // The keep set must cover BOTH the source string column name and
    // the synthesized dict column name. Otherwise `wrap_with_decode_except`
    // would decode the string column under the join even when the join
    // operates on dict ids — leaving a meaningless decoded copy of the
    // string column trailing along beside the dict slot it pairs with.
    let mut keep_left: std::collections::BTreeSet<String> = Default::default();
    let mut keep_right: std::collections::BTreeSet<String> = Default::default();
    for pair in &aligned {
        keep_left.insert(pair.left_column.to_ascii_lowercase());
        // Future-proofing: the dict column name never appears in
        // `plan_output_columns` lookup keys today (the scan publishes it
        // alongside the source column, and `wrap_with_decode_except`
        // skips entries whose source name is in `keep`), so this insert
        // is dead in the current shape. Kept so that future plans which
        // already carry rewritten `ColumnRef`s in non-equality
        // conjuncts — pointing at the dict column directly — still
        // route through the keep set without an unintended decode.
        if let Some(b) = left_scope.get(&pair.left_column) {
            keep_left.insert(b.dict_column.to_ascii_lowercase());
        }
        keep_right.insert(pair.right_column.to_ascii_lowercase());
        // See the note on `keep_left` above — same reasoning.
        if let Some(b) = right_scope.get(&pair.right_column) {
            keep_right.insert(b.dict_column.to_ascii_lowercase());
        }
    }

    let left = wrap_with_decode_except(left, &left_scope, &keep_left, ctx);
    let right = wrap_with_decode_except(right, &right_scope, &keep_right, ctx);

    // Rewrite the join condition: equality predicates that match an
    // aligned pair are rewritten so each side references its own dict
    // slot directly. Non-aligned predicates flow through unchanged (the
    // affected columns were decoded above, so they read strings).
    //
    // Idempotency: when a previous rule iteration already rewrote the
    // condition to dict columns, `aligned` still finds pairs (because
    // `pair_dict_columns` resolves by either name), but the rewrite is
    // a no-op. Only call `ctx.mark_changed()` when the condition's
    // ColumnRefs are not yet on the dict slot.
    let condition = if aligned.is_empty() {
        Some(condition.clone())
    } else {
        if condition_references_source_names(condition, &left_scope, &right_scope) {
            ctx.mark_changed();
        }
        Some(rewrite_join_condition_pairs(
            condition,
            &aligned,
            &left_scope,
            &right_scope,
        ))
    };

    // Output scope: the equi-keys we kept stay dict-encoded and are
    // surfaced upward by name. Names that collide between the two sides
    // (a common case for `t1.name = t2.name`) currently default to the
    // left side's binding — downstream consumers that disambiguate by
    // qualifier are out of scope for Task 8.
    let mut out_scope = DictScope::new();
    for name in keep_right.iter() {
        if let Some(b) = right_scope.get(name) {
            out_scope.insert(name.clone(), b.clone());
        }
    }
    for name in keep_left.iter() {
        if let Some(b) = left_scope.get(name) {
            out_scope.insert(name.clone(), b.clone());
        }
    }

    Ok((
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: node.join_type,
            condition,
            required_output_columns: node.required_output_columns,
        }),
        out_scope,
    ))
}

/// An equality predicate that compares a dict-encoded column on each
/// side of the join under matching snapshots. `left_column` is the
/// (string) output-column name on the join's LEFT input; `right_column`
/// is the same on the RIGHT input. The two AST sides may appear in
/// either order in the predicate — `predicate_left_is_left_input`
/// records the orientation so `rewrite_join_condition_pairs` can swap
/// in the correct dict slot per side.
#[derive(Clone, Debug)]
struct AlignedEquiPair {
    left_column: String,
    right_column: String,
    /// `true`  ⇔ the predicate is `<left_input.col> = <right_input.col>`.
    /// `false` ⇔ the predicate is `<right_input.col> = <left_input.col>`.
    predicate_left_is_left_input: bool,
}

/// Walk the join condition collecting equi-pairs that are safe to
/// compare on dict ids. Handles top-level conjunctions of equalities;
/// any other predicate shape is ignored (those columns will decode).
///
/// `Eq` and `EqForNull` (null-safe equality) are both safe — comparing
/// dict ids inherits the same null behaviour as comparing strings,
/// because the scan emits NULL → null_id consistently per snapshot.
fn aligned_dict_join_pairs(
    cond: &TypedExpr,
    left_scope: &DictScope,
    right_scope: &DictScope,
) -> Vec<AlignedEquiPair> {
    let mut out = Vec::new();
    collect_equi_pairs(cond, left_scope, right_scope, &mut out);
    out
}

fn collect_equi_pairs(
    expr: &TypedExpr,
    left_scope: &DictScope,
    right_scope: &DictScope,
    out: &mut Vec<AlignedEquiPair>,
) {
    match &expr.kind {
        ExprKind::BinaryOp { left, op, right } if matches!(op, BinOp::And) => {
            collect_equi_pairs(left, left_scope, right_scope, out);
            collect_equi_pairs(right, left_scope, right_scope, out);
        }
        ExprKind::BinaryOp { left, op, right } if matches!(op, BinOp::Eq | BinOp::EqForNull) => {
            if let Some(pair) = pair_dict_columns(left, right, left_scope, right_scope) {
                out.push(pair);
            }
        }
        ExprKind::Nested(inner) => collect_equi_pairs(inner, left_scope, right_scope, out),
        _ => {}
    }
}

fn pair_dict_columns(
    a: &TypedExpr,
    b: &TypedExpr,
    left_scope: &DictScope,
    right_scope: &DictScope,
) -> Option<AlignedEquiPair> {
    let a_name = match &a.kind {
        ExprKind::ColumnRef { column, .. } => column.clone(),
        _ => return None,
    };
    let b_name = match &b.kind {
        ExprKind::ColumnRef { column, .. } => column.clone(),
        _ => return None,
    };
    // Resolve by EITHER the source column name (`name`) OR the dict
    // column name (`__nr_dict_t1_name`). After a prior pipeline
    // iteration the ColumnRefs in the condition already point at the
    // dict slot; `resolve_either` finds the binding under that name
    // too. The returned `key` is the binding's source-name registration
    // — used by the caller for the keep-set bookkeeping.
    let (left_key, right_key, left_binding, right_binding, lhs_is_left_input) = match (
        left_scope.resolve_either(&a_name),
        right_scope.resolve_either(&b_name),
    ) {
        (Some(l), Some(r)) => (l.0.to_string(), r.0.to_string(), l.1, r.1, true),
        _ => match (
            left_scope.resolve_either(&b_name),
            right_scope.resolve_either(&a_name),
        ) {
            (Some(l), Some(r)) => (l.0.to_string(), r.0.to_string(), l.1, r.1, false),
            _ => return None,
        },
    };
    if dict_keys_compatible(&left_binding.snapshot, &right_binding.snapshot) {
        Some(AlignedEquiPair {
            left_column: left_key,
            right_column: right_key,
            predicate_left_is_left_input: lhs_is_left_input,
        })
    } else {
        None
    }
}

/// True when the condition contains at least one ColumnRef whose name
/// resolves under either scope by source-name lookup but does NOT
/// already match the binding's `dict_column`. Used by `rewrite_join` to
/// avoid marking the rewrite as "changed" when iteration N+1 sees a
/// condition that iteration N already rewrote.
fn condition_references_source_names(
    expr: &TypedExpr,
    left_scope: &DictScope,
    right_scope: &DictScope,
) -> bool {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => {
            if let Some(b) = left_scope.get(column) {
                return !column.eq_ignore_ascii_case(&b.dict_column);
            }
            if let Some(b) = right_scope.get(column) {
                return !column.eq_ignore_ascii_case(&b.dict_column);
            }
            false
        }
        ExprKind::BinaryOp { left, right, .. } => {
            condition_references_source_names(left, left_scope, right_scope)
                || condition_references_source_names(right, left_scope, right_scope)
        }
        ExprKind::Nested(inner) => {
            condition_references_source_names(inner, left_scope, right_scope)
        }
        _ => false,
    }
}

/// Rewrite top-level conjunction equi-pairs to dict-id comparisons. Each
/// aligned pair is matched by reference identity-of-shape: a
/// `BinaryOp(Eq|EqForNull, ColumnRef, ColumnRef)` whose two column refs
/// match an aligned pair's `left_column`/`right_column` (in the recorded
/// orientation) is rewritten to compare the dict slots directly. Other
/// predicates are returned unchanged.
fn rewrite_join_condition_pairs(
    cond: &TypedExpr,
    aligned: &[AlignedEquiPair],
    left_scope: &DictScope,
    right_scope: &DictScope,
) -> TypedExpr {
    match &cond.kind {
        ExprKind::BinaryOp { left, op, right } if matches!(op, BinOp::And) => TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(rewrite_join_condition_pairs(
                    left,
                    aligned,
                    left_scope,
                    right_scope,
                )),
                op: *op,
                right: Box::new(rewrite_join_condition_pairs(
                    right,
                    aligned,
                    left_scope,
                    right_scope,
                )),
            },
            data_type: cond.data_type.clone(),
            nullable: cond.nullable,
        },
        ExprKind::Nested(inner) => TypedExpr {
            kind: ExprKind::Nested(Box::new(rewrite_join_condition_pairs(
                inner,
                aligned,
                left_scope,
                right_scope,
            ))),
            data_type: cond.data_type.clone(),
            nullable: cond.nullable,
        },
        ExprKind::BinaryOp { left, op, right } if matches!(op, BinOp::Eq | BinOp::EqForNull) => {
            if let (
                ExprKind::ColumnRef { column: a_col, .. },
                ExprKind::ColumnRef { column: b_col, .. },
            ) = (&left.kind, &right.kind)
            {
                for pair in aligned {
                    let (predicate_left, predicate_right) = if pair.predicate_left_is_left_input {
                        (&pair.left_column, &pair.right_column)
                    } else {
                        (&pair.right_column, &pair.left_column)
                    };
                    let lhs_scope = if pair.predicate_left_is_left_input {
                        left_scope
                    } else {
                        right_scope
                    };
                    let rhs_scope = if pair.predicate_left_is_left_input {
                        right_scope
                    } else {
                        left_scope
                    };
                    // Match by EITHER the source-column name or the
                    // already-rewritten dict-column name. The latter is
                    // how iteration 2 sees the condition after the rule
                    // first fired.
                    let lhs_match = a_col.eq_ignore_ascii_case(predicate_left)
                        || lhs_scope
                            .get(predicate_left)
                            .is_some_and(|b| a_col.eq_ignore_ascii_case(&b.dict_column));
                    let rhs_match = b_col.eq_ignore_ascii_case(predicate_right)
                        || rhs_scope
                            .get(predicate_right)
                            .is_some_and(|b| b_col.eq_ignore_ascii_case(&b.dict_column));
                    if lhs_match && rhs_match {
                        // Build new ColumnRef nodes that point at the
                        // dict slots. Re-using `rewrite_column_ref_with_scope`
                        // would need a lookup-by-name, but the column may
                        // already be the dict slot — easier to construct
                        // directly from the binding.
                        let lhs_binding = lhs_scope.get(predicate_left).expect("scope has binding");
                        let rhs_binding =
                            rhs_scope.get(predicate_right).expect("scope has binding");
                        let new_left = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: ColumnId::UNSET,
                                qualifier: match &left.kind {
                                    ExprKind::ColumnRef { qualifier, .. } => qualifier.clone(),
                                    _ => None,
                                },
                                column: lhs_binding.dict_column.clone(),
                            },
                            data_type: DataType::Int32,
                            nullable: left.nullable,
                        };
                        let new_right = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: ColumnId::UNSET,
                                qualifier: match &right.kind {
                                    ExprKind::ColumnRef { qualifier, .. } => qualifier.clone(),
                                    _ => None,
                                },
                                column: rhs_binding.dict_column.clone(),
                            },
                            data_type: DataType::Int32,
                            nullable: right.nullable,
                        };
                        return TypedExpr {
                            kind: ExprKind::BinaryOp {
                                left: Box::new(new_left),
                                op: *op,
                                right: Box::new(new_right),
                            },
                            data_type: cond.data_type.clone(),
                            nullable: cond.nullable,
                        };
                    }
                }
            }
            cond.clone()
        }
        _ => cond.clone(),
    }
}

/// Wrap `plan` with a `Decode` for every dict column in `scope` whose
/// name is NOT in `keep`. The kept columns continue to flow upward as
/// dict ids; everything else is decoded back to strings.
fn wrap_with_decode_except(
    plan: LogicalPlan,
    scope: &DictScope,
    keep: &std::collections::BTreeSet<String>,
    ctx: &mut DictionaryRewriteContext,
) -> LogicalPlan {
    let mut decoded_scope = DictScope::new();
    for col in plan_output_columns(&plan) {
        let key = col.name.to_ascii_lowercase();
        // Bug B aftermath: the Scan's published output column is now
        // named after the dict column directly (e.g. `__nr_dict_t_s`),
        // not the user-facing source name. The `keep` set is built from
        // source names (and the dict_column names where available), so
        // resolve via `resolve_either` and also check whether the
        // resolved SOURCE name is in `keep` — otherwise a Union ALL with
        // matching snapshots would still wrap each input in a Decode,
        // defeating the dict-preservation path.
        if let Some((source_name, b)) = scope.resolve_either(&col.name) {
            if keep.contains(&source_name.to_ascii_lowercase()) {
                continue;
            }
            decoded_scope.insert(source_name.to_string(), b.clone());
        } else if keep.contains(&key) {
            continue;
        }
    }
    if decoded_scope.is_empty() {
        plan
    } else {
        wrap_with_decode(plan, &decoded_scope, ctx)
    }
}

/// UNION ALL: preserve dict columns only when *every* input exposes a
/// `dict_keys_compatible` snapshot for that output column (Task 8 item
/// 2). UNION DISTINCT decodes (the distinct-on-string semantics make a
/// dict-id union unsafe across snapshots).
fn rewrite_union(
    mut node: UnionNode,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    // Rewrite every input subtree first so we have per-input scopes.
    let mut rewritten_inputs: Vec<(LogicalPlan, DictScope)> = Vec::with_capacity(node.inputs.len());
    for input in node.inputs.drain(..) {
        rewritten_inputs.push(rewrite_node(input, ctx)?);
    }

    // UNION DISTINCT always decodes.
    if !node.all {
        let mut new_inputs = Vec::with_capacity(rewritten_inputs.len());
        for (plan, scope) in rewritten_inputs {
            new_inputs.push(wrap_with_decode(plan, &scope, ctx));
        }
        node.inputs = new_inputs;
        return Ok((LogicalPlan::Union(node), DictScope::new()));
    }

    // UNION ALL: find columns that are dict-encoded across EVERY input
    // with mutually compatible snapshots. Start from the first input's
    // bindings; intersect with each subsequent input. DictScope doesn't
    // expose its map directly, so we probe each input's plan output
    // columns by name. After Bug B's FE rewrite, the published output
    // column is named after the dict slot (`__nr_dict_t_<col>`); use
    // `resolve_either` so the lookup also succeeds for that shape, and
    // key `preserved` by the SOURCE name so subsequent inputs are
    // matched on a stable user-facing identifier.
    let mut preserved: std::collections::BTreeMap<String, DictBinding> = Default::default();
    if let Some((first_plan, first_scope)) = rewritten_inputs.first() {
        for col in plan_output_columns(first_plan) {
            if let Some((source_name, b)) = first_scope.resolve_either(&col.name) {
                preserved.insert(source_name.to_ascii_lowercase(), b.clone());
            }
        }
    }
    for (plan, scope) in rewritten_inputs.iter().skip(1) {
        let cols = plan_output_columns(plan);
        preserved.retain(|name, kept| {
            let matching = cols.iter().find_map(|c| {
                scope
                    .resolve_either(&c.name)
                    .filter(|(src, _)| src.eq_ignore_ascii_case(name))
                    .map(|(_, b)| b)
            });
            match matching {
                Some(other) => dict_keys_compatible(&kept.snapshot, &other.snapshot),
                None => false,
            }
        });
    }

    if preserved.is_empty() {
        // At least one input lacks a matching snapshot → decode everywhere.
        let mut new_inputs = Vec::with_capacity(rewritten_inputs.len());
        for (plan, scope) in rewritten_inputs {
            new_inputs.push(wrap_with_decode(plan, &scope, ctx));
        }
        node.inputs = new_inputs;
        return Ok((LogicalPlan::Union(node), DictScope::new()));
    }

    // Preserve only the matching columns on each input; decode the rest.
    let keep_set: std::collections::BTreeSet<String> = preserved.keys().cloned().collect();
    let mut new_inputs = Vec::with_capacity(rewritten_inputs.len());
    for (plan, scope) in rewritten_inputs {
        new_inputs.push(wrap_with_decode_except(plan, &scope, &keep_set, ctx));
    }
    node.inputs = new_inputs;

    // Output scope: surface the preserved bindings upward.
    let mut out_scope = DictScope::new();
    for (name, binding) in preserved {
        out_scope.insert(name, binding);
    }
    // Preservation itself does not flip any plan bits — the scope is a
    // recursion-local effect, not a tree mutation. Avoid `mark_changed`
    // here so the pipeline's fixed-point loop terminates.
    Ok((LogicalPlan::Union(node), out_scope))
}

fn decode_boundary(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<(LogicalPlan, DictScope), String> {
    // For nodes Task 7 does not refine, recurse into their children to
    // pick up scan-side dict columns, then wrap each child with a
    // Decode so the node itself never has to know about dict ids.
    let rewritten = rewrite_children_decoded(plan, ctx)?;
    // After wrapping every child with Decode, the parent boundary's
    // own output is all strings — no scope leaks upward.
    Ok((rewritten, DictScope::new()))
}

/// Recurse into each child, then wrap that child with `Decode` using
/// the child's scope. This is the conservative variant the rewriter
/// applies at every node it does not specifically handle (Intersect,
/// Except, Window, etc.). Join and Union are NOT routed here — they
/// have their own dedicated `rewrite_join` / `rewrite_union` arms that
/// can preserve dict columns under matching snapshots.
fn rewrite_children_decoded(
    plan: LogicalPlan,
    ctx: &mut DictionaryRewriteContext,
) -> Result<LogicalPlan, String> {
    match plan {
        LogicalPlan::Intersect(mut node) => {
            let mut new_inputs = Vec::with_capacity(node.inputs.len());
            for input in node.inputs.drain(..) {
                let (rewritten, scope) = rewrite_node(input, ctx)?;
                new_inputs.push(wrap_with_decode(rewritten, &scope, ctx));
            }
            node.inputs = new_inputs;
            Ok(LogicalPlan::Intersect(node))
        }
        LogicalPlan::Except(mut node) => {
            let mut new_inputs = Vec::with_capacity(node.inputs.len());
            for input in node.inputs.drain(..) {
                let (rewritten, scope) = rewrite_node(input, ctx)?;
                new_inputs.push(wrap_with_decode(rewritten, &scope, ctx));
            }
            node.inputs = new_inputs;
            Ok(LogicalPlan::Except(node))
        }
        LogicalPlan::Window(mut node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            node.input = Box::new(wrap_with_decode(input, &scope, ctx));
            Ok(LogicalPlan::Window(node))
        }
        LogicalPlan::TableFunction(mut node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            node.input = Box::new(wrap_with_decode(input, &scope, ctx));
            Ok(LogicalPlan::TableFunction(node))
        }
        LogicalPlan::Repeat(mut node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            node.input = Box::new(wrap_with_decode(input, &scope, ctx));
            Ok(LogicalPlan::Repeat(node))
        }
        LogicalPlan::CTEAnchor(mut node) => {
            let (produce, produce_scope) = rewrite_node(*node.produce, ctx)?;
            let (consumer, consumer_scope) = rewrite_node(*node.consumer, ctx)?;
            node.produce = Box::new(wrap_with_decode(produce, &produce_scope, ctx));
            node.consumer = Box::new(wrap_with_decode(consumer, &consumer_scope, ctx));
            Ok(LogicalPlan::CTEAnchor(node))
        }
        LogicalPlan::CTEProduce(mut node) => {
            let (input, scope) = rewrite_node(*node.input, ctx)?;
            node.input = Box::new(wrap_with_decode(input, &scope, ctx));
            Ok(LogicalPlan::CTEProduce(node))
        }
        other => Ok(other),
    }
}

/// Wrap `plan` with a `Decode` for every dict column in `scope` so the
/// parent operator only sees string columns. No-op when the scope is
/// empty or none of the plan's output columns are dict-encoded.
pub(crate) fn wrap_with_decode(
    plan: LogicalPlan,
    scope: &DictScope,
    ctx: &mut DictionaryRewriteContext,
) -> LogicalPlan {
    if scope.is_empty() {
        return plan;
    }
    // Avoid double-decoding when the plan is already a Decode.
    if matches!(plan, LogicalPlan::Decode(_)) {
        return plan;
    }
    let mut mappings: Vec<DecodeMapping> = Vec::new();
    let mut renamed_outputs: Vec<OutputColumn> = Vec::new();
    let mut wrapped_any = false;
    // Dedupe dict_column references: the Bug A fix adds a pass-through
    // ProjectItem for the dict slot so downstream Aggregate / Sort
    // codegen can resolve it. When a Decode wraps that Project, the
    // plan output ends up with BOTH the source name AND the dict slot
    // name resolving to the same binding — emit only one DecodeMapping
    // and drop the duplicate output column.
    let mut seen_dict_columns: std::collections::BTreeSet<String> = Default::default();
    for mut col in plan_output_columns(&plan) {
        // After Bug B's FE rewrite, a Scan's output column is named
        // after the dict column (`__nr_dict_t_s`) directly. Resolve by
        // EITHER the user-facing source name OR the dict column name so
        // the post-decode column is restored to the source name with
        // the snapshot's data type (Utf8).
        if let Some((source_name, binding)) = scope.resolve_either(&col.name) {
            let dict_key = binding.dict_column.to_ascii_lowercase();
            if !seen_dict_columns.insert(dict_key) {
                // Already emitted a Decode for this dict column under a
                // sibling output (typically the source name). Skip the
                // duplicate so the Decode output isn't double-listed.
                continue;
            }
            mappings.push(DecodeMapping {
                dict_column: binding.dict_column.clone(),
                string_column: source_name.to_string(),
            });
            col.name = source_name.to_string();
            col.data_type = binding.snapshot.data_type.clone();
            wrapped_any = true;
        }
        renamed_outputs.push(col);
    }
    if !wrapped_any {
        return plan;
    }
    ctx.mark_changed();
    LogicalPlan::Decode(DecodeNode {
        input: Box::new(plan),
        mappings,
        output_columns: renamed_outputs,
        required_output_columns: None,
    })
}

/// Best-effort projection of a logical plan's output columns. Mirrors
/// the small subset of variants Task 7 actually manipulates;
/// downstream-of-decode boundaries do not need it.
fn plan_output_columns(plan: &LogicalPlan) -> Vec<OutputColumn> {
    match plan {
        LogicalPlan::Scan(scan) => scan.columns.clone(),
        LogicalPlan::Aggregate(node) => node.output_columns.clone(),
        LogicalPlan::Window(node) => node.output_columns.clone(),
        LogicalPlan::TableFunction(node) => node.output_columns.clone(),
        LogicalPlan::CTEProduce(node) => node.output_columns.clone(),
        LogicalPlan::CTEConsume(node) => node.output_columns.clone(),
        LogicalPlan::Decode(node) => node.output_columns.clone(),
        LogicalPlan::Filter(node) => plan_output_columns(&node.input),
        LogicalPlan::Project(node) => node
            .items
            .iter()
            .map(|item| OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
            })
            .collect(),
        LogicalPlan::Sort(node) => plan_output_columns(&node.input),
        LogicalPlan::Limit(node) => plan_output_columns(&node.input),
        LogicalPlan::Repeat(node) => plan_output_columns(&node.input),
        LogicalPlan::Join(node) => {
            let mut out = plan_output_columns(&node.left);
            out.extend(plan_output_columns(&node.right));
            out
        }
        LogicalPlan::Union(node) => node.output_columns.clone(),
        LogicalPlan::Intersect(node) => node.output_columns.clone(),
        LogicalPlan::Except(node) => node.output_columns.clone(),
        LogicalPlan::Values(node) => node.columns.clone(),
        LogicalPlan::GenerateSeries(node) => vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: node.column_name.clone(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }],
        LogicalPlan::CTEAnchor(node) => plan_output_columns(&node.consumer),
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::planner::plan::{ExceptNode, IntersectNode, UnionNode, ValuesNode};

    fn output_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        }
    }

    fn values_with_output(id: u32, name: &str) -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![output_col(id, name)],
            required_output_columns: None,
        })
    }

    #[test]
    fn set_op_plan_output_columns_use_explicit_set_op_outputs() {
        let left = values_with_output(1, "k");
        let right = values_with_output(2, "k");
        let output_columns = vec![output_col(100, "set_k")];

        let plans = vec![
            LogicalPlan::Union(UnionNode {
                inputs: vec![left.clone(), right.clone()],
                all: true,
                output_columns: output_columns.clone(),
                required_output_columns: None,
            }),
            LogicalPlan::Intersect(IntersectNode {
                inputs: vec![left.clone(), right.clone()],
                output_columns: output_columns.clone(),
                required_output_columns: None,
            }),
            LogicalPlan::Except(ExceptNode {
                inputs: vec![left, right],
                output_columns: output_columns.clone(),
                required_output_columns: None,
            }),
        ];

        for plan in plans {
            let actual = plan_output_columns(&plan);
            assert_eq!(actual.len(), output_columns.len());
            assert_eq!(actual[0].column_id, output_columns[0].column_id);
            assert_eq!(actual[0].name, output_columns[0].name);
            assert_eq!(actual[0].data_type, output_columns[0].data_type);
            assert_eq!(actual[0].nullable, output_columns[0].nullable);
        }
    }
}
