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

use crate::sql::column_id::ColumnId;
use crate::sql::common::{BinOp, OutputColumn};
use crate::sql::optimizer::operator::{
    DecodeOp, LogicalAggregateOp, LogicalJoinOp, Operator, ProjectOp, ScalarAggregateSpec,
    ScalarProjectItem, ScanOp, SortOp, TopNOp, UnionOp,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ColumnDisplay, ScalarArena, ScalarId, ScalarNode};
use crate::sql::planner::plan::{DecodeMapping, ScanDictionaryColumn};

use super::context::{DictBinding, DictScope, DictionaryRewriteContext};
use super::expr::{DICT_AGG_FUNCTIONS, dict_keys_compatible, rewrite_column_ref_with_scope};

pub(crate) fn rewrite(
    expr: OptExpr,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<OptExpr, String> {
    let (expr, _scope) = rewrite_node(expr, ctx, arena)?;
    Ok(expr)
}

fn rewrite_node(
    expr: OptExpr,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<(OptExpr, DictScope), String> {
    let OptExpr {
        op,
        mut children,
        required_output_columns,
    } = expr;
    match op {
        Operator::LogicalScan(mut scan) => {
            let scope = rewrite_scan(&mut scan, ctx);
            Ok((
                opt_expr(
                    Operator::LogicalScan(scan),
                    children,
                    required_output_columns,
                ),
                scope,
            ))
        }
        Operator::LogicalFilter(node) => {
            let input = take_unary_child(&mut children);
            let (input, scope) = rewrite_node(input, ctx, arena)?;
            Ok((
                opt_expr(
                    Operator::LogicalFilter(node),
                    vec![input],
                    required_output_columns,
                ),
                scope,
            ))
        }
        Operator::LogicalProject(node) => {
            rewrite_project(node, children, required_output_columns, ctx, arena)
        }
        Operator::LogicalAggregate(node) => {
            rewrite_aggregate(node, children, required_output_columns, ctx, arena)
        }
        Operator::LogicalSort(node) => {
            rewrite_sort(node, children, required_output_columns, ctx, arena)
        }
        Operator::LogicalTopN(node) => {
            rewrite_topn(node, children, required_output_columns, ctx, arena)
        }
        Operator::LogicalLimit(node) => {
            let input = take_unary_child(&mut children);
            let (input, scope) = rewrite_node(input, ctx, arena)?;
            Ok((
                opt_expr(
                    Operator::LogicalLimit(node),
                    vec![input],
                    required_output_columns,
                ),
                scope,
            ))
        }
        Operator::LogicalJoin(node) => {
            rewrite_join(node, children, required_output_columns, ctx, arena)
        }
        Operator::LogicalUnion(node) => {
            rewrite_union(node, children, required_output_columns, ctx, arena)
        }
        // UNION DISTINCT / INTERSECT / EXCEPT semantics require hashing
        // on the user-facing string value — dict ids from different
        // snapshots cannot be compared directly. Always decode here.
        Operator::LogicalIntersect(_)
        | Operator::LogicalExcept(_)
        | Operator::LogicalWindow(_)
        | Operator::LogicalTableFunction(_)
        | Operator::LogicalRepeat(_)
        | Operator::LogicalAggregateStateMerge(_)
        | Operator::LogicalApply(_)
        | Operator::LogicalAssertOneRow(_)
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
        | Operator::LogicalCTEAnchor(_)
        | Operator::LogicalCTEProduce(_) => {
            decode_boundary(opt_expr(op, children, required_output_columns), ctx, arena)
        }
        // Leaves that produce no dict columns of their own.
        Operator::LogicalCTEConsume(_)
        | Operator::LogicalValues(_)
        | Operator::LogicalGenerateSeries(_) => Ok((
            opt_expr(op, children, required_output_columns),
            DictScope::new(),
        )),
        // Decode is the rewrite's own output; do not recurse into it
        // again. The decoded output is all strings — no dict scope.
        Operator::LogicalDecode(_) => Ok((
            opt_expr(op, children, required_output_columns),
            DictScope::new(),
        )),

        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
        other => panic!(
            "low-cardinality dictionary rewrite received physical operator {:?}",
            other
        ),
    }
}

fn opt_expr(
    op: Operator,
    children: Vec<OptExpr>,
    required_output_columns: Option<std::collections::HashSet<ColumnId>>,
) -> OptExpr {
    OptExpr {
        op,
        children,
        required_output_columns,
    }
}

fn take_unary_child(children: &mut Vec<OptExpr>) -> OptExpr {
    assert_eq!(children.len(), 1, "expected one logical plan child");
    children.remove(0)
}

fn take_binary_children(children: &mut Vec<OptExpr>) -> (OptExpr, OptExpr) {
    assert_eq!(children.len(), 2, "expected two logical plan children");
    let right = children.remove(1);
    let left = children.remove(0);
    (left, right)
}

// Dict-slot expressions intentionally reuse the source ColumnId. ScalarArena
// stores display metadata per ColumnId, so promote the display only after the
// source expressions have been materialized and retargeted to the dict name.
fn remember_dict_column_display(arena: &mut ScalarArena, binding: &DictBinding) {
    arena.remember_project_output_display(
        binding.source_column_id,
        None,
        binding.dict_column.clone(),
    );
}

fn remember_scope_dict_ref_displays(arena: &mut ScalarArena, expr: ScalarId, scope: &DictScope) {
    remember_dict_ref_displays_by(arena, expr, &|arena, expr| {
        resolve_scope_dict_ref(arena, expr, scope)
    });
}

fn remember_join_dict_ref_displays(
    arena: &mut ScalarArena,
    expr: ScalarId,
    left_scope: &DictScope,
    right_scope: &DictScope,
) {
    remember_dict_ref_displays_by(arena, expr, &|arena, expr| {
        resolve_scope_dict_ref(arena, expr, left_scope)
            .or_else(|| resolve_scope_dict_ref(arena, expr, right_scope))
    });
}

fn resolve_scope_dict_ref(
    arena: &ScalarArena,
    expr: ScalarId,
    scope: &DictScope,
) -> Option<DictBinding> {
    let (_, binding) = resolve_scalar_ref(arena, expr, scope)?;
    if is_dict_ref(arena, expr, binding) {
        Some(binding.clone())
    } else {
        None
    }
}

fn remember_dict_ref_displays_by(
    arena: &mut ScalarArena,
    expr: ScalarId,
    resolve: &impl Fn(&ScalarArena, ScalarId) -> Option<DictBinding>,
) {
    let node = arena.node(expr).clone();
    match node {
        ScalarNode::ColumnRef(_) => {
            if let Some(binding) = resolve(arena, expr) {
                remember_dict_column_display(arena, &binding);
            }
        }
        ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            remember_dict_ref_displays_by(arena, left, resolve);
            remember_dict_ref_displays_by(arena, right, resolve);
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::LambdaFunction { body: child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child)
        | ScalarNode::Lambda { body: child, .. } => {
            remember_dict_ref_displays_by(arena, child, resolve);
        }
        ScalarNode::FunctionCall { args, .. } | ScalarNode::AggregateCall { args, .. } => {
            for arg in args {
                remember_dict_ref_displays_by(arena, arg, resolve);
            }
        }
        ScalarNode::InList { child, list, .. } => {
            remember_dict_ref_displays_by(arena, child, resolve);
            for item in list {
                remember_dict_ref_displays_by(arena, item, resolve);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            remember_dict_ref_displays_by(arena, child, resolve);
            remember_dict_ref_displays_by(arena, low, resolve);
            remember_dict_ref_displays_by(arena, high, resolve);
        }
        ScalarNode::Like { child, pattern, .. } => {
            remember_dict_ref_displays_by(arena, child, resolve);
            remember_dict_ref_displays_by(arena, pattern, resolve);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                remember_dict_ref_displays_by(arena, operand, resolve);
            }
            for (when, then) in when_then {
                remember_dict_ref_displays_by(arena, when, resolve);
                remember_dict_ref_displays_by(arena, then, resolve);
            }
            if let Some(else_expr) = else_expr {
                remember_dict_ref_displays_by(arena, else_expr, resolve);
            }
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                remember_dict_ref_displays_by(arena, arg, resolve);
            }
            for expr in partition_by {
                remember_dict_ref_displays_by(arena, expr, resolve);
            }
            for item in order_by {
                remember_dict_ref_displays_by(arena, item.expr, resolve);
            }
        }
    }
}

fn resolve_scalar_ref<'a>(
    arena: &ScalarArena,
    expr: ScalarId,
    scope: &'a DictScope,
) -> Option<(&'a str, &'a DictBinding)> {
    super::expr::resolve_column_ref(arena, expr, scope)
}

fn is_dict_ref(arena: &ScalarArena, expr: ScalarId, binding: &DictBinding) -> bool {
    matches!(arena.node(expr), ScalarNode::ColumnRef(column_id) if *column_id == binding.source_column_id)
        && matches!(arena.data_type(expr), DataType::Int32)
}

fn dict_column_display(binding: &DictBinding) -> ColumnDisplay {
    ColumnDisplay {
        qualifier: None,
        column: binding.dict_column.clone(),
    }
}

fn dict_column_ref(arena: &mut ScalarArena, expr: ScalarId, binding: &DictBinding) -> ScalarId {
    rewrite_column_ref_with_scope(arena, expr, &single_binding_scope(binding))
}

fn dict_column_ref_with_nullable(
    arena: &mut ScalarArena,
    binding: &DictBinding,
    nullable: bool,
) -> ScalarId {
    remember_dict_column_display(arena, binding);
    arena.intern(
        ScalarNode::ColumnRef(binding.source_column_id),
        DataType::Int32,
        nullable,
    )
}

fn single_binding_scope(binding: &DictBinding) -> DictScope {
    let mut scope = DictScope::new();
    scope.insert(binding.dict_column.clone(), binding.clone());
    scope
}

fn rewrite_scan(scan: &mut ScanOp, ctx: &mut DictionaryRewriteContext) -> DictScope {
    let mut scope = DictScope::new();
    // Idempotency guard: an already-populated `dict_columns` means a
    // previous application of this rule already rewrote the scan.
    // Rebuild the scope from the existing hints so callers above still
    // see the bindings.
    if !scan.dict_columns.is_empty() {
        for hint in &scan.dict_columns {
            let source_column_id = scan
                .columns
                .iter()
                .find(|c| {
                    c.name.eq_ignore_ascii_case(&hint.dict_column)
                        || c.name.eq_ignore_ascii_case(&hint.source_column)
                })
                .map(|c| c.column_id)
                .unwrap_or(ColumnId::UNSET);
            scope.insert(
                hint.source_column.clone(),
                DictBinding {
                    dict_column: hint.dict_column.clone(),
                    source_column_id,
                    snapshot: hint.dictionary.clone(),
                },
            );
        }
        return scope;
    }
    let eligible = ctx.dict_eligible_columns_for_scan(&scan.database, &scan.table.name);
    if eligible.is_empty() {
        return scope;
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
                source_column_id,
                snapshot,
            },
        );
        ctx.mark_changed();
    }
    scope
}

fn rewrite_project(
    node: ProjectOp,
    mut children: Vec<OptExpr>,
    required_output_columns: Option<std::collections::HashSet<ColumnId>>,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<(OptExpr, DictScope), String> {
    let input = take_unary_child(&mut children);
    let (input, input_scope) = rewrite_node(input, ctx, arena)?;
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
    let mut items: Vec<ScalarProjectItem> = Vec::with_capacity(node.items.len());
    for item in node.items.into_iter() {
        if matches!(arena.node(item.expr), ScalarNode::ColumnRef(_))
            && let Some((_, binding)) = resolve_scalar_ref(arena, item.expr, &input_scope)
        {
            let binding = binding.clone();
            output_scope.insert(item.output_name.clone(), binding.clone());
            // Idempotency: don't double-rewrite when the column is
            // already the dict slot (post-iteration-1 of the pipeline's
            // fixed-point loop).
            let already_dict = is_dict_ref(arena, item.expr, &binding);
            let rewritten = if already_dict {
                item.expr
            } else {
                dict_column_ref(arena, item.expr, &binding)
            };
            items.push(ScalarProjectItem {
                expr: rewritten,
                output_name: item.output_name,
                output_column_id: item.output_column_id,
                expr_display: Some(dict_column_display(&binding)),
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
    let input_cols = plan_output_columns(&input, arena);
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
        items.push(ScalarProjectItem {
            expr: dict_column_ref_with_nullable(arena, binding, nullable),
            output_name: dict_name,
            // Synthetic dict-slot pass-through; not addressed by the pruning pass.
            output_column_id: binding.source_column_id,
            expr_display: Some(dict_column_display(binding)),
        });
    }
    Ok((
        opt_expr(
            Operator::LogicalProject(ProjectOp {
                items,
                output_qualifier: node.output_qualifier,
            }),
            vec![input],
            required_output_columns,
        ),
        output_scope,
    ))
}

fn rewrite_aggregate(
    node: LogicalAggregateOp,
    mut children: Vec<OptExpr>,
    required_output_columns: Option<std::collections::HashSet<ColumnId>>,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<(OptExpr, DictScope), String> {
    let input = take_unary_child(&mut children);
    let (input, input_scope) = rewrite_node(input, ctx, arena)?;
    let mut group_by = Vec::with_capacity(node.group_by.len());
    let mut decoded_group_keys: Vec<(
        String,
        String,
        ColumnId,
        std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot>,
    )> = Vec::new();
    for (index, expr) in node.group_by.iter().copied().enumerate() {
        if matches!(arena.node(expr), ScalarNode::ColumnRef(_))
            && let Some((source_name, binding)) = resolve_scalar_ref(arena, expr, &input_scope)
        {
            let binding = binding.clone();
            group_by.push(dict_column_ref(arena, expr, &binding));
            // Skip the decode-wrap bookkeeping only when the aggregate
            // output is already the dict slot. With realistic shared
            // ColumnIds, ScalarArena display metadata can materialize a
            // pre-rewrite group key as `__nr_dict_*` in the same pass;
            // the aggregate output column is still the user-facing
            // source name in that case, so we must still add Decode.
            let output_already_dict = node
                .output_columns
                .get(index)
                .is_some_and(|out| out.name.eq_ignore_ascii_case(&binding.dict_column));
            if output_already_dict {
                continue;
            }
            // The aggregate node was emitting the original string
            // column name to consumers; we must surface that name
            // through a Decode boundary above the aggregate.
            decoded_group_keys.push((
                binding.dict_column.clone(),
                source_name.to_string(),
                binding.source_column_id,
                binding.snapshot.clone(),
            ));
            continue;
        }
        group_by.push(expr);
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
            if let Some((_, binding)) = input_scope.resolve_either(&out.name) {
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
        .map(|agg| rewrite_aggregate_call(agg, &input_scope, ctx, arena))
        .collect::<Vec<_>>();
    for expr in &group_by {
        remember_scope_dict_ref_displays(arena, *expr, &input_scope);
    }
    for agg in &aggregates {
        for arg in &agg.args {
            remember_scope_dict_ref_displays(arena, *arg, &input_scope);
        }
        for item in &agg.order_by {
            remember_scope_dict_ref_displays(arena, item.expr, &input_scope);
        }
    }

    let aggregate = opt_expr(
        Operator::LogicalAggregate(LogicalAggregateOp {
            stage: node.stage,
            group_by,
            aggregates,
            output_columns: output_columns.clone(),
            is_merge: node.is_merge,
            is_split: node.is_split,
        }),
        vec![input],
        required_output_columns,
    );
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
    for (dict, string, _, snap) in &decoded_group_keys {
        decoded_index.insert(dict.clone(), (string.clone(), snap.clone()));
    }
    let mappings: Vec<DecodeMapping> = decoded_group_keys
        .iter()
        .map(|(dict, string, source_column_id, _)| DecodeMapping {
            source_column_id: *source_column_id,
            output_column_id: *source_column_id,
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
        opt_expr(
            Operator::LogicalDecode(DecodeOp {
                mappings,
                output_columns,
            }),
            vec![aggregate],
            None,
        ),
        DictScope::new(),
    ))
}

fn rewrite_sort(
    node: SortOp,
    mut children: Vec<OptExpr>,
    required_output_columns: Option<std::collections::HashSet<ColumnId>>,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<(OptExpr, DictScope), String> {
    let input = take_unary_child(&mut children);
    let (input, input_scope) = rewrite_node(input, ctx, arena)?;
    let original_items = node.items;
    let needs_decode = original_items.iter().any(|item| {
        resolve_scalar_ref(arena, item.expr, &input_scope)
            .is_some_and(|(_, binding)| !binding.snapshot.order_preserving)
    });
    let (items, input, output_scope) = if needs_decode {
        // Decode below the sort: the sort itself now sees strings and
        // surfaces strings; no dict columns leak upward.
        (
            original_items,
            wrap_with_decode(input, &input_scope, ctx, arena),
            DictScope::new(),
        )
    } else {
        let mut items = Vec::with_capacity(original_items.len());
        for item in &original_items {
            if matches!(arena.node(item.expr), ScalarNode::ColumnRef(_))
                && let Some((_, binding)) = resolve_scalar_ref(arena, item.expr, &input_scope)
            {
                let binding = binding.clone();
                let already_dict = is_dict_ref(arena, item.expr, &binding);
                let mut rewritten = item.clone();
                rewritten.expr = if already_dict {
                    item.expr
                } else {
                    dict_column_ref(arena, item.expr, &binding)
                };
                rewritten.display = Some(dict_column_display(&binding));
                items.push(rewritten);
                if !already_dict {
                    ctx.mark_changed();
                }
                continue;
            }
            items.push(item.clone());
        }
        (items, input, input_scope)
    };
    Ok((
        opt_expr(
            Operator::LogicalSort(SortOp {
                items,
                analytic_partition_exprs: node.analytic_partition_exprs,
                partition_limit: node.partition_limit,
                topn_type: node.topn_type,
            }),
            vec![input],
            required_output_columns,
        ),
        output_scope,
    ))
}

fn rewrite_topn(
    node: TopNOp,
    mut children: Vec<OptExpr>,
    required_output_columns: Option<std::collections::HashSet<ColumnId>>,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<(OptExpr, DictScope), String> {
    let input = take_unary_child(&mut children);
    let (input, input_scope) = rewrite_node(input, ctx, arena)?;
    let original_items = node.items;
    let needs_decode = original_items.iter().any(|item| {
        resolve_scalar_ref(arena, item.expr, &input_scope)
            .is_some_and(|(_, binding)| !binding.snapshot.order_preserving)
    });
    let (items, input, output_scope) = if needs_decode {
        (
            original_items,
            wrap_with_decode(input, &input_scope, ctx, arena),
            DictScope::new(),
        )
    } else {
        let mut items = Vec::with_capacity(original_items.len());
        for item in &original_items {
            if matches!(arena.node(item.expr), ScalarNode::ColumnRef(_))
                && let Some((_, binding)) = resolve_scalar_ref(arena, item.expr, &input_scope)
            {
                let binding = binding.clone();
                let already_dict = is_dict_ref(arena, item.expr, &binding);
                let mut rewritten = item.clone();
                rewritten.expr = if already_dict {
                    item.expr
                } else {
                    dict_column_ref(arena, item.expr, &binding)
                };
                rewritten.display = Some(dict_column_display(&binding));
                items.push(rewritten);
                if !already_dict {
                    ctx.mark_changed();
                }
                continue;
            }
            items.push(item.clone());
        }
        (items, input, input_scope)
    };
    Ok((
        opt_expr(
            Operator::LogicalTopN(TopNOp {
                items,
                limit: node.limit,
                offset: node.offset,
                phase: node.phase,
                is_split: node.is_split,
            }),
            vec![input],
            required_output_columns,
        ),
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
    mut agg: ScalarAggregateSpec,
    input_scope: &DictScope,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> ScalarAggregateSpec {
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
        if !matches!(arena.node(item.expr), ScalarNode::ColumnRef(_)) {
            return false;
        }
        resolve_scalar_ref(arena, item.expr, input_scope)
            .is_none_or(|(_, binding)| binding.snapshot.order_preserving)
    });

    let mut rewrote_any_arg = false;
    let mut new_args = Vec::with_capacity(agg.args.len());
    for arg in agg.args.drain(..) {
        if order_by_dict_compatible
            && matches!(arena.node(arg), ScalarNode::ColumnRef(_))
            && let Some((_, binding)) = resolve_scalar_ref(arena, arg, input_scope)
        {
            let binding = binding.clone();
            // Idempotency: an already-dict-rewritten arg (post first
            // pipeline iteration) keeps the same ColumnRef without
            // marking the rewrite changed; otherwise the pipeline's
            // fixed-point loop would never terminate.
            let already_dict = is_dict_ref(arena, arg, &binding);
            new_args.push(if already_dict {
                arg
            } else {
                dict_column_ref(arena, arg, &binding)
            });
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
    node: LogicalJoinOp,
    mut children: Vec<OptExpr>,
    required_output_columns: Option<std::collections::HashSet<ColumnId>>,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<(OptExpr, DictScope), String> {
    let (left, right) = take_binary_children(&mut children);
    let (left, left_scope) = rewrite_node(left, ctx, arena)?;
    let (right, right_scope) = rewrite_node(right, ctx, arena)?;

    // No condition → CROSS JOIN. There is no opportunity to compare on
    // dict ids; keep the conservative boundary by decoding both sides.
    let Some(condition_id) = node.condition else {
        let left = wrap_with_decode(left, &left_scope, ctx, arena);
        let right = wrap_with_decode(right, &right_scope, ctx, arena);
        return Ok((
            opt_expr(
                Operator::LogicalJoin(LogicalJoinOp {
                    join_type: node.join_type,
                    condition: None,
                }),
                vec![left, right],
                required_output_columns,
            ),
            DictScope::new(),
        ));
    };
    // Collect the equality pairs that align two dict-compatible columns.
    // For each such pair we keep both sides' dict columns and rewrite the
    // ColumnRef nodes inside the condition. Pairs that don't align
    // (different snapshots, only one side has a dict binding, or non-
    // equality predicates) fall through to per-side Decode.
    let aligned = aligned_dict_join_pairs(arena, condition_id, &left_scope, &right_scope);

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

    let left = wrap_with_decode_except(left, &left_scope, &keep_left, ctx, arena);
    let right = wrap_with_decode_except(right, &right_scope, &keep_right, ctx, arena);

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
        Some(condition_id)
    } else {
        if condition_references_source_names(arena, condition_id, &left_scope, &right_scope) {
            ctx.mark_changed();
        }
        let rewritten =
            rewrite_join_condition_pairs(arena, condition_id, &aligned, &left_scope, &right_scope);
        remember_join_dict_ref_displays(arena, rewritten, &left_scope, &right_scope);
        Some(rewritten)
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
        opt_expr(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: node.join_type,
                condition,
            }),
            vec![left, right],
            required_output_columns,
        ),
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
    arena: &ScalarArena,
    cond: ScalarId,
    left_scope: &DictScope,
    right_scope: &DictScope,
) -> Vec<AlignedEquiPair> {
    let mut out = Vec::new();
    collect_equi_pairs(arena, cond, left_scope, right_scope, &mut out);
    out
}

fn collect_equi_pairs(
    arena: &ScalarArena,
    expr: ScalarId,
    left_scope: &DictScope,
    right_scope: &DictScope,
    out: &mut Vec<AlignedEquiPair>,
) {
    match arena.node(expr) {
        ScalarNode::BinaryOp { left, op, right } if matches!(op, BinOp::And) => {
            collect_equi_pairs(arena, *left, left_scope, right_scope, out);
            collect_equi_pairs(arena, *right, left_scope, right_scope, out);
        }
        ScalarNode::BinaryOp { left, op, right } if matches!(op, BinOp::Eq | BinOp::EqForNull) => {
            if let Some(pair) = pair_dict_columns(arena, *left, *right, left_scope, right_scope) {
                out.push(pair);
            }
        }
        ScalarNode::Nested(inner) => {
            collect_equi_pairs(arena, *inner, left_scope, right_scope, out)
        }
        _ => {}
    }
}

fn pair_dict_columns(
    arena: &ScalarArena,
    a: ScalarId,
    b: ScalarId,
    left_scope: &DictScope,
    right_scope: &DictScope,
) -> Option<AlignedEquiPair> {
    if !matches!(arena.node(a), ScalarNode::ColumnRef(_))
        || !matches!(arena.node(b), ScalarNode::ColumnRef(_))
    {
        return None;
    }
    // Resolve by EITHER the source column name (`name`) OR the dict
    // column name (`__nr_dict_t1_name`). After a prior pipeline
    // iteration the ColumnRefs in the condition already point at the
    // dict slot; `resolve_either` finds the binding under that name
    // too. The returned `key` is the binding's source-name registration
    // — used by the caller for the keep-set bookkeeping.
    let (left_key, right_key, left_binding, right_binding, lhs_is_left_input) = match (
        resolve_scalar_ref(arena, a, left_scope),
        resolve_scalar_ref(arena, b, right_scope),
    ) {
        (Some(l), Some(r)) => (l.0.to_string(), r.0.to_string(), l.1, r.1, true),
        _ => match (
            resolve_scalar_ref(arena, b, left_scope),
            resolve_scalar_ref(arena, a, right_scope),
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
    arena: &ScalarArena,
    expr: ScalarId,
    left_scope: &DictScope,
    right_scope: &DictScope,
) -> bool {
    match arena.node(expr) {
        ScalarNode::ColumnRef(_) => {
            if let Some((_, binding)) = resolve_scalar_ref(arena, expr, left_scope) {
                return !is_dict_ref(arena, expr, binding);
            }
            if let Some((_, binding)) = resolve_scalar_ref(arena, expr, right_scope) {
                return !is_dict_ref(arena, expr, binding);
            }
            false
        }
        ScalarNode::BinaryOp { left, right, .. } => {
            condition_references_source_names(arena, *left, left_scope, right_scope)
                || condition_references_source_names(arena, *right, left_scope, right_scope)
        }
        ScalarNode::Nested(inner) => {
            condition_references_source_names(arena, *inner, left_scope, right_scope)
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
    arena: &mut ScalarArena,
    cond: ScalarId,
    aligned: &[AlignedEquiPair],
    left_scope: &DictScope,
    right_scope: &DictScope,
) -> ScalarId {
    let node = arena.node(cond).clone();
    match node {
        ScalarNode::BinaryOp { left, op, right } if matches!(op, BinOp::And) => {
            let new_left =
                rewrite_join_condition_pairs(arena, left, aligned, left_scope, right_scope);
            let new_right =
                rewrite_join_condition_pairs(arena, right, aligned, left_scope, right_scope);
            arena.intern(
                ScalarNode::BinaryOp {
                    left: new_left,
                    op,
                    right: new_right,
                },
                arena.data_type(cond).clone(),
                arena.nullable(cond),
            )
        }
        ScalarNode::Nested(inner) => {
            let new_inner =
                rewrite_join_condition_pairs(arena, inner, aligned, left_scope, right_scope);
            arena.intern(
                ScalarNode::Nested(new_inner),
                arena.data_type(cond).clone(),
                arena.nullable(cond),
            )
        }
        ScalarNode::BinaryOp { left, op, right } if matches!(op, BinOp::Eq | BinOp::EqForNull) => {
            if !matches!(arena.node(left), ScalarNode::ColumnRef(_))
                || !matches!(arena.node(right), ScalarNode::ColumnRef(_))
            {
                return cond;
            }
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
                let lhs_match = scalar_ref_matches_binding(arena, left, lhs_scope, predicate_left);
                let rhs_match =
                    scalar_ref_matches_binding(arena, right, rhs_scope, predicate_right);
                if lhs_match && rhs_match {
                    let lhs_binding = lhs_scope.get(predicate_left).expect("scope has binding");
                    let rhs_binding = rhs_scope.get(predicate_right).expect("scope has binding");
                    let new_left = dict_column_ref(arena, left, lhs_binding);
                    let new_right = dict_column_ref(arena, right, rhs_binding);
                    return arena.intern(
                        ScalarNode::BinaryOp {
                            left: new_left,
                            op,
                            right: new_right,
                        },
                        arena.data_type(cond).clone(),
                        arena.nullable(cond),
                    );
                }
            }
            cond
        }
        _ => cond,
    }
}

fn scalar_ref_matches_binding(
    arena: &ScalarArena,
    expr: ScalarId,
    scope: &DictScope,
    source_name: &str,
) -> bool {
    resolve_scalar_ref(arena, expr, scope).is_some_and(|(resolved_name, binding)| {
        resolved_name.eq_ignore_ascii_case(source_name)
            || binding.dict_column.eq_ignore_ascii_case(source_name)
    })
}

/// Wrap `plan` with a `Decode` for every dict column in `scope` whose
/// name is NOT in `keep`. The kept columns continue to flow upward as
/// dict ids; everything else is decoded back to strings.
fn wrap_with_decode_except(
    plan: OptExpr,
    scope: &DictScope,
    keep: &std::collections::BTreeSet<String>,
    ctx: &mut DictionaryRewriteContext,
    arena: &ScalarArena,
) -> OptExpr {
    let mut decoded_scope = DictScope::new();
    for col in plan_output_columns(&plan, arena) {
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
        wrap_with_decode(plan, &decoded_scope, ctx, arena)
    }
}

/// UNION ALL: preserve dict columns only when *every* input exposes a
/// `dict_keys_compatible` snapshot for that output column (Task 8 item
/// 2). UNION DISTINCT decodes (the distinct-on-string semantics make a
/// dict-id union unsafe across snapshots).
fn rewrite_union(
    node: UnionOp,
    mut children: Vec<OptExpr>,
    required_output_columns: Option<std::collections::HashSet<ColumnId>>,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<(OptExpr, DictScope), String> {
    // Rewrite every input subtree first so we have per-input scopes.
    let mut rewritten_inputs: Vec<(OptExpr, DictScope)> = Vec::with_capacity(children.len());
    for input in children.drain(..) {
        rewritten_inputs.push(rewrite_node(input, ctx, arena)?);
    }

    // UNION DISTINCT always decodes.
    if !node.all {
        let mut new_inputs = Vec::with_capacity(rewritten_inputs.len());
        for (plan, scope) in rewritten_inputs {
            new_inputs.push(wrap_with_decode(plan, &scope, ctx, arena));
        }
        return Ok((
            opt_expr(
                Operator::LogicalUnion(node),
                new_inputs,
                required_output_columns,
            ),
            DictScope::new(),
        ));
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
        for col in plan_output_columns(first_plan, arena) {
            if let Some((source_name, b)) = first_scope.resolve_either(&col.name) {
                preserved.insert(source_name.to_ascii_lowercase(), b.clone());
            }
        }
    }
    for (plan, scope) in rewritten_inputs.iter().skip(1) {
        let cols = plan_output_columns(plan, arena);
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
            new_inputs.push(wrap_with_decode(plan, &scope, ctx, arena));
        }
        return Ok((
            opt_expr(
                Operator::LogicalUnion(node),
                new_inputs,
                required_output_columns,
            ),
            DictScope::new(),
        ));
    }

    // Preserve only the matching columns on each input; decode the rest.
    let keep_set: std::collections::BTreeSet<String> = preserved.keys().cloned().collect();
    let mut new_inputs = Vec::with_capacity(rewritten_inputs.len());
    for (plan, scope) in rewritten_inputs {
        new_inputs.push(wrap_with_decode_except(plan, &scope, &keep_set, ctx, arena));
    }
    // Output scope: surface the preserved bindings upward.
    let mut out_scope = DictScope::new();
    for (name, binding) in preserved {
        out_scope.insert(name, binding);
    }
    // Preservation itself does not flip any plan bits — the scope is a
    // recursion-local effect, not a tree mutation. Avoid `mark_changed`
    // here so the pipeline's fixed-point loop terminates.
    Ok((
        opt_expr(
            Operator::LogicalUnion(node),
            new_inputs,
            required_output_columns,
        ),
        out_scope,
    ))
}

fn decode_boundary(
    plan: OptExpr,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<(OptExpr, DictScope), String> {
    // For nodes Task 7 does not refine, recurse into their children to
    // pick up scan-side dict columns, then wrap each child with a
    // Decode so the node itself never has to know about dict ids.
    let rewritten = rewrite_children_decoded(plan, ctx, arena)?;
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
    mut plan: OptExpr,
    ctx: &mut DictionaryRewriteContext,
    arena: &mut ScalarArena,
) -> Result<OptExpr, String> {
    let mut children = Vec::with_capacity(plan.children.len());
    for child in std::mem::take(&mut plan.children) {
        let (rewritten, scope) = rewrite_node(child, ctx, arena)?;
        children.push(wrap_with_decode(rewritten, &scope, ctx, arena));
    }
    plan.children = children;
    Ok(plan)
}

/// Wrap `plan` with a `Decode` for every dict column in `scope` so the
/// parent operator only sees string columns. No-op when the scope is
/// empty or none of the plan's output columns are dict-encoded.
pub(crate) fn wrap_with_decode(
    plan: OptExpr,
    scope: &DictScope,
    ctx: &mut DictionaryRewriteContext,
    arena: &ScalarArena,
) -> OptExpr {
    if scope.is_empty() {
        return plan;
    }
    // Avoid double-decoding when the plan is already a Decode.
    if matches!(&plan.op, Operator::LogicalDecode(_)) {
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
    for mut col in plan_output_columns(&plan, arena) {
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
                source_column_id: binding.source_column_id,
                output_column_id: col.column_id,
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
    opt_expr(
        Operator::LogicalDecode(DecodeOp {
            mappings,
            output_columns: renamed_outputs,
        }),
        vec![plan],
        None,
    )
}

/// Best-effort projection of a logical plan's output columns. Mirrors
/// the small subset of variants Task 7 actually manipulates;
/// downstream-of-decode boundaries do not need it.
fn plan_output_columns(plan: &OptExpr, arena: &ScalarArena) -> Vec<OutputColumn> {
    match &plan.op {
        Operator::LogicalScan(scan) => scan.columns.clone(),
        Operator::LogicalAggregate(node) => node.output_columns.clone(),
        Operator::LogicalWindow(node) => node.output_columns.clone(),
        Operator::LogicalTableFunction(node) => node.output_columns.clone(),
        Operator::LogicalCTEProduce(node) => node.output_columns.clone(),
        Operator::LogicalCTEConsume(node) => node.output_columns.clone(),
        Operator::LogicalDecode(node) => node.output_columns.clone(),
        Operator::LogicalAggregateStateMerge(node) => node.output_columns.clone(),
        Operator::LogicalFilter(_) => plan_output_columns(plan.unary_input(), arena),
        Operator::LogicalProject(node) => node
            .items
            .iter()
            .map(|item| OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: arena.data_type(item.expr).clone(),
                nullable: arena.nullable(item.expr),
                is_internal: false,
            })
            .collect(),
        Operator::LogicalSort(_) | Operator::LogicalTopN(_) => {
            plan_output_columns(plan.unary_input(), arena)
        }
        Operator::LogicalLimit(_) => plan_output_columns(plan.unary_input(), arena),
        Operator::LogicalRepeat(_) => plan_output_columns(plan.unary_input(), arena),
        Operator::LogicalJoin(_) => {
            let mut out = plan_output_columns(plan.left(), arena);
            out.extend(plan_output_columns(plan.right(), arena));
            out
        }
        Operator::LogicalUnion(node) => node.output_columns.clone(),
        Operator::LogicalIntersect(node) => node.output_columns.clone(),
        Operator::LogicalExcept(node) => node.output_columns.clone(),
        Operator::LogicalValues(node) => node.columns.clone(),
        Operator::LogicalGenerateSeries(node) => vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: node.column_name.clone(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }],
        Operator::LogicalCTEAnchor(_) => plan_output_columns(plan.child(1), arena),
        Operator::LogicalApply(node) => {
            let mut out = plan_output_columns(plan.left(), arena);
            out.push(node.output_column.clone());
            out
        }
        Operator::LogicalAssertOneRow(_) => plan_output_columns(plan.unary_input(), arena),
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
        other => panic!(
            "low-cardinality dictionary rewrite received physical operator {:?}",
            other
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::convert::logical_plan_to_opt_expr;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{
        LogicalExceptNode, LogicalIntersectNode, LogicalUnionNode, LogicalValuesNode, PlanNodeKind,
    };

    fn output_col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        }
    }

    fn values_with_output(id: u32, name: &str) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![output_col(id, name)],
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn set_op_plan_output_columns_use_explicit_set_op_outputs() {
        let left = values_with_output(1, "k");
        let right = values_with_output(2, "k");
        let output_columns = vec![output_col(100, "set_k")];

        let plans = vec![
            LogicalPlanNode::new(
                PlanNodeKind::Union(LogicalUnionNode {
                    all: true,
                    output_columns: output_columns.clone(),
                }),
                vec![left.clone(), right.clone()],
                None,
            ),
            LogicalPlanNode::new(
                PlanNodeKind::Intersect(LogicalIntersectNode {
                    output_columns: output_columns.clone(),
                }),
                vec![left.clone(), right.clone()],
                None,
            ),
            LogicalPlanNode::new(
                PlanNodeKind::Except(LogicalExceptNode {
                    output_columns: output_columns.clone(),
                }),
                vec![left, right],
                None,
            ),
        ];

        for plan in plans {
            let mut arena = ScalarArena::new();
            let opt = logical_plan_to_opt_expr(&plan, &mut arena);
            let actual = plan_output_columns(&opt, &arena);
            assert_eq!(actual.len(), output_columns.len());
            assert_eq!(actual[0].column_id, output_columns[0].column_id);
            assert_eq!(actual[0].name, output_columns[0].name);
            assert_eq!(actual[0].data_type, output_columns[0].data_type);
            assert_eq!(actual[0].nullable, output_columns[0].nullable);
        }
    }
}
