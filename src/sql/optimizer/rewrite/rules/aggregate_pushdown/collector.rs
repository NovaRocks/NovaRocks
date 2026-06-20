//! Aggregate pushdown collector — phase 1 of the rule.

use std::collections::HashMap;

use crate::sql::column_id::ColumnId;
use crate::sql::common::BinOp;
use crate::sql::optimizer::operator::{LogicalAggregateOp, LogicalJoinOp, Operator};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::scalar_expr;
use crate::sql::optimizer::statistics::TableStatistics;

use super::context::{AggregatePushDownContext, ColumnRefIdentity, PushPlan, Side};

/// Examine the LogicalAggregateOp for entry-level rejections.
/// Returns Some(ctx) when the aggregate is a candidate to push;
/// returns None when an entry-level filter rejects it.
pub(crate) fn entry_safety_check(
    aggregate: &LogicalAggregateOp,
    arena: &ScalarArena,
) -> Option<AggregatePushDownContext> {
    // Idempotency guard.
    if aggregate.is_split {
        return None;
    }
    // Empty group-by: partial collapses to a single row.
    if aggregate.group_by.is_empty() {
        return None;
    }
    // Per-call filters.
    for spec in &aggregate.aggregates {
        // Distinct is SplitDistinctAgg's domain.
        if spec.distinct {
            return None;
        }
        // Order-sensitive aggregate.
        if !spec.order_by.is_empty() {
            return None;
        }
        // White-list check.
        let name = spec.name.to_ascii_lowercase();
        if !matches!(name.as_str(), "sum" | "min" | "max" | "count") {
            return None;
        }
        // COUNT(*) has no args.
        if name == "count" && spec.args.is_empty() {
            return None;
        }
        // Args must be bare ColumnRefs.
        for arg_id in &spec.args {
            if !matches!(arena.node(*arg_id), ScalarNode::ColumnRef(_)) {
                return None;
            }
            if scalar_expr::contains_non_deterministic_function(arena, *arg_id) {
                return None;
            }
        }
    }

    Some(AggregatePushDownContext {
        original_groupby: aggregate.group_by.clone(),
        original_aggregates: aggregate.aggregates.clone(),
        required_column_refs: collect_required_column_refs(aggregate, arena),
    })
}

fn collect_required_column_refs(
    aggregate: &LogicalAggregateOp,
    arena: &ScalarArena,
) -> Vec<ColumnRefIdentity> {
    let mut out = Vec::new();
    for gb_id in &aggregate.group_by {
        if let Some(identity) = column_ref_qualified(arena, *gb_id) {
            out.push(identity);
        }
    }
    for spec in &aggregate.aggregates {
        for arg_id in &spec.args {
            if let Some(identity) = column_ref_qualified(arena, *arg_id) {
                out.push(identity);
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Top-level collector entry.
#[allow(dead_code)]
pub(crate) fn collect_push_plan(
    aggregate: &LogicalAggregateOp,
    aggregate_input: &OptExpr,
    _table_stats: &HashMap<String, TableStatistics>,
    arena: &ScalarArena,
) -> Option<PushPlan> {
    let ctx = entry_safety_check(aggregate, arena)?;
    let join = match &aggregate_input.op {
        Operator::LogicalJoin(j) => j,
        _ => return None,
    };
    split_at_join(
        join,
        aggregate_input.left(),
        aggregate_input.right(),
        ctx,
        arena,
    )
}

fn split_at_join(
    join: &LogicalJoinOp,
    left: &OptExpr,
    right: &OptExpr,
    ctx: AggregatePushDownContext,
    arena: &ScalarArena,
) -> Option<PushPlan> {
    use crate::sql::common::JoinKind;

    // Step 1: join-shape filter.
    match join.join_type {
        JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter => {}
        _ => return None,
    }
    let cond_id = join.condition?;
    let equi_keys = extract_equi_key_pairs(arena, cond_id);
    if equi_keys.is_empty() {
        return None;
    }

    // Step 2: per-side column visibility.
    let left_qcols = collect_qualified_output_names(left);
    let right_qcols = collect_qualified_output_names(right);

    let side = if ctx
        .required_column_refs
        .iter()
        .all(|c| column_ref_belongs_to_side(c, &left_qcols, &right_qcols))
    {
        Side::Left
    } else if ctx
        .required_column_refs
        .iter()
        .all(|c| column_ref_belongs_to_side(c, &right_qcols, &left_qcols))
    {
        Side::Right
    } else {
        return None;
    };

    // Step 3: outer-join amplifier rejection.
    match (join.join_type, side) {
        (JoinKind::RightOuter, Side::Left) => return None,
        (JoinKind::LeftOuter, Side::Right) => return None,
        _ => {}
    }

    // Step 4: chosen-side subtree MUST be a Scan in v1 (no nested joins,
    // no intermediate Filter/Project on the side).
    let side_subtree = match side {
        Side::Left => left,
        Side::Right => right,
    };
    if !matches!(&side_subtree.op, Operator::LogicalScan(_)) {
        return None;
    }
    // Qualified columns of the chosen side (a bare Scan per Step 4), used to
    // disambiguate equi-keys that share a bare name across sides (`a.k = b.k`).
    let (side_qcols, other_qcols) = match side {
        Side::Left => (&left_qcols, &right_qcols),
        Side::Right => (&right_qcols, &left_qcols),
    };

    // Step 5: partial group-by = original group-by cols on this side
    //         + side-bound equi-keys.
    let partial_groupby: Vec<ScalarId> = ctx
        .original_groupby
        .iter()
        .filter(|gb_id| {
            column_ref_qualified(arena, **gb_id).is_some_and(|identity| {
                column_ref_belongs_to_side(&identity, side_qcols, other_qcols)
            })
        })
        .copied()
        .collect();

    let mut partial_extra_groupby: Vec<ScalarId> = Vec::new();
    for (left_key, right_key) in &equi_keys {
        let candidate_expr = side_bound_equi_key(arena, *left_key, *right_key, side_qcols)?;
        // Check if it's already in partial_groupby by ColumnId, falling back
        // to qualified identity only for synthetic/unset test expressions.
        let already = partial_groupby
            .iter()
            .any(|gb_id| same_column_ref_identity(arena, *gb_id, candidate_expr))
            || partial_extra_groupby
                .iter()
                .any(|gb| same_column_ref_identity(arena, *gb, candidate_expr));
        if !already {
            partial_extra_groupby.push(candidate_expr);
        }
    }

    Some(PushPlan {
        side,
        target_subtree: side_subtree.clone(),
        partial_groupby,
        partial_extra_groupby,
        partial_aggregates: ctx.original_aggregates,
    })
}

fn side_bound_equi_key(
    arena: &ScalarArena,
    left_key: ScalarId,
    right_key: ScalarId,
    side_qcols: &[(Option<String>, String)],
) -> Option<ScalarId> {
    // Disambiguate by QUALIFIED identity (qualifier + name). Bare column names
    // are ambiguous when both join keys share a name (the common `a.k = b.k`
    // case): both would test as "in side" and the key would be dropped. Matching
    // on (qualifier, name) keeps the operand actually bound to the chosen side.
    let left_q = column_ref_qualified(arena, left_key)?;
    let right_q = column_ref_qualified(arena, right_key)?;
    let left_in_side = side_qcols.contains(&left_q);
    let right_in_side = side_qcols.contains(&right_q);
    match (left_in_side, right_in_side) {
        (true, false) => Some(left_key),
        (false, true) => Some(right_key),
        _ => None,
    }
}

fn column_ref_qualified(arena: &ScalarArena, expr: ScalarId) -> Option<ColumnRefIdentity> {
    let ScalarNode::ColumnRef(column_id) = arena.node(expr) else {
        return None;
    };
    let Some(display) = arena.column_display(*column_id) else {
        return Some((None, format!("col{}", column_id.0)));
    };
    Some((display.qualifier.clone(), display.column.clone()))
}

fn same_column_ref_identity(arena: &ScalarArena, a: ScalarId, b: ScalarId) -> bool {
    match (arena.node(a), arena.node(b)) {
        (ScalarNode::ColumnRef(a_id), ScalarNode::ColumnRef(b_id)) => {
            if *a_id != ColumnId::UNSET && *b_id != ColumnId::UNSET {
                a_id == b_id
            } else {
                column_ref_qualified(arena, a) == column_ref_qualified(arena, b)
            }
        }
        _ => false,
    }
}

fn column_ref_belongs_to_side(
    column_ref: &ColumnRefIdentity,
    side_qcols: &[ColumnRefIdentity],
    other_qcols: &[ColumnRefIdentity],
) -> bool {
    match &column_ref.0 {
        Some(_) => side_qcols.contains(column_ref),
        None => side_qcols.contains(column_ref) && !other_qcols.contains(column_ref),
    }
}

/// Qualified output column identities `(qualifier, name)` for a plan subtree.
/// Scans contribute their alias (or table name) as the qualifier so equi-join
/// keys that share a bare name across sides can be told apart.
fn collect_qualified_output_names(plan: &OptExpr) -> Vec<(Option<String>, String)> {
    match &plan.op {
        Operator::LogicalScan(s) => {
            // Each column is acceptable unqualified, by alias, and by table
            // name — the equi-key operand may be written any of these ways. A
            // `Some(qualifier)` operand only matches the side whose alias/table
            // equals it, so `a.k` vs `b.k` are told apart; a bare operand
            // matches by name via the unqualified entry.
            let mut out = Vec::new();
            for c in &s.columns {
                let name = c.name.clone();
                out.push((None, name.clone()));
                if let Some(alias) = &s.alias {
                    out.push((Some(alias.clone()), name.clone()));
                }
                out.push((Some(s.table.name.clone()), name));
            }
            out
        }
        Operator::LogicalFilter(_) => collect_qualified_output_names(plan.unary_input()),
        Operator::LogicalProject(p) => p
            .items
            .iter()
            .map(|i| (None, i.output_name.clone()))
            .collect(),
        Operator::LogicalJoin(_) => {
            let mut l = collect_qualified_output_names(plan.left());
            l.extend(collect_qualified_output_names(plan.right()));
            l
        }
        Operator::LogicalAggregate(a) => a
            .output_columns
            .iter()
            .map(|c| (None, c.name.clone()))
            .collect(),
        _ => Vec::new(),
    }
}

fn extract_equi_key_pairs(arena: &ScalarArena, cond: ScalarId) -> Vec<(ScalarId, ScalarId)> {
    let mut out = Vec::new();
    walk_and_collect_equi(arena, cond, &mut out);
    out
}

fn walk_and_collect_equi(arena: &ScalarArena, expr: ScalarId, out: &mut Vec<(ScalarId, ScalarId)>) {
    match arena.node(expr) {
        ScalarNode::Nested(inner) => walk_and_collect_equi(arena, *inner, out),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if matches!(arena.node(*left), ScalarNode::ColumnRef(_))
                && matches!(arena.node(*right), ScalarNode::ColumnRef(_))
            {
                out.push((*left, *right));
            }
        }
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            walk_and_collect_equi(arena, *left, out);
            walk_and_collect_equi(arena, *right, out);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        AggStage, FilterOp, LogicalAggregateOp, LogicalJoinOp, Operator, ProjectOp,
        ScalarAggregateSpec, ScalarProjectItem,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::scalar::ScalarArena;

    use crate::sql::planner::optimizer_bridge::scalar::{intern_typed, materialize};
    use arrow::datatypes::DataType;

    fn make_arena() -> ScalarArena {
        ScalarArena::new()
    }

    fn make_ctx(arena: ScalarArena) -> RewriteContext {
        let mut ctx = RewriteContext::for_query(std::iter::empty());
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        ctx
    }

    /// Compute a stable non-zero test column ID from a (qualifier, name) pair.
    /// Uses a FNV-like hash to avoid the ColumnId::UNSET sentinel (0).
    fn test_col_id(qualifier: Option<&str>, name: &str) -> ColumnId {
        let mut hash: u32 = 2166136261;
        for b in qualifier
            .unwrap_or("")
            .bytes()
            .chain(std::iter::once(b'.'))
            .chain(name.bytes())
        {
            hash ^= b as u32;
            hash = hash.wrapping_mul(16777619);
        }
        // Ensure non-zero (UNSET is 0).
        let id = if hash == 0 { 1 } else { hash };
        ColumnId::new_for_test(id)
    }

    fn col_ref_typed(name: &str, ty: DataType) -> crate::sql::analysis::TypedExpr {
        crate::sql::analysis::TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(None, name),
                qualifier: None,
                column: name.into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn qualified_col_ref_typed(
        qualifier: &str,
        name: &str,
        ty: DataType,
    ) -> crate::sql::analysis::TypedExpr {
        crate::sql::analysis::TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(Some(qualifier), name),
                qualifier: Some(qualifier.into()),
                column: name.into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn make_agg(
        group_by_typed: Vec<crate::sql::analysis::TypedExpr>,
        agg_specs: Vec<ScalarAggregateSpec>,
        is_split: bool,
        arena: &mut ScalarArena,
    ) -> LogicalAggregateOp {
        let group_by = group_by_typed
            .iter()
            .map(|e| intern_typed(arena, e))
            .collect();
        LogicalAggregateOp::staged(
            AggStage::Single,
            group_by,
            agg_specs.clone(),
            vec![],
            vec![false; agg_specs.len()],
            is_split,
        )
    }

    fn sum_spec(col: &str, arena: &mut ScalarArena) -> ScalarAggregateSpec {
        let arg = col_ref_typed(col, DataType::Int64);
        ScalarAggregateSpec {
            name: "sum".into(),
            args: vec![intern_typed(arena, &arg)],
            distinct: false,
            order_by: vec![],
        }
    }

    fn scan_opt(cols: &[(&str, DataType)]) -> OptExpr {
        scan_opt_with_alias(None, cols)
    }

    fn scan_opt_with_alias(alias: Option<&str>, cols: &[(&str, DataType)]) -> OptExpr {
        use crate::sql::optimizer::operator::ScanOp;
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: alias.map(str::to_string),
            columns: cols
                .iter()
                .map(|(n, ty)| OutputColumn {
                    // Use the same stable ID that col_ref_typed would assign so
                    // the collector can match group_by ColumnIds to scan columns.
                    column_id: test_col_id(alias, n),
                    name: (*n).into(),
                    data_type: ty.clone(),
                    nullable: false,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn join_opt(
        join_type: JoinKind,
        condition: Option<crate::sql::analysis::TypedExpr>,
        left: OptExpr,
        right: OptExpr,
        arena: &mut ScalarArena,
    ) -> OptExpr {
        let cond_id = condition.as_ref().map(|c| intern_typed(arena, c));
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type,
                condition: cond_id,
            }),
            vec![left, right],
        )
    }

    fn agg_opt(
        input: OptExpr,
        group_by_typed: Vec<crate::sql::analysis::TypedExpr>,
        agg_specs: Vec<ScalarAggregateSpec>,
        arena: &mut ScalarArena,
    ) -> OptExpr {
        let group_by = group_by_typed
            .iter()
            .map(|e| intern_typed(arena, e))
            .collect();
        let is_merge = vec![false; agg_specs.len()];
        OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Single,
                group_by,
                agg_specs,
                vec![],
                is_merge,
                false,
            )),
            vec![input],
        )
    }

    fn eq_typed(a: &str, b: &str) -> crate::sql::analysis::TypedExpr {
        use crate::sql::analysis::BinOp;
        crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_typed(a, DataType::Int64)),
                op: BinOp::Eq,
                right: Box::new(col_ref_typed(b, DataType::Int64)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn collect_test_push_plan(
        agg_plan: &OptExpr,
        arena: &ScalarArena,
    ) -> Option<super::super::context::PushPlan> {
        let Operator::LogicalAggregate(agg) = &agg_plan.op else {
            panic!("expected Aggregate test plan");
        };
        collect_push_plan(agg, agg_plan.unary_input(), &HashMap::new(), arena)
    }

    #[test]
    fn rejects_empty_groupby() {
        let mut arena = make_arena();
        let agg = make_agg(vec![], vec![sum_spec("v", &mut arena)], false, &mut arena);
        assert!(entry_safety_check(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_distinct_aggregate() {
        let mut arena = make_arena();
        let mut spec = sum_spec("v", &mut arena);
        spec.distinct = true;
        let agg = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            false,
            &mut arena,
        );
        assert!(entry_safety_check(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_order_sensitive_aggregate() {
        let mut arena = make_arena();
        let mut spec = sum_spec("v", &mut arena);
        spec.order_by.push(crate::sql::optimizer::scalar::SortKey {
            expr: intern_typed(&mut arena, &col_ref_typed("v", DataType::Int64)),
            asc: true,
            nulls_first: false,
            display: None,
        });
        let agg = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            false,
            &mut arena,
        );
        assert!(entry_safety_check(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_count_star() {
        let mut arena = make_arena();
        let count_star = ScalarAggregateSpec {
            name: "count".into(),
            args: vec![],
            distinct: false,
            order_by: vec![],
        };
        let agg = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![count_star],
            false,
            &mut arena,
        );
        assert!(entry_safety_check(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_avg_function() {
        let mut arena = make_arena();
        let avg_arg = intern_typed(&mut arena, &col_ref_typed("v", DataType::Int64));
        let avg = ScalarAggregateSpec {
            name: "avg".into(),
            args: vec![avg_arg],
            distinct: false,
            order_by: vec![],
        };
        let agg = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![avg],
            false,
            &mut arena,
        );
        assert!(entry_safety_check(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_aggregate_expr_not_columnref() {
        let mut arena = make_arena();
        use crate::sql::analysis::BinOp;
        let non_col = crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_typed("a", DataType::Int64)),
                op: BinOp::Add,
                right: Box::new(col_ref_typed("b", DataType::Int64)),
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        let arg_id = intern_typed(&mut arena, &non_col);
        let spec = ScalarAggregateSpec {
            name: "sum".into(),
            args: vec![arg_id],
            distinct: false,
            order_by: vec![],
        };
        let agg = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            false,
            &mut arena,
        );
        assert!(entry_safety_check(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_nondeterministic_arg() {
        let mut arena = make_arena();
        let rand_expr = crate::sql::analysis::TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "rand".into(),
                args: vec![],
                distinct: false,
            },
            data_type: DataType::Float64,
            nullable: false,
        };
        let arg_id = intern_typed(&mut arena, &rand_expr);
        let spec = ScalarAggregateSpec {
            name: "sum".into(),
            args: vec![arg_id],
            distinct: false,
            order_by: vec![],
        };
        let agg = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            false,
            &mut arena,
        );
        assert!(entry_safety_check(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_already_pushed_aggregate() {
        let mut arena = make_arena();
        let agg = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![sum_spec("v", &mut arena)],
            true,
            &mut arena,
        );
        assert!(entry_safety_check(&agg, &arena).is_none());
    }

    #[test]
    fn accepts_inner_join_candidate() {
        let mut arena = make_arena();
        let agg = make_agg(
            vec![col_ref_typed("k", DataType::Int64)],
            vec![sum_spec("v", &mut arena)],
            false,
            &mut arena,
        );
        let ctx = entry_safety_check(&agg, &arena).expect("should pass entry checks");
        assert_eq!(ctx.original_groupby.len(), 1);
        assert_eq!(ctx.original_aggregates.len(), 1);
        assert!(ctx.required_column_refs.contains(&(None, "k".to_string())));
        assert!(ctx.required_column_refs.contains(&(None, "v".to_string())));
    }

    #[test]
    fn rejects_when_input_is_scan_directly() {
        let mut arena = make_arena();
        let scan = scan_opt(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            scan,
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        assert!(collect_test_push_plan(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_when_input_is_filter_above_join() {
        let mut arena = make_arena();
        let scan_a = scan_opt(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let scan_b = scan_opt(&[("k", DataType::Int64)]);
        let join = join_opt(
            JoinKind::Inner,
            Some(col_ref_typed("k", DataType::Boolean)),
            scan_a,
            scan_b,
            &mut arena,
        );
        let filter_pred = intern_typed(&mut arena, &col_ref_typed("k", DataType::Boolean));
        let filter = OptExpr::new(
            Operator::LogicalFilter(FilterOp {
                predicate: filter_pred,
            }),
            vec![join],
        );
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            filter,
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        assert!(collect_test_push_plan(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_when_input_is_project_above_join() {
        let mut arena = make_arena();
        let scan_a = scan_opt(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let scan_b = scan_opt(&[("k", DataType::Int64)]);
        let join = join_opt(
            JoinKind::Inner,
            Some(col_ref_typed("k", DataType::Boolean)),
            scan_a,
            scan_b,
            &mut arena,
        );
        let proj_expr = intern_typed(&mut arena, &col_ref_typed("k", DataType::Int64));
        let project = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    expr: proj_expr,
                    output_name: "k".into(),
                    output_column_id: ColumnId::UNSET,
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            vec![join],
        );
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            project,
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        assert!(collect_test_push_plan(&agg, &arena).is_none());
    }

    #[test]
    fn pushes_sum_under_inner_join_to_left() {
        let mut arena = make_arena();
        let a = scan_opt(&[("lk", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan_opt(&[("rk", DataType::Int64)]);
        let join = join_opt(
            JoinKind::Inner,
            Some(eq_typed("lk", "rk")),
            a,
            b,
            &mut arena,
        );
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            join,
            vec![col_ref_typed("lk", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        let plan = collect_test_push_plan(&agg, &arena).expect("should push to left");
        assert_eq!(plan.side, super::super::context::Side::Left);
        assert!(matches!(&plan.target_subtree.op, Operator::LogicalScan(_)));
    }

    #[test]
    fn adds_side_bound_join_key_to_extra_partial_groupby() {
        let mut arena = make_arena();
        let cs = scan_opt_with_alias(
            Some("cs"),
            &[
                ("cs_call_center_sk", DataType::Int64),
                ("cs_sold_date_sk", DataType::Int64),
                ("cs_sales_price", DataType::Int64),
            ],
        );
        let d = scan_opt_with_alias(Some("d"), &[("d_date_sk", DataType::Int64)]);

        use crate::sql::analysis::BinOp;
        let cond = crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(qualified_col_ref_typed(
                    "cs",
                    "cs_sold_date_sk",
                    DataType::Int64,
                )),
                op: BinOp::Eq,
                right: Box::new(qualified_col_ref_typed("d", "d_date_sk", DataType::Int64)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let join = join_opt(JoinKind::Inner, Some(cond), cs, d, &mut arena);
        let sum_arg = intern_typed(
            &mut arena,
            &qualified_col_ref_typed("cs", "cs_sales_price", DataType::Int64),
        );
        let agg = agg_opt(
            join,
            vec![qualified_col_ref_typed(
                "cs",
                "cs_call_center_sk",
                DataType::Int64,
            )],
            vec![ScalarAggregateSpec {
                name: "sum".into(),
                args: vec![sum_arg],
                distinct: false,
                order_by: vec![],
            }],
            &mut arena,
        );

        let plan = collect_test_push_plan(&agg, &arena).expect("should push to catalog_sales");
        assert_eq!(plan.side, super::super::context::Side::Left);
        assert!(plan.partial_groupby.iter().any(|id| {
            let expr = materialize(&arena, *id);
            matches!(&expr.kind, ExprKind::ColumnRef { column, .. } if column == "cs_call_center_sk")
        }));
        assert!(plan.partial_extra_groupby.iter().any(|id| {
            let expr = materialize(&arena, *id);
            matches!(&expr.kind, ExprKind::ColumnRef { column, column_id, .. }
                if column == "cs_sold_date_sk"
                    && *column_id == test_col_id(Some("cs"), "cs_sold_date_sk"))
        }));
    }

    #[test]
    fn orients_reversed_join_key_to_target_side() {
        let mut arena = make_arena();
        let a = scan_opt(&[("lk", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan_opt(&[("rk", DataType::Int64)]);
        let join = join_opt(
            JoinKind::Inner,
            Some(eq_typed("rk", "lk")),
            a,
            b,
            &mut arena,
        );
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            join,
            vec![col_ref_typed("lk", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        let plan = collect_test_push_plan(&agg, &arena).expect("should push to left");
        // group columns should contain lk but not rk
        let has_lk = plan.partial_groupby.iter().any(|id| {
            let e = materialize(&arena, *id);
            matches!(&e.kind, ExprKind::ColumnRef { column, .. } if column == "lk")
        });
        let has_rk = plan.partial_groupby.iter().any(|id| {
            let e = materialize(&arena, *id);
            matches!(&e.kind, ExprKind::ColumnRef { column, .. } if column == "rk")
        });
        assert!(has_lk);
        assert!(!has_rk);
    }

    #[test]
    fn rejects_outer_join_amplifier_side() {
        let mut arena = make_arena();
        let a = scan_opt(&[("lk", DataType::Int64)]);
        let b = scan_opt(&[("rk", DataType::Int64), ("v", DataType::Int64)]);
        let join = join_opt(
            JoinKind::LeftOuter,
            Some(eq_typed("lk", "rk")),
            a,
            b,
            &mut arena,
        );
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            join,
            vec![col_ref_typed("rk", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        assert!(collect_test_push_plan(&agg, &arena).is_none());
    }

    #[test]
    fn accepts_left_outer_when_agg_on_preserved_left() {
        let mut arena = make_arena();
        let a = scan_opt(&[("lk", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan_opt(&[("rk", DataType::Int64)]);
        let join = join_opt(
            JoinKind::LeftOuter,
            Some(eq_typed("rk", "lk")),
            a,
            b,
            &mut arena,
        );
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            join,
            vec![col_ref_typed("lk", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        let plan = collect_test_push_plan(&agg, &arena).expect("push to preserved left");
        assert!(matches!(&plan.target_subtree.op, Operator::LogicalScan(_)));
    }

    #[test]
    fn rejects_cross_join() {
        let mut arena = make_arena();
        let a = scan_opt(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan_opt(&[("x", DataType::Int64)]);
        let join = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Cross,
                condition: None,
            }),
            vec![a, b],
        );
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            join,
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        assert!(collect_test_push_plan(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_aggregate_columns_across_sides() {
        let mut arena = make_arena();
        let a = scan_opt(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan_opt(&[("k", DataType::Int64), ("w", DataType::Int64)]);
        let join = join_opt(JoinKind::Inner, Some(eq_typed("k", "k")), a, b, &mut arena);
        let spec_v = sum_spec("v", &mut arena);
        let spec_w = sum_spec("w", &mut arena);
        let agg = agg_opt(
            join,
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec_v, spec_w],
            &mut arena,
        );
        assert!(collect_test_push_plan(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_qualified_required_columns_split_across_same_named_sides() {
        let mut arena = make_arena();
        let a = scan_opt_with_alias(
            Some("l"),
            &[
                ("c0", DataType::Int64),
                ("c1", DataType::Utf8),
                ("c2", DataType::Utf8),
                ("c3", DataType::Int64),
            ],
        );
        let b = scan_opt_with_alias(
            Some("r"),
            &[
                ("c0", DataType::Int64),
                ("c1", DataType::Utf8),
                ("c2", DataType::Utf8),
                ("c3", DataType::Int64),
            ],
        );

        use crate::sql::analysis::BinOp;
        let cond = crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(crate::sql::analysis::TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(qualified_col_ref_typed("l", "c0", DataType::Int64)),
                        op: BinOp::Eq,
                        right: Box::new(qualified_col_ref_typed("r", "c0", DataType::Int64)),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
                op: BinOp::And,
                right: Box::new(crate::sql::analysis::TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(qualified_col_ref_typed("l", "c1", DataType::Utf8)),
                        op: BinOp::Eq,
                        right: Box::new(qualified_col_ref_typed("r", "c1", DataType::Utf8)),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let join = join_opt(JoinKind::Inner, Some(cond), a, b, &mut arena);

        let count_arg = intern_typed(
            &mut arena,
            &qualified_col_ref_typed("l", "c0", DataType::Int64),
        );
        let count_spec = ScalarAggregateSpec {
            name: "count".into(),
            args: vec![count_arg],
            distinct: false,
            order_by: vec![],
        };

        let agg = agg_opt(
            join,
            vec![
                qualified_col_ref_typed("l", "c0", DataType::Int64),
                qualified_col_ref_typed("r", "c1", DataType::Utf8),
                qualified_col_ref_typed("r", "c2", DataType::Utf8),
                qualified_col_ref_typed("r", "c3", DataType::Int64),
            ],
            vec![count_spec],
            &mut arena,
        );
        assert!(collect_test_push_plan(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_semi_anti_join() {
        let mut arena = make_arena();
        let a = scan_opt(&[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan_opt(&[("k", DataType::Int64)]);
        let join = join_opt(
            JoinKind::LeftSemi,
            Some(eq_typed("k", "k")),
            a,
            b,
            &mut arena,
        );
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            join,
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        assert!(collect_test_push_plan(&agg, &arena).is_none());
    }

    #[test]
    fn rejects_nested_join_on_target_side() {
        let mut arena = make_arena();
        let inner_join = join_opt(
            JoinKind::Inner,
            Some(eq_typed("k", "k")),
            scan_opt(&[("k", DataType::Int64), ("v", DataType::Int64)]),
            scan_opt(&[("k", DataType::Int64)]),
            &mut arena,
        );
        let outer_join = join_opt(
            JoinKind::Inner,
            Some(eq_typed("k", "k")),
            inner_join,
            scan_opt(&[("k", DataType::Int64)]),
            &mut arena,
        );
        let spec = sum_spec("v", &mut arena);
        let agg = agg_opt(
            outer_join,
            vec![col_ref_typed("k", DataType::Int64)],
            vec![spec],
            &mut arena,
        );
        assert!(collect_test_push_plan(&agg, &arena).is_none());
    }
}
