//! Aggregate rollup decision for SPJG-MV rewrites.
//! StarRocks counterpart: AggregatedMaterializedViewRewriter +
//! AggregateFunctionRollupUtils.

use std::collections::HashMap;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::ScalarAggregateSpec;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId};

use super::column_mapping::{NormExpr, normalize};
use super::descriptor::{SpjgAggregate, SpjgOutput, SpjgOutputExpr};

#[derive(Debug)]
pub(crate) enum RollupKind {
    /// Query group-by == MV group-by: each query aggregate maps 1:1 to an
    /// MV output column, no re-aggregation.
    Direct,
    /// Query group-by ⊂ MV group-by: re-aggregate MV rows.
    Rollup,
}

#[derive(Debug)]
pub(crate) struct RollupItem {
    /// Index into the MV outputs (the materialized aggregate column to read).
    pub mv_output_index: usize,
    /// Rollup function name ("sum"/"min"/"max"); for Direct this is unused.
    pub rollup_fn: &'static str,
    /// True when the query aggregate is COUNT-like and the query has no
    /// group-by: SUM over an empty input yields NULL where COUNT must
    /// yield 0, so the result needs COALESCE(_, 0).
    pub needs_coalesce: bool,
}

#[derive(Debug)]
pub(crate) struct RollupPlan {
    pub kind: RollupKind,
    /// One entry per query aggregate, in order.
    pub items: Vec<RollupItem>,
}

fn norm_agg(
    arena: &ScalarArena,
    call: &ScalarAggregateSpec,
    base_names: &HashMap<ColumnId, String>,
) -> Option<NormExpr> {
    // `order_by` is intentionally NOT part of the key: every aggregate on the
    // current whitelist (sum/min/max/count) is order-insensitive, and SPJG-MV
    // aggregate calls carry no order_by. If an order-sensitive aggregate
    // (e.g. group_concat / array_agg) is ever whitelisted, order_by MUST be
    // folded into this key, or two differently-ordered calls would wrongly
    // match.
    Some(NormExpr::Call {
        name: format!("agg:{}", call.name.to_ascii_lowercase()),
        distinct: call.distinct,
        args: call
            .args
            .iter()
            .map(|arg| normalize(arena, *arg, base_names))
            .collect::<Option<Vec<_>>>()?,
    })
}

/// Decide whether (and how) the query aggregate can be answered from the MV.
/// Returns None when not rewritable.
pub(crate) fn plan_rollup(
    query_group_by: &[ScalarId],
    query_aggregates: &[ScalarAggregateSpec],
    query_arena: &ScalarArena,
    query_base_names: &HashMap<ColumnId, String>,
    mv_agg: &SpjgAggregate,
    mv_outputs: &[SpjgOutput],
    mv_arena: &ScalarArena,
    mv_base_names: &HashMap<ColumnId, String>,
) -> Option<RollupPlan> {
    // Normalized group-key sets.
    let q_keys: Vec<NormExpr> = query_group_by
        .iter()
        .map(|expr| normalize(query_arena, *expr, query_base_names))
        .collect::<Option<Vec<_>>>()?;
    let m_keys: Vec<NormExpr> = mv_agg
        .group_by
        .iter()
        .map(|expr| normalize(mv_arena, *expr, mv_base_names))
        .collect::<Option<Vec<_>>>()?;
    if !q_keys.iter().all(|k| m_keys.contains(k)) {
        return None; // query groups by something the MV did not preserve
    }
    let equal = q_keys.len() == m_keys.len() && m_keys.iter().all(|k| q_keys.contains(k));

    // MV aggregate outputs by normalized call.
    let mut mv_agg_by_norm: HashMap<NormExpr, usize> = HashMap::new();
    for (i, out) in mv_outputs.iter().enumerate() {
        if let SpjgOutputExpr::Aggregate(call) = &out.expr
            && let Some(n) = norm_agg(mv_arena, call, mv_base_names)
        {
            mv_agg_by_norm.insert(n, i);
        }
    }

    let scalar_query = query_group_by.is_empty();
    let mut items = Vec::with_capacity(query_aggregates.len());
    for q in query_aggregates {
        if q.distinct {
            return None; // DISTINCT aggregates never rewrite onto SPJG MVs
        }
        let qn = norm_agg(query_arena, q, query_base_names)?;
        let mv_idx = *mv_agg_by_norm.get(&qn)?; // exact same call materialized?
        if equal {
            items.push(RollupItem {
                mv_output_index: mv_idx,
                rollup_fn: "",
                needs_coalesce: false,
            });
            continue;
        }
        // Rollup whitelist.
        let (rollup_fn, is_count) = match q.name.to_ascii_lowercase().as_str() {
            "sum" => ("sum", false),
            "min" => ("min", false),
            "max" => ("max", false),
            "count" => ("sum", true),
            _ => return None, // includes avg and everything exotic
        };
        items.push(RollupItem {
            mv_output_index: mv_idx,
            rollup_fn,
            needs_coalesce: is_count && scalar_query,
        });
    }

    Some(RollupPlan {
        kind: if equal {
            RollupKind::Direct
        } else {
            RollupKind::Rollup
        },
        items,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::ScalarAggregateSpec;
    use crate::sql::optimizer::scalar::{ScalarArena, ScalarId};

    use crate::sql::planner::optimizer_bridge::scalar::intern_aggregate_call;
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use crate::sql::planner::plan::AggregateCall;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    use super::super::descriptor::{SpjgAggregate, SpjgOutput, SpjgOutputExpr};

    // --- construction helpers (mirror sibling test modules) ---

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

    fn names(pairs: &[(u32, &str)]) -> HashMap<ColumnId, String> {
        pairs
            .iter()
            .map(|(id, n)| (ColumnId(*id), n.to_string()))
            .collect()
    }

    /// Build an aggregate call over `args` with the given name/distinct.
    fn agg(name: &str, args: Vec<TypedExpr>, distinct: bool) -> AggregateCall {
        AggregateCall {
            name: name.to_string(),
            args,
            distinct,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::UNSET,
        }
    }

    /// Wrap an aggregate call as a materialized MV output column.
    fn scalar_exprs(arena: &mut ScalarArena, exprs: Vec<TypedExpr>) -> Vec<ScalarId> {
        exprs.iter().map(|expr| intern_typed(arena, expr)).collect()
    }

    fn scalar_aggs(arena: &mut ScalarArena, calls: Vec<AggregateCall>) -> Vec<ScalarAggregateSpec> {
        calls
            .iter()
            .map(|call| intern_aggregate_call(arena, call))
            .collect()
    }

    fn agg_out(out: &OutputColumn, call: AggregateCall, arena: &mut ScalarArena) -> SpjgOutput {
        SpjgOutput {
            name: out.name.clone(),
            column_id: out.column_id,
            expr: SpjgOutputExpr::Aggregate(intern_aggregate_call(arena, &call)),
        }
    }

    /// Wrap a dimension expr as an MV output column.
    fn dim_out(out: &OutputColumn, expr: TypedExpr, arena: &mut ScalarArena) -> SpjgOutput {
        SpjgOutput {
            name: out.name.clone(),
            column_id: out.column_id,
            expr: SpjgOutputExpr::Dimension(intern_typed(arena, &expr)),
        }
    }

    #[test]
    fn rollup_plan_for_groupby_subset() {
        // MV: GROUP BY a, b -> [a, b, sum(v) as s, count(*) as c]
        let mv_a = col(1, "a");
        let mv_b = col(2, "b");
        let mv_v = col(3, "v");
        let mv_s = col(11, "s");
        let mv_c = col(12, "c");
        let mv_names = names(&[(1, "a"), (2, "b"), (3, "v")]);
        let mut mv_arena = ScalarArena::new();

        let mv_agg = SpjgAggregate {
            group_by: scalar_exprs(&mut mv_arena, vec![col_ref(&mv_a), col_ref(&mv_b)]),
        };
        let mv_outputs = vec![
            dim_out(&col(101, "a"), col_ref(&mv_a), &mut mv_arena),
            dim_out(&col(102, "b"), col_ref(&mv_b), &mut mv_arena),
            agg_out(
                &mv_s,
                agg("sum", vec![col_ref(&mv_v)], false),
                &mut mv_arena,
            ),
            agg_out(&mv_c, agg("count", vec![], false), &mut mv_arena),
        ];

        // query: SELECT a, sum(v), count(*) GROUP BY a (subset of {a, b}).
        let q_a = col(21, "a");
        let q_v = col(23, "v");
        let q_names = names(&[(21, "a"), (23, "v")]);
        let mut q_arena = ScalarArena::new();
        let q_group_by = scalar_exprs(&mut q_arena, vec![col_ref(&q_a)]);
        let q_aggs = scalar_aggs(
            &mut q_arena,
            vec![
                agg("sum", vec![col_ref(&q_v)], false),
                agg("count", vec![], false),
            ],
        );

        let plan = plan_rollup(
            &q_group_by,
            &q_aggs,
            &q_arena,
            &q_names,
            &mv_agg,
            &mv_outputs,
            &mv_arena,
            &mv_names,
        )
        .expect("subset rollup must be rewritable");

        assert!(matches!(plan.kind, RollupKind::Rollup));
        assert_eq!(plan.items.len(), 2);
        // sum(v) rolls up over MV output `s` (index 2) with rollup_fn=sum.
        assert_eq!(plan.items[0].mv_output_index, 2);
        assert_eq!(plan.items[0].rollup_fn, "sum");
        assert!(!plan.items[0].needs_coalesce);
        // count(*) rolls up over MV output `c` (index 3) with rollup_fn=sum.
        assert_eq!(plan.items[1].mv_output_index, 3);
        assert_eq!(plan.items[1].rollup_fn, "sum");
        // query has a group-by, so no scalar-count coalesce needed.
        assert!(!plan.items[1].needs_coalesce);
    }

    #[test]
    fn direct_mapping_when_groupby_equal() {
        // MV: GROUP BY a -> [a, sum(v) as s, count(*) as c]
        let mv_a = col(1, "a");
        let mv_v = col(3, "v");
        let mv_s = col(11, "s");
        let mv_c = col(12, "c");
        let mv_names = names(&[(1, "a"), (3, "v")]);
        let mut mv_arena = ScalarArena::new();

        let mv_agg = SpjgAggregate {
            group_by: scalar_exprs(&mut mv_arena, vec![col_ref(&mv_a)]),
        };
        let mv_outputs = vec![
            dim_out(&col(101, "a"), col_ref(&mv_a), &mut mv_arena),
            agg_out(
                &mv_s,
                agg("sum", vec![col_ref(&mv_v)], false),
                &mut mv_arena,
            ),
            agg_out(&mv_c, agg("count", vec![], false), &mut mv_arena),
        ];

        // query: SELECT a, sum(v), count(*) GROUP BY a (== MV group-by).
        let q_a = col(21, "a");
        let q_v = col(23, "v");
        let q_names = names(&[(21, "a"), (23, "v")]);
        let mut q_arena = ScalarArena::new();
        let q_group_by = scalar_exprs(&mut q_arena, vec![col_ref(&q_a)]);
        let q_aggs = scalar_aggs(
            &mut q_arena,
            vec![
                agg("sum", vec![col_ref(&q_v)], false),
                agg("count", vec![], false),
            ],
        );

        let plan = plan_rollup(
            &q_group_by,
            &q_aggs,
            &q_arena,
            &q_names,
            &mv_agg,
            &mv_outputs,
            &mv_arena,
            &mv_names,
        )
        .expect("equal group-by must map directly");

        assert!(matches!(plan.kind, RollupKind::Direct));
        assert_eq!(plan.items.len(), 2);
        // 1:1 mapping: sum(v) -> MV output s (index 1), count(*) -> c (index 2).
        assert_eq!(plan.items[0].mv_output_index, 1);
        assert_eq!(plan.items[1].mv_output_index, 2);
        // Direct mapping does not re-aggregate, so no coalesce.
        assert!(!plan.items[0].needs_coalesce);
        assert!(!plan.items[1].needs_coalesce);
    }

    #[test]
    fn distinct_agg_rejected() {
        // MV: GROUP BY a -> [a, count(distinct x) as d] — even if the MV
        // materialized a distinct aggregate, a query DISTINCT aggregate must
        // never rewrite onto an SPJG MV.
        let mv_a = col(1, "a");
        let mv_x = col(4, "x");
        let mv_d = col(13, "d");
        let mv_names = names(&[(1, "a"), (4, "x")]);
        let mut mv_arena = ScalarArena::new();

        let mv_agg = SpjgAggregate {
            group_by: scalar_exprs(&mut mv_arena, vec![col_ref(&mv_a)]),
        };
        let mv_outputs = vec![
            dim_out(&col(101, "a"), col_ref(&mv_a), &mut mv_arena),
            agg_out(
                &mv_d,
                agg("count", vec![col_ref(&mv_x)], true),
                &mut mv_arena,
            ),
        ];

        // query: SELECT a, count(distinct x) GROUP BY a (== MV group-by).
        let q_a = col(21, "a");
        let q_x = col(24, "x");
        let q_names = names(&[(21, "a"), (24, "x")]);
        let mut q_arena = ScalarArena::new();
        let q_group_by = scalar_exprs(&mut q_arena, vec![col_ref(&q_a)]);
        let q_aggs = scalar_aggs(&mut q_arena, vec![agg("count", vec![col_ref(&q_x)], true)]);

        assert!(
            plan_rollup(
                &q_group_by,
                &q_aggs,
                &q_arena,
                &q_names,
                &mv_agg,
                &mv_outputs,
                &mv_arena,
                &mv_names,
            )
            .is_none(),
            "DISTINCT query aggregate must not rewrite"
        );
    }

    #[test]
    fn avg_rejected_for_rollup_but_direct_ok() {
        // MV: GROUP BY a, b -> [a, b, avg(v) as m]
        let mv_a = col(1, "a");
        let mv_b = col(2, "b");
        let mv_v = col(3, "v");
        let mv_m = col(14, "m");
        let mv_names = names(&[(1, "a"), (2, "b"), (3, "v")]);
        let mut mv_arena = ScalarArena::new();

        let mv_agg = SpjgAggregate {
            group_by: scalar_exprs(&mut mv_arena, vec![col_ref(&mv_a), col_ref(&mv_b)]),
        };
        let mv_outputs = vec![
            dim_out(&col(101, "a"), col_ref(&mv_a), &mut mv_arena),
            dim_out(&col(102, "b"), col_ref(&mv_b), &mut mv_arena),
            agg_out(
                &mv_m,
                agg("avg", vec![col_ref(&mv_v)], false),
                &mut mv_arena,
            ),
        ];

        // Subset query: GROUP BY a only -> avg cannot be rolled up -> None.
        let q_a = col(21, "a");
        let q_v = col(23, "v");
        let q_names = names(&[(21, "a"), (23, "v")]);
        let mut q_arena = ScalarArena::new();
        let q_group_by_subset = scalar_exprs(&mut q_arena, vec![col_ref(&q_a)]);
        let q_aggs = scalar_aggs(&mut q_arena, vec![agg("avg", vec![col_ref(&q_v)], false)]);
        assert!(
            plan_rollup(
                &q_group_by_subset,
                &q_aggs,
                &q_arena,
                &q_names,
                &mv_agg,
                &mv_outputs,
                &mv_arena,
                &mv_names,
            )
            .is_none(),
            "avg in subset rollup must be rejected"
        );

        // Equal-group-by query: GROUP BY a, b with the SAME avg call -> Direct
        // mapping is allowed because no re-aggregation happens.
        let q_b = col(22, "b");
        let q_names_eq = names(&[(21, "a"), (22, "b"), (23, "v")]);
        let q_group_by_equal = scalar_exprs(&mut q_arena, vec![col_ref(&q_a), col_ref(&q_b)]);
        let plan = plan_rollup(
            &q_group_by_equal,
            &q_aggs,
            &q_arena,
            &q_names_eq,
            &mv_agg,
            &mv_outputs,
            &mv_arena,
            &mv_names,
        )
        .expect("equal group-by avg must map directly");
        assert!(matches!(plan.kind, RollupKind::Direct));
        assert_eq!(plan.items.len(), 1);
        assert_eq!(plan.items[0].mv_output_index, 2);
    }

    #[test]
    fn scalar_count_flags_coalesce() {
        // MV: GROUP BY a -> [a, count(*) as c]
        let mv_a = col(1, "a");
        let mv_c = col(12, "c");
        let mv_names = names(&[(1, "a")]);
        let mut mv_arena = ScalarArena::new();

        let mv_agg = SpjgAggregate {
            group_by: scalar_exprs(&mut mv_arena, vec![col_ref(&mv_a)]),
        };
        let mv_outputs = vec![
            dim_out(&col(101, "a"), col_ref(&mv_a), &mut mv_arena),
            agg_out(&mv_c, agg("count", vec![], false), &mut mv_arena),
        ];

        // query: SELECT count(*) with NO group-by (scalar). Group-by {} is a
        // subset of {a}, so this is a Rollup, and SUM over an empty MV result
        // is NULL where COUNT must be 0 -> needs_coalesce.
        let q_names = names(&[]);
        let mut q_arena = ScalarArena::new();
        let q_aggs = scalar_aggs(&mut q_arena, vec![agg("count", vec![], false)]);

        let plan = plan_rollup(
            &[],
            &q_aggs,
            &q_arena,
            &q_names,
            &mv_agg,
            &mv_outputs,
            &mv_arena,
            &mv_names,
        )
        .expect("scalar count rollup must be rewritable");
        assert!(matches!(plan.kind, RollupKind::Rollup));
        assert_eq!(plan.items.len(), 1);
        assert_eq!(plan.items[0].rollup_fn, "sum");
        assert!(
            plan.items[0].needs_coalesce,
            "scalar COUNT must carry needs_coalesce"
        );
    }
}
