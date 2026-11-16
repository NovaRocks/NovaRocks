// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Aggregate rollup decision for SPJG-MV rewrites.
//! StarRocks counterpart: AggregatedMaterializedViewRewriter +
//! AggregateFunctionRollupUtils.

use std::collections::HashMap;

use crate::column_id::ColumnId;
use crate::compiler::SqlCompileError;
use crate::optimizer::operator::ScalarAggregateSpec;
use crate::optimizer::scalar::{ScalarArena, ScalarId};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use super::column_mapping::{
    NormArgumentOrder, NormExpr, NormIndex, NormSortKey, norm_contains, normalize,
};
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
    /// COUNT rolls up through nullable SUM, so restore its exact non-null
    /// result with COALESCE(_, 0). Grouped aggregation still emits no row
    /// when its input has no group.
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
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<NormExpr>, SqlCompileError> {
    let novarocks_functions::FunctionResultType::Scalar(value_type) =
        &call.source.binding().selected.result_type
    else {
        work.step()?;
        return Ok(None);
    };
    let mut order_by = Vec::with_capacity(call.source.order_by().len());
    for key in call.source.order_by() {
        work.flush()?;
        let Some(expr) = normalize(arena, key.expr, base_names, work.control())? else {
            return Ok(None);
        };
        order_by.push(NormSortKey {
            expr,
            asc: key.asc,
            nulls_first: key.nulls_first,
        });
        work.step()?;
    }
    let mut args = Vec::with_capacity(call.source.arguments().len());
    for arg in call.source.arguments() {
        work.flush()?;
        let Some(expr) = normalize(arena, *arg, base_names, work.control())? else {
            return Ok(None);
        };
        args.push(expr);
        work.step()?;
    }
    // These borrowed catalog facts are copied only after the original scope is
    // observed. Their opaque library work is not a memory admission claim.
    work.flush()?;
    let normalized = NormExpr::Call {
        value_type: value_type.clone(),
        name: format!("agg:{}", call.name.to_ascii_lowercase()),
        distinct: call.distinct,
        binding: Some(call.source.binding().clone()),
        decimal_overflow_policy: None,
        array_failure_mode: None,
        argument_order: NormArgumentOrder::Ordered,
        order_by,
        args,
    };
    work.step()?;
    work.flush()?;
    Ok(Some(normalized))
}

/// Decide whether (and how) the query aggregate can be answered from the MV.
/// Returns None when not rewritable. No candidate is published before the
/// original scope observes its completed tail.
pub(crate) fn plan_rollup(
    query_group_by: &[ScalarId],
    query_aggregates: &[ScalarAggregateSpec],
    query_arena: &ScalarArena,
    query_base_names: &HashMap<ColumnId, String>,
    mv_agg: &SpjgAggregate,
    mv_outputs: &[SpjgOutput],
    mv_arena: &ScalarArena,
    mv_base_names: &HashMap<ColumnId, String>,
    control: &dyn PureCompileControl,
) -> Result<Option<RollupPlan>, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        let mut q_keys = Vec::with_capacity(query_group_by.len());
        for expr in query_group_by {
            work.flush()?;
            let Some(key) = normalize(query_arena, *expr, query_base_names, control)? else {
                return Ok(None);
            };
            q_keys.push(key);
            work.step()?;
        }
        let mut m_keys = Vec::with_capacity(mv_agg.group_by.len());
        for expr in &mv_agg.group_by {
            work.flush()?;
            let Some(key) = normalize(mv_arena, *expr, mv_base_names, control)? else {
                return Ok(None);
            };
            m_keys.push(key);
            work.step()?;
        }
        for key in &q_keys {
            work.flush()?;
            if !norm_contains(&m_keys, key, control)? {
                return Ok(None);
            }
            work.step()?;
        }
        let mut equal = q_keys.len() == m_keys.len();
        if equal {
            for key in &m_keys {
                work.flush()?;
                if !norm_contains(&q_keys, key, control)? {
                    equal = false;
                    break;
                }
                work.step()?;
            }
        }

        // Exact overwrites preserve the original last materialized output.
        let mut mv_agg_by_norm = NormIndex::new();
        for (i, out) in mv_outputs.iter().enumerate() {
            if let SpjgOutputExpr::Aggregate(call) = &out.expr
                && let Some(n) = norm_agg(mv_arena, call, mv_base_names, &mut work)?
            {
                work.flush()?;
                mv_agg_by_norm.insert(n, i, control)?;
            }
            work.step()?;
        }
        let mut items = Vec::with_capacity(query_aggregates.len());
        for q in query_aggregates {
            work.step()?;
            if q.distinct {
                return Ok(None);
            }
            let Some(qn) = norm_agg(query_arena, q, query_base_names, &mut work)? else {
                return Ok(None);
            };
            work.flush()?;
            let Some(&mv_idx) = mv_agg_by_norm.get(&qn, control)? else {
                return Ok(None);
            };
            if equal {
                items.push(RollupItem {
                    mv_output_index: mv_idx,
                    rollup_fn: "",
                    needs_coalesce: false,
                });
                continue;
            }
            // The rollup whitelist is deliberately unchanged.
            work.flush()?;
            let name = q.name.to_ascii_lowercase();
            work.step()?;
            work.flush()?;
            let (rollup_fn, is_count) = match name.as_str() {
                "sum" => ("sum", false),
                "min" => ("min", false),
                "max" => ("max", false),
                "count" => ("sum", true),
                _ => return Ok(None),
            };
            items.push(RollupItem {
                mv_output_index: mv_idx,
                rollup_fn,
                needs_coalesce: is_count,
            });
        }
        Ok(Some(RollupPlan {
            kind: if equal {
                RollupKind::Direct
            } else {
                RollupKind::Rollup
            },
            items,
        }))
    })();
    if matches!(
        &result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::ScalarAggregateSpec;
    use crate::optimizer::scalar::{ScalarArena, ScalarId};

    use crate::planner::optimizer_bridge::scalar::intern_aggregate_call;
    use crate::planner::optimizer_bridge::scalar::intern_typed;
    use crate::planner::payload::AggregateCall;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    use super::super::descriptor::{SpjgAggregate, SpjgOutput, SpjgOutputExpr};

    fn plan_rollup(
        query_group_by: &[ScalarId],
        query_aggregates: &[ScalarAggregateSpec],
        query_arena: &ScalarArena,
        query_base_names: &HashMap<ColumnId, String>,
        mv_agg: &SpjgAggregate,
        mv_outputs: &[SpjgOutput],
        mv_arena: &ScalarArena,
        mv_base_names: &HashMap<ColumnId, String>,
    ) -> Option<RollupPlan> {
        super::plan_rollup(
            query_group_by,
            query_aggregates,
            query_arena,
            query_base_names,
            mv_agg,
            mv_outputs,
            mv_arena,
            mv_base_names,
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap()
    }

    // --- construction helpers (mirror sibling test modules) ---

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),

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
            value_type: c.value_type.clone(),
        }
    }

    fn names(pairs: &[(u32, &str)]) -> HashMap<ColumnId, String> {
        pairs
            .iter()
            .map(|(id, n)| (ColumnId(*id), n.to_string()))
            .collect()
    }

    /// Build an aggregate call over `args` with the given name/distinct.
    fn agg(
        output_column_id: ColumnId,
        name: &str,
        args: Vec<TypedExpr>,
        distinct: bool,
    ) -> AggregateCall {
        let argument_types = args
            .iter()
            .map(|arg| arg.value_type.data_type().clone())
            .collect::<Vec<_>>();
        AggregateCall {
            name: name.to_string(),
            distinct,
            result_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            output_column_id,
            source: crate::binding::AggregateArgumentSource::uncertified(
                args,
                vec![],
                crate::functions::test_resolved_aggregate(name, &argument_types, distinct),
            ),
        }
    }

    /// Wrap an aggregate call as a materialized MV output column.
    fn scalar_exprs(arena: &mut ScalarArena, exprs: Vec<TypedExpr>) -> Vec<ScalarId> {
        exprs
            .iter()
            .map(|expr| {
                intern_typed(
                    arena,
                    expr,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap()
            })
            .collect()
    }

    fn scalar_aggs(arena: &mut ScalarArena, calls: Vec<AggregateCall>) -> Vec<ScalarAggregateSpec> {
        calls
            .iter()
            .map(|call| {
                intern_aggregate_call(arena, call, crate::optimizer::test_optimizer_control())
                    .unwrap()
            })
            .collect()
    }

    fn agg_out(out: &OutputColumn, call: AggregateCall, arena: &mut ScalarArena) -> SpjgOutput {
        SpjgOutput {
            name: out.name.clone(),
            column_id: out.column_id,
            expr: SpjgOutputExpr::Aggregate(
                intern_aggregate_call(arena, &call, crate::optimizer::test_optimizer_control())
                    .unwrap(),
            ),
        }
    }

    /// Wrap a dimension expr as an MV output column.
    fn dim_out(out: &OutputColumn, expr: TypedExpr, arena: &mut ScalarArena) -> SpjgOutput {
        SpjgOutput {
            name: out.name.clone(),
            column_id: out.column_id,
            expr: SpjgOutputExpr::Dimension(
                intern_typed(
                    arena,
                    &expr,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap(),
            ),
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
                agg(mv_s.column_id, "sum", vec![col_ref(&mv_v)], false),
                &mut mv_arena,
            ),
            agg_out(
                &mv_c,
                agg(mv_c.column_id, "count", vec![], false),
                &mut mv_arena,
            ),
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
                agg(
                    ColumnId::new_for_test(201),
                    "sum",
                    vec![col_ref(&q_v)],
                    false,
                ),
                agg(ColumnId::new_for_test(202), "count", vec![], false),
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
        // SUM remains nullable; preserve COUNT's non-null published type.
        assert!(plan.items[1].needs_coalesce);
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
                agg(mv_s.column_id, "sum", vec![col_ref(&mv_v)], false),
                &mut mv_arena,
            ),
            agg_out(
                &mv_c,
                agg(mv_c.column_id, "count", vec![], false),
                &mut mv_arena,
            ),
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
                agg(
                    ColumnId::new_for_test(211),
                    "sum",
                    vec![col_ref(&q_v)],
                    false,
                ),
                agg(ColumnId::new_for_test(212), "count", vec![], false),
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
                agg(mv_d.column_id, "count", vec![col_ref(&mv_x)], true),
                &mut mv_arena,
            ),
        ];

        // query: SELECT a, count(distinct x) GROUP BY a (== MV group-by).
        let q_a = col(21, "a");
        let q_x = col(24, "x");
        let q_names = names(&[(21, "a"), (24, "x")]);
        let mut q_arena = ScalarArena::new();
        let q_group_by = scalar_exprs(&mut q_arena, vec![col_ref(&q_a)]);
        let q_aggs = scalar_aggs(
            &mut q_arena,
            vec![agg(
                ColumnId::new_for_test(221),
                "count",
                vec![col_ref(&q_x)],
                true,
            )],
        );

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
                agg(mv_m.column_id, "avg", vec![col_ref(&mv_v)], false),
                &mut mv_arena,
            ),
        ];

        // Subset query: GROUP BY a only -> avg cannot be rolled up -> None.
        let q_a = col(21, "a");
        let q_v = col(23, "v");
        let q_names = names(&[(21, "a"), (23, "v")]);
        let mut q_arena = ScalarArena::new();
        let q_group_by_subset = scalar_exprs(&mut q_arena, vec![col_ref(&q_a)]);
        let q_aggs = scalar_aggs(
            &mut q_arena,
            vec![agg(
                ColumnId::new_for_test(231),
                "avg",
                vec![col_ref(&q_v)],
                false,
            )],
        );
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
            agg_out(
                &mv_c,
                agg(mv_c.column_id, "count", vec![], false),
                &mut mv_arena,
            ),
        ];

        // query: SELECT count(*) with NO group-by (scalar). Group-by {} is a
        // subset of {a}, so this is a Rollup, and SUM over an empty MV result
        // is NULL where COUNT must be 0 -> needs_coalesce.
        let q_names = names(&[]);
        let mut q_arena = ScalarArena::new();
        let q_aggs = scalar_aggs(
            &mut q_arena,
            vec![agg(ColumnId::new_for_test(241), "count", vec![], false)],
        );

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

#[cfg(test)]
#[path = "consumer_tests.rs"]
mod consumer_tests;
