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

//! AggregatePushdownRule entry point.

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule as RewriteRule;

#[allow(dead_code)]
pub(crate) struct AggregatePushdownRule;

impl RewriteRule for AggregatePushdownRule {
    fn name(&self) -> &'static str {
        "AggregatePushdown"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Aggregate,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // Extract the aggregate op; return Unchanged if shape doesn't match.
        let agg = match expr.op {
            Operator::LogicalAggregate(ref a) => a.clone(),
            _ => return Ok(RewriteResult::Unchanged),
        };

        let arena_rc = ctx.scalar_arena();
        let stats_input = ctx
            .query_stats_input()
            .cloned()
            .ok_or_else(|| "AggregatePushdown requires OptimizerStatsInput".to_string())?;

        // Phase 1: read-only borrow for collection and cost gating.
        let push = {
            let arena = arena_rc.borrow();
            let push = super::collector::collect_push_plan(&agg, expr.unary_input(), &arena);
            if let Some(ref p) = push {
                if !super::cost::should_push(p, &arena, &stats_input) {
                    return Ok(RewriteResult::Unchanged);
                }
            }
            push
        };
        let Some(push) = push else {
            return Ok(RewriteResult::Unchanged);
        };

        // Phase 2: mutable borrow for rewriting.
        let factory = ctx
            .column_ref_factory()
            .ok_or_else(|| "AggregatePushdown requires ColumnRefFactory".to_string())?;
        let mut factory = factory.borrow_mut();
        let mut arena = arena_rc.borrow_mut();

        Ok(RewriteResult::Changed(super::rewriter::rewrite(
            &agg,
            expr.unary_input(),
            push,
            &mut factory,
            &mut arena,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        AggStage, AggregateOutputLayout, LogicalAggregateOp, Operator, ScanOp,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::stats_input::OptimizerStatsInput;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    fn set_empty_stats_input(ctx: &mut RewriteContext) {
        ctx.set_query_stats_input(OptimizerStatsInput::from_test_table_statistics(
            &HashMap::new(),
        ));
    }

    fn dummy_scan(name: &str, cols: &[&str]) -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            stats_ref: None,
            columns: cols
                .iter()
                .map(|n| OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: (*n).into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn dummy_aggregate() -> OptExpr {
        let scan = dummy_scan("t", &["id"]);
        OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Single,
                vec![],
                vec![],
                AggregateOutputLayout::new(vec![], vec![]),
                vec![],
                vec![],
                false,
            )),
            vec![scan],
        )
    }

    #[test]
    fn pattern_matches_only_aggregate_roots() {
        use crate::sql::optimizer::rewrite::tree_binder::bind_tree;

        let rule = AggregatePushdownRule;
        assert!(bind_tree(&rule.pattern(), &dummy_aggregate()).is_some());
        assert!(bind_tree(&rule.pattern(), &dummy_scan("t", &["id"])).is_none());
    }

    #[test]
    fn stub_returns_none() {
        use crate::sql::optimizer::scalar::ScalarArena;
        use std::cell::RefCell;
        use std::rc::Rc;
        let rule = AggregatePushdownRule;
        let plan = dummy_aggregate();
        let mut ctx = RewriteContext::new(
            RewriteConsumer::Query,
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        );
        set_empty_stats_input(&mut ctx);
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        assert!(
            crate::sql::optimizer::rewrite::tree_binder::bind_tree(&rule.pattern(), &plan)
                .is_some()
        );
        assert!(matches!(
            rule.apply(plan, &mut ctx).unwrap(),
            RewriteResult::Unchanged
        ));
    }

    #[test]
    fn idempotent_does_not_repush_already_pushed_plan() {
        use crate::sql::analysis::{BinOp, ExprKind, JoinKind};
        use crate::sql::optimizer::operator::{LogicalJoinOp, ScalarAggregateSpec};
        use crate::sql::optimizer::scalar::ScalarArena;

        use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
        use std::cell::RefCell;
        use std::rc::Rc;

        fn col_typed(name: &str) -> crate::sql::analysis::TypedExpr {
            // Use a stable non-UNSET ColumnId derived from the name bytes (FNV-1a).
            let mut h: u32 = 2166136261;
            for b in name.bytes() {
                h ^= b as u32;
                h = h.wrapping_mul(16777619);
            }
            let col_id = ColumnId::new_for_test((h % 10000) + 1);
            crate::sql::analysis::TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: col_id,
                    qualifier: None,
                    column: name.into(),
                },
                data_type: DataType::Int64,
                nullable: true,
            }
        }

        let mut arena = ScalarArena::new();

        let gb_typed = col_typed("k");
        let ExprKind::ColumnRef {
            column_id: gb_output_id,
            ..
        } = &gb_typed.kind
        else {
            unreachable!("col_typed must build a ColumnRef");
        };
        let gb_output_id = *gb_output_id;
        let gb_id = intern_typed(&mut arena, &gb_typed);
        let sum_arg = intern_typed(&mut arena, &col_typed("v"));
        let sum_spec = ScalarAggregateSpec {
            output_column_id: ColumnId::new_for_test(9001),
            name: "sum".into(),
            args: vec![sum_arg],
            distinct: false,
            order_by: vec![],
        };
        let output_layout = AggregateOutputLayout::new(
            vec![OutputColumn {
                column_id: gb_output_id,
                name: "k".into(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
            vec![OutputColumn {
                column_id: sum_spec.output_column_id,
                name: "sum".into(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
        );

        let cond_typed = crate::sql::analysis::TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_typed("k")),
                op: BinOp::Eq,
                right: Box::new(col_typed("k")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let cond_id = intern_typed(&mut arena, &cond_typed);

        let join = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(cond_id),
            }),
            vec![dummy_scan("a", &["k", "v"]), dummy_scan("b", &["k"])],
        );

        // Build a plan with is_split = true. The rule must reject.
        let plan = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Single,
                vec![gb_id],
                vec![sum_spec],
                output_layout,
                vec![],
                vec![false],
                true, // is_split = true: already pushed, must not repush
            )),
            vec![join],
        );

        let rule = AggregatePushdownRule;
        let mut ctx = RewriteContext::new(
            RewriteConsumer::Query,
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        );
        set_empty_stats_input(&mut ctx);
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));

        assert!(
            matches!(
                rule.apply(plan, &mut ctx).unwrap(),
                RewriteResult::Unchanged
            ),
            "must not re-fire on is_split=true"
        );
    }
}
