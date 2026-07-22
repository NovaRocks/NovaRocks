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

//! PruneAggregateColumns — Phase 2 rule for Aggregate nodes.
//!
//! Public `LogicalAggregateOp.output_columns` may be a subset of the internal
//! `AggregateOutputLayout`.  This rule prunes parent-visible outputs and
//! aggregate calls by ColumnId while preserving the complete group-key layout
//! needed by physical property derivation and codegen.

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneAggregateColumns;

impl LogicalRewriteRule for PruneAggregateColumns {
    fn name(&self) -> &'static str {
        "PruneAggregateColumns"
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

    fn apply(&self, expr: OptExpr, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            children,
            required_output_columns,
        } = expr;
        let Operator::LogicalAggregate(mut node) = op else {
            unreachable!()
        };

        let Some(needed) = required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };
        let needed = node.effective_required_outputs(&needed);

        node.output_layout
            .validate_aggregate_calls(&node.aggregates, node.is_merge.len())
            .map_err(|err| format!("PruneAggregateColumns {err}"))?;

        let original_output_ids = node
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        let original_aggregate_ids = node
            .aggregates
            .iter()
            .map(|aggregate| aggregate.output_column_id)
            .collect::<Vec<_>>();

        node.output_columns
            .retain(|column| needed.contains(&column.column_id));

        let mut retained_aggregates = Vec::new();
        let mut retained_is_merge = Vec::new();
        let mut retained_aggregate_columns = Vec::new();
        for ((aggregate, is_merge), layout_column) in node
            .aggregates
            .into_iter()
            .zip(node.is_merge.into_iter())
            .zip(node.output_layout.aggregate_columns.into_iter())
        {
            if aggregate.output_column_id != layout_column.column_id {
                return Err(format!(
                    "aggregate output layout mismatch: spec id {} != layout id {}",
                    aggregate.output_column_id.0, layout_column.column_id.0
                ));
            }
            if needed.contains(&aggregate.output_column_id) {
                retained_aggregate_columns.push(layout_column);
                retained_is_merge.push(is_merge);
                retained_aggregates.push(aggregate);
            }
        }

        node.aggregates = retained_aggregates;
        node.is_merge = retained_is_merge;
        node.output_layout.aggregate_columns = retained_aggregate_columns;

        if node.output_columns.is_empty() {
            let fallback = node
                .output_layout
                .group_key_columns
                .iter()
                .chain(node.output_layout.aggregate_columns.iter())
                .find(|column| needed.contains(&column.column_id))
                .or_else(|| node.output_layout.group_key_columns.first())
                .or_else(|| node.output_layout.aggregate_columns.first())
                .ok_or_else(|| {
                    "AggregateOutputLayout must expose at least one fallback output".to_string()
                })?
                .clone();
            node.output_columns.push(fallback);
        }

        let pruned_output_ids = node
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        let pruned_aggregate_ids = node
            .aggregates
            .iter()
            .map(|aggregate| aggregate.output_column_id)
            .collect::<Vec<_>>();

        if pruned_output_ids == original_output_ids
            && pruned_aggregate_ids == original_aggregate_ids
        {
            return Ok(RewriteResult::Unchanged);
        }

        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalAggregate(node),
            children,
            required_output_columns,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        AggStage, AggregateOutputLayout, LogicalAggregateOp, Operator, ScalarAggregateSpec, ScanOp,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;
    use std::collections::HashSet;

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    fn make_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn dummy_input() -> OptExpr {
        let table = TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "x".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".to_string(),
            table,
            alias: None,
            stats_ref: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(99),
                name: "x".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    #[test]
    fn prune_aggregate_keeps_required_calls_and_drops_unused_calls() {
        let id_group = ColumnId::new_for_test(101);
        let id_sum = ColumnId::new_for_test(201);
        let id_count = ColumnId::new_for_test(202);
        let group = make_output_column(id_group, "g");
        let sum = make_output_column(id_sum, "sum_a");
        let count = make_output_column(id_count, "count_b");
        let layout =
            AggregateOutputLayout::new(vec![group.clone()], vec![sum.clone(), count.clone()]);

        let mut needed = HashSet::new();
        needed.insert(id_sum);

        let mut expr = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Single,
                vec![],
                vec![
                    ScalarAggregateSpec {
                        output_column_id: id_sum,
                        name: "sum".to_string(),
                        args: vec![],
                        distinct: false,
                        order_by: vec![],
                    },
                    ScalarAggregateSpec {
                        output_column_id: id_count,
                        name: "count".to_string(),
                        args: vec![],
                        distinct: false,
                        order_by: vec![],
                    },
                ],
                layout,
                vec![group.clone(), sum.clone(), count.clone()],
                vec![false, false],
                false,
            )),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let result = PruneAggregateColumns.apply(expr, &mut ctx()).unwrap();

        let RewriteResult::Changed(changed) = result else {
            panic!("expected prune aggregate to change");
        };
        let Operator::LogicalAggregate(node) = changed.op else {
            panic!("expected LogicalAggregate");
        };
        assert_eq!(
            node.output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![id_sum]
        );
        assert_eq!(node.aggregates.len(), 1);
        assert_eq!(node.aggregates[0].output_column_id, id_sum);
        assert_eq!(node.output_layout.group_key_columns[0].column_id, id_group);
        assert_eq!(node.output_layout.aggregate_columns.len(), 1);
        assert_eq!(node.output_layout.aggregate_columns[0].column_id, id_sum);
        assert_eq!(node.is_merge, vec![false]);
    }

    #[test]
    fn prune_aggregate_prunes_public_group_key_output_but_keeps_layout_group_key() {
        let id_group = ColumnId::new_for_test(101);
        let id_sum = ColumnId::new_for_test(201);
        let group = make_output_column(id_group, "g");
        let sum = make_output_column(id_sum, "sum_a");
        let layout = AggregateOutputLayout::new(vec![group.clone()], vec![sum.clone()]);
        let mut needed = HashSet::new();
        needed.insert(id_sum);

        let mut expr = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![],
                vec![ScalarAggregateSpec {
                    output_column_id: id_sum,
                    name: "sum".to_string(),
                    args: vec![],
                    distinct: false,
                    order_by: vec![],
                }],
                layout,
                vec![group.clone(), sum.clone()],
            )),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let result = PruneAggregateColumns.apply(expr, &mut ctx()).unwrap();

        let RewriteResult::Changed(changed) = result else {
            panic!("expected changed aggregate");
        };
        let Operator::LogicalAggregate(node) = changed.op else {
            panic!("expected LogicalAggregate");
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].column_id, id_sum);
        assert_eq!(node.output_layout.group_key_columns.len(), 1);
        assert_eq!(node.output_layout.group_key_columns[0].column_id, id_group);
    }

    #[test]
    fn prune_aggregate_fallback_public_output_prefers_needed_hidden_aggregate() {
        let id_group = ColumnId::new_for_test(101);
        let id_sum = ColumnId::new_for_test(201);
        let group = make_output_column(id_group, "g");
        let sum = make_output_column(id_sum, "sum_a");
        let layout = AggregateOutputLayout::new(vec![group.clone()], vec![sum.clone()]);
        let mut needed = HashSet::new();
        needed.insert(id_sum);

        let mut expr = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![],
                vec![ScalarAggregateSpec {
                    output_column_id: id_sum,
                    name: "sum".to_string(),
                    args: vec![],
                    distinct: false,
                    order_by: vec![],
                }],
                layout,
                vec![group.clone()],
            )),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let result = PruneAggregateColumns.apply(expr, &mut ctx()).unwrap();

        let RewriteResult::Changed(changed) = result else {
            panic!("expected fallback aggregate output to change the node");
        };
        let Operator::LogicalAggregate(node) = changed.op else {
            panic!("expected LogicalAggregate");
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].column_id, id_sum);
    }

    #[test]
    fn prune_aggregate_rejects_spec_layout_output_mismatch() {
        let id_sum = ColumnId::new_for_test(201);
        let id_count = ColumnId::new_for_test(202);
        let mut needed = HashSet::new();
        needed.insert(id_sum);

        let mut expr = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp {
                stage: AggStage::Single,
                group_by: vec![],
                aggregates: vec![ScalarAggregateSpec {
                    output_column_id: id_sum,
                    name: "sum".to_string(),
                    args: vec![],
                    distinct: false,
                    order_by: vec![],
                }],
                output_layout: AggregateOutputLayout::new(
                    vec![],
                    vec![make_output_column(id_count, "count_b")],
                ),
                output_columns: vec![make_output_column(id_sum, "sum_a")],
                is_merge: vec![false],
                is_split: false,
            }),
            vec![dummy_input()],
        );
        expr.required_output_columns = Some(needed);

        let err = PruneAggregateColumns.apply(expr, &mut ctx()).unwrap_err();
        assert!(err.contains("aggregate output layout mismatch"));
    }

    #[test]
    fn prune_aggregate_noop_when_required_output_columns_is_none() {
        let id_sum = ColumnId::new_for_test(201);
        let sum = make_output_column(id_sum, "sum_a");
        let expr = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![],
                vec![ScalarAggregateSpec {
                    output_column_id: id_sum,
                    name: "sum".to_string(),
                    args: vec![],
                    distinct: false,
                    order_by: vec![],
                }],
                AggregateOutputLayout::new(vec![], vec![sum.clone()]),
                vec![sum],
            )),
            vec![dummy_input()],
        );

        let result = PruneAggregateColumns.apply(expr, &mut ctx()).unwrap();

        assert!(matches!(result, RewriteResult::Unchanged));
    }
}
