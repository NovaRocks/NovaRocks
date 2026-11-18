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

//! Transformation rule: LogicalTopN(FINAL, !split) -> LogicalTopN(FINAL, split=true) over LogicalTopN(PARTIAL).
//!
//! Mirrors StarRocks's SplitTopNRule.java. Cost search picks between the
//! single-stage TopN (original) and this two-stage alternative.

use crate::optimizer::memo::{MExpr, Memo};
use crate::optimizer::operator::{Operator, TopNOp, TopNPhase};
use crate::optimizer::rule::{NewExpr, Rule, RuleType};

pub(crate) struct SplitTopN;

impl Rule for SplitTopN {
    fn name(&self) -> &str {
        "SplitTopN"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalTopN(t) if t.phase == TopNPhase::Final && !t.is_split
        )
    }

    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalTopN(src) = &expr.op else {
            return Ok(vec![]);
        };
        // Finite limit required; plain ORDER BY without LIMIT is out of scope.
        let limit = match src.limit {
            Some(l) if l >= 0 => l,
            _ => return Ok(vec![]),
        };
        let offset = src.offset.unwrap_or(0).max(0);
        if offset > 0 {
            // Standalone execution already preserves LIMIT/OFFSET correctly for
            // single-stage TopN. Split TopN with a non-zero final offset still
            // needs tighter parity work in the merging exchange path, so keep
            // the conservative single-stage plan for semantic correctness.
            return Ok(vec![]);
        }
        // Saturating add: if L+O would overflow, cap at i64::MAX (effectively
        // means "partial passes everything through"; cost search will prefer
        // single-stage in that corner case).
        let partial_limit = limit.saturating_add(offset);

        // PARTIAL child: same sort items, larger limit, zero offset.
        let partial_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: src.items.clone(),
                limit: Some(partial_limit),
                offset: Some(0),
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: expr.children.clone(),
        };
        let partial_group = memo.new_group(partial_mexpr);

        // FINAL with split flag, original limit/offset. Child = new partial group.
        let final_expr = NewExpr {
            op: Operator::LogicalTopN(TopNOp {
                items: src.items.clone(),
                limit: src.limit,
                offset: src.offset,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![partial_group],
        };
        Ok(vec![final_expr])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::memo::Memo;
    use crate::optimizer::operator::{ScanOp, TopNOp};

    fn mk_scan_group(memo: &mut Memo) -> usize {
        let m = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".into(),
                table: crate::planner::table::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::compiler::mv_rewrite::test_scan_source(
                        crate::planner::table::SqlScanKind::ConnectorRead,
                    ),
                },
                alias: None,
                stats_ref: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        };
        memo.new_group(m)
    }

    #[test]
    fn fires_on_final_unsplit_with_limit() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(100),
                offset: Some(0),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN
            .apply(
                &topn_mexpr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1, "expected one split alternative");
        match &out[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.phase, TopNPhase::Final);
                assert!(t.is_split);
                assert_eq!(t.limit, Some(100));
                assert_eq!(t.offset, Some(0));
            }
            other => panic!("expected LogicalTopN final+split, got {:?}", other),
        }
        // FINAL's child is a new group containing the PARTIAL TopN.
        assert_eq!(out[0].children.len(), 1);
        let partial_group = &memo.groups[out[0].children[0]];
        match &partial_group.logical_exprs[0].op {
            Operator::LogicalTopN(t) => {
                assert_eq!(t.phase, TopNPhase::Partial);
                assert!(!t.is_split);
                assert_eq!(t.limit, Some(100), "partial limit must be L+O = 100+0");
                assert_eq!(t.offset, Some(0));
            }
            other => panic!("expected LogicalTopN partial, got {:?}", other),
        }
        assert_eq!(partial_group.logical_exprs[0].children, vec![scan_group]);
    }

    #[test]
    fn does_not_fire_with_non_zero_offset() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(100),
                offset: Some(10),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN
            .apply(
                &topn_mexpr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert!(
            out.is_empty(),
            "non-zero offset should stay single-stage for now"
        );
    }

    #[test]
    fn does_not_fire_on_partial() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(100),
                offset: None,
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        assert!(
            !SplitTopN.matches(&topn_mexpr.op),
            "rule must not match on PARTIAL phase"
        );
    }

    #[test]
    fn does_not_fire_when_already_split() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(100),
                offset: None,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![scan_group],
        };
        assert!(
            !SplitTopN.matches(&topn_mexpr.op),
            "rule must not match when already split"
        );
    }

    #[test]
    fn does_not_fire_without_limit() {
        let mut memo = Memo::new();
        let scan_group = mk_scan_group(&mut memo);
        let topn_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: None,
                offset: Some(5),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan_group],
        };
        let out = SplitTopN
            .apply(
                &topn_mexpr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert!(out.is_empty(), "no limit => out of scope");
    }

    #[test]
    fn grouped_topn_completion_keeps_state_and_row_budgets_on_their_actual_producers() {
        use novarocks_physical_plan::{
            AggregateGrouping, AggregatePhase, NodeKind, RuntimeFilterConsumerActivation,
            RuntimeFilterDomain, RuntimeFilterProducerTarget, TopNPhase, TopNReduction,
        };

        fn assert_complete_rows(
            plan: &novarocks_physical_plan::PhysicalPlan,
            fragment: &novarocks_physical_plan::Fragment,
            node: &novarocks_physical_plan::PhysicalNode,
        ) {
            use {
                novarocks_expression_contract::ExprKind, novarocks_physical_plan::RowMultiplicity,
            };
            assert_eq!(
                node.output_properties.row_multiplicity,
                RowMultiplicity::SingleCopy
            );
            match &node.kind {
                NodeKind::Aggregate {
                    grouping: AggregateGrouping::Complete,
                    ..
                } => {}
                NodeKind::Project { expressions } => {
                    // Only an identity projection may carry this fixture's
                    // completed row contract through to its quota.
                    assert!(expressions.iter().all(|(expression, _)| matches!(
                        fragment.root_expression(node.id, *expression).unwrap().kind,
                        ExprKind::Value(_)
                    )));
                    assert_eq!(node.inputs.len(), 1);
                    assert_complete_rows(plan, fragment, &fragment.nodes()[&node.inputs[0]]);
                }
                NodeKind::TopN {
                    reduction: TopNReduction::Rows,
                    ..
                } => {
                    assert_eq!(node.inputs.len(), 1);
                    assert_complete_rows(plan, fragment, &fragment.nodes()[&node.inputs[0]]);
                }
                NodeKind::ExchangeSource { edge, imports } => {
                    let edge = &plan.edges()[edge];
                    assert_eq!(edge.destination.fragment, fragment.id());
                    assert_eq!(edge.destination.node, node.id);
                    assert_eq!(edge.destination.receive_mapping.as_ref(), imports.as_ref());
                    assert_eq!(
                        edge.partitioning.source_multiplicity,
                        RowMultiplicity::SingleCopy
                    );
                    assert_eq!(
                        edge.partitioning.destination_multiplicity,
                        RowMultiplicity::SingleCopy
                    );
                    let source = &plan.fragments()[&edge.source.fragment];
                    assert_complete_rows(plan, source, &source.nodes()[&source.root()]);
                }
                other => panic!("a Rows budget has no exact complete-group source: {other:?}"),
            }
        }

        for (sql, has_aggregate_state) in [
            (
                "SELECT order_key, COUNT(*), MIN(order_key), MAX(order_key) FROM orders GROUP BY order_key ORDER BY order_key LIMIT 2",
                true,
            ),
            (
                "SELECT DISTINCT order_key FROM orders ORDER BY order_key DESC LIMIT 2",
                false,
            ),
        ] {
            // The real optimizer owns the selected grouping path. Compiled
            // preaggregation can add a state budget before complete groups;
            // DISTINCT keeps Rows because it produces no aggregate state.
            let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
            let plan = owner.plan();
            let mut rows = 0;
            let mut grouped_states = 0;
            for fragment in plan.fragments().values() {
                for node in fragment.nodes().values() {
                    let NodeKind::TopN {
                        reduction, phase, ..
                    } = &node.kind
                    else {
                        continue;
                    };
                    match reduction {
                        TopNReduction::Rows => {
                            rows += 1;
                            assert_eq!(node.inputs.len(), 1);
                            assert_eq!(
                                node.required_inputs[0].row_multiplicity,
                                novarocks_physical_plan::RowMultiplicity::SingleCopy
                            );
                            assert_complete_rows(
                                plan,
                                fragment,
                                &fragment.nodes()[&node.inputs[0]],
                            );
                            if matches!(phase, TopNPhase::Partial { .. }) {
                                let input = &fragment.nodes()[&node.inputs[0]];
                                assert!(
                                    matches!(
                                        input.kind,
                                        NodeKind::Aggregate {
                                            grouping: AggregateGrouping::Complete,
                                            ..
                                        }
                                    ),
                                    "a row quota must read complete groups: {sql} {input:?}"
                                );
                            }
                        }
                        TopNReduction::GroupedStates {
                            group_by, calls, ..
                        } => {
                            grouped_states += 1;
                            assert!(has_aggregate_state);
                            assert!(matches!(phase, TopNPhase::Partial { .. }));
                            let input = &fragment.nodes()[&node.inputs[0]];
                            let NodeKind::Aggregate {
                                grouping: AggregateGrouping::Partial,
                                group_by: producer_keys,
                                calls: producer_calls,
                            } = &input.kind
                            else {
                                panic!(
                                    "a state budget must read its direct Partial producer: {sql} {input:?}"
                                );
                            };
                            assert_eq!(
                                group_by.iter().map(|(_, value)| value).collect::<Vec<_>>(),
                                producer_keys
                                    .iter()
                                    .map(|(_, value)| value)
                                    .collect::<Vec<_>>()
                            );
                            assert_eq!(calls.len(), producer_calls.len());
                            assert!(!calls.is_empty());
                            for (call, producer) in calls.iter().zip(producer_calls) {
                                let AggregatePhase::Intermediate { sequence } = call.binding.phase
                                else {
                                    panic!("grouped state budgets author Intermediate reductions");
                                };
                                assert_eq!(Some(sequence), producer.binding.phase.sequence());
                                assert_ne!(call.id, producer.id);
                                assert_ne!(call.output, producer.output);
                                let mut binding = producer.binding.clone();
                                binding.phase = call.binding.phase;
                                assert_eq!(call.binding, binding);
                            }
                        }
                    }
                }
            }
            assert!(rows > 0, "the final sequence retains its row quota: {sql}");
            assert_eq!(
                grouped_states > 0,
                has_aggregate_state,
                "only the aggregate query has Partial state quotas: {sql}"
            );

            // Optional RF placement is a separate proof: the compiled direct
            // Partial frontier admits UTF-8 only. The Int64 fixture cannot
            // demand that optional capability. Mandatory UTF-8 publication is
            // covered by the actual ordered-aggregate completion regression.
            for filter in plan.runtime_filters().values() {
                assert!(!filter.producers.is_empty());
                assert!(!filter.consumers.is_empty());
                for producer in &filter.producers {
                    let fragment = &plan.fragments()[&producer.endpoint.fragment];
                    let aggregate = &fragment.nodes()[&producer.endpoint.node];
                    let NodeKind::Aggregate { grouping, .. } = &aggregate.kind else {
                        panic!("AggregateTopN filter must keep its aggregate producer");
                    };
                    let RuntimeFilterProducerTarget::AggregateTopNKey { topn, .. } =
                        producer.target
                    else {
                        panic!("the grouped fixture has only AggregateTopN producers");
                    };
                    assert_eq!(fragment.nodes()[&topn].inputs.as_ref(), &[aggregate.id]);
                    if *grouping == AggregateGrouping::Partial {
                        let RuntimeFilterDomain::Ordered { key, .. } = &filter.domain else {
                            panic!("AggregateTopN uses an ordered domain");
                        };
                        assert_eq!(
                            key.ty.data_type(),
                            &arrow::datatypes::DataType::Utf8,
                            "a numeric Partial producer is outside the compiled RF frontier"
                        );
                    }
                }
                assert!(filter.consumers.iter().all(|consumer| matches!(
                    consumer.activation,
                    RuntimeFilterConsumerActivation::NonBlockingLive { .. }
                )));
            }
        }
    }
}
