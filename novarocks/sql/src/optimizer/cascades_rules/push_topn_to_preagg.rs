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

use crate::column_id::ColumnId;
use crate::common::OutputColumn;
use crate::optimizer::binder::Binding;
use crate::optimizer::cascades_rules::split_aggregate::find_existing_logical_group;
use crate::optimizer::memo::{GroupId, MExpr, Memo};
use crate::optimizer::operator::{AggStage, LogicalAggregateOp, Operator, TopNOp, TopNPhase};
use crate::optimizer::pattern::{OpKind, Pattern};
use crate::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::optimizer::scalar::{ColumnDisplay, ScalarArena, ScalarNode, SortKey};

pub(crate) struct PushDownTopNToPreAgg;

impl Rule for PushDownTopNToPreAgg {
    fn name(&self) -> &str {
        "PushDownTopNToPreAgg"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTopN(topn) = &expr.op else {
            return Vec::new();
        };
        if expr.children.len() != 1 {
            return Vec::new();
        }

        let Some(global_group) = memo.groups.get(expr.children[0]) else {
            return Vec::new();
        };
        let mut candidates = Vec::new();
        for global_expr in &global_group.logical_exprs {
            let Operator::LogicalAggregate(global) = &global_expr.op else {
                continue;
            };
            if global_expr.children.len() != 1 {
                continue;
            }
            let local_group_id = global_expr.children[0];
            let Some(local_group) = memo.groups.get(local_group_id) else {
                continue;
            };
            for local_expr in &local_group.logical_exprs {
                let Operator::LogicalAggregate(local) = &local_expr.op else {
                    continue;
                };
                candidates.push((global.clone(), local.clone(), local_group_id));
            }
        }

        let mut results = Vec::new();
        for (global, local, local_group_id) in candidates {
            results.extend(rewrite_topn_preagg(
                topn,
                &global,
                &local,
                local_group_id,
                memo,
            ));
        }
        results
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::TopN,
            children: vec![Pattern::Op {
                kind: OpKind::Aggregate,
                children: vec![Pattern::Op {
                    kind: OpKind::Aggregate,
                    children: vec![Pattern::Leaf],
                }],
            }],
        }
    }

    fn apply_bound(&self, binding: &Binding, memo: &mut Memo) -> Vec<NewExpr> {
        // interior 0 = TopN, 1 = global Aggregate, 2 = local Aggregate.
        let Operator::LogicalTopN(topn) = binding.op(memo, 0).clone() else {
            return Vec::new();
        };
        let Operator::LogicalAggregate(global) = binding.op(memo, 1).clone() else {
            return Vec::new();
        };
        let Operator::LogicalAggregate(local) = binding.op(memo, 2).clone() else {
            return Vec::new();
        };
        let local_group_id = binding.children(1)[0];
        rewrite_topn_preagg(&topn, &global, &local, local_group_id, memo)
    }
}

fn rewrite_topn_preagg(
    topn: &TopNOp,
    global: &LogicalAggregateOp,
    local: &LogicalAggregateOp,
    local_group_id: GroupId,
    memo: &mut Memo,
) -> Vec<NewExpr> {
    if topn.phase != TopNPhase::Final || topn.is_split {
        return Vec::new();
    }
    let Some(limit) = topn.limit else {
        return Vec::new();
    };
    if limit < 0 || topn.offset.unwrap_or(0) != 0 {
        return Vec::new();
    }
    if global.stage != AggStage::Global || local.stage != AggStage::Local {
        return Vec::new();
    }
    if !global.is_split || !local.is_split {
        return Vec::new();
    }
    if global.aggregates.iter().any(|agg| agg.distinct)
        || local.aggregates.iter().any(|agg| agg.distinct)
    {
        return Vec::new();
    }
    if !order_by_covers_group_by(&topn.items, global, &memo.scalars) {
        return Vec::new();
    }
    let Some(partial_items) =
        partial_order_by_for_local_group_by(&topn.items, global, local, &mut memo.scalars)
    else {
        return Vec::new();
    };

    let partial_op = Operator::LogicalTopN(TopNOp {
        items: partial_items,
        limit: topn.limit,
        offset: Some(0),
        phase: TopNPhase::Partial,
        // This partial TopN is introduced as one half of this pre-aggregate
        // pruning shape. Current property derivation ignores `is_split` for
        // Partial nodes, but keeping the marker explicit makes the intent
        // visible to later TopN rules.
        is_split: true,
    });
    let partial_children = vec![local_group_id];
    let partial_group_id = find_existing_logical_group(memo, &partial_op, &partial_children)
        .unwrap_or_else(|| {
            let partial_id = memo.next_expr_id();
            memo.new_group(MExpr {
                id: partial_id,
                op: partial_op,
                children: partial_children,
            })
        });

    let new_global_op = Operator::LogicalAggregate(global.clone());
    let new_global_children = vec![partial_group_id];
    let new_global_group_id =
        find_existing_logical_group(memo, &new_global_op, &new_global_children).unwrap_or_else(
            || {
                let global_id = memo.next_expr_id();
                memo.new_group(MExpr {
                    id: global_id,
                    op: new_global_op,
                    children: new_global_children,
                })
            },
        );

    vec![NewExpr {
        op: Operator::LogicalTopN(topn.clone()),
        children: vec![new_global_group_id],
    }]
}

fn order_by_covers_group_by(
    items: &[SortKey],
    global: &LogicalAggregateOp,
    arena: &ScalarArena,
) -> bool {
    if items.is_empty() {
        return false;
    }
    let Some(global_group_outputs) = group_key_outputs(global) else {
        return false;
    };

    if items.len() != global_group_outputs.len() {
        return false;
    }

    let mut item_columns = Vec::with_capacity(items.len());
    for item in items {
        let ScalarNode::ColumnRef(column_id) = arena.node(item.expr) else {
            return false;
        };
        if !global_group_outputs
            .iter()
            .any(|column| column.column_id == *column_id)
        {
            return false;
        }
        item_columns.push(*column_id);
    }

    global_group_outputs
        .iter()
        .all(|column| item_columns.contains(&column.column_id))
}

fn partial_order_by_for_local_group_by(
    items: &[SortKey],
    global: &LogicalAggregateOp,
    local: &LogicalAggregateOp,
    arena: &mut ScalarArena,
) -> Option<Vec<SortKey>> {
    if items.is_empty() {
        return None;
    }
    if global.group_by.len() != local.group_by.len() {
        return None;
    }
    let global_group_outputs = group_key_outputs(global)?;
    let local_group_outputs = group_key_outputs(local)?;

    let mut local_outputs_for_items = Vec::with_capacity(items.len());
    for item in items {
        let global_column_id = match arena.node(item.expr) {
            ScalarNode::ColumnRef(column_id) => *column_id,
            _ => return None,
        };
        let position = global_group_outputs
            .iter()
            .position(|column| column.column_id == global_column_id)?;
        local_outputs_for_items.push(&local_group_outputs[position]);
    }

    let mut remapped = Vec::with_capacity(items.len());
    for (item, local_output) in items.iter().zip(local_outputs_for_items) {
        let local_expr = arena.intern(
            ScalarNode::ColumnRef(local_output.column_id),
            local_output.data_type.clone(),
            local_output.nullable,
        );
        remapped.push(SortKey {
            expr: local_expr,
            asc: item.asc,
            nulls_first: item.nulls_first,
            display: Some(ColumnDisplay::new(None, local_output.name.clone())),
        });
    }
    Some(remapped)
}

fn group_key_outputs(agg: &LogicalAggregateOp) -> Option<&[OutputColumn]> {
    if agg.output_layout.group_key_columns.len() != agg.group_by.len() {
        return None;
    }
    if agg
        .output_layout
        .group_key_columns
        .iter()
        .any(|column| column.column_id == ColumnId::UNSET)
    {
        return None;
    }
    Some(&agg.output_layout.group_key_columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_id::ColumnId;
    use crate::common::OutputColumn;
    use crate::optimizer::binder::bind;
    use crate::optimizer::memo::{MExpr, Memo};
    use crate::optimizer::operator::{
        AggStage, AggregateOutputLayout, LogicalAggregateOp, ScalarAggregateSpec, TopNOp,
        TopNPhase, ValuesOp,
    };
    use arrow::datatypes::DataType;

    fn output_column(id: u32, name: &str) -> OutputColumn {
        output_column_with_id(ColumnId::new_for_test(id), name)
    }

    fn full_aggregate_layout(
        group_by_len: usize,
        output_columns: &[OutputColumn],
    ) -> AggregateOutputLayout {
        AggregateOutputLayout::new(
            output_columns.iter().take(group_by_len).cloned().collect(),
            output_columns.iter().skip(group_by_len).cloned().collect(),
        )
    }

    fn output_column_with_id(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref(arena: &mut ScalarArena, id: u32) -> crate::optimizer::scalar::ScalarId {
        arena.intern(
            ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
            DataType::Int64,
            false,
        )
    }

    fn sort_key(arena: &mut ScalarArena, id: u32) -> SortKey {
        SortKey {
            expr: col_ref(arena, id),
            asc: true,
            nulls_first: true,
            display: None,
        }
    }

    fn sum_spec(sales: crate::optimizer::scalar::ScalarId) -> ScalarAggregateSpec {
        ScalarAggregateSpec {
            output_column_id: ColumnId::new_for_test(201),
            name: "sum".to_string(),
            args: vec![sales],
            distinct: false,
            order_by: vec![],
            resolved: crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false),
        }
    }

    fn global_agg(arena: &mut ScalarArena) -> LogicalAggregateOp {
        let group_by = vec![col_ref(arena, 1), col_ref(arena, 2)];
        let sales = col_ref(arena, 3);
        let aggregates = vec![sum_spec(sales)];
        let output_columns = vec![
            output_column(101, "k1"),
            output_column(102, "k2"),
            output_column(201, "sum_v"),
        ];
        let output_layout = full_aggregate_layout(group_by.len(), &output_columns);
        LogicalAggregateOp::staged(
            AggStage::Global,
            group_by,
            aggregates,
            output_layout,
            output_columns,
            vec![true],
            true,
        )
    }

    struct PreAggMemo {
        memo: Memo,
        root_group: usize,
        global_group: usize,
        local_group: usize,
        local_sort_column: ColumnId,
    }

    fn preagg_memo() -> PreAggMemo {
        let mut memo = Memo::new();
        let city = col_ref(&mut memo.scalars, 1);
        let sales = col_ref(&mut memo.scalars, 2);
        let local_city_output = output_column(1, "city");
        let global_city_output = output_column(101, "city");
        let sum_output = output_column(201, "sum_sales");
        let sum = sum_spec(sales);

        let values_id = memo.next_expr_id();
        let values_group = memo.new_group(MExpr {
            id: values_id,
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(1, "city"), output_column(2, "sales")],
            }),
            children: vec![],
        });

        let local_id = memo.next_expr_id();
        let local_group_by = vec![city];
        let local_aggregates = vec![sum.clone()];
        let local_output_columns = vec![local_city_output.clone(), sum_output.clone()];
        let local_output_layout =
            full_aggregate_layout(local_group_by.len(), &local_output_columns);
        let local_group = memo.new_group(MExpr {
            id: local_id,
            op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Local,
                local_group_by,
                local_aggregates,
                local_output_layout,
                local_output_columns,
                vec![false],
                true,
            )),
            children: vec![values_group],
        });

        let global_id = memo.next_expr_id();
        let global_group_by = vec![city];
        let global_aggregates = vec![sum];
        let global_output_columns = vec![global_city_output, sum_output];
        let global_output_layout =
            full_aggregate_layout(global_group_by.len(), &global_output_columns);
        let global_group = memo.new_group(MExpr {
            id: global_id,
            op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Global,
                global_group_by,
                global_aggregates,
                global_output_layout,
                global_output_columns,
                vec![true],
                true,
            )),
            children: vec![local_group],
        });

        let topn_id = memo.next_expr_id();
        let topn_items = vec![sort_key(&mut memo.scalars, 101)];
        let root_group = memo.new_group(MExpr {
            id: topn_id,
            op: Operator::LogicalTopN(TopNOp {
                items: topn_items,
                limit: Some(10),
                offset: None,
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![global_group],
        });

        PreAggMemo {
            memo,
            root_group,
            global_group,
            local_group,
            local_sort_column: ColumnId::new_for_test(1),
        }
    }

    fn root_expr(fixture: &PreAggMemo) -> MExpr {
        fixture.memo.groups[fixture.root_group].logical_exprs[0].clone()
    }

    fn assert_does_not_fire(fixture: &mut PreAggMemo) {
        let expr = root_expr(fixture);
        let out = PushDownTopNToPreAgg.apply(&expr, &mut fixture.memo);

        assert!(out.is_empty(), "expected PushDownTopNToPreAgg not to fire");
    }

    fn assert_preagg_rewrite_shape(out: &[NewExpr], memo: &Memo, original: &PreAggMemo) {
        assert_eq!(out.len(), 1);
        let Operator::LogicalTopN(root_topn) = &out[0].op else {
            panic!("expected root LogicalTopN");
        };
        assert_eq!(root_topn.phase, TopNPhase::Final);
        assert_eq!(root_topn.limit, Some(10));
        assert_eq!(root_topn.offset, None);
        assert!(!root_topn.is_split);
        assert_eq!(out[0].children.len(), 1);

        let new_global_group = out[0].children[0];
        assert_ne!(new_global_group, original.global_group);
        let new_global_expr = memo.groups[new_global_group]
            .logical_exprs
            .iter()
            .find(|expr| matches!(expr.op, Operator::LogicalAggregate(_)))
            .expect("expected new global aggregate group");
        let Operator::LogicalAggregate(new_global) = &new_global_expr.op else {
            unreachable!();
        };
        assert_eq!(new_global.stage, AggStage::Global);
        assert_eq!(new_global_expr.children.len(), 1);

        let partial_group = new_global_expr.children[0];
        let partial_expr = memo.groups[partial_group]
            .logical_exprs
            .iter()
            .find(|expr| matches!(expr.op, Operator::LogicalTopN(_)))
            .expect("expected partial TopN group");
        let Operator::LogicalTopN(partial) = &partial_expr.op else {
            unreachable!();
        };
        assert_eq!(partial.phase, TopNPhase::Partial);
        assert_eq!(partial.limit, Some(10));
        assert_eq!(partial.offset.unwrap_or(0), 0);
        assert!(partial.is_split);
        assert_eq!(partial.items.len(), 1);
        let ScalarNode::ColumnRef(partial_sort_column) = memo.scalars.node(partial.items[0].expr)
        else {
            panic!("expected partial TopN sort key to be a ColumnRef");
        };
        assert_eq!(*partial_sort_column, original.local_sort_column);
        assert_eq!(partial_expr.children, vec![original.local_group]);
    }

    fn partial_group_under_global(memo: &Memo, global_group: usize) -> usize {
        let global_expr = memo.groups[global_group]
            .logical_exprs
            .iter()
            .find(|expr| matches!(expr.op, Operator::LogicalAggregate(_)))
            .expect("expected global aggregate group");
        assert_eq!(global_expr.children.len(), 1);

        let partial_group = global_expr.children[0];
        let partial_expr = memo.groups[partial_group]
            .logical_exprs
            .iter()
            .find(|expr| matches!(expr.op, Operator::LogicalTopN(_)))
            .expect("expected partial TopN group");
        let Operator::LogicalTopN(partial) = &partial_expr.op else {
            unreachable!();
        };
        assert_eq!(partial.phase, TopNPhase::Partial);

        partial_group
    }

    #[test]
    fn order_by_group_keys_cover_all_group_by() {
        let mut arena = ScalarArena::new();
        let global = global_agg(&mut arena);
        let items = vec![sort_key(&mut arena, 101), sort_key(&mut arena, 102)];

        assert!(order_by_covers_group_by(&items, &global, &arena));
    }

    #[test]
    fn order_by_proper_group_key_subset_is_not_safe() {
        let mut arena = ScalarArena::new();
        let global = global_agg(&mut arena);
        let items = vec![sort_key(&mut arena, 101)];

        assert!(!order_by_covers_group_by(&items, &global, &arena));
    }

    #[test]
    fn order_by_aggregate_output_does_not_cover_group_by() {
        let mut arena = ScalarArena::new();
        let global = global_agg(&mut arena);
        let items = vec![sort_key(&mut arena, 201)];

        assert!(!order_by_covers_group_by(&items, &global, &arena));
    }

    #[test]
    fn order_by_rejects_malformed_group_output_prefix() {
        let mut arena = ScalarArena::new();
        let mut global = global_agg(&mut arena);
        global
            .output_layout
            .group_key_columns
            .truncate(global.group_by.len() - 1);
        let items = vec![sort_key(&mut arena, 101)];

        assert!(!order_by_covers_group_by(&items, &global, &arena));
    }

    #[test]
    fn order_by_rejects_unset_group_output() {
        let mut arena = ScalarArena::new();
        let mut global = global_agg(&mut arena);
        global.output_layout.group_key_columns[0] = output_column_with_id(ColumnId::UNSET, "bad");
        let items = vec![sort_key(&mut arena, 102)];

        assert!(!order_by_covers_group_by(&items, &global, &arena));
    }

    #[test]
    fn pattern_matches_topn_over_two_aggregates() {
        let pattern = PushDownTopNToPreAgg.pattern();

        let Pattern::Op { kind, children } = pattern else {
            panic!("expected TopN root pattern");
        };
        assert_eq!(kind, OpKind::TopN);
        assert_eq!(children.len(), 1);
        let Pattern::Op {
            kind,
            children: global_children,
        } = &children[0]
        else {
            panic!("expected Aggregate child pattern");
        };
        assert_eq!(*kind, OpKind::Aggregate);
        assert_eq!(global_children.len(), 1);
        let Pattern::Op {
            kind,
            children: local_children,
        } = &global_children[0]
        else {
            panic!("expected nested Aggregate child pattern");
        };
        assert_eq!(*kind, OpKind::Aggregate);
        assert_eq!(local_children, &vec![Pattern::Leaf]);
    }

    #[test]
    fn apply_pushes_partial_topn_between_global_and_local_aggregates() {
        let mut fixture = preagg_memo();
        let expr = root_expr(&fixture);

        let out = PushDownTopNToPreAgg.apply(&expr, &mut fixture.memo);

        assert_preagg_rewrite_shape(&out, &fixture.memo, &fixture);
    }

    #[test]
    fn apply_uses_layout_group_keys_when_public_outputs_are_pruned() {
        let mut fixture = preagg_memo();
        let Operator::LogicalAggregate(local) =
            &mut fixture.memo.groups[fixture.local_group].logical_exprs[0].op
        else {
            panic!("expected local LogicalAggregate");
        };
        local.output_columns = local.output_layout.aggregate_columns.clone();

        let Operator::LogicalAggregate(global) =
            &mut fixture.memo.groups[fixture.global_group].logical_exprs[0].op
        else {
            panic!("expected global LogicalAggregate");
        };
        global.output_columns = global.output_layout.aggregate_columns.clone();

        let expr = root_expr(&fixture);
        let out = PushDownTopNToPreAgg.apply(&expr, &mut fixture.memo);

        assert_preagg_rewrite_shape(&out, &fixture.memo, &fixture);
    }

    #[test]
    fn repeated_apply_reuses_intermediate_groups() {
        let mut fixture = preagg_memo();
        let expr = root_expr(&fixture);

        let first = PushDownTopNToPreAgg.apply(&expr, &mut fixture.memo);
        assert_eq!(first.len(), 1);
        let groups_after_first = fixture.memo.groups.len();
        let first_global_group = first[0].children[0];
        let first_partial_group = partial_group_under_global(&fixture.memo, first_global_group);

        let second = PushDownTopNToPreAgg.apply(&expr, &mut fixture.memo);
        assert_eq!(second.len(), 1);
        let second_global_group = second[0].children[0];
        let second_partial_group = partial_group_under_global(&fixture.memo, second_global_group);

        assert_eq!(fixture.memo.groups.len(), groups_after_first);
        assert_eq!(second_global_group, first_global_group);
        assert_eq!(second_partial_group, first_partial_group);
    }

    #[test]
    fn order_by_aggregate_output_does_not_fire() {
        let mut fixture = preagg_memo();
        let aggregate_output_sort_key = sort_key(&mut fixture.memo.scalars, 201);
        let Operator::LogicalTopN(topn) =
            &mut fixture.memo.groups[fixture.root_group].logical_exprs[0].op
        else {
            panic!("expected root LogicalTopN");
        };
        topn.items = vec![aggregate_output_sort_key];

        assert_does_not_fire(&mut fixture);
    }

    #[test]
    fn proper_group_key_subset_does_not_fire() {
        let mut memo = Memo::new();
        let city = col_ref(&mut memo.scalars, 1);
        let sku = col_ref(&mut memo.scalars, 2);
        let sales = col_ref(&mut memo.scalars, 3);
        let sum = sum_spec(sales);

        let values_id = memo.next_expr_id();
        let values_group = memo.new_group(MExpr {
            id: values_id,
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![
                    output_column(1, "city"),
                    output_column(2, "sku"),
                    output_column(3, "sales"),
                ],
            }),
            children: vec![],
        });

        let local_id = memo.next_expr_id();
        let local_group_by = vec![city, sku];
        let local_aggregates = vec![sum.clone()];
        let local_output_columns = vec![
            output_column(1, "city"),
            output_column(2, "sku"),
            output_column(201, "sum_sales"),
        ];
        let local_output_layout =
            full_aggregate_layout(local_group_by.len(), &local_output_columns);
        let local_group = memo.new_group(MExpr {
            id: local_id,
            op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Local,
                local_group_by,
                local_aggregates,
                local_output_layout,
                local_output_columns,
                vec![false],
                true,
            )),
            children: vec![values_group],
        });

        let global_id = memo.next_expr_id();
        let global_group_by = vec![city, sku];
        let global_aggregates = vec![sum];
        let global_output_columns = vec![
            output_column(101, "city"),
            output_column(102, "sku"),
            output_column(201, "sum_sales"),
        ];
        let global_output_layout =
            full_aggregate_layout(global_group_by.len(), &global_output_columns);
        let global_group = memo.new_group(MExpr {
            id: global_id,
            op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
                AggStage::Global,
                global_group_by,
                global_aggregates,
                global_output_layout,
                global_output_columns,
                vec![true],
                true,
            )),
            children: vec![local_group],
        });

        let topn = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![sort_key(&mut memo.scalars, 101)],
                limit: Some(10),
                offset: None,
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![global_group],
        };

        let out = PushDownTopNToPreAgg.apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "expected partial group-key ORDER BY not to fire"
        );
    }

    #[test]
    fn offset_does_not_fire() {
        let mut fixture = preagg_memo();
        let Operator::LogicalTopN(topn) =
            &mut fixture.memo.groups[fixture.root_group].logical_exprs[0].op
        else {
            panic!("expected root LogicalTopN");
        };
        topn.offset = Some(1);

        assert_does_not_fire(&mut fixture);
    }

    #[test]
    fn no_limit_does_not_fire() {
        let mut fixture = preagg_memo();
        let Operator::LogicalTopN(topn) =
            &mut fixture.memo.groups[fixture.root_group].logical_exprs[0].op
        else {
            panic!("expected root LogicalTopN");
        };
        topn.limit = None;

        assert_does_not_fire(&mut fixture);
    }

    #[test]
    fn wrong_stage_does_not_fire() {
        let mut fixture = preagg_memo();
        let Operator::LogicalAggregate(agg) =
            &mut fixture.memo.groups[fixture.global_group].logical_exprs[0].op
        else {
            panic!("expected global LogicalAggregate");
        };
        agg.stage = AggStage::Single;

        assert_does_not_fire(&mut fixture);
    }

    #[test]
    fn single_stage_aggregate_does_not_fire() {
        let mut memo = Memo::new();
        let city = col_ref(&mut memo.scalars, 1);
        let sales = col_ref(&mut memo.scalars, 2);
        let sum = sum_spec(sales);

        let values_id = memo.next_expr_id();
        let values_group = memo.new_group(MExpr {
            id: values_id,
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![output_column(1, "city"), output_column(2, "sales")],
            }),
            children: vec![],
        });

        let single_id = memo.next_expr_id();
        let single_group_by = vec![city];
        let single_aggregates = vec![sum];
        let single_output_columns = vec![output_column(1, "city"), output_column(201, "sum_sales")];
        let single_output_layout =
            full_aggregate_layout(single_group_by.len(), &single_output_columns);
        let single_group = memo.new_group(MExpr {
            id: single_id,
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                single_group_by,
                single_aggregates,
                single_output_layout,
                single_output_columns,
            )),
            children: vec![values_group],
        });

        let topn = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![sort_key(&mut memo.scalars, 1)],
                limit: Some(10),
                offset: None,
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![single_group],
        };

        let out = PushDownTopNToPreAgg.apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "expected single-stage aggregate not to fire"
        );
    }

    #[test]
    fn distinct_aggregate_does_not_fire() {
        let mut fixture = preagg_memo();
        let Operator::LogicalAggregate(agg) =
            &mut fixture.memo.groups[fixture.global_group].logical_exprs[0].op
        else {
            panic!("expected global LogicalAggregate");
        };
        agg.aggregates[0].distinct = true;

        assert_does_not_fire(&mut fixture);
    }

    #[test]
    fn local_group_key_output_mismatch_does_not_fire() {
        let mut fixture = preagg_memo();
        let Operator::LogicalAggregate(agg) =
            &mut fixture.memo.groups[fixture.local_group].logical_exprs[0].op
        else {
            panic!("expected local LogicalAggregate");
        };
        agg.output_layout.group_key_columns[0] = output_column_with_id(ColumnId::UNSET, "bad");

        assert_does_not_fire(&mut fixture);
    }

    #[test]
    fn apply_bound_pushes_partial_topn_between_global_and_local_aggregates() {
        let mut fixture = preagg_memo();
        let bindings = bind(
            &PushDownTopNToPreAgg.pattern(),
            &fixture.memo,
            fixture.root_group,
            0,
        );
        assert_eq!(bindings.len(), 1);

        let out = PushDownTopNToPreAgg.apply_bound(&bindings[0], &mut fixture.memo);

        assert_preagg_rewrite_shape(&out, &fixture.memo, &fixture);
    }
}
