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

use crate::common::JoinKind;
use crate::optimizer::binder::Binding;
use crate::optimizer::cascades_rules::split_aggregate::find_existing_logical_group;
use crate::optimizer::logical_props;
use crate::optimizer::memo::{GroupId, MExpr, Memo};
use crate::optimizer::operator::{LogicalJoinOp, Operator, TopNOp, TopNPhase};
use crate::optimizer::pattern::{OpKind, Pattern};
use crate::optimizer::property::ColumnIdSet;
use crate::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::optimizer::statistics::Confidence;
use crate::optimizer::topn_proof::{TopNWindow, collect_column_ids};

pub(crate) struct PushTopNThroughJoin;

impl Rule for PushTopNThroughJoin {
    fn name(&self) -> &str {
        "PushTopNThroughJoin"
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

        let join_group_id = expr.children[0];
        let Some(join_group) = memo.groups.get(join_group_id).cloned() else {
            return Vec::new();
        };
        let mut candidates = Vec::new();
        for join_expr in &join_group.logical_exprs {
            let Operator::LogicalJoin(join) = &join_expr.op else {
                continue;
            };
            if join_expr.children.len() != 2 {
                continue;
            }
            candidates.push((join.clone(), join_expr.children.clone()));
        }

        let mut results = Vec::new();
        for (join, join_children) in candidates {
            results.extend(rewrite_topn_through_join(
                topn,
                &join,
                &join_children,
                join_group_id,
                memo,
            ));
        }
        results
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::TopN,
            children: vec![Pattern::Op {
                kind: OpKind::Join,
                children: vec![Pattern::Leaf, Pattern::Leaf],
            }],
        }
    }

    fn apply_bound(&self, binding: &Binding, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTopN(topn) = binding.op(memo, 0).clone() else {
            return Vec::new();
        };
        let Operator::LogicalJoin(join) = binding.op(memo, 1).clone() else {
            return Vec::new();
        };
        let Some(&join_group_id) = binding.children(0).first() else {
            return Vec::new();
        };
        rewrite_topn_through_join(&topn, &join, binding.children(1), join_group_id, memo)
    }
}

fn rewrite_topn_through_join(
    topn: &TopNOp,
    join: &LogicalJoinOp,
    join_children: &[GroupId],
    join_group_id: GroupId,
    memo: &mut Memo,
) -> Vec<NewExpr> {
    if join_children.len() != 2 {
        return Vec::new();
    }
    if topn.phase != TopNPhase::Final || topn.is_split {
        return Vec::new();
    }
    let Some(window) = TopNWindow::from_limit_offset(topn.limit, topn.offset) else {
        return Vec::new();
    };
    if window.offset != 0 {
        return Vec::new();
    }
    if !is_primary_join_alternative(memo, join_group_id, join, join_children) {
        return Vec::new();
    }

    // The pushed limit suppresses join-condition evaluation for removed rows.
    // Keep an errorful condition above its original complete input domain.
    if join
        .condition
        .is_some_and(|condition| crate::optimizer::scalar_expr::can_fail(&memo.scalars, condition))
    {
        return Vec::new();
    }

    let Some(preserved_idx) = preserved_child_index(join.join_type) else {
        return Vec::new();
    };
    let preserved_group = join_children[preserved_idx];
    if group_has_logical_topn(memo, preserved_group) {
        return Vec::new();
    }
    if !sort_keys_reference_only_group_outputs(topn, preserved_group, memo) {
        return Vec::new();
    }

    let pushed_op = Operator::LogicalTopN(TopNOp {
        items: topn.items.clone(),
        limit: topn.limit,
        offset: Some(0),
        phase: TopNPhase::Final,
        is_split: false,
    });
    let pushed_children = vec![preserved_group];
    let pushed_group = find_existing_logical_group(memo, &pushed_op, &pushed_children)
        .unwrap_or_else(|| {
            let pushed_id = memo.next_expr_id();
            memo.new_group(MExpr {
                id: pushed_id,
                op: pushed_op,
                children: pushed_children,
            })
        });
    seed_pushed_topn_group_props_if_missing(memo, pushed_group);

    let mut new_join_children = join_children.to_vec();
    new_join_children[preserved_idx] = pushed_group;
    let pushed_join_op = Operator::LogicalJoin(join.clone());
    let pushed_join_group = find_existing_logical_group(memo, &pushed_join_op, &new_join_children)
        .unwrap_or_else(|| {
            let join_id = memo.next_expr_id();
            memo.new_group(MExpr {
                id: join_id,
                op: pushed_join_op,
                children: new_join_children.clone(),
            })
        });

    vec![NewExpr {
        op: Operator::LogicalTopN(topn.clone()),
        children: vec![pushed_join_group],
    }]
}

fn is_primary_join_alternative(
    memo: &Memo,
    join_group_id: GroupId,
    join: &LogicalJoinOp,
    join_children: &[GroupId],
) -> bool {
    let Some(group) = memo.groups.get(join_group_id) else {
        return false;
    };
    let Some(primary) = group
        .logical_exprs
        .iter()
        .find(|expr| matches!(expr.op, Operator::LogicalJoin(_)))
    else {
        return false;
    };
    let Operator::LogicalJoin(primary_join) = &primary.op else {
        return false;
    };
    primary_join.join_type == join.join_type
        && primary_join.condition == join.condition
        && primary.children == join_children
}

fn seed_pushed_topn_group_props_if_missing(memo: &mut Memo, group_id: GroupId) {
    if memo.groups[group_id].logical_props.is_none() {
        seed_pushed_topn_group_props(memo, group_id);
    }
}

fn seed_pushed_topn_group_props(memo: &mut Memo, group_id: GroupId) {
    let Some(expr) = memo.groups[group_id].logical_exprs.first().cloned() else {
        return;
    };
    let Operator::LogicalTopN(topn) = &expr.op else {
        return;
    };
    let Some(&child_group) = expr.children.first() else {
        return;
    };
    let Some(child_props) = memo
        .groups
        .get(child_group)
        .and_then(|group| group.logical_props.as_ref())
        .cloned()
    else {
        return;
    };
    let output_rows = match (topn.limit, topn.offset) {
        (Some(limit), Some(offset)) => {
            ((limit as f64) + (offset as f64)).min(child_props.row_count)
        }
        (Some(limit), None) => (limit as f64).min(child_props.row_count),
        _ => child_props.row_count,
    };

    memo.groups[group_id].logical_props = Some(logical_props::derive_for_expr(
        &expr,
        memo,
        child_props.output_columns,
        output_rows.max(0.0),
        Confidence::Estimated,
        child_props.column_statistics,
    ));
}

fn preserved_child_index(join_type: JoinKind) -> Option<usize> {
    match join_type {
        JoinKind::LeftOuter => Some(0),
        _ => None,
    }
}

fn group_has_logical_topn(memo: &Memo, group_id: GroupId) -> bool {
    memo.groups
        .get(group_id)
        .map(|group| {
            group
                .logical_exprs
                .iter()
                .any(|expr| matches!(expr.op, Operator::LogicalTopN(_)))
        })
        .unwrap_or(false)
}

fn sort_keys_reference_only_group_outputs(
    topn: &TopNOp,
    preserved_group: GroupId,
    memo: &Memo,
) -> bool {
    if topn.items.is_empty() {
        return false;
    }
    let Some(props) = memo
        .groups
        .get(preserved_group)
        .and_then(|group| group.logical_props.as_ref())
    else {
        return false;
    };
    let preserved_columns =
        ColumnIdSet::from_columns(props.output_columns.iter().map(|column| column.column_id));
    if preserved_columns.is_empty() {
        return false;
    }
    topn.items.iter().all(|item| {
        let item_columns = collect_column_ids(&memo.scalars, item.expr);
        !item_columns.is_empty() && item_columns.is_subset(&preserved_columns)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_id::ColumnId;
    use crate::common::BinOp;
    use crate::common::OutputColumn;
    use crate::optimizer::binder::bind;
    use crate::optimizer::memo::LogicalProperties;
    use crate::optimizer::operator::{ProjectOp, ScalarProjectItem, ValuesOp};
    use crate::optimizer::property::PhysicalPropertySet;
    use crate::optimizer::scalar::{ColumnDisplay, ScalarNode, SortKey as ScalarSortKey};
    use crate::optimizer::stats_input::{OptimizerStatsInput, QueryStatsSnapshot};
    use arrow::datatypes::DataType;
    use std::time::{Duration, Instant};

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn values_group(memo: &mut Memo, column_ids: &[u32]) -> GroupId {
        let columns = column_ids
            .iter()
            .map(|id| output_column(*id, &format!("c{id}")))
            .collect::<Vec<_>>();
        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }),
            children: vec![],
        });
        memo.groups[group].logical_props = Some(LogicalProperties::new(columns, 0.0));
        group
    }

    fn join_group(memo: &mut Memo, kind: JoinKind, left: GroupId, right: GroupId) -> GroupId {
        join_group_with_condition(memo, kind, left, right, None)
    }

    fn join_group_with_condition(
        memo: &mut Memo,
        kind: JoinKind,
        left: GroupId,
        right: GroupId,
        condition: Option<crate::optimizer::scalar::ScalarId>,
    ) -> GroupId {
        let output_columns = memo.groups[left]
            .logical_props
            .as_ref()
            .into_iter()
            .flat_map(|props| props.output_columns.iter().cloned())
            .chain(
                memo.groups[right]
                    .logical_props
                    .as_ref()
                    .into_iter()
                    .flat_map(|props| props.output_columns.iter().cloned()),
            )
            .collect();
        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: kind,
                condition,
            }),
            children: vec![left, right],
        });
        memo.groups[group].logical_props = Some(LogicalProperties::new(output_columns, 0.0));
        group
    }

    fn eq_condition(memo: &mut Memo, left: u32, right: u32) -> crate::optimizer::scalar::ScalarId {
        let left =
            memo.scalars
                .intern(ScalarNode::ColumnRef(ColumnId(left)), DataType::Int64, true);
        let right = memo.scalars.intern(
            ScalarNode::ColumnRef(ColumnId(right)),
            DataType::Int64,
            true,
        );
        memo.scalars.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            DataType::Boolean,
            true,
        )
    }

    fn sort_key(memo: &mut Memo, id: u32) -> ScalarSortKey {
        let expr = memo
            .scalars
            .intern(ScalarNode::ColumnRef(ColumnId(id)), DataType::Int64, true);
        ScalarSortKey {
            expr,
            asc: true,
            nulls_first: false,
            display: None,
        }
    }

    fn binary_sort_key(memo: &mut Memo, left: u32, right: u32) -> ScalarSortKey {
        let left =
            memo.scalars
                .intern(ScalarNode::ColumnRef(ColumnId(left)), DataType::Int64, true);
        let right = memo.scalars.intern(
            ScalarNode::ColumnRef(ColumnId(right)),
            DataType::Int64,
            true,
        );
        let expr = memo.scalars.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Add,
                left,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            DataType::Int64,
            true,
        );
        ScalarSortKey {
            expr,
            asc: true,
            nulls_first: false,
            display: None,
        }
    }

    fn project_passthrough_group(
        memo: &mut Memo,
        child: GroupId,
        mappings: &[(u32, u32, &str)],
    ) -> GroupId {
        let items = mappings
            .iter()
            .map(|(input_id, output_id, name)| {
                let expr = memo.scalars.intern(
                    ScalarNode::ColumnRef(ColumnId(*input_id)),
                    DataType::Int64,
                    true,
                );
                ScalarProjectItem {
                    expr,
                    output_name: (*name).to_string(),
                    output_column_id: ColumnId(*output_id),
                    expr_display: Some(ColumnDisplay {
                        qualifier: None,
                        column: (*name).to_string(),
                    }),
                }
            })
            .collect();
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalProject(ProjectOp {
                items,
                output_qualifier: None,
            }),
            children: vec![child],
        })
    }

    fn has_physical_hash_join_with_pushed_preserved_topn(memo: &Memo) -> bool {
        memo.groups.iter().any(|group| {
            group.physical_exprs.iter().any(|expr| {
                let Operator::PhysicalHashJoin(join) = &expr.op else {
                    return false;
                };
                let Some(idx) = preserved_child_index(join.join_type) else {
                    return false;
                };
                expr.children.get(idx).is_some_and(|child| {
                    memo.groups[*child]
                        .logical_exprs
                        .iter()
                        .any(|child_expr| matches!(child_expr.op, Operator::LogicalTopN(_)))
                })
            })
        })
    }

    fn topn_with_key(
        memo: &mut Memo,
        item: ScalarSortKey,
        limit: Option<i64>,
        offset: Option<i64>,
        phase: TopNPhase,
        is_split: bool,
        child: GroupId,
    ) -> MExpr {
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![item],
                limit,
                offset,
                phase,
                is_split,
            }),
            children: vec![child],
        }
    }

    fn topn_group(
        memo: &mut Memo,
        item: ScalarSortKey,
        limit: Option<i64>,
        offset: Option<i64>,
        phase: TopNPhase,
        is_split: bool,
        child: GroupId,
    ) -> GroupId {
        let expr = topn_with_key(memo, item, limit, offset, phase, is_split, child);
        memo.new_group(expr)
    }

    fn assert_rewrite_pushes_preserved_side(
        memo: &Memo,
        rewrite: &NewExpr,
        preserved_original: GroupId,
        null_producing_original: GroupId,
        preserved_idx: usize,
    ) {
        match &rewrite.op {
            Operator::LogicalTopN(topn) => {
                assert_eq!(topn.limit, Some(10));
                assert_eq!(topn.offset, Some(0));
                assert_eq!(topn.phase, TopNPhase::Final);
                assert!(!topn.is_split);
            }
            other => panic!("expected final LogicalTopN, got {other:?}"),
        }
        assert_eq!(rewrite.children.len(), 1);
        let pushed_join_group = rewrite.children[0];
        let pushed_join = memo.groups[pushed_join_group]
            .logical_exprs
            .iter()
            .find(|expr| matches!(expr.op, Operator::LogicalJoin(_)))
            .expect("rewrite should create a pushed join group");
        let pushed_preserved_group = pushed_join.children[preserved_idx];
        let other_idx = 1 - preserved_idx;
        assert_eq!(pushed_join.children[other_idx], null_producing_original);

        let pushed_topn = memo.groups[pushed_preserved_group]
            .logical_exprs
            .iter()
            .find(|expr| matches!(expr.op, Operator::LogicalTopN(_)))
            .expect("preserved side should be replaced with a pushed LogicalTopN");
        assert_eq!(pushed_topn.children, vec![preserved_original]);
        match &pushed_topn.op {
            Operator::LogicalTopN(topn) => {
                assert_eq!(topn.limit, Some(10));
                assert_eq!(topn.offset, Some(0));
                assert_eq!(topn.phase, TopNPhase::Final);
                assert!(!topn.is_split);
            }
            other => panic!("expected pushed LogicalTopN, got {other:?}"),
        }
    }

    #[test]
    fn left_outer_pushes_topn_to_left_preserved_side() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        let condition = eq_condition(&mut memo, 1, 2);
        let join =
            join_group_with_condition(&mut memo, JoinKind::LeftOuter, left, right, Some(condition));
        let item = sort_key(&mut memo, 1);
        let topn = topn_with_key(
            &mut memo,
            item,
            Some(10),
            Some(0),
            TopNPhase::Final,
            false,
            join,
        );

        let out = PushTopNThroughJoin.apply(&topn, &mut memo);

        assert_eq!(out.len(), 1);
        assert_rewrite_pushes_preserved_side(&memo, &out[0], left, right, 0);
    }

    #[test]
    fn right_outer_fails_closed_until_stream_projection_is_executable() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        let join = join_group(&mut memo, JoinKind::RightOuter, left, right);
        let item = sort_key(&mut memo, 2);
        let topn = topn_with_key(
            &mut memo,
            item,
            Some(10),
            Some(0),
            TopNPhase::Final,
            false,
            join,
        );

        let out = PushTopNThroughJoin.apply(&topn, &mut memo);

        assert!(
            out.is_empty(),
            "right outer TopN pushdown must remain disabled until the preserved-side sort alias is executable across stream boundaries"
        );
    }

    #[test]
    fn commuted_left_outer_alternative_does_not_reenable_right_outer_pushdown() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        let join = join_group(&mut memo, JoinKind::RightOuter, left, right);
        let commuted_id = memo.next_expr_id();
        memo.groups[join].logical_exprs.push(MExpr {
            id: commuted_id,
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftOuter,
                condition: None,
            }),
            children: vec![right, left],
        });
        let item = sort_key(&mut memo, 2);
        let topn_group = topn_group(
            &mut memo,
            item,
            Some(10),
            Some(0),
            TopNPhase::Final,
            false,
            join,
        );
        let bindings = bind(&PushTopNThroughJoin.pattern(), &memo, topn_group, 0);
        assert_eq!(bindings.len(), 2);

        let rewrites = bindings
            .iter()
            .flat_map(|binding| PushTopNThroughJoin.apply_bound(binding, &mut memo))
            .collect::<Vec<_>>();

        assert!(
            rewrites.is_empty(),
            "the commuted LEFT OUTER alternative must not bypass the original RIGHT OUTER fail-closed guard"
        );
    }

    #[test]
    fn rejects_inner_full_semi_anti_cross_joins() {
        for kind in [
            JoinKind::Inner,
            JoinKind::FullOuter,
            JoinKind::Cross,
            JoinKind::LeftSemi,
            JoinKind::RightSemi,
            JoinKind::LeftAnti,
            JoinKind::RightAnti,
            JoinKind::NullAwareLeftAnti,
        ] {
            let mut memo = Memo::new();
            let left = values_group(&mut memo, &[1]);
            let right = values_group(&mut memo, &[2]);
            let join = join_group(&mut memo, kind, left, right);
            let item = sort_key(&mut memo, 1);
            let topn = topn_with_key(
                &mut memo,
                item,
                Some(10),
                Some(0),
                TopNPhase::Final,
                false,
                join,
            );

            let out = PushTopNThroughJoin.apply(&topn, &mut memo);

            assert!(out.is_empty(), "{kind:?} must not push TopN through Join");
        }
    }

    #[test]
    fn rejects_sort_key_from_null_producing_side() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        let condition = eq_condition(&mut memo, 1, 2);
        let join =
            join_group_with_condition(&mut memo, JoinKind::LeftOuter, left, right, Some(condition));
        let item = sort_key(&mut memo, 2);
        let topn = topn_with_key(
            &mut memo,
            item,
            Some(10),
            Some(0),
            TopNPhase::Final,
            false,
            join,
        );

        let out = PushTopNThroughJoin.apply(&topn, &mut memo);

        assert!(out.is_empty());
    }

    #[test]
    fn rejects_sort_key_spanning_both_sides() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        let condition = eq_condition(&mut memo, 1, 2);
        let join =
            join_group_with_condition(&mut memo, JoinKind::LeftOuter, left, right, Some(condition));
        let item = binary_sort_key(&mut memo, 1, 2);
        let topn = topn_with_key(
            &mut memo,
            item,
            Some(10),
            Some(0),
            TopNPhase::Final,
            false,
            join,
        );

        let out = PushTopNThroughJoin.apply(&topn, &mut memo);

        assert!(out.is_empty());
    }

    #[test]
    fn rejects_missing_limit_offset_or_split_topn() {
        let cases = [
            (None, Some(0), TopNPhase::Final, false),
            (Some(10), Some(1), TopNPhase::Final, false),
            (Some(10), Some(0), TopNPhase::Partial, false),
            (Some(10), Some(0), TopNPhase::Final, true),
        ];
        for (limit, offset, phase, is_split) in cases {
            let mut memo = Memo::new();
            let left = values_group(&mut memo, &[1]);
            let right = values_group(&mut memo, &[2]);
            let join = join_group(&mut memo, JoinKind::LeftOuter, left, right);
            let item = sort_key(&mut memo, 1);
            let topn = topn_with_key(&mut memo, item, limit, offset, phase, is_split, join);

            let out = PushTopNThroughJoin.apply(&topn, &mut memo);

            assert!(out.is_empty());
        }
    }

    #[test]
    fn idempotency_guard_rejects_already_pushed_preserved_side_topn() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        let join = join_group(&mut memo, JoinKind::LeftOuter, left, right);
        let item = sort_key(&mut memo, 1);
        let topn = topn_with_key(
            &mut memo,
            item,
            Some(10),
            Some(0),
            TopNPhase::Final,
            false,
            join,
        );

        let first = PushTopNThroughJoin.apply(&topn, &mut memo);
        assert_eq!(first.len(), 1);
        let first = first.into_iter().next().unwrap();
        let pushed_root = MExpr {
            id: memo.next_expr_id(),
            op: first.op,
            children: first.children,
        };

        let second = PushTopNThroughJoin.apply(&pushed_root, &mut memo);

        assert!(second.is_empty());
    }

    #[test]
    fn stamps_props_when_reusing_existing_pushed_topn_group() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        let item = sort_key(&mut memo, 1);
        let existing_pushed_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![item.clone()],
                limit: Some(10),
                offset: Some(0),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![left],
        });
        assert!(memo.groups[existing_pushed_group].logical_props.is_none());
        let condition = eq_condition(&mut memo, 1, 2);
        let join =
            join_group_with_condition(&mut memo, JoinKind::LeftOuter, left, right, Some(condition));
        let topn = topn_with_key(
            &mut memo,
            item,
            Some(10),
            Some(0),
            TopNPhase::Final,
            false,
            join,
        );

        let out = PushTopNThroughJoin.apply(&topn, &mut memo);

        assert_eq!(out.len(), 1);
        assert!(
            memo.groups[existing_pushed_group].logical_props.is_some(),
            "reused pushed TopN group must be stamped before implement()"
        );
        assert_rewrite_pushes_preserved_side(&memo, &out[0], left, right, 0);
    }

    #[test]
    fn apply_bound_pushes_left_outer_topn() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        let join = join_group(&mut memo, JoinKind::LeftOuter, left, right);
        let item = sort_key(&mut memo, 1);
        let topn_group = topn_group(
            &mut memo,
            item,
            Some(10),
            Some(0),
            TopNPhase::Final,
            false,
            join,
        );
        let bindings = bind(&PushTopNThroughJoin.pattern(), &memo, topn_group, 0);
        assert_eq!(bindings.len(), 1);

        let out = PushTopNThroughJoin.apply_bound(&bindings[0], &mut memo);

        assert_eq!(out.len(), 1);
        assert_rewrite_pushes_preserved_side(&memo, &out[0], left, right, 0);
    }

    #[test]
    fn explore_finds_join_pushdown_after_project_pushdown() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        let join = join_group(&mut memo, JoinKind::LeftOuter, left, right);
        let project =
            project_passthrough_group(&mut memo, join, &[(1, 10, "score"), (2, 20, "payload")]);
        let item = sort_key(&mut memo, 10);
        let _root = topn_group(
            &mut memo,
            item,
            Some(10),
            Some(0),
            TopNPhase::Final,
            false,
            project,
        );

        crate::optimizer::explore(
            &mut memo,
            &crate::optimizer::cascades_rules::all_transformation_rules(),
            &crate::optimizer::options::OptimizerOptions::default_settings(),
            Instant::now() + Duration::from_secs(5),
        )
        .expect("explore should finish");

        let pushed = memo.groups.iter().any(|group| {
            group.logical_exprs.iter().any(|expr| {
                matches!(expr.op, Operator::LogicalJoin(_))
                    && expr.children.first().is_some_and(|child| {
                        memo.groups[*child].logical_exprs.iter().any(|child_expr| {
                            matches!(child_expr.op, Operator::LogicalTopN(_))
                                && child_expr.children == vec![left]
                        })
                    })
                    && expr.children.get(1) == Some(&right)
            })
        });

        assert!(
            pushed,
            "explore should chain Project pushdown into Join preserved-side TopN pushdown"
        );
    }

    #[test]
    fn project_then_join_pushdown_stays_hash_join_implementable() {
        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[1]);
        let right = values_group(&mut memo, &[2]);
        memo.groups[left].logical_props = Some(LogicalProperties::new(
            vec![output_column(1, "score")],
            100_000.0,
        ));
        memo.groups[right].logical_props = Some(LogicalProperties::new(
            vec![output_column(2, "payload")],
            1_000.0,
        ));
        let condition = eq_condition(&mut memo, 1, 2);
        let join =
            join_group_with_condition(&mut memo, JoinKind::LeftOuter, left, right, Some(condition));
        let project =
            project_passthrough_group(&mut memo, join, &[(1, 10, "score"), (2, 20, "payload")]);
        let item = sort_key(&mut memo, 10);
        let root = topn_group(
            &mut memo,
            item,
            Some(1),
            Some(0),
            TopNPhase::Final,
            false,
            project,
        );
        let options = crate::optimizer::options::OptimizerOptions::default_settings();
        let stats_input = OptimizerStatsInput::from_query_stats(&QueryStatsSnapshot::empty());

        crate::optimizer::explore(
            &mut memo,
            &crate::optimizer::cascades_rules::all_transformation_rules(),
            &options,
            Instant::now() + Duration::from_secs(5),
        )
        .expect("explore should finish");
        crate::optimizer::implement(
            &mut memo,
            &crate::optimizer::cascades_rules::all_implementation_rules(),
            &options,
        );
        assert!(
            has_physical_hash_join_with_pushed_preserved_topn(&memo),
            "pushed join must stay hash-join implementable before post-explore stats derivation"
        );
        crate::optimizer::stats::derive_group_statistics(&mut memo, &stats_input);

        let required = PhysicalPropertySet::gather();
        let mut ctx =
            crate::optimizer::search::SearchContext::new(stats_input, options.cost_options.clone());
        let total_cost = ctx
            .optimize_group(&memo, root, &required)
            .expect("search should finish");
        assert!(
            total_cost.is_finite(),
            "search should keep a feasible plan after adding the pushed join alternative"
        );
    }

    fn checked_decimal_join_condition(
        memo: &mut Memo,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
    ) -> crate::optimizer::scalar::ScalarId {
        let equality = eq_condition(memo, 1, 2);
        let source = memo
            .scalars
            .intern(ScalarNode::ColumnRef(ColumnId(1)), DataType::Int64, true);
        let cast = memo.scalars.intern(
            ScalarNode::Cast {
                child: source,
                target: DataType::Decimal128(2, 0),
                decimal_overflow_policy: policy,
            },
            DataType::Decimal128(2, 0),
            true,
        );
        let checked = memo.scalars.intern(
            ScalarNode::IsNull {
                child: cast,
                negated: true,
            },
            DataType::Boolean,
            false,
        );
        memo.scalars.intern(
            ScalarNode::BinaryOp {
                left: equality,
                op: BinOp::And,
                right: checked,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            DataType::Boolean,
            true,
        )
    }

    #[test]
    fn throwing_outer_join_condition_prevents_topn_input_reduction() {
        use novarocks_type_contract::DecimalOverflowPolicy as Policy;
        for policy in [Policy::OutputNull, Policy::ReportError] {
            let mut memo = Memo::new();
            let left = values_group(&mut memo, &[1]);
            let right = values_group(&mut memo, &[2]);
            let condition = checked_decimal_join_condition(&mut memo, policy);
            let join = join_group_with_condition(
                &mut memo,
                JoinKind::LeftOuter,
                left,
                right,
                Some(condition),
            );
            let item = sort_key(&mut memo, 1);
            let topn = topn_with_key(
                &mut memo,
                item,
                Some(10),
                Some(0),
                TopNPhase::Final,
                false,
                join,
            );
            let groups_before = memo.groups.len();
            let out = PushTopNThroughJoin.apply(&topn, &mut memo);
            if policy == Policy::ReportError {
                assert!(out.is_empty());
                assert_eq!(
                    memo.groups.len(),
                    groups_before,
                    "rejected rule must not add pruned groups"
                );
            } else {
                assert_eq!(out.len(), 1);
                assert_rewrite_pushes_preserved_side(&memo, &out[0], left, right, 0);
            }
        }
    }

    #[test]
    fn bound_outer_join_rule_preserves_throwing_condition_domain() {
        use novarocks_type_contract::DecimalOverflowPolicy as Policy;
        for policy in [Policy::OutputNull, Policy::ReportError] {
            let mut memo = Memo::new();
            let left = values_group(&mut memo, &[1]);
            let right = values_group(&mut memo, &[2]);
            let condition = checked_decimal_join_condition(&mut memo, policy);
            let join = join_group_with_condition(
                &mut memo,
                JoinKind::LeftOuter,
                left,
                right,
                Some(condition),
            );
            let item = sort_key(&mut memo, 1);
            let topn = topn_group(
                &mut memo,
                item,
                Some(10),
                Some(0),
                TopNPhase::Final,
                false,
                join,
            );
            let bindings = bind(&PushTopNThroughJoin.pattern(), &memo, topn, 0);
            assert_eq!(bindings.len(), 1);
            let groups_before = memo.groups.len();
            let out = PushTopNThroughJoin.apply_bound(&bindings[0], &mut memo);
            if policy == Policy::ReportError {
                assert!(out.is_empty());
                assert_eq!(memo.groups.len(), groups_before);
            } else {
                assert_eq!(out.len(), 1);
                assert_rewrite_pushes_preserved_side(&memo, &out[0], left, right, 0);
            }
        }
    }
}
