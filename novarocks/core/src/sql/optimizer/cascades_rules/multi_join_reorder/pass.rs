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

//! The one-shot in-memo join-reorder pass.
//!
//! Walks the memo, finds inner/cross join-chain roots, and for each chain larger
//! than the exhaustive threshold injects multiple candidate orders (LeftDeep +
//! DP + Greedy-TopK) as alternative expressions in the chain's group, for the
//! cost search to choose. Faithful to StarRocks `ReorderJoinRule.transform` +
//! `Memo.copyIn` (a single imperative pass, not a fixpoint rule). Invoked from
//! `optimize()` after `derive_group_statistics`.

use std::collections::HashSet;

use crate::sql::common::JoinKind;
use crate::sql::optimizer::memo::{GroupId, JoinTree, MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalJoinOp, Operator};
use crate::sql::optimizer::statistics::Confidence;
use crate::sql::optimizer::stats::copy_in_join_tree;
use crate::sql::optimizer::stats_input::OptimizerStatsInput;

use super::{ReorderCaps, enumerate_orders, flatten_join_chain};

/// Knobs for the reorder pass, mirroring StarRocks session variables. Defaults
/// match StarRocks (`SessionVariable`); Phase 5 threads these from
/// `OptimizerOptions`/`SessionOptimizerSettings`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReorderOptions {
    pub(crate) enable_dp: bool,
    pub(crate) enable_greedy: bool,
    /// Master gate: chains larger than this are not reordered.
    pub(crate) max_reorder_node: usize,
    /// Chains this size or smaller are left to `JoinAssociativity` (D2/M3).
    pub(crate) max_reorder_node_use_exhaustive: usize,
    pub(crate) max_reorder_node_use_dp: usize,
    pub(crate) max_reorder_node_use_greedy: usize,
    pub(crate) topk: usize,
}

impl Default for ReorderOptions {
    fn default() -> Self {
        Self {
            enable_dp: true,
            enable_greedy: true,
            max_reorder_node: 50,
            max_reorder_node_use_exhaustive: 4,
            max_reorder_node_use_dp: 10,
            max_reorder_node_use_greedy: 16,
            topk: 10,
        }
    }
}

impl ReorderOptions {
    fn caps(&self) -> ReorderCaps {
        ReorderCaps {
            enable_dp: self.enable_dp,
            max_dp: self.max_reorder_node_use_dp,
            enable_greedy: self.enable_greedy,
            max_greedy: self.max_reorder_node_use_greedy,
            topk: self.topk,
        }
    }
}

/// Inject multi-candidate join orders into every reorderable inner/cross chain.
pub(crate) fn run_multi_join_reorder(
    memo: &mut Memo,
    opts: &ReorderOptions,
    stats_input: &OptimizerStatsInput,
) {
    // Snapshot the chain roots before injecting, so the new alternative groups
    // (appended at higher indices) are not themselves reprocessed.
    for root in find_chain_roots(memo) {
        reorder_chain(memo, root, opts, stats_input);
    }
}

fn reorder_chain(
    memo: &mut Memo,
    root: GroupId,
    opts: &ReorderOptions,
    stats_input: &OptimizerStatsInput,
) {
    let Some(graph) = flatten_join_chain(memo, root) else {
        return;
    };
    let n = graph.atom_count();
    // Small chains stay with JoinAssociativity; oversized chains are skipped.
    if n <= opts.max_reorder_node_use_exhaustive || n > opts.max_reorder_node {
        return;
    }
    // This chain is reorder-owned: record its join groups so explore's
    // JoinAssociativity skips them and does not re-enumerate the orders we are
    // about to inject (D2: reorder/associativity mutual exclusion).
    memo.reorder_owned_groups
        .extend(graph.chain_join_groups.iter().copied());
    // Degrade to LeftDeep-only when base statistics are unknown (StarRocks
    // `Utils.hasUnknownColumnsStats`).
    let mut caps = opts.caps();
    if graph
        .atom_stats
        .iter()
        .any(|s| s.row_count_confidence == Confidence::Fallback)
    {
        caps.enable_dp = false;
        caps.enable_greedy = false;
    }
    for tree in enumerate_orders(&graph, caps, &mut memo.scalars) {
        inject_candidate(memo, root, tree, stats_input);
    }
}

/// Materialize a candidate order's sub-trees into the memo and add its root join
/// as an alternative expression in the chain-root group (deduplicated).
fn inject_candidate(
    memo: &mut Memo,
    root: GroupId,
    tree: JoinTree,
    stats_input: &OptimizerStatsInput,
) {
    let JoinTree::Join { left, right, op } = tree else {
        return; // a reorder candidate over >= 2 atoms is always a join
    };
    let left_id = copy_in_join_tree(memo, &left, stats_input);
    let right_id = copy_in_join_tree(memo, &right, stats_input);
    let new_op = Operator::LogicalJoin(op);
    let children = vec![left_id, right_id];
    let already_present = memo.groups[root]
        .logical_exprs
        .iter()
        .any(|e| e.children == children && format!("{:?}", e.op) == format!("{new_op:?}"));
    if already_present {
        return;
    }
    let id = memo.next_expr_id();
    memo.add_expr_to_group(
        root,
        MExpr {
            id,
            op: new_op,
            children,
        },
    );
}

/// Chain roots: inner/cross join groups that are not themselves the inner/cross
/// join child of another inner/cross join. Each maximal chain is reordered once;
/// chains nested under non-join atoms (e.g. under an aggregate) are still found
/// because their root is not a join's child.
fn find_chain_roots(memo: &Memo) -> Vec<GroupId> {
    let mut mid_chain: HashSet<GroupId> = HashSet::new();
    for group in &memo.groups {
        if let Some(expr) = group.logical_exprs.first() {
            if is_inner_cross_join_op(&expr.op) {
                for &child in &expr.children {
                    if child_is_inner_cross_join(memo, child) {
                        mid_chain.insert(child);
                    }
                }
            }
        }
    }
    (0..memo.groups.len())
        .filter(|g| {
            !mid_chain.contains(g)
                && memo.groups[*g]
                    .logical_exprs
                    .first()
                    .is_some_and(|e| is_inner_cross_join_op(&e.op))
        })
        .collect()
}

fn is_inner_cross_join_op(op: &Operator) -> bool {
    matches!(
        op,
        Operator::LogicalJoin(LogicalJoinOp { join_type, .. })
            if matches!(join_type, JoinKind::Inner | JoinKind::Cross)
    )
}

fn child_is_inner_cross_join(memo: &Memo, group: GroupId) -> bool {
    memo.groups
        .get(group)
        .and_then(|g| g.logical_exprs.first())
        .is_some_and(|e| is_inner_cross_join_op(&e.op))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::LogicalProperties;
    use crate::sql::optimizer::operator::ValuesOp;
    use crate::sql::optimizer::statistics::ColumnStatistic;
    use crate::sql::optimizer::stats_input::OptimizerStatsInput;
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use std::collections::HashMap;

    fn empty_stats_input() -> OptimizerStatsInput {
        OptimizerStatsInput::from_test_table_statistics(&HashMap::new())
    }

    fn col(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn eq(l: TypedExpr, r: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(l),
                op: BinOp::Eq,
                right: Box::new(r),
            },
            data_type: arrow::datatypes::DataType::Boolean,
            nullable: false,
        }
    }

    fn leaf(memo: &mut Memo, col_id: u32, rows: f64, conf: Confidence) -> GroupId {
        let g = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(
            vec![OutputColumn {
                column_id: ColumnId::new_for_test(col_id),
                name: format!("c{col_id}"),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            rows,
        );
        props.row_count_confidence = conf;
        props.column_statistics.insert(
            ColumnId::new_for_test(col_id),
            ColumnStatistic {
                min_value: 0.0,
                max_value: rows,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(rows, conf)
            },
        );
        memo.groups[g].logical_props = Some(props);
        g
    }

    fn inner(memo: &mut Memo, cond: TypedExpr) -> LogicalJoinOp {
        LogicalJoinOp {
            join_type: JoinKind::Inner,
            condition: Some(intern_typed(&mut memo.scalars, &cond)),
        }
    }

    /// Build a left-deep path chain over `n` leaf atoms (columns 1..=n), joined
    /// on consecutive columns (c_i = c_{i+1}), and return the root group id.
    fn build_path_chain(memo: &mut Memo, n: u32, conf: Confidence) -> GroupId {
        let leaves: Vec<GroupId> = (1..=n)
            .map(|i| leaf(memo, i, 1000.0 * i as f64, conf))
            .collect();
        let mut tree = JoinTree::Leaf(leaves[0]);
        for i in 1..n as usize {
            tree = JoinTree::Join {
                left: Box::new(tree),
                right: Box::new(JoinTree::Leaf(leaves[i])),
                op: inner(memo, eq(col(i as u32), col(i as u32 + 1))),
            };
        }
        copy_in_join_tree(memo, &tree, &empty_stats_input())
    }

    #[test]
    fn pass_injects_alternatives_for_large_chain() {
        let mut memo = Memo::new();
        let root = build_path_chain(&mut memo, 6, Confidence::Estimated);
        let before = memo.groups[root].logical_exprs.len();
        assert_eq!(before, 1, "root starts with the single converted order");

        run_multi_join_reorder(&mut memo, &ReorderOptions::default(), &empty_stats_input());

        let after = memo.groups[root].logical_exprs.len();
        assert!(
            after > before,
            "reorder must add candidate orders to the chain root (was {before}, now {after})"
        );
        // Every group in the memo (including injected intermediates) must carry
        // stamped logical_props, so implement() keeps HashJoins (M1).
        for (gid, group) in memo.groups.iter().enumerate() {
            assert!(
                group.logical_props.is_some(),
                "group {gid} must have stamped logical_props after the pass"
            );
        }
    }

    #[test]
    fn reorder_marks_owned_groups_for_large_chain() {
        // A chain larger than the exhaustive threshold is reorder-owned: its
        // join groups are recorded so explore's JoinAssociativity skips them (D2).
        let mut memo = Memo::new();
        let root = build_path_chain(&mut memo, 6, Confidence::Estimated);
        run_multi_join_reorder(&mut memo, &ReorderOptions::default(), &empty_stats_input());
        assert!(
            memo.reorder_owned_groups.contains(&root),
            "the chain root must be marked reorder-owned"
        );
        assert_eq!(
            memo.reorder_owned_groups.len(),
            5,
            "a 6-atom left-deep chain has 5 join groups (root + 4 internal); all marked"
        );
    }

    #[test]
    fn reorder_does_not_mark_small_chain() {
        // A chain <= the exhaustive threshold is left to JoinAssociativity, so it
        // must NOT be marked reorder-owned.
        let mut memo = Memo::new();
        build_path_chain(&mut memo, 3, Confidence::Estimated);
        run_multi_join_reorder(&mut memo, &ReorderOptions::default(), &empty_stats_input());
        assert!(
            memo.reorder_owned_groups.is_empty(),
            "small chain must not be reorder-owned, got {:?}",
            memo.reorder_owned_groups
        );
    }

    #[test]
    fn pass_skips_small_chain_left_to_associativity() {
        let mut memo = Memo::new();
        // 3 atoms <= exhaustive threshold (4) -> reorder pass leaves it alone.
        let root = build_path_chain(&mut memo, 3, Confidence::Estimated);
        let before = memo.groups[root].logical_exprs.len();
        let groups_before = memo.groups.len();

        run_multi_join_reorder(&mut memo, &ReorderOptions::default(), &empty_stats_input());

        assert_eq!(
            memo.groups[root].logical_exprs.len(),
            before,
            "small chain must not be reordered (left to JoinAssociativity)"
        );
        assert_eq!(
            memo.groups.len(),
            groups_before,
            "no new groups for a small chain"
        );
    }

    #[test]
    fn pass_degrades_to_left_deep_when_stats_unknown() {
        let mut memo = Memo::new();
        // Fallback confidence on the atoms -> only the LeftDeep candidate is
        // enumerated (DP/Greedy disabled), so at most one alternative is added.
        let root = build_path_chain(&mut memo, 6, Confidence::Fallback);
        let before = memo.groups[root].logical_exprs.len();

        run_multi_join_reorder(&mut memo, &ReorderOptions::default(), &empty_stats_input());

        let added = memo.groups[root].logical_exprs.len() - before;
        assert!(
            added <= 1,
            "unknown-stats chain should add at most the LeftDeep order, added {added}"
        );
    }
}
