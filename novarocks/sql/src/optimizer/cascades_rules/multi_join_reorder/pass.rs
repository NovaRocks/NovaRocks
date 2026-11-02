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

use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use crate::compiler::SqlCompileError;

use crate::common::JoinKind;
use crate::optimizer::memo::{GroupId, JoinTree, MExpr, Memo};
use crate::optimizer::operator::{LogicalJoinOp, Operator};
use crate::optimizer::statistics::Confidence;
use crate::optimizer::stats::copy_in_join_tree;
use crate::optimizer::stats_input::OptimizerStatsInput;

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
/// The pass owns one counter for its traversals; it observes the caller's
/// existing policy rather than introducing a separate work or memory allowance.
pub(crate) fn run_multi_join_reorder(
    memo: &mut Memo,
    opts: &ReorderOptions,
    stats_input: &OptimizerStatsInput,
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    // Snapshot the chain roots before injecting, so the new alternative groups
    // (appended at higher indices) are not themselves reprocessed.
    for root in find_chain_roots(memo, &mut work, control)? {
        work.step()?;
        reorder_chain(memo, root, opts, stats_input, &mut work, control)?;
    }
    work.finish()?;
    Ok(())
}

// Flattening, order enumeration, operator debug formatting, and memo insertion
// remain opaque owner operations. Before/after observations do not prove that
// their internal traversals are cooperative.
fn observe_opaque<T>(
    work: &mut CompileCheckpoints<'_>,
    control: &dyn PureCompileControl,
    operation: impl FnOnce() -> T,
) -> Result<T, SqlCompileError> {
    control.checkpoint(CompilePhase::Validate, 0)?;
    let value = operation();
    control.checkpoint(CompilePhase::Validate, 0)?;
    work.step()?;
    Ok(value)
}

fn reorder_chain(
    memo: &mut Memo,
    root: GroupId,
    opts: &ReorderOptions,
    stats_input: &OptimizerStatsInput,
    work: &mut CompileCheckpoints<'_>,
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
    let Some(graph) = observe_opaque(work, control, || flatten_join_chain(memo, root, control))??
    else {
        return Ok(());
    };
    let n = graph.atom_count();
    // Small chains stay with JoinAssociativity; oversized chains are skipped.
    if n <= opts.max_reorder_node_use_exhaustive || n > opts.max_reorder_node {
        return Ok(());
    }
    // This chain is reorder-owned: record its join groups so explore's
    // JoinAssociativity skips them and does not re-enumerate the orders we are
    // about to inject (D2: reorder/associativity mutual exclusion).
    for &group in &graph.chain_join_groups {
        work.step()?;
        memo.reorder_owned_groups.insert(group);
    }
    // Degrade to LeftDeep-only when base statistics are unknown (StarRocks
    // `Utils.hasUnknownColumnsStats`). Preserve the original short circuit.
    let mut caps = opts.caps();
    for stats in &graph.atom_stats {
        work.step()?;
        if stats.row_count_confidence == Confidence::Fallback {
            caps.enable_dp = false;
            caps.enable_greedy = false;
            break;
        }
    }
    let candidates = observe_opaque(work, control, || {
        enumerate_orders(&graph, caps, &mut memo.scalars, control)
    })??;
    for tree in candidates {
        work.step()?;
        inject_candidate(memo, root, tree, stats_input, work, control)?;
    }
    Ok(())
}

/// Materialize a candidate order's sub-trees into the memo and add its root join
/// as an alternative expression in the chain-root group (deduplicated).
fn inject_candidate(
    memo: &mut Memo,
    root: GroupId,
    tree: JoinTree,
    stats_input: &OptimizerStatsInput,
    work: &mut CompileCheckpoints<'_>,
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
    let JoinTree::Join { left, right, op } = tree else {
        return Ok(()); // a reorder candidate over >= 2 atoms is always a join
    };
    let left_id = copy_in_join_tree(memo, &left, stats_input, control)?;
    let right_id = copy_in_join_tree(memo, &right, stats_input, control)?;
    let new_op = Operator::LogicalJoin(op);
    let children = vec![left_id, right_id];
    for expression in &memo.groups[root].logical_exprs {
        work.step()?;
        // Candidate roots always have exactly two ordered children. A different
        // arity cannot match, so no unbounded vector comparison is needed here.
        if expression.children.len() == children.len()
            && expression.children == children
            && observe_opaque(work, control, || {
                format!("{:?}", expression.op) == format!("{new_op:?}")
            })?
        {
            return Ok(());
        }
    }
    let id = crate::optimizer::next_expr_id_observed(memo, work)?;
    observe_opaque(work, control, || {
        memo.add_expr_to_group(
            root,
            MExpr {
                id,
                op: new_op,
                children,
            },
        );
    })?;
    Ok(())
}

/// Chain roots: inner/cross join groups that are not themselves the inner/cross
/// join child of another inner/cross join. Each maximal chain is reordered once;
/// chains nested under non-join atoms (e.g. under an aggregate) are still found
/// because their root is not a join's child.
fn find_chain_roots(
    memo: &Memo,
    work: &mut CompileCheckpoints<'_>,
    _control: &dyn PureCompileControl,
) -> Result<Vec<GroupId>, SqlCompileError> {
    let mut mid_chain: HashSet<GroupId> = HashSet::new();
    for group in &memo.groups {
        work.step()?;
        if let Some(expr) = group.logical_exprs.first()
            && is_inner_cross_join_op(&expr.op)
        {
            for &child in &expr.children {
                work.step()?;
                if child_is_inner_cross_join(memo, child) {
                    mid_chain.insert(child);
                }
            }
        }
    }
    let mut roots = Vec::new();
    for (group_id, group) in memo.groups.iter().enumerate() {
        work.step()?;
        if !mid_chain.contains(&group_id)
            && group
                .logical_exprs
                .first()
                .is_some_and(|e| is_inner_cross_join_op(&e.op))
        {
            roots.push(group_id);
        }
    }
    Ok(roots)
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
    use crate::analysis::{BinOp, ExprKind, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::memo::LogicalProperties;
    use crate::optimizer::operator::ValuesOp;
    use crate::optimizer::statistics::ColumnStatistic;
    use crate::optimizer::stats_input::OptimizerStatsInput;
    use crate::planner::optimizer_bridge::scalar::intern_typed;
    use std::collections::HashMap;

    fn test_control() -> &'static dyn PureCompileControl {
        crate::optimizer::test_optimizer_control()
    }

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
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),
        }
    }

    fn eq(l: TypedExpr, r: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(l),
                op: BinOp::Eq,
                right: Box::new(r),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Boolean,
                false,
            ),
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
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                ),

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
            naa_author: None,
            join_type: JoinKind::Inner,
            condition: Some(
                intern_typed(
                    &mut memo.scalars,
                    &cond,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap(),
            ),
        }
    }

    /// Build a left-deep path chain over `n` leaf atoms (columns 1..=n), joined
    /// on consecutive columns (c_i = c_{i+1}), and return the root group id.
    fn build_path_chain(memo: &mut Memo, n: u32, conf: Confidence) -> GroupId {
        let leaves: Vec<GroupId> = (1..=n)
            .map(|i| leaf(memo, i, 1000.0 * i as f64, conf))
            .collect();
        let mut tree = JoinTree::Leaf(leaves[0]);
        for (i, &leaf) in leaves.iter().enumerate().skip(1) {
            tree = JoinTree::Join {
                left: Box::new(tree),
                right: Box::new(JoinTree::Leaf(leaf)),
                op: inner(memo, eq(col(i as u32), col(i as u32 + 1))),
            };
        }
        copy_in_join_tree(memo, &tree, &empty_stats_input(), test_control()).unwrap()
    }

    #[test]
    fn pass_injects_alternatives_for_large_chain() {
        let mut memo = Memo::new();
        let root = build_path_chain(&mut memo, 6, Confidence::Estimated);
        let before = memo.groups[root].logical_exprs.len();
        assert_eq!(before, 1, "root starts with the single converted order");

        run_multi_join_reorder(
            &mut memo,
            &ReorderOptions::default(),
            &empty_stats_input(),
            test_control(),
        )
        .unwrap();

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
        run_multi_join_reorder(
            &mut memo,
            &ReorderOptions::default(),
            &empty_stats_input(),
            test_control(),
        )
        .unwrap();
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
        run_multi_join_reorder(
            &mut memo,
            &ReorderOptions::default(),
            &empty_stats_input(),
            test_control(),
        )
        .unwrap();
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

        run_multi_join_reorder(
            &mut memo,
            &ReorderOptions::default(),
            &empty_stats_input(),
            test_control(),
        )
        .unwrap();

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

        run_multi_join_reorder(
            &mut memo,
            &ReorderOptions::default(),
            &empty_stats_input(),
            test_control(),
        )
        .unwrap();

        let added = memo.groups[root].logical_exprs.len() - before;
        assert!(
            added <= 1,
            "unknown-stats chain should add at most the LeftDeep order, added {added}"
        );
    }

    #[derive(Clone, Copy)]
    enum StopPoint {
        Entry,
        Batch,
        Finish,
    }
    struct ObservedControl {
        units: std::sync::Mutex<Vec<u32>>,
        stop: Option<(StopPoint, novarocks_type_contract::CompileControlError)>,
    }
    impl PureCompileControl for ObservedControl {
        fn checkpoint(
            &self,
            _: CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            let mut observations = self.units.lock().unwrap();
            observations.push(units);
            if let Some((point, error)) = self.stop.clone() {
                let stop = match point {
                    StopPoint::Entry => observations.len() == 1,
                    StopPoint::Batch => units == 256,
                    StopPoint::Finish => units > 0 && units < 256,
                };
                if stop {
                    return Err(error);
                }
            }
            Ok(())
        }
    }
    fn observed_control(
        stop: Option<(StopPoint, novarocks_type_contract::CompileControlError)>,
    ) -> ObservedControl {
        ObservedControl {
            units: Default::default(),
            stop,
        }
    }
    fn wide_leaf_memo() -> Memo {
        let mut memo = Memo::new();
        for column in 1..=320 {
            leaf(&mut memo, column, 100.0, Confidence::Estimated);
        }
        memo
    }
    #[test]
    fn pass_observes_both_real_group_scans_and_final_pending_work() {
        let mut memo = wide_leaf_memo();
        let control = observed_control(None);
        run_multi_join_reorder(
            &mut memo,
            &ReorderOptions::default(),
            &empty_stats_input(),
            &control,
        )
        .unwrap();
        assert_eq!(*control.units.lock().unwrap(), vec![0, 256, 256, 128]);
        assert_eq!(memo.groups.len(), 320);
        assert!(memo.reorder_owned_groups.is_empty());
    }
    #[test]
    fn pass_keeps_three_typed_controls_at_entry_mid_scan_and_finish() {
        use novarocks_type_contract::CompileControlError as Error;
        for error in [
            Error::Cancelled,
            Error::DeadlineExceeded,
            Error::ResourceExhausted,
        ] {
            for point in [StopPoint::Entry, StopPoint::Batch, StopPoint::Finish] {
                let control = observed_control(Some((point, error.clone())));
                let mut memo = wide_leaf_memo();
                let failure = run_multi_join_reorder(
                    &mut memo,
                    &ReorderOptions::default(),
                    &empty_stats_input(),
                    &control,
                )
                .unwrap_err();
                assert_eq!(failure, SqlCompileError::from(error.clone()));
                let units = control.units.lock().unwrap();
                assert!(units.iter().all(|units| *units <= 256));
                match point {
                    StopPoint::Entry => assert_eq!(*units, vec![0]),
                    StopPoint::Batch => assert_eq!(*units, vec![0, 256]),
                    StopPoint::Finish => assert_eq!(*units, vec![0, 256, 256, 128]),
                }
            }
        }
    }
    #[test]
    fn roots_preserve_real_ordered_edges_and_non_inner_parent_boundary() {
        let mut memo = Memo::new();
        let first = build_path_chain(&mut memo, 3, Confidence::Estimated);
        let second = build_path_chain(&mut memo, 6, Confidence::Estimated);
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                naa_author: None,
                join_type: JoinKind::LeftOuter,
                condition: None,
            }),
            children: vec![first, second],
        });
        let control = observed_control(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let roots = find_chain_roots(&memo, &mut work, &control).unwrap();
        work.finish().unwrap();
        // The outer join is a strong chain boundary. Each inner maximal chain
        // remains a root, in the same ascending actual group order.
        assert_eq!(roots, vec![first, second]);
        let edge_count = memo
            .groups
            .iter()
            .filter_map(|group| group.logical_exprs.first())
            .filter(|expression| is_inner_cross_join_op(&expression.op))
            .map(|expression| expression.children.len())
            .sum::<usize>();
        assert_eq!(
            control.units.lock().unwrap().iter().sum::<u32>() as usize,
            memo.groups.len() * 2 + edge_count
        );
    }
    fn candidate_comparison_fixture() -> (Memo, GroupId, JoinTree) {
        let mut memo = Memo::new();
        let left = leaf(&mut memo, 1, 100.0, Confidence::Estimated);
        let right = leaf(&mut memo, 2, 100.0, Confidence::Estimated);
        // INNER without a predicate and CROSS are equivalent here. The memo
        // has many actual alternatives before the exact INNER candidate.
        let root = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                naa_author: None,
                join_type: JoinKind::Cross,
                condition: None,
            }),
            children: vec![left, right],
        });
        for _ in 1..320 {
            let id = memo.next_expr_id();
            memo.groups[root].logical_exprs.push(MExpr {
                id,
                op: Operator::LogicalJoin(LogicalJoinOp {
                    naa_author: None,
                    join_type: JoinKind::Cross,
                    condition: None,
                }),
                children: vec![left, right],
            });
        }
        let op = LogicalJoinOp {
            naa_author: None,
            join_type: JoinKind::Inner,
            condition: None,
        };
        let id = memo.next_expr_id();
        memo.groups[root].logical_exprs.push(MExpr {
            id,
            op: Operator::LogicalJoin(op.clone()),
            children: vec![left, right],
        });
        let candidate = JoinTree::Join {
            left: Box::new(JoinTree::Leaf(left)),
            right: Box::new(JoinTree::Leaf(right)),
            op,
        };
        (memo, root, candidate)
    }
    #[test]
    fn candidate_dedup_compares_actual_alternatives_cooperatively() {
        use novarocks_type_contract::CompileControlError as Error;
        for error in [
            Error::Cancelled,
            Error::DeadlineExceeded,
            Error::ResourceExhausted,
        ] {
            let (mut memo, root, candidate) = candidate_comparison_fixture();
            let control = observed_control(Some((StopPoint::Batch, error.clone())));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
            let failure = inject_candidate(
                &mut memo,
                root,
                candidate,
                &empty_stats_input(),
                &mut work,
                &control,
            )
            .unwrap_err();
            assert_eq!(failure, SqlCompileError::from(error));
            assert!(control.units.lock().unwrap().contains(&256));
            assert_eq!(memo.groups.len(), 3);
            assert_eq!(memo.groups[root].logical_exprs.len(), 321);
        }
        let (mut memo, root, candidate) = candidate_comparison_fixture();
        let control = observed_control(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        inject_candidate(
            &mut memo,
            root,
            candidate,
            &empty_stats_input(),
            &mut work,
            &control,
        )
        .unwrap();
        work.finish().unwrap();
        assert_eq!(
            memo.groups[root].logical_exprs.len(),
            321,
            "the last exact candidate is reused"
        );
        assert_eq!(memo.groups.len(), 3);
        assert!(
            control
                .units
                .lock()
                .unwrap()
                .iter()
                .all(|units| *units <= 256)
        );
    }
    #[test]
    fn empty_pass_observes_entry_and_finish_and_memo_does_not_retain_control() {
        let mut memo = Memo::new();
        let control = std::sync::Arc::new(observed_control(None));
        let weak = std::sync::Arc::downgrade(&control);
        run_multi_join_reorder(
            &mut memo,
            &ReorderOptions::default(),
            &empty_stats_input(),
            control.as_ref(),
        )
        .unwrap();
        assert_eq!(*control.units.lock().unwrap(), vec![0, 0]);
        drop(control);
        assert!(weak.upgrade().is_none());
        assert!(memo.groups.is_empty());
    }
}
