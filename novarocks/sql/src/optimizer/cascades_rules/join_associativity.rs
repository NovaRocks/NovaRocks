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

//! JoinAssociativity transformation rule.
//!
//! Re-associates inner joins: `(A JOIN B) JOIN C` -> `A JOIN (B JOIN C)`.
//!
//! Only applies when both the outer and inner joins are INNER joins, AND
//! when reusing the original conditions in their new positions is sound:
//! the inner_op.condition (originally over A∪B) must reference only
//! columns from B in its new position over (B JOIN C). If it references
//! any column from A, we cannot reuse it without redistribution and the
//! rewrite is skipped. Full predicate re-association across the rewrite
//! is a future improvement.

use crate::common::JoinKind;
use crate::optimizer::binder::Binding;
use crate::optimizer::memo::{GroupId, MExpr, Memo};
use crate::optimizer::operator::{LogicalJoinOp, Operator};
use crate::optimizer::pattern::{OpKind, Pattern};
use crate::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::optimizer::scalar::ScalarId;

use crate::optimizer::rewrite::rules::utils::collect_scalar_column_id_refs_strict;

use super::implement::get_group_column_ids;

pub(crate) struct JoinAssociativity;

impl Rule for JoinAssociativity {
    fn name(&self) -> &str {
        "JoinAssociativity"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                ..
            })
        )
    }

    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalJoin(outer_op) = &expr.op else {
            return Ok(vec![]);
        };

        // Outer join must be INNER.
        if outer_op.join_type != JoinKind::Inner {
            return Ok(vec![]);
        }

        // Must have two children: child[0] = inner join group, child[1] = C.
        if expr.children.len() != 2 {
            return Ok(vec![]);
        }

        let inner_group_id = expr.children[0];
        let c_group = expr.children[1];

        // Check if the inner group contains a LogicalJoin(Inner) expression.
        let inner_group = &memo.groups[inner_group_id];
        let inner_join = inner_group.logical_exprs.iter().find(|e| {
            matches!(
                &e.op,
                Operator::LogicalJoin(LogicalJoinOp {
                    join_type: JoinKind::Inner,
                    ..
                })
            )
        });

        let Some(inner_expr) = inner_join else {
            return Ok(vec![]);
        };

        // inner_expr represents LogicalJoin(A, B) with INNER join.
        if inner_expr.children.len() != 2 {
            return Ok(vec![]);
        }

        let a_group = inner_expr.children[0];
        let b_group = inner_expr.children[1];

        let inner_op = match &inner_expr.op {
            Operator::LogicalJoin(op) => op,
            _ => return Ok(vec![]),
        };

        // Copy the `Copy` ids out before borrowing `&mut memo` in the helper.
        let outer_cond = outer_op.condition;
        let inner_cond = inner_op.condition;
        Ok(self.associate(memo, outer_cond, inner_cond, a_group, b_group, c_group))
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Join,
            children: vec![
                Pattern::Op {
                    kind: OpKind::Join,
                    children: vec![Pattern::Leaf, Pattern::Leaf],
                },
                Pattern::Leaf,
            ],
        }
    }

    /// Consume only the FIRST binding, reproducing the legacy `.find(Inner)`.
    ///
    /// INVARIANT (byte-identity depends on it): an inner-join group is
    /// homogeneous today — JoinCommutativity maps Inner→Inner and the reorder
    /// pass injects only Inner/Cross into inner chains — so the binder's first
    /// Join-kind alternative is always the Inner one legacy `.find` picked.
    /// If a future rule (e.g. the outer→inner simplification in the
    /// outer-join-reorder arc) ever places a non-Inner alternative ahead of an
    /// Inner one in the same group, `apply_bound` below would bail on it and
    /// MISS the Inner re-association legacy would have found. Then revisit: drop
    /// `first_match_only` and have the rule scan bindings for the first Inner
    /// (deduping to one result) instead of slicing to binding 0.
    fn first_match_only(&self) -> bool {
        true
    }

    fn apply_bound(
        &self,
        binding: &Binding,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        // interior 0 = outer join, interior 1 = inner join (binder guarantees
        // both are Join kind; field predicates are checked here).
        let Operator::LogicalJoin(outer) = binding.op(memo, 0).clone() else {
            return Ok(vec![]);
        };
        if outer.join_type != JoinKind::Inner {
            return Ok(vec![]);
        }
        let Operator::LogicalJoin(inner) = binding.op(memo, 1).clone() else {
            return Ok(vec![]);
        };
        if inner.join_type != JoinKind::Inner {
            return Ok(vec![]);
        }
        // inner = LogicalJoin(A, B); outer child[1] = C.
        let inner_children = binding.children(1);
        let (a_group, b_group) = (inner_children[0], inner_children[1]);
        let c_group = binding.children(0)[1];
        Ok(self.associate(
            memo,
            outer.condition,
            inner.condition,
            a_group,
            b_group,
            c_group,
        ))
    }
}

impl JoinAssociativity {
    /// Shared core for both the legacy `apply` (unit-tested directly) and the
    /// declarative `apply_bound`: the soundness gate, the `B JOIN C` mint, and
    /// the `A JOIN (B JOIN C)` emit. All scalar/group ids are `Copy`, so the
    /// caller pre-extracts them before this takes `&mut memo`.
    fn associate(
        &self,
        memo: &mut Memo,
        outer_cond: Option<ScalarId>,
        inner_cond: Option<ScalarId>,
        a_group: GroupId,
        b_group: GroupId,
        c_group: GroupId,
    ) -> Vec<NewExpr> {
        // Soundness gate: the new inner join (B JOIN C) reuses inner_cond
        // verbatim, so that condition must reference only columns available in
        // (B ∪ C). Originally inner_cond was over (A ∪ B); if it
        // references any column from A, A is no longer in the inner join's
        // scope after re-association, and reusing the condition would either
        // panic the fragment builder (column-not-resolvable) or silently
        // produce wrong rows. Skip the rewrite in that case rather than emit
        // an unsound plan. A future improvement would split the condition by
        // conjunct and re-distribute across the new structure.
        if let Some(cond) = inner_cond {
            let Some(cond_ids) = collect_scalar_column_id_refs_strict(&memo.scalars, cond) else {
                return vec![];
            };
            let a_ids = get_group_column_ids(memo, a_group);
            let b_ids = get_group_column_ids(memo, b_group);
            let c_ids = get_group_column_ids(memo, c_group);
            let bc_ids: std::collections::HashSet<_> = b_ids.union(&c_ids).copied().collect();
            // Only fire if every column the condition references is available
            // in B ∪ C, and at least one column also lies in B (otherwise the
            // condition is purely over A, which is even more clearly wrong).
            let refs_only_bc = cond_ids.iter().all(|id| bc_ids.contains(id));
            let refs_any_a = cond_ids
                .iter()
                .any(|id| a_ids.contains(id) && !bc_ids.contains(id));
            if !refs_only_bc || refs_any_a {
                return vec![];
            }
        }

        // Produce: A JOIN_outer (B JOIN_inner C)
        //
        // The outer join's condition (originally over (A∪B)∪C) is reused on
        // the new outer (A JOIN BC), which has the same combined scope; that
        // is always safe.
        // The inner join's condition is reused on the new inner (B JOIN C);
        // soundness was checked above.

        // Create the new inner join group: B JOIN C
        let new_inner_join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                naa_author: None,
                join_type: JoinKind::Inner,
                condition: inner_cond,
            }),
            children: vec![b_group, c_group],
        };
        let new_inner_group = memo.new_group(new_inner_join);

        // New outer join: A JOIN (B JOIN C)
        vec![NewExpr {
            op: Operator::LogicalJoin(LogicalJoinOp {
                naa_author: None,
                join_type: JoinKind::Inner,
                condition: outer_cond,
            }),
            children: vec![a_group, new_inner_group],
        }]
    }
}
