use crate::exprs;
use crate::opcodes;
use crate::plan_nodes;
use crate::sql::ir::{BinOp, ExprKind, TypedExpr};
use crate::sql::plan::*;

use crate::sql::physical::expr_compiler::ExprCompiler;
use crate::sql::physical::nodes;
use crate::sql::physical::resolve::ExprScope;

use super::EmitResult;
use super::helpers::{join_kind_to_op, split_and_conjuncts_typed};

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_join(&mut self, node: JoinNode) -> Result<EmitResult, String> {
        let left = self.emit_node(*node.left)?;
        let right = self.emit_node(*node.right)?;

        let join_op = join_kind_to_op(node.join_type);

        // Extract equi-join conditions from the join condition
        let (eq_conds, other_conds) = match node.condition {
            Some(cond) => self.extract_join_conditions_typed(&cond, &left.scope, &right.scope)?,
            None => (vec![], vec![]),
        };

        let join_node_id = self.alloc_node();
        let join_plan_node = if join_op == plan_nodes::TJoinOp::CROSS_JOIN {
            nodes::build_nestloop_join_node(
                join_node_id,
                &left.tuple_ids,
                &right.tuple_ids,
                join_op,
                other_conds,
            )
        } else if eq_conds.is_empty() {
            // No equality conditions — fall back to nested loop join
            nodes::build_nestloop_join_node(
                join_node_id,
                &left.tuple_ids,
                &right.tuple_ids,
                join_op,
                other_conds,
            )
        } else {
            nodes::build_hash_join_node(
                join_node_id,
                &left.tuple_ids,
                &right.tuple_ids,
                join_op,
                eq_conds,
                other_conds,
            )
        };

        // Merge scopes
        let mut merged_scope = left.scope;
        merged_scope.merge(&right.scope);

        let mut merged_tuple_ids = left.tuple_ids;
        merged_tuple_ids.extend(right.tuple_ids);

        // Pre-order: join_node, then left subtree, then right subtree
        let mut plan_nodes = vec![join_plan_node];
        plan_nodes.extend(left.plan_nodes);
        plan_nodes.extend(right.plan_nodes);

        Ok(EmitResult {
            plan_nodes,
            scope: merged_scope,
            tuple_ids: merged_tuple_ids,
        })
    }

    pub(super) fn extract_join_conditions_typed(
        &mut self,
        condition: &TypedExpr,
        left_scope: &ExprScope,
        right_scope: &ExprScope,
    ) -> Result<(Vec<plan_nodes::TEqJoinCondition>, Vec<exprs::TExpr>), String> {
        let mut eq_conds = Vec::new();
        let mut other_conds = Vec::new();

        let conjuncts = split_and_conjuncts_typed(condition);

        for conj in conjuncts {
            match &conj.kind {
                ExprKind::BinaryOp {
                    left,
                    op: BinOp::Eq,
                    right,
                } => {
                    // Try left against left_scope, right against right_scope
                    let mut left_compiler = ExprCompiler::new(left_scope);
                    let mut right_compiler = ExprCompiler::new(right_scope);
                    if let (Ok(left_texpr), Ok(right_texpr)) = (
                        left_compiler.compile_typed(left),
                        right_compiler.compile_typed(right),
                    ) {
                        eq_conds.push(plan_nodes::TEqJoinCondition {
                            left: left_texpr,
                            right: right_texpr,
                            opcode: Some(opcodes::TExprOpcode::EQ),
                        });
                    } else {
                        // Try reversed
                        let mut left_compiler2 = ExprCompiler::new(right_scope);
                        let mut right_compiler2 = ExprCompiler::new(left_scope);
                        if let (Ok(left_texpr), Ok(right_texpr)) = (
                            left_compiler2.compile_typed(left),
                            right_compiler2.compile_typed(right),
                        ) {
                            eq_conds.push(plan_nodes::TEqJoinCondition {
                                left: right_texpr,
                                right: left_texpr,
                                opcode: Some(opcodes::TExprOpcode::EQ),
                            });
                        } else {
                            // Compile against merged scope
                            let mut merged = ExprScope::new();
                            merged.merge(left_scope);
                            merged.merge(right_scope);
                            let mut compiler = ExprCompiler::new(&merged);
                            other_conds.push(compiler.compile_typed(conj)?);
                        }
                    }
                }
                _ => {
                    // Non-equality condition — check if it's an OR that
                    // contains a common equi-join key across all branches.
                    if let Some(extracted) =
                        self.try_extract_equijoin_from_or(conj, left_scope, right_scope)
                    {
                        eq_conds.push(extracted.eq_condition);
                        other_conds.push(extracted.remaining_condition);
                    } else {
                        let mut merged = ExprScope::new();
                        merged.merge(left_scope);
                        merged.merge(right_scope);
                        let mut compiler = ExprCompiler::new(&merged);
                        other_conds.push(compiler.compile_typed(conj)?);
                    }
                }
            }
        }

        Ok((eq_conds, other_conds))
    }

    /// Try to extract a common equi-join condition from an OR expression.
    ///
    /// When a join condition is a single OR expression like:
    ///   (a = b AND ...) OR (a = b AND ...) OR (a = b AND ...)
    /// all branches share the same equality `a = b`. This function extracts
    /// that common equality as an equi-join condition, enabling hash join
    /// instead of nested-loop join. The full OR expression is still kept as
    /// an "other" condition for correctness.
    fn try_extract_equijoin_from_or(
        &mut self,
        expr: &TypedExpr,
        left_scope: &ExprScope,
        right_scope: &ExprScope,
    ) -> Option<ExtractedOrEquiJoin> {
        // Collect OR branches
        let branches = split_or(expr);
        if branches.len() < 2 {
            return None;
        }

        // For each branch, collect all equalities that could be equi-join conditions
        let mut branch_eq_keys: Vec<Vec<EqKey>> = Vec::new();
        for branch in &branches {
            let conjuncts = split_and_conjuncts_typed(branch);
            let mut keys = Vec::new();
            for conj in conjuncts {
                if let ExprKind::BinaryOp {
                    left,
                    op: BinOp::Eq,
                    right,
                } = &conj.kind
                {
                    // Check if this is a cross-side equi-join condition
                    if self.is_equijoin_pair(left, right, left_scope, right_scope) {
                        keys.push(EqKey::Normal(
                            column_ref_name(left).unwrap_or_default(),
                            column_ref_name(right).unwrap_or_default(),
                        ));
                    } else if self.is_equijoin_pair(right, left, left_scope, right_scope) {
                        keys.push(EqKey::Normal(
                            column_ref_name(right).unwrap_or_default(),
                            column_ref_name(left).unwrap_or_default(),
                        ));
                    }
                }
            }
            branch_eq_keys.push(keys);
        }

        // Find equi-join keys common to ALL branches
        if branch_eq_keys.is_empty() {
            return None;
        }
        let first_keys = &branch_eq_keys[0];
        let common_key = first_keys.iter().find(|key| {
            branch_eq_keys[1..]
                .iter()
                .all(|branch_keys| branch_keys.iter().any(|k| k == *key))
        })?;

        // Extract the actual TypedExpr pair from the first branch's common equality
        let (left_expr, right_expr) =
            self.find_eq_exprs_in_branch(branches[0], common_key, left_scope, right_scope)?;

        // Build the equi-join condition
        let mut left_compiler = ExprCompiler::new(left_scope);
        let mut right_compiler = ExprCompiler::new(right_scope);
        let left_texpr = left_compiler.compile_typed(left_expr).ok()?;
        let right_texpr = right_compiler.compile_typed(right_expr).ok()?;

        let eq_condition = plan_nodes::TEqJoinCondition {
            left: left_texpr,
            right: right_texpr,
            opcode: Some(opcodes::TExprOpcode::EQ),
        };

        // Compile the full OR expression as the remaining condition
        let mut merged = ExprScope::new();
        merged.merge(left_scope);
        merged.merge(right_scope);
        let mut compiler = ExprCompiler::new(&merged);
        let remaining_condition = compiler.compile_typed(expr).ok()?;

        Some(ExtractedOrEquiJoin {
            eq_condition,
            remaining_condition,
        })
    }

    /// Check if (left_expr, right_expr) form a cross-side equi-join pair:
    /// left_expr resolves in left_scope and right_expr resolves in right_scope.
    fn is_equijoin_pair(
        &self,
        left_expr: &TypedExpr,
        right_expr: &TypedExpr,
        left_scope: &ExprScope,
        right_scope: &ExprScope,
    ) -> bool {
        let left_ok = ExprCompiler::new(left_scope)
            .compile_typed(left_expr)
            .is_ok();
        let right_ok = ExprCompiler::new(right_scope)
            .compile_typed(right_expr)
            .is_ok();
        left_ok && right_ok
    }

    /// Find the equality expression in a branch that matches the given EqKey,
    /// returning references to the left and right TypedExpr (in left-scope,
    /// right-scope order).
    fn find_eq_exprs_in_branch<'b>(
        &self,
        branch: &'b TypedExpr,
        key: &EqKey,
        left_scope: &ExprScope,
        right_scope: &ExprScope,
    ) -> Option<(&'b TypedExpr, &'b TypedExpr)> {
        let conjuncts = split_and_conjuncts_typed(branch);
        for conj in conjuncts {
            if let ExprKind::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
            } = &conj.kind
            {
                let left_name = column_ref_name(left).unwrap_or_default();
                let right_name = column_ref_name(right).unwrap_or_default();
                match key {
                    EqKey::Normal(kl, kr) => {
                        if &left_name == kl
                            && &right_name == kr
                            && self.is_equijoin_pair(left, right, left_scope, right_scope)
                        {
                            return Some((left, right));
                        }
                        if &right_name == kl
                            && &left_name == kr
                            && self.is_equijoin_pair(right, left, left_scope, right_scope)
                        {
                            return Some((right, left));
                        }
                    }
                }
            }
        }
        None
    }
}

/// Result of extracting a common equi-join condition from an OR expression.
struct ExtractedOrEquiJoin {
    eq_condition: plan_nodes::TEqJoinCondition,
    remaining_condition: exprs::TExpr,
}

/// Represents an equi-join key pair (left_col_name, right_col_name).
#[derive(PartialEq, Eq)]
enum EqKey {
    Normal(String, String),
}

/// Extract the column name from a simple ColumnRef expression (lowercase).
fn column_ref_name(expr: &TypedExpr) -> Option<String> {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
        _ => None,
    }
}

/// Split a TypedExpr on OR into a flat list of disjuncts.
fn split_or(expr: &TypedExpr) -> Vec<&TypedExpr> {
    let mut out = Vec::new();
    split_or_inner(expr, &mut out);
    out
}

fn split_or_inner<'a>(expr: &'a TypedExpr, out: &mut Vec<&'a TypedExpr>) {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            split_or_inner(left, out);
            split_or_inner(right, out);
        }
        ExprKind::Nested(inner) => split_or_inner(inner, out),
        _ => out.push(expr),
    }
}
