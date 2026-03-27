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
        let left_tuple = *left.tuple_ids.last().unwrap();
        let right_tuple = *right.tuple_ids.last().unwrap();

        // Extract equi-join conditions from the join condition
        let (eq_conds, other_conds) = match node.condition {
            Some(cond) => self.extract_join_conditions_typed(&cond, &left.scope, &right.scope)?,
            None => (vec![], vec![]),
        };

        let join_node_id = self.alloc_node();
        let join_plan_node = if join_op == plan_nodes::TJoinOp::CROSS_JOIN {
            nodes::build_nestloop_join_node(
                join_node_id,
                left_tuple,
                right_tuple,
                join_op,
                other_conds,
            )
        } else if eq_conds.is_empty() {
            // No equality conditions — fall back to nested loop join
            nodes::build_nestloop_join_node(
                join_node_id,
                left_tuple,
                right_tuple,
                join_op,
                other_conds,
            )
        } else {
            nodes::build_hash_join_node(
                join_node_id,
                left_tuple,
                right_tuple,
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
                    // Non-equality condition
                    let mut merged = ExprScope::new();
                    merged.merge(left_scope);
                    merged.merge(right_scope);
                    let mut compiler = ExprCompiler::new(&merged);
                    other_conds.push(compiler.compile_typed(conj)?);
                }
            }
        }

        Ok((eq_conds, other_conds))
    }
}
