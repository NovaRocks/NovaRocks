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

//! PruneScanColumns — Phase 2 rule for Scan nodes.
//!
//! Translates the ColumnId-based `required_output_columns` set (written by the
//! Phase-1 tagging pass) into the string-name-based `required_columns` list
//! that fragment materialization reads.
//!
//! Also unions in any columns referenced by pushed-down predicates so that
//! predicate evaluation is not broken by column pruning.

use std::collections::HashSet;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::pattern::{OpKind, Pattern};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::scalar::{self, ScalarNode};

pub(crate) struct PruneScanColumns;

/// Collect all ColumnIds referenced by a scalar expression tree rooted at `id`.
/// Walks the scalar arena transitively.
fn collect_scalar_column_ids(
    arena: &scalar::ScalarArena,
    id: scalar::ScalarId,
    out: &mut HashSet<ColumnId>,
) {
    match arena.node(id) {
        ScalarNode::ColumnRef(column_id) => {
            out.insert(*column_id);
        }
        ScalarNode::Literal(_) => {}
        ScalarNode::BinaryOp { left, right, .. } => {
            collect_scalar_column_ids(arena, *left, out);
            collect_scalar_column_ids(arena, *right, out);
        }
        ScalarNode::UnaryOp { child, .. } => {
            collect_scalar_column_ids(arena, *child, out);
        }
        ScalarNode::FunctionCall { args, .. } => {
            for &arg in args {
                collect_scalar_column_ids(arena, arg, out);
            }
        }
        ScalarNode::LambdaFunction { body, .. } => {
            collect_scalar_column_ids(arena, *body, out);
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            for &arg in args {
                collect_scalar_column_ids(arena, arg, out);
            }
            for key in order_by {
                collect_scalar_column_ids(arena, key.expr, out);
            }
        }
        ScalarNode::Cast { child, .. } => {
            collect_scalar_column_ids(arena, *child, out);
        }
        ScalarNode::IsNull { child, .. } => {
            collect_scalar_column_ids(arena, *child, out);
        }
        ScalarNode::InList { child, list, .. } => {
            collect_scalar_column_ids(arena, *child, out);
            for &item in list {
                collect_scalar_column_ids(arena, item, out);
            }
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            collect_scalar_column_ids(arena, *child, out);
            collect_scalar_column_ids(arena, *low, out);
            collect_scalar_column_ids(arena, *high, out);
        }
        ScalarNode::Like { child, pattern, .. } => {
            collect_scalar_column_ids(arena, *child, out);
            collect_scalar_column_ids(arena, *pattern, out);
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_scalar_column_ids(arena, *op, out);
            }
            for &(when, then) in when_then {
                collect_scalar_column_ids(arena, when, out);
                collect_scalar_column_ids(arena, then, out);
            }
            if let Some(e) = else_expr {
                collect_scalar_column_ids(arena, *e, out);
            }
        }
        ScalarNode::IsTruthValue { child, .. } => {
            collect_scalar_column_ids(arena, *child, out);
        }
        ScalarNode::Nested(child) => {
            collect_scalar_column_ids(arena, *child, out);
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for &arg in args {
                collect_scalar_column_ids(arena, arg, out);
            }
            for &pb in partition_by {
                collect_scalar_column_ids(arena, pb, out);
            }
            for key in order_by {
                collect_scalar_column_ids(arena, key.expr, out);
            }
        }
        ScalarNode::Lambda { body, .. } => {
            collect_scalar_column_ids(arena, *body, out);
        }
        ScalarNode::LambdaParamRef { .. } => {}
    }
}

impl LogicalRewriteRule for PruneScanColumns {
    fn name(&self) -> &'static str {
        "PruneScanColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Scan,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            children,
            required_output_columns,
        } = expr;
        let Operator::LogicalScan(mut node) = op else {
            unreachable!()
        };

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        // Collect names for all columns whose id is in `needed`.
        let mut required_names: Vec<String> = node
            .columns
            .iter()
            .filter(|c| needed.contains(&c.column_id))
            .map(|c| c.name.clone())
            .collect();

        // Union in columns referenced by any pushed-down predicates so that
        // predicate evaluation can still access them even if the parent didn't
        // explicitly request them.
        //
        // Predicates are ScalarId handles; use the arena to collect referenced
        // ColumnIds, then map those ids to column names.
        let pred_col_ids: HashSet<ColumnId> = if node.predicates.is_empty() {
            HashSet::new()
        } else {
            let arena_rc = ctx.scalar_arena();
            let arena = arena_rc.borrow();
            let mut ids = HashSet::new();
            for &pred_id in &node.predicates {
                collect_scalar_column_ids(&arena, pred_id, &mut ids);
            }
            ids
        };

        let mut existing_lower: HashSet<String> =
            required_names.iter().map(|n| n.to_lowercase()).collect();

        for col in &node.columns {
            let col_lower = col.name.to_lowercase();
            if pred_col_ids.contains(&col.column_id) && !existing_lower.contains(&col_lower) {
                existing_lower.insert(col_lower);
                required_names.push(col.name.clone());
            }
        }

        for col in &node.columns {
            let col_lower = col.name.to_lowercase();
            if col.is_internal && !existing_lower.contains(&col_lower) {
                existing_lower.insert(col_lower);
                required_names.push(col.name.clone());
            }
        }

        // Keep at least one column (so the scan has a valid output layout, e.g.
        // for COUNT(*) queries that reference no specific columns).
        if required_names.is_empty() && !node.columns.is_empty() {
            required_names.push(node.columns[0].name.clone());
        }

        // Unchanged check: if the set of names is already the same, no-op.
        let required_names_set: HashSet<&str> = required_names.iter().map(|s| s.as_str()).collect();

        let unchanged = match &node.required_columns {
            Some(existing) => {
                let existing_set: HashSet<&str> = existing.iter().map(|s| s.as_str()).collect();
                existing_set == required_names_set
            }
            None => false, // was None, now Some — that's a change
        };

        if unchanged {
            return Ok(RewriteResult::Unchanged);
        }

        node.required_columns = Some(required_names);
        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalScan(node),
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
    use crate::sql::optimizer::operator::{Operator, ScanOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::optimizer::scalar::{self, ScalarArena, ScalarNode};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    fn make_scan(cols: &[(&str, ColumnId)]) -> ScanOp {
        let table = TableDef {
            name: "t".to_string(),
            columns: cols
                .iter()
                .map(|(name, _)| ColumnDef {
                    name: name.to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        ScanOp {
            database: "db".to_string(),
            table,
            alias: None,
            stats_ref: None,
            columns: cols
                .iter()
                .map(|(name, id)| OutputColumn {
                    column_id: *id,
                    name: name.to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    fn scan_expr(scan: ScanOp, required_output_columns: Option<HashSet<ColumnId>>) -> OptExpr {
        OptExpr {
            op: Operator::LogicalScan(scan),
            children: vec![],
            required_output_columns,
        }
    }

    fn ctx_with_arena() -> RewriteContext {
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        let arena = Rc::new(RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(arena);
        ctx
    }

    #[test]
    fn prune_scan_filters_to_needed_subset() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let scan = make_scan(&[("a", id_a), ("b", id_b), ("c", id_c)]);
        // Tag: only column b needed.
        let mut needed = HashSet::new();
        needed.insert(id_b);

        let expr = scan_expr(scan, Some(needed));
        let rule = PruneScanColumns;
        let mut ctx = ctx_with_arena();
        let result = rule.apply(expr, &mut ctx).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalScan(pruned) = &changed.op else {
            panic!("expected Scan");
        };

        let req = pruned
            .required_columns
            .as_ref()
            .expect("required_columns must be set");
        assert_eq!(req.len(), 1);
        assert_eq!(req[0], "b");
    }

    #[test]
    fn prune_scan_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let scan = make_scan(&[("a", id_a)]);
        // No Phase-1 tag (None).

        let expr = scan_expr(scan, None);
        let rule = PruneScanColumns;
        let mut ctx = ctx_with_arena();
        let result = rule.apply(expr, &mut ctx).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_scan_includes_predicate_columns() {
        // needed = {a}, but there's a predicate referencing b.
        // After pruning: required_columns should include both a and b.
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut scan = make_scan(&[("a", id_a), ("b", id_b), ("c", id_c)]);

        // Build a scalar predicate: b > 0 (referencing id_b).
        let mut arena = ScalarArena::new();
        let col_b = arena.intern(ScalarNode::ColumnRef(id_b), DataType::Int32, false);
        let zero = arena.intern(
            ScalarNode::Literal(scalar::HashableLiteral(
                crate::sql::analysis::LiteralValue::Int(0),
            )),
            DataType::Int32,
            false,
        );
        let pred = arena.intern(
            ScalarNode::BinaryOp {
                op: crate::sql::analysis::BinOp::Gt,
                left: col_b,
                right: zero,
            },
            DataType::Boolean,
            false,
        );
        scan.predicates.push(pred);

        let mut needed = HashSet::new();
        needed.insert(id_a);

        let expr = scan_expr(scan, Some(needed));
        let rule = PruneScanColumns;

        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        ctx.set_scalar_arena(Rc::new(RefCell::new(arena)));
        let result = rule.apply(expr, &mut ctx).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            _ => panic!("expected Changed"),
        };
        let Operator::LogicalScan(pruned) = &changed.op else {
            panic!("expected Scan");
        };

        let req = pruned
            .required_columns
            .as_ref()
            .expect("required_columns must be set");
        let req_set: HashSet<&str> = req.iter().map(|s| s.as_str()).collect();
        assert!(req_set.contains("a"), "a must be kept (in needed)");
        assert!(
            req_set.contains("b"),
            "b must be kept (predicate reference)"
        );
        assert!(!req_set.contains("c"), "c not needed");
    }

    #[test]
    fn prune_scan_preserves_internal_columns() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_internal = ColumnId::new_for_test(3);

        let mut scan = make_scan(&[("a", id_a), ("b", id_b), ("__change_op", id_internal)]);
        scan.columns
            .iter_mut()
            .find(|col| col.name == "__change_op")
            .expect("internal column exists")
            .is_internal = true;

        let mut needed = HashSet::new();
        needed.insert(id_a);

        let rule = PruneScanColumns;
        let mut ctx = ctx_with_arena();
        let result = rule.apply(scan_expr(scan, Some(needed)), &mut ctx).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            _ => panic!("expected Changed"),
        };
        let Operator::LogicalScan(pruned) = &changed.op else {
            panic!("expected Scan");
        };

        let req = pruned
            .required_columns
            .as_ref()
            .expect("required_columns must be set");
        let req_set: HashSet<&str> = req.iter().map(|s| s.as_str()).collect();
        assert!(req_set.contains("a"), "requested column must be kept");
        assert!(
            req_set.contains("__change_op"),
            "internal column must be preserved"
        );
        assert!(
            !req_set.contains("b"),
            "ordinary unrequested column is pruned"
        );
    }

    #[test]
    fn prune_scan_keeps_at_least_one_column_when_needed_is_empty() {
        // needed is Some(empty set) — still need at least one column.
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);

        let scan = make_scan(&[("a", id_a), ("b", id_b)]);

        let expr = scan_expr(scan, Some(HashSet::new()));
        let rule = PruneScanColumns;
        let mut ctx = ctx_with_arena();
        let result = rule.apply(expr, &mut ctx).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            _ => panic!("expected Changed"),
        };
        let Operator::LogicalScan(pruned) = &changed.op else {
            panic!("expected Scan");
        };

        let req = pruned
            .required_columns
            .as_ref()
            .expect("required_columns must be set");
        assert_eq!(req.len(), 1, "at least one column must survive");
    }
}
