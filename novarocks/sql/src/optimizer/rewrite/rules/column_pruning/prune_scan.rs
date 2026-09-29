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
//! Freezes the ColumnId-based `required_output_columns` set (written by the
//! Phase-1 tagging pass) as the scan's ordered source-column projection.
//!
//! Also unions in any columns referenced by pushed-down predicates so that
//! predicate evaluation is not broken by column pruning.

use std::collections::HashSet;

use crate::column_id::ColumnId;
use crate::optimizer::operator::Operator;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::pattern::{OpKind, Pattern};
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::optimizer::scalar::{self, ScalarNode};

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

        // Union in columns referenced by any pushed-down predicates so that
        // predicate evaluation can still access them even if the parent didn't
        // explicitly request them.
        //
        // Predicates are ScalarId handles; use the arena to collect referenced
        // ColumnIds.
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

        // A retained derived VARIANT value is computed by the engine from its
        // exact source value. Keep that source in the physical scan contract;
        // the synthetic output itself must never be requested from the provider.
        let retained_synthetic = node
            .variant_columns
            .iter()
            .filter(|descriptor| {
                needed.contains(&descriptor.synthetic_column_id)
                    || pred_col_ids.contains(&descriptor.synthetic_column_id)
            })
            .map(|descriptor| descriptor.synthetic_column_id)
            .collect::<HashSet<_>>();
        let all_synthetic = node
            .variant_columns
            .iter()
            .map(|descriptor| descriptor.synthetic_column_id)
            .collect::<HashSet<_>>();
        let variant_sources = node
            .variant_columns
            .iter()
            .filter(|descriptor| retained_synthetic.contains(&descriptor.synthetic_column_id))
            .map(|descriptor| descriptor.source_column_id)
            .collect::<HashSet<_>>();
        let required_columns = node
            .columns
            .iter()
            .filter(|column| {
                if all_synthetic.contains(&column.column_id) {
                    retained_synthetic.contains(&column.column_id)
                } else {
                    needed.contains(&column.column_id)
                        || pred_col_ids.contains(&column.column_id)
                        || column.is_internal
                        || variant_sources.contains(&column.column_id)
                }
            })
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        // Counting rows needs no column, but a read does: the scan source
        // carries an ordered assignment per column and a provider has nothing
        // to return rows from without one. So a read that needs no value still
        // names one, chosen here rather than left to whoever notices the gap.
        // Keeping the first is deliberate and not a fallback to everything -
        // the old behaviour, which read the whole relation to count it.
        let required_columns = if required_columns.is_empty() {
            node.columns
                .iter()
                .find(|column| !all_synthetic.contains(&column.column_id))
                .map(|column| vec![column.column_id])
                .unwrap_or_default()
        } else {
            required_columns
        };

        let column_count = node.columns.len();
        node.columns.retain(|column| {
            !all_synthetic.contains(&column.column_id)
                || retained_synthetic.contains(&column.column_id)
        });
        let variant_count = node.variant_columns.len();
        node.variant_columns
            .retain(|descriptor| retained_synthetic.contains(&descriptor.synthetic_column_id));

        let unchanged = node.required_columns.as_ref() == Some(&required_columns)
            && node.columns.len() == column_count
            && node.variant_columns.len() == variant_count;

        if unchanged {
            return Ok(RewriteResult::Unchanged);
        }

        node.required_columns = Some(required_columns);
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
    use crate::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::{Operator, ScanOp, ScanVariantColumn};
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::optimizer::scalar::{self, ScalarArena, ScalarNode};
    use crate::planner::table::TableDef;
    use arrow::datatypes::DataType;
    use novarocks_types::schema::ColumnDef;
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
            source: crate::compiler::mv_rewrite::test_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
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
        let mut ctx = RewriteContext::new(
            RewriteConsumer::Query,
            crate::optimizer::options::SessionOptimizerSettings::default(),
        );
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
        assert_eq!(req[0], id_b);
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
            ScalarNode::Literal(scalar::HashableLiteral(crate::analysis::LiteralValue::Int(
                0,
            ))),
            DataType::Int32,
            false,
        );
        let pred = arena.intern(
            ScalarNode::BinaryOp {
                op: crate::analysis::BinOp::Gt,
                left: col_b,
                right: zero,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            DataType::Boolean,
            false,
        );
        scan.predicates.push(pred);

        let mut needed = HashSet::new();
        needed.insert(id_a);

        let expr = scan_expr(scan, Some(needed));
        let rule = PruneScanColumns;

        let mut ctx = RewriteContext::new(
            RewriteConsumer::Query,
            crate::optimizer::options::SessionOptimizerSettings::default(),
        );
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
        let req_set: HashSet<ColumnId> = req.iter().copied().collect();
        assert!(req_set.contains(&id_a), "a must be kept (in needed)");
        assert!(
            req_set.contains(&id_b),
            "b must be kept (predicate reference)"
        );
        assert!(!req_set.contains(&id_c), "c not needed");
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
        let req_set: HashSet<ColumnId> = req.iter().copied().collect();
        assert!(req_set.contains(&id_a), "requested column must be kept");
        assert!(
            req_set.contains(&id_internal),
            "internal column must be preserved"
        );
        assert!(
            !req_set.contains(&id_b),
            "ordinary unrequested column is pruned"
        );
    }

    #[test]
    fn prune_scan_keeps_source_for_retained_variant_derivation() {
        let source_id = ColumnId::new_for_test(1);
        let synthetic_id = ColumnId::new_for_test(2);
        let mut scan = make_scan(&[("payload", source_id)]);
        scan.table.columns[0].data_type = DataType::LargeBinary;
        scan.table.columns[0].nullable = true;
        scan.columns[0].data_type = DataType::LargeBinary;
        scan.columns[0].nullable = true;
        let source = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: source_id,
                qualifier: None,
                column: "payload".to_string(),
            },
            data_type: DataType::LargeBinary,
            nullable: true,
        };
        let path = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String("$.id".to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        };
        let requested_type = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String("bigint".to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        };
        let args = vec![source, path, requested_type];
        scan.columns.push(OutputColumn {
            column_id: synthetic_id,
            name: "__nr_var_payload_0".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: true,
        });
        scan.variant_columns.push(ScanVariantColumn {
            source_column_id: source_id,
            source_column: "payload".to_string(),
            synthetic_column_id: synthetic_id,
            synthetic_column: "__nr_var_payload_0".to_string(),
            canonical_path: "$.id".to_string(),
            requested_type: DataType::Int64,
            requested_type_literal: "bigint".to_string(),
            strict: true,
            binding: crate::analysis::test_function_binding(
                "variant_get",
                &args,
                DataType::Int64,
                true,
                novarocks_functions::FunctionVolatility::Immutable,
            ),
        });

        let rule = PruneScanColumns;
        let mut ctx = ctx_with_arena();
        let result = rule
            .apply(
                scan_expr(scan, Some(HashSet::from([synthetic_id]))),
                &mut ctx,
            )
            .unwrap();
        let RewriteResult::Changed(changed) = result else {
            panic!("expected Changed");
        };
        let Operator::LogicalScan(pruned) = changed.op else {
            panic!("expected Scan");
        };
        assert_eq!(
            pruned.required_columns.unwrap(),
            vec![source_id, synthetic_id]
        );
    }

    #[test]
    fn prune_scan_removes_an_unused_variant_output_and_its_descriptor() {
        let source_id = ColumnId::new_for_test(1);
        let synthetic_id = ColumnId::new_for_test(2);
        let mut scan = make_scan(&[("payload", source_id)]);
        scan.columns.push(OutputColumn {
            column_id: synthetic_id,
            name: "__nr_var_payload_0".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: true,
        });
        scan.variant_columns.push(ScanVariantColumn {
            source_column_id: source_id,
            source_column: "payload".to_string(),
            synthetic_column_id: synthetic_id,
            synthetic_column: "__nr_var_payload_0".to_string(),
            canonical_path: "$.id".to_string(),
            requested_type: DataType::Int64,
            requested_type_literal: "bigint".to_string(),
            strict: true,
            binding: crate::analysis::test_function_binding(
                "variant_get",
                &[],
                DataType::Int64,
                true,
                novarocks_functions::FunctionVolatility::Immutable,
            ),
        });

        let rule = PruneScanColumns;
        let mut ctx = ctx_with_arena();
        let result = rule
            .apply(scan_expr(scan, Some(HashSet::new())), &mut ctx)
            .expect("unused VARIANT pruning must succeed");
        let RewriteResult::Changed(changed) = result else {
            panic!("expected Changed");
        };
        let Operator::LogicalScan(pruned) = changed.op else {
            panic!("expected Scan");
        };
        assert_eq!(
            pruned
                .columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![source_id]
        );
        // The synthetic output and its descriptor are gone; the source column
        // stays, and is what the read now names.
        assert_eq!(
            pruned.required_columns.as_deref(),
            Some(&[source_id][..]),
            "a read that needs no value still names one column"
        );
        assert!(pruned.variant_columns.is_empty());
    }

    #[test]
    fn prune_scan_names_one_column_for_a_row_count_read() {
        // Counting rows needs no value, but reading them needs a column.
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
        // Counting rows needs no value, but reading them needs a column: the
        // scan source has an assignment per column and none to read from
        // otherwise. One column is named, not all of them.
        assert_eq!(req.len(), 1, "a row-count read still names one column");
    }
}
