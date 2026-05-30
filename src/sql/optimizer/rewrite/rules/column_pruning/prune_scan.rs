//! PruneScanColumns — Phase 2 rule for Scan nodes.
//!
//! Translates the ColumnId-based `required_output_columns` set (written by the
//! Phase-1 tagging pass) into the string-name-based `required_columns` list
//! that codegen / fragment_builder reads.
//!
//! Also unions in any columns referenced by pushed-down predicates so that
//! predicate evaluation is not broken by column pruning.

use std::collections::HashSet;

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::collect_column_refs;
use crate::sql::planner::plan::*;

pub(crate) struct PruneScanColumns;

impl LogicalRewriteRule for PruneScanColumns {
    fn name(&self) -> &'static str {
        "PruneScanColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Scan(_))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Scan(mut node) = plan else {
            unreachable!()
        };

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = node.required_output_columns.clone() else {
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
        let pred_col_names: HashSet<String> = node
            .predicates
            .iter()
            .flat_map(|pred| collect_column_refs(pred))
            .map(|name| name.to_lowercase())
            .collect();

        let mut existing_lower: HashSet<String> =
            required_names.iter().map(|n| n.to_lowercase()).collect();

        for col in &node.columns {
            let col_lower = col.name.to_lowercase();
            if pred_col_names.contains(&col_lower) && !existing_lower.contains(&col_lower) {
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
        Ok(RewriteResult::Changed(LogicalPlan::Scan(node)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use arrow::datatypes::DataType;

    fn make_scan(cols: &[(&str, ColumnId)]) -> ScanNode {
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
        ScanNode {
            database: "db".to_string(),
            table,
            alias: None,
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
            dict_columns: vec![],
            required_output_columns: None,
        }
    }

    fn col_ref_expr(id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn ctx() -> RewriteContext {
        RewriteContext::new(RewriteConsumer::Query)
    }

    #[test]
    fn prune_scan_filters_to_needed_subset() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut scan = make_scan(&[("a", id_a), ("b", id_b), ("c", id_c)]);
        // Tag: only column b needed.
        let mut needed = HashSet::new();
        needed.insert(id_b);
        scan.required_output_columns = Some(needed);

        let plan = LogicalPlan::Scan(scan);
        let rule = PruneScanColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::Scan(pruned) = changed else {
            panic!("expected Scan");
        };

        let req = pruned
            .required_columns
            .expect("required_columns must be set");
        assert_eq!(req.len(), 1);
        assert_eq!(req[0], "b");
    }

    #[test]
    fn prune_scan_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let mut scan = make_scan(&[("a", id_a)]);
        // No Phase-1 tag (None).
        scan.required_output_columns = None;

        let plan = LogicalPlan::Scan(scan);
        let rule = PruneScanColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

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
        // Push a predicate referencing b.
        let pred = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref_expr(id_b, "b")),
                op: BinOp::Gt,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(0)),
                    data_type: DataType::Int32,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        scan.predicates.push(pred);

        let mut needed = HashSet::new();
        needed.insert(id_a);
        scan.required_output_columns = Some(needed);

        let plan = LogicalPlan::Scan(scan);
        let rule = PruneScanColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            _ => panic!("expected Changed"),
        };
        let LogicalPlan::Scan(pruned) = changed else {
            panic!("expected Scan");
        };

        let req = pruned
            .required_columns
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
        scan.required_output_columns = Some(needed);

        let rule = PruneScanColumns;
        let result = rule.apply(LogicalPlan::Scan(scan), &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            _ => panic!("expected Changed"),
        };
        let LogicalPlan::Scan(pruned) = changed else {
            panic!("expected Scan");
        };

        let req = pruned
            .required_columns
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

        let mut scan = make_scan(&[("a", id_a), ("b", id_b)]);
        scan.required_output_columns = Some(HashSet::new());

        let plan = LogicalPlan::Scan(scan);
        let rule = PruneScanColumns;
        let result = rule.apply(plan, &mut ctx()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            _ => panic!("expected Changed"),
        };
        let LogicalPlan::Scan(pruned) = changed else {
            panic!("expected Scan");
        };

        let req = pruned
            .required_columns
            .expect("required_columns must be set");
        assert_eq!(req.len(), 1, "at least one column must survive");
    }
}
