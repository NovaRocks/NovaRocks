//! PruneProjectColumns — Phase 2 rule for Project nodes.
//!
//! Filters `ProjectNode.items` to only those whose `output_column_id` is in
//! `required_output_columns`. When all items would be filtered out, an
//! auto-fill placeholder item (`const 1 AS auto_fill_<id>`) is inserted using
//! a freshly minted ColumnId from the ColumnRefFactory in context (Gap 2).

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, LiteralValue, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::column_pruning::auto_fill_column_id;
use crate::sql::planner::plan::*;

pub(crate) struct PruneProjectColumns;

impl LogicalRewriteRule for PruneProjectColumns {
    fn name(&self) -> &'static str {
        "PruneProjectColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Project(_))
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Project(mut node) = plan else {
            unreachable!()
        };

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = node.required_output_columns.clone() else {
            return Ok(RewriteResult::Unchanged);
        };

        let original_len = node.items.len();

        // Filter items to those whose output_column_id is in needed.
        // Items with UNSET output_column_id are kept (synthetic dict-slot items
        // that are never addressed by pruning — same logic as
        // collect_output_ids_ordered which excludes UNSET).
        let mut new_items: Vec<ProjectItem> = node
            .items
            .into_iter()
            .filter(|item| {
                item.output_column_id == ColumnId::UNSET || needed.contains(&item.output_column_id)
            })
            .collect();

        // If all items were filtered out, insert one auto-fill placeholder.
        let was_auto_filled = new_items.is_empty();
        if was_auto_filled {
            let fill_id = auto_fill_column_id(ctx).unwrap_or(ColumnId::UNSET);
            let fill_name = format!("auto_fill_{}", fill_id.0);
            new_items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: fill_name,
                output_column_id: fill_id,
            });
        }

        // Unchanged check: same number of items as before means nothing was pruned.
        // BUT: if auto-fill fired, we must return Changed even when lengths coincidentally
        // match (e.g. single-item Project whose only item was dropped and replaced by the
        // auto-fill const).
        if !was_auto_filled && new_items.len() == original_len {
            return Ok(RewriteResult::Unchanged);
        }

        node.items = new_items;
        Ok(RewriteResult::Changed(LogicalPlan::Project(node)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnRefFactory;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use arrow::datatypes::DataType;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    fn make_scan(id_a: ColumnId, id_b: ColumnId, id_c: ColumnId) -> LogicalPlan {
        let table = TableDef {
            name: "t".to_string(),
            columns: vec![
                ColumnDef {
                    name: "a".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "b".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "c".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        };
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table,
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: id_a,
                    name: "a".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: id_b,
                    name: "b".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: id_c,
                    name: "c".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                },
            ],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn col_ref_item(id: ColumnId, name: &str) -> ProjectItem {
        ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: id,
                    qualifier: None,
                    column: name.to_string(),
                },
                data_type: DataType::Int32,
                nullable: false,
            },
            output_name: name.to_string(),
            output_column_id: id,
        }
    }

    fn ctx_with_factory() -> RewriteContext {
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        ctx.set_column_ref_factory(factory);
        ctx
    }

    #[test]
    fn prune_project_filters_items_to_needed_subset() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut node = ProjectNode {
            input: Box::new(make_scan(id_a, id_b, id_c)),
            items: vec![
                col_ref_item(id_a, "a"),
                col_ref_item(id_b, "b"),
                col_ref_item(id_c, "c"),
            ],
            required_output_columns: None,
        };

        // Only b is needed.
        let mut needed = HashSet::new();
        needed.insert(id_b);
        node.required_output_columns = Some(needed);

        let plan = LogicalPlan::Project(node);
        let rule = PruneProjectColumns;
        let result = rule.apply(plan, &mut ctx_with_factory()).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::Project(pruned) = changed else {
            panic!("expected Project");
        };

        assert_eq!(pruned.items.len(), 1);
        assert_eq!(pruned.items[0].output_column_id, id_b);
    }

    #[test]
    fn prune_project_noop_when_required_output_columns_is_none() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let node = ProjectNode {
            input: Box::new(make_scan(id_a, id_b, id_c)),
            items: vec![
                col_ref_item(id_a, "a"),
                col_ref_item(id_b, "b"),
                col_ref_item(id_c, "c"),
            ],
            required_output_columns: None, // No Phase-1 tag
        };

        let plan = LogicalPlan::Project(node);
        let rule = PruneProjectColumns;
        let result = rule.apply(plan, &mut ctx_with_factory()).unwrap();

        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must be no-op when required_output_columns is None"
        );
    }

    #[test]
    fn prune_project_all_items_dropped_produces_auto_fill() {
        // needed is a set that contains no item output_column_ids.
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);
        let id_unknown = ColumnId::new_for_test(999);

        let mut node = ProjectNode {
            input: Box::new(make_scan(id_a, id_b, id_c)),
            items: vec![col_ref_item(id_a, "a"), col_ref_item(id_b, "b")],
            required_output_columns: None,
        };

        // Needed is {999} — not present in any item's output_column_id.
        let mut needed = HashSet::new();
        needed.insert(id_unknown);
        node.required_output_columns = Some(needed);

        let plan = LogicalPlan::Project(node);
        let rule = PruneProjectColumns;
        let mut ctx = ctx_with_factory();
        let result = rule.apply(plan, &mut ctx).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let LogicalPlan::Project(pruned) = changed else {
            panic!("expected Project");
        };

        assert_eq!(
            pruned.items.len(),
            1,
            "auto-fill must produce exactly 1 item"
        );
        // The auto-fill item should be a literal expression.
        assert!(
            matches!(pruned.items[0].expr.kind, ExprKind::Literal(_)),
            "auto-fill expr must be a literal"
        );
    }

    /// Regression test for the false-Unchanged bug: a Project with exactly ONE item
    /// whose `output_column_id` is NOT in `needed`. Auto-fill fires and replaces the
    /// single item — lengths coincidentally match (1 == 1) but the result must be
    /// `Changed` and the surviving item must be the auto-fill const, not the original.
    #[test]
    fn prune_project_single_item_dropped_auto_fill_is_changed() {
        let id_a = ColumnId::new_for_test(1);
        let id_unknown = ColumnId::new_for_test(999);

        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let mut node = ProjectNode {
            input: Box::new(make_scan(id_a, id_b, id_c)),
            items: vec![col_ref_item(id_a, "a")], // single item, NOT in needed
            required_output_columns: None,
        };

        // needed = {999} — not present in the single item's output_column_id.
        let mut needed = HashSet::new();
        needed.insert(id_unknown);
        node.required_output_columns = Some(needed);

        let plan = LogicalPlan::Project(node);
        let rule = PruneProjectColumns;
        let mut ctx = ctx_with_factory();
        let result = rule.apply(plan, &mut ctx).unwrap();

        // Must be Changed — not Unchanged — even though original_len == new_items.len() == 1.
        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!(
                "expected Changed but got {:?} — auto-fill false-Unchanged bug is present",
                other
            ),
        };
        let LogicalPlan::Project(pruned) = changed else {
            panic!("expected Project");
        };

        assert_eq!(
            pruned.items.len(),
            1,
            "must have exactly one item (auto-fill)"
        );
        // The surviving item must be the auto-fill const literal, not the original column ref.
        assert!(
            matches!(pruned.items[0].expr.kind, ExprKind::Literal(_)),
            "surviving item must be the auto-fill literal, not the original ColumnRef"
        );
        // The output name must follow the auto_fill_ convention, not the original "a".
        assert!(
            pruned.items[0].output_name.starts_with("auto_fill_"),
            "auto-fill item name must start with 'auto_fill_', got: {}",
            pruned.items[0].output_name
        );
    }
}
