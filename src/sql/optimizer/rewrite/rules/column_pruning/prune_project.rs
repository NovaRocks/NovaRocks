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

/// Returns `true` when a project item's expression is an `assert_true(...)` call.
///
/// Such items must never be dropped by column pruning, even when their
/// `output_column_id` is not referenced by any upstream operator.  This mirrors
/// the StarRocks `PruneProjectColumnsRule` carve-out for `assert_true` items
/// (used, e.g., for the per-group row-check emitted by `ScalarApplyToJoin`).
fn is_assert_true_item(item: &ProjectItem) -> bool {
    matches!(
        &item.expr.kind,
        ExprKind::FunctionCall { name, .. } if name == "assert_true"
    )
}

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
        // assert_true items are always kept regardless of whether their
        // output_column_id appears in needed: they carry runtime correctness
        // checks (e.g. the per-group row-check from ScalarApplyToJoin) that
        // must not be silently dropped when nothing upstream references them.
        let mut new_items: Vec<ProjectItem> = node
            .items
            .into_iter()
            .filter(|item| {
                item.output_column_id == ColumnId::UNSET
                    || needed.contains(&item.output_column_id)
                    || is_assert_true_item(item)
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
            variant_columns: vec![],
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
            output_qualifier: None,
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
            output_qualifier: None,
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
            output_qualifier: None,
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
            output_qualifier: None,
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

    /// Carve-out regression test: an `assert_true` item in an inner Project is
    /// NEVER dropped by `PruneProjectColumns`, even when nothing upstream
    /// references its `output_column_id`.
    ///
    /// Plan shape:
    ///   Project_outer(needed={out_x}) [items: x→out_x]
    ///     Project_inner [items: x→out_x (passthrough), assert_true(cnt IS NULL OR cnt<=1)→assert_id]
    ///       Scan[x@id_x, cnt@id_cnt, dummy@id_dummy]
    ///
    /// Without the carve-out, PruneProjectColumns would drop the assert_true item
    /// from Project_inner because assert_id ∉ {out_x}.  With it, the item survives
    /// and tag_required_columns also unions cnt into child_needed so id_cnt reaches
    /// the Scan's required_output_columns.
    #[test]
    fn prune_project_assert_true_item_survives_even_when_not_in_needed() {
        use crate::sql::analysis::{BinOp, LiteralValue};
        use crate::sql::optimizer::rewrite::required_columns::tag_required_columns;

        let id_x = ColumnId::new_for_test(1);
        let id_cnt = ColumnId::new_for_test(2);
        let id_dummy = ColumnId::new_for_test(3);
        let out_x = ColumnId::new_for_test(101); // output id for x in inner project
        let assert_id = ColumnId::new_for_test(200);

        // 3-column scan: x, cnt, dummy (uses existing make_scan helper).
        let scan = make_scan(id_x, id_cnt, id_dummy);

        // Build the assert_true condition: cnt IS NULL OR cnt <= 1.
        let cnt_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id_cnt,
                qualifier: None,
                column: "cnt".to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };
        let cnt_is_null = TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(cnt_ref.clone()),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let cnt_le_1 = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(cnt_ref),
                op: BinOp::Le,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let assert_cond = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(cnt_is_null),
                op: BinOp::Or,
                right: Box::new(cnt_le_1),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let assert_true_expr = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "assert_true".to_string(),
                args: vec![
                    assert_cond,
                    TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::String(
                            "subquery must return at most 1 row".to_string(),
                        )),
                        data_type: DataType::Utf8,
                        nullable: false,
                    },
                ],
                distinct: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };

        // Project_inner: [x→out_x (passthrough, different output_column_id),
        //                 assert_true(...)→assert_id]
        // The x item's expr references id_x (from scan) but its output_column_id
        // is out_x (a different ColumnId, simulating a planner-assigned output id).
        let inner_x_item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: id_x,
                    qualifier: None,
                    column: "x".to_string(),
                },
                data_type: DataType::Int32,
                nullable: false,
            },
            output_name: "x".to_string(),
            output_column_id: out_x,
        };
        let inner_project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![
                inner_x_item,
                ProjectItem {
                    expr: assert_true_expr,
                    output_name: "__assert".to_string(),
                    output_column_id: assert_id,
                },
            ],
            output_qualifier: None,
            required_output_columns: None,
        });

        // Project_outer: [x→out_x] — only references out_x, does NOT reference assert_id.
        let outer_x_item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: out_x,
                    qualifier: None,
                    column: "x".to_string(),
                },
                data_type: DataType::Int32,
                nullable: false,
            },
            output_name: "x".to_string(),
            output_column_id: out_x,
        };
        let outer_project = LogicalPlan::Project(ProjectNode {
            input: Box::new(inner_project),
            items: vec![outer_x_item],
            output_qualifier: None,
            required_output_columns: None,
        });

        // Phase 1: tag_required_columns with parent_needed = None (root).
        // After tagging, inner Project's required_output_columns = Some({out_x})
        // because the outer Project only needs out_x; but the assert_true carve-out
        // in tag_project must ALSO include id_cnt in the Scan's child_needed.
        let tagged = tag_required_columns(outer_project, None);

        // Phase 2: apply PruneProjectColumns on each Project node top-down.
        let rule = PruneProjectColumns;
        let mut ctx = ctx_with_factory();

        // Pull out the tagged outer Project node and apply the rule.
        let LogicalPlan::Project(outer_node) = tagged else {
            panic!("expected outer Project after tagging");
        };
        let outer_result = rule
            .apply(LogicalPlan::Project(outer_node.clone()), &mut ctx)
            .unwrap();
        // Outer has 1 item (out_x) which is in needed — result is Unchanged.
        let outer_after_node = match outer_result {
            RewriteResult::Changed(LogicalPlan::Project(p)) => p,
            RewriteResult::Unchanged => outer_node,
            other => panic!("unexpected outer result: {:?}", other),
        };

        // Now apply the rule to the inner Project (which was tagged with needed={out_x}).
        // With the carve-out both items survive (x ∈ needed, assert_true via carve-out),
        // so lengths are equal and the rule returns Unchanged.
        // Without the carve-out the assert_true item would be dropped → Changed with 1 item.
        // Either way we need the inner ProjectNode to inspect items; carry it along.
        let inner_plan = *outer_after_node.input;
        // Clone the plan so we can inspect items regardless of Changed/Unchanged.
        let inner_plan_clone = inner_plan.clone();
        let inner_result = rule.apply(inner_plan, &mut ctx).unwrap();

        let inner_pruned_node = match inner_result {
            RewriteResult::Changed(LogicalPlan::Project(p)) => p,
            RewriteResult::Unchanged => {
                // Carve-out preserved both items → no change → extract from clone.
                let LogicalPlan::Project(p) = inner_plan_clone else {
                    panic!("expected inner Project in clone");
                };
                p
            }
            other => panic!("unexpected inner result: {:?}", other),
        };

        // The assert_true item MUST survive even though assert_id ∉ {out_x}.
        let has_assert_true = inner_pruned_node.items.iter().any(|item| {
            matches!(&item.expr.kind, ExprKind::FunctionCall { name, .. } if name == "assert_true")
        });
        assert!(
            has_assert_true,
            "assert_true item must survive PruneProjectColumns (carve-out missing)"
        );

        // The cnt column must appear in the child Scan's required_output_columns.
        // tag_required_columns unions assert_true item's column refs (id_cnt) into
        // child_needed, so the Scan must expose cnt even though out_x doesn't reference it.
        let LogicalPlan::Scan(scan_node) = inner_pruned_node.input.as_ref() else {
            panic!("expected Scan under inner Project");
        };
        let scan_req = scan_node
            .required_output_columns
            .as_ref()
            .expect("Scan must have required_output_columns after tagging");
        assert!(
            scan_req.contains(&id_cnt),
            "cnt column must reach the Scan (assert_true item refs must be in child_needed)"
        );
    }
}
