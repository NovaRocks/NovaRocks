//! PruneProjectColumns — Phase 2 rule for Project nodes.
//!
//! Filters `ProjectOp.items` to only those whose `output_column_id` is in
//! `required_output_columns`. When all items would be filtered out, an
//! auto-fill placeholder item (`const 1 AS auto_fill_<id>`) is inserted using
//! a freshly minted ColumnId from the ColumnRefFactory in context (Gap 2).

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{Operator, ScalarProjectItem};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::column_pruning::auto_fill_column_id;
use crate::sql::optimizer::scalar::{self, ScalarArena, ScalarId, ScalarNode};

/// Returns `true` when a project item's expression is an `assert_true(...)` call.
///
/// Such items must never be dropped by column pruning, even when their
/// `output_column_id` is not referenced by any upstream operator.  This mirrors
/// the StarRocks `PruneProjectColumnsRule` carve-out for `assert_true` items
/// (used, e.g., for the per-group row-check emitted by `ScalarApplyToJoin`).
fn is_assert_true_item(arena: &ScalarArena, item: &ScalarProjectItem) -> bool {
    matches!(
        arena.node(item.expr),
        ScalarNode::FunctionCall { name, .. } if name == "assert_true"
    )
}

/// Intern `const 1` (Int64) into the arena and return its `ScalarId`.
fn intern_const_one(arena: &mut ScalarArena) -> ScalarId {
    use crate::sql::common::LiteralValue;
    arena.intern(
        ScalarNode::Literal(scalar::HashableLiteral(LiteralValue::Int(1))),
        DataType::Int64,
        false,
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

    fn matches(&self, expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        matches!(&expr.op, Operator::LogicalProject(_))
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let OptExpr {
            op,
            children,
            required_output_columns,
        } = expr;
        let Operator::LogicalProject(mut node) = op else {
            unreachable!()
        };

        // None means Phase 1 hasn't tagged this node — no-op.
        let Some(needed) = required_output_columns.clone() else {
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
        let arena_rc = ctx.scalar_arena();
        let arena = arena_rc.borrow();
        let mut new_items: Vec<ScalarProjectItem> = node
            .items
            .into_iter()
            .filter(|item| {
                item.output_column_id == ColumnId::UNSET
                    || needed.contains(&item.output_column_id)
                    || is_assert_true_item(&arena, item)
            })
            .collect();
        drop(arena);

        // If all items were filtered out, insert one auto-fill placeholder.
        let was_auto_filled = new_items.is_empty();
        if was_auto_filled {
            let fill_id = auto_fill_column_id(ctx).unwrap_or(ColumnId::UNSET);
            let fill_name = format!("auto_fill_{}", fill_id.0);
            let arena_rc = ctx.scalar_arena();
            let const_id = intern_const_one(&mut arena_rc.borrow_mut());
            new_items.push(ScalarProjectItem {
                expr: const_id,
                output_name: fill_name,
                output_column_id: fill_id,
                expr_display: None,
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
        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalProject(node),
            children,
            required_output_columns,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::optimizer::operator::{Operator, ProjectOp, ScalarProjectItem, ScanOp};
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::optimizer::scalar::{self, ScalarArena, ScalarNode};
    use arrow::datatypes::DataType;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    fn make_scan(id_a: ColumnId, id_b: ColumnId, id_c: ColumnId) -> OptExpr {
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
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
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
            mv_rewritten_from: None,
        }))
    }

    /// Intern a simple column-ref expression into the arena.
    fn col_ref_item(arena: &mut ScalarArena, id: ColumnId, name: &str) -> ScalarProjectItem {
        let expr = arena.intern(ScalarNode::ColumnRef(id), DataType::Int32, false);
        ScalarProjectItem {
            expr,
            output_name: name.to_string(),
            output_column_id: id,
            expr_display: None,
        }
    }

    fn ctx_with_factory_and_arena() -> (RewriteContext, Rc<RefCell<ScalarArena>>) {
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        ctx.set_column_ref_factory(factory);
        let arena = Rc::new(RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(Rc::clone(&arena));
        (ctx, arena)
    }

    fn project_expr(
        input: OptExpr,
        items: Vec<ScalarProjectItem>,
        required_output_columns: Option<HashSet<ColumnId>>,
    ) -> OptExpr {
        OptExpr {
            op: Operator::LogicalProject(ProjectOp {
                items,
                output_qualifier: None,
            }),
            children: vec![input],
            required_output_columns,
        }
    }

    #[test]
    fn prune_project_filters_items_to_needed_subset() {
        let id_a = ColumnId::new_for_test(1);
        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);

        let (mut ctx, arena_rc) = ctx_with_factory_and_arena();
        let items = {
            let mut arena = arena_rc.borrow_mut();
            vec![
                col_ref_item(&mut arena, id_a, "a"),
                col_ref_item(&mut arena, id_b, "b"),
                col_ref_item(&mut arena, id_c, "c"),
            ]
        };

        // Only b is needed.
        let mut needed = HashSet::new();
        needed.insert(id_b);

        let expr = project_expr(make_scan(id_a, id_b, id_c), items, Some(needed));
        let rule = PruneProjectColumns;
        let result = rule.apply(expr, &mut ctx).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalProject(pruned) = &changed.op else {
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

        let (mut ctx, arena_rc) = ctx_with_factory_and_arena();
        let items = {
            let mut arena = arena_rc.borrow_mut();
            vec![
                col_ref_item(&mut arena, id_a, "a"),
                col_ref_item(&mut arena, id_b, "b"),
                col_ref_item(&mut arena, id_c, "c"),
            ]
        };

        let expr = project_expr(make_scan(id_a, id_b, id_c), items, None);

        let rule = PruneProjectColumns;
        let result = rule.apply(expr, &mut ctx).unwrap();

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

        let (mut ctx, arena_rc) = ctx_with_factory_and_arena();
        let items = {
            let mut arena = arena_rc.borrow_mut();
            vec![
                col_ref_item(&mut arena, id_a, "a"),
                col_ref_item(&mut arena, id_b, "b"),
            ]
        };

        // Needed is {999} — not present in any item's output_column_id.
        let mut needed = HashSet::new();
        needed.insert(id_unknown);

        let expr = project_expr(make_scan(id_a, id_b, id_c), items, Some(needed));
        let rule = PruneProjectColumns;
        let result = rule.apply(expr, &mut ctx).unwrap();

        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got {:?}", other),
        };
        let Operator::LogicalProject(pruned) = &changed.op else {
            panic!("expected Project");
        };

        assert_eq!(
            pruned.items.len(),
            1,
            "auto-fill must produce exactly 1 item"
        );
        // The auto-fill item's output_name must follow the auto_fill_ convention.
        assert!(
            pruned.items[0].output_name.starts_with("auto_fill_"),
            "auto-fill item name must start with 'auto_fill_'"
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

        let (mut ctx, arena_rc) = ctx_with_factory_and_arena();
        let items = {
            let mut arena = arena_rc.borrow_mut();
            vec![col_ref_item(&mut arena, id_a, "a")] // single item, NOT in needed
        };

        // needed = {999} — not present in the single item's output_column_id.
        let mut needed = HashSet::new();
        needed.insert(id_unknown);

        let expr = project_expr(make_scan(id_a, id_b, id_c), items, Some(needed));
        let rule = PruneProjectColumns;
        let result = rule.apply(expr, &mut ctx).unwrap();

        // Must be Changed — not Unchanged — even though original_len == new_items.len() == 1.
        let changed = match result {
            RewriteResult::Changed(p) => p,
            other => panic!(
                "expected Changed but got {:?} — auto-fill false-Unchanged bug is present",
                other
            ),
        };
        let Operator::LogicalProject(pruned) = &changed.op else {
            panic!("expected Project");
        };

        assert_eq!(
            pruned.items.len(),
            1,
            "must have exactly one item (auto-fill)"
        );
        // The auto-fill item's output name must follow the auto_fill_ convention.
        assert!(
            pruned.items[0].output_name.starts_with("auto_fill_"),
            "surviving item must be the auto-fill literal, not the original ColumnRef"
        );
    }

    /// Carve-out regression test: an `assert_true` item in a Project is
    /// NEVER dropped by `PruneProjectColumns`, even when nothing upstream
    /// references its `output_column_id`.
    #[test]
    fn prune_project_assert_true_item_survives_even_when_not_in_needed() {
        let id_x = ColumnId::new_for_test(1);
        let out_x = ColumnId::new_for_test(101);
        let assert_id = ColumnId::new_for_test(200);

        let (mut ctx, arena_rc) = ctx_with_factory_and_arena();
        let items = {
            let mut arena = arena_rc.borrow_mut();
            // x → out_x item
            let x_expr = arena.intern(ScalarNode::ColumnRef(id_x), DataType::Int32, false);
            let x_item = ScalarProjectItem {
                expr: x_expr,
                output_name: "x".to_string(),
                output_column_id: out_x,
                expr_display: None,
            };
            // assert_true(true) → assert_id item
            let true_lit = arena.intern(
                ScalarNode::Literal(scalar::HashableLiteral(
                    crate::sql::analysis::LiteralValue::Bool(true),
                )),
                DataType::Boolean,
                false,
            );
            let assert_expr = arena.intern(
                ScalarNode::FunctionCall {
                    name: "assert_true".to_string(),
                    args: vec![true_lit],
                    distinct: false,
                },
                DataType::Boolean,
                false,
            );
            let assert_item = ScalarProjectItem {
                expr: assert_expr,
                output_name: "__assert".to_string(),
                output_column_id: assert_id,
                expr_display: None,
            };
            vec![x_item, assert_item]
        };

        // needed = {out_x} — assert_id is NOT in needed.
        let mut needed = HashSet::new();
        needed.insert(out_x);

        let id_b = ColumnId::new_for_test(2);
        let id_c = ColumnId::new_for_test(3);
        let expr = project_expr(make_scan(id_x, id_b, id_c), items, Some(needed));
        let rule = PruneProjectColumns;
        let result = rule.apply(expr, &mut ctx).unwrap();

        // With the carve-out both items survive (x ∈ needed, assert_true via carve-out),
        // so lengths are equal and the rule returns Unchanged.
        let pruned_plan = match result {
            RewriteResult::Unchanged => {
                // Unchanged is correct — both items survive, nothing was pruned.
                return;
            }
            RewriteResult::Changed(p) => p,
            other => panic!("unexpected result: {:?}", other),
        };

        let Operator::LogicalProject(pruned) = &pruned_plan.op else {
            panic!("expected Project");
        };

        // The assert_true item MUST survive even though assert_id ∉ {out_x}.
        let arena = arena_rc.borrow();
        let has_assert_true = pruned.items.iter().any(|item| {
            matches!(
                arena.node(item.expr),
                ScalarNode::FunctionCall { name, .. } if name == "assert_true"
            )
        });
        assert!(
            has_assert_true,
            "assert_true item must survive PruneProjectColumns (carve-out missing)"
        );
    }
}
