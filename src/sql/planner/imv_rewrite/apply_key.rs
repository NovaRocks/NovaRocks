//! IMV apply-key projection rule.
//!
//! Wraps the rewrite plan root in a `Project` that appends the apply-key column
//! `__nova_base_row_id` derived from the internal `_row_id` column. The merge
//! sink reads this column by name to locate target rows for DELETE. Fires once
//! at the root; idempotent.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::engine::mv::iceberg_target_apply::{
    ICEBERG_MV_APPLY_KEY_COLUMN, ICEBERG_MV_BRANCH_ID_COLUMN,
};
use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_propagation::{
    descendant_internal_columns, is_supported_fan_in_delta_union,
};
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::plan::{LogicalPlanNode, LogicalProjectNode, PlanNodeKind};

pub(crate) struct InjectApplyKeyProjectRule {
    checked_root: AtomicBool,
    fired: AtomicBool,
}

impl InjectApplyKeyProjectRule {
    pub(crate) fn new() -> Self {
        Self {
            checked_root: AtomicBool::new(false),
            fired: AtomicBool::new(false),
        }
    }
}

/// Find the propagated `_row_id` column id from the plan's effective output,
/// walking Project items first then descendant scans.
fn root_row_id_ref(plan: &LogicalPlanNode) -> Option<(ColumnId, String)> {
    if let PlanNodeKind::Project(p) = &plan.kind {
        if let Some(item) = p
            .items
            .iter()
            .find(|i| i.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
        {
            if let ExprKind::ColumnRef {
                column_id, column, ..
            } = &item.expr.kind
            {
                return Some((*column_id, column.clone()));
            }
        }
    }
    if let PlanNodeKind::Union(u) = &plan.kind {
        if is_branch_delta_union(plan) {
            if let Some(column) = u
                .output_columns
                .iter()
                .find(|column| ImvRowIdColumn::matches(column))
            {
                return Some((column.column_id, column.name.clone()));
            }
        }
    }
    descendant_internal_columns(plan)
        .into_iter()
        .find(|c| ImvRowIdColumn::matches(c))
        .map(|c| (c.column_id, c.name))
}

fn is_branch_delta_union(plan: &LogicalPlanNode) -> bool {
    let PlanNodeKind::Union(node) = &plan.kind else {
        return false;
    };
    is_supported_fan_in_delta_union(plan)
        && node.output_columns.iter().any(|column| {
            column
                .name
                .eq_ignore_ascii_case(ICEBERG_MV_BRANCH_ID_COLUMN)
        })
}

fn output_has_apply_key(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Project(p) => p.items.iter().any(|i| {
            i.output_name
                .eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)
        }),
        _ => false,
    }
}

impl LogicalRewriteRule for InjectApplyKeyProjectRule {
    fn name(&self) -> &'static str {
        "InjectApplyKeyProject"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        if self.checked_root.swap(true, Ordering::SeqCst) {
            return false;
        }
        if self.fired.load(Ordering::SeqCst) {
            return false;
        }
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        if !matches!(
            &plan.kind,
            PlanNodeKind::Project(_) | PlanNodeKind::Filter(_) | PlanNodeKind::Union(_)
        ) {
            return false;
        }
        root_row_id_ref(&plan).is_some() && !output_has_apply_key(&plan)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, Ordering::SeqCst);
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let Some((row_id_col, row_id_name)) = root_row_id_ref(&plan) else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            let ext = ctx.extension::<ImvExtension>().ok_or_else(|| {
                "InjectApplyKeyProject requires ImvExtension in RewriteContext".to_string()
            })?;
            let apply_key_col = ext.allocate_column_id();
            let apply_item = ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: row_id_col,
                        qualifier: None,
                        column: row_id_name,
                    },
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                },
                output_name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
                output_column_id: apply_key_col,
            };
            let LogicalPlanNode {
                kind,
                children,
                required_output_columns,
            } = plan;
            match kind {
                PlanNodeKind::Project(mut p) => {
                    p.items.push(apply_item);
                    Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                        PlanNodeKind::Project(p),
                        children,
                        required_output_columns,
                    )))
                }
                PlanNodeKind::Union(u) => {
                    let mut items = u
                        .output_columns
                        .iter()
                        .map(project_item_for_output_column)
                        .collect::<Vec<_>>();
                    items.push(apply_item);
                    let union = LogicalPlanNode::new(
                        PlanNodeKind::Union(u),
                        children,
                        required_output_columns,
                    );
                    Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                        PlanNodeKind::Project(LogicalProjectNode {
                            items,
                            output_qualifier: None,
                        }),
                        vec![union],
                        None,
                    )))
                }
                other_kind => Err(format!(
                    "InjectApplyKeyProject expected root Project or Union for PF MV rewrite, got {}",
                    plan_kind_from_kind(&other_kind)
                )),
            }
        })
    }
}

fn project_item_for_output_column(column: &OutputColumn) -> ProjectItem {
    ProjectItem {
        expr: TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: column.column_id,
                qualifier: None,
                column: column.name.clone(),
            },
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        },
        output_name: column.name.clone(),
        output_column_id: column.column_id,
    }
}

fn plan_kind(plan: &LogicalPlanNode) -> &'static str {
    plan_kind_from_kind(&plan.kind)
}

fn plan_kind_from_kind(kind: &PlanNodeKind) -> &'static str {
    kind.variant_name()
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::plan::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::convert::{logical_plan_to_opt_expr, opt_expr_to_logical_plan};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::plan::{
        LogicalFilterNode, LogicalPlanNode, LogicalProjectNode, LogicalScanNode, PlanNodeKind,
    };

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(200)),
        });
        ctx
    }

    fn delta_scan_with_row_id(row_id: ColumnId) -> LogicalScanNode {
        LogicalScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDeltaTable {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "b".to_string(),
                        table_uuid: Some("uuid-b".to_string()),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: "file:///tmp/ice/db/b".to_string(),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                        serialized_metadata_rows: None,
                    },
                    from_snapshot_id: 11,
                    to_snapshot_id: 22,
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: ColumnId(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                ImvRowIdColumn::output_column(row_id),
            ],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
    }

    fn project_root(scan: LogicalScanNode, row_id: ColumnId) -> LogicalPlanNode {
        // Project carrying user col k + propagated _row_id (as Task 3 would leave it).
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: ColumnId(1),
                                qualifier: None,
                                column: "k".to_string(),
                            },
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: "k".to_string(),
                        output_column_id: ColumnId(1),
                    },
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: row_id,
                                qualifier: None,
                                column: "_row_id".to_string(),
                            },
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: "_row_id".to_string(),
                        output_column_id: row_id,
                    },
                ],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(PlanNodeKind::Scan(scan), vec![], None)],
            None,
        )
    }

    #[test]
    fn wraps_root_with_apply_key_project() {
        let rule = InjectApplyKeyProjectRule::new();
        let mut ctx = build_ctx();
        let plan = project_root(delta_scan_with_row_id(ColumnId(101)), ColumnId(101));
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        drop(arena_rc);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Project)");
        };
        let arena_rc = ctx.scalar_arena();
        let changed = opt_expr_to_logical_plan(changed_expr, &arena_rc.borrow());
        let PlanNodeKind::Project(root) = changed.kind else {
            panic!("expected Changed(Project)");
        };
        assert!(root.items.iter().any(|i| {
            i.output_name
                .eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)
        }));
    }

    #[test]
    fn idempotent_when_apply_key_present() {
        let rule = InjectApplyKeyProjectRule::new();
        let ctx = build_ctx();
        let mut plan = project_root(delta_scan_with_row_id(ColumnId(101)), ColumnId(101));
        if let PlanNodeKind::Project(p) = &mut plan.kind {
            p.items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(101),
                        qualifier: None,
                        column: "_row_id".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
                output_column_id: ColumnId(200),
            });
        }
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn rejects_non_project_root_instead_of_dropping_visible_output() {
        let rule = InjectApplyKeyProjectRule::new();
        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Filter(LogicalFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(true)),
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![LogicalPlanNode::new(
                PlanNodeKind::Scan(delta_scan_with_row_id(ColumnId(101))),
                vec![],
                None,
            )],
            None,
        );
        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        drop(arena_rc);
        assert!(rule.matches(&expr, &ctx));
        let err = rule
            .apply(expr, &mut ctx)
            .expect_err("non-Project root must fail fast");
        assert!(
            err.contains("expected root Project"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn does_not_inject_apply_key_below_non_apply_key_root() {
        let rule = InjectApplyKeyProjectRule::new();
        let ctx = build_ctx();
        let k = OutputColumn {
            column_id: ColumnId(1),
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let old_input = LogicalPlanNode::new(
            PlanNodeKind::Values(LogicalValuesNode {
                rows: Vec::new(),
                columns: vec![k.clone()],
            }),
            vec![],
            None,
        );
        let delta_input = project_root(delta_scan_with_row_id(ColumnId(101)), ColumnId(101));
        let plan = LogicalPlanNode::new(
            PlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
                group_key_names: vec!["k".to_string()],
                aggregate_state_names: Vec::new(),
                change_op_column: ImvActionColumn::NAME.to_string(),
                output_columns: vec![k],
            }),
            vec![old_input, delta_input],
            None,
        );

        let arena_rc = ctx.scalar_arena();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena_rc.borrow_mut());
        drop(arena_rc);

        assert!(
            !rule.matches(&expr, &ctx),
            "AggregateStateMerge root should not get an apply-key wrapper"
        );
        assert!(
            !rule.matches(expr.child(1), &ctx),
            "root-only rule must not fire on descendant Projects"
        );
    }
}
