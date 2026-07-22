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

//! IMV apply-key projection rule.
//!
//! Wraps the rewrite plan root in a `Project` that appends the apply-key column
//! `__nova_base_row_id` derived from the internal `_row_id` column. The merge
//! sink reads this column by name to locate target rows for DELETE. Fires once
//! at the root; idempotent.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::mv::persistence::schema::{BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME};
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
use crate::sql::planner::imv_rewrite::join_delta_shape::is_supported_join_delta_union;
use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::sql::planner::payload::PlanProjectNode;

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
fn root_row_id_ref(
    plan: &LogicalPlanNode,
) -> Option<(ColumnId, String, arrow::datatypes::DataType, bool)> {
    if let LogicalPlanKind::Project(p) = &plan.kind {
        if let Some(item) = p
            .items
            .iter()
            .find(|i| i.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
        {
            if let ExprKind::ColumnRef {
                column_id, column, ..
            } = &item.expr.kind
            {
                return Some((
                    *column_id,
                    column.clone(),
                    item.expr.data_type.clone(),
                    item.expr.nullable,
                ));
            }
        }
    }
    if let LogicalPlanKind::Union(u) = &plan.kind {
        if is_branch_delta_union(plan) {
            if let Some(column) = u
                .output_columns
                .iter()
                .find(|column| ImvRowIdColumn::matches(column))
            {
                return Some((
                    column.column_id,
                    column.name.clone(),
                    column.data_type.clone(),
                    column.nullable,
                ));
            }
        }
    }
    descendant_internal_columns(plan)
        .into_iter()
        .find(|c| ImvRowIdColumn::matches(c))
        .map(|c| (c.column_id, c.name, c.data_type, c.nullable))
}

fn is_branch_delta_union(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Union(node) = &plan.kind else {
        return false;
    };
    is_supported_fan_in_delta_union(plan)
        && node
            .output_columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME))
}

fn output_has_apply_key(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Project(p) => p.items.iter().any(|i| {
            i.output_name
                .eq_ignore_ascii_case(HIDDEN_APPLY_KEY_COLUMN_NAME)
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
            LogicalPlanKind::Project(_) | LogicalPlanKind::Filter(_) | LogicalPlanKind::Union(_)
        ) {
            return false;
        }
        if contains_join_delta_union(&plan) {
            return false;
        }
        root_row_id_ref(&plan).is_some() && !output_has_apply_key(&plan)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, Ordering::SeqCst);
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let Some((row_id_col, row_id_name, row_id_type, row_id_nullable)) =
                root_row_id_ref(&plan)
            else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            let apply_key_col =
                crate::sql::planner::imv_rewrite::column_alloc::allocate_imv_column(
                    ctx,
                    HIDDEN_APPLY_KEY_COLUMN_NAME,
                    row_id_type.clone(),
                    row_id_nullable,
                )?;
            let apply_item = ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: row_id_col,
                        qualifier: None,
                        column: row_id_name,
                    },
                    data_type: row_id_type,
                    nullable: row_id_nullable,
                },
                output_name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                output_column_id: apply_key_col,
            };
            let LogicalPlanNode {
                kind,
                children,
                required_output_columns,
            } = plan;
            match kind {
                LogicalPlanKind::Project(mut p) => {
                    p.items.push(apply_item);
                    Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                        LogicalPlanKind::Project(p),
                        children,
                        required_output_columns,
                    )))
                }
                LogicalPlanKind::Union(u) => {
                    let mut items = u
                        .output_columns
                        .iter()
                        .map(project_item_for_output_column)
                        .collect::<Vec<_>>();
                    items.push(apply_item);
                    let union = LogicalPlanNode::new(
                        LogicalPlanKind::Union(u),
                        children,
                        required_output_columns,
                    );
                    Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                        LogicalPlanKind::Project(PlanProjectNode {
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

fn contains_join_delta_union(plan: &LogicalPlanNode) -> bool {
    is_supported_join_delta_union(plan) || plan.children.iter().any(contains_join_delta_union)
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

fn plan_kind_from_kind(kind: &LogicalPlanKind) -> &'static str {
    kind.variant_name()
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::DataType;

    use super::*;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::mv::rewrite::context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
    use crate::sql::planner::optimizer_bridge::logical::{to_logical_plan, to_optimizer_expr};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::logical::{LogicalPlanKind, LogicalPlanNode};
    use crate::sql::planner::payload::{PlanFilterNode, PlanProjectNode, PlanScanNode};

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        let factory = Rc::new(RefCell::new(crate::sql::column_id::ColumnRefFactory::new()));
        factory.borrow_mut().reserve_until(200);
        ctx.set_column_ref_factory(Rc::clone(&factory));
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
        });
        ctx
    }

    fn delta_scan_with_row_id(row_id: ColumnId) -> PlanScanNode {
        PlanScanNode {
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
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
    }

    fn project_root(scan: PlanScanNode, row_id: ColumnId) -> LogicalPlanNode {
        // Project carrying user col k + propagated _row_id (as Task 3 would leave it).
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
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
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Scan(scan),
                vec![],
                None,
            )],
            None,
        )
    }

    #[test]
    fn wraps_root_with_apply_key_project() {
        let rule = InjectApplyKeyProjectRule::new();
        let mut ctx = build_ctx();
        let plan = project_root(delta_scan_with_row_id(ColumnId(101)), ColumnId(101));
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        drop(arena_rc);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Project)");
        };
        let arena_rc = ctx.scalar_arena();
        let changed = to_logical_plan(changed_expr, &arena_rc.borrow());
        let LogicalPlanKind::Project(root) = changed.kind else {
            panic!("expected Changed(Project)");
        };
        assert!(root.items.iter().any(|i| {
            i.output_name
                .eq_ignore_ascii_case(HIDDEN_APPLY_KEY_COLUMN_NAME)
        }));
    }

    #[test]
    fn idempotent_when_apply_key_present() {
        let rule = InjectApplyKeyProjectRule::new();
        let ctx = build_ctx();
        let mut plan = project_root(delta_scan_with_row_id(ColumnId(101)), ColumnId(101));
        if let LogicalPlanKind::Project(p) = &mut plan.kind {
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
                output_name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                output_column_id: ColumnId(200),
            });
        }
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
        assert!(!rule.matches(&expr, &ctx));
    }

    #[test]
    fn rejects_non_project_root_instead_of_dropping_visible_output() {
        let rule = InjectApplyKeyProjectRule::new();
        let mut ctx = build_ctx();
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(true)),
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Scan(delta_scan_with_row_id(ColumnId(101))),
                vec![],
                None,
            )],
            None,
        );
        let arena_rc = ctx.scalar_arena();
        let expr = to_optimizer_expr(&plan, &mut arena_rc.borrow_mut());
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
}
