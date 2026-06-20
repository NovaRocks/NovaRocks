//! IMV `_row_id` internal column descriptor + injection rule.
//!
//! `_row_id` is the Iceberg v3 row-lineage identity. The IMV apply key
//! (`__nova_base_row_id`) is derived from it. Phase 3 injects it (internal,
//! Int64, non-null) on Delta-bound scans so the root apply-key project can
//! reference it. It is never exposed to user-visible output.

use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::catalog::ScanSource;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::plan::{LogicalPlanNode, PlanNodeKind};

pub(crate) struct ImvRowIdColumn;

impl ImvRowIdColumn {
    pub(crate) const NAME: &'static str = crate::exec::row_position::ICEBERG_ROW_ID_COL;

    pub(crate) fn output_column(column_id: ColumnId) -> OutputColumn {
        OutputColumn {
            column_id,
            name: Self::NAME.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: true,
        }
    }

    pub(crate) fn matches(column: &OutputColumn) -> bool {
        column.is_internal && column.name.eq_ignore_ascii_case(Self::NAME)
    }
}

/// Rule to add the `_row_id` internal column to Delta-bound scans (idempotent).
/// Will be registered into the `imv-action-propagation` stage alongside
/// `InjectActionColumnRule` by the pipeline wiring task.
pub(crate) struct InjectRowIdRule;

impl LogicalRewriteRule for InjectRowIdRule {
    fn name(&self) -> &'static str {
        "InjectRowId"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        match &plan.kind {
            PlanNodeKind::Scan(scan) => {
                matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
                    && !scan.columns.iter().any(ImvRowIdColumn::matches)
            }
            _ => false,
        }
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let LogicalPlanNode {
                kind,
                children,
                required_output_columns,
            } = plan;
            let PlanNodeKind::Scan(mut scan) = kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            let ext = ctx
                .extension::<ImvExtension>()
                .ok_or_else(|| "InjectRowId requires ImvExtension in RewriteContext".to_string())?;
            let column_id = ext.allocate_column_id();
            scan.columns
                .retain(|column| !column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME));
            scan.columns.push(ImvRowIdColumn::output_column(column_id));
            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                PlanNodeKind::Scan(scan),
                children,
                required_output_columns,
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::plan::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::convert::logical_plan_to_opt_expr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::planner::plan::{LogicalPlanNode, LogicalScanNode, PlanNodeKind};

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn delta_scan() -> LogicalScanNode {
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
            columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
    }

    fn scan_plan(scan: LogicalScanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(PlanNodeKind::Scan(scan), vec![], None)
    }

    #[test]
    fn row_id_output_column_shape() {
        let col = ImvRowIdColumn::output_column(ColumnId(7));
        assert_eq!(col.name, "_row_id");
        assert_eq!(col.data_type, DataType::Int64);
        assert!(!col.nullable);
        assert!(col.is_internal);
    }

    #[test]
    fn inject_row_id_on_delta_scan() {
        let rule = InjectRowIdRule;
        let mut ctx = build_ctx();
        let plan = scan_plan(delta_scan());
        let mut arena = ScalarArena::new();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Scan)");
        };
        let arena = ctx.scalar_arena();
        let changed =
            crate::sql::optimizer::convert::opt_expr_to_logical_plan(changed_expr, &arena.borrow());
        let PlanNodeKind::Scan(scan) = changed.kind else {
            panic!("expected Changed(Scan)");
        };
        assert!(scan.columns.iter().any(ImvRowIdColumn::matches));
    }

    #[test]
    fn inject_row_id_replaces_preexisting_non_internal_row_id_column() {
        let rule = InjectRowIdRule;
        let mut ctx = build_ctx();
        let mut scan = delta_scan();
        scan.columns.push(OutputColumn {
            column_id: ColumnId(9),
            name: ImvRowIdColumn::NAME.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        });
        let plan = scan_plan(scan);

        let mut arena = ScalarArena::new();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Scan)");
        };
        let arena = ctx.scalar_arena();
        let changed =
            crate::sql::optimizer::convert::opt_expr_to_logical_plan(changed_expr, &arena.borrow());
        let PlanNodeKind::Scan(scan) = changed.kind else {
            panic!("expected Changed(Scan)");
        };
        let row_id_columns = scan
            .columns
            .iter()
            .filter(|column| column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
            .collect::<Vec<_>>();
        assert_eq!(row_id_columns.len(), 1);
        assert_eq!(row_id_columns[0].column_id, ColumnId(100));
        assert!(row_id_columns[0].is_internal);
    }

    #[test]
    fn inject_row_id_is_idempotent() {
        let rule = InjectRowIdRule;
        let ctx = build_ctx();
        let mut scan = delta_scan();
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(9)));
        let plan = scan_plan(scan);
        let mut arena = ScalarArena::new();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena);
        assert!(!rule.matches(&expr, &ctx));
    }
}
