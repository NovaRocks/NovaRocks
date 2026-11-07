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

//! IMV `_row_id` internal column descriptor + injection rule.
//!
//! `_row_id` is the Iceberg v3 row-lineage identity. The IMV apply key
//! (`__nova_base_row_id`) is derived from it. Phase 3 injects it (internal,
//! Int64, with its source NULL contract) on Delta-bound scans and version
//! snapshot scans so refresh rewrite rules can build row-identity apply keys
//! from real plan outputs. It is never exposed to user-visible output.

use crate::compiler::SqlCompileError;
use arrow::datatypes::DataType;

use crate::analysis::OutputColumn;
use crate::column_id::ColumnId;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::planner::table::{ScanSource, SqlScanKind};
use novarocks_types::schema::ColumnDef;

pub(crate) struct ImvRowIdColumn;

impl ImvRowIdColumn {
    pub(crate) const NAME: &'static str = crate::common::ICEBERG_ROW_ID_COL;

    pub(crate) fn output_column(column_id: ColumnId) -> OutputColumn {
        OutputColumn {
            column_id,
            name: Self::NAME.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),

            is_internal: true,
        }
    }

    pub(crate) fn matches(column: &OutputColumn) -> bool {
        column.is_internal && column.name.eq_ignore_ascii_case(Self::NAME)
    }

    pub(crate) fn ensure_metadata_column(columns: &mut Vec<ColumnDef>) {
        if columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(Self::NAME))
        {
            return;
        }
        columns.push(ColumnDef {
            name: Self::NAME.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        });
    }
}

/// Rule to add the `_row_id` internal column to row-lineage Iceberg scans
/// (idempotent).
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
            LogicalPlanKind::Scan(scan) => {
                matches!(
                    &scan.table.source,
                    ScanSource::Sql(source)
                        if matches!(
                            source.kind,
                            SqlScanKind::Delta { .. } | SqlScanKind::FrozenInputSet { .. }
                        )
                ) && !scan.columns.iter().any(ImvRowIdColumn::matches)
            }
            _ => false,
        }
    }

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let LogicalPlanNode {
                kind,
                children,
                required_output_columns,
            } = plan;
            let LogicalPlanKind::Scan(mut scan) = kind else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            ImvRowIdColumn::ensure_metadata_column(
                &mut scan.table.iceberg_row_lineage_metadata_columns,
            );
            let metadata = scan
                .table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
                .expect("row-ID metadata was retained or inserted");
            let value_type =
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, metadata.nullable);
            let column_id = match scan
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
            {
                Some(existing) => existing.column_id,
                None => crate::planner::imv_rewrite::column_alloc::allocate_imv_column(
                    ctx,
                    ImvRowIdColumn::NAME,
                    value_type.clone(),
                )?,
            };
            scan.columns
                .retain(|column| !column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME));
            scan.columns.push(OutputColumn {
                value_type,
                ..ImvRowIdColumn::output_column(column_id)
            });
            Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                LogicalPlanKind::Scan(scan),
                children,
                required_output_columns,
            )))
        })
    }
}

#[cfg(test)]
mod tests {

    use arrow::datatypes::DataType;

    use super::*;
    use crate::analysis::OutputColumn;
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use novarocks_types::schema::ColumnDef;
    use std::cell::RefCell;
    use std::num::{NonZeroU32, NonZeroU64};
    use std::rc::Rc;

    use crate::column_id::ColumnId;
    use crate::optimizer::rewrite::context::RewriteContext;
    use crate::optimizer::rewrite::result::RewriteResult;
    use crate::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::optimizer::scalar::ScalarArena;
    use crate::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
    use crate::planner::optimizer_bridge::logical::to_optimizer_expr;
    use crate::planner::payload::PlanScanNode;

    fn build_ctx() -> RewriteContext<'static> {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        let factory = Rc::new(RefCell::new(crate::column_id::ColumnRefFactory::new()));
        factory.borrow_mut().reserve_until(100);
        ctx.set_column_ref_factory(Rc::clone(&factory));
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            snapshot: crate::compiler::mv_rewrite::test_incremental_snapshot(),
            annotation: ImvPlanAnnotation::default(),
        });
        ctx
    }

    fn delta_scan() -> PlanScanNode {
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
                source: sql_scan_source(SqlScanKind::Delta {
                    from_snapshot_id: 11,
                    to_snapshot_id: 22,
                }),
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

                is_internal: false,
            }],
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
    }

    fn version_scan() -> PlanScanNode {
        let mut scan = delta_scan();
        scan.table.source = sql_scan_source(SqlScanKind::FrozenInputSet {
            version: SqlTableVersionSelector::Snapshot(22),
        });
        scan
    }

    fn sql_scan_source(kind: SqlScanKind) -> ScanSource {
        ScanSource::Sql(SqlScanSource::new(
            SqlTableBindingId::new(
                SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
                NonZeroU32::new(1).expect("ordinal"),
            ),
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "b".to_string(),
            },
            kind,
        ))
    }

    fn scan_plan(scan: PlanScanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(LogicalPlanKind::scan(scan), vec![], None)
    }

    #[test]
    fn row_id_output_column_shape() {
        let col = ImvRowIdColumn::output_column(ColumnId(7));
        assert_eq!(col.name, "_row_id");
        assert_eq!(col.value_type.data_type().clone(), DataType::Int64);
        assert!(col.value_type.nullable());
        assert!(col.is_internal);
    }

    #[test]
    fn inject_row_id_on_delta_scan() {
        let rule = InjectRowIdRule;
        let mut ctx = build_ctx();
        let plan = scan_plan(delta_scan());
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Scan)");
        };
        let arena = ctx.scalar_arena();
        let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Scan(scan) = changed.kind else {
            panic!("expected Changed(Scan)");
        };
        assert!(scan.columns.iter().any(ImvRowIdColumn::matches));
        assert!(
            scan.table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
        );
    }

    #[test]
    fn inject_row_id_on_version_scan() {
        let rule = InjectRowIdRule;
        let mut ctx = build_ctx();
        let plan = scan_plan(version_scan());
        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Scan)");
        };
        let arena = ctx.scalar_arena();
        let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Scan(scan) = changed.kind else {
            panic!("expected Changed(Scan)");
        };
        assert!(scan.columns.iter().any(ImvRowIdColumn::matches));
        assert!(
            scan.table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
        );
    }

    #[test]
    fn inject_row_id_records_factory_metadata() {
        let rule = InjectRowIdRule;
        let mut ctx = build_ctx();
        let factory = ctx
            .column_ref_factory()
            .cloned()
            .expect("build_ctx must install ColumnRefFactory");
        let plan = scan_plan(delta_scan());
        let expr = to_optimizer_expr(&plan, &mut ctx.scalar_arena().borrow_mut());
        let allocated_id = ColumnId(factory.borrow().peek_next_id());

        let RewriteResult::Changed(changed) = rule.apply(expr, &mut ctx).expect("apply") else {
            panic!("expected Changed(Scan)");
        };
        let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
            changed,
            &ctx.scalar_arena().borrow(),
        );
        let LogicalPlanKind::Scan(scan) = changed.kind else {
            panic!("expected injected scan");
        };
        let row_id = scan
            .columns
            .iter()
            .find(|column| ImvRowIdColumn::matches(column))
            .unwrap();
        assert_eq!(row_id.column_id, allocated_id);
        let factory = factory.borrow();
        let metadata = factory.get(row_id.column_id);
        assert_eq!(metadata.name, row_id.name);
        assert_eq!(metadata.value_type, row_id.value_type);
        assert!(metadata.value_type.nullable());
        assert_eq!(factory.peek_next_id(), allocated_id.0 + 1);
        // Reservations carry identity only; they are not authored columns.
        assert!(factory.value_type(ColumnId(1)).is_none());
    }

    #[test]
    fn inject_row_id_normalizes_preexisting_non_internal_row_id_column_id_in_place() {
        let rule = InjectRowIdRule;
        let mut ctx = build_ctx();
        let mut scan = delta_scan();
        scan.columns.push(OutputColumn {
            column_id: ColumnId(9),
            name: ImvRowIdColumn::NAME.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        });
        let plan = scan_plan(scan);

        let mut arena = ScalarArena::new();
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(rule.matches(&expr, &ctx));
        let RewriteResult::Changed(changed_expr) = rule.apply(expr, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Scan)");
        };
        let arena = ctx.scalar_arena();
        let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr,
            &arena.borrow(),
        );
        let LogicalPlanKind::Scan(scan) = changed.kind else {
            panic!("expected Changed(Scan)");
        };
        let row_id_columns = scan
            .columns
            .iter()
            .filter(|column| column.name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
            .collect::<Vec<_>>();
        assert_eq!(row_id_columns.len(), 1);
        assert_eq!(row_id_columns[0].column_id, ColumnId(9));
        assert!(row_id_columns[0].is_internal);
    }

    #[test]
    fn inject_row_id_preserves_authored_source_nullability_in_factory_and_scan() {
        for nullable in [false, true] {
            let rule = InjectRowIdRule;
            let mut ctx = build_ctx();
            let mut scan = delta_scan();
            scan.table
                .iceberg_row_lineage_metadata_columns
                .push(ColumnDef {
                    name: "_row_id".to_string(),
                    data_type: DataType::Int64,
                    nullable,
                    write_default: None,
                    logical_type: None,
                });
            let plan = scan_plan(scan);
            let mut arena = ScalarArena::new();
            let expr = to_optimizer_expr(&plan, &mut arena);
            let RewriteResult::Changed(changed) = rule.apply(expr, &mut ctx).unwrap() else {
                panic!("expected injected scan");
            };
            let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
                changed,
                &ctx.scalar_arena().borrow(),
            );
            let LogicalPlanKind::Scan(scan) = changed.kind else {
                panic!("expected injected scan");
            };
            let row_id = scan
                .columns
                .iter()
                .find(|column| ImvRowIdColumn::matches(column))
                .unwrap();
            assert_eq!(row_id.value_type.nullable(), nullable);
            let factory = ctx.column_ref_factory().unwrap().borrow();
            assert_eq!(factory.get(row_id.column_id).value_type, row_id.value_type);
            assert_eq!(scan.table.iceberg_row_lineage_metadata_columns.len(), 1);
            assert_eq!(
                scan.table.iceberg_row_lineage_metadata_columns[0].nullable,
                nullable
            );
        }
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
        let expr = to_optimizer_expr(&plan, &mut arena);
        assert!(!rule.matches(&expr, &ctx));
    }
}
