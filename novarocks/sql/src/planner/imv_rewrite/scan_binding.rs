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

//! Iceberg IMV scan binding.
//!
//! This module consumes refresh-only IMV scan markers by resolving snapshot
//! windows from the SQL-owned IMV rewrite snapshot. It must never fall back to the
//! current Iceberg snapshot: the refresh pin is the read upper bound.

pub(crate) use crate::common::ImvVersionRole;
use crate::compiler::SqlCompileError;
use crate::compiler::mv_rewrite::SqlImvRewriteSnapshot;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::planner::imv_rewrite::annotation::ImvExtension;
use crate::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::planner::payload::PlanScanNode;
use crate::planner::table::{ScanSource, SqlScanKind, SqlScanSource, SqlTableVersionSelector};
use novarocks_spi::connector::ConnectorTableObjectId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvSnapshotWindow {
    pub(crate) base_fqn: String,
    pub(crate) from_snapshot_id: i64,
    pub(crate) to_snapshot_id: i64,
    pub(crate) table_object_id: ConnectorTableObjectId,
    context_from_snapshot_id: i64,
}

pub(crate) struct BindIcebergScanRule;

impl LogicalRewriteRule for BindIcebergScanRule {
    fn name(&self) -> &'static str {
        "BindIcebergScan"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        matches!(
            &plan.kind,
            LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_)
        ) && matches!(&plan.unary_input().kind, LogicalPlanKind::Scan(_))
    }

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let ext = ctx.extension::<ImvExtension>().ok_or_else(|| {
                "BindIcebergScan requires ImvExtension in RewriteContext".to_string()
            })?;
            let LogicalPlanNode {
                kind,
                mut children,
                required_output_columns: _,
            } = plan;
            if children.len() != 1 {
                return Ok(PlanRewriteResult::Unchanged);
            }
            let scan_plan = children.remove(0);
            let LogicalPlanNode {
                kind: scan_kind,
                required_output_columns,
                ..
            } = scan_plan;
            match &kind {
                LogicalPlanKind::ImvDelta(node) => {
                    let LogicalPlanKind::Scan(scan) = scan_kind else {
                        return Ok(PlanRewriteResult::Unchanged);
                    };
                    let mut bound = bind_delta_scan(*scan, &ext.snapshot)?;
                    if let Some(column_id) = node.action_column {
                        bound
                            .columns
                            .retain(|column| !is_action_column_name(&column.name));
                        bound
                            .columns
                            .push(ImvActionColumn::output_column(column_id));
                        ImvActionColumn::ensure_metadata_column(
                            &mut bound.table.iceberg_row_lineage_metadata_columns,
                        );
                    }
                    Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                        LogicalPlanKind::scan(bound),
                        vec![],
                        required_output_columns,
                    )))
                }
                LogicalPlanKind::ImvVersion(node) => {
                    let LogicalPlanKind::Scan(scan) = scan_kind else {
                        return Ok(PlanRewriteResult::Unchanged);
                    };
                    let bound = bind_version_scan(*scan, &ext.snapshot, node.version_ref.role)?;
                    Ok(PlanRewriteResult::Changed(LogicalPlanNode::new(
                        LogicalPlanKind::scan(bound),
                        vec![],
                        required_output_columns,
                    )))
                }
                _ => Ok(PlanRewriteResult::Unchanged),
            }
        })
    }
}

fn bind_delta_scan(
    mut scan: PlanScanNode,
    snapshot: &SqlImvRewriteSnapshot,
) -> Result<PlanScanNode, String> {
    let mut source = sql_base_scan_source(&scan.table.source)?;
    let window = resolve_snapshot_window(snapshot, &source)?;
    source.kind = SqlScanKind::Delta {
        from_snapshot_id: window.from_snapshot_id,
        to_snapshot_id: window.to_snapshot_id,
    };
    scan.table.source = ScanSource::Sql(source);
    Ok(scan)
}

fn bind_version_scan(
    mut scan: PlanScanNode,
    snapshot: &SqlImvRewriteSnapshot,
    role: ImvVersionRole,
) -> Result<PlanScanNode, String> {
    let mut source = sql_base_scan_source(&scan.table.source)?;
    let window = resolve_snapshot_window(snapshot, &source)?;
    let snapshot_id = match role {
        ImvVersionRole::From => window.context_from_snapshot_id,
        ImvVersionRole::To => window.to_snapshot_id,
    };
    source.kind = SqlScanKind::FrozenInputSet {
        version: SqlTableVersionSelector::Snapshot(snapshot_id),
    };
    scan.table.source = ScanSource::Sql(source);
    scan.columns
        .retain(|column| !is_action_column_name(&column.name));
    scan.table
        .iceberg_row_lineage_metadata_columns
        .retain(|column| !is_action_column_name(&column.name));
    Ok(scan)
}

fn is_action_column_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(ImvActionColumn::NAME)
}

fn sql_base_scan_source(source: &ScanSource) -> Result<SqlScanSource, String> {
    match source {
        ScanSource::Sql(source)
            if matches!(
                source.kind,
                SqlScanKind::Data { .. } | SqlScanKind::FrozenInputSet { .. }
            ) =>
        {
            Ok(source.clone())
        }
        ScanSource::Sql(_) => {
            Err("BindIcebergScan requires a data or frozen-input SQL scan source".to_string())
        }
    }
}

fn resolve_snapshot_window(
    snapshot: &SqlImvRewriteSnapshot,
    source: &SqlScanSource,
) -> Result<ImvSnapshotWindow, String> {
    let base = find_base_ref(snapshot, source)?;
    let base_fqn = base.table.fqn();
    let from_snapshot_id = snapshot
        .previous_snapshot_ids
        .get(&base.occurrence_id)
        .copied()
        .ok_or_else(|| {
            format!(
                "IMV scan binding requires previous snapshot for base {base_fqn}; first refresh/full rebuild must not enter incremental scan binding"
            )
        })?;
    let to_snapshot_id = base.snapshot_id;
    let pin_object_id = &base.table_object_id;
    let context_from_snapshot_id = if base.equivalent_context_from == Some(from_snapshot_id) {
        to_snapshot_id
    } else {
        from_snapshot_id
    };
    Ok(ImvSnapshotWindow {
        base_fqn,
        from_snapshot_id,
        to_snapshot_id,
        table_object_id: pin_object_id.clone(),
        context_from_snapshot_id,
    })
}

fn find_base_ref<'a>(
    snapshot: &'a SqlImvRewriteSnapshot,
    source: &SqlScanSource,
) -> Result<&'a crate::compiler::mv_rewrite::SqlImvBaseSnapshot, String> {
    let occurrence = source
        .mv_occurrence
        .ok_or_else(|| "IMV scan has no definition occurrence binding".to_string())?;
    snapshot
        .base_snapshot_for_occurrence(occurrence)
        .filter(|base| {
            base.table.catalog == source.table.catalog
                && base.table.namespace == source.table.namespace
                && base.table.table == source.table.table
        })
        .ok_or_else(|| {
            format!(
                "IMV scan binding base {}.{}.{} is not part of MV refresh context",
                source.table.catalog, source.table.namespace, source.table.table
            )
        })
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::analysis::OutputColumn;
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::column_id::ColumnId;
    use crate::optimizer::rewrite::context::RewriteContext;
    use crate::planner::imv_rewrite::action_column::ImvActionColumn;
    use crate::planner::logical::*;
    use crate::planner::payload::*;
    use crate::planner::table::{SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef};
    use novarocks_types::schema::ColumnDef;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::optimizer::rewrite::result::RewriteResult;
    use crate::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::optimizer::scalar::ScalarArena;
    use crate::planner::imv_rewrite::action_propagation::InjectActionColumnRule;
    use crate::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::planner::optimizer_bridge::logical::to_optimizer_expr;

    fn sql_source(table: &str) -> ScanSource {
        ScanSource::Sql(
            SqlScanSource::new(
                SqlTableBindingId::new(
                    SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
                    NonZeroU32::new(1).expect("ordinal"),
                ),
                SqlTableIdentity {
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: table.to_string(),
                },
                SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                },
            )
            .with_mv_occurrence(crate::compiler::SqlMvRelationOccurrenceId::new(7)),
        )
    }

    fn base_identity(table: &str) -> SqlScanSource {
        let ScanSource::Sql(source) = sql_source(table);
        source
    }

    fn iceberg_scan() -> PlanScanNode {
        let column = ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![column],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: sql_source("b"),
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

    #[test]
    fn resolve_window_uses_previous_snapshot_and_refresh_pin() {
        let snapshot = crate::compiler::mv_rewrite::test_incremental_snapshot();
        let window =
            resolve_snapshot_window(&snapshot, &base_identity("b")).expect("window should resolve");
        assert_eq!(window.base_fqn, "ice.db.b");
        assert_eq!(window.from_snapshot_id, 11);
        assert_eq!(window.to_snapshot_id, 22);
        assert_eq!(window.table_object_id.as_bytes().as_ref(), b"object-b");
    }

    #[test]
    fn resolve_window_rejects_unbound_base_identity() {
        let snapshot = crate::compiler::mv_rewrite::test_incremental_snapshot();
        let err = resolve_snapshot_window(&snapshot, &base_identity("other"))
            .expect_err("unbound base must fail");
        assert!(
            err.contains("not part of MV refresh context"),
            "unexpected error: {err}"
        );
        assert!(err.contains("ice.db.other"), "unexpected error: {err}");
    }

    #[test]
    fn bind_delta_scan_replaces_source_with_sql_delta_fact() {
        let snapshot = crate::compiler::mv_rewrite::test_incremental_snapshot();
        let bound = bind_delta_scan(iceberg_scan(), &snapshot).expect("delta scan should bind");
        match bound.table.source {
            ScanSource::Sql(SqlScanSource {
                kind:
                    SqlScanKind::Delta {
                        from_snapshot_id,
                        to_snapshot_id,
                    },
                ..
            }) => {
                assert_eq!(from_snapshot_id, 11);
                assert_eq!(to_snapshot_id, 22);
            }
            other => panic!("expected SQL delta source, got {other:?}"),
        }
    }

    #[test]
    fn bind_delta_marker_preserves_existing_action_column_id_for_injection() {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let arena = Rc::new(RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(Rc::clone(&arena));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            snapshot: crate::compiler::mv_rewrite::test_incremental_snapshot(),
            annotation: ImvPlanAnnotation::default(),
        });
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId::new_for_test(77)),
                branch_scope: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::scan(iceberg_scan()),
                vec![],
                None,
            )],
            None,
        );
        let bind = BindIcebergScanRule;
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        let RewriteResult::Changed(changed_expr) =
            bind.apply(expr, &mut ctx).expect("bind must succeed")
        else {
            panic!("expected changed scan");
        };
        let arena_ref = ctx.scalar_arena();
        let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr.clone(),
            &arena_ref.borrow(),
        );
        let LogicalPlanKind::Scan(bound) = &changed.kind else {
            panic!("expected changed scan");
        };
        let action = bound
            .columns
            .iter()
            .find(|column| ImvActionColumn::matches(column))
            .expect("bound delta scan must carry marker action column");
        assert_eq!(action.column_id, ColumnId::new_for_test(77));
        assert!(
            bound
                .table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(
                    |column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)
                        && column.data_type == DataType::Int32
                        && !column.nullable
                ),
            "bound delta scan table metadata must expose the action pseudo-column for codegen"
        );

        let inject = InjectActionColumnRule;
        assert!(
            !inject.matches(&changed_expr, &ctx),
            "inject action must skip a scan that already carries the marker action id"
        );
    }

    #[test]
    fn bind_delta_marker_rebinds_preexisting_action_column_to_marker_id() {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let arena = Rc::new(RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(Rc::clone(&arena));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            snapshot: crate::compiler::mv_rewrite::test_incremental_snapshot(),
            annotation: ImvPlanAnnotation::default(),
        });
        let mut scan = iceberg_scan();
        scan.columns.push(OutputColumn {
            column_id: ColumnId::new_for_test(9),
            name: ImvActionColumn::NAME.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),

            is_internal: false,
        });
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: false,
                action_column: Some(ColumnId::new_for_test(77)),
                branch_scope: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::scan(scan),
                vec![],
                None,
            )],
            None,
        );

        let bind = BindIcebergScanRule;
        let expr = to_optimizer_expr(&plan, &mut arena.borrow_mut());
        let RewriteResult::Changed(changed_expr) =
            bind.apply(expr, &mut ctx).expect("bind must succeed")
        else {
            panic!("expected changed scan");
        };
        let arena_ref = ctx.scalar_arena();
        let changed = crate::planner::optimizer_bridge::logical::to_logical_plan(
            changed_expr,
            &arena_ref.borrow(),
        );
        let LogicalPlanKind::Scan(bound) = &changed.kind else {
            panic!("expected changed scan");
        };
        let actions = bound
            .columns
            .iter()
            .filter(|column| ImvActionColumn::matches(column))
            .collect::<Vec<_>>();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].column_id, ColumnId::new_for_test(77));
        let metadata_actions = bound
            .table
            .iceberg_row_lineage_metadata_columns
            .iter()
            .filter(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
            .collect::<Vec<_>>();
        assert_eq!(metadata_actions.len(), 1);
        assert_eq!(metadata_actions[0].data_type, DataType::Int32);
        assert!(!metadata_actions[0].nullable);
    }

    #[test]
    fn bind_version_scan_uses_from_snapshot() {
        let snapshot = crate::compiler::mv_rewrite::test_incremental_snapshot();
        let bound = bind_version_scan(iceberg_scan(), &snapshot, ImvVersionRole::From)
            .expect("version scan should bind");
        match bound.table.source {
            ScanSource::Sql(SqlScanSource {
                kind:
                    SqlScanKind::FrozenInputSet {
                        version: SqlTableVersionSelector::Snapshot(snapshot_id),
                    },
                ..
            }) => {
                assert_eq!(snapshot_id, 11);
            }
            other => panic!("expected frozen SQL source, got {other:?}"),
        }
    }

    #[test]
    fn net_zero_join_context_uses_to_and_preserves_the_delta_window() {
        let mut snapshot = crate::compiler::mv_rewrite::test_join_snapshot(false);
        let bases = &mut std::sync::Arc::make_mut(&mut snapshot).base_snapshots;
        let bases = std::sync::Arc::make_mut(bases);
        bases[1].equivalent_context_from = Some(33);
        let scan_for = |base: &crate::compiler::mv_rewrite::SqlImvBaseSnapshot| {
            let mut scan = iceberg_scan();
            scan.table.source = ScanSource::Sql(
                base_identity(&base.table.table).with_mv_occurrence(base.occurrence_id),
            );
            scan
        };
        let changed = scan_for(&bases[0]);
        let net_zero = scan_for(&bases[1]);

        let context = bind_version_scan(net_zero.clone(), &snapshot, ImvVersionRole::From)
            .expect("version scan should bind to the equivalent To");
        let changed_context = bind_version_scan(changed, &snapshot, ImvVersionRole::From)
            .expect("a different occurrence should bind to its original From");
        let delta = bind_delta_scan(net_zero, &snapshot)
            .expect("NetZero delta scan should bind to its original window");

        assert!(matches!(
            context.table.source,
            ScanSource::Sql(SqlScanSource {
                kind: SqlScanKind::FrozenInputSet {
                    version: SqlTableVersionSelector::Snapshot(44),
                },
                ..
            })
        ));
        assert!(matches!(
            changed_context.table.source,
            ScanSource::Sql(SqlScanSource {
                kind: SqlScanKind::FrozenInputSet {
                    version: SqlTableVersionSelector::Snapshot(11),
                },
                ..
            })
        ));
        assert!(matches!(
            delta.table.source,
            ScanSource::Sql(SqlScanSource {
                kind: SqlScanKind::Delta {
                    from_snapshot_id: 33,
                    to_snapshot_id: 44,
                },
                ..
            })
        ));
    }

    #[test]
    fn bind_version_scan_uses_to_snapshot() {
        let snapshot = crate::compiler::mv_rewrite::test_incremental_snapshot();
        let bound = bind_version_scan(iceberg_scan(), &snapshot, ImvVersionRole::To)
            .expect("version scan should bind");
        match bound.table.source {
            ScanSource::Sql(SqlScanSource {
                kind:
                    SqlScanKind::FrozenInputSet {
                        version: SqlTableVersionSelector::Snapshot(snapshot_id),
                    },
                ..
            }) => {
                assert_eq!(snapshot_id, 22);
            }
            other => panic!("expected frozen SQL source, got {other:?}"),
        }
    }

    #[test]
    fn bind_version_scan_strips_refresh_action_column() {
        let snapshot = crate::compiler::mv_rewrite::test_incremental_snapshot();
        let mut scan = iceberg_scan();
        scan.columns
            .push(ImvActionColumn::output_column(ColumnId::new_for_test(99)));
        scan.table
            .iceberg_row_lineage_metadata_columns
            .push(ColumnDef {
                name: ImvActionColumn::NAME.to_string(),
                data_type: DataType::Int8,
                nullable: false,
                write_default: None,
                logical_type: None,
            });

        let bound = bind_version_scan(scan, &snapshot, ImvVersionRole::To)
            .expect("version scan should bind");

        assert!(
            !bound.columns.iter().any(ImvActionColumn::matches),
            "version scan must not project refresh action column"
        );
        assert!(
            !bound
                .table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)),
            "version scan table metadata must not advertise refresh action column"
        );
    }
}
