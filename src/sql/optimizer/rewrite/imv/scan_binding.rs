//! Iceberg IMV scan binding.
//!
//! This module consumes refresh-only IMV scan markers by resolving snapshot
//! windows from `IcebergMvRewriteContext`. It must never fall back to the
//! current Iceberg snapshot: the refresh pin is the read upper bound.

use crate::connector::starrocks::table::model::IcebergTableRef;
use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
use crate::sql::catalog::{IcebergTableInfo, ScanSource};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::marker::{ImvDeltaNode, ImvVersionNode};
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ScanNode};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ImvVersionRole {
    From,
    To,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvSnapshotWindow {
    pub(crate) base_fqn: String,
    pub(crate) from_snapshot_id: i64,
    pub(crate) to_snapshot_id: i64,
    pub(crate) table_uuid: String,
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

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(ImvDeltaNode { input, .. })
                if matches!(input.as_ref(), LogicalPlan::Scan(_))
        ) || matches!(
            plan,
            LogicalPlan::ImvVersion(ImvVersionNode { input, .. })
                if matches!(input.as_ref(), LogicalPlan::Scan(_))
        )
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or_else(|| "BindIcebergScan requires ImvExtension in RewriteContext".to_string())?;
        match plan {
            LogicalPlan::ImvDelta(node) => {
                let LogicalPlan::Scan(scan) = *node.input else {
                    return Ok(RewriteResult::Unchanged);
                };
                let mut bound = bind_delta_scan(scan, &ext.mv_ctx)?;
                if let Some(column_id) = node.action_column {
                    bound
                        .columns
                        .retain(|column| !is_action_column_name(&column.name));
                    bound
                        .columns
                        .push(ImvActionColumn::output_column(column_id));
                }
                Ok(RewriteResult::Changed(LogicalPlan::Scan(bound)))
            }
            LogicalPlan::ImvVersion(node) => {
                let LogicalPlan::Scan(scan) = *node.input else {
                    return Ok(RewriteResult::Unchanged);
                };
                let bound = bind_version_scan(scan, &ext.mv_ctx, node.version_ref.role)?;
                Ok(RewriteResult::Changed(LogicalPlan::Scan(bound)))
            }
            _ => Ok(RewriteResult::Unchanged),
        }
    }
}

fn bind_delta_scan(
    mut scan: ScanNode,
    mv_ctx: &IcebergMvRewriteContext,
) -> Result<ScanNode, String> {
    let table = iceberg_table_info_from_source(&scan.table.source)?.clone();
    let window = resolve_snapshot_window(mv_ctx, &table)?;
    scan.table.source = ScanSource::IcebergDeltaTable {
        table,
        from_snapshot_id: window.from_snapshot_id,
        to_snapshot_id: window.to_snapshot_id,
    };
    Ok(scan)
}

fn bind_version_scan(
    mut scan: ScanNode,
    mv_ctx: &IcebergMvRewriteContext,
    role: ImvVersionRole,
) -> Result<ScanNode, String> {
    let table = iceberg_table_info_from_source(&scan.table.source)?.clone();
    let window = resolve_snapshot_window(mv_ctx, &table)?;
    let snapshot_id = match role {
        ImvVersionRole::From => window.from_snapshot_id,
        ImvVersionRole::To => window.to_snapshot_id,
    };
    scan.table.source = ScanSource::IcebergVersionTable { table, snapshot_id };
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

fn iceberg_table_info_from_source(source: &ScanSource) -> Result<&IcebergTableInfo, String> {
    match source {
        ScanSource::IcebergDataFiles { table, .. }
        | ScanSource::IcebergMetadataTable { table, .. }
        | ScanSource::IcebergDeltaTable { table, .. }
        | ScanSource::IcebergVersionTable { table, .. } => Ok(table),
        ScanSource::StarRocks { .. } | ScanSource::IcebergMvTargetState { .. } => {
            Err("BindIcebergScan only supports Iceberg scan sources".to_string())
        }
    }
}

fn resolve_snapshot_window(
    mv_ctx: &IcebergMvRewriteContext,
    table: &IcebergTableInfo,
) -> Result<ImvSnapshotWindow, String> {
    let base_ref = find_base_ref(mv_ctx, table)?;
    let base_fqn = base_ref.fqn();
    let from_snapshot_id = mv_ctx
        .previous_snapshot_ids
        .get(&base_fqn)
        .copied()
        .ok_or_else(|| {
            format!(
                "IMV scan binding requires previous snapshot for base {base_fqn}; first refresh/full rebuild must not enter incremental scan binding"
            )
        })?;
    let to_snapshot_id = mv_ctx.pin.get(base_ref).ok_or_else(|| {
        format!("IMV scan binding refresh pin missing snapshot for base {base_fqn}")
    })?;
    let pin_uuid = mv_ctx
        .pin
        .uuid(base_ref)
        .ok_or_else(|| format!("IMV scan binding refresh pin missing uuid for base {base_fqn}"))?;
    if let Some(table_uuid) = table.table_uuid.as_deref()
        && table_uuid != pin_uuid
    {
        return Err(format!(
            "IMV scan binding base table uuid mismatch for {base_fqn}: plan has {table_uuid}, pin has {pin_uuid}"
        ));
    }
    Ok(ImvSnapshotWindow {
        base_fqn,
        from_snapshot_id,
        to_snapshot_id,
        table_uuid: pin_uuid.to_string(),
    })
}

fn find_base_ref<'a>(
    mv_ctx: &'a IcebergMvRewriteContext,
    table: &IcebergTableInfo,
) -> Result<&'a IcebergTableRef, String> {
    mv_ctx
        .base_refs
        .iter()
        .find(|base| {
            base.catalog.eq_ignore_ascii_case(&table.catalog)
                && base.namespace.eq_ignore_ascii_case(&table.namespace)
                && base.table.eq_ignore_ascii_case(&table.table)
        })
        .ok_or_else(|| {
            format!(
                "IMV scan binding base {}.{}.{} is not part of MV refresh context",
                table.catalog, table.namespace, table.table
            )
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
    use crate::sql::optimizer::rewrite::imv::action_propagation::InjectActionColumnRule;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    fn iceberg_table_info(uuid: Option<&str>) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "b".to_string(),
            table_uuid: uuid.map(str::to_string),
            current_snapshot_id: Some(22),
            schema_id: 7,
            location: "file:///tmp/ice/db/b".to_string(),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn iceberg_scan(uuid: Option<&str>) -> ScanNode {
        let column = ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![column],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: iceberg_table_info(uuid),
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                    binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
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
            required_output_columns: None,
        }
    }

    #[test]
    fn resolve_window_uses_previous_snapshot_and_refresh_pin() {
        let ctx = dummy_rewrite_context();
        let window = resolve_snapshot_window(&ctx, &iceberg_table_info(Some("uuid-b")))
            .expect("window should resolve");
        assert_eq!(window.base_fqn, "ice.db.b");
        assert_eq!(window.from_snapshot_id, 11);
        assert_eq!(window.to_snapshot_id, 22);
        assert_eq!(window.table_uuid, "uuid-b");
    }

    #[test]
    fn resolve_window_rejects_uuid_mismatch() {
        let ctx = dummy_rewrite_context();
        let err = resolve_snapshot_window(&ctx, &iceberg_table_info(Some("other-uuid")))
            .expect_err("uuid mismatch must fail");
        assert!(err.contains("uuid mismatch"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn bind_delta_scan_replaces_source_with_iceberg_delta_table() {
        let ctx = dummy_rewrite_context();
        let bound =
            bind_delta_scan(iceberg_scan(Some("uuid-b")), &ctx).expect("delta scan should bind");
        match bound.table.source {
            ScanSource::IcebergDeltaTable {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => {
                assert_eq!(from_snapshot_id, 11);
                assert_eq!(to_snapshot_id, 22);
            }
            other => panic!("expected IcebergDeltaTable, got {other:?}"),
        }
    }

    #[test]
    fn bind_delta_marker_preserves_existing_action_column_id_for_injection() {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(1000)),
        });
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(LogicalPlan::Scan(iceberg_scan(Some("uuid-b")))),
            is_root: false,
            action_column: Some(ColumnId::new_for_test(77)),
            branch_scope: None,
        });
        let bind = BindIcebergScanRule;
        let RewriteResult::Changed(LogicalPlan::Scan(bound)) =
            bind.apply(plan, &mut ctx).expect("bind must succeed")
        else {
            panic!("expected changed scan");
        };
        let action = bound
            .columns
            .iter()
            .find(|column| ImvActionColumn::matches(column))
            .expect("bound delta scan must carry marker action column");
        assert_eq!(action.column_id, ColumnId::new_for_test(77));

        let inject = InjectActionColumnRule;
        assert!(
            !inject.matches(&LogicalPlan::Scan(bound), &ctx),
            "inject action must skip a scan that already carries the marker action id"
        );
    }

    #[test]
    fn bind_delta_marker_rebinds_preexisting_action_column_to_marker_id() {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(1000)),
        });
        let mut scan = iceberg_scan(Some("uuid-b"));
        scan.columns.push(OutputColumn {
            column_id: ColumnId::new_for_test(9),
            name: ImvActionColumn::NAME.to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: false,
        });
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(LogicalPlan::Scan(scan)),
            is_root: false,
            action_column: Some(ColumnId::new_for_test(77)),
            branch_scope: None,
        });

        let bind = BindIcebergScanRule;
        let RewriteResult::Changed(LogicalPlan::Scan(bound)) =
            bind.apply(plan, &mut ctx).expect("bind must succeed")
        else {
            panic!("expected changed scan");
        };
        let actions = bound
            .columns
            .iter()
            .filter(|column| ImvActionColumn::matches(column))
            .collect::<Vec<_>>();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].column_id, ColumnId::new_for_test(77));
    }

    #[test]
    fn bind_version_scan_uses_from_snapshot() {
        let ctx = dummy_rewrite_context();
        let bound = bind_version_scan(iceberg_scan(Some("uuid-b")), &ctx, ImvVersionRole::From)
            .expect("version scan should bind");
        match bound.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(snapshot_id, 11);
            }
            other => panic!("expected IcebergVersionTable, got {other:?}"),
        }
    }

    #[test]
    fn bind_version_scan_uses_to_snapshot() {
        let ctx = dummy_rewrite_context();
        let bound = bind_version_scan(iceberg_scan(Some("uuid-b")), &ctx, ImvVersionRole::To)
            .expect("version scan should bind");
        match bound.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(snapshot_id, 22);
            }
            other => panic!("expected IcebergVersionTable, got {other:?}"),
        }
    }

    #[test]
    fn bind_version_scan_strips_refresh_action_column() {
        let ctx = dummy_rewrite_context();
        let mut scan = iceberg_scan(Some("uuid-b"));
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

        let bound =
            bind_version_scan(scan, &ctx, ImvVersionRole::To).expect("version scan should bind");

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
