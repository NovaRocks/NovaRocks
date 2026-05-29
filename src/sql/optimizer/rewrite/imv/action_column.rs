//! IMV action column descriptor.
//!
//! The action column is an optimizer-internal `Int8` non-nullable column
//! produced by `InjectActionColumnRule` on Delta-bound scans. It carries
//! `+1` for inserts/upserts and `-1` for deletes at runtime (Phase 3+),
//! and is never exposed to user-visible output.

use std::sync::atomic::AtomicBool;

use arrow::datatypes::DataType;

use crate::sql::analysis::OutputColumn;
use crate::sql::catalog::ScanSource;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_propagation::first_delta_base_fqn;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{LogicalPlan, ScanNode};

pub(crate) struct ImvActionColumn;

impl ImvActionColumn {
    pub(crate) const NAME: &'static str = crate::exec::change_op::CHANGE_OP_COLUMN;
    // consumed by Phase 3 execution cutover
    #[allow(dead_code)]
    pub(crate) const INSERT_VALUE: i8 = crate::exec::change_op::CHANGE_OP_INSERT;
    // consumed by Phase 3 execution cutover
    #[allow(dead_code)]
    pub(crate) const DELETE_VALUE: i8 = crate::exec::change_op::CHANGE_OP_DELETE;

    /// Construct an `OutputColumn` for the action column with the given id.
    pub(crate) fn output_column(column_id: ColumnId) -> OutputColumn {
        OutputColumn {
            column_id,
            name: Self::NAME.to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        }
    }

    /// Returns true iff `column` is the IMV action column.
    pub(crate) fn matches(column: &OutputColumn) -> bool {
        column.is_internal && column.name.eq_ignore_ascii_case(Self::NAME)
    }
}

// ---------------------------------------------------------------------------
// ActionColumnValidationRule
// ---------------------------------------------------------------------------

/// Validates IMV action column invariants (V1-V5). Runs once at the root in
/// the validation stage. Errors identify the offending node kind / base FQN.
pub(crate) struct ActionColumnValidationRule {
    fired: AtomicBool,
}

impl ActionColumnValidationRule {
    pub(crate) fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
        }
    }
}

impl LogicalRewriteRule for ActionColumnValidationRule {
    fn name(&self) -> &'static str {
        "ActionColumnValidation"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::Validation
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        // Fire exactly once per pipeline invocation, at the first (root) node.
        !self.fired.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, std::sync::atomic::Ordering::SeqCst);
        match validate(&plan) {
            Ok(()) => Ok(RewriteResult::Unchanged),
            Err(message) => Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                "ActionColumnValidation",
                message,
            ))),
        }
    }
}

fn validate(plan: &LogicalPlan) -> Result<(), String> {
    validate_node(plan)?;
    // V4: root visible output must not be empty
    if !has_visible_output(plan) {
        return Err(
            "root plan has no user-visible output; action column or other internal column may have leaked"
                .to_string(),
        );
    }
    Ok(())
}

// Markers (ImvDelta/ImvVersion) are guaranteed absent here because
// UnresolvedMarkerCheckRule precedes ActionColumnValidation in the
// imv-validation stage and rejects any surviving marker.
fn validate_node(plan: &LogicalPlan) -> Result<(), String> {
    match plan {
        LogicalPlan::Scan(scan) => validate_scan(scan),
        LogicalPlan::Filter(node) => validate_node(&node.input),
        LogicalPlan::Project(node) => {
            validate_node(&node.input)?;
            // V3: if a delta is below, Project must expose the action column.
            // NOTE: this re-walks the subtree per Project node, so validation is
            // O(depth * subtree) on deep linear plans. Negligible for Phase 2's
            // single-table shapes; revisit with memoization if plans grow.
            if subtree_has_delta(&node.input) {
                let has = node
                    .items
                    .iter()
                    .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME));
                if !has {
                    let fqn = first_delta_base_fqn(&node.input)
                        .unwrap_or_else(|| "<unknown>".to_string());
                    return Err(format!(
                        "action column dropped at Project above delta-bound scan {fqn}"
                    ));
                }
            }
            Ok(())
        }
        LogicalPlan::Aggregate(_) if subtree_has_delta(plan) => {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Phase 2 does not support Aggregate above delta-bound scan {fqn}; deferred to Phase 4"
            ))
        }
        LogicalPlan::Join(_) if subtree_has_delta(plan) => {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Phase 2 does not support Join above delta-bound scan {fqn}; deferred to Phase 5"
            ))
        }
        LogicalPlan::Union(_) if subtree_has_delta(plan) => {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Phase 2 does not support Union above delta-bound scan {fqn}; deferred to Phase 6"
            ))
        }
        // Last safety gate: any unhandled node kind (Sort/Limit/Window/etc.)
        // sitting above a delta subtree is unsupported in Phase 2 and rejected.
        other if subtree_has_delta(other) => {
            let fqn = first_delta_base_fqn(other).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Phase 2 does not support this plan shape above delta-bound scan {fqn}; \
                 only Scan/Project/Filter are supported"
            ))
        }
        _ => Ok(()),
    }
}

fn validate_scan(scan: &ScanNode) -> Result<(), String> {
    let fqn = match &scan.table.source {
        ScanSource::IcebergDeltaTable { table, .. }
        | ScanSource::IcebergVersionTable { table, .. } => {
            format!("{}.{}.{}", table.catalog, table.namespace, table.table)
        }
        _ => scan.table.name.clone(),
    };

    let action_columns: Vec<&OutputColumn> = scan
        .columns
        .iter()
        .filter(|c| ImvActionColumn::matches(c))
        .collect();

    match &scan.table.source {
        ScanSource::IcebergDeltaTable { .. } => match action_columns.as_slice() {
            [] => Err(format!("Delta-bound scan {fqn} missing action column")),
            [col] => {
                if col.data_type != DataType::Int8 {
                    return Err(format!("Delta-bound scan {fqn} has non-Int8 action column"));
                }
                if col.nullable {
                    return Err(format!("Delta-bound scan {fqn} has nullable action column"));
                }
                Ok(())
            }
            _ => Err(format!(
                "Delta-bound scan {fqn} has duplicate action columns"
            )),
        },
        ScanSource::IcebergVersionTable { .. } => {
            if !action_columns.is_empty() {
                return Err(format!(
                    "Version-bound scan {fqn} must not carry action column"
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn subtree_has_delta(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
        }
        LogicalPlan::Filter(node) => subtree_has_delta(&node.input),
        LogicalPlan::Project(node) => subtree_has_delta(&node.input),
        LogicalPlan::Aggregate(node) => subtree_has_delta(&node.input),
        LogicalPlan::Join(node) => subtree_has_delta(&node.left) || subtree_has_delta(&node.right),
        LogicalPlan::Union(node) => node.inputs.iter().any(subtree_has_delta),
        LogicalPlan::ImvDelta(node) => subtree_has_delta(&node.input),
        LogicalPlan::ImvVersion(node) => subtree_has_delta(&node.input),
        _ => false,
    }
}

fn has_visible_output(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => scan.columns.iter().any(|c| !c.is_internal),
        LogicalPlan::Filter(node) => has_visible_output(&node.input),
        LogicalPlan::Project(node) => node
            .items
            .iter()
            .any(|item| !item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)),
        LogicalPlan::Aggregate(node) => node.output_columns.iter().any(|c| !c.is_internal),
        LogicalPlan::Join(node) => {
            has_visible_output(&node.left) || has_visible_output(&node.right)
        }
        LogicalPlan::Union(node) => node.inputs.iter().any(has_visible_output),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::ProjectNode;

    #[test]
    fn output_column_has_expected_shape() {
        let col = ImvActionColumn::output_column(ColumnId(7));
        assert_eq!(col.column_id, ColumnId(7));
        assert_eq!(col.name, "__change_op");
        assert_eq!(col.data_type, DataType::Int8);
        assert!(!col.nullable);
        assert!(col.is_internal);
    }

    #[test]
    fn matches_recognizes_action_column() {
        let col = ImvActionColumn::output_column(ColumnId(1));
        assert!(ImvActionColumn::matches(&col));
    }

    #[test]
    fn matches_rejects_external_column_with_same_name() {
        let mut col = ImvActionColumn::output_column(ColumnId(1));
        col.is_internal = false;
        assert!(!ImvActionColumn::matches(&col));
    }

    #[test]
    fn matches_rejects_other_internal_column() {
        let col = OutputColumn {
            column_id: ColumnId(1),
            name: "other".to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        };
        assert!(!ImvActionColumn::matches(&col));
    }

    #[test]
    fn constants_match_change_op_module() {
        assert_eq!(ImvActionColumn::NAME, "__change_op");
        assert_eq!(ImvActionColumn::INSERT_VALUE, 1);
        assert_eq!(ImvActionColumn::DELETE_VALUE, -1);
    }

    // ── Validation tests (V1-V5) ────────────────────────────────────────────

    fn delta_scan_with(action: Option<OutputColumn>) -> ScanNode {
        let mut scan = ScanNode {
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
        };
        if let Some(a) = action {
            scan.columns.push(a);
        }
        scan
    }

    #[test]
    fn validation_passes_on_well_formed_delta_scan() {
        let plan = LogicalPlan::Scan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        validate(&plan).expect("must validate");
    }

    #[test]
    fn validation_rejects_missing_action_column_on_delta() {
        let plan = LogicalPlan::Scan(delta_scan_with(None));
        let err = validate(&plan).expect_err("missing action must fail");
        assert!(err.contains("missing action column"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_non_int8_action_column() {
        let mut bad = ImvActionColumn::output_column(ColumnId(100));
        bad.data_type = DataType::Int64;
        let plan = LogicalPlan::Scan(delta_scan_with(Some(bad)));
        let err = validate(&plan).expect_err("non-Int8 must fail");
        assert!(err.contains("non-Int8"), "got: {err}");
    }

    #[test]
    fn validation_rejects_nullable_action_column() {
        let mut bad = ImvActionColumn::output_column(ColumnId(100));
        bad.nullable = true;
        let plan = LogicalPlan::Scan(delta_scan_with(Some(bad)));
        let err = validate(&plan).expect_err("nullable must fail");
        assert!(err.contains("nullable"), "got: {err}");
    }

    #[test]
    fn validation_rejects_duplicate_action_columns() {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns
            .push(ImvActionColumn::output_column(ColumnId(101)));
        let plan = LogicalPlan::Scan(scan);
        let err = validate(&plan).expect_err("duplicates must fail");
        assert!(err.contains("duplicate"), "got: {err}");
    }

    #[test]
    fn validation_rejects_dropped_action_above_project() {
        let scan = LogicalPlan::Scan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
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
            }],
        });
        let err = validate(&project).expect_err("dropped action must fail");
        assert!(err.contains("dropped at Project"), "got: {err}");
    }

    fn version_scan_with_action() -> ScanNode {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        // Re-point the source to a version scan while keeping the (illegal) action column.
        let table = match &scan.table.source {
            ScanSource::IcebergDeltaTable { table, .. } => table.clone(),
            _ => unreachable!(),
        };
        scan.table.source = ScanSource::IcebergVersionTable {
            table,
            snapshot_id: 22,
        };
        scan
    }

    #[test]
    fn validation_rejects_action_column_on_version_scan() {
        let plan = LogicalPlan::Scan(version_scan_with_action());
        let err = validate(&plan).expect_err("version scan with action must fail");
        assert!(err.contains("must not carry action column"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_aggregate_above_delta() {
        use crate::sql::planner::plan::AggregateNode;
        let scan = LogicalPlan::Scan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            output_columns: Vec::new(),
            already_pushed: false,
        });
        let err = validate(&plan).expect_err("aggregate above delta must fail");
        assert!(err.contains("Phase 4"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_join_above_delta() {
        use crate::sql::analysis::JoinKind;
        use crate::sql::planner::plan::JoinNode;
        let left = LogicalPlan::Scan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let right = LogicalPlan::Scan(delta_scan_with(None));
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition: None,
        });
        let err = validate(&plan).expect_err("join above delta must fail");
        assert!(err.contains("Phase 5"), "got: {err}");
    }

    #[test]
    fn validation_rejects_union_above_delta() {
        use crate::sql::planner::plan::UnionNode;
        let plan = LogicalPlan::Union(UnionNode {
            inputs: vec![LogicalPlan::Scan(delta_scan_with(Some(
                ImvActionColumn::output_column(ColumnId(100)),
            )))],
            all: true,
        });
        let err = validate(&plan).expect_err("union above delta must fail");
        assert!(err.contains("Phase 6"), "got: {err}");
    }
}
