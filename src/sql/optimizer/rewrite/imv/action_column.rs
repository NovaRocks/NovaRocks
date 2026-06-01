//! IMV action column descriptor.
//!
//! The action column is an optimizer-internal `Int8` non-nullable column
//! produced by `InjectActionColumnRule` on Delta-bound scans. It carries
//! `+1` for inserts/upserts and `-1` for deletes at runtime (Phase 3+),
//! and is never exposed to user-visible output.

use std::sync::atomic::AtomicBool;

use arrow::datatypes::DataType;

use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN;
use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn};
use crate::sql::catalog::ScanSource;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_propagation::first_delta_base_fqn;
use crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn;
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
    // V6: if a delta subtree exists, root output must carry the apply key.
    if !matches!(plan, LogicalPlan::AggregateStateMerge(_))
        && subtree_has_delta(plan)
        && !output_has_apply_key(plan)
    {
        let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
        return Err(format!(
            "plan above delta-bound scan {fqn} is missing apply key column \
             {ICEBERG_MV_APPLY_KEY_COLUMN}"
        ));
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
        LogicalPlan::AggregateStateMerge(node) => {
            validate_node(&node.old_input)?;
            validate_state_merge_delta_input(&node.delta_input)
        }
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

fn validate_state_merge_delta_input(plan: &LogicalPlan) -> Result<(), String> {
    match plan {
        LogicalPlan::Aggregate(node) if is_signed_state_aggregate(node) => {
            validate_signed_delta_input(&node.input)
        }
        LogicalPlan::Project(project)
            if matches!(
                project.input.as_ref(),
                LogicalPlan::Aggregate(node) if is_signed_state_aggregate(node)
            ) =>
        {
            let LogicalPlan::Aggregate(node) = project.input.as_ref() else {
                unreachable!();
            };
            validate_signed_delta_input(&node.input)
        }
        _ => validate_node(plan),
    }
}

fn is_signed_state_aggregate(node: &crate::sql::planner::plan::AggregateNode) -> bool {
    !node.aggregates.is_empty()
        && node
            .aggregates
            .iter()
            .any(|call| call.name.ends_with("_state_signed"))
        && node.aggregates.iter().all(|call| {
            call.name.ends_with("_state_signed") || is_hidden_retraction_count_call(call)
        })
}

fn is_hidden_retraction_count_call(call: &crate::sql::planner::plan::AggregateCall) -> bool {
    call.name.eq_ignore_ascii_case("sum")
        && call.args.len() == 1
        && matches!(
            &call.args[0].kind,
            ExprKind::ColumnRef { column, .. } if column.eq_ignore_ascii_case(ImvActionColumn::NAME)
        )
}

fn validate_signed_delta_input(plan: &LogicalPlan) -> Result<(), String> {
    match plan {
        LogicalPlan::Scan(scan) => validate_scan(scan),
        LogicalPlan::Filter(node) => validate_signed_delta_input(&node.input),
        LogicalPlan::Project(node) => {
            validate_signed_delta_input(&node.input)?;
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
        LogicalPlan::Join(node) if is_supported_join_delta_branch(node) => {
            validate_signed_delta_input(&node.left)?;
            validate_signed_delta_input(&node.right)
        }
        LogicalPlan::Union(node) if is_supported_join_delta_union(node) => {
            for input in &node.inputs {
                validate_signed_delta_input(input)?;
            }
            Ok(())
        }
        _ => validate_node(plan),
    }
}

fn is_supported_join_delta_union(node: &crate::sql::planner::plan::UnionNode) -> bool {
    node.all
        && !node.inputs.is_empty()
        && node
            .inputs
            .iter()
            .all(is_supported_normalized_join_delta_branch)
}

fn is_supported_normalized_join_delta_branch(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Project(project) => {
            project
                .items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
                && matches!(
                    project.input.as_ref(),
                    LogicalPlan::Join(join) if is_supported_join_delta_branch(join)
                )
        }
        LogicalPlan::Join(join) => is_supported_join_delta_branch(join),
        _ => false,
    }
}

fn is_supported_join_delta_branch(node: &crate::sql::planner::plan::JoinNode) -> bool {
    matches!(node.join_type, JoinKind::Inner | JoinKind::Cross)
        && exactly_one_delta_one_version(&node.left, &node.right)
}

fn exactly_one_delta_one_version(left: &LogicalPlan, right: &LogicalPlan) -> bool {
    let left_delta = subtree_has_delta(left);
    let right_delta = subtree_has_delta(right);
    let left_version = subtree_has_version(left);
    let right_version = subtree_has_version(right);
    ((left_delta && right_version) || (right_delta && left_version))
        && !(left_delta && right_delta)
        && !(left_version && right_version)
}

fn subtree_has_version(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergVersionTable { .. })
        }
        LogicalPlan::Filter(node) => subtree_has_version(&node.input),
        LogicalPlan::Project(node) => subtree_has_version(&node.input),
        LogicalPlan::Aggregate(node) => subtree_has_version(&node.input),
        LogicalPlan::AggregateStateMerge(node) => {
            subtree_has_version(&node.old_input) || subtree_has_version(&node.delta_input)
        }
        LogicalPlan::Join(node) => {
            subtree_has_version(&node.left) || subtree_has_version(&node.right)
        }
        LogicalPlan::Union(node) => node.inputs.iter().any(subtree_has_version),
        LogicalPlan::ImvDelta(node) => subtree_has_version(&node.input),
        LogicalPlan::ImvVersion(node) => subtree_has_version(&node.input),
        _ => false,
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
                // V7: _row_id must be present so the apply-key projection can reference it.
                if !scan.columns.iter().any(ImvRowIdColumn::matches) {
                    return Err(format!("Delta-bound scan {fqn} missing _row_id column"));
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

fn output_has_apply_key(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Project(p) => p.items.iter().any(|i| {
            i.output_name
                .eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)
        }),
        LogicalPlan::Filter(node) => output_has_apply_key(&node.input),
        _ => false,
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
        LogicalPlan::AggregateStateMerge(node) => {
            subtree_has_delta(&node.old_input) || subtree_has_delta(&node.delta_input)
        }
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
        LogicalPlan::Project(node) => node.items.iter().any(|item| {
            !item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)
                && !item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME)
                && !item
                    .output_name
                    .eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)
        }),
        LogicalPlan::Aggregate(node) => node.output_columns.iter().any(|c| !c.is_internal),
        LogicalPlan::AggregateStateMerge(node) => {
            node.output_columns.iter().any(|c| !c.is_internal)
        }
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
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, AggregateStateMergeNode, ProjectNode, ValuesNode,
    };

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
            required_output_columns: None,
        };
        if let Some(a) = action {
            scan.columns.push(a);
        }
        scan
    }

    #[test]
    fn validation_passes_on_well_formed_delta_scan() {
        // Scan with k + action + _row_id, root Project carrying all three plus
        // the apply key. This is the shape the IMV pipeline produces.
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Scan(scan)),
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
                            column_id: ColumnId(100),
                            qualifier: None,
                            column: ImvActionColumn::NAME.to_string(),
                        },
                        data_type: DataType::Int8,
                        nullable: false,
                    },
                    output_name: ImvActionColumn::NAME.to_string(),
                    output_column_id: ColumnId(100),
                },
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId(101),
                            qualifier: None,
                            column: ImvRowIdColumn::NAME.to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: ImvRowIdColumn::NAME.to_string(),
                    output_column_id: ColumnId(101),
                },
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId(101),
                            qualifier: None,
                            column: ImvRowIdColumn::NAME.to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
                    output_column_id: ColumnId(102),
                },
            ],
            required_output_columns: None,
        });
        validate(&project).expect("must validate");
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
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        // Add _row_id so V7 passes; V3 fires because the Project below drops action.
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Scan(scan)),
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
                output_column_id: ColumnId(1),
            }],
            required_output_columns: None,
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
            required_output_columns: None,
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
            required_output_columns: None,
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
            output_columns: Vec::new(),
            required_output_columns: None,
        });
        let err = validate(&plan).expect_err("union above delta must fail");
        assert!(err.contains("Phase 6"), "got: {err}");
    }

    // ── V6 / V7 failing tests (RED phase) ───────────────────────────────────

    /// Helper: delta scan with both a valid action column and `_row_id`.
    fn delta_scan_with_action_and_row_id() -> ScanNode {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        scan
    }

    #[test]
    fn validation_rejects_missing_apply_key_above_delta() {
        // Project carries k + __change_op + _row_id but NOT __nova_base_row_id.
        let scan = delta_scan_with_action_and_row_id();
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Scan(scan)),
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
                            column_id: ColumnId(100),
                            qualifier: None,
                            column: ImvActionColumn::NAME.to_string(),
                        },
                        data_type: DataType::Int8,
                        nullable: false,
                    },
                    output_name: ImvActionColumn::NAME.to_string(),
                    output_column_id: ColumnId(100),
                },
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId(101),
                            qualifier: None,
                            column: ImvRowIdColumn::NAME.to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: ImvRowIdColumn::NAME.to_string(),
                    output_column_id: ColumnId(101),
                },
                // __nova_base_row_id is intentionally absent.
            ],
            required_output_columns: None,
        });
        let err = validate(&project).expect_err("missing apply key must fail");
        assert!(err.contains("apply key"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_delta_scan_missing_row_id() {
        // Scan has action column but NOT _row_id.
        let plan = LogicalPlan::Scan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let err = validate(&plan).expect_err("missing _row_id must fail");
        assert!(err.contains("_row_id"), "got: {err}");
    }

    fn signed_aggregate_state_merge(action: Option<OutputColumn>) -> LogicalPlan {
        let mut scan = delta_scan_with(action);
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        LogicalPlan::AggregateStateMerge(AggregateStateMergeNode {
            old_input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
                required_output_columns: None,
            })),
            delta_input: Box::new(LogicalPlan::Aggregate(AggregateNode {
                input: Box::new(LogicalPlan::Scan(scan)),
                group_by: Vec::new(),
                aggregates: vec![AggregateCall {
                    name: "sum_state_signed".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: DataType::Binary,
                    order_by: Vec::new(),
                }],
                output_columns: vec![OutputColumn {
                    column_id: ColumnId(10),
                    name: "s".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                }],
                already_pushed: false,
                required_output_columns: None,
            })),
            group_key_names: vec!["k".to_string()],
            aggregate_state_names: vec!["__agg_state_s".to_string()],
            change_op_column: "__change_op".to_string(),
            output_columns: vec![OutputColumn {
                column_id: ColumnId(10),
                name: "s".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
        })
    }

    #[test]
    fn validation_accepts_signed_aggregate_inside_state_merge() {
        let plan =
            signed_aggregate_state_merge(Some(ImvActionColumn::output_column(ColumnId(100))));

        validate(&plan).expect("signed aggregate inside AggregateStateMerge must validate");
    }

    #[test]
    fn validation_traverses_state_merge_delta_input_for_missing_action_column() {
        let plan = signed_aggregate_state_merge(None);

        let err = validate(&plan).expect_err("missing action inside state merge must fail");
        assert!(err.contains("missing action column"), "got: {err}");
    }

    #[test]
    fn validation_uses_state_merge_output_for_visible_output_check() {
        let mut plan =
            signed_aggregate_state_merge(Some(ImvActionColumn::output_column(ColumnId(100))));
        let LogicalPlan::AggregateStateMerge(node) = &mut plan else {
            unreachable!();
        };
        node.output_columns = vec![ImvActionColumn::output_column(ColumnId(200))];

        let err = validate(&plan).expect_err("internal-only state merge output must fail");
        assert!(err.contains("no user-visible output"), "got: {err}");
    }
}
