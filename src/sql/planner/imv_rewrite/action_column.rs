//! IMV action column descriptor.
//!
//! The action column is an optimizer-internal `Int8` non-nullable column
//! produced by `InjectActionColumnRule` on Delta-bound scans. It carries
//! `+1` for inserts/upserts and `-1` for deletes at runtime, and is never
//! exposed to user-visible output.

use std::sync::atomic::AtomicBool;

use arrow::datatypes::DataType;

use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN;
use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn};
use crate::sql::catalog::ScanSource;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_propagation::{
    first_delta_base_fqn, is_supported_branch_union, is_supported_fan_in_delta_union,
};
use crate::sql::planner::imv_rewrite::join_delta_shape::{
    is_supported_join_delta_branch, is_supported_join_delta_union,
};
use crate::sql::planner::imv_rewrite::opt_expr_to_plan;
use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
use crate::sql::planner::plan::{
    LogicalAggregateNode, LogicalPlanNode, LogicalScanNode, PlanNodeKind,
};

pub(crate) struct ImvActionColumn;

impl ImvActionColumn {
    pub(crate) const NAME: &'static str = crate::exec::change_op::CHANGE_OP_COLUMN;
    // Consumed by IMV refresh execution.
    #[allow(dead_code)]
    pub(crate) const INSERT_VALUE: i8 = crate::exec::change_op::CHANGE_OP_INSERT;
    // Consumed by IMV refresh execution.
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

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        // Fire exactly once per pipeline invocation, at the first (root) node.
        !self.fired.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, std::sync::atomic::Ordering::SeqCst);
        let plan = opt_expr_to_plan(expr, ctx);
        match validate(&plan) {
            Ok(()) => Ok(RewriteResult::Unchanged),
            Err(message) => Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                "ActionColumnValidation",
                message,
            ))),
        }
    }
}

fn validate(plan: &LogicalPlanNode) -> Result<(), String> {
    validate_node(plan)?;
    // V4: root visible output must not be empty
    if !has_visible_output(plan) {
        return Err(
            "root plan has no user-visible output; action column or other internal column may have leaked"
                .to_string(),
        );
    }
    // V6: if a delta subtree exists, root output must carry the apply key.
    if !matches!(&plan.kind, PlanNodeKind::AggregateStateMerge(_))
        && !(matches!(&plan.kind, PlanNodeKind::Union(_)) && is_supported_branch_union(plan))
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
fn validate_node(plan: &LogicalPlanNode) -> Result<(), String> {
    match &plan.kind {
        PlanNodeKind::Scan(scan) => validate_scan(scan),
        PlanNodeKind::Filter(_) => validate_node(plan.unary_input()),
        PlanNodeKind::AggregateStateMerge(_) => {
            validate_node(plan.left())?;
            validate_state_merge_delta_input(plan.right())
        }
        PlanNodeKind::Project(node) => {
            let input = plan.unary_input();
            validate_node(input)?;
            // V3: if a delta is below, Project must expose the action column.
            // NOTE: this re-walks the subtree per Project node, so validation is
            // O(depth * subtree) on deep linear plans. Negligible for current
            // single-table shapes; revisit with memoization if plans grow.
            if subtree_has_delta(input) {
                let has = node
                    .items
                    .iter()
                    .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME));
                if !has {
                    let fqn =
                        first_delta_base_fqn(input).unwrap_or_else(|| "<unknown>".to_string());
                    return Err(format!(
                        "action column dropped at Project above delta-bound scan {fqn}"
                    ));
                }
            }
            Ok(())
        }
        PlanNodeKind::Aggregate(_) if subtree_has_delta(plan) => {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Iceberg IMV rewrite does not support this aggregate shape above delta-bound scan {fqn}"
            ))
        }
        PlanNodeKind::Join(_) if subtree_has_delta(plan) => {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Iceberg IMV rewrite does not support this join shape above delta-bound scan {fqn}"
            ))
        }
        PlanNodeKind::Union(_) if is_supported_fan_in_delta_union(plan) => {
            for input in &plan.children {
                validate_node(input)?;
            }
            Ok(())
        }
        PlanNodeKind::Union(_) if is_supported_branch_union(plan) => {
            validate_supported_branch_union(plan)
        }
        PlanNodeKind::Union(_) if is_supported_join_delta_union(plan) => {
            for input in &plan.children {
                validate_signed_delta_input(input)?;
            }
            Ok(())
        }
        PlanNodeKind::Union(_)
            if subtree_has_delta(plan)
                && !is_supported_join_delta_union(plan)
                && !is_supported_fan_in_delta_union(plan)
                && !is_supported_branch_union(plan) =>
        {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Iceberg IMV rewrite does not support this union shape above delta-bound scan {fqn}"
            ))
        }
        // Last safety gate: any unhandled node kind (Sort/Limit/Window/etc.)
        // sitting above a delta subtree is unsupported and rejected.
        _ if subtree_has_delta(plan) => {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Iceberg IMV rewrite does not support this plan shape above delta-bound scan {fqn}; \
                 supported shapes must be consumed before validation"
            ))
        }
        _ => Ok(()),
    }
}

fn validate_supported_branch_union(plan: &LogicalPlanNode) -> Result<(), String> {
    for input in &plan.children {
        validate_branch_union_input(input)?;
    }
    Ok(())
}

fn validate_branch_union_input(plan: &LogicalPlanNode) -> Result<(), String> {
    let PlanNodeKind::Project(_) = &plan.kind else {
        return Err("supported branch UNION expected Project branch input".to_string());
    };
    if !is_supported_branch_state_merge_project(plan) {
        return Err(
            "supported branch UNION expected Project over AggregateStateMerge with branch id"
                .to_string(),
        );
    }
    let merge = plan.unary_input();
    if !matches!(&merge.kind, PlanNodeKind::AggregateStateMerge(_)) {
        return Err("supported branch UNION expected AggregateStateMerge branch input".to_string());
    }
    validate_node(merge.left())?;
    validate_state_merge_delta_input(merge.right())
}

fn is_supported_branch_state_merge_project(plan: &LogicalPlanNode) -> bool {
    let PlanNodeKind::Project(node) = &plan.kind else {
        return false;
    };
    matches!(
        &plan.unary_input().kind,
        PlanNodeKind::AggregateStateMerge(_)
    ) && node.items.iter().any(|item| {
        item.output_name.eq_ignore_ascii_case(
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
        ) && is_branch_id_literal_expr(&item.expr)
    })
}

fn is_branch_id_literal_expr(expr: &crate::sql::analysis::TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::Literal(LiteralValue::Int(_)) => true,
        ExprKind::Cast { expr, target } => {
            *target == DataType::Int32
                && matches!(&expr.kind, ExprKind::Literal(LiteralValue::Int(_)))
        }
        _ => false,
    }
}

fn validate_state_merge_delta_input(plan: &LogicalPlanNode) -> Result<(), String> {
    match &plan.kind {
        PlanNodeKind::Aggregate(node) if is_signed_state_aggregate(node) => {
            validate_signed_delta_input(plan.unary_input())
        }
        PlanNodeKind::Project(_)
            if matches!(
                &plan.unary_input().kind,
                PlanNodeKind::Aggregate(node) if is_signed_state_aggregate(node)
            ) =>
        {
            validate_signed_delta_input(plan.unary_input().unary_input())
        }
        _ => validate_node(plan),
    }
}

fn is_signed_state_aggregate(node: &LogicalAggregateNode) -> bool {
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

fn validate_signed_delta_input(plan: &LogicalPlanNode) -> Result<(), String> {
    match &plan.kind {
        PlanNodeKind::Scan(scan) => validate_scan(scan),
        PlanNodeKind::Filter(_) => validate_signed_delta_input(plan.unary_input()),
        PlanNodeKind::Project(node) => {
            let input = plan.unary_input();
            validate_signed_delta_input(input)?;
            if subtree_has_delta(input) {
                let has = node
                    .items
                    .iter()
                    .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME));
                if !has {
                    let fqn =
                        first_delta_base_fqn(input).unwrap_or_else(|| "<unknown>".to_string());
                    return Err(format!(
                        "action column dropped at Project above delta-bound scan {fqn}"
                    ));
                }
            }
            Ok(())
        }
        PlanNodeKind::Join(_) if is_supported_join_delta_branch(plan) => {
            validate_signed_delta_input(plan.left())?;
            validate_signed_delta_input(plan.right())
        }
        PlanNodeKind::Union(_)
            if is_supported_join_delta_union(plan) || is_supported_fan_in_delta_union(plan) =>
        {
            for input in &plan.children {
                validate_signed_delta_input(input)?;
            }
            Ok(())
        }
        _ => validate_node(plan),
    }
}

fn validate_scan(scan: &LogicalScanNode) -> Result<(), String> {
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

fn output_has_apply_key(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Project(p) => p.items.iter().any(|i| {
            i.output_name
                .eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)
        }),
        PlanNodeKind::Filter(_) => output_has_apply_key(plan.unary_input()),
        _ => false,
    }
}

fn subtree_has_delta(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
        }
        _ => plan.children.iter().any(subtree_has_delta),
    }
}

fn has_visible_output(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Scan(scan) => scan.columns.iter().any(|c| !c.is_internal),
        PlanNodeKind::Filter(_) => has_visible_output(plan.unary_input()),
        PlanNodeKind::Project(node) => node.items.iter().any(|item| {
            !item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)
                && !item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME)
                && !item
                    .output_name
                    .eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)
        }),
        PlanNodeKind::Aggregate(node) => node.output_columns.iter().any(|c| !c.is_internal),
        PlanNodeKind::AggregateStateMerge(node) => {
            node.output_columns.iter().any(|c| !c.is_internal)
        }
        PlanNodeKind::Join(_) => {
            has_visible_output(plan.left()) || has_visible_output(plan.right())
        }
        PlanNodeKind::Union(_) => plan.children.iter().any(has_visible_output),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN;
    use crate::sql::analysis::{ExprKind, JoinKind, LiteralValue, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::*;
    use crate::sql::planner::plan::{
        AggregateCall, LogicalAggregateNode, LogicalAggregateStateMergeNode, LogicalJoinNode,
        LogicalProjectNode, LogicalUnionNode, LogicalValuesNode, PlanNodeKind,
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

    fn delta_scan_with(action: Option<OutputColumn>) -> LogicalScanNode {
        let mut scan = LogicalScanNode {
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
        };
        if let Some(a) = action {
            scan.columns.push(a);
        }
        scan
    }

    fn scan_plan(scan: LogicalScanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(PlanNodeKind::Scan(scan), vec![], None)
    }

    #[test]
    fn validation_passes_on_well_formed_delta_scan() {
        // Scan with k + action + _row_id, root Project carrying all three plus
        // the apply key. This is the shape the IMV pipeline produces.
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        let project = LogicalPlanNode::new(
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
                output_qualifier: None,
            }),
            vec![scan_plan(scan)],
            None,
        );
        validate(&project).expect("must validate");
    }

    #[test]
    fn validation_rejects_missing_action_column_on_delta() {
        let plan = scan_plan(delta_scan_with(None));
        let err = validate(&plan).expect_err("missing action must fail");
        assert!(err.contains("missing action column"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_non_int8_action_column() {
        let mut bad = ImvActionColumn::output_column(ColumnId(100));
        bad.data_type = DataType::Int64;
        let plan = scan_plan(delta_scan_with(Some(bad)));
        let err = validate(&plan).expect_err("non-Int8 must fail");
        assert!(err.contains("non-Int8"), "got: {err}");
    }

    #[test]
    fn validation_rejects_nullable_action_column() {
        let mut bad = ImvActionColumn::output_column(ColumnId(100));
        bad.nullable = true;
        let plan = scan_plan(delta_scan_with(Some(bad)));
        let err = validate(&plan).expect_err("nullable must fail");
        assert!(err.contains("nullable"), "got: {err}");
    }

    #[test]
    fn validation_rejects_duplicate_action_columns() {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns
            .push(ImvActionColumn::output_column(ColumnId(101)));
        let plan = scan_plan(scan);
        let err = validate(&plan).expect_err("duplicates must fail");
        assert!(err.contains("duplicate"), "got: {err}");
    }

    #[test]
    fn validation_rejects_dropped_action_above_project() {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        // Add _row_id so V7 passes; V3 fires because the Project below drops action.
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        let project = LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
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
                output_qualifier: None,
            }),
            vec![scan_plan(scan)],
            None,
        );
        let err = validate(&project).expect_err("dropped action must fail");
        assert!(err.contains("dropped at Project"), "got: {err}");
    }

    fn version_scan_with_action() -> LogicalScanNode {
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
        let plan = scan_plan(version_scan_with_action());
        let err = validate(&plan).expect_err("version scan with action must fail");
        assert!(err.contains("must not carry action column"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_aggregate_above_delta() {
        use crate::sql::planner::plan::LogicalAggregateNode;
        let scan = scan_plan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: Vec::new(),
                aggregates: Vec::new(),
                output_columns: Vec::new(),
                already_pushed: false,
            }),
            vec![scan],
            None,
        );
        let err = validate(&plan).expect_err("aggregate above delta must fail");
        assert!(
            err.contains("Iceberg IMV rewrite does not support this aggregate shape"),
            "got: {err}"
        );
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_join_above_delta() {
        use crate::sql::analysis::JoinKind;
        use crate::sql::planner::plan::LogicalJoinNode;
        let left = scan_plan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let right = scan_plan(delta_scan_with(None));
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![left, right],
            None,
        );
        let err = validate(&plan).expect_err("join above delta must fail");
        assert!(
            err.contains("Iceberg IMV rewrite does not support this join shape"),
            "got: {err}"
        );
    }

    #[test]
    fn validation_rejects_union_above_delta() {
        let plan = LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: Vec::new(),
            }),
            vec![
                normalized_delta_project(ColumnId(100), ColumnId(1), ColumnId(101)),
                normalized_delta_project_without_action(ColumnId(10), ColumnId(111)),
            ],
            None,
        );
        let err = validate(&plan).expect_err("union above delta must fail");
        assert!(
            err.contains("Iceberg IMV rewrite does not support this union shape"),
            "got: {err}"
        );
    }

    fn column_ref_item(
        column_id: ColumnId,
        column: &str,
        data_type: DataType,
        nullable: bool,
        output_name: &str,
        output_column_id: ColumnId,
    ) -> ProjectItem {
        ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id,
                    qualifier: None,
                    column: column.to_string(),
                },
                data_type,
                nullable,
            },
            output_name: output_name.to_string(),
            output_column_id,
        }
    }

    fn normalized_delta_project(
        action_id: ColumnId,
        user_col_id: ColumnId,
        row_id: ColumnId,
    ) -> LogicalPlanNode {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(action_id)));
        scan.columns[0].column_id = user_col_id;
        scan.columns.push(ImvRowIdColumn::output_column(row_id));
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    column_ref_item(user_col_id, "k", DataType::Int64, false, "k", user_col_id),
                    column_ref_item(
                        action_id,
                        ImvActionColumn::NAME,
                        DataType::Int8,
                        false,
                        ImvActionColumn::NAME,
                        action_id,
                    ),
                    column_ref_item(
                        row_id,
                        ImvRowIdColumn::NAME,
                        DataType::Int64,
                        false,
                        ImvRowIdColumn::NAME,
                        row_id,
                    ),
                ],
                output_qualifier: None,
            }),
            vec![scan_plan(scan)],
            None,
        )
    }

    fn normalized_delta_project_without_action(
        user_col_id: ColumnId,
        row_id: ColumnId,
    ) -> LogicalPlanNode {
        let mut scan = delta_scan_with(None);
        scan.columns[0].column_id = user_col_id;
        scan.columns.push(ImvRowIdColumn::output_column(row_id));
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    column_ref_item(user_col_id, "k", DataType::Int64, false, "k", user_col_id),
                    column_ref_item(
                        row_id,
                        ImvRowIdColumn::NAME,
                        DataType::Int64,
                        false,
                        ImvRowIdColumn::NAME,
                        row_id,
                    ),
                ],
                output_qualifier: None,
            }),
            vec![scan_plan(scan)],
            None,
        )
    }

    fn version_scan_without_action() -> LogicalScanNode {
        let mut scan = delta_scan_with(None);
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

    fn normalized_version_project(user_col_id: ColumnId) -> LogicalPlanNode {
        let mut scan = version_scan_without_action();
        scan.columns[0].column_id = user_col_id;
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![column_ref_item(
                    user_col_id,
                    "k",
                    DataType::Int64,
                    false,
                    "k",
                    user_col_id,
                )],
                output_qualifier: None,
            }),
            vec![scan_plan(scan)],
            None,
        )
    }

    fn join_plan(left: LogicalPlanNode, right: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![left, right],
            None,
        )
    }

    fn normalized_join_delta_branch(
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        action_id: ColumnId,
        user_col_id: ColumnId,
        row_id: ColumnId,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    column_ref_item(user_col_id, "k", DataType::Int64, false, "k", user_col_id),
                    column_ref_item(
                        action_id,
                        ImvActionColumn::NAME,
                        DataType::Int8,
                        false,
                        ImvActionColumn::NAME,
                        action_id,
                    ),
                    column_ref_item(
                        row_id,
                        ImvRowIdColumn::NAME,
                        DataType::Int64,
                        false,
                        ImvRowIdColumn::NAME,
                        row_id,
                    ),
                ],
                output_qualifier: None,
            }),
            vec![join_plan(left, right)],
            None,
        )
    }

    fn recursive_join_delta_union(action_id: ColumnId) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    ImvActionColumn::output_column(action_id),
                    ImvRowIdColumn::output_column(ColumnId(101)),
                ],
            }),
            vec![
                normalized_join_delta_branch(
                    normalized_delta_project(action_id, ColumnId(1), ColumnId(101)),
                    normalized_version_project(ColumnId(10)),
                    action_id,
                    ColumnId(1),
                    ColumnId(101),
                ),
                normalized_join_delta_branch(
                    normalized_version_project(ColumnId(1)),
                    normalized_delta_project(action_id, ColumnId(10), ColumnId(111)),
                    action_id,
                    ColumnId(1),
                    ColumnId(101),
                ),
            ],
            None,
        )
    }

    fn fan_in_delta_union(action_id: ColumnId) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId(1),
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    ImvActionColumn::output_column(action_id),
                    ImvRowIdColumn::output_column(ColumnId(101)),
                ],
            }),
            vec![
                normalized_delta_project(action_id, ColumnId(1), ColumnId(101)),
                normalized_delta_project(action_id, ColumnId(10), ColumnId(111)),
            ],
            None,
        )
    }

    fn output_column(
        column_id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(column_id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal,
        }
    }

    fn aggregate_state_merge_stub() -> LogicalPlanNode {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        LogicalPlanNode::new(
            PlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
                group_key_names: vec!["region".to_string()],
                aggregate_state_names: vec!["__agg_state_s".to_string()],
                change_op_column: ImvActionColumn::NAME.to_string(),
                output_columns: vec![
                    output_column(1, "region", DataType::Utf8, false, false),
                    output_column(2, "s", DataType::Int64, true, false),
                ],
            }),
            vec![
                LogicalPlanNode::new(
                    PlanNodeKind::Values(LogicalValuesNode {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    }),
                    vec![],
                    None,
                ),
                LogicalPlanNode::new(
                    PlanNodeKind::Aggregate(LogicalAggregateNode {
                        group_by: Vec::new(),
                        aggregates: vec![AggregateCall {
                            name: "sum_state_signed".to_string(),
                            args: Vec::new(),
                            distinct: false,
                            result_type: DataType::Binary,
                            order_by: Vec::new(),
                            output_column_id: ColumnId::UNSET,
                        }],
                        output_columns: vec![output_column(2, "s", DataType::Int64, true, false)],
                        already_pushed: false,
                    }),
                    vec![scan_plan(scan)],
                    None,
                ),
            ],
            None,
        )
    }

    fn project_with_branch_id(input: LogicalPlanNode, branch_id: i32) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    column_ref_item(
                        ColumnId(1),
                        "region",
                        DataType::Utf8,
                        false,
                        "region",
                        ColumnId(1),
                    ),
                    column_ref_item(ColumnId(2), "s", DataType::Int64, true, "s", ColumnId(2)),
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::Cast {
                                expr: Box::new(TypedExpr {
                                    kind: ExprKind::Literal(LiteralValue::Int(branch_id as i64)),
                                    data_type: DataType::Int64,
                                    nullable: false,
                                }),
                                target: DataType::Int32,
                            },
                            data_type: DataType::Int32,
                            nullable: false,
                        },
                        output_name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
                        output_column_id: ColumnId(100),
                    },
                ],
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    fn branch_union_with_aggregate_state_merge() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![
                    output_column(1, "region", DataType::Utf8, false, false),
                    output_column(2, "s", DataType::Int64, true, false),
                    output_column(
                        100,
                        ICEBERG_MV_BRANCH_ID_COLUMN,
                        DataType::Int32,
                        false,
                        true,
                    ),
                ],
            }),
            vec![
                project_with_branch_id(aggregate_state_merge_stub(), 0),
                project_with_branch_id(aggregate_state_merge_stub(), 1),
            ],
            None,
        )
    }

    fn root_project_with_apply_key(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![
                    column_ref_item(ColumnId(1), "k", DataType::Int64, false, "k", ColumnId(1)),
                    column_ref_item(
                        ColumnId(100),
                        ImvActionColumn::NAME,
                        DataType::Int8,
                        false,
                        ImvActionColumn::NAME,
                        ColumnId(100),
                    ),
                    column_ref_item(
                        ColumnId(101),
                        ImvRowIdColumn::NAME,
                        DataType::Int64,
                        false,
                        ImvRowIdColumn::NAME,
                        ColumnId(101),
                    ),
                    column_ref_item(
                        ColumnId(101),
                        ImvRowIdColumn::NAME,
                        DataType::Int64,
                        false,
                        ICEBERG_MV_APPLY_KEY_COLUMN,
                        ColumnId(102),
                    ),
                ],
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    #[test]
    fn validation_accepts_fan_in_delta_union_above_delta_scans() {
        let plan = root_project_with_apply_key(fan_in_delta_union(ColumnId(100)));

        validate(&plan).expect("fan-in delta union must validate");
    }

    #[test]
    fn validation_accepts_rewritten_branch_union() {
        let plan = branch_union_with_aggregate_state_merge();

        validate(&plan).expect("rewritten branch union should validate");
    }

    #[test]
    fn validation_rejects_standalone_branch_project_without_action_column() {
        let plan = project_with_branch_id(aggregate_state_merge_stub(), 0);

        let err = validate_node(&plan).expect_err("standalone branch Project must fail");
        assert!(
            err.contains("action column dropped at Project"),
            "got: {err}"
        );
    }

    // ── V6 / V7 failing tests (RED phase) ───────────────────────────────────

    /// Helper: delta scan with both a valid action column and `_row_id`.
    fn delta_scan_with_action_and_row_id() -> LogicalScanNode {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        scan
    }

    #[test]
    fn validation_rejects_missing_apply_key_above_delta() {
        // Project carries k + __change_op + _row_id but NOT __nova_base_row_id.
        let scan = delta_scan_with_action_and_row_id();
        let project = LogicalPlanNode::new(
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
                output_qualifier: None,
            }),
            vec![scan_plan(scan)],
            None,
        );
        let err = validate(&project).expect_err("missing apply key must fail");
        assert!(err.contains("apply key"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_rejects_delta_scan_missing_row_id() {
        // Scan has action column but NOT _row_id.
        let plan = scan_plan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let err = validate(&plan).expect_err("missing _row_id must fail");
        assert!(err.contains("_row_id"), "got: {err}");
    }

    fn signed_aggregate_state_merge(action: Option<OutputColumn>) -> LogicalPlanNode {
        let mut scan = delta_scan_with(action);
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        LogicalPlanNode::new(
            PlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
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
            }),
            vec![
                LogicalPlanNode::new(
                    PlanNodeKind::Values(LogicalValuesNode {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    }),
                    vec![],
                    None,
                ),
                LogicalPlanNode::new(
                    PlanNodeKind::Aggregate(LogicalAggregateNode {
                        group_by: Vec::new(),
                        aggregates: vec![AggregateCall {
                            name: "sum_state_signed".to_string(),
                            args: Vec::new(),
                            distinct: false,
                            result_type: DataType::Binary,
                            order_by: Vec::new(),
                            output_column_id: ColumnId::UNSET,
                        }],
                        output_columns: vec![OutputColumn {
                            column_id: ColumnId(10),
                            name: "s".to_string(),
                            data_type: DataType::Int64,
                            nullable: true,
                            is_internal: false,
                        }],
                        already_pushed: false,
                    }),
                    vec![scan_plan(scan)],
                    None,
                ),
            ],
            None,
        )
    }

    fn signed_aggregate_state_merge_over(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
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
            }),
            vec![
                LogicalPlanNode::new(
                    PlanNodeKind::Values(LogicalValuesNode {
                        rows: Vec::new(),
                        columns: Vec::new(),
                    }),
                    vec![],
                    None,
                ),
                LogicalPlanNode::new(
                    PlanNodeKind::Aggregate(LogicalAggregateNode {
                        group_by: Vec::new(),
                        aggregates: vec![AggregateCall {
                            name: "sum_state_signed".to_string(),
                            args: Vec::new(),
                            distinct: false,
                            result_type: DataType::Binary,
                            order_by: Vec::new(),
                            output_column_id: ColumnId::UNSET,
                        }],
                        output_columns: vec![OutputColumn {
                            column_id: ColumnId(10),
                            name: "s".to_string(),
                            data_type: DataType::Int64,
                            nullable: true,
                            is_internal: false,
                        }],
                        already_pushed: false,
                    }),
                    vec![input],
                    None,
                ),
            ],
            None,
        )
    }

    #[test]
    fn validation_accepts_signed_aggregate_inside_state_merge() {
        let plan =
            signed_aggregate_state_merge(Some(ImvActionColumn::output_column(ColumnId(100))));

        validate(&plan).expect("signed aggregate inside AggregateStateMerge must validate");
    }

    #[test]
    fn validation_accepts_fan_in_delta_union_inside_state_merge() {
        let plan = signed_aggregate_state_merge_over(fan_in_delta_union(ColumnId(100)));

        validate(&plan).expect("fan-in delta union inside AggregateStateMerge must validate");
    }

    #[test]
    fn validation_accepts_recursive_join_delta_union_inside_state_merge() {
        let nested_delta_side = recursive_join_delta_union(ColumnId(100));
        let version_side = join_plan(
            normalized_version_project(ColumnId(20)),
            normalized_version_project(ColumnId(30)),
        );
        let plan = signed_aggregate_state_merge_over(join_plan(nested_delta_side, version_side));

        validate(&plan)
            .expect("recursive join-delta union inside AggregateStateMerge must validate");
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
        let PlanNodeKind::AggregateStateMerge(node) = &mut plan.kind else {
            unreachable!();
        };
        node.output_columns = vec![ImvActionColumn::output_column(ColumnId(200))];

        let err = validate(&plan).expect_err("internal-only state merge output must fail");
        assert!(err.contains("no user-visible output"), "got: {err}");
    }
}
