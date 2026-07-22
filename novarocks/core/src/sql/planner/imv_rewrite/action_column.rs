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

//! IMV action column descriptor.
//!
//! The action column is an optimizer-internal `Int8` non-nullable column
//! produced by `InjectActionColumnRule` on Delta-bound scans. It carries
//! `+1` for inserts/upserts and `-1` for deletes at runtime, and is never
//! exposed to user-visible output.

use std::sync::atomic::AtomicBool;

use arrow::datatypes::DataType;

use crate::mv::persistence::schema::{HIDDEN_APPLY_KEY_COLUMN_NAME, JOIN_APPLY_KEY_COLUMN_NAME};
use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_propagation::{
    first_delta_base_fqn, is_supported_fan_in_delta_union,
};
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor;
use crate::sql::planner::imv_rewrite::join_delta_shape::{
    is_supported_join_delta_branch, is_supported_join_delta_union,
};
use crate::sql::planner::imv_rewrite::opt_expr_to_plan;
use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
use crate::sql::planner::imv_rewrite::target_locator::is_target_locator_join;
use crate::sql::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;
use novarocks_catalog::schema::ColumnDef;

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

    pub(crate) fn ensure_metadata_column(columns: &mut Vec<ColumnDef>) {
        if columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(Self::NAME))
        {
            return;
        }
        columns.push(ColumnDef {
            name: Self::NAME.to_string(),
            data_type: DataType::Int8,
            nullable: false,
            write_default: None,
            logical_type: None,
        });
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
        let change_stream = ctx
            .extension::<ImvExtension>()
            .map(|ext| ext.annotation.change_stream.clone())
            .unwrap_or_default();
        match validate_with_change_stream(&plan, &change_stream) {
            Ok(()) => Ok(RewriteResult::Unchanged),
            Err(message) => Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                "ActionColumnValidation",
                message,
            ))),
        }
    }
}

fn validate(plan: &LogicalPlanNode) -> Result<(), String> {
    validate_with_change_stream(plan, &ImvChangeStreamDescriptor::default())
}

fn validate_with_change_stream(
    plan: &LogicalPlanNode,
    change_stream: &ImvChangeStreamDescriptor,
) -> Result<(), String> {
    if change_stream.has_aggregate() {
        if !has_visible_output(plan) {
            return Err(
                "root plan has no user-visible output; action column or other internal column may have leaked"
                    .to_string(),
            );
        }
        return Ok(());
    }

    validate_node(plan)?;
    // V4: root visible output must not be empty
    if !has_visible_output(plan) {
        return Err(
            "root plan has no user-visible output; action column or other internal column may have leaked"
                .to_string(),
        );
    }
    // V6: if a delta subtree exists, root output must carry the apply key.
    if contains_join_delta_union(plan) {
        if !output_has_join_apply_key(plan) {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            return Err(format!(
                "join refresh plan above delta-bound scan {fqn} is missing join apply-key column \
                 {JOIN_APPLY_KEY_COLUMN_NAME}"
            ));
        }
    } else if subtree_has_delta(plan) && !output_has_apply_key(plan) {
        let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
        return Err(format!(
            "plan above delta-bound scan {fqn} is missing apply key column \
             {HIDDEN_APPLY_KEY_COLUMN_NAME}"
        ));
    }
    Ok(())
}

// Markers (ImvDelta/ImvVersion) are guaranteed absent here because
// UnresolvedMarkerCheckRule precedes ActionColumnValidation in the
// imv-validation stage and rejects any surviving marker.
fn validate_node(plan: &LogicalPlanNode) -> Result<(), String> {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => validate_scan(scan),
        LogicalPlanKind::Filter(_) => validate_node(plan.unary_input()),
        LogicalPlanKind::Project(node) => {
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
        LogicalPlanKind::Aggregate(_) if subtree_has_delta(plan) => {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Iceberg IMV rewrite does not support this aggregate shape above delta-bound scan {fqn}"
            ))
        }
        LogicalPlanKind::Join(_) if is_target_locator_join(plan) => {
            validate_node(plan.left())?;
            validate_node(plan.right())
        }
        LogicalPlanKind::Join(_) if subtree_has_delta(plan) => {
            let fqn = first_delta_base_fqn(plan).unwrap_or_else(|| "<unknown>".to_string());
            Err(format!(
                "Iceberg IMV rewrite does not support this join shape above delta-bound scan {fqn}"
            ))
        }
        LogicalPlanKind::Union(_) if is_supported_fan_in_delta_union(plan) => {
            for input in &plan.children {
                validate_node(input)?;
            }
            Ok(())
        }
        LogicalPlanKind::Union(_) if is_supported_join_delta_union(plan) => {
            for input in &plan.children {
                validate_signed_delta_input(input)?;
            }
            Ok(())
        }
        LogicalPlanKind::Union(_)
            if subtree_has_delta(plan)
                && !is_supported_join_delta_union(plan)
                && !is_supported_fan_in_delta_union(plan) =>
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

fn validate_signed_delta_input(plan: &LogicalPlanNode) -> Result<(), String> {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => validate_scan(scan),
        LogicalPlanKind::Filter(_) => validate_signed_delta_input(plan.unary_input()),
        LogicalPlanKind::Project(node) => {
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
        LogicalPlanKind::Join(_) if is_supported_join_delta_branch(plan) => {
            validate_signed_delta_input(plan.left())?;
            validate_signed_delta_input(plan.right())
        }
        LogicalPlanKind::Union(_)
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

fn validate_scan(scan: &PlanScanNode) -> Result<(), String> {
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
        LogicalPlanKind::Project(p) => p.items.iter().any(|i| {
            i.output_name
                .eq_ignore_ascii_case(HIDDEN_APPLY_KEY_COLUMN_NAME)
        }),
        LogicalPlanKind::Filter(_) => output_has_apply_key(plan.unary_input()),
        _ => false,
    }
}

fn output_has_join_apply_key(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Project(p) => p.items.iter().any(|i| {
            i.output_name
                .eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)
        }),
        LogicalPlanKind::Union(u) => u
            .output_columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)),
        LogicalPlanKind::Filter(_) => output_has_join_apply_key(plan.unary_input()),
        _ => false,
    }
}

fn contains_join_delta_union(plan: &LogicalPlanNode) -> bool {
    is_supported_join_delta_union(plan) || plan.children.iter().any(contains_join_delta_union)
}

fn subtree_has_delta(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
        }
        _ => plan.children.iter().any(subtree_has_delta),
    }
}

fn has_visible_output(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => scan.columns.iter().any(|c| !c.is_internal),
        LogicalPlanKind::Filter(_) => has_visible_output(plan.unary_input()),
        LogicalPlanKind::Project(node) => node.items.iter().any(|item| {
            !item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)
                && !item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME)
                && !item
                    .output_name
                    .eq_ignore_ascii_case(HIDDEN_APPLY_KEY_COLUMN_NAME)
                && !item
                    .output_name
                    .eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)
        }),
        LogicalPlanKind::Aggregate(node) => node.output_columns.iter().any(|c| !c.is_internal),
        LogicalPlanKind::Join(_) => {
            has_visible_output(plan.left()) || has_visible_output(plan.right())
        }
        LogicalPlanKind::Union(_) => plan.children.iter().any(has_visible_output),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::sql::analysis::{ExprKind, ProjectItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::imv_rewrite::change_stream::{
        AggregateChangeStreamDescriptor, AggregateChangeStreamShape, ImvChangeStreamDescriptor,
        SignedStateAggregateProof, TargetStateProof,
    };
    use crate::sql::planner::logical::*;
    use crate::sql::planner::logical::{LogicalPlanKind, LogicalUnionNode};
    use crate::sql::planner::payload::PlanProjectNode;
    use crate::sql::planner::payload::*;
    use crate::sql::planner::table::TableDef;
    use novarocks_catalog::schema::ColumnDef;

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

    fn delta_scan_with(action: Option<OutputColumn>) -> PlanScanNode {
        let mut scan = PlanScanNode {
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
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        };
        if let Some(a) = action {
            scan.columns.push(a);
        }
        scan
    }

    fn scan_plan(scan: PlanScanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(LogicalPlanKind::Scan(scan), vec![], None)
    }

    #[test]
    fn validation_passes_on_well_formed_delta_scan() {
        // Scan with k + action + _row_id, root Project carrying all three plus
        // the apply key. This is the shape the IMV pipeline produces.
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        let project = LogicalPlanNode::new(
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
                        output_name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
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
            LogicalPlanKind::Project(PlanProjectNode {
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

    fn version_scan_with_action() -> PlanScanNode {
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
        use crate::sql::planner::logical::LogicalAggregateNode;
        let scan = scan_plan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
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
        use crate::sql::planner::logical::LogicalJoinNode;
        let left = scan_plan(delta_scan_with(Some(ImvActionColumn::output_column(
            ColumnId(100),
        ))));
        let right = scan_plan(delta_scan_with(None));
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
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
            LogicalPlanKind::Union(LogicalUnionNode {
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
            LogicalPlanKind::Project(PlanProjectNode {
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
            LogicalPlanKind::Project(PlanProjectNode {
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

    fn fan_in_delta_union(action_id: ColumnId) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
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

    fn root_project_with_apply_key(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
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
                        HIDDEN_APPLY_KEY_COLUMN_NAME,
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

    // ── V6 / V7 failing tests (RED phase) ───────────────────────────────────

    /// Helper: delta scan with both a valid action column and `_row_id`.
    fn delta_scan_with_action_and_row_id() -> PlanScanNode {
        let mut scan = delta_scan_with(Some(ImvActionColumn::output_column(ColumnId(100))));
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        scan
    }

    fn project_without_apply_key_above_delta() -> LogicalPlanNode {
        // Project carries k + __change_op + _row_id but NOT __nova_base_row_id.
        let scan = delta_scan_with_action_and_row_id();
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
        )
    }

    #[test]
    fn validation_rejects_missing_apply_key_above_delta() {
        let project = project_without_apply_key_above_delta();
        let err = validate(&project).expect_err("missing apply key must fail");
        assert!(err.contains("apply key"), "got: {err}");
        assert!(err.contains("ice.db.b"), "got: {err}");
    }

    #[test]
    fn validation_uses_descriptor_for_aggregate_change_stream_bypass() {
        let project = project_without_apply_key_above_delta();
        let descriptor = ImvChangeStreamDescriptor {
            aggregate: Some(AggregateChangeStreamDescriptor {
                action_column_id: ColumnId(100),
                action_column_name: ImvActionColumn::NAME.to_string(),
                shape: AggregateChangeStreamShape::RelationalChangeStream,
                target_state: TargetStateProof { present: false },
                signed_state_aggregate: SignedStateAggregateProof { present: false },
            }),
            ..Default::default()
        };

        validate_with_change_stream(&project, &descriptor)
            .expect("aggregate change-stream descriptor should own this semantic bypass");
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
}
