//! Shared shape predicates for rewritten IMV join-delta branches.

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, ProjectItem};
use crate::sql::catalog::ScanSource;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::plan::{LogicalPlanNode, PlanNodeKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JoinDeltaOrientation {
    LeftDelta,
    RightDelta,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct JoinDeltaBranchShape {
    orientation: JoinDeltaOrientation,
    action_column_id: ColumnId,
}

pub(crate) fn is_supported_join_delta_union(plan: &LogicalPlanNode) -> bool {
    supported_join_delta_union_action_column_id(plan).is_some()
}

fn supported_join_delta_union_action_column_id(plan: &LogicalPlanNode) -> Option<ColumnId> {
    let PlanNodeKind::Union(node) = &plan.kind else {
        return None;
    };
    if !node.all || plan.children.len() != 2 {
        return None;
    }
    let action_column_id = unique_action_output_column_id(&node.output_columns)?;
    let left = normalized_join_delta_branch_shape(&plan.children[0], action_column_id)?;
    let right = normalized_join_delta_branch_shape(&plan.children[1], action_column_id)?;
    match (left.orientation, right.orientation) {
        (JoinDeltaOrientation::LeftDelta, JoinDeltaOrientation::RightDelta)
        | (JoinDeltaOrientation::RightDelta, JoinDeltaOrientation::LeftDelta) => {
            Some(action_column_id)
        }
        _ => None,
    }
}

pub(crate) fn is_supported_join_delta_branch(plan: &LogicalPlanNode) -> bool {
    supported_join_delta_branch_shape(plan).is_some()
}

fn normalized_join_delta_branch_shape(
    plan: &LogicalPlanNode,
    action_column_id: ColumnId,
) -> Option<JoinDeltaBranchShape> {
    match &plan.kind {
        PlanNodeKind::Project(project) => {
            let projected_action_id = unique_project_action_column_id(&project.items)?;
            if projected_action_id != action_column_id {
                return None;
            }
            let shape = supported_join_delta_branch_shape(plan.unary_input())?;
            (shape.action_column_id == action_column_id).then_some(shape)
        }
        _ => None,
    }
}

#[cfg(test)]
fn is_supported_normalized_join_delta_branch(
    plan: &LogicalPlanNode,
    action_column_id: ColumnId,
) -> bool {
    normalized_join_delta_branch_shape(plan, action_column_id).is_some()
}

fn supported_join_delta_branch_shape(plan: &LogicalPlanNode) -> Option<JoinDeltaBranchShape> {
    let PlanNodeKind::Join(node) = &plan.kind else {
        return None;
    };
    if !matches!(node.join_type, JoinKind::Inner | JoinKind::Cross) {
        return None;
    }
    let left_delta_action = join_delta_delta_action_column_id(plan.left());
    let right_delta_action = join_delta_delta_action_column_id(plan.right());
    let left_version = is_join_delta_version_like(plan.left());
    let right_version = is_join_delta_version_like(plan.right());
    match (
        left_delta_action,
        right_delta_action,
        left_version,
        right_version,
    ) {
        (Some(action_column_id), None, false, true) => Some(JoinDeltaBranchShape {
            orientation: JoinDeltaOrientation::LeftDelta,
            action_column_id,
        }),
        (None, Some(action_column_id), true, false) => Some(JoinDeltaBranchShape {
            orientation: JoinDeltaOrientation::RightDelta,
            action_column_id,
        }),
        _ => None,
    }
}

fn unique_action_output_column_id(columns: &[OutputColumn]) -> Option<ColumnId> {
    let mut found = None;
    let mut action_name_count = 0usize;
    for column in columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
    {
        action_name_count += 1;
        found = Some(valid_action_output_column_id(column)?);
    }
    (action_name_count == 1).then_some(found).flatten()
}

fn valid_action_output_column_id(column: &OutputColumn) -> Option<ColumnId> {
    (column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)
        && column.is_internal
        && column.data_type == DataType::Int8
        && !column.nullable)
        .then_some(column.column_id)
}

fn has_reserved_action_output_name(column: &OutputColumn) -> bool {
    column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)
}

fn unique_project_action_column_id(items: &[ProjectItem]) -> Option<ColumnId> {
    let mut found = None;
    let mut action_name_count = 0usize;
    for item in items
        .iter()
        .filter(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
    {
        action_name_count += 1;
        found = Some(valid_project_action_column_id(item)?);
    }
    (action_name_count == 1).then_some(found).flatten()
}

fn valid_project_action_column_id(item: &ProjectItem) -> Option<ColumnId> {
    if !item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME) {
        return None;
    }
    let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind else {
        return None;
    };
    (*column_id == item.output_column_id
        && item.expr.data_type == DataType::Int8
        && !item.expr.nullable)
        .then_some(item.output_column_id)
}

fn has_reserved_action_project_output_name(item: &ProjectItem) -> bool {
    item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)
}

fn join_delta_delta_action_column_id(plan: &LogicalPlanNode) -> Option<ColumnId> {
    match &plan.kind {
        PlanNodeKind::Scan(scan)
            if matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. }) =>
        {
            unique_action_output_column_id(&scan.columns)
        }
        PlanNodeKind::Filter(_) => join_delta_delta_action_column_id(plan.unary_input()),
        PlanNodeKind::Project(project) => {
            let input_action_id = join_delta_delta_action_column_id(plan.unary_input())?;
            let projected_action_id = unique_project_action_column_id(&project.items)?;
            (projected_action_id == input_action_id).then_some(input_action_id)
        }
        PlanNodeKind::Join(_) => {
            supported_join_delta_branch_shape(plan).map(|shape| shape.action_column_id)
        }
        PlanNodeKind::Union(_) => supported_join_delta_union_action_column_id(plan),
        PlanNodeKind::ImvDelta(_) => join_delta_delta_action_column_id(plan.unary_input()),
        _ => None,
    }
}

fn is_join_delta_version_like(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergVersionTable { .. })
                && !scan.columns.iter().any(has_reserved_action_output_name)
        }
        PlanNodeKind::Filter(_) => is_join_delta_version_like(plan.unary_input()),
        PlanNodeKind::Project(node) => {
            !node
                .items
                .iter()
                .any(has_reserved_action_project_output_name)
                && is_join_delta_version_like(plan.unary_input())
        }
        PlanNodeKind::Join(node) => {
            matches!(node.join_type, JoinKind::Inner | JoinKind::Cross)
                && is_join_delta_version_like(plan.left())
                && is_join_delta_version_like(plan.right())
        }
        PlanNodeKind::ImvVersion(_) => {
            is_supported_marker_input(plan.unary_input())
                && !subtree_has_delta_marker_or_scan(plan.unary_input())
                && !subtree_has_reserved_action_output(plan.unary_input())
        }
        _ => false,
    }
}

fn subtree_has_reserved_action_output(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Scan(scan) => scan.columns.iter().any(has_reserved_action_output_name),
        PlanNodeKind::Filter(_) => subtree_has_reserved_action_output(plan.unary_input()),
        PlanNodeKind::Project(node) => {
            node.items
                .iter()
                .any(has_reserved_action_project_output_name)
                || subtree_has_reserved_action_output(plan.unary_input())
        }
        PlanNodeKind::Aggregate(_) => subtree_has_reserved_action_output(plan.unary_input()),
        PlanNodeKind::AggregateStateMerge(_) | PlanNodeKind::Join(_) => {
            plan.children.iter().any(subtree_has_reserved_action_output)
        }
        PlanNodeKind::Union(node) => {
            node.output_columns
                .iter()
                .any(has_reserved_action_output_name)
                || plan.children.iter().any(subtree_has_reserved_action_output)
        }
        PlanNodeKind::ImvDelta(_) | PlanNodeKind::ImvVersion(_) => {
            subtree_has_reserved_action_output(plan.unary_input())
        }
        _ => false,
    }
}

fn subtree_has_delta_marker_or_scan(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
        }
        PlanNodeKind::ImvDelta(_) => true,
        PlanNodeKind::Filter(_)
        | PlanNodeKind::Project(_)
        | PlanNodeKind::Aggregate(_)
        | PlanNodeKind::AggregateStateMerge(_)
        | PlanNodeKind::Join(_)
        | PlanNodeKind::Union(_)
        | PlanNodeKind::ImvVersion(_) => plan.children.iter().any(subtree_has_delta_marker_or_scan),
        _ => false,
    }
}

fn is_supported_marker_input(plan: &LogicalPlanNode) -> bool {
    match &plan.kind {
        PlanNodeKind::Scan(_) => true,
        PlanNodeKind::Filter(_) | PlanNodeKind::Project(_) => {
            is_supported_marker_input(plan.unary_input())
        }
        PlanNodeKind::Join(node) => {
            matches!(node.join_type, JoinKind::Inner | JoinKind::Cross)
                && is_supported_marker_input(plan.left())
                && is_supported_marker_input(plan.right())
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::planner::plan::*;
    use arrow::datatypes::DataType;

    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::{LogicalProjectNode, LogicalScanNode, PlanNodeKind};

    fn table_info(table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: table.to_string(),
            table_uuid: Some(format!("uuid-{table}")),
            current_snapshot_id: Some(22),
            schema_id: 7,
            location: format!("file:///tmp/ice/db/{table}"),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn scan(table: &str, column_id: ColumnId, source: ScanSource) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: table.to_string(),
                    columns: vec![ColumnDef {
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source,
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id,
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
            }),
            vec![],
            None,
        )
    }

    fn delta_scan(table: &str, column_id: ColumnId) -> LogicalPlanNode {
        scan(
            table,
            column_id,
            ScanSource::IcebergDeltaTable {
                table: table_info(table),
                from_snapshot_id: 11,
                to_snapshot_id: 22,
            },
        )
    }

    fn delta_scan_with_action(
        table: &str,
        column_id: ColumnId,
        action_id: ColumnId,
    ) -> LogicalPlanNode {
        let mut plan = delta_scan(table, column_id);
        let PlanNodeKind::Scan(scan) = &mut plan.kind else {
            unreachable!();
        };
        scan.columns.push(ImvActionColumn::output_column(action_id));
        plan
    }

    fn version_scan(table: &str, column_id: ColumnId) -> LogicalPlanNode {
        scan(
            table,
            column_id,
            ScanSource::IcebergVersionTable {
                table: table_info(table),
                snapshot_id: 22,
            },
        )
    }

    fn version_scan_with_reserved_action_output(
        table: &str,
        column_id: ColumnId,
        action_id: ColumnId,
    ) -> LogicalPlanNode {
        let mut plan = version_scan(table, column_id);
        let PlanNodeKind::Scan(scan) = &mut plan.kind else {
            unreachable!();
        };
        scan.columns.push(OutputColumn {
            column_id: action_id,
            name: ImvActionColumn::NAME.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        });
        plan
    }

    fn column_ref_item(column_id: ColumnId, name: &str, data_type: DataType) -> ProjectItem {
        ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id,
                    qualifier: None,
                    column: name.to_string(),
                },
                data_type,
                nullable: false,
            },
            output_name: name.to_string(),
            output_column_id: column_id,
        }
    }

    fn fake_action_item(action_id: ColumnId) -> ProjectItem {
        ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(1)),
                data_type: DataType::Int8,
                nullable: false,
            },
            output_name: ImvActionColumn::NAME.to_string(),
            output_column_id: action_id,
        }
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

    fn project_with_items(input: LogicalPlanNode, items: Vec<ProjectItem>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: items,
                output_qualifier: None,
            }),
            vec![input],
            None,
        )
    }

    fn normalized_join_delta_branch(
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        action_item: ProjectItem,
    ) -> LogicalPlanNode {
        project_with_items(
            join_plan(left, right),
            vec![
                column_ref_item(ColumnId(1), "k", DataType::Int64),
                action_item,
            ],
        )
    }

    fn left_delta_branch(action_id: ColumnId) -> LogicalPlanNode {
        normalized_join_delta_branch(
            delta_scan_with_action("a", ColumnId(1), action_id),
            version_scan("b", ColumnId(10)),
            column_ref_item(action_id, ImvActionColumn::NAME, DataType::Int8),
        )
    }

    fn right_delta_branch(action_id: ColumnId) -> LogicalPlanNode {
        normalized_join_delta_branch(
            version_scan("a", ColumnId(1)),
            delta_scan_with_action("b", ColumnId(10), action_id),
            column_ref_item(action_id, ImvActionColumn::NAME, DataType::Int8),
        )
    }

    fn join_delta_union_with_inputs(
        inputs: Vec<LogicalPlanNode>,
        output_columns: Vec<OutputColumn>,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Union(LogicalUnionNode {
                all: true,
                output_columns,
            }),
            inputs,
            None,
        )
    }

    fn join_delta_union(output_columns: Vec<OutputColumn>) -> LogicalPlanNode {
        join_delta_union_with_inputs(
            vec![
                left_delta_branch(ColumnId(100)),
                right_delta_branch(ColumnId(100)),
            ],
            output_columns,
        )
    }

    #[test]
    fn rejects_normalized_branch_with_fake_action_project_item() {
        let branch = normalized_join_delta_branch(
            delta_scan("a", ColumnId(1)),
            version_scan("b", ColumnId(10)),
            fake_action_item(ColumnId(100)),
        );

        assert!(
            !is_supported_normalized_join_delta_branch(&branch, ColumnId(100)),
            "fake __change_op Project item must not mark a join-delta branch supported"
        );
    }

    #[test]
    fn rejects_join_delta_union_missing_action_output_column() {
        let union = join_delta_union(vec![OutputColumn {
            column_id: ColumnId(1),
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }]);

        assert!(
            !is_supported_join_delta_union(&union),
            "join-delta Union must expose a valid action output column"
        );
    }

    #[test]
    fn rejects_join_delta_union_with_malformed_action_output_column() {
        let union = join_delta_union(vec![
            OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            },
            OutputColumn {
                column_id: ColumnId(100),
                name: ImvActionColumn::NAME.to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: true,
            },
        ]);

        assert!(
            !is_supported_join_delta_union(&union),
            "join-delta Union action output must be internal Int8 non-null"
        );
    }

    #[test]
    fn rejects_join_delta_union_with_branch_action_id_mismatch() {
        let union = join_delta_union(vec![
            OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            },
            ImvActionColumn::output_column(ColumnId(101)),
        ]);

        assert!(
            !is_supported_join_delta_union(&union),
            "join-delta Union action output id must match branch action ids"
        );
    }

    #[test]
    fn rejects_one_branch_join_delta_union() {
        let union = join_delta_union_with_inputs(
            vec![left_delta_branch(ColumnId(100))],
            vec![ImvActionColumn::output_column(ColumnId(100))],
        );

        assert!(
            !is_supported_join_delta_union(&union),
            "join-delta Union must contain both left-delta and right-delta branches"
        );
    }

    #[test]
    fn rejects_duplicate_same_side_join_delta_union_branches() {
        let union = join_delta_union_with_inputs(
            vec![
                left_delta_branch(ColumnId(100)),
                left_delta_branch(ColumnId(100)),
            ],
            vec![ImvActionColumn::output_column(ColumnId(100))],
        );

        assert!(
            !is_supported_join_delta_union(&union),
            "join-delta Union must contain one branch for each delta orientation"
        );
    }

    #[test]
    fn rejects_branch_action_not_produced_by_delta_side() {
        let union = join_delta_union_with_inputs(
            vec![
                normalized_join_delta_branch(
                    delta_scan_with_action("a", ColumnId(1), ColumnId(100)),
                    version_scan("b", ColumnId(10)),
                    column_ref_item(ColumnId(101), ImvActionColumn::NAME, DataType::Int8),
                ),
                right_delta_branch(ColumnId(101)),
            ],
            vec![ImvActionColumn::output_column(ColumnId(101))],
        );

        assert!(
            !is_supported_join_delta_union(&union),
            "branch action id must be produced by the delta-like side"
        );
    }

    #[test]
    fn rejects_version_side_project_with_reserved_action_output_name() {
        let version_side = project_with_items(
            version_scan("b", ColumnId(10)),
            vec![
                column_ref_item(ColumnId(10), "k", DataType::Int64),
                fake_action_item(ColumnId(200)),
            ],
        );
        let union = join_delta_union_with_inputs(
            vec![
                normalized_join_delta_branch(
                    delta_scan_with_action("a", ColumnId(1), ColumnId(100)),
                    version_side,
                    column_ref_item(ColumnId(100), ImvActionColumn::NAME, DataType::Int8),
                ),
                right_delta_branch(ColumnId(100)),
            ],
            vec![ImvActionColumn::output_column(ColumnId(100))],
        );

        assert!(
            !is_supported_join_delta_union(&union),
            "version-like side must reject reserved __change_op Project outputs"
        );
    }

    #[test]
    fn rejects_version_side_scan_with_reserved_action_output_name() {
        let union = join_delta_union_with_inputs(
            vec![
                normalized_join_delta_branch(
                    delta_scan_with_action("a", ColumnId(1), ColumnId(100)),
                    version_scan_with_reserved_action_output("b", ColumnId(10), ColumnId(200)),
                    column_ref_item(ColumnId(100), ImvActionColumn::NAME, DataType::Int8),
                ),
                right_delta_branch(ColumnId(100)),
            ],
            vec![ImvActionColumn::output_column(ColumnId(100))],
        );

        assert!(
            !is_supported_join_delta_union(&union),
            "version-like side must reject reserved __change_op Scan outputs"
        );
    }

    #[test]
    fn rejects_join_delta_union_with_duplicate_action_outputs() {
        let union = join_delta_union(vec![
            ImvActionColumn::output_column(ColumnId(100)),
            ImvActionColumn::output_column(ColumnId(100)),
        ]);

        assert!(
            !is_supported_join_delta_union(&union),
            "join-delta Union must expose exactly one action output"
        );
    }
}
