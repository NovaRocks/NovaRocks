//! IMV action column injection and propagation rules.
//!
//! Phase 2: Delta-bound scans get an internal `__change_op` Int8
//! non-nullable column. Project transparently carries **all** internal columns
//! (including `_row_id` added in Task 2, and any future internal column).
//! Filter is a schema-passthrough node and requires no work.
//! Unsupported Join/UnionAll/Aggregate shapes above a Delta scan fail fast;
//! recognized IMV delta algebra rewrites are accepted by shape-specific
//! predicates.

use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN;
use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::catalog::ScanSource;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{AggregateNode, LogicalPlan};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns true iff the plan's effective output schema contains the IMV
/// action column. Used by `matches()` predicates and validation.
pub(crate) fn output_has_action_column(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => scan.columns.iter().any(ImvActionColumn::matches),
        LogicalPlan::Filter(node) => output_has_action_column(&node.input),
        LogicalPlan::Project(node) => {
            // NOTE: ProjectItem carries no `is_internal` flag (unlike
            // OutputColumn), so we can only detect the propagated action
            // column by its reserved name `__change_op`. Phase 2 assumes no
            // user-visible projection legitimately uses this name; the
            // analyzer does not yet reject it. Task 8's validation (V4)
            // backstops by rejecting internal columns leaking to visible
            // output. Revisit if MV definitions ever expose `__change_op`.
            node.items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
        }
        LogicalPlan::ImvDelta(node) => output_has_action_column(&node.input),
        LogicalPlan::ImvVersion(node) => output_has_action_column(&node.input),
        LogicalPlan::AggregateStateMerge(node) => {
            output_has_action_column(&node.old_input) || output_has_action_column(&node.delta_input)
        }
        _ => false,
    }
}

/// Returns the action column descriptor from the first descendant Scan/Project
/// in the subtree that exposes one, or `None` if no descendant carries it.
pub(crate) fn find_action_column(plan: &LogicalPlan) -> Option<OutputColumn> {
    match plan {
        LogicalPlan::Scan(scan) => scan
            .columns
            .iter()
            .find(|c| ImvActionColumn::matches(c))
            .cloned(),
        LogicalPlan::Filter(node) => find_action_column(&node.input),
        LogicalPlan::Project(node) => find_action_column(&node.input),
        LogicalPlan::AggregateStateMerge(node) => {
            find_action_column(&node.old_input).or_else(|| find_action_column(&node.delta_input))
        }
        _ => None,
    }
}

/// Whether any descendant of the plan exposes an action column.
pub(crate) fn subtree_has_action_column(plan: &LogicalPlan) -> bool {
    output_has_action_column(plan)
        || match plan {
            LogicalPlan::Filter(node) => subtree_has_action_column(&node.input),
            LogicalPlan::Project(node) => subtree_has_action_column(&node.input),
            LogicalPlan::Aggregate(node) => subtree_has_action_column(&node.input),
            LogicalPlan::AggregateStateMerge(node) => {
                subtree_has_action_column(&node.old_input)
                    || subtree_has_action_column(&node.delta_input)
            }
            LogicalPlan::Join(node) => {
                subtree_has_action_column(&node.left) || subtree_has_action_column(&node.right)
            }
            LogicalPlan::Union(node) => node.inputs.iter().any(subtree_has_action_column),
            _ => false,
        }
}

/// Returns the fully-qualified name of the first `IcebergDeltaTable`-backed
/// scan found anywhere in the subtree, for use in fail-fast diagnostics.
/// Recurses through every child-bearing variant (unlike the action-column
/// helpers, which only need Scan/Filter/Project), because an unsupported
/// Join/Union/Aggregate node's delta scan can sit under any branch.
pub(crate) fn first_delta_base_fqn(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan(scan) => match &scan.table.source {
            ScanSource::IcebergDeltaTable { table, .. } => Some(format!(
                "{}.{}.{}",
                table.catalog, table.namespace, table.table
            )),
            _ => None,
        },
        LogicalPlan::Filter(node) => first_delta_base_fqn(&node.input),
        LogicalPlan::Project(node) => first_delta_base_fqn(&node.input),
        LogicalPlan::Aggregate(node) => first_delta_base_fqn(&node.input),
        LogicalPlan::AggregateStateMerge(node) => first_delta_base_fqn(&node.old_input)
            .or_else(|| first_delta_base_fqn(&node.delta_input)),
        LogicalPlan::Join(node) => {
            first_delta_base_fqn(&node.left).or_else(|| first_delta_base_fqn(&node.right))
        }
        LogicalPlan::Union(node) => node.inputs.iter().find_map(first_delta_base_fqn),
        LogicalPlan::ImvDelta(node) => first_delta_base_fqn(&node.input),
        LogicalPlan::ImvVersion(node) => first_delta_base_fqn(&node.input),
        _ => None,
    }
}

/// Collect every internal (`is_internal`) output column exposed by the first
/// descendant Scan, threaded up through Filter/Project. Used by the generalized
/// propagation rule to carry `__change_op`, `_row_id`, and any future internal
/// column through the unary chain.
pub(crate) fn descendant_internal_columns(plan: &LogicalPlan) -> Vec<OutputColumn> {
    match plan {
        LogicalPlan::Scan(scan) => scan
            .columns
            .iter()
            .filter(|c| c.is_internal)
            .cloned()
            .collect(),
        LogicalPlan::Filter(node) => descendant_internal_columns(&node.input),
        LogicalPlan::Project(node) => descendant_internal_columns(&node.input),
        LogicalPlan::AggregateStateMerge(node) => {
            let mut columns = descendant_internal_columns(&node.old_input);
            columns.extend(descendant_internal_columns(&node.delta_input));
            columns
        }
        _ => Vec::new(),
    }
}

fn is_signed_state_aggregate(node: &AggregateNode) -> bool {
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

// ---------------------------------------------------------------------------
// InjectActionColumnRule
// ---------------------------------------------------------------------------

// Registered into the IMV rewrite pipeline's `imv-action-propagation` stage.
pub(crate) struct InjectActionColumnRule;

impl LogicalRewriteRule for InjectActionColumnRule {
    fn name(&self) -> &'static str {
        "InjectActionColumn"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        match plan {
            LogicalPlan::Scan(scan) => {
                matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
                    && !scan.columns.iter().any(ImvActionColumn::matches)
            }
            _ => false,
        }
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Scan(mut scan) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let ext = ctx.extension::<ImvExtension>().ok_or_else(|| {
            "InjectActionColumn requires ImvExtension in RewriteContext".to_string()
        })?;
        let column_id = ext.allocate_column_id();
        scan.columns.push(ImvActionColumn::output_column(column_id));
        Ok(RewriteResult::Changed(LogicalPlan::Scan(scan)))
    }
}

// ---------------------------------------------------------------------------
// PropagateActionColumnRule
// ---------------------------------------------------------------------------

// Registered into the IMV rewrite pipeline's `imv-action-propagation` stage.
pub(crate) struct PropagateActionColumnRule;

impl LogicalRewriteRule for PropagateActionColumnRule {
    fn name(&self) -> &'static str {
        "PropagateActionColumn"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::BottomUp
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        match plan {
            LogicalPlan::Project(p) => {
                let internal = descendant_internal_columns(&p.input);
                !internal.is_empty()
                    && internal.iter().any(|c| {
                        !p.items
                            .iter()
                            .any(|item| item.output_name.eq_ignore_ascii_case(&c.name))
                    })
            }
            // Filter is a schema-passthrough node: it exposes its child's
            // schema verbatim, so once the child has the action column the
            // Filter's effective output also has it. No work needed.
            LogicalPlan::Filter(_) => false,
            // Unsupported Aggregate / Join / Union shapes above a delta subtree
            // match here so `apply` can fail-fast with a clear error.
            LogicalPlan::Aggregate(a) => {
                subtree_has_action_column(&a.input) && !is_signed_state_aggregate(a)
            }
            LogicalPlan::Join(j) => {
                (subtree_has_action_column(&j.left) || subtree_has_action_column(&j.right))
                    && !is_supported_join_delta_branch(j)
            }
            LogicalPlan::Union(u) => {
                if branch_delta_union_needs_row_id_output(u) {
                    true
                } else {
                    u.inputs.iter().any(subtree_has_action_column)
                        && !is_supported_join_delta_union(u)
                        && !is_supported_fan_in_delta_union(u)
                }
            }
            _ => false,
        }
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // Diagnostic: the delta base under an unsupported node, if any. Computed
        // up-front from `&plan` so the fail-fast arms can name the offending
        // base table; harmless for the Project happy path.
        let base = first_delta_base_fqn(&plan).unwrap_or_else(|| "<unknown>".to_string());
        match plan {
            LogicalPlan::Project(mut p) => {
                let internal = descendant_internal_columns(&p.input);
                for col in internal {
                    let already = p
                        .items
                        .iter()
                        .any(|item| item.output_name.eq_ignore_ascii_case(&col.name));
                    if already {
                        continue;
                    }
                    p.items.push(ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: col.column_id,
                                qualifier: None,
                                column: col.name.clone(),
                            },
                            data_type: col.data_type.clone(),
                            nullable: col.nullable,
                        },
                        output_name: col.name.clone(),
                        output_column_id: col.column_id,
                    });
                }
                Ok(RewriteResult::Changed(LogicalPlan::Project(p)))
            }
            LogicalPlan::Aggregate(_) => Err(format!(
                "IMV action column propagation does not support Aggregate above \
                 delta-bound scan {base} in Phase 2; aggregate state rewrite is \
                 scheduled for Phase 4"
            )),
            LogicalPlan::Join(_) => Err(format!(
                "IMV action column propagation does not support Join above \
                 delta-bound scan {base} in Phase 2; join delta algebra is \
                 scheduled for Phase 5"
            )),
            LogicalPlan::Union(mut u) if branch_delta_union_needs_row_id_output(&u) => {
                let ext = ctx.extension::<ImvExtension>().ok_or_else(|| {
                    "PropagateActionColumn requires ImvExtension in RewriteContext".to_string()
                })?;
                let row_id_column = ext.allocate_column_id();
                for input in &mut u.inputs {
                    normalize_branch_row_id_output(input, row_id_column)?;
                }
                u.output_columns
                    .push(ImvRowIdColumn::output_column(row_id_column));
                Ok(RewriteResult::Changed(LogicalPlan::Union(u)))
            }
            LogicalPlan::Union(_) => Err(format!(
                "IMV action column propagation does not support UNION above \
                 delta-bound scan {base} in Phase 2; union delta rewrite is \
                 scheduled for Phase 6"
            )),
            _ => Ok(RewriteResult::Unchanged),
        }
    }
}

fn branch_delta_union_needs_row_id_output(node: &crate::sql::planner::plan::UnionNode) -> bool {
    is_branch_delta_union(node)
        && !node.output_columns.iter().any(ImvRowIdColumn::matches)
        && node.inputs.iter().all(branch_project_has_row_id)
}

fn is_branch_delta_union(node: &crate::sql::planner::plan::UnionNode) -> bool {
    is_supported_fan_in_delta_union(node)
        && node.output_columns.iter().any(|column| {
            column
                .name
                .eq_ignore_ascii_case(ICEBERG_MV_BRANCH_ID_COLUMN)
        })
}

fn branch_project_has_row_id(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Project(project)
            if project
                .items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
    )
}

fn normalize_branch_row_id_output(
    plan: &mut LogicalPlan,
    row_id_column: crate::sql::column_id::ColumnId,
) -> Result<(), String> {
    let LogicalPlan::Project(project) = plan else {
        return Err(
            "IMV branch UNION row-id propagation expected normalized Project branch".to_string(),
        );
    };
    let Some(item) = project
        .items
        .iter_mut()
        .find(|item| item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME))
    else {
        return Err("IMV branch UNION row-id propagation expected _row_id output".to_string());
    };
    item.output_column_id = row_id_column;
    item.output_name = ImvRowIdColumn::NAME.to_string();
    Ok(())
}

fn is_supported_join_delta_union(node: &crate::sql::planner::plan::UnionNode) -> bool {
    node.all
        && !node.inputs.is_empty()
        && node
            .inputs
            .iter()
            .all(is_supported_normalized_join_delta_branch)
}

pub(crate) fn is_supported_fan_in_delta_union(node: &crate::sql::planner::plan::UnionNode) -> bool {
    if !node.all || node.inputs.is_empty() {
        return false;
    }
    let Some(action_column_id) = branch_output_action_column_id(&node.inputs[0]) else {
        return false;
    };
    node.inputs.iter().all(|branch| {
        subtree_has_delta_scan(branch)
            && !subtree_has_version_scan(branch)
            && branch_output_action_column_id(branch) == Some(action_column_id)
    })
}

fn branch_output_action_column_id(plan: &LogicalPlan) -> Option<crate::sql::column_id::ColumnId> {
    match plan {
        LogicalPlan::Scan(scan) => scan
            .columns
            .iter()
            .find(|column| ImvActionColumn::matches(column))
            .map(|column| column.column_id),
        LogicalPlan::Filter(node) => branch_output_action_column_id(&node.input),
        LogicalPlan::Project(project) => project
            .items
            .iter()
            .find(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
            .and_then(|item| {
                let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind else {
                    return None;
                };
                (*column_id == item.output_column_id
                    && item.expr.data_type == arrow::datatypes::DataType::Int8
                    && !item.expr.nullable)
                    .then_some(item.output_column_id)
            }),
        LogicalPlan::ImvDelta(node) => branch_output_action_column_id(&node.input),
        LogicalPlan::ImvVersion(node) => branch_output_action_column_id(&node.input),
        _ => None,
    }
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
    let left_delta = subtree_has_delta_scan(left);
    let right_delta = subtree_has_delta_scan(right);
    let left_version = subtree_has_version_scan(left);
    let right_version = subtree_has_version_scan(right);
    ((left_delta && right_version) || (right_delta && left_version))
        && !(left_delta && right_delta)
        && !(left_version && right_version)
}

fn subtree_has_delta_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
        }
        LogicalPlan::Filter(node) => subtree_has_delta_scan(&node.input),
        LogicalPlan::Project(node) => subtree_has_delta_scan(&node.input),
        LogicalPlan::Join(node) => {
            subtree_has_delta_scan(&node.left) || subtree_has_delta_scan(&node.right)
        }
        LogicalPlan::Union(node) => node.inputs.iter().any(subtree_has_delta_scan),
        LogicalPlan::ImvDelta(_) => true,
        LogicalPlan::ImvVersion(node) => subtree_has_delta_scan(&node.input),
        _ => false,
    }
}

fn subtree_has_version_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => {
            matches!(scan.table.source, ScanSource::IcebergVersionTable { .. })
        }
        LogicalPlan::Filter(node) => subtree_has_version_scan(&node.input),
        LogicalPlan::Project(node) => subtree_has_version_scan(&node.input),
        LogicalPlan::Join(node) => {
            subtree_has_version_scan(&node.left) || subtree_has_version_scan(&node.right)
        }
        LogicalPlan::Union(node) => node.inputs.iter().any(subtree_has_version_scan),
        LogicalPlan::ImvDelta(node) => subtree_has_version_scan(&node.input),
        LogicalPlan::ImvVersion(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::analysis::{JoinKind, LiteralValue};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode;
    use crate::sql::planner::plan::{
        AggregateNode, FilterNode, JoinNode, LogicalPlan, ScanNode, UnionNode,
    };

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn delta_scan() -> ScanNode {
        ScanNode {
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
        }
    }

    fn version_scan() -> ScanNode {
        let mut s = delta_scan();
        s.table.source = ScanSource::IcebergVersionTable {
            table: match &delta_scan().table.source {
                ScanSource::IcebergDeltaTable { table, .. } => table.clone(),
                _ => unreachable!(),
            },
            snapshot_id: 22,
        };
        s
    }

    fn starrocks_scan() -> ScanNode {
        let mut s = delta_scan();
        s.table.source = ScanSource::StarRocks {
            db_id: 0,
            table_id: 0,
        };
        s
    }

    #[test]
    fn inject_action_column_on_delta_scan() {
        let rule = InjectActionColumnRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::Scan(delta_scan());
        assert!(rule.matches(&plan, &ctx));
        let result = rule.apply(plan, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(LogicalPlan::Scan(scan)) = result else {
            panic!("expected Changed(Scan), got {:?}", result);
        };
        let action = scan
            .columns
            .iter()
            .find(|c| ImvActionColumn::matches(c))
            .expect("action column must be present");
        assert_eq!(action.data_type, DataType::Int8);
        assert!(!action.nullable);
        assert!(action.is_internal);
        assert_eq!(action.column_id, ColumnId(100));
    }

    #[test]
    fn inject_does_not_touch_version_scan() {
        let rule = InjectActionColumnRule;
        let ctx = build_ctx();
        let plan = LogicalPlan::Scan(version_scan());
        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn inject_is_idempotent() {
        let rule = InjectActionColumnRule;
        let ctx = build_ctx();
        let mut scan = delta_scan();
        scan.columns
            .push(ImvActionColumn::output_column(ColumnId(9)));
        let plan = LogicalPlan::Scan(scan);
        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn inject_skips_starrocks_scan() {
        let rule = InjectActionColumnRule;
        let ctx = build_ctx();
        let plan = LogicalPlan::Scan(starrocks_scan());
        assert!(!rule.matches(&plan, &ctx));
    }

    use crate::sql::planner::plan::ProjectNode;

    fn project_over(input: LogicalPlan, projected_user_col_id: ColumnId) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: vec![ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: projected_user_col_id,
                        qualifier: None,
                        column: "k".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "k".to_string(),
                output_column_id: projected_user_col_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn delta_scan_with_action(action_id: ColumnId) -> ScanNode {
        let mut s = delta_scan();
        s.columns.push(ImvActionColumn::output_column(action_id));
        s
    }

    fn normalized_delta_project(action_id: ColumnId, user_col_id: ColumnId) -> LogicalPlan {
        let mut scan = delta_scan_with_action(action_id);
        scan.columns[0].column_id = user_col_id;
        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::ImvDelta(ImvDeltaNode {
                input: Box::new(LogicalPlan::Scan(scan)),
                is_root: false,
                action_column: Some(action_id),
            })),
            items: vec![
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: user_col_id,
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "k".to_string(),
                    output_column_id: user_col_id,
                },
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: action_id,
                            qualifier: None,
                            column: ImvActionColumn::NAME.to_string(),
                        },
                        data_type: DataType::Int8,
                        nullable: false,
                    },
                    output_name: ImvActionColumn::NAME.to_string(),
                    output_column_id: action_id,
                },
            ],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn normalized_delta_project_without_action(user_col_id: ColumnId) -> LogicalPlan {
        let mut scan = delta_scan();
        scan.columns[0].column_id = user_col_id;
        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::ImvDelta(ImvDeltaNode {
                input: Box::new(LogicalPlan::Scan(scan)),
                is_root: false,
                action_column: None,
            })),
            items: vec![ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: user_col_id,
                        qualifier: None,
                        column: "k".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "k".to_string(),
                output_column_id: user_col_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn malformed_delta_project_with_action_name(
        action_id: ColumnId,
        user_col_id: ColumnId,
    ) -> LogicalPlan {
        let mut scan = delta_scan_with_action(action_id);
        scan.columns[0].column_id = user_col_id;
        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::ImvDelta(ImvDeltaNode {
                input: Box::new(LogicalPlan::Scan(scan)),
                is_root: false,
                action_column: Some(action_id),
            })),
            items: vec![
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: user_col_id,
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: "k".to_string(),
                    output_column_id: user_col_id,
                },
                ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: user_col_id,
                            qualifier: None,
                            column: "k".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    output_name: ImvActionColumn::NAME.to_string(),
                    output_column_id: action_id,
                },
            ],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    #[test]
    fn propagate_through_project() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let scan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        let plan = project_over(scan, ColumnId(1));
        assert!(rule.matches(&plan, &ctx));
        let result = rule.apply(plan, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(LogicalPlan::Project(project)) = result else {
            panic!("expected Changed(Project)");
        };
        assert_eq!(project.items.len(), 2);
        let last = &project.items[1];
        assert_eq!(last.output_name, "__change_op");
        match &last.expr.kind {
            ExprKind::ColumnRef { column_id, .. } => assert_eq!(*column_id, ColumnId(100)),
            other => panic!("expected ColumnRef, got {:?}", other),
        }
        assert_eq!(last.expr.data_type, DataType::Int8);
        assert!(!last.expr.nullable);
    }

    #[test]
    fn propagate_is_idempotent_on_project_with_action() {
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let scan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        let mut plan = project_over(scan, ColumnId(1));
        if let LogicalPlan::Project(p) = &mut plan {
            p.items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(100),
                        qualifier: None,
                        column: "__change_op".to_string(),
                    },
                    data_type: DataType::Int8,
                    nullable: false,
                },
                output_name: "__change_op".to_string(),
                output_column_id: ColumnId(100),
            });
        }
        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn propagate_skips_bare_scan() {
        // A bare Scan is not a Project; the rule should not match.
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let plan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn propagate_rejects_aggregate() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let scan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            output_columns: Vec::new(),
            already_pushed: false,
            required_output_columns: None,
        });
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("Aggregate must fail");
        assert!(err.contains("Phase 4"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn propagate_rejects_join() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let left = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        let right = LogicalPlan::Scan(delta_scan());
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition: None,
            required_output_columns: None,
        });
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("Join must fail");
        assert!(err.contains("Phase 5"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn propagate_rejects_union() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::Union(UnionNode {
            inputs: vec![
                LogicalPlan::Scan(delta_scan_with_action(ColumnId(100))),
                LogicalPlan::Scan(starrocks_scan()),
            ],
            all: true,
            output_columns: Vec::new(),
            required_output_columns: None,
        });
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("Union must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn accepts_fan_in_delta_union_above_delta_scans() {
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let action_id = ColumnId(100);
        let union = LogicalPlan::Union(UnionNode {
            inputs: vec![
                normalized_delta_project(action_id, ColumnId(1)),
                normalized_delta_project(action_id, ColumnId(10)),
            ],
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
            ],
            required_output_columns: None,
        });

        assert!(!rule.matches(&union, &ctx));
    }

    #[test]
    fn accepts_bare_delta_scan_union_with_shared_action_column() {
        let rule = PropagateActionColumnRule;
        let ctx = build_ctx();
        let action_id = ColumnId(100);
        let union = LogicalPlan::Union(UnionNode {
            inputs: vec![
                LogicalPlan::Scan(delta_scan_with_action(action_id)),
                LogicalPlan::Scan(delta_scan_with_action(action_id)),
            ],
            all: true,
            output_columns: Vec::new(),
            required_output_columns: None,
        });

        assert!(!rule.matches(&union, &ctx));
    }

    #[test]
    fn rejects_fan_in_delta_union_missing_branch_action_column() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let union = LogicalPlan::Union(UnionNode {
            inputs: vec![
                normalized_delta_project(ColumnId(100), ColumnId(1)),
                normalized_delta_project_without_action(ColumnId(10)),
            ],
            all: true,
            output_columns: Vec::new(),
            required_output_columns: None,
        });

        assert!(rule.matches(&union, &ctx));
        let err = rule.apply(union, &mut ctx).expect_err("Union must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_fan_in_delta_union_with_mismatched_action_column_ids() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let union = LogicalPlan::Union(UnionNode {
            inputs: vec![
                normalized_delta_project(ColumnId(100), ColumnId(1)),
                normalized_delta_project(ColumnId(101), ColumnId(10)),
            ],
            all: true,
            output_columns: Vec::new(),
            required_output_columns: None,
        });

        assert!(rule.matches(&union, &ctx));
        let err = rule.apply(union, &mut ctx).expect_err("Union must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_fan_in_delta_union_with_malformed_project_action_item() {
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let union = LogicalPlan::Union(UnionNode {
            inputs: vec![
                normalized_delta_project(ColumnId(100), ColumnId(1)),
                malformed_delta_project_with_action_name(ColumnId(100), ColumnId(10)),
            ],
            all: true,
            output_columns: Vec::new(),
            required_output_columns: None,
        });

        assert!(rule.matches(&union, &ctx));
        let err = rule.apply(union, &mut ctx).expect_err("Union must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
    }

    #[test]
    fn propagate_carries_all_internal_columns_through_project() {
        use crate::sql::optimizer::rewrite::imv::row_id_column::ImvRowIdColumn;

        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let mut scan = delta_scan_with_action(ColumnId(100));
        scan.columns
            .push(ImvRowIdColumn::output_column(ColumnId(101)));
        let plan = project_over(LogicalPlan::Scan(scan), ColumnId(1));
        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::Project(project)) =
            rule.apply(plan, &mut ctx).expect("apply")
        else {
            panic!("expected Changed(Project)");
        };
        // k + __change_op + _row_id
        assert_eq!(project.items.len(), 3);
        assert!(project.items.iter().any(|i| i.output_name == "__change_op"));
        assert!(project.items.iter().any(|i| i.output_name == "_row_id"));
    }

    #[test]
    fn propagate_through_project_over_filter_over_scan() {
        // Filter is schema-passthrough: it should NOT match, but the action
        // column injected on the Scan must remain findable through the Filter
        // so the Project above can propagate it.
        let rule = PropagateActionColumnRule;
        let mut ctx = build_ctx();
        let scan = LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Bool(true)),
                data_type: DataType::Boolean,
                nullable: false,
            },
            required_output_columns: None,
        });
        // Filter itself must not match (schema-passthrough, no work).
        assert!(!rule.matches(&filter, &ctx));
        // find_action_column traverses the Filter to the Scan.
        assert!(find_action_column(&filter).is_some());
        // Project over the Filter propagates the action column.
        let project = project_over(filter, ColumnId(1));
        assert!(rule.matches(&project, &ctx));
        let result = rule.apply(project, &mut ctx).expect("apply must succeed");
        let RewriteResult::Changed(LogicalPlan::Project(p)) = result else {
            panic!("expected Changed(Project)");
        };
        assert!(p.items.iter().any(|i| i.output_name == "__change_op"));
    }
}
