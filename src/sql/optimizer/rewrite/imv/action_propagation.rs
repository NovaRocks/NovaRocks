//! IMV action column injection and propagation rules.
//!
//! Phase 2: Delta-bound scans get an internal `__change_op` Int8
//! non-nullable column. Project transparently carries it. Filter is a
//! schema-passthrough node and requires no work. Join/UnionAll/Aggregate
//! above a Delta scan are unsupported in Phase 2 and fail-fast.

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::catalog::ScanSource;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::LogicalPlan;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns true iff the plan's effective output schema contains the IMV
/// action column. Used by `matches()` predicates and validation.
//
// Called by `PropagateActionColumnRule::matches`, but that rule struct has no
// non-test constructor until Task 7 registers it in the pipeline, so the call
// chain is unreachable in non-test builds. Allow dead_code until then.
#[allow(dead_code)]
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
        _ => false,
    }
}

/// Returns the action column descriptor from the first descendant Scan/Project
/// in the subtree that exposes one, or `None` if no descendant carries it.
//
// Called by `PropagateActionColumnRule::apply`, but that rule struct has no
// non-test constructor until Task 7 registers it in the pipeline, so the call
// chain is unreachable in non-test builds. Allow dead_code until then.
#[allow(dead_code)]
pub(crate) fn find_action_column(plan: &LogicalPlan) -> Option<OutputColumn> {
    match plan {
        LogicalPlan::Scan(scan) => scan
            .columns
            .iter()
            .find(|c| ImvActionColumn::matches(c))
            .cloned(),
        LogicalPlan::Filter(node) => find_action_column(&node.input),
        LogicalPlan::Project(node) => find_action_column(&node.input),
        _ => None,
    }
}

/// Whether any descendant of the plan exposes an action column.
//
// Called by `PropagateActionColumnRule::matches`, but that rule struct has no
// non-test constructor until Task 7 registers it in the pipeline, so the call
// chain is unreachable in non-test builds. Allow dead_code until then.
#[allow(dead_code)]
pub(crate) fn subtree_has_action_column(plan: &LogicalPlan) -> bool {
    output_has_action_column(plan)
        || match plan {
            LogicalPlan::Filter(node) => subtree_has_action_column(&node.input),
            LogicalPlan::Project(node) => subtree_has_action_column(&node.input),
            _ => false,
        }
}

/// Returns the fully-qualified name of the first `IcebergDeltaTable`-backed
/// scan found anywhere in the subtree, for use in fail-fast diagnostics.
/// Recurses through every child-bearing variant (unlike the action-column
/// helpers, which only need Scan/Filter/Project), because an unsupported
/// Join/Union/Aggregate node's delta scan can sit under any branch.
//
// Called by `PropagateActionColumnRule::apply`, but that rule struct has no
// non-test constructor until Task 7 registers it in the pipeline, so the call
// chain is unreachable in non-test builds. Allow dead_code until then.
#[allow(dead_code)]
fn first_delta_base_fqn(plan: &LogicalPlan) -> Option<String> {
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
        LogicalPlan::Join(node) => {
            first_delta_base_fqn(&node.left).or_else(|| first_delta_base_fqn(&node.right))
        }
        LogicalPlan::Union(node) => node.inputs.iter().find_map(first_delta_base_fqn),
        LogicalPlan::ImvDelta(node) => first_delta_base_fqn(&node.input),
        LogicalPlan::ImvVersion(node) => first_delta_base_fqn(&node.input),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// InjectActionColumnRule
// ---------------------------------------------------------------------------

// Registered into the IMV rewrite pipeline by a later phase-2 task. Until that
// registration lands the rule has no non-test constructor, so allow dead_code
// to keep the build clean; this also keeps `ImvExtension::allocate_column_id`
// and its `next_column_id` field from tripping the transitive dead-code chain.
#[allow(dead_code)]
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

// Registered in the IMV rewrite pipeline by Task 7. Until that registration
// lands the rule has no non-test constructor, so allow dead_code to keep the
// build clean.
#[allow(dead_code)]
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
                subtree_has_action_column(&p.input) && !output_has_action_column(plan)
            }
            // Filter is a schema-passthrough node: it exposes its child's
            // schema verbatim, so once the child has the action column the
            // Filter's effective output also has it. No work needed.
            LogicalPlan::Filter(_) => false,
            // Aggregate / Join / Union above a delta subtree are unsupported in
            // Phase 2; match here so `apply` can fail-fast with a clear error.
            LogicalPlan::Aggregate(a) => subtree_has_action_column(&a.input),
            LogicalPlan::Join(j) => {
                subtree_has_action_column(&j.left) || subtree_has_action_column(&j.right)
            }
            LogicalPlan::Union(u) => u.inputs.iter().any(subtree_has_action_column),
            _ => false,
        }
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        // Diagnostic: the delta base under an unsupported node, if any. Computed
        // up-front from `&plan` so the fail-fast arms can name the offending
        // base table; harmless for the Project happy path.
        let base = first_delta_base_fqn(&plan).unwrap_or_else(|| "<unknown>".to_string());
        match plan {
            LogicalPlan::Project(mut p) => {
                let action = find_action_column(&p.input).ok_or_else(|| {
                    "PropagateActionColumn matched Project but child has no action column"
                        .to_string()
                })?;
                p.items.push(ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: action.column_id,
                            qualifier: None,
                            column: action.name.clone(),
                        },
                        data_type: DataType::Int8,
                        nullable: false,
                    },
                    output_name: action.name.clone(),
                });
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
            LogicalPlan::Union(_) => Err(format!(
                "IMV action column propagation does not support UNION above \
                 delta-bound scan {base} in Phase 2; union delta rewrite is \
                 scheduled for Phase 6"
            )),
            _ => Ok(RewriteResult::Unchanged),
        }
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
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::analysis::{JoinKind, LiteralValue};
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
        scan.columns.push(ImvActionColumn::output_column(ColumnId(9)));
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
            }],
        })
    }

    fn delta_scan_with_action(action_id: ColumnId) -> ScanNode {
        let mut s = delta_scan();
        s.columns.push(ImvActionColumn::output_column(action_id));
        s
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
            inputs: vec![LogicalPlan::Scan(delta_scan_with_action(ColumnId(100)))],
            all: true,
        });
        assert!(rule.matches(&plan, &ctx));
        let err = rule.apply(plan, &mut ctx).expect_err("Union must fail");
        assert!(err.contains("Phase 6"), "unexpected error: {err}");
        assert!(err.contains("ice.db.b"), "unexpected error: {err}");
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
