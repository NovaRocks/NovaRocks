use arrow::datatypes::DataType;

use crate::sql::analysis::{JoinKind, OutputColumn, ProjectItem};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::marker::{ImvDeltaNode, ImvVersionNode, ImvVersionRef};
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{JoinNode, LogicalPlan, ProjectNode, UnionNode};

pub(crate) struct RewriteJoinAggregateDeltaRule;

pub(crate) fn join_delta_kind_supported(kind: crate::sql::analysis::JoinKind) -> bool {
    matches!(
        kind,
        crate::sql::analysis::JoinKind::Inner | crate::sql::analysis::JoinKind::Cross
    )
}

impl LogicalRewriteRule for RewriteJoinAggregateDeltaRule {
    fn name(&self) -> &'static str {
        "RewriteJoinAggregateDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(delta)
                if delta.is_root
                    && matches!(
                        delta.input.as_ref(),
                        LogicalPlan::Aggregate(aggregate)
                            if matches!(aggregate.input.as_ref(), LogicalPlan::Join(_))
                    )
        )
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        if !delta.is_root {
            return Ok(RewriteResult::Unchanged);
        }
        let LogicalPlan::Aggregate(mut aggregate) = *delta.input else {
            return Ok(RewriteResult::Unchanged);
        };
        let LogicalPlan::Join(join) = *aggregate.input else {
            return Ok(RewriteResult::Unchanged);
        };

        if !join_delta_kind_supported(join.join_type) {
            return Err(format!(
                "Iceberg IMV join aggregate rewrite supports inner/cross joins only, got {:?}",
                join.join_type
            ));
        }

        let action_column = match delta.action_column {
            Some(action_column) => action_column,
            None => ctx
                .extension::<ImvExtension>()
                .ok_or_else(|| {
                    "RewriteJoinAggregateDelta requires ImvExtension in RewriteContext".to_string()
                })?
                .allocate_column_id(),
        };

        let JoinNode {
            left,
            right,
            join_type,
            condition,
            required_output_columns,
        } = join;
        let left = *left;
        let right = *right;
        let mut output_columns = join_output_columns(join_type, &left, &right)?;
        output_columns.push(ImvActionColumn::output_column(action_column));

        let left_delta_branch = normalize_branch_output(
            LogicalPlan::Join(JoinNode {
                left: Box::new(mark_delta_scan(left.clone(), action_column)?),
                right: Box::new(mark_version_scan(
                    right.clone(),
                    ImvVersionRef::from_snapshot(),
                )?),
                join_type,
                condition: condition.clone(),
                required_output_columns: required_output_columns.clone(),
            }),
            &output_columns,
        );

        let right_delta_branch = normalize_branch_output(
            LogicalPlan::Join(JoinNode {
                left: Box::new(mark_version_scan(left, ImvVersionRef::to_snapshot())?),
                right: Box::new(mark_delta_scan(right, action_column)?),
                join_type,
                condition,
                required_output_columns: required_output_columns.clone(),
            }),
            &output_columns,
        );

        aggregate.input = Box::new(LogicalPlan::Union(UnionNode {
            inputs: vec![left_delta_branch, right_delta_branch],
            all: true,
            output_columns,
            required_output_columns,
        }));

        Ok(RewriteResult::Changed(LogicalPlan::ImvDelta(
            ImvDeltaNode {
                input: Box::new(LogicalPlan::Aggregate(aggregate)),
                is_root: true,
                action_column: Some(action_column),
            },
        )))
    }
}

fn join_output_columns(
    join_type: JoinKind,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Result<Vec<OutputColumn>, String> {
    if !join_delta_kind_supported(join_type) {
        return Err(format!(
            "Iceberg IMV join aggregate rewrite cannot derive output columns for unsupported join kind {:?}",
            join_type
        ));
    }
    let left_cols = plan_output_columns(left)?;
    let right_cols = plan_output_columns(right)?;
    let mut out = left_cols;
    out.extend(right_cols);
    Ok(out)
}

fn mark_delta_scan(plan: LogicalPlan, action_column: ColumnId) -> Result<LogicalPlan, String> {
    mark_scan(plan, MarkerKind::Delta(action_column))
}

fn mark_version_scan(plan: LogicalPlan, version_ref: ImvVersionRef) -> Result<LogicalPlan, String> {
    mark_scan(plan, MarkerKind::Version(version_ref))
}

enum MarkerKind {
    Delta(ColumnId),
    Version(ImvVersionRef),
}

fn mark_scan(plan: LogicalPlan, marker: MarkerKind) -> Result<LogicalPlan, String> {
    Ok(match plan {
        LogicalPlan::Scan(_) => wrap_scan_marker(plan, marker),
        LogicalPlan::Project(mut project) => {
            project.input = Box::new(mark_scan(*project.input, marker)?);
            LogicalPlan::Project(project)
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Box::new(mark_scan(*filter.input, marker)?);
            LogicalPlan::Filter(filter)
        }
        other => {
            return Err(format!(
                "Iceberg IMV join aggregate rewrite supports only Scan/Project/Filter join sides, got {}",
                plan_kind(&other)
            ));
        }
    })
}

fn wrap_scan_marker(scan: LogicalPlan, marker: MarkerKind) -> LogicalPlan {
    match marker {
        MarkerKind::Delta(action_column) => LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(scan),
            is_root: false,
            action_column: Some(action_column),
        }),
        MarkerKind::Version(version_ref) => LogicalPlan::ImvVersion(ImvVersionNode {
            input: Box::new(scan),
            version_ref,
        }),
    }
}

fn plan_kind(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan(_) => "Scan",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Project(_) => "Project",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::Intersect(_) => "Intersect",
        LogicalPlan::Except(_) => "Except",
        LogicalPlan::Values(_) => "Values",
        LogicalPlan::GenerateSeries(_) => "GenerateSeries",
        LogicalPlan::TableFunction(_) => "TableFunction",
        LogicalPlan::Window(_) => "Window",
        LogicalPlan::SubqueryAlias(_) => "SubqueryAlias",
        LogicalPlan::Repeat(_) => "Repeat",
        LogicalPlan::CTEAnchor(_) => "CTEAnchor",
        LogicalPlan::CTEProduce(_) => "CTEProduce",
        LogicalPlan::CTEConsume(_) => "CTEConsume",
        LogicalPlan::Decode(_) => "Decode",
        LogicalPlan::AggregateStateMerge(_) => "AggregateStateMerge",
        LogicalPlan::ImvDelta(_) => "ImvDelta",
        LogicalPlan::ImvVersion(_) => "ImvVersion",
    }
}

fn normalize_branch_output(input: LogicalPlan, output_columns: &[OutputColumn]) -> LogicalPlan {
    LogicalPlan::Project(ProjectNode {
        input: Box::new(input),
        items: output_columns
            .iter()
            .map(|column| ProjectItem {
                expr: crate::sql::analysis::TypedExpr {
                    kind: crate::sql::analysis::ExprKind::ColumnRef {
                        column_id: column.column_id,
                        qualifier: None,
                        column: column.name.clone(),
                    },
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                },
                output_name: column.name.clone(),
                output_column_id: column.column_id,
            })
            .collect(),
        required_output_columns: None,
    })
}

pub(crate) fn plan_output_columns(plan: &LogicalPlan) -> Result<Vec<OutputColumn>, String> {
    Ok(match plan {
        LogicalPlan::Scan(scan) => scan.columns.clone(),
        LogicalPlan::Project(project) => project
            .items
            .iter()
            .filter(|item| item.output_column_id != ColumnId::UNSET)
            .map(project_item_output_column)
            .collect(),
        LogicalPlan::Aggregate(aggregate) => aggregate.output_columns.clone(),
        LogicalPlan::Join(join) => join_output_columns(join.join_type, &join.left, &join.right)?,
        LogicalPlan::Sort(sort) => plan_output_columns(&sort.input)?,
        LogicalPlan::Limit(limit) => plan_output_columns(&limit.input)?,
        LogicalPlan::Filter(filter) => plan_output_columns(&filter.input)?,
        LogicalPlan::Union(union) => union.output_columns.clone(),
        LogicalPlan::Intersect(intersect) => intersect.output_columns.clone(),
        LogicalPlan::Except(except) => except.output_columns.clone(),
        LogicalPlan::Values(values) => values.columns.clone(),
        LogicalPlan::GenerateSeries(generate) => vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: generate.column_name.clone(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }],
        LogicalPlan::TableFunction(table_function) => {
            let mut out = plan_output_columns(&table_function.input)?;
            out.extend(table_function.output_columns.clone());
            out
        }
        LogicalPlan::Window(window) => window.output_columns.clone(),
        LogicalPlan::SubqueryAlias(alias) => alias.output_columns.clone(),
        LogicalPlan::Repeat(repeat) => plan_output_columns(&repeat.input)?,
        LogicalPlan::CTEAnchor(anchor) => plan_output_columns(&anchor.consumer)?,
        LogicalPlan::CTEProduce(produce) => produce.output_columns.clone(),
        LogicalPlan::CTEConsume(consume) => consume.output_columns.clone(),
        LogicalPlan::Decode(decode) => decode.output_columns.clone(),
        LogicalPlan::AggregateStateMerge(merge) => merge.output_columns.clone(),
        LogicalPlan::ImvDelta(delta) => plan_output_columns(&delta.input)?,
        LogicalPlan::ImvVersion(version) => plan_output_columns(&version.input)?,
    })
}

fn project_item_output_column(item: &ProjectItem) -> OutputColumn {
    OutputColumn {
        column_id: item.output_column_id,
        name: item.output_name.clone(),
        data_type: item.expr.data_type.clone(),
        nullable: item.expr.nullable,
        is_internal: false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::imv::marker::{ImvDeltaNode, ImvVersionRef};
    use crate::sql::optimizer::rewrite::imv::scan_binding::ImvVersionRole;
    use crate::sql::planner::plan::{AggregateNode, JoinNode, ProjectNode, ScanNode};

    #[test]
    fn supported_join_delta_kinds_are_inner_and_cross_only() {
        assert!(join_delta_kind_supported(JoinKind::Inner));
        assert!(join_delta_kind_supported(JoinKind::Cross));
        assert!(!join_delta_kind_supported(JoinKind::LeftOuter));
        assert!(!join_delta_kind_supported(JoinKind::RightOuter));
        assert!(!join_delta_kind_supported(JoinKind::FullOuter));
        assert!(!join_delta_kind_supported(JoinKind::LeftSemi));
        assert!(!join_delta_kind_supported(JoinKind::LeftAnti));
        assert!(!join_delta_kind_supported(JoinKind::RightSemi));
        assert!(!join_delta_kind_supported(JoinKind::RightAnti));
        assert!(!join_delta_kind_supported(JoinKind::NullAwareLeftAnti));
    }

    #[test]
    fn rewrite_join_aggregate_delta_rejects_outer_join() {
        let rule = RewriteJoinAggregateDeltaRule;
        let mut ctx = build_ctx();
        let plan = delta(aggregate_over(join_over(JoinKind::LeftOuter)));

        assert!(rule.matches(&plan, &ctx));
        let err = rule
            .apply(plan, &mut ctx)
            .expect_err("outer join must be rejected");
        assert_eq!(
            err,
            "Iceberg IMV join aggregate rewrite supports inner/cross joins only, got LeftOuter"
        );
    }

    #[test]
    fn rewrite_inner_join_aggregate_delta_expands_two_stable_branches() {
        assert_supported_join_rewrite(JoinKind::Inner);
    }

    #[test]
    fn rewrite_cross_join_aggregate_delta_expands_two_stable_branches() {
        assert_supported_join_rewrite(JoinKind::Cross);
    }

    fn assert_supported_join_rewrite(join_type: JoinKind) {
        let rule = RewriteJoinAggregateDeltaRule;
        let mut ctx = build_ctx();
        let plan = delta(aggregate_over(join_over(join_type)));

        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::ImvDelta(root_delta)) = rule
            .apply(plan, &mut ctx)
            .expect("supported join must rewrite")
        else {
            panic!("expected Changed(ImvDelta)");
        };
        assert!(root_delta.is_root);
        let action_column = root_delta
            .action_column
            .expect("root delta must carry allocated action column");
        assert_eq!(action_column, ColumnId(100));

        let LogicalPlan::Aggregate(aggregate) = root_delta.input.as_ref() else {
            panic!("expected root ImvDelta(Aggregate)");
        };
        let LogicalPlan::Union(union) = aggregate.input.as_ref() else {
            panic!("expected Aggregate(UnionAll)");
        };
        assert!(union.all);
        assert_eq!(union.inputs.len(), 2);
        assert_eq!(
            union
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![
                ColumnId(1),
                ColumnId(2),
                ColumnId(10),
                ColumnId(11),
                ColumnId(100)
            ]
        );

        let left_delta_branch = assert_normalized_branch(&union.inputs[0], action_column);
        assert_eq!(left_delta_branch.join_type, join_type);
        assert_condition_refs(left_delta_branch.condition.as_ref());
        assert_delta(left_delta_branch.left.as_ref(), "left", action_column);
        assert_version(
            left_delta_branch.right.as_ref(),
            "right",
            ImvVersionRole::From,
        );

        let right_delta_branch = assert_normalized_branch(&union.inputs[1], action_column);
        assert_eq!(right_delta_branch.join_type, join_type);
        assert_condition_refs(right_delta_branch.condition.as_ref());
        assert_version(right_delta_branch.left.as_ref(), "left", ImvVersionRole::To);
        assert_delta(right_delta_branch.right.as_ref(), "right", action_column);
    }

    fn assert_normalized_branch(plan: &LogicalPlan, action_column: ColumnId) -> &JoinNode {
        let LogicalPlan::Project(project) = plan else {
            panic!("expected normalized branch Project");
        };
        assert_eq!(
            project
                .items
                .iter()
                .map(|item| item.output_column_id)
                .collect::<Vec<_>>(),
            vec![
                ColumnId(1),
                ColumnId(2),
                ColumnId(10),
                ColumnId(11),
                action_column
            ]
        );
        assert!(
            project
                .items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case("__change_op")
                    && item.output_column_id == action_column),
            "normalized branch Project must expose shared action column"
        );
        let LogicalPlan::Join(join) = project.input.as_ref() else {
            panic!("expected Project(Join)");
        };
        join
    }

    fn build_ctx() -> RewriteContext {
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_rewrite_context(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn delta(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(input),
            is_root: true,
            action_column: None,
        })
    }

    fn aggregate_over(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(input),
            group_by: vec![col_expr(1, "l_k")],
            aggregates: Vec::new(),
            output_columns: vec![output_column(1, "l_k"), output_column(10, "r_k")],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    fn join_over(join_type: JoinKind) -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(project_over(scan("left", 1))),
            right: Box::new(project_over(scan("right", 10))),
            join_type,
            condition: Some(condition()),
            required_output_columns: None,
        })
    }

    fn project_over(input: LogicalPlan) -> LogicalPlan {
        let columns = match &input {
            LogicalPlan::Scan(scan) => scan.columns.clone(),
            _ => unreachable!(),
        };
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: columns
                .into_iter()
                .map(|column| ProjectItem {
                    expr: col_expr(column.column_id.0, &column.name),
                    output_name: column.name,
                    output_column_id: column.column_id,
                })
                .collect(),
            required_output_columns: None,
        })
    }

    fn scan(name: &str, first_id: u32) -> LogicalPlan {
        let columns = vec![
            column_def(&format!("{name}_k")),
            column_def(&format!("{name}_v")),
        ];
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns,
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: name.to_string(),
                        table_uuid: Some(format!("uuid-{name}")),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: format!("file:///tmp/ice/db/{name}"),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                    },
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                },
            },
            alias: None,
            columns: vec![
                output_column(first_id, &format!("{name}_k")),
                output_column(first_id + 1, &format!("{name}_v")),
            ],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    fn column_def(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn condition() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_expr(1, "left_k")),
                op: BinOp::Eq,
                right: Box::new(col_expr(10, "right_k")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn assert_condition_refs(condition: Option<&TypedExpr>) {
        let Some(TypedExpr {
            kind: ExprKind::BinaryOp { left, op, right },
            ..
        }) = condition
        else {
            panic!("expected binary join condition");
        };
        assert_eq!(*op, BinOp::Eq);
        assert!(matches!(
            &left.kind,
            ExprKind::ColumnRef { column_id, column, .. }
                if *column_id == ColumnId(1) && column == "left_k"
        ));
        assert!(matches!(
            &right.kind,
            ExprKind::ColumnRef { column_id, column, .. }
                if *column_id == ColumnId(10) && column == "right_k"
        ));
    }

    fn assert_delta(plan: &LogicalPlan, expected_scan: &str, action_column: ColumnId) {
        let LogicalPlan::Project(project) = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::ImvDelta(delta) = project.input.as_ref() else {
            panic!("expected Project(ImvDelta(...))");
        };
        assert!(!delta.is_root);
        assert_eq!(delta.action_column, Some(action_column));
        assert_scan(delta.input.as_ref(), expected_scan);
    }

    fn assert_version(plan: &LogicalPlan, expected_scan: &str, role: ImvVersionRole) {
        let LogicalPlan::Project(project) = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::ImvVersion(version) = project.input.as_ref() else {
            panic!("expected Project(ImvVersion(...))");
        };
        assert_eq!(version.version_ref, ImvVersionRef { role });
        assert_scan(version.input.as_ref(), expected_scan);
    }

    fn assert_scan(plan: &LogicalPlan, expected_scan: &str) {
        let LogicalPlan::Scan(scan) = plan else {
            panic!("expected Scan");
        };
        assert_eq!(scan.table.name, expected_scan);
    }
}
