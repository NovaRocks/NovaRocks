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

pub(crate) struct RewriteJoinDeltaRule;

pub(crate) fn join_delta_kind_supported(kind: crate::sql::analysis::JoinKind) -> bool {
    matches!(
        kind,
        crate::sql::analysis::JoinKind::Inner | crate::sql::analysis::JoinKind::Cross
    )
}

impl LogicalRewriteRule for RewriteJoinDeltaRule {
    fn name(&self) -> &'static str {
        "RewriteJoinDelta"
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
            LogicalPlan::ImvDelta(delta) if matches!(delta.input.as_ref(), LogicalPlan::Join(_))
        )
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let LogicalPlan::Join(join) = *delta.input else {
            return Ok(RewriteResult::Unchanged);
        };

        if !join_delta_kind_supported(join.join_type) {
            return Err(format!(
                "Iceberg IMV join delta rewrite supports inner/cross joins only, got {:?}",
                join.join_type
            ));
        }

        let action_column = match delta.action_column {
            Some(action_column) => action_column,
            None => ctx
                .extension::<ImvExtension>()
                .ok_or_else(|| {
                    "RewriteJoinDelta requires ImvExtension in RewriteContext".to_string()
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

        Ok(RewriteResult::Changed(LogicalPlan::Union(UnionNode {
            inputs: vec![left_delta_branch, right_delta_branch],
            all: true,
            output_columns,
            required_output_columns,
        })))
    }
}

fn join_output_columns(
    join_type: JoinKind,
    left: &LogicalPlan,
    right: &LogicalPlan,
) -> Result<Vec<OutputColumn>, String> {
    if !join_delta_kind_supported(join_type) {
        return Err(format!(
            "Iceberg IMV join delta rewrite cannot derive output columns for unsupported join kind {:?}",
            join_type
        ));
    }
    let left_cols = plan_output_columns(left)?;
    let right_cols = plan_output_columns(right)?;
    let mut out = left_cols
        .into_iter()
        .filter(|column| !column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
        .collect::<Vec<_>>();
    out.extend(right_cols);
    out.retain(|column| !column.name.eq_ignore_ascii_case(ImvActionColumn::NAME));
    Ok(out)
}

pub(crate) fn mark_delta_scan(
    plan: LogicalPlan,
    action_column: ColumnId,
) -> Result<LogicalPlan, String> {
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
        LogicalPlan::Join(join) => match marker {
            MarkerKind::Delta(action_column) => {
                wrap_scan_marker(LogicalPlan::Join(join), MarkerKind::Delta(action_column))
            }
            MarkerKind::Version(version_ref) => {
                let JoinNode {
                    left,
                    right,
                    join_type,
                    condition,
                    required_output_columns,
                } = join;
                LogicalPlan::Join(JoinNode {
                    left: Box::new(mark_scan(*left, MarkerKind::Version(version_ref.clone()))?),
                    right: Box::new(mark_scan(*right, MarkerKind::Version(version_ref))?),
                    join_type,
                    condition,
                    required_output_columns,
                })
            }
        },
        other => {
            return Err(format!(
                "Iceberg IMV join delta rewrite supports only Scan/Project/Filter/Join join sides, got {}",
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
            branch_scope: None,
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
        LogicalPlan::Repeat(_) => "Repeat",
        LogicalPlan::CTEAnchor(_) => "CTEAnchor",
        LogicalPlan::CTEProduce(_) => "CTEProduce",
        LogicalPlan::CTEConsume(_) => "CTEConsume",
        LogicalPlan::Decode(_) => "Decode",
        LogicalPlan::AggregateStateMerge(_) => "AggregateStateMerge",
        LogicalPlan::Apply(_) => "Apply",
        LogicalPlan::AssertOneRow(_) => "AssertOneRow",
        LogicalPlan::ImvDelta(_) => "ImvDelta",
        LogicalPlan::ImvVersion(_) => "ImvVersion",
    }
}

pub(crate) fn normalize_branch_output(
    input: LogicalPlan,
    output_columns: &[OutputColumn],
) -> LogicalPlan {
    LogicalPlan::Project(ProjectNode {
        input: Box::new(input),
        output_qualifier: None,
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
        LogicalPlan::Repeat(repeat) => plan_output_columns(&repeat.input)?,
        LogicalPlan::CTEAnchor(anchor) => plan_output_columns(&anchor.consumer)?,
        LogicalPlan::CTEProduce(produce) => produce.output_columns.clone(),
        LogicalPlan::CTEConsume(consume) => consume.output_columns.clone(),
        LogicalPlan::Decode(decode) => decode.output_columns.clone(),
        LogicalPlan::AggregateStateMerge(merge) => merge.output_columns.clone(),
        LogicalPlan::Apply(apply) => {
            let mut out = plan_output_columns(&apply.left)?;
            out.push(apply.output_column.clone());
            out
        }
        LogicalPlan::AssertOneRow(assert) => plan_output_columns(&assert.input)?,
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
    fn pure_join_delta_matches_imv_delta_over_join_any_root() {
        let rule = RewriteJoinDeltaRule;
        let ctx = build_ctx();
        let non_root = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(join_of(scan("l", 1), scan("r", 10))),
            is_root: false,
            action_column: Some(ColumnId(100)),
            branch_scope: None,
        });
        assert!(rule.matches(&non_root, &ctx));

        let over_agg = delta(aggregate_over(join_over(JoinKind::Inner)));
        assert!(!rule.matches(&over_agg, &ctx));
    }

    #[test]
    fn pure_join_delta_expands_into_union_without_outer_aggregate() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(join_over(JoinKind::Inner)),
            is_root: false,
            action_column: Some(ColumnId(100)),
            branch_scope: None,
        });

        let RewriteResult::Changed(LogicalPlan::Union(union)) =
            rule.apply(plan, &mut ctx).expect("expand")
        else {
            panic!("pure join-delta must expand ImvDelta(Join) directly into a Union");
        };

        assert!(union.all);
        assert_eq!(union.inputs.len(), 2);
        let left = assert_normalized_branch(&union.inputs[0], ColumnId(100));
        assert_condition_refs(left.condition.as_ref());
        assert_delta(left.left.as_ref(), "left", ColumnId(100));
        assert_version(left.right.as_ref(), "right", ImvVersionRole::From);

        let right = assert_normalized_branch(&union.inputs[1], ColumnId(100));
        assert_condition_refs(right.condition.as_ref());
        assert_version(right.left.as_ref(), "left", ImvVersionRole::To);
        assert_delta(right.right.as_ref(), "right", ColumnId(100));
    }

    #[test]
    fn pure_join_delta_drops_preexisting_action_metadata_outputs() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(join_of(
                project_over(scan_with_action_metadata("left", 1, 8)),
                project_over(scan_with_action_metadata("right", 10, 15)),
            )),
            is_root: false,
            action_column: Some(ColumnId(100)),
            branch_scope: None,
        });

        let RewriteResult::Changed(LogicalPlan::Union(union)) =
            rule.apply(plan, &mut ctx).expect("expand")
        else {
            panic!("pure join-delta must expand into a Union");
        };

        let action_outputs = union
            .output_columns
            .iter()
            .filter(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
            .collect::<Vec<_>>();
        assert_eq!(action_outputs.len(), 1);
        assert_eq!(action_outputs[0].column_id, ColumnId(100));
        assert!(action_outputs[0].is_internal);
        for input in &union.inputs {
            let LogicalPlan::Project(project) = input else {
                panic!("expected normalized branch Project");
            };
            let action_items = project
                .items
                .iter()
                .filter(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
                .collect::<Vec<_>>();
            assert_eq!(action_items.len(), 1);
            assert_eq!(action_items[0].output_column_id, ColumnId(100));
        }
    }

    #[test]
    fn pure_join_delta_rejects_outer_join() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(join_over(JoinKind::LeftOuter)),
            is_root: false,
            action_column: Some(ColumnId(100)),
            branch_scope: None,
        });

        let err = rule.apply(plan, &mut ctx).expect_err("outer must reject");
        assert!(err.contains("inner/cross"), "unexpected: {err}");
    }

    #[test]
    fn pure_join_delta_nested_leaves_inner_join_delta_for_next_iteration() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let inner = join_of(scan("a", 1), scan("b", 10));
        let outer = join_of_with_left(inner, scan("c", 20));
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(outer),
            is_root: false,
            action_column: Some(ColumnId(100)),
            branch_scope: None,
        });

        let RewriteResult::Changed(LogicalPlan::Union(union)) =
            rule.apply(plan, &mut ctx).expect("expand outer")
        else {
            panic!("expected Union");
        };

        let left = assert_normalized_branch(&union.inputs[0], ColumnId(100));
        assert!(
            plan_contains_inner_join_delta(left.left.as_ref()),
            "outer-left delta side must leave ImvDelta(Join(a,b)) for the next fixpoint iteration"
        );
    }

    #[test]
    fn mark_delta_scan_wraps_nested_join_whole() {
        // Delta marker over a Join must wrap the entire join (pending recursive join-delta expansion),
        // NOT push into the two sides.
        let join = join_of(scan("a", 1), scan("b", 10));
        let marked = mark_delta_scan(join, ColumnId(100)).expect("mark delta over join");
        let LogicalPlan::ImvDelta(delta) = marked else {
            panic!("expected ImvDelta wrapping the whole join, got {marked:?}");
        };
        assert!(!delta.is_root, "nested join delta marker is not root");
        assert_eq!(delta.action_column, Some(ColumnId(100)));
        assert!(matches!(delta.input.as_ref(), LogicalPlan::Join(_)));
    }

    #[test]
    fn mark_version_scan_pushes_same_role_down_both_join_sides() {
        // Version marker over a Join distributes over the join:
        // Version(Join(a,b), from) == Join(Version(a, from), Version(b, from)).
        let join = join_of(scan("a", 1), scan("b", 10));
        let marked = mark_version_scan(join, ImvVersionRef::from_snapshot())
            .expect("mark version over join");
        let LogicalPlan::Join(j) = marked else {
            panic!("expected Join with both sides version-marked, got {marked:?}");
        };
        let left_v = assert_version_side(j.left.as_ref());
        let right_v = assert_version_side(j.right.as_ref());
        assert_eq!(
            left_v.version_ref,
            ImvVersionRef {
                role: ImvVersionRole::From
            }
        );
        assert_eq!(
            right_v.version_ref,
            ImvVersionRef {
                role: ImvVersionRole::From
            }
        );
    }

    fn assert_version_side(plan: &LogicalPlan) -> &ImvVersionNode {
        match plan {
            LogicalPlan::ImvVersion(v) => v,
            other => panic!("expected ImvVersion on join side, got {other:?}"),
        }
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
            plan_output_columns(project.input.as_ref())
                .expect("branch output columns")
                .into_iter()
                .map(|column| column.column_id)
                .chain(std::iter::once(action_column))
                .collect::<Vec<_>>()
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
            branch_scope: None,
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

    fn join_of(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        let left_cols = plan_output_columns(&left).expect("left output columns");
        let right_cols = plan_output_columns(&right).expect("right output columns");
        let left_key = &left_cols[0];
        let right_key = &right_cols[0];
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition: Some(TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_expr(left_key.column_id.0, &left_key.name)),
                    op: BinOp::Eq,
                    right: Box::new(col_expr(right_key.column_id.0, &right_key.name)),
                },
                data_type: DataType::Boolean,
                nullable: false,
            }),
            required_output_columns: None,
        })
    }

    fn join_of_with_left(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        join_of(left, right)
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
            output_qualifier: None,
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
                        serialized_metadata_rows: None,
                    },
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                    binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
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
            variant_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    fn scan_with_action_metadata(name: &str, first_id: u32, action_id: u32) -> LogicalPlan {
        let mut plan = scan(name, first_id);
        let LogicalPlan::Scan(scan) = &mut plan else {
            unreachable!();
        };
        scan.columns.push(OutputColumn {
            column_id: ColumnId(action_id),
            name: ImvActionColumn::NAME.to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: false,
        });
        plan
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

    fn plan_contains_inner_join_delta(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::ImvDelta(delta) => {
                matches!(delta.input.as_ref(), LogicalPlan::Join(_))
                    || plan_contains_inner_join_delta(delta.input.as_ref())
            }
            LogicalPlan::Project(project) => plan_contains_inner_join_delta(project.input.as_ref()),
            LogicalPlan::Filter(filter) => plan_contains_inner_join_delta(filter.input.as_ref()),
            LogicalPlan::Join(join) => {
                plan_contains_inner_join_delta(join.left.as_ref())
                    || plan_contains_inner_join_delta(join.right.as_ref())
            }
            _ => false,
        }
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
