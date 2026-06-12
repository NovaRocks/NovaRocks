use arrow::datatypes::DataType;

use crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN;
use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::join_delta::plan_output_columns;
use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{AggregateNode, LogicalPlan, ProjectNode, UnionNode};

pub(crate) struct RewriteBranchUnionRule;

impl LogicalRewriteRule for RewriteBranchUnionRule {
    fn name(&self) -> &'static str {
        "RewriteBranchUnion"
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
                        LogicalPlan::Union(union)
                            if union.all
                                && union.inputs.iter().all(is_branch_union_aggregate_branch)
                                && !plan_contains_imv_marker(delta.input.as_ref())
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
        let action_column = delta.action_column;
        let LogicalPlan::Union(union) = *delta.input else {
            return Ok(RewriteResult::Unchanged);
        };
        if !union.all {
            return Err("Iceberg IMV branch UNION rewrite supports UNION ALL only".to_string());
        }
        if union.inputs.len() < 2 {
            return Err(
                "Iceberg IMV branch UNION rewrite requires at least two aggregate branches"
                    .to_string(),
            );
        }

        let UnionNode {
            inputs,
            all: _,
            output_columns,
            required_output_columns,
        } = union;
        for branch in &inputs {
            if !is_branch_union_aggregate_branch(branch) {
                return Err(format!(
                    "Iceberg IMV branch UNION rewrite supports only aggregate or Project-over-Aggregate branches, got {}",
                    plan_kind(branch)
                ));
            }
        }

        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or_else(|| {
                "RewriteBranchUnion requires ImvExtension in RewriteContext".to_string()
            })?
            .clone();
        let branch_id_column = ext.allocate_column_id();
        let mut rewritten_inputs = Vec::with_capacity(inputs.len());
        for (idx, branch) in inputs.into_iter().enumerate() {
            let branch_id = i32::try_from(idx)
                .map_err(|_| "Iceberg IMV branch UNION branch index overflow".to_string())?;
            let branch_kind = plan_kind(&branch);
            let branch = extract_branch_union_aggregate_branch(branch).ok_or_else(|| {
                format!(
                    "Iceberg IMV branch UNION rewrite supports only aggregate or Project-over-Aggregate branches, got {}",
                    branch_kind
                )
            })?;
            // Tag the aggregate core as an independent, branch-scoped delta sub-problem.
            // The existing aggregate-state (and join/union-delta beneath it) rules
            // decompose it in later stages, reading branch_scope off this marker.
            // Each branch becomes its own root delta sub-problem: `is_root` is
            // per-sub-problem here, so the post-branch plan intentionally holds one
            // root delta per branch (not a single global root).
            let scope = crate::sql::catalog::BranchScope {
                branch_id_column_name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
                branch_id,
            };
            let core =
                LogicalPlan::ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode {
                    input: Box::new(LogicalPlan::Aggregate(branch.aggregate)),
                    is_root: true,
                    action_column,
                    branch_scope: Some(scope),
                });
            let rewritten = match branch.post_project {
                Some(project) => append_branch_id_to_project(
                    ProjectNode {
                        input: Box::new(core),
                        items: project.items,
                        output_qualifier: project.output_qualifier,
                        required_output_columns: None,
                    },
                    branch_id,
                    branch_id_column,
                ),
                None => append_branch_id_project(core, branch_id, branch_id_column),
            }?;
            rewritten_inputs.push(rewritten);
        }

        Ok(RewriteResult::Changed(LogicalPlan::Union(UnionNode {
            inputs: rewritten_inputs,
            all: true,
            output_columns: branch_union_output_columns(output_columns, branch_id_column),
            required_output_columns,
        })))
    }
}

struct BranchUnionAggregateBranch {
    aggregate: AggregateNode,
    post_project: Option<BranchUnionAggregateProject>,
}

struct BranchUnionAggregateProject {
    items: Vec<ProjectItem>,
    output_qualifier: Option<String>,
}

fn is_branch_union_aggregate_branch(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Project(project) => {
            matches!(project.input.as_ref(), LogicalPlan::Aggregate(_))
        }
        _ => false,
    }
}

fn extract_branch_union_aggregate_branch(
    branch: LogicalPlan,
) -> Option<BranchUnionAggregateBranch> {
    match branch {
        LogicalPlan::Aggregate(aggregate) => Some(BranchUnionAggregateBranch {
            aggregate,
            post_project: None,
        }),
        LogicalPlan::Project(project) => {
            let ProjectNode {
                input,
                items,
                output_qualifier,
                required_output_columns: _,
            } = project;
            let LogicalPlan::Aggregate(aggregate) = *input else {
                return None;
            };
            Some(BranchUnionAggregateBranch {
                aggregate,
                post_project: Some(BranchUnionAggregateProject {
                    items,
                    output_qualifier,
                }),
            })
        }
        _ => None,
    }
}

fn append_branch_id_to_project(
    mut project: ProjectNode,
    branch_id: i32,
    branch_id_column: ColumnId,
) -> Result<LogicalPlan, String> {
    project
        .items
        .push(branch_id_project_item(branch_id, branch_id_column));
    Ok(LogicalPlan::Project(project))
}

fn branch_id_project_item(branch_id: i32, branch_id_column: ColumnId) -> ProjectItem {
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
        output_column_id: branch_id_column,
    }
}

fn append_branch_id_project(
    input: LogicalPlan,
    branch_id: i32,
    branch_id_column: ColumnId,
) -> Result<LogicalPlan, String> {
    let mut items = plan_output_columns(&input)?
        .into_iter()
        .map(|column| ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: column.column_id,
                    qualifier: None,
                    column: column.name.clone(),
                },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            },
            output_name: column.name,
            output_column_id: column.column_id,
        })
        .collect::<Vec<_>>();
    items.push(branch_id_project_item(branch_id, branch_id_column));
    Ok(LogicalPlan::Project(ProjectNode {
        input: Box::new(input),
        items,
        output_qualifier: None,
        required_output_columns: None,
    }))
}
fn branch_union_output_columns(
    mut output_columns: Vec<OutputColumn>,
    branch_id_column: ColumnId,
) -> Vec<OutputColumn> {
    output_columns.push(OutputColumn {
        column_id: branch_id_column,
        name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_internal: true,
    });
    output_columns
}

fn plan_kind(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan(_) => "Scan",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Project(_) => "Project",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Union(_) => "Union",
        _ => "Other",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;
    use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
    use crate::engine::mv::refresh_context::tests_support::{
        make_mv_definition, make_pin, make_ref, make_schema_contract, make_target, parse_query,
    };
    use crate::meta::repository::mv_contract::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BranchIdColumnContract, BranchUnionContract,
    };
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, FilterNode, JoinNode, LogicalPlan, ProjectNode, ScanNode,
        UnionNode,
    };

    #[test]
    fn rewrites_top_union_of_aggregates_into_branch_scoped_merges() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlan::Union(UnionNode {
            inputs: vec![
                aggregate_over(scan("t1", 1)),
                aggregate_over(scan("t2", 10)),
            ],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            required_output_columns: None,
        }));

        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::Union(union)) =
            rule.apply(plan, &mut ctx).expect("rewrite")
        else {
            panic!("expected Changed(Union)");
        };

        assert_eq!(union.inputs.len(), 2);
        for (idx, branch) in union.inputs.iter().enumerate() {
            let LogicalPlan::Project(project) = branch else {
                panic!("expected Project branch");
            };
            let branch_item = project
                .items
                .iter()
                .find(|item| item.output_name.eq_ignore_ascii_case("__branch_id__"))
                .expect("branch id item");
            assert_branch_id_cast(branch_item, idx as i64);
            let LogicalPlan::ImvDelta(d) = project.input.as_ref() else {
                panic!(
                    "branch core must be a delegated ImvDelta, got {:?}",
                    project.input
                )
            };
            assert!(d.is_root, "branch sub-problem delta must be a root delta");
            assert_eq!(
                d.branch_scope.as_ref().map(|s| s.branch_id),
                Some(idx as i32)
            );
            assert!(
                matches!(d.input.as_ref(), LogicalPlan::Aggregate(_)),
                "delta must sit directly over the Aggregate core"
            );
        }
    }

    #[test]
    fn rewrites_project_over_aggregate_branches_into_branch_scoped_merges() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlan::Union(UnionNode {
            inputs: vec![
                project_over_aggregate(scan("t1", 1)),
                project_over_aggregate(scan("t2", 10)),
            ],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(30, "total")],
            required_output_columns: None,
        }));

        assert!(rule.matches(&plan, &ctx));
        let RewriteResult::Changed(LogicalPlan::Union(union)) =
            rule.apply(plan, &mut ctx).expect("rewrite")
        else {
            panic!("expected Changed(Union)");
        };

        assert_eq!(union.inputs.len(), 2);
        for (idx, branch) in union.inputs.iter().enumerate() {
            let LogicalPlan::Project(project) = branch else {
                panic!("expected Project branch");
            };
            let LogicalPlan::ImvDelta(d) = project.input.as_ref() else {
                panic!(
                    "branch core must be a delegated ImvDelta, got {:?}",
                    project.input
                )
            };
            assert!(d.is_root, "branch sub-problem delta must be a root delta");
            assert_eq!(
                d.branch_scope.as_ref().map(|s| s.branch_id),
                Some(idx as i32)
            );
            assert!(
                matches!(d.input.as_ref(), LogicalPlan::Aggregate(_)),
                "delta must sit directly over the Aggregate core"
            );
            assert!(project.items.iter().any(|item| {
                item.output_name == "total" && item.output_column_id == ColumnId::new_for_test(30)
            }));
            let branch_item = project
                .items
                .iter()
                .find(|item| item.output_name.eq_ignore_ascii_case("__branch_id__"))
                .expect("branch id item");
            assert_branch_id_cast(branch_item, idx as i64);
        }
    }

    #[test]
    fn rejects_non_aggregate_branch() {
        let rule = RewriteBranchUnionRule;
        let mut ctx = build_ctx();
        let plan = root_delta(LogicalPlan::Union(UnionNode {
            inputs: vec![aggregate_over(scan("t1", 1)), scan("t2", 10)],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            required_output_columns: None,
        }));

        let err = rule
            .apply(plan, &mut ctx)
            .expect_err("scan branch must fail");
        assert!(
            err.contains("supports only aggregate or Project-over-Aggregate branches"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn does_not_match_marked_union() {
        let rule = RewriteBranchUnionRule;
        let ctx = build_ctx();
        let plan = root_delta(LogicalPlan::Union(UnionNode {
            inputs: vec![
                LogicalPlan::ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode {
                    input: Box::new(aggregate_over(scan("t1", 1))),
                    is_root: false,
                    action_column: None,
                    branch_scope: None,
                }),
                aggregate_over(scan("t2", 10)),
            ],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            required_output_columns: None,
        }));

        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn does_not_match_projection_filter_union() {
        let rule = RewriteBranchUnionRule;
        let ctx = build_ctx();
        let plan = root_delta(LogicalPlan::Union(UnionNode {
            inputs: vec![project_over_filter("t1", 1), project_over_filter("t2", 10)],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(2, "amount")],
            required_output_columns: None,
        }));

        assert!(!rule.matches(&plan, &ctx));
    }

    #[test]
    fn pipeline_branch_union_of_aggregates_final_shape_is_stable() {
        use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;
        use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        // build_ctx() registers ice.db.b as the only known base table; both
        // branches must reference that same table so scan binding succeeds.
        let plan = LogicalPlan::Union(UnionNode {
            inputs: vec![aggregate_over(scan("b", 1)), aggregate_over(scan("b", 10))],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            required_output_columns: None,
        });

        let out = build_imv_pipeline()
            .rewrite(plan, &mut ctx)
            .expect("pipeline must succeed");

        // Top is a Union whose branches each end in Project over AggregateStateMerge,
        // carrying a __branch_id__ column, with no IMV marker left anywhere.
        assert!(
            !plan_contains_imv_marker(&out),
            "no marker may survive validation"
        );
        let LogicalPlan::Union(union) = &out else {
            panic!("expected top Union, got {out:?}")
        };
        assert_eq!(union.inputs.len(), 2);
        assert!(
            union
                .output_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case("__branch_id__")),
            "union output must expose __branch_id__"
        );
        for branch in &union.inputs {
            let LogicalPlan::Project(p) = branch else {
                panic!("expected Project branch, got {branch:?}")
            };
            assert!(
                matches!(p.input.as_ref(), LogicalPlan::AggregateStateMerge(_)),
                "expected Project over AggregateStateMerge, got {:?}",
                p.input
            );
            assert!(
                p.items
                    .iter()
                    .any(|i| i.output_name.eq_ignore_ascii_case("__branch_id__")),
                "branch Project must carry __branch_id__, items: {:?}",
                p.items
            );
        }
    }

    fn assert_branch_id_cast(item: &ProjectItem, expected_branch_id: i64) {
        assert_eq!(item.expr.data_type, DataType::Int32);
        assert!(!item.expr.nullable);
        let ExprKind::Cast { expr, target } = &item.expr.kind else {
            panic!("expected branch id Cast, got {:?}", item.expr.kind);
        };
        assert_eq!(*target, DataType::Int32);
        assert_eq!(expr.data_type, DataType::Int64);
        assert!(!expr.nullable);
        assert!(matches!(
            &expr.kind,
            ExprKind::Literal(LiteralValue::Int(value)) if *value == expected_branch_id
        ));
    }

    fn single_state_column(type_signature: &str) -> AggregateStateColumnContract {
        AggregateStateColumnContract {
            column_name: "__agg_state_s".to_string(),
            target_field_id: 200,
            type_signature: type_signature.to_string(),
            nullable: true,
            role: AggregateStateRoleContract::Single,
        }
    }

    fn retraction_count_state_column() -> AggregateStateColumnContract {
        AggregateStateColumnContract {
            column_name: "__agg_state___ivm_row_count".to_string(),
            target_field_id: 201,
            type_signature: "long".to_string(),
            nullable: false,
            role: AggregateStateRoleContract::RetractionCount,
        }
    }

    fn build_ctx() -> RewriteContext {
        let mut mv_def = make_mv_definition();
        mv_def.select_sql =
            "SELECT region, sum(amount) AS s FROM ice.db.b GROUP BY region".to_string();
        mv_def.primary_key_columns = vec!["region".to_string()];
        let mut contract = make_schema_contract();
        contract.target.visible_columns[0].output_name = "region".to_string();
        contract.target.visible_columns[1].output_name = "s".to_string();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.branch = Some(BranchUnionContract {
            branch_id_column: BranchIdColumnContract {
                column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN
                    .to_string(),
                target_field_id: 998,
            },
            branch_count: 2,
            inner_apply_key_source: ApplyKeySource::GroupRowId,
        });
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: vec![
                single_state_column("binary"),
                retraction_count_state_column(),
            ],
        });
        mv_def.schema_contract = Some(contract.clone());

        let target_schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "region",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "s",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        999,
                        "__row_id__",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        998,
                        "__branch_id__",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        200,
                        "__agg_state_s",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                    Arc::new(NestedField::required(
                        201,
                        "__agg_state___ivm_row_count",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        );
        let mv_ctx = Arc::new(
            IcebergMvRewriteContext::from_parts(
                make_target(),
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(mv_def),
                Arc::new(parse_query(
                    "SELECT region, sum(amount) AS s FROM ice.db.b GROUP BY region",
                )),
                Arc::from(vec![make_ref("ice", "db", "b")]),
                Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")])),
                Some(99),
                "uuid-tgt".to_string(),
                target_schema,
                Some(Arc::new(contract)),
            )
            .expect("aggregate rewrite context must build"),
        );

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn root_delta(input: LogicalPlan) -> LogicalPlan {
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
            group_by: vec![col_expr(1, "region")],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![col_expr(2, "amount")],
                distinct: false,
                result_type: DataType::Int64,
                order_by: Vec::new(),
                output_column_id: ColumnId::UNSET,
            }],
            output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    fn project_over_aggregate(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(aggregate_over(input)),
            items: vec![
                ProjectItem {
                    expr: col_expr(1, "region"),
                    output_name: "region".to_string(),
                    output_column_id: ColumnId::new_for_test(1),
                },
                ProjectItem {
                    expr: col_expr(3, "s"),
                    output_name: "total".to_string(),
                    output_column_id: ColumnId::new_for_test(30),
                },
            ],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn scan(name: &str, first_id: u32) -> LogicalPlan {
        let columns = vec![column_def("region"), column_def("amount")];
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
                output_column(first_id, "region"),
                output_column(first_id + 1, "amount"),
            ],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            variant_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    fn project_over_filter(name: &str, first_id: u32) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(filter_over(scan(name, first_id), first_id, "region")),
            items: vec![
                ProjectItem {
                    expr: col_expr(first_id, "region"),
                    output_name: "region".to_string(),
                    output_column_id: ColumnId::new_for_test(first_id),
                },
                ProjectItem {
                    expr: col_expr(first_id + 1, "amount"),
                    output_name: "amount".to_string(),
                    output_column_id: ColumnId::new_for_test(first_id + 1),
                },
            ],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn filter_over(input: LogicalPlan, column_id: u32, column: &str) -> LogicalPlan {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(input),
            predicate: TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_expr(column_id, column)),
                    op: BinOp::Ge,
                    right: Box::new(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(0)),
                        data_type: DataType::Int32,
                        nullable: false,
                    }),
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
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
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: name.eq_ignore_ascii_case("s"),
            is_internal: false,
        }
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn join_of(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        join_of_on(left, right, 1, 10)
    }

    fn join_of_on(
        left: LogicalPlan,
        right: LogicalPlan,
        left_region_id: u32,
        right_region_id: u32,
    ) -> LogicalPlan {
        // An inner equi-join on caller-selected region column ids.
        let condition = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_expr(left_region_id, "region")),
                op: BinOp::Eq,
                right: Box::new(col_expr(right_region_id, "region")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition: Some(condition),
            required_output_columns: None,
        })
    }

    fn assert_rule_changed(ctx: &RewriteContext, rule_name: &str) {
        use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;

        assert!(
            ctx.trace().events().iter().any(|event| {
                matches!(event, RewriteTraceEvent::RuleChanged { rule, .. } if *rule == rule_name)
            }),
            "{rule_name} must change the plan, trace: {:?}",
            ctx.trace().events()
        );
    }

    #[test]
    fn pipeline_aggregate_over_filtered_join_composes() {
        use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;
        use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        let join = join_of(scan("b", 1), scan("b", 10));
        let filtered = filter_over(join, 1, "region");
        let plan = aggregate_over(filtered);

        let out = build_imv_pipeline()
            .rewrite(plan, &mut ctx)
            .expect("aggregate over filtered join must compose");

        assert!(
            !plan_contains_imv_marker(&out),
            "no IMV marker may survive: {out:?}"
        );
        assert_rule_changed(&ctx, "RewriteJoinDelta");
    }

    #[test]
    fn pipeline_aggregate_over_nested_join_composes() {
        use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;
        use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        let inner = join_of(scan("b", 1), scan("b", 10));
        let outer = join_of_on(inner, scan("b", 20), 1, 20);
        let plan = aggregate_over(outer);

        let out = build_imv_pipeline()
            .rewrite(plan, &mut ctx)
            .expect("aggregate over nested join must compose");

        assert!(
            !plan_contains_imv_marker(&out),
            "no IMV marker may survive: {out:?}"
        );
        assert_rule_changed(&ctx, "RewriteJoinDelta");
    }

    #[test]
    fn pipeline_branch_union_of_project_over_aggregate_composes() {
        use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;
        use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        // project_over_aggregate outputs: region (id=1) and total (id=30).
        // Both branches reference the registered base "ice.db.b" so scan binding succeeds.
        let plan = LogicalPlan::Union(UnionNode {
            inputs: vec![
                project_over_aggregate(scan("b", 1)),
                project_over_aggregate(scan("b", 10)),
            ],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(30, "total")],
            required_output_columns: None,
        });

        let out = build_imv_pipeline()
            .rewrite(plan, &mut ctx)
            .expect("branch union of Project-over-Aggregate must compose");
        assert!(
            !plan_contains_imv_marker(&out),
            "no marker may survive: each Project-over-Aggregate branch must fully decompose"
        );
        let LogicalPlan::Union(union) = &out else {
            panic!("expected top Union, got {out:?}")
        };
        assert_eq!(union.inputs.len(), 2);
        assert!(
            union
                .output_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case("__branch_id__")),
            "union output must expose __branch_id__"
        );
        for branch in &union.inputs {
            let LogicalPlan::Project(p) = branch else {
                panic!("expected Project branch, got {branch:?}")
            };
            assert!(
                matches!(p.input.as_ref(), LogicalPlan::AggregateStateMerge(_)),
                "Project-over-Aggregate branch must land on AggregateStateMerge, got {:?}",
                p.input
            );
            assert!(
                p.items
                    .iter()
                    .any(|i| i.output_name.eq_ignore_ascii_case("__branch_id__")),
                "branch Project must carry __branch_id__, items: {:?}",
                p.items
            );
        }
    }

    #[test]
    fn pipeline_branch_union_of_aggregate_over_join_composes() {
        use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;
        use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;

        let mut ctx = build_ctx();
        let plan = LogicalPlan::Union(UnionNode {
            inputs: vec![
                aggregate_over(join_of(scan("b", 1), scan("b", 10))),
                aggregate_over(join_of(scan("b", 20), scan("b", 30))),
            ],
            all: true,
            output_columns: vec![output_column(1, "region"), output_column(3, "s")],
            required_output_columns: None,
        });

        let out = build_imv_pipeline()
            .rewrite(plan, &mut ctx)
            .expect("branch union of aggregate-over-join must compose");
        assert!(
            !plan_contains_imv_marker(&out),
            "no marker may survive: the inner joins must be delta-expanded and bound"
        );
    }
}
