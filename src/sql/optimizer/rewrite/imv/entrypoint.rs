//! Entrypoint for the IMV rewrite pipeline. See
//! docs/superpowers/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Instant;

use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;
use crate::sql::optimizer::rewrite::trace::RewriteTrace;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct ImvRewriteInput {
    pub plan: LogicalPlan,
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub disabled_rules: Vec<String>,
    pub deadline: Option<Instant>,
    /// Next free `ColumnId` value, taken from the `ColumnRefFactory` that
    /// produced `plan`. Seeds the IMV rewrite's internal ColumnId allocator
    /// so new columns (e.g. the action column) never collide with existing ids.
    pub next_column_id: u32,
}

#[derive(Debug)]
pub(crate) struct ImvRewriteOutcome {
    pub plan: LogicalPlan,
    pub trace: RewriteTrace,
    pub annotation: ImvPlanAnnotation,
}

pub(crate) fn run_imv_rewrite(input: ImvRewriteInput) -> Result<ImvRewriteOutcome, String> {
    let ImvRewriteInput {
        plan,
        mv_ctx,
        disabled_rules,
        deadline,
        next_column_id,
    } = input;

    let mut ctx_rw = RewriteContext::for_mv_refresh(disabled_rules);
    // Seed from the factory's next-free id (passed by the caller), guarding
    // against a degenerate 0 seed which would alias ColumnId::UNSET.
    let next_column_id = Arc::new(AtomicU32::new(next_column_id.max(1)));
    ctx_rw.set_extension::<ImvExtension>(ImvExtension {
        mv_ctx,
        annotation: ImvPlanAnnotation::default(),
        next_column_id,
    });
    if let Some(deadline) = deadline {
        ctx_rw.set_deadline(deadline);
    }

    let pipeline = build_imv_pipeline();
    let plan_out = pipeline.rewrite(plan, &mut ctx_rw)?;

    let ext = ctx_rw
        .extension::<ImvExtension>()
        .expect("ImvExtension installed before rewrite")
        .clone();

    Ok(ImvRewriteOutcome {
        plan: plan_out,
        trace: ctx_rw.trace().clone(),
        annotation: ext.annotation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::iceberg_target_apply::{
        ICEBERG_MV_APPLY_KEY_COLUMN, ICEBERG_MV_BRANCH_ID_COLUMN,
    };
    use crate::engine::mv::refresh_context::tests_support::{
        make_mv_definition, make_pin, make_ref, make_schema_contract, make_target, parse_query,
    };
    use crate::meta::repository::mv_contract::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource,
    };
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
    use crate::sql::optimizer::rewrite::imv::marker::{
        ImvVersionNode, ImvVersionRef, plan_contains_imv_marker,
    };
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, AggregateStateMergeNode, FilterNode, JoinNode, LogicalPlan,
        ProjectNode, ScanNode, UnionNode, ValuesNode,
    };
    use arrow::datatypes::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn dummy_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        crate::engine::mv::refresh_context::tests_support::dummy_rewrite_context()
    }

    fn empty_values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
            required_output_columns: None,
        })
    }

    fn iceberg_scan_plan() -> LogicalPlan {
        iceberg_scan_plan_with_column_id(1)
    }

    fn iceberg_scan_plan_with_column_id(column_id: u32) -> LogicalPlan {
        let column = ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![column],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
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
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                    binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId(column_id),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    fn top_level_project_filter_union_plan() -> LogicalPlan {
        LogicalPlan::Union(UnionNode {
            inputs: vec![project_filter_branch(1), project_filter_branch(10)],
            all: true,
            output_columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            required_output_columns: None,
        })
    }

    fn project_filter_branch(first_id: u32) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(iceberg_scan_plan_with_column_id(first_id)),
                predicate: TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(column_ref(first_id, "k", DataType::Int64, false)),
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
            })),
            items: vec![ProjectItem {
                expr: column_ref(first_id, "k", DataType::Int64, false),
                output_name: "k".to_string(),
                output_column_id: ColumnId(first_id),
            }],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn column_ref(id: u32, name: &str, data_type: DataType, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type,
            nullable,
        }
    }

    fn project_output_names(project: &ProjectNode) -> Vec<String> {
        project
            .items
            .iter()
            .map(|item| item.output_name.clone())
            .collect()
    }

    fn aggregate_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        let mut mv_def = make_mv_definition();
        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: vec![
                AggregateStateColumnContract {
                    column_name: "__agg_state_s".to_string(),
                    target_field_id: 200,
                    type_signature: "binary".to_string(),
                    nullable: true,
                    role: AggregateStateRoleContract::Single,
                },
                AggregateStateColumnContract {
                    column_name: "__agg_state___ivm_row_count".to_string(),
                    target_field_id: 201,
                    type_signature: "long".to_string(),
                    nullable: false,
                    role: AggregateStateRoleContract::RetractionCount,
                },
            ],
        });
        mv_def.schema_contract = Some(contract.clone());
        let target_schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "k",
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
                .expect("build target schema"),
        );
        Arc::new(
            IcebergMvRewriteContext::from_parts(
                make_target(),
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(mv_def),
                Arc::new(parse_query(
                    "SELECT k, sum(v) AS s FROM ice.db.b GROUP BY k",
                )),
                Arc::from(vec![make_ref("ice", "db", "b")]),
                Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")])),
                Some(99),
                "uuid-tgt".to_string(),
                target_schema,
                Some(Arc::new(contract)),
            )
            .expect("aggregate mv context must build"),
        )
    }

    fn aggregate_scan_plan() -> LogicalPlan {
        let columns = vec![
            ColumnDef {
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "v".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ];
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns,
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: match iceberg_scan_plan() {
                    LogicalPlan::Scan(scan) => scan.table.source,
                    _ => unreachable!(),
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: ColumnId(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId(2),
                    name: "v".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    fn aggregate_plan() -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(aggregate_scan_plan()),
            group_by: vec![TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: None,
                    column: "k".to_string(),
                },
                data_type: DataType::Int64,
                nullable: false,
            }],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(2),
                        qualifier: None,
                        column: "v".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                }],
                distinct: false,
                result_type: DataType::Int64,
                order_by: Vec::new(),
            }],
            output_columns: vec![
                OutputColumn {
                    column_id: ColumnId(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId(3),
                    name: "s".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    fn join_aggregate_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        let mut mv_def = make_mv_definition();
        mv_def.base_table_refs = vec!["ice.db.l".to_string(), "ice.db.r".to_string()];
        mv_def.last_refresh_snapshots = [
            ("ice.db.l".to_string(), 11i64),
            ("ice.db.r".to_string(), 33i64),
        ]
        .into_iter()
        .collect();
        mv_def.last_refresh_table_uuids = [
            ("ice.db.l".to_string(), "uuid-l".to_string()),
            ("ice.db.r".to_string(), "uuid-r".to_string()),
        ]
        .into_iter()
        .collect();
        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: vec![
                AggregateStateColumnContract {
                    column_name: "__agg_state_s".to_string(),
                    target_field_id: 200,
                    type_signature: "binary".to_string(),
                    nullable: true,
                    role: AggregateStateRoleContract::Single,
                },
                AggregateStateColumnContract {
                    column_name: "__agg_state___ivm_row_count".to_string(),
                    target_field_id: 201,
                    type_signature: "long".to_string(),
                    nullable: false,
                    role: AggregateStateRoleContract::RetractionCount,
                },
            ],
        });
        mv_def.schema_contract = Some(contract.clone());
        let target_schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "k",
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
                .expect("build target schema"),
        );
        Arc::new(
            IcebergMvRewriteContext::from_parts(
                make_target(),
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(mv_def),
                Arc::new(parse_query(
                    "SELECT l.k, sum(r.v) AS s FROM ice.db.l JOIN ice.db.r ON l.k = r.k GROUP BY l.k",
                )),
                Arc::from(vec![make_ref("ice", "db", "l"), make_ref("ice", "db", "r")]),
                Arc::new(make_pin(&[
                    ("ice.db.l", 22, "uuid-l"),
                    ("ice.db.r", 44, "uuid-r"),
                ])),
                Some(99),
                "uuid-tgt".to_string(),
                target_schema,
                Some(Arc::new(contract)),
            )
            .expect("join aggregate mv context must build"),
        )
    }

    fn join_base_scan(table: &str, first_id: u32, current_snapshot_id: i64) -> LogicalPlan {
        let columns = vec![
            ColumnDef {
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "v".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ];
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: table.to_string(),
                columns,
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: table.to_string(),
                        table_uuid: Some(format!("uuid-{table}")),
                        current_snapshot_id: Some(current_snapshot_id),
                        schema_id: 7,
                        location: format!("file:///tmp/ice/db/{table}"),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                        serialized_metadata_rows: None,
                    },
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                    binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                },
            },
            alias: Some(table.to_string()),
            columns: vec![
                OutputColumn {
                    column_id: ColumnId(first_id),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId(first_id + 1),
                    name: "v".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    fn project_all(input: LogicalPlan, first_id: u32) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: vec![
                ProjectItem {
                    expr: column_expr(first_id, "k", false),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId(first_id),
                },
                ProjectItem {
                    expr: column_expr(first_id + 1, "v", true),
                    output_name: "v".to_string(),
                    output_column_id: ColumnId(first_id + 1),
                },
            ],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn column_expr(id: u32, column: &str, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: column.to_string(),
            },
            data_type: DataType::Int64,
            nullable,
        }
    }

    fn join_aggregate_plan() -> LogicalPlan {
        let left = project_all(join_base_scan("l", 1, 22), 1);
        let right = project_all(join_base_scan("r", 10, 44), 10);
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Join(JoinNode {
                left: Box::new(left),
                right: Box::new(right),
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(column_expr(1, "k", false)),
                        op: BinOp::Eq,
                        right: Box::new(column_expr(10, "k", false)),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
                required_output_columns: None,
            })),
            group_by: vec![column_expr(1, "k", false)],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![column_expr(11, "v", true)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: Vec::new(),
            }],
            output_columns: vec![
                OutputColumn {
                    column_id: ColumnId(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId(12),
                    name: "s".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    // ── Task-3 helpers ──────────────────────────────────────────────────────

    /// Test-only rule that asserts ImvExtension is reachable from the
    /// RewriteContext. Captures whether the observed target fqn matched into
    /// an AtomicBool for assertion outside the rule.
    struct AssertMvCtxVisibleRule {
        saw_mv_ctx: Arc<AtomicBool>,
        expected_target: String,
    }

    impl LogicalRewriteRule for AssertMvCtxVisibleRule {
        fn name(&self) -> &'static str {
            "AssertMvCtxVisibleRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn traversal(&self) -> RewriteTraversal {
            RewriteTraversal::TopDown
        }

        fn matches(&self, _plan: &LogicalPlan, ctx: &RewriteContext) -> bool {
            let ext = ctx
                .extension::<ImvExtension>()
                .expect("ImvExtension installed");
            let t = &ext.mv_ctx.target;
            let fqn = format!("{}.{}.{}", t.catalog, t.namespace, t.table);
            if fqn == self.expected_target {
                self.saw_mv_ctx.store(true, Ordering::SeqCst);
            }
            false
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Unchanged)
        }
    }

    #[test]
    fn annotation_is_default_initialized_in_extension_slot() {
        // Disable WrapRootInImvDelta so the pipeline succeeds and we can
        // inspect the annotation; annotation initialization is independent
        // of whether wrapping occurs.
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            next_column_id: 100,
        })
        .unwrap();
        assert_eq!(
            format!("{:?}", outcome.annotation),
            format!("{:?}", ImvPlanAnnotation::default()),
        );
    }

    #[test]
    fn imv_rewrite_context_visible_through_extension() {
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        let mv_ctx = dummy_mv_ctx();
        let t = &mv_ctx.target;
        let expected_target = format!("{}.{}.{}", t.catalog, t.namespace, t.table);
        let saw_mv_ctx = Arc::new(AtomicBool::new(false));

        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-logical-normalize",
            RewritePhase::LogicalNormalize,
            vec![Box::new(AssertMvCtxVisibleRule {
                saw_mv_ctx: Arc::clone(&saw_mv_ctx),
                expected_target,
            })],
        )]);

        let mut ctx_rw = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx_rw.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(1)),
        });

        let _ = pipeline.rewrite(empty_values_plan(), &mut ctx_rw).unwrap();

        assert!(saw_mv_ctx.load(Ordering::SeqCst));
    }

    // ── Task-4 helpers ──────────────────────────────────────────────────────

    struct CountingRule {
        name: &'static str,
        matches_called: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl LogicalRewriteRule for CountingRule {
        fn name(&self) -> &'static str {
            self.name
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            self.matches_called
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            false
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Unchanged)
        }
    }

    #[test]
    fn disabled_imv_rule_skipped_with_trace() {
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;

        let matches_called = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-logical-normalize",
            RewritePhase::LogicalNormalize,
            vec![Box::new(CountingRule {
                name: "DummyImvRule",
                matches_called: Arc::clone(&matches_called),
            })],
        )]);

        let mut ctx_rw = RewriteContext::for_mv_refresh(vec!["DummyImvRule".to_string()]);
        ctx_rw.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_mv_ctx(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(1)),
        });

        let _ = pipeline.rewrite(empty_values_plan(), &mut ctx_rw).unwrap();

        assert_eq!(matches_called.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(ctx_rw.trace().events().iter().any(|e| matches!(
            e,
            RewriteTraceEvent::RuleSkipped { rule, reason, .. }
                if *rule == "DummyImvRule" && reason == "disabled"
        )));
    }

    #[test]
    fn unknown_disabled_rule_name_is_ignored() {
        // An unknown name in disabled_rules must not crash or produce a
        // pipeline-internal error. Disable WrapRootInImvDelta too so that
        // the pipeline can succeed and we can inspect the trace count.
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["NoSuchRule".to_string(), "WrapRootInImvDelta".to_string()],
            deadline: None,
            next_column_id: 100,
        })
        .expect("unknown disabled rule must not break the pipeline");

        assert_eq!(outcome.trace.stage_names().len(), 12);
    }

    // ── Task-5 helpers ──────────────────────────────────────────────────────

    struct FailingDummyRule;

    impl LogicalRewriteRule for FailingDummyRule {
        fn name(&self) -> &'static str {
            "FailingDummyRule"
        }

        fn phase(&self) -> RewritePhase {
            RewritePhase::LogicalNormalize
        }

        fn matches(&self, _plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
            true
        }

        fn apply(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Err("synthetic failure".to_string())
        }
    }

    #[test]
    fn failing_imv_rule_does_not_mutate_input_plan() {
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;

        let original = empty_values_plan();
        let before = format!("{original:?}");

        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-logical-normalize",
            RewritePhase::LogicalNormalize,
            vec![Box::new(FailingDummyRule)],
        )]);

        let mut ctx_rw = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx_rw.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx: dummy_mv_ctx(),
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(1)),
        });

        let plan = empty_values_plan();
        let err = pipeline.rewrite(plan, &mut ctx_rw).unwrap_err();
        assert_eq!(err, "synthetic failure");

        // Original plan binding is intact (Rust value semantics guarantee
        // this; the assert documents the contract for future readers).
        assert_eq!(format!("{original:?}"), before);

        assert!(ctx_rw.trace().events().iter().any(|e| matches!(
            e,
            RewriteTraceEvent::RuleFailed { rule, .. }
                if *rule == "FailingDummyRule"
        )));
    }

    // ── Pre-existing tests ──────────────────────────────────────────────────

    #[test]
    fn imv_pipeline_returns_err_on_plain_plan_in_pr_beta() {
        // PR-α: pipeline was identity. PR-β: wrap+validation rejects.
        // This test preserves the spirit of the original
        // empty_imv_pipeline_returns_input_plan_verbatim test by checking
        // the marker-rejection contract rather than identity.
        let err = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect_err("PR-β pipeline rejects plain plans");
        assert!(err.starts_with("IVM rewrite failed to resolve incremental markers:"));
    }

    // ── PR-β tests (Task 7) ─────────────────────────────────────────────────

    #[test]
    fn pr_beta_pipeline_runs_wrap_and_validation_against_plain_plan() {
        // End-to-end through run_imv_rewrite. Plain plan → wrap → validation
        // rejects → Err propagated to caller. This is PR-β's headline
        // behavior; iceberg-ivm continues to pass because
        // try_run_imv_rewrite_pipeline swallows the Err.
        let err = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect_err("PR-β pipeline must Reject on plain plan");
        assert!(
            err.starts_with("IVM rewrite failed to resolve incremental markers:"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn pr_beta_pipeline_passes_when_wrap_rule_disabled() {
        // If the user disables WrapRootInImvDelta, no marker is produced,
        // and Validation has nothing to reject. Confirms the disable
        // wire-up reaches the new rule.
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            next_column_id: 100,
        })
        .expect("disabled wrap rule must let the pipeline succeed");

        // outcome.plan must still be the original (no marker added).
        assert!(matches!(outcome.plan, LogicalPlan::Values(_)));
    }

    #[test]
    fn imv_pipeline_traces_stage_names() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            next_column_id: 100,
        })
        .expect("pipeline must succeed when wrap rule is disabled");

        assert_eq!(
            outcome.trace.stage_names(),
            vec![
                "imv-logical-normalize",
                "imv-delta-marker",
                "imv-branch-union",
                "imv-join-delta",
                "imv-union-delta",
                "imv-aggregate-state",
                "imv-delta-pushdown",
                "imv-scan-binding",
                "imv-action-propagation",
                "imv-apply-key",
                "imv-marker-cleanup",
                "imv-validation",
            ]
        );
    }

    #[test]
    fn imv_pipeline_binds_root_delta_scan() {
        // Disable InjectApplyKeyProject and ActionColumnValidation so this
        // test stays focused on scan binding (snapshot-id promotion) without
        // requiring a Project wrapper above the Scan.
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: iceberg_scan_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec![
                "InjectApplyKeyProject".to_string(),
                "ActionColumnValidation".to_string(),
            ],
            deadline: None,
            next_column_id: 100,
        })
        .expect("Delta(Scan) must bind successfully");

        let LogicalPlan::Scan(scan) = outcome.plan else {
            panic!("expected scan outcome");
        };
        match scan.table.source {
            ScanSource::IcebergDeltaTable {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => {
                assert_eq!(from_snapshot_id, 11);
                assert_eq!(to_snapshot_id, 22);
            }
            other => panic!("expected IcebergDeltaTable, got {other:?}"),
        }
    }

    #[test]
    fn imv_pipeline_binds_version_from_scan() {
        let plan = LogicalPlan::ImvVersion(ImvVersionNode {
            input: Box::new(iceberg_scan_plan()),
            version_ref: ImvVersionRef::from_snapshot(),
        });
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            next_column_id: 100,
        })
        .expect("Version(Scan, From) must bind and pass validation");

        let LogicalPlan::Scan(scan) = outcome.plan else {
            panic!("expected scan outcome");
        };
        match scan.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(snapshot_id, 11);
            }
            other => panic!("expected IcebergVersionTable, got {other:?}"),
        }
    }

    #[test]
    fn imv_pipeline_binds_version_to_scan() {
        let plan = LogicalPlan::ImvVersion(ImvVersionNode {
            input: Box::new(iceberg_scan_plan()),
            version_ref: ImvVersionRef::to_snapshot(),
        });
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            next_column_id: 100,
        })
        .expect("Version(Scan, To) must bind and pass validation");

        let LogicalPlan::Scan(scan) = outcome.plan else {
            panic!("expected scan outcome");
        };
        match scan.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(snapshot_id, 22);
            }
            other => panic!("expected IcebergVersionTable, got {other:?}"),
        }
    }

    #[test]
    fn imv_pipeline_injects_action_on_delta_scan() {
        // Disable InjectApplyKeyProject and ActionColumnValidation so this
        // test stays focused on __change_op injection into the Scan without
        // requiring a Project wrapper above the Scan.
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: iceberg_scan_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec![
                "InjectApplyKeyProject".to_string(),
                "ActionColumnValidation".to_string(),
            ],
            deadline: None,
            next_column_id: 100,
        })
        .expect("pipeline must succeed");

        let LogicalPlan::Scan(scan) = outcome.plan else {
            panic!("expected scan outcome");
        };
        let action = scan
            .columns
            .iter()
            .find(|c| c.is_internal && c.name.eq_ignore_ascii_case("__change_op"))
            .expect("action column must be present");
        assert_eq!(action.data_type, arrow::datatypes::DataType::Int8);
        assert!(!action.nullable);
    }

    #[test]
    fn imv_pipeline_propagates_action_through_project_end_to_end() {
        // Build Project(k) over the iceberg scan. The full pipeline must:
        // wrap → bind (DataFiles→DeltaTable) → inject __change_op on the scan
        // → propagate it into the Project → pass validation.
        let scan = iceberg_scan_plan();
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
                output_column_id: ColumnId(1),
            }],
            output_qualifier: None,
            required_output_columns: None,
        });

        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: project,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect("Project over delta scan must rewrite and pass validation");

        // Outcome root is a Project that exposes the propagated action column.
        let LogicalPlan::Project(project) = outcome.plan else {
            panic!("expected Project outcome, got {:?}", outcome.plan);
        };
        assert!(
            project
                .items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case("__change_op")),
            "Project must expose propagated action column; items: {:?}",
            project
                .items
                .iter()
                .map(|i| &i.output_name)
                .collect::<Vec<_>>()
        );
        // The user column is still present.
        assert!(
            project.items.iter().any(|item| item.output_name == "k"),
            "user column k must remain"
        );
        // The child scan is delta-bound and carries the internal action column.
        let LogicalPlan::Scan(scan) = *project.input else {
            panic!("expected Scan under Project");
        };
        assert!(
            matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. }),
            "child scan must be delta-bound"
        );
        assert!(
            scan.columns
                .iter()
                .any(|c| c.is_internal && c.name.eq_ignore_ascii_case("__change_op")),
            "child scan must carry the internal action column"
        );
    }

    #[test]
    fn imv_pipeline_rewrites_top_level_union_all_delta_end_to_end() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: top_level_project_filter_union_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect("top-level projection/filter UNION ALL must rewrite through the full IMV pipeline");

        assert!(
            !plan_contains_imv_marker(&outcome.plan),
            "final plan must not contain unresolved IMV markers: {:?}",
            outcome.plan
        );
        let LogicalPlan::Project(project) = &outcome.plan else {
            panic!("expected root apply-key Project, got {:?}", outcome.plan);
        };
        assert!(
            project.items.iter().any(|item| item
                .output_name
                .eq_ignore_ascii_case(ICEBERG_MV_BRANCH_ID_COLUMN)),
            "root output must include branch id; items: {:?}",
            project_output_names(project)
        );
        assert!(
            project
                .items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME)),
            "root output must include action column; items: {:?}",
            project_output_names(project)
        );
        assert!(
            project.items.iter().any(|item| item
                .output_name
                .eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)),
            "root output must include apply key; items: {:?}",
            project_output_names(project)
        );
        let LogicalPlan::Union(union) = project.input.as_ref() else {
            panic!("expected root Project over Union, got {:?}", project.input);
        };
        assert!(
            union.output_columns.iter().any(|column| column
                .name
                .eq_ignore_ascii_case(ICEBERG_MV_BRANCH_ID_COLUMN)),
            "Union output must include branch id"
        );
        assert!(
            union
                .output_columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)),
            "Union output must include action column"
        );
        for branch in &union.inputs {
            let LogicalPlan::Project(branch_project) = branch else {
                panic!("expected normalized branch Project, got {branch:?}");
            };
            assert_eq!(
                branch_project.items.len(),
                union.output_columns.len(),
                "branch Project output count must match Union output count"
            );
        }
    }

    #[test]
    fn imv_pipeline_rewrites_aggregate_refresh_to_state_merge() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            mv_ctx: aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect("aggregate IMV pipeline must rewrite and validate");

        let LogicalPlan::AggregateStateMerge(AggregateStateMergeNode { delta_input, .. }) =
            outcome.plan
        else {
            panic!("expected AggregateStateMerge");
        };
        let LogicalPlan::Project(delta_project) = delta_input.as_ref() else {
            panic!("expected signed aggregate projection delta input");
        };
        let LogicalPlan::Aggregate(delta_aggregate) = delta_project.input.as_ref() else {
            panic!("expected signed aggregate under projection");
        };
        assert_eq!(delta_aggregate.aggregates[0].name, "sum_state_signed");
        let LogicalPlan::Scan(scan) = delta_aggregate.input.as_ref() else {
            panic!("expected bound delta scan under signed aggregate");
        };
        assert!(
            matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. }),
            "signed aggregate input must be delta-bound"
        );
        assert!(
            scan.columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case("__change_op")),
            "delta scan must carry action column"
        );
        let action_id = scan
            .columns
            .iter()
            .find(|column| ImvActionColumn::matches(column))
            .expect("delta scan must carry action column")
            .column_id;
        let signed_input = &delta_aggregate.aggregates[0].args[0];
        let ExprKind::FunctionCall { args, .. } = &signed_input.kind else {
            panic!("expected signed state named_struct input");
        };
        let ExprKind::ColumnRef { column_id, .. } = &args[3].kind else {
            panic!("expected signed state input to reference action column");
        };
        assert_eq!(
            *column_id, action_id,
            "signed state input and delta scan must share the action ColumnId"
        );
    }

    #[test]
    fn imv_pipeline_rewrites_join_aggregate_refresh_to_bound_state_merge() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: join_aggregate_plan(),
            mv_ctx: join_aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect("join aggregate IMV pipeline must rewrite and validate");

        let LogicalPlan::AggregateStateMerge(AggregateStateMergeNode { delta_input, .. }) =
            outcome.plan
        else {
            panic!("expected AggregateStateMerge");
        };
        assert!(
            !plan_contains_imv_marker(&delta_input),
            "final delta input must not contain unresolved IMV markers"
        );
        let LogicalPlan::Project(delta_project) = delta_input.as_ref() else {
            panic!("expected signed aggregate projection delta input");
        };
        let LogicalPlan::Aggregate(delta_aggregate) = delta_project.input.as_ref() else {
            panic!("expected signed aggregate under projection");
        };
        assert_eq!(delta_aggregate.aggregates[0].name, "sum_state_signed");
        let signed_action_id = signed_action_column_id(delta_aggregate);

        let LogicalPlan::Union(union) = delta_aggregate.input.as_ref() else {
            panic!("expected join delta UnionAll under signed aggregate");
        };
        assert_join_delta_union_shape(union, signed_action_id);
    }

    #[test]
    fn query_rewrite_preserves_join_aggregate_action_column() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: join_aggregate_plan(),
            mv_ctx: join_aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            next_column_id: 100,
        })
        .expect("join aggregate IMV pipeline must rewrite and validate");

        let pipeline = query_rewrite_pipeline(&HashMap::new());
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let rewritten = pipeline
            .rewrite(outcome.plan, &mut ctx)
            .expect("query rewrite must preserve join aggregate delta action");

        let LogicalPlan::AggregateStateMerge(AggregateStateMergeNode { delta_input, .. }) =
            rewritten
        else {
            panic!("expected AggregateStateMerge after query rewrite");
        };
        let LogicalPlan::Project(delta_project) = delta_input.as_ref() else {
            panic!("expected signed aggregate projection delta input");
        };
        let LogicalPlan::Aggregate(delta_aggregate) = delta_project.input.as_ref() else {
            panic!("expected signed aggregate under projection");
        };
        let signed_action_id = signed_action_column_id(delta_aggregate);

        let LogicalPlan::Union(union) = delta_aggregate.input.as_ref() else {
            panic!("expected join delta UnionAll under signed aggregate");
        };
        assert!(
            union
                .output_columns
                .iter()
                .any(|column| column.column_id == signed_action_id
                    && column.name.eq_ignore_ascii_case("__change_op")),
            "Union output schema must retain action column after pruning"
        );
        assert_join_delta_union_shape(union, signed_action_id);
    }

    fn assert_join_delta_union_shape(
        union: &crate::sql::planner::plan::UnionNode,
        signed_action_id: ColumnId,
    ) {
        assert!(union.all);
        assert_eq!(union.inputs.len(), 2);
        assert!(
            union
                .output_columns
                .iter()
                .any(|column| column.column_id == signed_action_id
                    && column.name.eq_ignore_ascii_case("__change_op")),
            "Union output schema must include shared action column"
        );

        let mut delta_windows = Vec::new();
        let mut version_snapshots = Vec::new();
        for input in &union.inputs {
            let join = assert_normalized_branch(input, signed_action_id);
            collect_branch_binding(
                join.left.as_ref(),
                signed_action_id,
                &mut delta_windows,
                &mut version_snapshots,
            );
            collect_branch_binding(
                join.right.as_ref(),
                signed_action_id,
                &mut delta_windows,
                &mut version_snapshots,
            );
        }
        delta_windows.sort();
        version_snapshots.sort();
        assert_eq!(
            delta_windows,
            vec![("l".to_string(), 11, 22), ("r".to_string(), 33, 44)]
        );
        assert_eq!(
            version_snapshots,
            vec![("l".to_string(), 22), ("r".to_string(), 33)]
        );
    }

    fn assert_normalized_branch(plan: &LogicalPlan, signed_action_id: ColumnId) -> &JoinNode {
        let LogicalPlan::Project(project) = plan else {
            panic!("expected normalized branch Project");
        };
        assert!(
            project
                .items
                .iter()
                .any(|item| item.output_column_id == signed_action_id
                    && item.output_name.eq_ignore_ascii_case("__change_op")),
            "normalized branch Project must retain action column"
        );

        let LogicalPlan::Join(join) = project.input.as_ref() else {
            panic!("expected Project(Join)");
        };
        join
    }

    fn collect_branch_binding(
        plan: &LogicalPlan,
        signed_action_id: ColumnId,
        delta_windows: &mut Vec<(String, i64, i64)>,
        version_snapshots: &mut Vec<(String, i64)>,
    ) {
        let scan = assert_project_scan_any_table(plan);
        match &scan.table.source {
            ScanSource::IcebergDeltaTable {
                table,
                from_snapshot_id,
                to_snapshot_id,
            } => {
                let action = scan
                    .columns
                    .iter()
                    .find(|column| ImvActionColumn::matches(column))
                    .expect("delta scan must carry action column")
                    .column_id;
                assert_eq!(action, signed_action_id);
                delta_windows.push((table.table.clone(), *from_snapshot_id, *to_snapshot_id));
            }
            ScanSource::IcebergVersionTable { table, snapshot_id } => {
                assert!(
                    !scan.columns.iter().any(ImvActionColumn::matches),
                    "version scan must not carry action column"
                );
                version_snapshots.push((table.table.clone(), *snapshot_id));
            }
            other => panic!("expected delta/version scan source, got {other:?}"),
        }
    }

    fn signed_action_column_id(aggregate: &AggregateNode) -> ColumnId {
        let signed_input = &aggregate.aggregates[0].args[0];
        let ExprKind::FunctionCall { args, .. } = &signed_input.kind else {
            panic!("expected signed state named_struct input");
        };
        let ExprKind::ColumnRef { column_id, .. } = &args[3].kind else {
            panic!("expected signed state input to reference action column");
        };
        *column_id
    }

    fn assert_project_scan_any_table(plan: &LogicalPlan) -> &ScanNode {
        let LogicalPlan::Project(project) = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Scan(scan) = project.input.as_ref() else {
            panic!("expected Project(Scan)");
        };
        scan
    }
}
