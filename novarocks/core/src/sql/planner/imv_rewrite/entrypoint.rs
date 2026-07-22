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

//! Entrypoint for the IMV rewrite pipeline. See
//! docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use crate::mv::rewrite::context::IcebergMvRewriteContext;
use crate::sql::analysis::{ExprKind, OutputColumn, SortItem, TypedExpr};
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::trace::RewriteTrace;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
use crate::sql::planner::imv_rewrite::pipeline::build_imv_pipeline;
use crate::sql::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::sql::planner::optimizer_bridge::logical::{to_logical_plan, try_to_optimizer_expr};
use crate::sql::planner::payload::{AggregateCall, WindowExpr};

pub(crate) struct ImvRewriteInput {
    pub plan: LogicalPlanNode,
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub disabled_rules: Vec<String>,
    pub deadline: Option<Instant>,
    pub column_ref_factory: Rc<RefCell<ColumnRefFactory>>,
}

#[derive(Debug)]
pub(crate) struct ImvRewriteOutcome {
    pub plan: LogicalPlanNode,
    pub trace: RewriteTrace,
    pub annotation: ImvPlanAnnotation,
}

pub(crate) fn run_imv_rewrite(input: ImvRewriteInput) -> Result<ImvRewriteOutcome, String> {
    let ImvRewriteInput {
        plan,
        mv_ctx,
        disabled_rules,
        deadline,
        column_ref_factory,
    } = input;

    reserve_existing_plan_column_ids(&column_ref_factory, &plan);
    let mut ctx_rw = RewriteContext::for_mv_refresh(disabled_rules);
    ctx_rw.set_column_ref_factory(Rc::clone(&column_ref_factory));
    ctx_rw.set_extension::<ImvExtension>(ImvExtension {
        mv_ctx,
        annotation: ImvPlanAnnotation::default(),
    });
    if let Some(deadline) = deadline {
        ctx_rw.set_deadline(deadline);
    }

    // Boundary materialization for ImvRewriteInput: engine-side refresh code
    // hands this entrypoint a LogicalPlanNode, while the optimizer rewrite
    // pipeline operates on OptExpr. This is not a production rewrite
    // round-trip inside the optimizer.
    let scalars = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
    let opt_in = try_to_optimizer_expr(&plan, &mut scalars.borrow_mut())?;
    ctx_rw.set_scalar_arena(std::rc::Rc::clone(&scalars));

    let pipeline = build_imv_pipeline();
    let opt_out = pipeline.rewrite(opt_in, &mut ctx_rw)?;

    // Boundary materialization for ImvRewriteOutcome: callers outside the
    // optimizer still consume LogicalPlanNode. This is the optimizer-to-engine
    // exit, not an internal optimizer rewrite round-trip.
    let plan_out = to_logical_plan(opt_out, &scalars.borrow());

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

fn reserve_existing_plan_column_ids(
    column_ref_factory: &Rc<RefCell<ColumnRefFactory>>,
    plan: &LogicalPlanNode,
) {
    let mut max_id = 0u32;
    collect_plan_column_ids(plan, &mut max_id);
    if max_id > 0 {
        column_ref_factory
            .borrow_mut()
            .reserve_until(max_id.saturating_add(1));
    }
}

fn collect_plan_column_ids(plan: &LogicalPlanNode, max_id: &mut u32) {
    if let Some(required) = &plan.required_output_columns {
        for column_id in required {
            collect_column_id(*column_id, max_id);
        }
    }
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => {
            collect_output_columns(&scan.columns, max_id);
            for predicate in &scan.predicates {
                collect_expr_column_ids(predicate, max_id);
            }
            for variant in &scan.variant_columns {
                collect_column_id(variant.source_column_id, max_id);
                collect_column_id(variant.synthetic_column_id, max_id);
            }
        }
        LogicalPlanKind::Filter(filter) => collect_expr_column_ids(&filter.predicate, max_id),
        LogicalPlanKind::Project(project) => {
            for item in &project.items {
                collect_column_id(item.output_column_id, max_id);
                collect_expr_column_ids(&item.expr, max_id);
            }
        }
        LogicalPlanKind::Sort(sort) => {
            collect_sort_items(&sort.items, max_id);
            for expr in &sort.analytic_partition_by {
                collect_expr_column_ids(expr, max_id);
            }
            collect_output_columns(&sort.output_columns, max_id);
        }
        LogicalPlanKind::Values(values) => {
            collect_output_columns(&values.columns, max_id);
            for row in &values.rows {
                for expr in row {
                    collect_expr_column_ids(expr, max_id);
                }
            }
        }
        LogicalPlanKind::Repeat(repeat) => {
            for ids in &repeat.repeat_column_ref_ids {
                for column_id in ids {
                    collect_column_id(*column_id, max_id);
                }
            }
            for column_id in &repeat.all_rollup_column_ids {
                collect_column_id(*column_id, max_id);
            }
            for ids in &repeat.grouping_fn_arg_ids {
                for column_id in ids {
                    collect_column_id(*column_id, max_id);
                }
            }
            for (_, column_id) in &repeat.grouping_fn_ids {
                collect_column_id(*column_id, max_id);
            }
        }
        LogicalPlanKind::Window(window) => {
            for expr in &window.window_exprs {
                collect_window_expr_column_ids(expr, max_id);
            }
            collect_output_columns(&window.output_columns, max_id);
        }
        LogicalPlanKind::GenerateSeries(generate) => {
            collect_column_id(generate.output_column_id, max_id);
        }
        LogicalPlanKind::TableFunction(table_function) => {
            for arg in &table_function.args {
                collect_expr_column_ids(arg, max_id);
            }
            collect_output_columns(&table_function.output_columns, max_id);
        }
        LogicalPlanKind::Aggregate(aggregate) => {
            for expr in &aggregate.group_by {
                collect_expr_column_ids(expr, max_id);
            }
            for call in &aggregate.aggregates {
                collect_aggregate_call_column_ids(call, max_id);
            }
            collect_output_columns(&aggregate.output_columns, max_id);
        }
        LogicalPlanKind::Join(join) => {
            if let Some(condition) = &join.condition {
                collect_expr_column_ids(condition, max_id);
            }
        }
        LogicalPlanKind::Union(union) => collect_output_columns(&union.output_columns, max_id),
        LogicalPlanKind::Intersect(intersect) => {
            collect_output_columns(&intersect.output_columns, max_id)
        }
        LogicalPlanKind::Except(except) => collect_output_columns(&except.output_columns, max_id),
        LogicalPlanKind::CTEProduce(produce) => {
            collect_output_columns(&produce.output_columns, max_id)
        }
        LogicalPlanKind::CTEConsume(consume) => {
            collect_output_columns(&consume.output_columns, max_id)
        }
        LogicalPlanKind::Apply(apply) => {
            collect_expr_column_ids(&apply.subquery_expr, max_id);
            collect_output_column(&apply.output_column, max_id);
            collect_column_id(apply.inner_output_column_id, max_id);
            for column_id in &apply.correlation_column_ids {
                collect_column_id(*column_id, max_id);
            }
            for expr in &apply.correlation_conjuncts {
                collect_expr_column_ids(expr, max_id);
            }
            if let Some(predicate) = &apply.residual_predicate {
                collect_expr_column_ids(predicate, max_id);
            }
            for column_id in &apply.uncorrelated_outer_predicate_columns {
                collect_column_id(*column_id, max_id);
            }
        }
        LogicalPlanKind::ImvDelta(delta) => {
            if let Some(action_column) = delta.action_column {
                collect_column_id(action_column, max_id);
            }
        }
        LogicalPlanKind::Limit(_)
        | LogicalPlanKind::AssertOneRow(_)
        | LogicalPlanKind::CTEAnchor(_)
        | LogicalPlanKind::ImvVersion(_) => {}
    }
    for child in &plan.children {
        collect_plan_column_ids(child, max_id);
    }
}

fn collect_output_columns(columns: &[OutputColumn], max_id: &mut u32) {
    for column in columns {
        collect_output_column(column, max_id);
    }
}

fn collect_output_column(column: &OutputColumn, max_id: &mut u32) {
    collect_column_id(column.column_id, max_id);
}

fn collect_aggregate_call_column_ids(call: &AggregateCall, max_id: &mut u32) {
    collect_column_id(call.output_column_id, max_id);
    for arg in &call.args {
        collect_expr_column_ids(arg, max_id);
    }
    collect_sort_items(&call.order_by, max_id);
}

fn collect_window_expr_column_ids(window: &WindowExpr, max_id: &mut u32) {
    collect_column_id(window.output_column_id, max_id);
    for arg in &window.args {
        collect_expr_column_ids(arg, max_id);
    }
    for expr in &window.partition_by {
        collect_expr_column_ids(expr, max_id);
    }
    collect_sort_items(&window.order_by, max_id);
}

fn collect_sort_items(items: &[SortItem], max_id: &mut u32) {
    for item in items {
        collect_expr_column_ids(&item.expr, max_id);
    }
}

fn collect_expr_column_ids(expr: &TypedExpr, max_id: &mut u32) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => collect_column_id(*column_id, max_id),
        ExprKind::BinaryOp { left, right, .. } => {
            collect_expr_column_ids(left, max_id);
            collect_expr_column_ids(right, max_id);
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::Lambda { body: expr, .. }
        | ExprKind::LambdaFunction { body: expr, .. } => collect_expr_column_ids(expr, max_id),
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_column_ids(arg, max_id);
            }
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_expr_column_ids(arg, max_id);
            }
            collect_sort_items(order_by, max_id);
        }
        ExprKind::InList { expr, list, .. } => {
            collect_expr_column_ids(expr, max_id);
            for item in list {
                collect_expr_column_ids(item, max_id);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_expr_column_ids(expr, max_id);
            collect_expr_column_ids(low, max_id);
            collect_expr_column_ids(high, max_id);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_expr_column_ids(expr, max_id);
            collect_expr_column_ids(pattern, max_id);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_expr_column_ids(operand, max_id);
            }
            for (when_expr, then_expr) in when_then {
                collect_expr_column_ids(when_expr, max_id);
                collect_expr_column_ids(then_expr, max_id);
            }
            if let Some(else_expr) = else_expr {
                collect_expr_column_ids(else_expr, max_id);
            }
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_expr_column_ids(arg, max_id);
            }
            for expr in partition_by {
                collect_expr_column_ids(expr, max_id);
            }
            collect_sort_items(order_by, max_id);
        }
        ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

fn collect_column_id(column_id: ColumnId, max_id: &mut u32) {
    if column_id != ColumnId::UNSET {
        *max_id = (*max_id).max(column_id.0);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::mv::persistence::schema::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BRANCH_ID_COLUMN_NAME, BaseContract, BaseFieldRecord, BaseSchemaSnapshot,
        BranchIdColumnContract, BranchUnionContract, HIDDEN_APPLY_KEY_COLUMN_NAME,
        JOIN_APPLY_KEY_COLUMN_NAME, JoinContract, JoinContractKind, JoinPredicateLineage,
        MvSchemaContract, QualifiedFieldLineage,
    };
    use crate::mv::rewrite::context::tests_support::{
        make_mv_definition, make_pin, make_ref, make_schema_contract, make_target, parse_query,
    };
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::common::ImvVersionRef;
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
    use crate::sql::planner::imv_rewrite::annotation::ImvPartitionAnnotation;
    use crate::sql::planner::imv_rewrite::change_stream::AggregateChangeStreamShape;
    use crate::sql::planner::imv_rewrite::marker::plan_contains_imv_marker;
    use crate::sql::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
    use crate::sql::planner::logical::*;
    use crate::sql::planner::logical::{
        LogicalAggregateNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode, LogicalUnionNode,
    };
    use crate::sql::planner::payload::*;
    use crate::sql::planner::payload::{
        AggregateCall, PlanFilterNode, PlanProjectNode, PlanScanNode, PlanValuesNode,
    };
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use novarocks_catalog::schema::ColumnDef;

    /// Set up a fresh ScalarArena on `ctx`, convert `plan` to `OptExpr`, and
    /// return the `OptExpr`. Use this when calling `pipeline.rewrite()` directly
    /// in tests that don't go through `run_imv_rewrite`.
    fn plan_to_opt_expr_with_arena(plan: &LogicalPlanNode, ctx: &mut RewriteContext) -> OptExpr {
        let arena = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(std::rc::Rc::clone(&arena));
        crate::sql::planner::optimizer_bridge::logical::to_optimizer_expr(
            plan,
            &mut arena.borrow_mut(),
        )
    }
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn dummy_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        crate::mv::rewrite::context::tests_support::dummy_rewrite_context()
    }

    fn test_column_ref_factory() -> Rc<RefCell<ColumnRefFactory>> {
        Rc::new(RefCell::new(ColumnRefFactory::new()))
    }

    fn test_column_ref_factory_reserved_until(next_id: u32) -> Rc<RefCell<ColumnRefFactory>> {
        let factory = test_column_ref_factory();
        factory.borrow_mut().reserve_until(next_id);
        factory
    }

    fn optimize_logical_for_test(
        plan: LogicalPlanNode,
    ) -> crate::sql::optimizer::OptimizedOperatorNode {
        let mut scalar_arena = ScalarArena::new();
        let optimizer_expr = crate::sql::planner::optimizer_bridge::logical::to_optimizer_expr(
            &plan,
            &mut scalar_arena,
        );
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        factory.reserve_until(300);
        crate::sql::optimizer::optimize_with_legacy_table_stats_for_migration(
            optimizer_expr,
            scalar_arena,
            &HashMap::new(),
            factory,
            Vec::new(),
        )
        .expect("physical optimization")
    }

    fn empty_values_plan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )
    }

    fn iceberg_scan_plan() -> LogicalPlanNode {
        iceberg_scan_plan_with_column_id(1)
    }

    fn iceberg_scan_plan_with_column_id(column_id: u32) -> LogicalPlanNode {
        let column = ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
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
                        binding:
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
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
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn top_level_project_filter_union_plan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![OutputColumn {
                    column_id: ColumnId(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![project_filter_branch(1), project_filter_branch(10)],
            None,
        )
    }

    fn project_filter_branch(first_id: u32) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref(first_id, "k", DataType::Int64, false),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId(first_id),
                }],
                output_qualifier: None,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
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
                }),
                vec![iceberg_scan_plan_with_column_id(first_id)],
                None,
            )],
            None,
        )
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

    fn project_output_names(project: &PlanProjectNode) -> Vec<String> {
        project
            .items
            .iter()
            .map(|item| item.output_name.clone())
            .collect()
    }

    fn locator_join_left_input(plan: &LogicalPlanNode) -> &LogicalPlanNode {
        let LogicalPlanKind::Project(_) = &plan.kind else {
            panic!("expected root Project over target locator join, got {plan:?}");
        };
        let join_plan = plan.unary_input();
        let LogicalPlanKind::Join(join) = &join_plan.kind else {
            panic!("expected target locator Join under root Project, got {join_plan:?}");
        };
        assert_eq!(join.join_type, JoinKind::LeftOuter);
        let LogicalPlanKind::Scan(scan) = &join_plan.right().kind else {
            panic!("expected target locator scan on join right side");
        };
        assert!(
            matches!(scan.table.source, ScanSource::IcebergMvTargetLocator(_)),
            "join right side must be target locator scan"
        );
        join_plan.left()
    }

    fn find_delta_scan(plan: &LogicalPlanNode) -> Option<&PlanScanNode> {
        match &plan.kind {
            LogicalPlanKind::Scan(scan)
                if matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. }) =>
            {
                Some(scan)
            }
            _ => plan.children.iter().find_map(find_delta_scan),
        }
    }

    fn find_union_plan(plan: &LogicalPlanNode) -> Option<&LogicalPlanNode> {
        match &plan.kind {
            LogicalPlanKind::Union(_) => Some(plan),
            _ => plan.children.iter().find_map(find_union_plan),
        }
    }

    fn aggregate_mv_ctx_customized(
        mutate: impl FnOnce(&mut crate::mv::persistence::schema::MvSchemaContract),
    ) -> Arc<IcebergMvRewriteContext> {
        let mut mv_def = make_mv_definition();
        let mut contract = make_schema_contract();
        contract.target.visible_columns[0].output_name = "k".to_string();
        contract.target.visible_columns[1].output_name = "s".to_string();
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
        // Let the caller mutate the fully-built contract (e.g. attach a
        // partition spec or perturb output lineage) before it is cloned into
        // the mv definition and wrapped into the rewrite context.
        mutate(&mut contract);
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
            IcebergMvRewriteContext::from_definition_parts(
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

    fn aggregate_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        aggregate_mv_ctx_customized(|_| {})
    }

    fn partitioned_aggregate_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        use crate::mv::persistence::schema::{
            MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
        };
        aggregate_mv_ctx_customized(|contract| {
            contract.target.partition = Some(MvPartitionContract {
                target_spec_id: 7,
                fields: vec![MvPartitionFieldContract {
                    partition_field_id: 1000,
                    partition_field_name: "k".to_string(),
                    source_target_field_id: 100,
                    source_column_name: "k".to_string(),
                    transform: MvPartitionTransformContract::Identity,
                }],
            });
        })
    }

    fn aggregate_scan_plan() -> LogicalPlanNode {
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
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: "b".to_string(),
                    columns,
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: match &iceberg_scan_plan().kind {
                        LogicalPlanKind::Scan(scan) => scan.table.source.clone(),
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
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn aggregate_plan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
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
                    output_column_id: ColumnId(3),
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
            }),
            vec![aggregate_scan_plan()],
            None,
        )
    }

    fn join_aggregate_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        join_aggregate_mv_ctx_customized(|_| {})
    }

    fn join_projection_mv_ctx() -> Arc<IcebergMvRewriteContext> {
        crate::mv::rewrite::context::tests_support::join_projection_rewrite_context()
    }

    fn join_aggregate_mv_ctx_customized(
        mutate: impl FnOnce(&mut MvSchemaContract),
    ) -> Arc<IcebergMvRewriteContext> {
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
        contract.target.visible_columns[0].output_name = "k".to_string();
        contract.target.visible_columns[1].output_name = "s".to_string();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.bases = vec![
            join_base_contract("ice.db.l", "uuid-l", "l"),
            join_base_contract("ice.db.r", "uuid-r", "r"),
        ];
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids
            .clear();
        contract.output.columns[0].expression.referenced_base_fields =
            vec![qualified_field("ice.db.l", "l", 1)];
        contract.output.columns[1]
            .expression
            .referenced_base_field_ids
            .clear();
        contract.output.columns[1].expression.referenced_base_fields =
            vec![qualified_field("ice.db.r", "r", 2)];
        contract.join = Some(JoinContract {
            kind: JoinContractKind::InnerEquiJoin,
            predicates: vec![JoinPredicateLineage {
                left: qualified_field("ice.db.l", "l", 1),
                right: qualified_field("ice.db.r", "r", 1),
            }],
        });
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
        mutate(&mut contract);
        mv_def.schema_contract = Some(contract.clone());
        let mut target_fields = vec![
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
        ];
        if let Some(branch) = &contract.branch {
            target_fields.push(Arc::new(NestedField::required(
                branch.branch_id_column.target_field_id,
                branch.branch_id_column.column_name.clone(),
                Type::Primitive(PrimitiveType::Int),
            )));
        }
        let target_schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(target_fields)
                .build()
                .expect("build target schema"),
        );
        Arc::new(
            IcebergMvRewriteContext::from_definition_parts(
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

    fn join_base_contract(table_fqn: &str, table_uuid: &str, alias: &str) -> BaseContract {
        BaseContract {
            table_fqn: table_fqn.to_string(),
            table_uuid: table_uuid.to_string(),
            alias_at_create: Some(alias.to_string()),
            schema_id_at_create: 7,
            schema_at_create: BaseSchemaSnapshot {
                fields: vec![
                    BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "k".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    },
                    BaseFieldRecord {
                        field_id: 2,
                        name_at_create: "v".to_string(),
                        type_signature: "long".to_string(),
                        required: false,
                    },
                ],
            },
        }
    }

    fn qualified_field(table_fqn: &str, qualifier: &str, field_id: i32) -> QualifiedFieldLineage {
        QualifiedFieldLineage {
            table_fqn: table_fqn.to_string(),
            qualifier_at_create: qualifier.to_string(),
            field_id,
        }
    }

    fn join_base_scan(table: &str, first_id: u32, current_snapshot_id: i64) -> LogicalPlanNode {
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
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
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
                        binding:
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
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
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn project_all(input: LogicalPlanNode, first_id: u32) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
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
            }),
            vec![input],
            None,
        )
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

    fn join_aggregate_plan() -> LogicalPlanNode {
        let left = project_all(join_base_scan("l", 1, 22), 1);
        let right = project_all(join_base_scan("r", 10, 44), 10);
        LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![column_expr(1, "k", false)],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![column_expr(11, "v", true)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: ColumnId(12),
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
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Join(LogicalJoinNode {
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
                }),
                vec![left, right],
                None,
            )],
            None,
        )
    }

    fn join_projection_plan() -> LogicalPlanNode {
        let left = project_all(join_base_scan("l", 1, 22), 1);
        let right = project_all(join_base_scan("r", 10, 44), 10);
        let join = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
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
            }),
            vec![left, right],
            None,
        );
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_expr(1, "k", false),
                        output_name: "k".to_string(),
                        output_column_id: ColumnId(1),
                    },
                    ProjectItem {
                        expr: column_expr(11, "v", true),
                        output_name: "v".to_string(),
                        output_column_id: ColumnId(11),
                    },
                ],
                output_qualifier: None,
            }),
            vec![join],
            None,
        )
    }

    fn join_projection_filter_plan() -> LogicalPlanNode {
        let LogicalPlanKind::Project(project) = join_projection_plan().kind else {
            unreachable!("join projection helper must return Project");
        };
        let left = project_all(join_base_scan("l", 1, 22), 1);
        let right = project_all(join_base_scan("r", 10, 44), 10);
        let join = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
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
            }),
            vec![left, right],
            None,
        );
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(column_expr(1, "k", false)),
                        op: BinOp::Gt,
                        right: Box::new(TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Int(0)),
                            data_type: DataType::Int64,
                            nullable: false,
                        }),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![join],
            None,
        );
        LogicalPlanNode::new(LogicalPlanKind::Project(project), vec![filter], None)
    }

    fn join_projection_left_filter_plan() -> LogicalPlanNode {
        let LogicalPlanKind::Project(project) = join_projection_plan().kind else {
            unreachable!("join projection helper must return Project");
        };
        let left_scan = join_base_scan("l", 1, 22);
        let left_filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(column_expr(1, "k", false)),
                        op: BinOp::Gt,
                        right: Box::new(TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Int(0)),
                            data_type: DataType::Int64,
                            nullable: false,
                        }),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![left_scan],
            None,
        );
        let left = project_all(left_filter, 1);
        let right = project_all(join_base_scan("r", 10, 44), 10);
        let join = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
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
            }),
            vec![left, right],
            None,
        );
        LogicalPlanNode::new(LogicalPlanKind::Project(project), vec![join], None)
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

        fn matches(
            &self,
            _expr: &crate::sql::optimizer::opt_expr::OptExpr,
            ctx: &RewriteContext,
        ) -> bool {
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
            _expr: crate::sql::optimizer::opt_expr::OptExpr,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Unchanged)
        }
    }

    #[test]
    fn run_imv_rewrite_accepts_column_ref_factory() {
        let factory = test_column_ref_factory();

        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: iceberg_scan_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            column_ref_factory: std::rc::Rc::clone(&factory),
        })
        .expect("plain Iceberg scan should pass through IMV rewrite");

        assert_eq!(factory.borrow().peek_next_id(), 2);
        assert!(matches!(outcome.plan.kind, LogicalPlanKind::Scan(_)));
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
            column_ref_factory: test_column_ref_factory(),
        })
        .unwrap();
        assert_eq!(
            format!("{:?}", outcome.annotation),
            format!("{:?}", ImvPlanAnnotation::default()),
        );
    }

    #[test]
    fn run_imv_rewrite_normalizes_scan_mv_rewrite_sidecar() {
        let mut plan = iceberg_scan_plan();
        let LogicalPlanKind::Scan(scan) = &mut plan.kind else {
            panic!("expected scan plan");
        };
        scan.mv_rewritten_from = Some("mv_b".to_string());

        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("logical scan sidecars should not be stage validation errors");

        let LogicalPlanKind::Scan(scan) = &outcome.plan.kind else {
            panic!("expected scan plan");
        };
        assert_eq!(scan.mv_rewritten_from, None);
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
        });

        let opt_in = plan_to_opt_expr_with_arena(&empty_values_plan(), &mut ctx_rw);
        let _ = pipeline.rewrite(opt_in, &mut ctx_rw).unwrap();

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

        fn matches(
            &self,
            _expr: &crate::sql::optimizer::opt_expr::OptExpr,
            _ctx: &RewriteContext,
        ) -> bool {
            self.matches_called
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            false
        }

        fn apply(
            &self,
            _expr: crate::sql::optimizer::opt_expr::OptExpr,
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
        });

        let opt_in = plan_to_opt_expr_with_arena(&empty_values_plan(), &mut ctx_rw);
        let _ = pipeline.rewrite(opt_in, &mut ctx_rw).unwrap();

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
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("unknown disabled rule must not break the pipeline");

        assert!(
            outcome
                .trace
                .stage_names()
                .contains(&"imv-change-stream-descriptor"),
            "IMV pipeline should include the change-stream descriptor stage"
        );
    }

    #[test]
    fn imv_rewrite_outcome_has_no_external_allocator_state() {
        let _outcome = ImvRewriteOutcome {
            plan: empty_values_plan(),
            trace: RewriteTrace::default(),
            annotation: ImvPlanAnnotation::default(),
        };
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

        fn matches(
            &self,
            _expr: &crate::sql::optimizer::opt_expr::OptExpr,
            _ctx: &RewriteContext,
        ) -> bool {
            true
        }

        fn apply(
            &self,
            _expr: crate::sql::optimizer::opt_expr::OptExpr,
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
        });

        let plan = empty_values_plan();
        let opt_in = plan_to_opt_expr_with_arena(&plan, &mut ctx_rw);
        let err = pipeline.rewrite(opt_in, &mut ctx_rw).unwrap_err();
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
            column_ref_factory: test_column_ref_factory(),
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
            column_ref_factory: test_column_ref_factory(),
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
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("disabled wrap rule must let the pipeline succeed");

        // outcome.plan must still be the original (no marker added).
        assert!(matches!(&outcome.plan.kind, LogicalPlanKind::Values(_)));
    }

    #[test]
    fn imv_pipeline_traces_stage_names() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("pipeline must succeed when wrap rule is disabled");

        assert_eq!(
            outcome.trace.stage_names(),
            vec![
                "imv-logical-normalize",
                "imv-delta-marker",
                "imv-branch-union",
                "imv-union-delta",
                "imv-aggregate-state",
                "imv-delta-pushdown",
                "imv-scan-binding",
                "imv-action-propagation",
                "imv-apply-key",
                "imv-target-locator",
                "imv-change-stream-descriptor",
                "imv-partition-derivation",
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
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("Delta(Scan) must bind successfully");

        let LogicalPlanKind::Scan(scan) = &outcome.plan.kind else {
            panic!("expected scan outcome");
        };
        match &scan.table.source {
            ScanSource::IcebergDeltaTable {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => {
                assert_eq!(*from_snapshot_id, 11);
                assert_eq!(*to_snapshot_id, 22);
            }
            other => panic!("expected IcebergDeltaTable, got {other:?}"),
        }
    }

    #[test]
    fn imv_pipeline_binds_version_from_scan() {
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvVersion(LogicalImvVersionNode {
                version_ref: ImvVersionRef::from_snapshot(),
            }),
            vec![iceberg_scan_plan()],
            None,
        );
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("Version(Scan, From) must bind and pass validation");

        let LogicalPlanKind::Scan(scan) = &outcome.plan.kind else {
            panic!("expected scan outcome");
        };
        match &scan.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(*snapshot_id, 11);
            }
            other => panic!("expected IcebergVersionTable, got {other:?}"),
        }
    }

    #[test]
    fn imv_pipeline_binds_version_to_scan() {
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::ImvVersion(LogicalImvVersionNode {
                version_ref: ImvVersionRef::to_snapshot(),
            }),
            vec![iceberg_scan_plan()],
            None,
        );
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("Version(Scan, To) must bind and pass validation");

        let LogicalPlanKind::Scan(scan) = &outcome.plan.kind else {
            panic!("expected scan outcome");
        };
        match &scan.table.source {
            ScanSource::IcebergVersionTable { snapshot_id, .. } => {
                assert_eq!(*snapshot_id, 22);
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
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("pipeline must succeed");

        let LogicalPlanKind::Scan(scan) = &outcome.plan.kind else {
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
            vec![scan],
            None,
        );

        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: project,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("Project over delta scan must rewrite and pass validation");

        // Outcome root is a Project that exposes the propagated action column.
        let LogicalPlanKind::Project(project) = &outcome.plan.kind else {
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
        // The user plan is on the left side of the injected target locator join.
        let scan = find_delta_scan(locator_join_left_input(&outcome.plan))
            .expect("expected delta-bound scan under target locator join left side");
        assert!(
            scan.columns
                .iter()
                .any(|c| c.is_internal && c.name.eq_ignore_ascii_case("__change_op")),
            "child scan must carry the internal action column"
        );
    }

    #[test]
    fn imv_pipeline_projection_filter_outputs_target_locator_metadata() {
        let scan = iceberg_scan_plan();
        let project = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref(1, "k", DataType::Int64, false),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId(1),
                }],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );

        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: project,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory_reserved_until(100),
        })
        .expect("projection/filter rewrite must carry target locator metadata");

        let LogicalPlanKind::Project(project) = &outcome.plan.kind else {
            panic!("expected root Project, got {:?}", outcome.plan);
        };
        let output_names = project_output_names(project);
        assert!(
            output_names
                .iter()
                .any(|name| name
                    .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)),
            "root output must include target _file locator metadata; items: {output_names:?}"
        );
        assert!(
            output_names
                .iter()
                .any(|name| name
                    .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)),
            "root output must include target _pos locator metadata; items: {output_names:?}"
        );
        assert!(
            output_names.iter().any(
                |name| name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
            ),
            "root output must include target _row_id lineage metadata; items: {output_names:?}"
        );
        assert!(
            output_names.iter().any(|name| name
                .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)),
            "root output must include target _last_updated_sequence_number lineage metadata; items: {output_names:?}"
        );
    }

    #[test]
    fn imv_pipeline_rejects_preexisting_locator_metadata_name_collision() {
        let scan = iceberg_scan_plan();
        let project = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_ref(1, "k", DataType::Int64, false),
                        output_name: "k".to_string(),
                        output_column_id: ColumnId(1),
                    },
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::String(
                                "not-target-file".to_string(),
                            )),
                            data_type: DataType::Utf8,
                            nullable: false,
                        },
                        output_name: crate::exec::row_position::ICEBERG_FILE_PATH_COL.to_string(),
                        output_column_id: ColumnId(2),
                    },
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Int(7)),
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: crate::exec::row_position::ICEBERG_ROW_POS_COL.to_string(),
                        output_column_id: ColumnId(3),
                    },
                ],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );

        let err = run_imv_rewrite(ImvRewriteInput {
            plan: project,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory_reserved_until(100),
        })
        .expect_err("preexisting _file/_pos names must not bypass target locator injection");

        assert!(
            err.contains("reserved target locator metadata column"),
            "{err}"
        );
    }

    #[test]
    fn imv_pipeline_rewrites_top_level_union_all_delta_end_to_end() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: top_level_project_filter_union_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("top-level projection/filter UNION ALL must rewrite through the full IMV pipeline");

        assert!(
            !plan_contains_imv_marker(&outcome.plan),
            "final plan must not contain unresolved IMV markers: {:?}",
            outcome.plan
        );
        let LogicalPlanKind::Project(project) = &outcome.plan.kind else {
            panic!("expected root apply-key Project, got {:?}", outcome.plan);
        };
        assert!(
            project
                .items
                .iter()
                .any(|item| item.output_name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME)),
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
                .eq_ignore_ascii_case(HIDDEN_APPLY_KEY_COLUMN_NAME)),
            "root output must include apply key; items: {:?}",
            project_output_names(project)
        );
        let union_plan = find_union_plan(locator_join_left_input(&outcome.plan))
            .expect("expected union under target locator join left side");
        let LogicalPlanKind::Union(union) = &union_plan.kind else {
            panic!(
                "expected Union under target locator join left side, got {:?}",
                union_plan
            );
        };
        assert!(
            union
                .output_columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME)),
            "Union output must include branch id"
        );
        assert!(
            union
                .output_columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)),
            "Union output must include action column"
        );
        for branch in &union_plan.children {
            let LogicalPlanKind::Project(branch_project) = &branch.kind else {
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
    fn imv_pipeline_annotates_partition_spec_for_partitioned_aggregate() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            mv_ctx: partitioned_aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("aggregate IMV pipeline must rewrite and validate");

        let Some(ImvPartitionAnnotation::Derivable { specs }) = &outcome.annotation.partition
        else {
            panic!(
                "expected Derivable partition annotation, got {:?}",
                outcome.annotation.partition
            );
        };
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].target_spec_id, 7);
        assert_eq!(specs[0].fields.len(), 1);
        assert_eq!(specs[0].fields[0].partition_field_name, "k");
        assert_eq!(specs[0].fields[0].source_target_field_id, 100);
        assert_eq!(specs[0].fields[0].output_index, 0);
        assert_eq!(
            specs[0].fields[0].transform,
            iceberg::spec::Transform::Identity
        );
    }

    #[test]
    fn imv_pipeline_annotates_unpartitioned_for_plain_aggregate() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            mv_ctx: aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("aggregate IMV pipeline must rewrite and validate");
        assert_eq!(
            outcome.annotation.partition,
            Some(ImvPartitionAnnotation::Unpartitioned)
        );
    }

    #[test]
    fn imv_pipeline_annotates_not_derivable_for_non_pure_partition_lineage() {
        use crate::mv::persistence::schema::{
            ExpressionKind, MvPartitionContract, MvPartitionFieldContract,
            MvPartitionTransformContract,
        };
        let ctx = aggregate_mv_ctx_customized(|contract| {
            contract.target.partition = Some(MvPartitionContract {
                target_spec_id: 7,
                fields: vec![MvPartitionFieldContract {
                    partition_field_id: 1000,
                    partition_field_name: "k".to_string(),
                    source_target_field_id: 100,
                    source_column_name: "k".to_string(),
                    transform: MvPartitionTransformContract::Identity,
                }],
            });
            contract.output.columns[0].expression.kind = ExpressionKind::Func;
            contract.output.columns[0]
                .expression
                .referenced_base_field_ids = vec![1, 2];
        });
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            mv_ctx: ctx,
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("NotDerivable must not fail the rewrite");
        let Some(ImvPartitionAnnotation::NotDerivable { reason }) = &outcome.annotation.partition
        else {
            panic!(
                "expected NotDerivable, got {:?}",
                outcome.annotation.partition
            );
        };
        assert!(reason.contains("k"), "reason must name the field: {reason}");
    }

    #[test]
    fn imv_pipeline_leaves_partition_annotation_unset_for_projection_filter() {
        // Reuses the existing project-over-scan shape, so the rule never
        // matches and the slot stays None (P1 scope).
        let scan = iceberg_scan_plan();
        let project = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref(1, "k", DataType::Int64, false),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId(1),
                }],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: project,
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("projection/filter rewrite must succeed");
        assert!(outcome.annotation.partition.is_none());
    }

    #[test]
    fn imv_pipeline_rewrites_aggregate_refresh_to_state_merge() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            mv_ctx: aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("aggregate IMV pipeline must rewrite and validate");

        assert_aggregate_change_stream_outcome(&outcome);
        let delta_input = find_signed_delta_project(&outcome.plan);
        let LogicalPlanKind::Project(_) = &delta_input.kind else {
            panic!("expected signed aggregate projection delta input");
        };
        let delta_aggregate_plan = delta_input.unary_input();
        let LogicalPlanKind::Aggregate(delta_aggregate) = &delta_aggregate_plan.kind else {
            panic!("expected signed aggregate under projection");
        };
        assert_eq!(delta_aggregate.aggregates[0].name, "sum_state_signed");
        let LogicalPlanKind::Scan(scan) = &delta_aggregate_plan.unary_input().kind else {
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
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("join aggregate IMV pipeline must rewrite and validate");

        assert_aggregate_change_stream_outcome(&outcome);
        let delta_input = find_signed_delta_project(&outcome.plan);
        assert!(
            !plan_contains_imv_marker(delta_input),
            "final delta input must not contain unresolved IMV markers"
        );
        let LogicalPlanKind::Project(_) = &delta_input.kind else {
            panic!("expected signed aggregate projection delta input");
        };
        let delta_aggregate_plan = delta_input.unary_input();
        let LogicalPlanKind::Aggregate(delta_aggregate) = &delta_aggregate_plan.kind else {
            panic!("expected signed aggregate under projection");
        };
        assert_eq!(delta_aggregate.aggregates[0].name, "sum_state_signed");
        let signed_action_id = signed_action_column_id(delta_aggregate);

        let union_plan = delta_aggregate_plan.unary_input();
        let LogicalPlanKind::Union(_) = &union_plan.kind else {
            panic!("expected join delta UnionAll under signed aggregate");
        };
        assert_join_delta_union_shape(union_plan, signed_action_id);
        assert!(
            outcome.annotation.change_stream.has_aggregate(),
            "join aggregate refresh must use aggregate change-stream semantics"
        );
        assert!(
            outcome.annotation.change_stream.join_refresh.is_none(),
            "aggregate-over-join refresh must not record a pure join-refresh descriptor"
        );
    }

    #[test]
    fn join_aggregate_refresh_does_not_record_join_payload_descriptor() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: join_aggregate_plan(),
            mv_ctx: join_aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("join aggregate IMV pipeline must rewrite and validate");

        assert!(outcome.annotation.change_stream.has_aggregate());
        assert!(outcome.annotation.change_stream.join_refresh.is_none());
    }

    #[test]
    fn pure_join_refresh_pipeline_keeps_internal_outputs_above_projection() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: join_projection_plan(),
            mv_ctx: join_projection_mv_ctx(),
            disabled_rules: vec!["InjectTargetLocatorJoin".to_string()],
            deadline: None,
            column_ref_factory: test_column_ref_factory_reserved_until(30),
        })
        .expect("join projection IMV pipeline must rewrite and validate");
        let descriptor = outcome
            .annotation
            .change_stream
            .join_refresh
            .as_ref()
            .expect("join projection rewrite must record join refresh descriptor");
        let output_columns = crate::sql::planner::plan_output_columns(&outcome.plan)
            .expect("pipeline output columns");

        assert!(
            output_columns.iter().any(|column| column.column_id
                == descriptor.action_column.column_id
                && column.name.eq_ignore_ascii_case(ImvActionColumn::NAME)),
            "coalesce input must expose the recorded action column"
        );
        assert!(
            output_columns.iter().any(|column| {
                column.column_id == descriptor.join_apply_key_column.column_id
                    && column.name.eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)
            }),
            "coalesce input must expose the recorded join apply-key column"
        );
        assert!(
            !output_columns.iter().any(ImvRowIdColumn::matches),
            "raw base _row_id columns are join-key inputs, not change-stream outputs: {output_columns:?}"
        );

        let union_plan = find_union_plan(&outcome.plan).expect("pure join refresh must keep union");
        let LogicalPlanKind::Union(union) = &union_plan.kind else {
            panic!("expected pure join refresh union");
        };
        for branch in &union_plan.children {
            let LogicalPlanKind::Project(project) = &branch.kind else {
                panic!("expected normalized branch Project");
            };
            assert_eq!(
                project.items.len(),
                union.output_columns.len(),
                "branch Project output count must match pruned Union output count"
            );
            assert!(
                project.items.iter().all(|item| {
                    item.output_column_id != descriptor.left_row_id_column.column_id
                        && item.output_column_id != descriptor.right_row_id_column.column_id
                        && !item.output_name.eq_ignore_ascii_case(ImvRowIdColumn::NAME)
                }),
                "branch Project must not expose raw base row-id outputs after join apply-key injection: {:?}",
                project.items
            );
        }

        let optimized_tree = optimize_logical_for_test(outcome.plan.clone());
        assert_physical_project_refs_resolve_to_child_outputs(&optimized_tree);
        assert!(
            !optimized_tree
                .output_columns
                .iter()
                .any(ImvRowIdColumn::matches),
            "physical root must not advertise raw base _row_id columns as change-stream outputs: {:?}",
            optimized_tree.output_columns
        );
    }

    #[test]
    fn pure_join_refresh_union_branches_match_declared_output_schema() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: join_projection_plan(),
            mv_ctx: join_projection_mv_ctx(),
            disabled_rules: vec!["InjectTargetLocatorJoin".to_string()],
            deadline: None,
            column_ref_factory: test_column_ref_factory_reserved_until(30),
        })
        .expect("join projection IMV pipeline must rewrite and validate");
        let union_plan = find_union_plan(&outcome.plan).expect("join delta union");
        let LogicalPlanKind::Union(union) = &union_plan.kind else {
            panic!("expected Union");
        };
        let output_names = union
            .output_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();

        for child in &union_plan.children {
            let LogicalPlanKind::Project(project) = &child.kind else {
                panic!("expected normalized Project branch");
            };
            let child_names = project
                .items
                .iter()
                .map(|item| item.output_name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                child_names, output_names,
                "join refresh union branch output must match union schema"
            );
        }
    }

    #[test]
    fn pure_join_refresh_coalesce_plan_keeps_project_refs_in_child_scope() {
        let factory_cell = test_column_ref_factory_reserved_until(30);
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: join_projection_plan(),
            mv_ctx: join_projection_mv_ctx(),
            disabled_rules: vec!["InjectTargetLocatorJoin".to_string()],
            deadline: None,
            column_ref_factory: Rc::clone(&factory_cell),
        })
        .expect("join projection IMV pipeline must rewrite and validate");
        let descriptor = outcome
            .annotation
            .change_stream
            .join_refresh
            .as_ref()
            .expect("join projection rewrite must record join refresh descriptor");
        let coalesce = {
            let mut factory = factory_cell.borrow_mut();
            crate::sql::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                outcome.plan,
                descriptor,
                &crate::sql::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding {
                    target_table_uuid: "uuid-tgt".to_string(),
                    target_snapshot_id: Some(99),
                },
                &mut factory,
                200,
                201,
                202,
                203,
                204,
            )
        }
        .expect("join projection coalesce plan");

        assert_project_refs_resolve_to_child_outputs(&coalesce);
    }

    #[test]
    fn pure_join_refresh_optimized_tree_keeps_project_refs_in_child_scope() {
        std::thread::Builder::new()
            .name("imv-join-physical-scope-test".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let factory_cell = test_column_ref_factory_reserved_until(30);
                let outcome = run_imv_rewrite(ImvRewriteInput {
                    plan: join_projection_plan(),
                    mv_ctx: join_projection_mv_ctx(),
                    disabled_rules: vec!["InjectTargetLocatorJoin".to_string()],
                    deadline: None,
                    column_ref_factory: Rc::clone(&factory_cell),
                })
                .expect("join projection IMV pipeline must rewrite and validate");
                let descriptor = outcome
                    .annotation
                    .change_stream
                    .join_refresh
                    .as_ref()
                    .expect("join projection rewrite must record join refresh descriptor");
                let coalesce = {
                    let mut factory = factory_cell.borrow_mut();
                    crate::sql::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                        outcome.plan,
                        descriptor,
                        &crate::sql::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding {
                            target_table_uuid: "uuid-tgt".to_string(),
                            target_snapshot_id: Some(99),
                        },
                        &mut factory,
                        200,
                        201,
                        202,
                        203,
                        204,
                    )
                }
                .expect("join projection coalesce plan");
                let optimized_tree = optimize_logical_for_test(coalesce);

                assert_physical_project_refs_resolve_to_child_outputs(&optimized_tree);
            })
            .expect("spawn physical scope test")
            .join()
            .expect("physical scope test");
    }

    #[test]
    fn pure_join_refresh_filter_optimized_tree_keeps_action_refs_in_child_scope() {
        std::thread::Builder::new()
            .name("imv-join-filter-physical-scope-test".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let factory_cell = test_column_ref_factory_reserved_until(30);
                let outcome = run_imv_rewrite(ImvRewriteInput {
                    plan: join_projection_filter_plan(),
                    mv_ctx: join_projection_mv_ctx(),
                    disabled_rules: vec!["InjectTargetLocatorJoin".to_string()],
                    deadline: None,
                    column_ref_factory: Rc::clone(&factory_cell),
                })
                .expect("join projection/filter IMV pipeline must rewrite and validate");
                let descriptor = outcome
                    .annotation
                    .change_stream
                    .join_refresh
                    .as_ref()
                    .expect("join projection/filter rewrite must record join refresh descriptor");
                let coalesce = {
                    let mut factory = factory_cell.borrow_mut();
                    crate::sql::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                        outcome.plan,
                        descriptor,
                        &crate::sql::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding {
                            target_table_uuid: "uuid-tgt".to_string(),
                            target_snapshot_id: Some(99),
                        },
                        &mut factory,
                        200,
                        201,
                        202,
                        203,
                        204,
                    )
                }
                .expect("join projection/filter coalesce plan");
                let optimized_tree = optimize_logical_for_test(coalesce);

                crate::sql::planner::optimizer_bridge::id_binding::verify_optimized_tree_id_binding(
                    &optimized_tree,
                )
                .expect("join projection/filter physical coalesce plan must bind ids");
                assert_physical_project_refs_resolve_to_child_outputs(&optimized_tree);
            })
            .expect("spawn join filter physical scope test")
            .join()
            .expect("join filter physical scope test");
    }

    #[test]
    fn pure_join_refresh_side_filter_optimized_tree_keeps_action_refs_in_child_scope() {
        std::thread::Builder::new()
            .name("imv-join-side-filter-physical-scope-test".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let factory_cell = test_column_ref_factory_reserved_until(30);
                let outcome = run_imv_rewrite(ImvRewriteInput {
                    plan: join_projection_left_filter_plan(),
                    mv_ctx: join_projection_mv_ctx(),
                    disabled_rules: vec!["InjectTargetLocatorJoin".to_string()],
                    deadline: None,
                    column_ref_factory: Rc::clone(&factory_cell),
                })
                .expect("join projection side-filter IMV pipeline must rewrite and validate");
                let descriptor = outcome
                    .annotation
                    .change_stream
                    .join_refresh
                    .as_ref()
                    .expect("join side-filter rewrite must record join refresh descriptor");
                let coalesce = {
                    let mut factory = factory_cell.borrow_mut();
                    crate::sql::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                        outcome.plan,
                        descriptor,
                        &crate::sql::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding {
                            target_table_uuid: "uuid-tgt".to_string(),
                            target_snapshot_id: Some(99),
                        },
                        &mut factory,
                        200,
                        201,
                        202,
                        203,
                        204,
                    )
                }
                .expect("join side-filter coalesce plan");
                let optimized_tree = optimize_logical_for_test(coalesce);

                assert_physical_project_refs_resolve_to_child_outputs(&optimized_tree);
            })
            .expect("spawn join side-filter physical scope test")
            .join()
            .expect("join side-filter physical scope test");
    }

    fn bind_iceberg_scan_metadata_to_refresh_pin(
        mut plan: LogicalPlanNode,
        refresh_ctx: &IcebergMvRewriteContext,
    ) -> LogicalPlanNode {
        fn visit(plan: &mut LogicalPlanNode, refresh_ctx: &IcebergMvRewriteContext) {
            if let LogicalPlanKind::Scan(scan) = &mut plan.kind
                && let ScanSource::IcebergDataFiles { table, .. } = &mut scan.table.source
            {
                let fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
                if let Some(base_ref) = refresh_ctx
                    .base_refs
                    .iter()
                    .find(|base_ref| base_ref.fqn().eq_ignore_ascii_case(&fqn))
                {
                    table.current_snapshot_id = Some(
                        refresh_ctx
                            .pin
                            .get(base_ref)
                            .expect("test refresh pin covers base"),
                    );
                    table.table_uuid = Some(
                        refresh_ctx
                            .pin
                            .uuid(base_ref)
                            .expect("test refresh pin carries base uuid")
                            .to_string(),
                    );
                }
            }
            for child in &mut plan.children {
                visit(child, refresh_ctx);
            }
        }

        visit(&mut plan, refresh_ctx);
        plan
    }

    pub(crate) mod tests_support {
        use super::*;

        pub(crate) fn build_join_refresh_coalesce_plan_for_lowering(
            refresh_ctx: &Arc<IcebergMvRewriteContext>,
        ) -> crate::sql::optimizer::OptimizedOperatorNode {
            let plan = bind_iceberg_scan_metadata_to_refresh_pin(
                join_projection_plan(),
                refresh_ctx.as_ref(),
            );
            let factory_cell = test_column_ref_factory_reserved_until(30);
            let outcome = run_imv_rewrite(ImvRewriteInput {
                plan,
                mv_ctx: Arc::clone(refresh_ctx),
                disabled_rules: vec!["InjectTargetLocatorJoin".to_string()],
                deadline: None,
                column_ref_factory: Rc::clone(&factory_cell),
            })
            .expect("join projection IMV pipeline must rewrite and validate");
            let descriptor = outcome
                .annotation
                .change_stream
                .join_refresh
                .as_ref()
                .expect("join projection rewrite must record join refresh descriptor");
            let coalesce = {
                let mut factory = factory_cell.borrow_mut();
                crate::sql::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                    outcome.plan,
                    descriptor,
                    &crate::sql::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding::from_rewrite_context(refresh_ctx),
                    &mut factory,
                    200,
                    201,
                    202,
                    203,
                    204,
                )
            }
            .expect("join projection coalesce plan");
            optimize_logical_for_test(coalesce)
        }
    }

    #[test]
    fn imv_pipeline_uses_aggregate_change_stream_without_join_contract() {
        let ctx = join_aggregate_mv_ctx_customized(|contract| {
            contract.join = None;
            contract.branch = Some(BranchUnionContract {
                branch_id_column: BranchIdColumnContract {
                    column_name: BRANCH_ID_COLUMN_NAME.to_string(),
                    target_field_id: 4242,
                },
                branch_count: 2,
                inner_apply_key_source: ApplyKeySource::GroupRowId,
            });
        });

        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: join_aggregate_plan(),
            mv_ctx: ctx,
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("aggregate change stream should not require join refresh descriptor");

        assert!(outcome.annotation.change_stream.has_aggregate());
        assert!(outcome.annotation.change_stream.join_refresh.is_none());
    }

    #[test]
    fn query_rewrite_preserves_join_aggregate_action_column() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: join_aggregate_plan(),
            mv_ctx: join_aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("join aggregate IMV pipeline must rewrite and validate");

        let pipeline = query_rewrite_pipeline();
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_query_stats_input(
            crate::sql::optimizer::stats_input::OptimizerStatsInput::from_legacy_table_stats_for_migration(
                &HashMap::new(),
            ),
        );
        let opt_in = plan_to_opt_expr_with_arena(&outcome.plan, &mut ctx);
        let opt_out = pipeline
            .rewrite(opt_in, &mut ctx)
            .expect("query rewrite must preserve join aggregate delta action");
        let rewritten = crate::sql::planner::optimizer_bridge::logical::to_logical_plan(
            opt_out,
            &ctx.scalar_arena().borrow(),
        );

        assert_aggregate_change_stream_shape(&rewritten);
        let delta_input = find_signed_delta_project(&rewritten);
        let LogicalPlanKind::Project(_) = &delta_input.kind else {
            panic!("expected signed aggregate projection delta input");
        };
        let delta_aggregate_plan = delta_input.unary_input();
        let LogicalPlanKind::Aggregate(delta_aggregate) = &delta_aggregate_plan.kind else {
            panic!("expected signed aggregate under projection");
        };
        let signed_action_id = signed_action_column_id(delta_aggregate);

        let union_plan = delta_aggregate_plan.unary_input();
        let LogicalPlanKind::Union(union) = &union_plan.kind else {
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
        assert_join_delta_union_shape(union_plan, signed_action_id);
    }

    fn assert_aggregate_change_stream_outcome(outcome: &ImvRewriteOutcome) {
        let aggregate = outcome
            .annotation
            .change_stream
            .aggregate()
            .expect("expected aggregate change-stream descriptor");
        assert!(
            matches!(
                aggregate.shape,
                AggregateChangeStreamShape::UnionChangeStream
                    | AggregateChangeStreamShape::RelationalChangeStream
            ),
            "unexpected aggregate change-stream shape: {:?}",
            aggregate.shape
        );
        assert!(aggregate.target_state.present);
        assert!(aggregate.signed_state_aggregate.present);
        assert_aggregate_change_stream_shape(&outcome.plan);
    }

    fn assert_aggregate_change_stream_shape(plan: &LogicalPlanNode) {
        assert!(
            contains_target_state_scan(plan),
            "expected target-state old input scan in plan: {plan:?}"
        );
        assert!(
            contains_signed_delta_project(plan),
            "expected signed aggregate delta input in plan: {plan:?}"
        );
        assert!(
            !plan_contains_imv_marker(plan),
            "final aggregate change-stream plan must not contain IMV markers"
        );
    }

    fn assert_project_refs_resolve_to_child_outputs(plan: &LogicalPlanNode) {
        if let LogicalPlanKind::Project(project) = &plan.kind {
            let child_output_ids = crate::sql::planner::plan_output_columns(plan.unary_input())
                .expect("project child output columns")
                .into_iter()
                .map(|column| column.column_id)
                .collect::<HashSet<_>>();
            for item in &project.items {
                let mut refs = HashSet::new();
                collect_column_refs(&item.expr, &mut refs);
                for column_id in refs {
                    assert!(
                        child_output_ids.contains(&column_id),
                        "Project item `{}` references {column_id}, but child outputs are {:?}",
                        item.output_name,
                        child_output_ids
                    );
                }
            }
        }
        for child in &plan.children {
            assert_project_refs_resolve_to_child_outputs(child);
        }
    }

    fn assert_physical_project_refs_resolve_to_child_outputs(
        plan: &crate::sql::optimizer::OptimizedOperatorNode,
    ) {
        if matches!(
            &plan.op,
            crate::sql::optimizer::Operator::PhysicalHashJoin(_)
                | crate::sql::optimizer::Operator::PhysicalNestLoopJoin(_)
        ) {
            let child_output_ids = plan
                .children
                .iter()
                .flat_map(|child| child.output_columns.iter().map(|column| column.column_id))
                .collect::<HashSet<_>>();
            for column in &plan.output_columns {
                assert!(
                    child_output_ids.contains(&column.column_id),
                    "Physical join declares output `{}` {}, but children output {:?}",
                    column.name,
                    column.column_id,
                    child_output_ids
                );
            }
        }
        if let crate::sql::optimizer::Operator::PhysicalProject(project) = &plan.op {
            let child = plan
                .children
                .first()
                .expect("PhysicalProject must have one child");
            let child_output_ids = child
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<HashSet<_>>();
            let arena = plan
                .execution_props
                .scalar_arena
                .as_ref()
                .expect("physical plan must carry scalar arena");
            for item in &project.items {
                let refs =
                    crate::sql::optimizer::scalar_expr::collect_column_ids_strict(arena, item.expr)
                        .expect("project scalar must have resolved column ids");
                for column_id in refs {
                    assert!(
                        child_output_ids.contains(&column_id),
                        "PhysicalProject item `{}` references {column_id}, but child outputs are {:?}",
                        item.output_name,
                        child_output_ids
                    );
                }
            }
        }
        if let crate::sql::optimizer::Operator::PhysicalUnion(union) = &plan.op {
            for (idx, child) in plan.children.iter().enumerate() {
                assert_eq!(
                    child.output_columns.len(),
                    union.output_columns.len(),
                    "PhysicalUnion child {idx} output length must match union output length; child={:?}, union={:?}",
                    child
                        .output_columns
                        .iter()
                        .map(|column| format!("{}:{}", column.column_id, column.name))
                        .collect::<Vec<_>>(),
                    union
                        .output_columns
                        .iter()
                        .map(|column| format!("{}:{}", column.column_id, column.name))
                        .collect::<Vec<_>>()
                );
            }
        }
        for child in &plan.children {
            assert_physical_project_refs_resolve_to_child_outputs(child);
        }
    }

    fn collect_column_refs(expr: &TypedExpr, refs: &mut HashSet<ColumnId>) {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => {
                if *column_id != ColumnId::UNSET {
                    refs.insert(*column_id);
                }
            }
            ExprKind::BinaryOp { left, right, .. } => {
                collect_column_refs(left, refs);
                collect_column_refs(right, refs);
            }
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                for arg in args {
                    collect_column_refs(arg, refs);
                }
                if let ExprKind::AggregateCall { order_by, .. } = &expr.kind {
                    for item in order_by {
                        collect_column_refs(&item.expr, refs);
                    }
                }
            }
            ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::UnaryOp { expr, .. }
            | ExprKind::Nested(expr)
            | ExprKind::IsTruthValue { expr, .. } => collect_column_refs(expr, refs),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    collect_column_refs(operand, refs);
                }
                for (when, then) in when_then {
                    collect_column_refs(when, refs);
                    collect_column_refs(then, refs);
                }
                if let Some(else_expr) = else_expr {
                    collect_column_refs(else_expr, refs);
                }
            }
            ExprKind::InList { expr, list, .. } => {
                collect_column_refs(expr, refs);
                for item in list {
                    collect_column_refs(item, refs);
                }
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                collect_column_refs(expr, refs);
                collect_column_refs(low, refs);
                collect_column_refs(high, refs);
            }
            ExprKind::Like { expr, pattern, .. } => {
                collect_column_refs(expr, refs);
                collect_column_refs(pattern, refs);
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    collect_column_refs(arg, refs);
                }
                for expr in partition_by {
                    collect_column_refs(expr, refs);
                }
                for item in order_by {
                    collect_column_refs(&item.expr, refs);
                }
            }
            ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
                collect_column_refs(body, refs);
            }
            ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::SubqueryPlaceholder { .. } => {}
        }
    }

    fn find_signed_delta_project(plan: &LogicalPlanNode) -> &LogicalPlanNode {
        if let LogicalPlanKind::Project(_) = &plan.kind
            && matches!(
                &plan.unary_input().kind,
                LogicalPlanKind::Aggregate(LogicalAggregateNode { aggregates, .. })
                    if aggregates.iter().any(|call| call.name.ends_with("_state_signed"))
            )
        {
            return plan;
        }
        plan.children
            .iter()
            .find_map(|child| {
                if contains_signed_delta_project(child) {
                    Some(find_signed_delta_project(child))
                } else {
                    None
                }
            })
            .expect("expected signed aggregate projection")
    }

    fn contains_signed_delta_project(plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            LogicalPlanKind::Project(_)
                if matches!(
                    &plan.unary_input().kind,
                    LogicalPlanKind::Aggregate(LogicalAggregateNode { aggregates, .. })
                        if aggregates.iter().any(|call| call.name.ends_with("_state_signed"))
                )
        ) || plan.children.iter().any(contains_signed_delta_project)
    }

    fn contains_target_state_scan(plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            LogicalPlanKind::Scan(PlanScanNode {
                table: TableDef {
                    source: ScanSource::IcebergMvTargetState(_),
                    ..
                },
                ..
            })
        ) || plan.children.iter().any(contains_target_state_scan)
    }

    fn assert_join_delta_union_shape(union_plan: &LogicalPlanNode, signed_action_id: ColumnId) {
        let LogicalPlanKind::Union(union) = &union_plan.kind else {
            panic!("expected Union, got {union_plan:?}");
        };
        assert!(union.all);
        assert_eq!(union_plan.children.len(), 2);
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
        for input in &union_plan.children {
            let join = assert_normalized_branch(input, signed_action_id);
            collect_branch_binding(
                join.left(),
                signed_action_id,
                &mut delta_windows,
                &mut version_snapshots,
            );
            collect_branch_binding(
                join.right(),
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

    fn assert_normalized_branch(
        plan: &LogicalPlanNode,
        signed_action_id: ColumnId,
    ) -> &LogicalPlanNode {
        let LogicalPlanKind::Project(project) = &plan.kind else {
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

        let join_plan = plan.unary_input();
        let LogicalPlanKind::Join(_) = &join_plan.kind else {
            panic!("expected Project(Join)");
        };
        join_plan
    }

    fn collect_branch_binding(
        plan: &LogicalPlanNode,
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

    fn signed_action_column_id(aggregate: &LogicalAggregateNode) -> ColumnId {
        let signed_input = &aggregate.aggregates[0].args[0];
        let ExprKind::FunctionCall { args, .. } = &signed_input.kind else {
            panic!("expected signed state named_struct input");
        };
        let ExprKind::ColumnRef { column_id, .. } = &args[3].kind else {
            panic!("expected signed state input to reference action column");
        };
        *column_id
    }

    fn assert_project_scan_any_table(plan: &LogicalPlanNode) -> &PlanScanNode {
        let LogicalPlanKind::Project(_) = &plan.kind else {
            panic!("expected Project");
        };
        let LogicalPlanKind::Scan(scan) = &plan.unary_input().kind else {
            panic!("expected Project(Scan)");
        };
        scan
    }
}
