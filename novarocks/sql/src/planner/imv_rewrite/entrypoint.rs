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

use crate::analysis::{ExprKind, OutputColumn, SortItem, TypedExpr};
use crate::column_id::{ColumnId, ColumnRefFactory};
use crate::compiler::mv_rewrite::SqlImvRewriteSnapshot;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::trace::RewriteTrace;
use crate::optimizer::scalar::ScalarArena;
use crate::planner::imv_rewrite::annotation::{ImvExtension, ImvPlanAnnotation};
use crate::planner::imv_rewrite::pipeline::build_imv_pipeline;
use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::planner::optimizer_bridge::logical::{to_logical_plan, try_to_optimizer_expr};
use crate::planner::payload::{AggregateCall, WindowExpr};

pub(crate) struct ImvRewriteInput {
    pub plan: LogicalPlanNode,
    pub snapshot: Arc<SqlImvRewriteSnapshot>,
    pub disabled_rules: Vec<String>,
    pub deadline: Option<Instant>,
    pub column_ref_factory: Rc<RefCell<ColumnRefFactory>>,
    #[cfg(not(test))]
    pub function_catalog: Arc<dyn crate::compiler::SqlFunctionCatalog>,
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
        snapshot,
        disabled_rules,
        deadline,
        column_ref_factory,
        #[cfg(not(test))]
        function_catalog,
    } = input;

    reserve_existing_plan_column_ids(&column_ref_factory, &plan);
    let mut ctx_rw = RewriteContext::for_mv_refresh_with_settings(
        crate::optimizer::options::SessionOptimizerSettings {
            disabled_rules,
            ..Default::default()
        },
    );
    ctx_rw.set_column_ref_factory(Rc::clone(&column_ref_factory));
    #[cfg(test)]
    let function_catalog = crate::functions::test_function_catalog_snapshot();
    ctx_rw.set_function_catalog(function_catalog);
    ctx_rw.set_extension::<ImvExtension>(ImvExtension {
        snapshot,
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

/// Normalize a transparent root projection before IMV rewrite.  This is SQL
/// planning behavior, so application refresh adapters must not own a parallel
/// normalization path.
pub(crate) fn normalize_imv_rewrite_root_project(plan: LogicalPlanNode) -> LogicalPlanNode {
    let LogicalPlanNode {
        kind,
        mut children,
        required_output_columns,
    } = plan;
    let LogicalPlanKind::Project(project) = kind else {
        return LogicalPlanNode::new(kind, children, required_output_columns);
    };
    let input = children.remove(0);
    let LogicalPlanNode {
        kind: input_kind,
        children: aggregate_children,
        required_output_columns: aggregate_required_output_columns,
    } = input;
    let LogicalPlanKind::Aggregate(mut aggregate) = input_kind else {
        let input = LogicalPlanNode::new(
            input_kind,
            aggregate_children,
            aggregate_required_output_columns,
        );
        return LogicalPlanNode::new(
            LogicalPlanKind::Project(project),
            vec![input],
            required_output_columns,
        );
    };
    if project.items.len() != aggregate.output_columns.len() {
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(aggregate),
            aggregate_children,
            aggregate_required_output_columns,
        );
        return LogicalPlanNode::new(
            LogicalPlanKind::Project(project),
            vec![input],
            required_output_columns,
        );
    }
    let Some(output_columns) = project
        .items
        .iter()
        .zip(aggregate.output_columns.iter())
        .map(|(item, aggregate_output)| {
            let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind else {
                return None;
            };
            if *column_id != aggregate_output.column_id {
                return None;
            }
            Some(OutputColumn {
                column_id: *column_id,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
            })
        })
        .collect::<Option<Vec<_>>>()
    else {
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(aggregate),
            aggregate_children,
            aggregate_required_output_columns,
        );
        return LogicalPlanNode::new(
            LogicalPlanKind::Project(project),
            vec![input],
            required_output_columns,
        );
    };
    aggregate.output_columns = output_columns;
    LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(aggregate),
        aggregate_children,
        aggregate_required_output_columns,
    )
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

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };

    use crate::column_id::{ColumnId, ColumnRefFactory};
    #[allow(
        unused_imports,
        reason = "Used by package-level IMV rewrite tests that are not compiled in the workspace feature set."
    )]
    use crate::common::ImvVersionRef;
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::rewrite::context::RewriteContext;
    use crate::optimizer::rewrite::phase::RewritePhase;
    #[allow(
        unused_imports,
        reason = "Used by package-level IMV rewrite tests that are not compiled in the workspace feature set."
    )]
    use crate::optimizer::rewrite::registry::query_rewrite_pipeline;
    use crate::optimizer::rewrite::result::RewriteResult;
    use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
    use crate::optimizer::scalar::ScalarArena;
    use crate::planner::imv_rewrite::action_column::ImvActionColumn;
    #[allow(
        unused_imports,
        reason = "Used by package-level IMV rewrite tests that are not compiled in the workspace feature set."
    )]
    use crate::planner::imv_rewrite::annotation::ImvPartitionAnnotation;
    use crate::planner::imv_rewrite::change_stream::AggregateChangeStreamShape;
    use crate::planner::imv_rewrite::marker::plan_contains_imv_marker;
    #[allow(
        unused_imports,
        reason = "Used by package-level IMV rewrite tests that are not compiled in the workspace feature set."
    )]
    use crate::planner::imv_rewrite::row_id_column::ImvRowIdColumn;
    #[allow(
        unused_imports,
        reason = "Used by package-level IMV rewrite tests that are not compiled in the workspace feature set."
    )]
    use crate::planner::logical::LogicalImvVersionNode;
    use crate::planner::logical::{
        LogicalAggregateNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode, LogicalUnionNode,
    };

    use crate::planner::payload::{
        AggregateCall, PlanFilterNode, PlanProjectNode, PlanScanNode, PlanValuesNode,
    };
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    #[allow(
        unused_imports,
        reason = "Used by package-level IMV rewrite tests that are not compiled in the workspace feature set."
    )]
    use crate::planner::vocabulary::{
        BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME, JOIN_APPLY_KEY_COLUMN_NAME,
    };
    use arrow::datatypes::DataType;
    use novarocks_types::schema::ColumnDef;

    /// Set up a fresh ScalarArena on `ctx`, convert `plan` to `OptExpr`, and
    /// return the `OptExpr`. Use this when calling `pipeline.rewrite()` directly
    /// in tests that don't go through `run_imv_rewrite`.
    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn plan_to_opt_expr_with_arena(plan: &LogicalPlanNode, ctx: &mut RewriteContext) -> OptExpr {
        let arena = std::rc::Rc::new(std::cell::RefCell::new(ScalarArena::new()));
        ctx.set_scalar_arena(std::rc::Rc::clone(&arena));
        crate::planner::optimizer_bridge::logical::to_optimizer_expr(plan, &mut arena.borrow_mut())
    }
    use std::collections::{HashMap, HashSet};

    use std::sync::atomic::{AtomicBool, Ordering};

    fn test_column_ref_factory() -> Rc<RefCell<ColumnRefFactory>> {
        Rc::new(RefCell::new(ColumnRefFactory::new()))
    }

    fn test_column_ref_factory_reserved_until(next_id: u32) -> Rc<RefCell<ColumnRefFactory>> {
        let factory = test_column_ref_factory();
        factory.borrow_mut().reserve_until(next_id);
        factory
    }

    fn optimize_logical_for_test(plan: LogicalPlanNode) -> crate::optimizer::OptimizedOperatorNode {
        let mut scalar_arena = ScalarArena::new();
        let optimizer_expr =
            crate::planner::optimizer_bridge::logical::to_optimizer_expr(&plan, &mut scalar_arena);
        let mut factory = crate::column_id::ColumnRefFactory::new();
        factory.reserve_until(300);
        crate::optimizer::optimize_with_test_table_statistics(
            optimizer_expr,
            scalar_arena,
            &HashMap::new(),
            factory,
            Vec::new(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
        )
        .expect("physical optimization")
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn normalization_column_ref(column: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: column.column_id,
                qualifier: None,
                column: column.name.clone(),
            },
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        }
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn normalization_aggregate_outputs() -> (OutputColumn, OutputColumn, OutputColumn) {
        (
            normalization_output_column(1, "g1", DataType::Utf8, false),
            normalization_output_column(2, "g2", DataType::Utf8, false),
            normalization_output_column(11, "sum(amount)", DataType::Int64, true),
        )
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn normalization_output_column(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn normalization_project_item(
        source: &OutputColumn,
        output_id: u32,
        name: &str,
    ) -> ProjectItem {
        ProjectItem {
            expr: normalization_column_ref(source),
            output_name: name.to_string(),
            output_column_id: ColumnId(output_id),
        }
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn normalization_project_over_aggregate(project_items: Vec<ProjectItem>) -> LogicalPlanNode {
        let (g1, g2, sum_output) = normalization_aggregate_outputs();
        let child = LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![g1.clone(), g2.clone()],
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![normalization_column_ref(&g1), normalization_column_ref(&g2)],
                aggregates: vec![AggregateCall {
                    name: "count".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: sum_output.column_id,
                    resolved: crate::functions::test_resolved_aggregate("count", &[], false),
                }],
                output_columns: vec![g1, g2, sum_output],
                already_pushed: false,
            }),
            vec![child],
            None,
        );
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: project_items,
                output_qualifier: None,
            }),
            vec![aggregate],
            None,
        )
    }

    #[test]
    fn normalize_imv_rewrite_root_project_preserves_aggregate_output_identity() {
        let group_output = normalization_output_column(1, "region", DataType::Utf8, false);
        let aggregate_output =
            normalization_output_column(11, "sum(amount)", DataType::Int64, true);
        let child = LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![group_output.clone()],
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![normalization_column_ref(&group_output)],
                aggregates: vec![AggregateCall {
                    name: "count".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: aggregate_output.column_id,
                    resolved: crate::functions::test_resolved_aggregate("count", &[], false),
                }],
                output_columns: vec![group_output.clone(), aggregate_output.clone()],
                already_pushed: false,
            }),
            vec![child],
            None,
        );
        let root = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    normalization_project_item(&group_output, 21, "region"),
                    normalization_project_item(&aggregate_output, 22, "s"),
                ],
                output_qualifier: None,
            }),
            vec![aggregate],
            None,
        );

        let normalized = normalize_imv_rewrite_root_project(root);
        let LogicalPlanKind::Aggregate(aggregate) = &normalized.kind else {
            panic!(
                "expected normalized root Aggregate, got {:?}",
                normalized.kind
            );
        };
        assert_eq!(
            aggregate
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (group_output.column_id, "region"),
                (aggregate_output.column_id, "s")
            ]
        );
        assert_eq!(
            aggregate.aggregates[0].output_column_id,
            aggregate_output.column_id
        );

        let mut arena = ScalarArena::new();
        try_to_optimizer_expr(&normalized, &mut arena)
            .expect("normalized aggregate must satisfy optimizer bridge contract");
    }

    #[test]
    fn normalize_imv_rewrite_root_project_keeps_reordered_passthrough_project() {
        let (g1, g2, sum_output) = normalization_aggregate_outputs();
        let root = normalization_project_over_aggregate(vec![
            normalization_project_item(&g2, 21, "g2"),
            normalization_project_item(&g1, 22, "g1"),
            normalization_project_item(&sum_output, 23, "s"),
        ]);

        let normalized = normalize_imv_rewrite_root_project(root);
        assert!(matches!(&normalized.kind, LogicalPlanKind::Project(_)));
        let LogicalPlanKind::Aggregate(aggregate) = &normalized.unary_input().kind else {
            panic!("expected preserved Project over Aggregate");
        };
        assert_eq!(
            aggregate
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![g1.column_id, g2.column_id, sum_output.column_id]
        );
    }

    #[test]
    fn normalize_imv_rewrite_root_project_keeps_duplicate_passthrough_project() {
        let (g1, g2, sum_output) = normalization_aggregate_outputs();
        let root = normalization_project_over_aggregate(vec![
            normalization_project_item(&g1, 21, "g1"),
            normalization_project_item(&g1, 22, "g1_again"),
            normalization_project_item(&sum_output, 23, "s"),
        ]);

        let normalized = normalize_imv_rewrite_root_project(root);
        assert!(matches!(&normalized.kind, LogicalPlanKind::Project(_)));
        let LogicalPlanKind::Aggregate(aggregate) = &normalized.unary_input().kind else {
            panic!("expected preserved Project over Aggregate");
        };
        assert_eq!(
            aggregate
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![g1.column_id, g2.column_id, sum_output.column_id]
        );
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn iceberg_scan_plan() -> LogicalPlanNode {
        iceberg_scan_plan_with_column_id(1)
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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
                    source: ScanSource::Sql(SqlScanSource::new(
                        crate::compiler::mv_rewrite::test_target_binding(),
                        SqlTableIdentity {
                            catalog: "ice".to_string(),
                            namespace: "db".to_string(),
                            table: "b".to_string(),
                        },
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    )),
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn project_output_names(project: &PlanProjectNode) -> Vec<String> {
        project
            .items
            .iter()
            .map(|item| item.output_name.clone())
            .collect()
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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
            matches!(
                scan.table.source,
                ScanSource::Sql(SqlScanSource {
                    kind: SqlScanKind::MvTargetLocator { .. },
                    ..
                })
            ),
            "join right side must be target locator scan"
        );
        join_plan.left()
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn find_delta_scan(plan: &LogicalPlanNode) -> Option<&PlanScanNode> {
        match &plan.kind {
            LogicalPlanKind::Scan(scan)
                if matches!(
                    scan.table.source,
                    ScanSource::Sql(SqlScanSource {
                        kind: SqlScanKind::Delta { .. },
                        ..
                    })
                ) =>
            {
                Some(scan)
            }
            _ => plan.children.iter().find_map(find_delta_scan),
        }
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn find_union_plan(plan: &LogicalPlanNode) -> Option<&LogicalPlanNode> {
        match &plan.kind {
            LogicalPlanKind::Union(_) => Some(plan),
            _ => plan.children.iter().find_map(find_union_plan),
        }
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn dummy_mv_ctx() -> Arc<crate::compiler::mv_rewrite::SqlImvRewriteSnapshot> {
        crate::compiler::mv_rewrite::test_incremental_snapshot()
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn aggregate_mv_ctx_customized(
        mutate: impl FnOnce(&mut crate::compiler::mv_rewrite::SqlImvRewriteSnapshot),
    ) -> Arc<crate::compiler::mv_rewrite::SqlImvRewriteSnapshot> {
        let mut snapshot = (*crate::compiler::mv_rewrite::test_aggregate_snapshot(
            vec![
                crate::compiler::mv_rewrite::SqlImvAggregateStateColumnContract {
                    column_name: "__agg_state_s".to_string(),
                    type_signature: "binary".to_string(),
                    role: crate::compiler::mv_rewrite::SqlImvAggregateStateRoleContract::Single,
                },
                crate::compiler::mv_rewrite::SqlImvAggregateStateColumnContract {
                    column_name: "__agg_state___ivm_row_count".to_string(),
                    type_signature: "long".to_string(),
                    role: crate::compiler::mv_rewrite::SqlImvAggregateStateRoleContract::RetractionCount,
                },
            ],
            None,
            None,
        ))
        .clone();
        let contract = Arc::make_mut(&mut snapshot.schema_contract);
        contract.output_columns = vec![
            crate::compiler::mv_rewrite::SqlImvOutputColumnLineage {
                expression: crate::compiler::mv_rewrite::SqlImvExpressionLineage {
                    kind: crate::compiler::mv_rewrite::SqlImvExpressionKind::Column,
                    referenced_base_field_ids: vec![1],
                    referenced_base_fields: Vec::new(),
                },
            },
            crate::compiler::mv_rewrite::SqlImvOutputColumnLineage {
                expression: crate::compiler::mv_rewrite::SqlImvExpressionLineage {
                    kind: crate::compiler::mv_rewrite::SqlImvExpressionKind::Column,
                    referenced_base_field_ids: vec![2],
                    referenced_base_fields: Vec::new(),
                },
            },
        ];
        mutate(&mut snapshot);
        Arc::new(snapshot)
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn aggregate_mv_ctx() -> Arc<crate::compiler::mv_rewrite::SqlImvRewriteSnapshot> {
        aggregate_mv_ctx_customized(|_| {})
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn partitioned_aggregate_mv_ctx() -> Arc<crate::compiler::mv_rewrite::SqlImvRewriteSnapshot> {
        aggregate_mv_ctx_customized(|snapshot| {
            Arc::make_mut(&mut snapshot.schema_contract)
                .target
                .partition = Some(crate::compiler::mv_rewrite::SqlImvPartitionContract {
                target_spec_id: 7,
                fields: vec![crate::compiler::mv_rewrite::SqlImvPartitionField {
                    partition_field_name: "k".to_string(),
                    source_target_field_id: 100,
                    transform: crate::compiler::mv_rewrite::SqlImvPartitionTransform::Identity,
                }],
            });
        })
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn join_aggregate_mv_ctx() -> Arc<crate::compiler::mv_rewrite::SqlImvRewriteSnapshot> {
        crate::compiler::mv_rewrite::test_join_snapshot(true)
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn join_projection_mv_ctx() -> Arc<crate::compiler::mv_rewrite::SqlImvRewriteSnapshot> {
        crate::compiler::mv_rewrite::test_join_snapshot(false)
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn join_aggregate_mv_ctx_customized(
        mutate: impl FnOnce(&mut crate::compiler::mv_rewrite::SqlImvRewriteSnapshot),
    ) -> Arc<crate::compiler::mv_rewrite::SqlImvRewriteSnapshot> {
        let mut snapshot = (*crate::compiler::mv_rewrite::test_join_snapshot(true)).clone();
        mutate(&mut snapshot);
        Arc::new(snapshot)
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn aggregate_plan() -> LogicalPlanNode {
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
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: "b".to_string(),
                    columns,
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::compiler::mv_rewrite::test_data_scan_source(),
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
        );
        LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![column_ref(1, "k", DataType::Int64, false)],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: vec![column_ref(2, "v", DataType::Int64, true)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: ColumnId(3),
                    resolved: crate::functions::test_resolved_aggregate(
                        "sum",
                        &[DataType::Int64],
                        false,
                    ),
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
            vec![scan],
            None,
        )
    }

    fn join_base_scan(table: &str, first_id: u32, _current_snapshot_id: i64) -> LogicalPlanNode {
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
                    source: ScanSource::Sql(SqlScanSource::new(
                        crate::compiler::mv_rewrite::test_target_binding(),
                        SqlTableIdentity {
                            catalog: "ice".to_string(),
                            namespace: "db".to_string(),
                            table: table.to_string(),
                        },
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    )),
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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
                    resolved: crate::functions::test_resolved_aggregate(
                        "sum",
                        &[DataType::Int64],
                        false,
                    ),
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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
    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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
            _expr: &crate::optimizer::opt_expr::OptExpr,
            ctx: &RewriteContext,
        ) -> bool {
            let ext = ctx
                .extension::<ImvExtension>()
                .expect("ImvExtension installed");
            let t = &ext.snapshot.target;
            let fqn = format!("{}.{}.{}", t.catalog, t.namespace, t.table);
            if fqn == self.expected_target {
                self.saw_mv_ctx.store(true, Ordering::SeqCst);
            }
            false
        }

        fn apply(
            &self,
            _expr: crate::optimizer::opt_expr::OptExpr,
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
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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
        use crate::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};

        let snapshot = dummy_mv_ctx();
        let t = &snapshot.target;
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
            snapshot,
            annotation: ImvPlanAnnotation::default(),
        });

        let opt_in = plan_to_opt_expr_with_arena(&empty_values_plan(), &mut ctx_rw);
        let _ = pipeline.rewrite(opt_in, &mut ctx_rw).unwrap();

        assert!(saw_mv_ctx.load(Ordering::SeqCst));
    }

    // ── Task-4 helpers ──────────────────────────────────────────────────────

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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
            _expr: &crate::optimizer::opt_expr::OptExpr,
            _ctx: &RewriteContext,
        ) -> bool {
            self.matches_called
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            false
        }

        fn apply(
            &self,
            _expr: crate::optimizer::opt_expr::OptExpr,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Ok(RewriteResult::Unchanged)
        }
    }

    #[test]
    fn disabled_imv_rule_skipped_with_trace() {
        use crate::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::optimizer::rewrite::trace::RewriteTraceEvent;

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
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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
            _expr: &crate::optimizer::opt_expr::OptExpr,
            _ctx: &RewriteContext,
        ) -> bool {
            true
        }

        fn apply(
            &self,
            _expr: crate::optimizer::opt_expr::OptExpr,
            _ctx: &mut RewriteContext,
        ) -> Result<RewriteResult, String> {
            Err("synthetic failure".to_string())
        }
    }

    #[test]
    fn failing_imv_rule_does_not_mutate_input_plan() {
        use crate::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::optimizer::rewrite::trace::RewriteTraceEvent;

        let original = empty_values_plan();
        let before = format!("{original:?}");

        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-logical-normalize",
            RewritePhase::LogicalNormalize,
            vec![Box::new(FailingDummyRule)],
        )]);

        let mut ctx_rw = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx_rw.set_extension::<ImvExtension>(ImvExtension {
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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
    #[expect(
        unreachable_patterns,
        reason = "The test asserts the frozen SQL-source invariant while preserving its explicit failure diagnostic."
    )]
    fn imv_pipeline_binds_root_delta_scan() {
        // Disable InjectApplyKeyProject and ActionColumnValidation so this
        // test stays focused on scan binding (snapshot-id promotion) without
        // requiring a Project wrapper above the Scan.
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: iceberg_scan_plan(),
            snapshot: dummy_mv_ctx(),
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
            ScanSource::Sql(source) => match source.kind {
                SqlScanKind::Delta {
                    from_snapshot_id,
                    to_snapshot_id,
                } => {
                    assert_eq!(from_snapshot_id, 11);
                    assert_eq!(to_snapshot_id, 22);
                }
                ref other => panic!("expected delta SQL source, got {other:?}"),
            },
            other => panic!("expected token-bound SQL source, got {other:?}"),
        }
    }

    #[test]
    #[expect(
        unreachable_patterns,
        reason = "The test asserts the frozen SQL-source invariant while preserving its explicit failure diagnostic."
    )]
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
            snapshot: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("Version(Scan, From) must bind and pass validation");

        let LogicalPlanKind::Scan(scan) = &outcome.plan.kind else {
            panic!("expected scan outcome");
        };
        match &scan.table.source {
            ScanSource::Sql(source) => match source.kind {
                SqlScanKind::FrozenInputSet {
                    version: SqlTableVersionSelector::Snapshot(snapshot_id),
                } => assert_eq!(snapshot_id, 11),
                ref other => panic!("expected frozen-input SQL source, got {other:?}"),
            },
            other => panic!("expected token-bound SQL source, got {other:?}"),
        }
    }

    #[test]
    #[expect(
        unreachable_patterns,
        reason = "The test asserts the frozen SQL-source invariant while preserving its explicit failure diagnostic."
    )]
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
            snapshot: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("Version(Scan, To) must bind and pass validation");

        let LogicalPlanKind::Scan(scan) = &outcome.plan.kind else {
            panic!("expected scan outcome");
        };
        match &scan.table.source {
            ScanSource::Sql(source) => match source.kind {
                SqlScanKind::FrozenInputSet {
                    version: SqlTableVersionSelector::Snapshot(snapshot_id),
                } => assert_eq!(snapshot_id, 22),
                ref other => panic!("expected frozen-input SQL source, got {other:?}"),
            },
            other => panic!("expected token-bound SQL source, got {other:?}"),
        }
    }

    #[test]
    fn imv_pipeline_injects_action_on_delta_scan() {
        // Disable InjectApplyKeyProject and ActionColumnValidation so this
        // test stays focused on __change_op injection into the Scan without
        // requiring a Project wrapper above the Scan.
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: iceberg_scan_plan(),
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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
                .any(|name| name.eq_ignore_ascii_case(crate::common::ICEBERG_FILE_PATH_COL)),
            "root output must include target _file locator metadata; items: {output_names:?}"
        );
        assert!(
            output_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(crate::common::ICEBERG_ROW_POS_COL)),
            "root output must include target _pos locator metadata; items: {output_names:?}"
        );
        assert!(
            output_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(crate::common::ICEBERG_ROW_ID_COL)),
            "root output must include target _row_id lineage metadata; items: {output_names:?}"
        );
        assert!(
            output_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(crate::common::ICEBERG_LAST_UPDATED_SEQ_COL)),
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
                        output_name: crate::common::ICEBERG_FILE_PATH_COL.to_string(),
                        output_column_id: ColumnId(2),
                    },
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::Literal(LiteralValue::Int(7)),
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: crate::common::ICEBERG_ROW_POS_COL.to_string(),
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
            snapshot: dummy_mv_ctx(),
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
            snapshot: dummy_mv_ctx(),
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
            snapshot: partitioned_aggregate_mv_ctx(),
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
            crate::compiler::mv_rewrite::SqlImvPartitionTransform::Identity
        );
    }

    #[test]
    fn imv_pipeline_annotates_unpartitioned_for_plain_aggregate() {
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            snapshot: aggregate_mv_ctx(),
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
        let ctx = aggregate_mv_ctx_customized(|snapshot| {
            let contract = Arc::make_mut(&mut snapshot.schema_contract);
            contract.target.partition =
                Some(crate::compiler::mv_rewrite::SqlImvPartitionContract {
                    target_spec_id: 7,
                    fields: vec![crate::compiler::mv_rewrite::SqlImvPartitionField {
                        partition_field_name: "k".to_string(),
                        source_target_field_id: 100,
                        transform: crate::compiler::mv_rewrite::SqlImvPartitionTransform::Identity,
                    }],
                });
            contract.output_columns[0].expression.kind =
                crate::compiler::mv_rewrite::SqlImvExpressionKind::Func;
            contract.output_columns[0]
                .expression
                .referenced_base_field_ids = vec![1, 2];
        });
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: aggregate_plan(),
            snapshot: ctx,
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
            snapshot: dummy_mv_ctx(),
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
            snapshot: aggregate_mv_ctx(),
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
            matches!(
                &scan.table.source,
                ScanSource::Sql(source) if matches!(source.kind, SqlScanKind::Delta { .. })
            ),
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
            snapshot: join_aggregate_mv_ctx(),
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
            snapshot: join_aggregate_mv_ctx(),
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
            snapshot: join_projection_mv_ctx(),
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
        let output_columns =
            crate::planner::plan_output_columns(&outcome.plan).expect("pipeline output columns");

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
            snapshot: join_projection_mv_ctx(),
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
            snapshot: join_projection_mv_ctx(),
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
            crate::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                outcome.plan,
                descriptor,
                &crate::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding {
                    target_binding: crate::compiler::mv_rewrite::test_target_binding(),
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
                    snapshot: join_projection_mv_ctx(),
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
                    crate::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                        outcome.plan,
                        descriptor,
                        &crate::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding {
                            target_binding: crate::compiler::mv_rewrite::test_target_binding(),
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
                    snapshot: join_projection_mv_ctx(),
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
                    crate::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                        outcome.plan,
                        descriptor,
                        &crate::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding {
                            target_binding: crate::compiler::mv_rewrite::test_target_binding(),
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

                crate::planner::optimizer_bridge::id_binding::verify_optimized_tree_id_binding(
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
                    snapshot: join_projection_mv_ctx(),
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
                    crate::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                        outcome.plan,
                        descriptor,
                        &crate::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding {
                            target_binding: crate::compiler::mv_rewrite::test_target_binding(),
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

    pub(crate) mod tests_support {
        use super::*;

        /// Request-local token assignment for the join-refresh coalesce
        /// lowering fixture. The old SQL-only builder intentionally reused
        /// its placeholder token for every scan. Owner tests that exercise
        /// preparation must instead retain one exact binding per physical
        /// base/target identity.
        #[derive(Clone, Copy, Debug)]
        pub(crate) struct JoinRefreshCoalesceBindingTokens {
            pub(crate) left: crate::binding::SqlTableBindingId,
            pub(crate) right: crate::binding::SqlTableBindingId,
            pub(crate) target: crate::binding::SqlTableBindingId,
        }

        impl JoinRefreshCoalesceBindingTokens {
            pub(crate) fn for_scope(scope: crate::binding::SqlTableBindingScopeId) -> Self {
                use std::num::NonZeroU32;

                Self {
                    left: crate::binding::SqlTableBindingId::new(
                        scope,
                        NonZeroU32::new(1).expect("nonzero fixture ordinal"),
                    ),
                    right: crate::binding::SqlTableBindingId::new(
                        scope,
                        NonZeroU32::new(2).expect("nonzero fixture ordinal"),
                    ),
                    target: crate::binding::SqlTableBindingId::new(
                        scope,
                        NonZeroU32::new(3).expect("nonzero fixture ordinal"),
                    ),
                }
            }
        }

        pub(crate) fn build_join_refresh_coalesce_plan_for_lowering()
        -> crate::optimizer::OptimizedOperatorNode {
            let plan = join_projection_plan();
            let factory_cell = test_column_ref_factory_reserved_until(30);
            let snapshot = crate::compiler::mv_rewrite::test_join_snapshot(false);
            let outcome = run_imv_rewrite(ImvRewriteInput {
                plan,
                snapshot: Arc::clone(&snapshot),
                disabled_rules: vec!["InjectTargetLocatorJoin".to_string()],
                deadline: None,
                column_ref_factory: Rc::clone(&factory_cell),
                #[cfg(not(test))]
                function_catalog: crate::functions::test_function_catalog_snapshot(),
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
                crate::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                    outcome.plan,
                    descriptor,
                    &crate::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding::from_snapshot(&snapshot),
                    &mut factory,
                    200,
                    201,
                    202,
                    203,
                    204,
                    #[cfg(not(test))]
                    crate::functions::builtin_sql_function_catalog(),
                )
            }
            .expect("join projection coalesce plan");
            optimize_logical_for_test(coalesce)
        }

        /// Build the same coalesce plan as the SQL-only rule test, but bind
        /// every scan to the request-local identity that preparation will
        /// materialize. Repeated scans of the same base deliberately reuse
        /// its one exact admitted binding; the target locator has its own
        /// target binding.
        pub(crate) fn build_tokenized_join_refresh_coalesce_plan_for_lowering(
            scope: crate::binding::SqlTableBindingScopeId,
        ) -> (
            crate::optimizer::OptimizedOperatorNode,
            JoinRefreshCoalesceBindingTokens,
        ) {
            let tokens = JoinRefreshCoalesceBindingTokens::for_scope(scope);
            let mut optimized = build_join_refresh_coalesce_plan_for_lowering();
            retokenize_coalesce_scans(&mut optimized, tokens);
            (optimized, tokens)
        }

        fn retokenize_coalesce_scans(
            node: &mut crate::optimizer::OptimizedOperatorNode,
            tokens: JoinRefreshCoalesceBindingTokens,
        ) {
            if let crate::optimizer::Operator::PhysicalScan(scan) = &mut node.op {
                let ScanSource::Sql(source) = &mut scan.table.source;
                source.binding = match source.table.table.as_str() {
                    "l" => tokens.left,
                    "r" => tokens.right,
                    "mv" => tokens.target,
                    table => panic!("unexpected coalesce fixture scan table {table}"),
                };
            }
            for child in &mut node.children {
                retokenize_coalesce_scans(child, tokens);
            }
        }
    }

    #[test]
    fn imv_pipeline_uses_aggregate_change_stream_without_join_contract() {
        let ctx = join_aggregate_mv_ctx_customized(|snapshot| {
            let contract = Arc::make_mut(&mut snapshot.schema_contract);
            contract.join = None;
            contract.branch = Some(crate::compiler::mv_rewrite::SqlImvBranchContract {
                branch_id_column_name: BRANCH_ID_COLUMN_NAME.to_string(),
            });
        });

        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: join_aggregate_plan(),
            snapshot: ctx,
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
            snapshot: join_aggregate_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
            column_ref_factory: test_column_ref_factory(),
        })
        .expect("join aggregate IMV pipeline must rewrite and validate");

        let pipeline = query_rewrite_pipeline();
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_query_stats_input(
            crate::optimizer::stats_input::OptimizerStatsInput::from_test_table_statistics(
                &HashMap::new(),
            ),
        );
        let opt_in = plan_to_opt_expr_with_arena(&outcome.plan, &mut ctx);
        let opt_out = pipeline
            .rewrite(opt_in, &mut ctx)
            .expect("query rewrite must preserve join aggregate delta action");
        let rewritten = crate::planner::optimizer_bridge::logical::to_logical_plan(
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn assert_project_refs_resolve_to_child_outputs(plan: &LogicalPlanNode) {
        if let LogicalPlanKind::Project(project) = &plan.kind {
            let child_output_ids = crate::planner::plan_output_columns(plan.unary_input())
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn assert_physical_project_refs_resolve_to_child_outputs(
        plan: &crate::optimizer::OptimizedOperatorNode,
    ) {
        if matches!(
            &plan.op,
            crate::optimizer::Operator::PhysicalHashJoin(_)
                | crate::optimizer::Operator::PhysicalNestLoopJoin(_)
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
        if let crate::optimizer::Operator::PhysicalProject(project) = &plan.op {
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
                    crate::optimizer::scalar_expr::collect_column_ids_strict(arena, item.expr)
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
        if let crate::optimizer::Operator::PhysicalUnion(union) = &plan.op {
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn contains_target_state_scan(plan: &LogicalPlanNode) -> bool {
        matches!(
            &plan.kind,
            LogicalPlanKind::Scan(PlanScanNode {
                table: TableDef {
                    source: ScanSource::Sql(SqlScanSource {
                        kind: SqlScanKind::MvTargetState { .. },
                        ..
                    }),
                    ..
                },
                ..
            })
        ) || plan.children.iter().any(contains_target_state_scan)
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
    fn collect_branch_binding(
        plan: &LogicalPlanNode,
        signed_action_id: ColumnId,
        delta_windows: &mut Vec<(String, i64, i64)>,
        version_snapshots: &mut Vec<(String, i64)>,
    ) {
        let scan = assert_project_scan_any_table(plan);
        match &scan.table.source {
            ScanSource::Sql(source) if matches!(source.kind, SqlScanKind::Delta { .. }) => {
                let action = scan
                    .columns
                    .iter()
                    .find(|column| ImvActionColumn::matches(column))
                    .expect("delta scan must carry action column")
                    .column_id;
                assert_eq!(action, signed_action_id);
                let SqlScanKind::Delta {
                    from_snapshot_id,
                    to_snapshot_id,
                } = source.kind
                else {
                    unreachable!("matched delta SQL scan")
                };
                delta_windows.push((source.table.table.clone(), from_snapshot_id, to_snapshot_id));
            }
            ScanSource::Sql(source)
                if matches!(source.kind, SqlScanKind::FrozenInputSet { .. }) =>
            {
                assert!(
                    !scan.columns.iter().any(ImvActionColumn::matches),
                    "version scan must not carry action column"
                );
                let SqlScanKind::FrozenInputSet {
                    version: SqlTableVersionSelector::Snapshot(snapshot_id),
                } = source.kind
                else {
                    panic!("expected snapshot-bound frozen-input SQL scan");
                };
                version_snapshots.push((source.table.table.clone(), snapshot_id));
            }
            other => panic!("expected delta/version scan source, got {other:?}"),
        }
    }

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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

    #[allow(
        dead_code,
        reason = "Retained as an IMV rewrite fixture or assertion for feature-specific test targets."
    )]
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
