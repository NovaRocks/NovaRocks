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

//! IMV change-stream semantic descriptor.
//!
//! The descriptor is the single semantic contract produced by IMV rewrite
//! rules for downstream consumers that need to know whether the rewritten
//! plan contains an aggregate change stream.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::{RewriteDiagnostic, RewriteResult};
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshDescriptor;
use crate::sql::planner::imv_rewrite::opt_expr_to_plan;
use crate::sql::planner::logical::{LogicalAggregateNode, LogicalPlanKind, LogicalPlanNode};
use crate::sql::planner::payload::{PlanProjectNode, PlanScanNode};
use crate::sql::planner::table::{ScanSource, TableDef};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ImvChangeStreamDescriptor {
    pub(crate) aggregate: Option<AggregateChangeStreamDescriptor>,
    pub(crate) join_refresh: Option<JoinRefreshDescriptor>,
}

impl ImvChangeStreamDescriptor {
    pub(crate) fn has_aggregate(&self) -> bool {
        self.aggregate.is_some()
    }

    pub(crate) fn aggregate(&self) -> Option<&AggregateChangeStreamDescriptor> {
        self.aggregate.as_ref()
    }

    pub(crate) fn describes_aggregate_root(&self, plan: &LogicalPlanNode) -> bool {
        let Some(expected) = &self.aggregate else {
            return false;
        };
        build_change_stream_root_descriptor(plan)
            .as_ref()
            .is_some_and(|actual| actual == expected)
    }

    pub(crate) fn covers_aggregate_validation_root(&self, plan: &LogicalPlanNode) -> bool {
        if self.describes_aggregate_root(plan) {
            return true;
        }
        self.aggregate.is_some()
            && matches!(&plan.kind, LogicalPlanKind::Join(_))
            && contains_target_state_scan(plan)
            && contains_signed_state_aggregate(plan)
    }

    pub(crate) fn validate_against_plan(&self, plan: &LogicalPlanNode) -> Result<(), String> {
        let Some(aggregate) = &self.aggregate else {
            return Ok(());
        };

        if !plan_contains_output_column(
            plan,
            aggregate.action_column_id,
            &aggregate.action_column_name,
        ) {
            return Err(format!(
                "change stream descriptor action column {:?} ({}) is not present in plan output",
                aggregate.action_column_id, aggregate.action_column_name
            ));
        }
        if aggregate.target_state.present && !contains_target_state_scan(plan) {
            return Err(
                "change stream descriptor requires target state scan, but plan does not contain one"
                    .to_string(),
            );
        }
        if aggregate.signed_state_aggregate.present && !contains_signed_state_aggregate(plan) {
            return Err(
                "change stream descriptor requires signed state aggregate, but plan does not contain one"
                    .to_string(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateChangeStreamDescriptor {
    pub(crate) action_column_id: ColumnId,
    pub(crate) action_column_name: String,
    pub(crate) shape: AggregateChangeStreamShape,
    pub(crate) target_state: TargetStateProof,
    pub(crate) signed_state_aggregate: SignedStateAggregateProof,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateChangeStreamShape {
    UnionChangeStream,
    RelationalChangeStream,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetStateProof {
    pub(crate) present: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SignedStateAggregateProof {
    pub(crate) present: bool,
}

pub(crate) struct BuildChangeStreamDescriptorRule {
    fired: AtomicBool,
}

impl BuildChangeStreamDescriptorRule {
    pub(crate) fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
        }
    }
}

impl LogicalRewriteRule for BuildChangeStreamDescriptorRule {
    fn name(&self) -> &'static str {
        "BuildChangeStreamDescriptor"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, _expr: &OptExpr, ctx: &RewriteContext) -> bool {
        !self.fired.load(Ordering::SeqCst)
            && ctx
                .extension::<ImvExtension>()
                .is_some_and(|ext| ext.annotation.change_stream.aggregate.is_none())
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, Ordering::SeqCst);
        let plan = opt_expr_to_plan(expr, ctx);
        let descriptor = build_change_stream_descriptor(&plan);
        if descriptor.has_aggregate() {
            let ext = ctx
                .extension::<ImvExtension>()
                .ok_or("BuildChangeStreamDescriptor requires ImvExtension")?
                .clone();
            let mut annotation = ext.annotation.clone();
            annotation.change_stream.aggregate = descriptor.aggregate;
            ctx.set_extension::<ImvExtension>(ImvExtension { annotation, ..ext });
        }
        Ok(RewriteResult::Unchanged)
    }
}

pub(crate) struct ValidateChangeStreamDescriptorRule {
    fired: AtomicBool,
}

impl ValidateChangeStreamDescriptorRule {
    pub(crate) fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
        }
    }
}

impl LogicalRewriteRule for ValidateChangeStreamDescriptorRule {
    fn name(&self) -> &'static str {
        "ValidateChangeStreamDescriptor"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::Validation
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, _expr: &OptExpr, ctx: &RewriteContext) -> bool {
        !self.fired.load(Ordering::SeqCst)
            && ctx
                .extension::<ImvExtension>()
                .is_some_and(|ext| ext.annotation.change_stream.has_aggregate())
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, Ordering::SeqCst);
        let plan = opt_expr_to_plan(expr, ctx);
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or("ValidateChangeStreamDescriptor requires ImvExtension")?;
        match ext.annotation.change_stream.validate_against_plan(&plan) {
            Ok(()) => Ok(RewriteResult::Unchanged),
            Err(message) => Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
                self.name(),
                message,
            ))),
        }
    }
}

pub(crate) fn build_change_stream_descriptor(plan: &LogicalPlanNode) -> ImvChangeStreamDescriptor {
    if let Some(aggregate) = build_union_change_stream_descriptor(plan) {
        return ImvChangeStreamDescriptor {
            aggregate: Some(aggregate),
            ..Default::default()
        };
    }
    if let Some(aggregate) = build_relational_change_stream_descriptor(plan) {
        return ImvChangeStreamDescriptor {
            aggregate: Some(aggregate),
            ..Default::default()
        };
    }
    ImvChangeStreamDescriptor::default()
}

fn build_change_stream_root_descriptor(
    plan: &LogicalPlanNode,
) -> Option<AggregateChangeStreamDescriptor> {
    build_union_change_stream_root_descriptor(plan)
        .or_else(|| build_relational_change_stream_root_descriptor(plan))
}

fn build_union_change_stream_descriptor(
    plan: &LogicalPlanNode,
) -> Option<AggregateChangeStreamDescriptor> {
    if let Some(aggregate) = build_union_change_stream_root_descriptor(plan) {
        return Some(aggregate);
    }

    plan.children
        .iter()
        .find_map(build_union_change_stream_descriptor)
}

fn build_union_change_stream_root_descriptor(
    plan: &LogicalPlanNode,
) -> Option<AggregateChangeStreamDescriptor> {
    let matched = match &plan.kind {
        LogicalPlanKind::Union(_) => change_stream_union_output_column(plan)
            .filter(|_| contains_target_state_scan(plan) && contains_signed_state_aggregate(plan)),
        LogicalPlanKind::CTEAnchor(_) if plan.children.len() == 2 => {
            change_stream_union_output_column(plan.child(1)).filter(|_| {
                contains_target_state_scan(plan.child(0))
                    && contains_signed_state_aggregate(plan.child(0))
            })
        }
        _ => None,
    };

    if let Some(column) = matched {
        return Some(AggregateChangeStreamDescriptor {
            action_column_id: column.column_id,
            action_column_name: column.name,
            shape: AggregateChangeStreamShape::UnionChangeStream,
            target_state: TargetStateProof { present: true },
            signed_state_aggregate: SignedStateAggregateProof { present: true },
        });
    }

    None
}

fn build_relational_change_stream_descriptor(
    plan: &LogicalPlanNode,
) -> Option<AggregateChangeStreamDescriptor> {
    if let Some(aggregate) = build_relational_change_stream_root_descriptor(plan) {
        return Some(aggregate);
    }

    plan.children
        .iter()
        .find_map(build_relational_change_stream_descriptor)
}

fn build_relational_change_stream_root_descriptor(
    plan: &LogicalPlanNode,
) -> Option<AggregateChangeStreamDescriptor> {
    let matched = match &plan.kind {
        LogicalPlanKind::Project(project) => {
            change_stream_project_output_column(project).filter(|_| {
                project_filter_contains_state_all_zero(plan)
                    && contains_join_kind(plan, JoinKind::LeftOuter)
                    && contains_supported_delta_join_kind(plan)
                    && contains_branch_marker_values(plan)
                    && contains_target_state_scan(plan)
                    && contains_signed_state_aggregate(plan)
            })
        }
        _ => None,
    };

    if let Some(column) = matched {
        return Some(AggregateChangeStreamDescriptor {
            action_column_id: column.column_id,
            action_column_name: column.name,
            shape: AggregateChangeStreamShape::RelationalChangeStream,
            target_state: TargetStateProof { present: true },
            signed_state_aggregate: SignedStateAggregateProof { present: true },
        });
    }

    None
}

fn change_stream_union_output_column(plan: &LogicalPlanNode) -> Option<OutputColumn> {
    let LogicalPlanKind::Union(union) = &plan.kind else {
        return None;
    };
    union
        .output_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(ImvActionColumn::NAME))
        .cloned()
}

fn change_stream_project_output_column(project: &PlanProjectNode) -> Option<OutputColumn> {
    project
        .items
        .iter()
        .find(|item| item.output_name.eq_ignore_ascii_case(ImvActionColumn::NAME))
        .map(|item| OutputColumn {
            column_id: item.output_column_id,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: true,
        })
}

fn project_filter_contains_state_all_zero(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Project(_) = &plan.kind else {
        return false;
    };
    let Some(filter_plan) = plan.children.first() else {
        return false;
    };
    let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
        return false;
    };
    expr_contains_function(&filter.predicate, "state_all_zero")
}

fn expr_contains_function(expr: &TypedExpr, name: &str) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall {
            name: func, args, ..
        }
        | ExprKind::AggregateCall {
            name: func, args, ..
        } => {
            func.eq_ignore_ascii_case(name)
                || args.iter().any(|arg| expr_contains_function(arg, name))
        }
        ExprKind::BinaryOp { left, right, .. } => {
            expr_contains_function(left, name) || expr_contains_function(right, name)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => expr_contains_function(expr, name),
        ExprKind::InList { expr, list, .. } => {
            expr_contains_function(expr, name)
                || list.iter().any(|item| expr_contains_function(item, name))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expr_contains_function(expr, name)
                || expr_contains_function(low, name)
                || expr_contains_function(high, name)
        }
        ExprKind::Like { expr, pattern, .. } => {
            expr_contains_function(expr, name) || expr_contains_function(pattern, name)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_some_and(|expr| expr_contains_function(expr, name))
                || when_then.iter().any(|(when_expr, then_expr)| {
                    expr_contains_function(when_expr, name)
                        || expr_contains_function(then_expr, name)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(|expr| expr_contains_function(expr, name))
        }
        ExprKind::LambdaFunction { body, .. } => expr_contains_function(body, name),
        ExprKind::Nested(expr) | ExprKind::Lambda { body: expr, .. } => {
            expr_contains_function(expr, name)
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(|arg| expr_contains_function(arg, name))
                || partition_by
                    .iter()
                    .any(|expr| expr_contains_function(expr, name))
                || order_by
                    .iter()
                    .any(|item| expr_contains_function(&item.expr, name))
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => false,
    }
}

fn contains_join_kind(plan: &LogicalPlanNode, join_type: JoinKind) -> bool {
    matches!(
        &plan.kind,
        LogicalPlanKind::Join(join) if join.join_type == join_type
    ) || plan
        .children
        .iter()
        .any(|child| contains_join_kind(child, join_type))
}

fn contains_supported_delta_join_kind(plan: &LogicalPlanNode) -> bool {
    contains_join_kind(plan, JoinKind::Inner) || contains_join_kind(plan, JoinKind::Cross)
}

fn contains_branch_marker_values(plan: &LogicalPlanNode) -> bool {
    matches!(&plan.kind, LogicalPlanKind::Values(values)
        if values.columns.iter().any(|column| {
            column.name.eq_ignore_ascii_case("__imv_change_branch")
        })
    ) || plan.children.iter().any(contains_branch_marker_values)
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

fn contains_signed_state_aggregate(plan: &LogicalPlanNode) -> bool {
    matches!(
        &plan.kind,
        LogicalPlanKind::Aggregate(LogicalAggregateNode { aggregates, .. })
            if aggregates.iter().any(|call| call.name.ends_with("_state_signed"))
    ) || plan.children.iter().any(contains_signed_state_aggregate)
}

fn plan_contains_output_column(plan: &LogicalPlanNode, column_id: ColumnId, name: &str) -> bool {
    node_output_contains_column(plan, column_id, name)
        || plan
            .children
            .iter()
            .any(|child| plan_contains_output_column(child, column_id, name))
}

fn node_output_contains_column(plan: &LogicalPlanNode, column_id: ColumnId, name: &str) -> bool {
    match &plan.kind {
        LogicalPlanKind::Scan(scan) => scan
            .columns
            .iter()
            .any(|column| column.column_id == column_id && column.name.eq_ignore_ascii_case(name)),
        LogicalPlanKind::Project(project) => project.items.iter().any(|item| {
            item.output_column_id == column_id && item.output_name.eq_ignore_ascii_case(name)
        }),
        LogicalPlanKind::Aggregate(aggregate) => aggregate
            .output_columns
            .iter()
            .any(|column| column.column_id == column_id && column.name.eq_ignore_ascii_case(name)),
        LogicalPlanKind::Union(union) => union
            .output_columns
            .iter()
            .any(|column| column.column_id == column_id && column.name.eq_ignore_ascii_case(name)),
        LogicalPlanKind::Values(values) => values
            .columns
            .iter()
            .any(|column| column.column_id == column_id && column.name.eq_ignore_ascii_case(name)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::DataType;

    use crate::sql::analysis::{
        ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
    use crate::sql::planner::imv_rewrite::target_state::build_target_state_scan_source;
    use crate::sql::planner::logical::{
        LogicalAggregateNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode, LogicalUnionNode,
    };
    use crate::sql::planner::payload::{
        AggregateCall, PlanFilterNode, PlanProjectNode, PlanScanNode, PlanValuesNode,
    };
    use crate::sql::planner::table::{
        IcebergMvTargetStatePartitionConstraint, IcebergMvTargetStateRowFilter, TableDef,
    };
    use novarocks_catalog::schema::ColumnDef;

    fn output_column(id: u32, name: &str, data_type: DataType, is_internal: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable: false,
            is_internal,
        }
    }

    fn empty_values_plan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
            vec![],
            None,
        )
    }

    fn target_state_scan_plan() -> LogicalPlanNode {
        let columns = vec![ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: "mv_target".to_string(),
                    columns: columns.clone(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: build_target_state_scan_source(
                        "ice".to_string(),
                        "db".to_string(),
                        "mv_target".to_string(),
                        "target-uuid".to_string(),
                        Some(10),
                        1,
                        columns,
                        vec!["k".to_string()],
                        vec!["sum_v_state".to_string()],
                        vec!["k".to_string(), "sum_v_state".to_string()],
                        "__row_id__".to_string(),
                        IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                            row_id_column_name: "__row_id__".to_string(),
                            branch_scope: None,
                        },
                        IcebergMvTargetStatePartitionConstraint::Unpartitioned,
                    ),
                },
                alias: None,
                columns: vec![output_column(10, "k", DataType::Int64, false)],
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn signed_state_aggregate(input: LogicalPlanNode) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: Vec::new(),
                aggregates: vec![AggregateCall {
                    name: "sum_state_signed".to_string(),
                    args: vec![TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(1)),
                        data_type: DataType::Int64,
                        nullable: false,
                    }],
                    distinct: false,
                    result_type: DataType::Binary,
                    order_by: Vec::new(),
                    output_column_id: ColumnId::new_for_test(20),
                }],
                output_columns: Vec::new(),
                already_pushed: false,
            }),
            vec![input],
            None,
        )
    }

    fn branch_marker_values_plan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![output_column(
                    30,
                    "__imv_change_branch",
                    DataType::Int8,
                    true,
                )],
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn build_descriptor_recognizes_union_change_stream() {
        let action = output_column(1, ImvActionColumn::NAME, DataType::Int8, true);
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![action.clone()],
            }),
            vec![signed_state_aggregate(target_state_scan_plan())],
            None,
        );

        let descriptor = build_change_stream_descriptor(&plan);
        let aggregate = descriptor
            .aggregate()
            .expect("union aggregate change stream should be described");
        assert_eq!(aggregate.action_column_id, action.column_id);
        assert_eq!(aggregate.action_column_name, ImvActionColumn::NAME);
        assert_eq!(
            aggregate.shape,
            AggregateChangeStreamShape::UnionChangeStream
        );
        assert!(aggregate.target_state.present);
        assert!(aggregate.signed_state_aggregate.present);
    }

    #[test]
    fn descriptor_describes_only_matching_change_stream_root() {
        let action = output_column(1, ImvActionColumn::NAME, DataType::Int8, true);
        let union = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![action],
            }),
            vec![signed_state_aggregate(target_state_scan_plan())],
            None,
        );
        let descriptor = build_change_stream_descriptor(&union);
        assert!(descriptor.describes_aggregate_root(&union));

        let wrapped = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(true)),
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![union],
            None,
        );
        let nested_descriptor = build_change_stream_descriptor(&wrapped);
        assert!(
            nested_descriptor.has_aggregate(),
            "recursive builder should still find the nested descriptor"
        );
        assert_eq!(nested_descriptor, descriptor);
        assert!(
            !descriptor.describes_aggregate_root(&wrapped),
            "root-only checks must not treat arbitrary wrappers as change-stream roots"
        );
    }

    #[test]
    fn descriptor_covers_only_proven_aggregate_validation_join_root() {
        let descriptor = ImvChangeStreamDescriptor {
            aggregate: Some(AggregateChangeStreamDescriptor {
                action_column_id: ColumnId::new_for_test(1),
                action_column_name: ImvActionColumn::NAME.to_string(),
                shape: AggregateChangeStreamShape::RelationalChangeStream,
                target_state: TargetStateProof { present: true },
                signed_state_aggregate: SignedStateAggregateProof { present: true },
            }),
            ..Default::default()
        };
        let aggregate_merge_join = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::LeftOuter,
                condition: None,
            }),
            vec![
                target_state_scan_plan(),
                signed_state_aggregate(empty_values_plan()),
            ],
            None,
        );
        assert!(descriptor.covers_aggregate_validation_root(&aggregate_merge_join));

        let plain_outer_join = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::LeftOuter,
                condition: None,
            }),
            vec![empty_values_plan(), empty_values_plan()],
            None,
        );
        assert!(!descriptor.covers_aggregate_validation_root(&plain_outer_join));
    }

    #[test]
    fn build_descriptor_recognizes_relational_change_stream() {
        let action_id = ColumnId::new_for_test(1);
        let target_and_signed = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::LeftOuter,
                condition: None,
            }),
            vec![
                target_state_scan_plan(),
                signed_state_aggregate(empty_values_plan()),
            ],
            None,
        );
        let expanded = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::Cross,
                condition: None,
            }),
            vec![target_and_signed, branch_marker_values_plan()],
            None,
        );
        let filtered = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::FunctionCall {
                        name: "state_all_zero".to_string(),
                        args: Vec::new(),
                        distinct: false,
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            vec![expanded],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(1)),
                        data_type: DataType::Int8,
                        nullable: false,
                    },
                    output_name: ImvActionColumn::NAME.to_string(),
                    output_column_id: action_id,
                }],
                output_qualifier: None,
            }),
            vec![filtered],
            None,
        );

        let descriptor = build_change_stream_descriptor(&plan);
        let aggregate = descriptor
            .aggregate()
            .expect("relational aggregate change stream should be described");
        assert_eq!(aggregate.action_column_id, action_id);
        assert_eq!(aggregate.action_column_name, ImvActionColumn::NAME);
        assert_eq!(
            aggregate.shape,
            AggregateChangeStreamShape::RelationalChangeStream
        );
        assert!(aggregate.target_state.present);
        assert!(aggregate.signed_state_aggregate.present);
    }

    #[test]
    fn descriptor_validation_rejects_stale_action_column_id() {
        let action = output_column(1, ImvActionColumn::NAME, DataType::Int8, true);
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: vec![action],
            }),
            vec![signed_state_aggregate(target_state_scan_plan())],
            None,
        );
        let descriptor = ImvChangeStreamDescriptor {
            aggregate: Some(AggregateChangeStreamDescriptor {
                action_column_id: ColumnId::new_for_test(99),
                action_column_name: ImvActionColumn::NAME.to_string(),
                shape: AggregateChangeStreamShape::UnionChangeStream,
                target_state: TargetStateProof { present: true },
                signed_state_aggregate: SignedStateAggregateProof { present: true },
            }),
            ..Default::default()
        };

        let err = descriptor
            .validate_against_plan(&plan)
            .expect_err("stale action column id must fail validation");
        assert!(
            err.contains("change stream descriptor action column"),
            "got: {err}"
        );
    }
}
