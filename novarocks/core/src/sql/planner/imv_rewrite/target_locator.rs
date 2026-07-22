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

//! IMV target locator join injection.
//!
//! Generic projection/filter change streams produce logical DELETE rows keyed
//! by an apply key. The merge sink consumes physical Iceberg row locators, so
//! this rule appends a refresh-only target scan and LEFT JOINs it for DELETE
//! rows before the sink boundary.

use std::sync::atomic::{AtomicBool, Ordering};

use arrow::datatypes::DataType;

use crate::mv::persistence::schema::{
    ApplyKeySource, BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME, JOIN_APPLY_KEY_COLUMN_NAME,
};
use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::sql::planner::imv_rewrite::annotation::ImvExtension;
use crate::sql::planner::imv_rewrite::column_alloc::allocate_imv_column;
use crate::sql::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::sql::planner::logical::{LogicalJoinNode, LogicalPlanKind, LogicalPlanNode};
use crate::sql::planner::payload::{PlanProjectNode, PlanScanNode};
use crate::sql::planner::table::{IcebergMvTargetLocatorScan, ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;

pub(crate) struct InjectTargetLocatorJoinRule {
    checked_root: AtomicBool,
    fired: AtomicBool,
}

impl InjectTargetLocatorJoinRule {
    pub(crate) fn new() -> Self {
        Self {
            checked_root: AtomicBool::new(false),
            fired: AtomicBool::new(false),
        }
    }
}

impl LogicalRewriteRule for InjectTargetLocatorJoinRule {
    fn name(&self) -> &'static str {
        "InjectTargetLocatorJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        if self.checked_root.swap(true, Ordering::SeqCst) || self.fired.load(Ordering::SeqCst) {
            return false;
        }
        let plan = opt_expr_to_plan(expr.clone(), ctx);
        matches!(target_locator_join_input(&plan, ctx), Ok(Some(_)) | Err(_))
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        self.fired.store(true, Ordering::SeqCst);
        bridge_apply_result(expr, ctx, |plan, ctx| {
            let Some(input) = target_locator_join_input(&plan, ctx)? else {
                return Ok(PlanRewriteResult::Unchanged);
            };
            Ok(PlanRewriteResult::Changed(build_target_locator_join(
                plan, input, ctx,
            )?))
        })
    }
}

pub(crate) fn is_target_locator_join(plan: &LogicalPlanNode) -> bool {
    let LogicalPlanKind::Join(join) = &plan.kind else {
        return false;
    };
    join.join_type == JoinKind::LeftOuter
        && matches!(
            &plan.right().kind,
            LogicalPlanKind::Scan(scan)
                if matches!(scan.table.source, ScanSource::IcebergMvTargetLocator(_))
        )
}

#[derive(Clone)]
struct LocatorJoinInput {
    output: Vec<OutputColumn>,
    action: OutputColumn,
    left_apply_key: OutputColumn,
    target_apply_key_column: String,
    branch: Option<LocatorBranchInput>,
}

#[derive(Clone)]
struct LocatorBranchInput {
    left: OutputColumn,
    target_column: String,
}

fn target_locator_join_input(
    plan: &LogicalPlanNode,
    ctx: &RewriteContext,
) -> Result<Option<LocatorJoinInput>, String> {
    if subtree_has_target_locator_join(plan) {
        return Ok(None);
    }
    let Some(output) = effective_output_columns(plan) else {
        return Ok(None);
    };
    let action = output
        .iter()
        .find(|column| ImvActionColumn::matches(column));
    let Some(action) = action else {
        return Ok(None);
    };
    let Some(ext) = ctx.extension::<ImvExtension>() else {
        return Ok(None);
    };
    let contract = &ext.mv_ctx.schema_contract;
    if contract.target.hidden_apply_key.source == ApplyKeySource::GroupRowId {
        return Ok(None);
    }
    let left_apply_key_name = match contract.target.hidden_apply_key.source {
        ApplyKeySource::BaseRowId => HIDDEN_APPLY_KEY_COLUMN_NAME,
        ApplyKeySource::JoinRowKey => JOIN_APPLY_KEY_COLUMN_NAME,
        ApplyKeySource::GroupRowId => return Ok(None),
    };
    let Some(left_apply_key) = output
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(left_apply_key_name))
    else {
        return Ok(None);
    };
    if let Some(name) = reserved_locator_output_name(&output) {
        return Err(format!(
            "IMV target locator rewrite found reserved target locator metadata column `{name}` before target locator injection"
        ));
    }
    let action = action.clone();
    let left_apply_key = left_apply_key.clone();
    let branch = contract.branch.as_ref().and_then(|branch| {
        let left = output
            .iter()
            .find(|column| {
                column
                    .name
                    .eq_ignore_ascii_case(&branch.branch_id_column.column_name)
                    || column.name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME)
            })?
            .clone();
        Some(LocatorBranchInput {
            left,
            target_column: branch.branch_id_column.column_name.clone(),
        })
    });
    Ok(Some(LocatorJoinInput {
        output,
        action,
        left_apply_key,
        target_apply_key_column: contract.target.hidden_apply_key.column_name.clone(),
        branch,
    }))
}

fn build_target_locator_join(
    left: LogicalPlanNode,
    input: LocatorJoinInput,
    ctx: &RewriteContext,
) -> Result<LogicalPlanNode, String> {
    let ext = ctx
        .extension::<ImvExtension>()
        .ok_or_else(|| "InjectTargetLocatorJoin requires ImvExtension".to_string())?;
    let right_apply_key_id = allocate_imv_column(
        ctx,
        &input.target_apply_key_column,
        input.left_apply_key.data_type.clone(),
        input.left_apply_key.nullable,
    )?;
    let right_branch_id = input
        .branch
        .as_ref()
        .map(|branch| {
            allocate_imv_column(
                ctx,
                &branch.target_column,
                branch.left.data_type.clone(),
                branch.left.nullable,
            )
        })
        .transpose()?;
    let right_file_id = allocate_imv_column(
        ctx,
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        DataType::Utf8,
        false,
    )?;
    let right_pos_id = allocate_imv_column(
        ctx,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        DataType::Int64,
        false,
    )?;
    let right_row_id_id = allocate_imv_column(
        ctx,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        DataType::Int64,
        false,
    )?;
    let right_last_updated_seq_id = allocate_imv_column(
        ctx,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        DataType::Int64,
        true,
    )?;

    let right_scan = build_target_locator_scan(
        ext,
        &input,
        right_apply_key_id,
        right_branch_id,
        right_file_id,
        right_pos_id,
        right_row_id_id,
        right_last_updated_seq_id,
    );
    let join = LogicalPlanNode::new(
        LogicalPlanKind::Join(LogicalJoinNode {
            join_type: JoinKind::LeftOuter,
            condition: Some(target_locator_join_condition(
                &input,
                right_apply_key_id,
                right_branch_id,
            )?),
        }),
        vec![left, right_scan],
        None,
    );

    let mut items = input
        .output
        .iter()
        .filter(|column| !is_row_lineage_locator_name(&column.name))
        .map(project_item_for_output_column)
        .collect::<Vec<_>>();
    items.push(nullable_locator_project_item(
        right_file_id,
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        DataType::Utf8,
    ));
    items.push(nullable_locator_project_item(
        right_pos_id,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        DataType::Int64,
    ));
    items.push(nullable_locator_project_item(
        right_row_id_id,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        DataType::Int64,
    ));
    items.push(nullable_locator_project_item(
        right_last_updated_seq_id,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        DataType::Int64,
    ));
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![join],
        None,
    ))
}

fn build_target_locator_scan(
    ext: &ImvExtension,
    input: &LocatorJoinInput,
    right_apply_key_id: ColumnId,
    right_branch_id: Option<ColumnId>,
    right_file_id: ColumnId,
    right_pos_id: ColumnId,
    right_row_id_id: ColumnId,
    right_last_updated_seq_id: ColumnId,
) -> LogicalPlanNode {
    let target = &ext.mv_ctx.target;
    let mut columns = vec![ColumnDef {
        name: input.target_apply_key_column.clone(),
        data_type: input.left_apply_key.data_type.clone(),
        nullable: input.left_apply_key.nullable,
        write_default: None,
        logical_type: None,
    }];
    if let Some(branch) = &input.branch {
        columns.push(ColumnDef {
            name: branch.target_column.clone(),
            data_type: branch.left.data_type.clone(),
            nullable: branch.left.nullable,
            write_default: None,
            logical_type: None,
        });
    }
    let metadata_columns = vec![
        ColumnDef {
            name: crate::exec::row_position::ICEBERG_FILE_PATH_COL.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::exec::row_position::ICEBERG_ROW_POS_COL.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
    ];
    let mut scan_columns = vec![output_column(
        right_apply_key_id,
        &input.target_apply_key_column,
        input.left_apply_key.data_type.clone(),
        input.left_apply_key.nullable,
        false,
    )];
    if let (Some(branch), Some(column_id)) = (&input.branch, right_branch_id) {
        scan_columns.push(output_column(
            column_id,
            &branch.target_column,
            branch.left.data_type.clone(),
            branch.left.nullable,
            false,
        ));
    }
    scan_columns.push(output_column(
        right_file_id,
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        DataType::Utf8,
        false,
        true,
    ));
    scan_columns.push(output_column(
        right_pos_id,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        DataType::Int64,
        false,
        true,
    ));
    scan_columns.push(output_column(
        right_row_id_id,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        DataType::Int64,
        false,
        true,
    ));
    scan_columns.push(output_column(
        right_last_updated_seq_id,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        DataType::Int64,
        true,
        true,
    ));

    LogicalPlanNode::new(
        LogicalPlanKind::Scan(PlanScanNode {
            database: target.namespace.clone(),
            table: TableDef {
                name: target.table.clone(),
                columns,
                iceberg_row_lineage_metadata_columns: metadata_columns,
                source: ScanSource::IcebergMvTargetLocator(IcebergMvTargetLocatorScan {
                    catalog: target.catalog.clone(),
                    database: target.namespace.clone(),
                    table: target.table.clone(),
                    target_table_uuid: ext.mv_ctx.target_table_uuid.clone(),
                    target_snapshot_id: ext.mv_ctx.target_snapshot_id,
                    apply_key_column: input.target_apply_key_column.clone(),
                    branch_id_column: input
                        .branch
                        .as_ref()
                        .map(|branch| branch.target_column.clone()),
                }),
            },
            alias: None,
            columns: scan_columns,
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
        Vec::new(),
        None,
    )
}

fn target_locator_join_condition(
    input: &LocatorJoinInput,
    right_apply_key_id: ColumnId,
    right_branch_id: Option<ColumnId>,
) -> Result<TypedExpr, String> {
    let delete_only = binary(
        column_ref(&input.action),
        BinOp::Eq,
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(i64::from(
                crate::exec::change_op::CHANGE_OP_DELETE,
            ))),
            data_type: DataType::Int8,
            nullable: false,
        },
    );
    let apply_key_eq = binary(
        column_ref(&input.left_apply_key),
        BinOp::Eq,
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: right_apply_key_id,
                qualifier: None,
                column: input.target_apply_key_column.clone(),
            },
            data_type: input.left_apply_key.data_type.clone(),
            nullable: input.left_apply_key.nullable,
        },
    );
    let mut condition = binary(delete_only, BinOp::And, apply_key_eq);
    if let Some(branch) = &input.branch {
        let right_branch_id = right_branch_id.ok_or_else(|| {
            "target locator branch join missing right branch column id".to_string()
        })?;
        let branch_eq = binary(
            column_ref(&branch.left),
            BinOp::Eq,
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: right_branch_id,
                    qualifier: None,
                    column: branch.target_column.clone(),
                },
                data_type: branch.left.data_type.clone(),
                nullable: branch.left.nullable,
            },
        );
        condition = binary(condition, BinOp::And, branch_eq);
    }
    Ok(condition)
}

fn effective_output_columns(plan: &LogicalPlanNode) -> Option<Vec<OutputColumn>> {
    match &plan.kind {
        LogicalPlanKind::Project(project) => Some(
            project
                .items
                .iter()
                .map(|item| OutputColumn {
                    column_id: item.output_column_id,
                    name: item.output_name.clone(),
                    data_type: item.expr.data_type.clone(),
                    nullable: item.expr.nullable,
                    is_internal: is_internal_output_name(&item.output_name),
                })
                .collect(),
        ),
        LogicalPlanKind::Union(union) => Some(union.output_columns.clone()),
        LogicalPlanKind::Filter(_) => effective_output_columns(plan.unary_input()),
        _ => None,
    }
}

fn reserved_locator_output_name(columns: &[OutputColumn]) -> Option<&str> {
    columns.iter().find_map(|column| {
        if column
            .name
            .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)
            || column
                .name
                .eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)
        {
            Some(column.name.as_str())
        } else {
            None
        }
    })
}

fn subtree_has_target_locator_join(plan: &LogicalPlanNode) -> bool {
    is_target_locator_join(plan) || plan.children.iter().any(subtree_has_target_locator_join)
}

fn is_row_lineage_locator_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
}

fn project_item_for_output_column(column: &OutputColumn) -> ProjectItem {
    ProjectItem {
        expr: column_ref(column),
        output_name: column.name.clone(),
        output_column_id: column.column_id,
    }
}

fn nullable_locator_project_item(
    column_id: ColumnId,
    name: &str,
    data_type: DataType,
) -> ProjectItem {
    ProjectItem {
        expr: TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type,
            nullable: true,
        },
        output_name: name.to_string(),
        output_column_id: column_id,
    }
}

fn column_ref(column: &OutputColumn) -> TypedExpr {
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

fn binary(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn output_column(
    column_id: ColumnId,
    name: &str,
    data_type: DataType,
    nullable: bool,
    is_internal: bool,
) -> OutputColumn {
    OutputColumn {
        column_id,
        name: name.to_string(),
        data_type,
        nullable,
        is_internal,
    }
}

fn is_internal_output_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(ImvActionColumn::NAME)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
        || name.eq_ignore_ascii_case(HIDDEN_APPLY_KEY_COLUMN_NAME)
        || name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME)
        || name.eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)
}
