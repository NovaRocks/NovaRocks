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

use crate::compiler::SqlCompileError;
use crate::planner::vocabulary::ApplyKeySource;

use std::sync::atomic::{AtomicBool, Ordering};

use arrow::datatypes::DataType;

use crate::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
};
use crate::column_id::ColumnId;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::planner::imv_rewrite::action_column::ImvActionColumn;
use crate::planner::imv_rewrite::annotation::ImvExtension;
use crate::planner::imv_rewrite::column_alloc::allocate_imv_column;
use crate::planner::imv_rewrite::{PlanRewriteResult, bridge_apply_result, opt_expr_to_plan};
use crate::planner::logical::{LogicalJoinNode, LogicalPlanKind, LogicalPlanNode};
use crate::planner::payload::{PlanProjectNode, PlanScanNode};
use crate::planner::table::{
    ScanSource, SqlMvTargetLocatorScan, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
    sql_mv_target_locator_scan,
};
use crate::planner::vocabulary::{
    BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME, JOIN_APPLY_KEY_COLUMN_NAME,
};
use novarocks_types::schema::ColumnDef;

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

    fn apply(
        &self,
        expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
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
                if sql_mv_target_locator_scan(&scan.table.source).is_some()
        )
}

#[derive(Clone)]
struct LocatorJoinInput {
    output: Vec<OutputColumn>,
    action: OutputColumn,
    left_apply_key: OutputColumn,
    target_apply_key_column: String,
    target_apply_key_type: novarocks_type_contract::FunctionValueType,
    branch: Option<LocatorBranchInput>,
}

#[derive(Clone)]
struct LocatorBranchInput {
    left: OutputColumn,
    target_column: String,
    target_value_type: novarocks_type_contract::FunctionValueType,
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
    let contract = &ext.snapshot.schema_contract;
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
    let branch = contract
        .branch
        .as_ref()
        .map(|branch| -> Result<_, String> {
            let left = output.iter().find(|column| {
                column
                    .name
                    .eq_ignore_ascii_case(&branch.branch_id_column_name)
                    || column.name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME)
            });
            let Some(left) = left else {
                return Ok(None);
            };
            let left = left.clone();
            Ok(Some(LocatorBranchInput {
                left,
                target_column: branch.branch_id_column_name.clone(),
                target_value_type: target_column_value_type(
                    &ext.snapshot.target_columns,
                    &branch.branch_id_column_name,
                )?,
            }))
        })
        .transpose()?
        .flatten();
    Ok(Some(LocatorJoinInput {
        output,
        action,
        left_apply_key,
        target_apply_key_column: contract.target.hidden_apply_key.column_name.clone(),
        target_apply_key_type: target_column_value_type(
            &ext.snapshot.target_columns,
            &contract.target.hidden_apply_key.column_name,
        )?,
        branch,
    }))
}

fn target_column_value_type(
    columns: &[ColumnDef],
    name: &str,
) -> Result<novarocks_type_contract::FunctionValueType, String> {
    let mut matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name));
    let column = matches
        .next()
        .ok_or_else(|| format!("IMV target locator requires sealed target column `{name}`"))?;
    if matches.next().is_some() {
        return Err(format!(
            "IMV target locator requires unique sealed target column `{name}`"
        ));
    }
    column
        .declared_value_type()
        .map_err(|error| format!("invalid IMV locator target column `{name}` declaration: {error}"))
}

fn build_target_locator_join(
    left: LogicalPlanNode,
    input: LocatorJoinInput,
    ctx: &RewriteContext,
) -> Result<LogicalPlanNode, String> {
    let ext = ctx
        .extension::<ImvExtension>()
        .ok_or_else(|| "InjectTargetLocatorJoin requires ImvExtension".to_string())?;
    if ext.annotation.target_read
        == crate::planner::imv_rewrite::annotation::TargetReadRequirement::NewOutputRowsOnly
    {
        return build_new_output_locator_projection(left, input, ctx);
    }
    let right_apply_key_id = allocate_imv_column(
        ctx,
        &input.target_apply_key_column,
        input.target_apply_key_type.clone(),
    )?;
    let right_branch_id = input
        .branch
        .as_ref()
        .map(|branch| {
            allocate_imv_column(ctx, &branch.target_column, branch.target_value_type.clone())
        })
        .transpose()?;
    let right_file_id = allocate_imv_column(
        ctx,
        crate::common::ICEBERG_FILE_PATH_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
    )?;
    let right_pos_id = allocate_imv_column(
        ctx,
        crate::common::ICEBERG_ROW_POS_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
    )?;
    let right_row_id_id = allocate_imv_column(
        ctx,
        crate::common::ICEBERG_ROW_ID_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
    )?;
    let right_last_updated_seq_id = allocate_imv_column(
        ctx,
        crate::common::ICEBERG_LAST_UPDATED_SEQ_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
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
    )?;
    let join = LogicalPlanNode::new(
        LogicalPlanKind::Join(LogicalJoinNode {
            naa_author: None,
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

    // The row-delta publication's writer input is the provider's signed shape:
    // the target's own columns and the v3 lineage they carry forward, then the
    // `_file`/`_pos` row identity. Each branch names its columns by their
    // position in that shape and the router reads this producer at those
    // positions, so everything the shape does not name has to stand after the
    // columns it does -- the action column included. It is the input the
    // change events are selected by, not a column the target stores, and
    // leaving it among the data columns shifts every column after it.
    let mut items = input
        .output
        .iter()
        .filter(|column| {
            !is_row_lineage_locator_name(&column.name) && !ImvActionColumn::matches(column)
        })
        .map(project_item_for_output_column)
        .collect::<Vec<_>>();
    items.push(nullable_locator_project_item(
        right_row_id_id,
        crate::common::ICEBERG_ROW_ID_COL,
        DataType::Int64,
    ));
    items.push(nullable_locator_project_item(
        right_last_updated_seq_id,
        crate::common::ICEBERG_LAST_UPDATED_SEQ_COL,
        DataType::Int64,
    ));
    items.push(nullable_locator_project_item(
        right_file_id,
        crate::common::ICEBERG_FILE_PATH_COL,
        DataType::Utf8,
    ));
    items.push(nullable_locator_project_item(
        right_pos_id,
        crate::common::ICEBERG_ROW_POS_COL,
        DataType::Int64,
    ));
    items.extend(
        input
            .output
            .iter()
            .filter(|column| ImvActionColumn::matches(column))
            .map(project_item_for_output_column),
    );
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![join],
        None,
    ))
}

/// The original Insert-only proof makes the DELETE-only target join have no
/// matches. Preserve its nullable output contract and retire source lineage;
/// skipping the whole rule would leak source row IDs into the MV writer.
fn build_new_output_locator_projection(
    left: LogicalPlanNode,
    input: LocatorJoinInput,
    ctx: &RewriteContext,
) -> Result<LogicalPlanNode, String> {
    let mut items = input
        .output
        .iter()
        .filter(|column| {
            !is_row_lineage_locator_name(&column.name) && !ImvActionColumn::matches(column)
        })
        .map(project_item_for_output_column)
        .collect::<Vec<_>>();
    for (name, data_type) in [
        (crate::common::ICEBERG_ROW_ID_COL, DataType::Int64),
        (crate::common::ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64),
        (crate::common::ICEBERG_FILE_PATH_COL, DataType::Utf8),
        (crate::common::ICEBERG_ROW_POS_COL, DataType::Int64),
    ] {
        let value_type = novarocks_type_contract::FunctionValueType::new(data_type, true);
        items.push(ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Null),
                value_type: value_type.clone(),
            },
            output_name: name.to_owned(),
            output_column_id: allocate_imv_column(ctx, name, value_type)?,
        });
    }
    items.extend(
        input
            .output
            .iter()
            .filter(|column| ImvActionColumn::matches(column))
            .map(project_item_for_output_column),
    );
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![left],
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
) -> Result<LogicalPlanNode, String> {
    let target = &ext.snapshot.target;
    let mut columns = vec![
        ColumnDef::from_value_type(
            input.target_apply_key_column.clone(),
            input.target_apply_key_type.clone(),
            None,
        )
        .map_err(|error| format!("invalid IMV locator apply-key declaration: {error}"))?,
    ];
    if let Some(branch) = &input.branch {
        columns.push(
            ColumnDef::from_value_type(
                branch.target_column.clone(),
                branch.target_value_type.clone(),
                None,
            )
            .map_err(|error| format!("invalid IMV locator branch declaration: {error}"))?,
        );
    }
    let metadata_columns = vec![
        ColumnDef {
            name: crate::common::ICEBERG_FILE_PATH_COL.to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::common::ICEBERG_ROW_POS_COL.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::common::ICEBERG_ROW_ID_COL.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: crate::common::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
    ];
    let mut scan_columns = vec![output_column(
        right_apply_key_id,
        &input.target_apply_key_column,
        input.target_apply_key_type.clone(),
        false,
    )];
    if let (Some(branch), Some(column_id)) = (&input.branch, right_branch_id) {
        scan_columns.push(output_column(
            column_id,
            &branch.target_column,
            branch.target_value_type.clone(),
            false,
        ));
    }
    scan_columns.push(output_column(
        right_file_id,
        crate::common::ICEBERG_FILE_PATH_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        true,
    ));
    scan_columns.push(output_column(
        right_pos_id,
        crate::common::ICEBERG_ROW_POS_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        true,
    ));
    scan_columns.push(output_column(
        right_row_id_id,
        crate::common::ICEBERG_ROW_ID_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        true,
    ));
    scan_columns.push(output_column(
        right_last_updated_seq_id,
        crate::common::ICEBERG_LAST_UPDATED_SEQ_COL,
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        true,
    ));

    Ok(LogicalPlanNode::new(
        LogicalPlanKind::scan(PlanScanNode {
            database: target.namespace.clone(),
            table: TableDef {
                name: target.table.clone(),
                columns,
                iceberg_row_lineage_metadata_columns: metadata_columns,
                source: ScanSource::Sql(SqlScanSource::new(
                    ext.snapshot.target_binding,
                    SqlTableIdentity {
                        catalog: target.catalog.clone(),
                        namespace: target.namespace.clone(),
                        table: target.table.clone(),
                    },
                    SqlScanKind::MvTargetLocator {
                        facts: SqlMvTargetLocatorScan {
                            target_table_uuid: ext.snapshot.target_table_uuid.clone(),
                            target_snapshot_id: ext.snapshot.target_snapshot_id,
                            apply_key_column: input.target_apply_key_column.clone(),
                            branch_id_column: input
                                .branch
                                .as_ref()
                                .map(|branch| branch.target_column.clone()),
                        },
                    },
                )),
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
    ))
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
                crate::common::CHANGE_OP_DELETE,
            ))),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
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
            value_type: input.target_apply_key_type.clone(),
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
                value_type: branch.target_value_type.clone(),
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
                    value_type: item.expr.value_type.clone(),

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
            .eq_ignore_ascii_case(crate::common::ICEBERG_FILE_PATH_COL)
            || column
                .name
                .eq_ignore_ascii_case(crate::common::ICEBERG_ROW_POS_COL)
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
    name.eq_ignore_ascii_case(crate::common::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::common::ICEBERG_LAST_UPDATED_SEQ_COL)
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
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, true),
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
        value_type: column.value_type.clone(),
    }
}

fn binary(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
    }
}

fn output_column(
    column_id: ColumnId,
    name: &str,
    value_type: novarocks_type_contract::FunctionValueType,
    is_internal: bool,
) -> OutputColumn {
    OutputColumn {
        column_id,
        name: name.to_string(),
        value_type,

        is_internal,
    }
}

fn is_internal_output_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(ImvActionColumn::NAME)
        || name.eq_ignore_ascii_case(crate::common::ICEBERG_FILE_PATH_COL)
        || name.eq_ignore_ascii_case(crate::common::ICEBERG_ROW_POS_COL)
        || name.eq_ignore_ascii_case(crate::common::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::common::ICEBERG_LAST_UPDATED_SEQ_COL)
        || name.eq_ignore_ascii_case(HIDDEN_APPLY_KEY_COLUMN_NAME)
        || name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME)
        || name.eq_ignore_ascii_case(JOIN_APPLY_KEY_COLUMN_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_id::ColumnRefFactory;
    use crate::compiler::mv_rewrite::SqlImvBranchContract;
    use crate::planner::imv_rewrite::annotation::ImvPlanAnnotation;
    use novarocks_type_contract::FunctionValueType;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    fn target_column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn context(target_columns: Vec<ColumnDef>) -> RewriteContext<'static> {
        let mut snapshot = (*crate::compiler::mv_rewrite::test_incremental_snapshot()).clone();
        snapshot.target_columns = Arc::from(target_columns);
        Arc::make_mut(&mut snapshot.schema_contract).branch = Some(SqlImvBranchContract {
            branch_id_column_name: BRANCH_ID_COLUMN_NAME.to_string(),
        });
        let mut ctx = RewriteContext::for_mv_refresh(Vec::new());
        let mut factory = ColumnRefFactory::new();
        factory.reserve_until(100);
        ctx.set_column_ref_factory(Rc::new(RefCell::new(factory)));
        ctx.set_extension::<ImvExtension>(ImvExtension {
            snapshot: Arc::new(snapshot),
            annotation: ImvPlanAnnotation::default(),
        });
        ctx
    }

    fn source_plan(key_nullable: bool, branch_nullable: bool) -> LogicalPlanNode {
        let output = [
            output_column(
                ColumnId(1),
                HIDDEN_APPLY_KEY_COLUMN_NAME,
                FunctionValueType::new(DataType::Int64, key_nullable),
                true,
            ),
            output_column(
                ColumnId(2),
                BRANCH_ID_COLUMN_NAME,
                FunctionValueType::new(DataType::Int32, branch_nullable),
                true,
            ),
            ImvActionColumn::output_column(ColumnId(3)),
        ];
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: output.iter().map(project_item_for_output_column).collect(),
                output_qualifier: None,
            }),
            Vec::new(),
            None,
        )
    }

    fn find_ref_type(expr: &TypedExpr, id: ColumnId) -> Option<&FunctionValueType> {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } if *column_id == id => Some(&expr.value_type),
            ExprKind::BinaryOp { left, right, .. } => {
                find_ref_type(left, id).or_else(|| find_ref_type(right, id))
            }
            _ => None,
        }
    }

    #[test]
    fn target_locator_uses_sealed_target_types_independently_of_source_nullability() {
        for target_key_nullable in [false, true] {
            let target_branch_nullable = !target_key_nullable;
            let columns = vec![
                target_column(
                    HIDDEN_APPLY_KEY_COLUMN_NAME,
                    DataType::Int64,
                    target_key_nullable,
                ),
                target_column(
                    BRANCH_ID_COLUMN_NAME,
                    DataType::Int32,
                    target_branch_nullable,
                ),
            ];
            let ctx = context(columns.clone());
            let source = source_plan(!target_key_nullable, !target_branch_nullable);
            let input = target_locator_join_input(&source, &ctx).unwrap().unwrap();
            assert_eq!(
                input.left_apply_key.value_type.nullable(),
                !target_key_nullable
            );
            assert_eq!(
                input.branch.as_ref().unwrap().left.value_type.nullable(),
                !target_branch_nullable
            );
            let plan = build_target_locator_join(source, input, &ctx).unwrap();
            let join_plan = plan.unary_input();
            let LogicalPlanKind::Join(join) = &join_plan.kind else {
                panic!("expected locator join");
            };
            let LogicalPlanKind::Scan(scan) = &join_plan.right().kind else {
                panic!("expected target scan");
            };
            let factory = ctx.column_ref_factory().unwrap().borrow();
            for (target, output) in columns.iter().zip(&scan.columns) {
                let expected = target.declared_value_type().unwrap();
                assert_eq!(output.value_type, expected);
                assert_eq!(factory.value_type(output.column_id), Some(&expected));
                assert_eq!(
                    find_ref_type(join.condition.as_ref().unwrap(), output.column_id),
                    Some(&expected)
                );
            }
            assert_eq!(
                scan.table.columns,
                columns
                    .iter()
                    .map(|column| {
                        ColumnDef::from_value_type(
                            column.name.clone(),
                            column.declared_value_type().unwrap(),
                            None,
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>()
            );
            for column in &scan.table.iceberg_row_lineage_metadata_columns {
                assert_eq!(column.nullable, is_row_lineage_locator_name(&column.name));
            }
        }
    }

    #[test]
    fn target_locator_rejects_missing_or_ambiguous_sealed_target_columns() {
        let key = target_column(HIDDEN_APPLY_KEY_COLUMN_NAME, DataType::Int64, false);
        let branch = target_column(BRANCH_ID_COLUMN_NAME, DataType::Int32, false);
        for missing in [HIDDEN_APPLY_KEY_COLUMN_NAME, BRANCH_ID_COLUMN_NAME] {
            let columns = [key.clone(), branch.clone()]
                .into_iter()
                .filter(|column| column.name != missing)
                .collect();
            let error = target_locator_join_input(&source_plan(true, true), &context(columns))
                .err()
                .expect("missing target fact must fail");
            assert!(error.contains(missing), "{error}");
        }
        for duplicated in [key.clone(), branch.clone()] {
            let mut duplicate = duplicated.clone();
            duplicate.name = duplicate.name.to_ascii_uppercase();
            let error = target_locator_join_input(
                &source_plan(true, true),
                &context(vec![key.clone(), branch.clone(), duplicate]),
            )
            .err()
            .expect("ambiguous target fact must fail");
            assert!(error.contains("unique sealed target column"), "{error}");
            assert!(error.contains(&duplicated.name), "{error}");
        }
    }
}
