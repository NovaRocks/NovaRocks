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

//! The MvRewrite Cascades transformation rule.
//!
//! Given a set of prepared `MvRewriteCandidate`s, this rule extracts the SPJG
//! view of each matched memo subtree (`SpjgDescriptor::from_memo`), checks each
//! candidate for table identity + predicate containment + aggregate rollup
//! compatibility, and — when all checks pass — injects an alternative
//! expression that reads the MV's materialized target table instead of the base
//! table. The alternative is added to the SAME memo group, so the cost-based
//! search later picks whichever is cheaper.
//!
//! StarRocks counterparts: MaterializedViewRewriter /
//! AggregatedMaterializedViewRewriter.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::sql::column_id::ColumnId;
use crate::sql::common::{LiteralValue, OutputColumn};
use crate::sql::optimizer::memo::{GroupId, MExpr, MExprId, Memo};
use crate::sql::optimizer::operator::{
    AggregateOutputLayout, FilterOp, LogicalAggregateOp, Operator, ProjectOp, ScalarAggregateSpec,
    ScalarProjectItem, ScanOp,
};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::optimizer::scalar::{
    ColumnDisplay, HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey,
};
use crate::sql::optimizer::scalar_expr;

use super::aggregate_rollup::{RollupKind, plan_rollup};
use super::column_mapping::{MvColumnMap, NormExpr, normalize};
use super::descriptor::{MatchedShape, SpjgDescriptor, SpjgOutputExpr};
use super::predicate_split::check_containment;
use super::{MvRewriteCandidate, RULE_NAME};

pub(crate) struct MvRewriteRule {
    candidates: Vec<MvRewriteCandidate>,
    /// (matched MExpr id, candidate index) pairs already attempted. The
    /// explore loop re-visits expressions every round; without this guard
    /// each round would mint fresh child groups forever.
    applied: Mutex<HashSet<(MExprId, usize)>>,
}

impl MvRewriteRule {
    pub(crate) fn new(candidates: Vec<MvRewriteCandidate>) -> Self {
        Self {
            candidates,
            applied: Mutex::new(HashSet::new()),
        }
    }
}

impl Rule for MvRewriteRule {
    fn name(&self) -> &str {
        RULE_NAME
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalAggregate(_) | Operator::LogicalFilter(_) | Operator::LogicalScan(_)
        )
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Some((query, shape)) = SpjgDescriptor::from_memo(expr, memo) else {
            return vec![];
        };
        let mut out = Vec::new();
        for (idx, cand) in self.candidates.iter().enumerate() {
            {
                let mut applied = self.applied.lock().expect("mv rewrite applied set");
                if !applied.insert((expr.id, idx)) {
                    continue;
                }
            }
            if let Some(alt) = try_rewrite(&query, &shape, cand, memo) {
                out.push(alt);
            }
        }
        out
    }
}

enum AggregateOutputPosition {
    GroupKey(usize),
    Aggregate(usize),
}

fn aggregate_output_position(
    layout: &AggregateOutputLayout,
    column_id: ColumnId,
) -> Option<AggregateOutputPosition> {
    if let Some((idx, _)) = layout
        .group_key_columns
        .iter()
        .enumerate()
        .find(|(_, column)| column.column_id == column_id)
    {
        return Some(AggregateOutputPosition::GroupKey(idx));
    }
    layout
        .aggregate_columns
        .iter()
        .enumerate()
        .find(|(_, column)| column.column_id == column_id)
        .map(|(idx, _)| AggregateOutputPosition::Aggregate(idx))
}

fn remap_visible_outputs_to_layout(
    visible_outputs: &[OutputColumn],
    original_layout: &AggregateOutputLayout,
    rewritten_layout: &AggregateOutputLayout,
) -> Option<Vec<OutputColumn>> {
    visible_outputs
        .iter()
        .map(|visible| {
            let mut output = visible.clone();
            output.column_id = match aggregate_output_position(original_layout, visible.column_id)?
            {
                AggregateOutputPosition::GroupKey(idx) => {
                    rewritten_layout.group_key_columns[idx].column_id
                }
                AggregateOutputPosition::Aggregate(idx) => {
                    rewritten_layout.aggregate_columns[idx].column_id
                }
            };
            Some(output)
        })
        .collect()
}

fn try_rewrite(
    query: &SpjgDescriptor,
    shape: &MatchedShape,
    cand: &MvRewriteCandidate,
    memo: &mut Memo,
) -> Option<NewExpr> {
    if query.joins.is_some() || cand.mv.joins.is_some() {
        return None;
    }

    // 1. Same physical base table (compare Iceberg identity, not names).
    if !same_iceberg_table(&query.table, &cand.mv.table) {
        return None;
    }
    let q_names = query.base_name_of();
    let m_names = cand.mv.base_name_of();

    // 2. Predicate containment + compensation (still over base columns).
    let containment = check_containment(
        &query.predicates,
        &memo.scalars,
        &cand.mv.predicates,
        &cand.mv_scalars,
        &q_names,
        &m_names,
    )?;

    // 3. Allocate the MV scan: one new ColumnId per MV visible output,
    //    bound by NAME to the target table columns.
    let mut scan_columns: Vec<OutputColumn> = Vec::new();
    let mut dims: Vec<(NormExpr, OutputColumn)> = Vec::new();
    let mut agg_cols: Vec<Option<OutputColumn>> = vec![None; cand.mv.outputs.len()];
    for (i, mv_out) in cand.mv.outputs.iter().enumerate() {
        let col_def = cand
            .target_table
            .columns
            .iter()
            .find(|c| c.name == mv_out.name)?; // visible-by-name mapping (spec §5)
        let id = memo.factory.create(
            Some(cand.target_table.name.clone()),
            col_def.name.clone(),
            col_def.data_type.clone(),
            col_def.nullable,
        );
        let oc = OutputColumn {
            column_id: id,
            name: col_def.name.clone(),
            data_type: col_def.data_type.clone(),
            nullable: col_def.nullable,
            is_internal: false,
        };
        scan_columns.push(oc.clone());
        match &mv_out.expr {
            SpjgOutputExpr::Dimension(e) => {
                dims.push((normalize(&cand.mv_scalars, *e, &m_names)?, oc));
            }
            SpjgOutputExpr::Aggregate(_) => agg_cols[i] = Some(oc),
        }
    }
    let col_map = MvColumnMap::new(dims);

    // 4. Compensation predicates rewritten onto MV columns. For SPJG MVs
    //    they may only land on group-key columns (spec §6.3): aggregate
    //    columns are not row-filterable. MvColumnMap only contains
    //    Dimension outputs, so any compensation touching an aggregate
    //    column simply fails to rewrite -> candidate dropped.
    let compensation: Vec<ScalarId> = containment
        .compensation
        .iter()
        .map(|p| col_map.rewrite(&mut memo.scalars, *p, &q_names))
        .collect::<Option<Vec<_>>>()?;
    let mut compensation_required_columns = HashSet::new();
    for predicate in &compensation {
        collect_required_columns(
            &memo.scalars,
            *predicate,
            &mut compensation_required_columns,
        );
    }

    // 5. Build the operator chain bottom-up.
    let scan_group = memo.new_group(MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalScan(ScanOp {
            database: cand.target_database.clone(),
            table: cand.target_table.clone(),
            alias: None,
            stats_ref: Some(cand.target_stats_ref),
            columns: scan_columns.clone(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: Some(cand.mv_name.clone()),
        }),
        children: vec![],
    });
    let mut child_group = scan_group;
    if !compensation.is_empty() {
        let predicate = scalar_expr::combine_conjuncts(&mut memo.scalars, compensation)?;
        child_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(FilterOp { predicate }),
            children: vec![scan_group],
        });
    }

    // 6. Top operator: reproduce the matched group's output ColumnIds.
    match (shape, &cand.mv.aggregate) {
        // SPJ query on SPJ MV: Project binding original output ids.
        (MatchedShape::Spj, None) => {
            let items = query
                .outputs
                .iter()
                .map(|o| {
                    let SpjgOutputExpr::Dimension(e) = &o.expr else {
                        return None;
                    };
                    let expr = col_map.rewrite(&mut memo.scalars, *e, &q_names)?;
                    Some(project_item(
                        &mut memo.scalars,
                        expr,
                        o.name.clone(),
                        o.column_id,
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            let mut required_columns = compensation_required_columns.clone();
            collect_project_required_columns(&memo.scalars, &items, &mut required_columns);
            set_mv_scan_required_columns(memo, scan_group, &scan_columns, &required_columns);
            Some(NewExpr {
                op: Operator::LogicalProject(ProjectOp {
                    items,
                    output_qualifier: None,
                }),
                children: vec![child_group],
            })
        }
        // SPJ query cannot read an aggregated MV (detail rows are gone).
        (MatchedShape::Spj, Some(_)) => None,
        // SPJG query on SPJ MV: keep the query aggregate, args rewritten.
        (MatchedShape::Spjg { original_agg }, None) => {
            let group_by = original_agg
                .group_by
                .iter()
                .map(|expr| col_map.rewrite(&mut memo.scalars, *expr, &q_names))
                .collect::<Option<Vec<_>>>()?;
            let aggregates = original_agg
                .aggregates
                .iter()
                .map(|c| rewrite_aggregate_to_mv(&mut memo.scalars, c, &col_map, &q_names))
                .collect::<Option<Vec<_>>>()?;
            let mut required_columns = compensation_required_columns.clone();
            for expr in &group_by {
                collect_required_columns(&memo.scalars, *expr, &mut required_columns);
            }
            for aggregate in &aggregates {
                collect_aggregate_required_columns(&memo.scalars, aggregate, &mut required_columns);
            }
            set_mv_scan_required_columns(memo, scan_group, &scan_columns, &required_columns);
            // DISTINCT over an SPJ MV is sound (detail rows preserved); args
            // were rewritten like any other above, so no special handling.
            Some(NewExpr {
                op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                    group_by,
                    aggregates,
                    original_agg.output_layout.clone(),
                    original_agg.output_columns.clone(),
                )),
                children: vec![child_group],
            })
        }
        // SPJG query on SPJG MV: direct mapping or rollup.
        (MatchedShape::Spjg { original_agg }, Some(mv_agg)) => {
            let plan = plan_rollup(
                &original_agg.group_by,
                &original_agg.aggregates,
                &memo.scalars,
                &q_names,
                mv_agg,
                &cand.mv.outputs,
                &cand.mv_scalars,
                &m_names,
            )?;
            match plan.kind {
                RollupKind::Direct => {
                    // One row per group already: Project binding the original
                    // visible output ids.
                    let mut items: Vec<ScalarProjectItem> = Vec::new();
                    for oc in &original_agg.output_columns {
                        let expr = match aggregate_output_position(
                            &original_agg.output_layout,
                            oc.column_id,
                        )? {
                            AggregateOutputPosition::GroupKey(idx) => col_map.rewrite(
                                &mut memo.scalars,
                                original_agg.group_by[idx],
                                &q_names,
                            )?,
                            AggregateOutputPosition::Aggregate(idx) => {
                                let item = &plan.items[idx];
                                let mv_col = agg_cols[item.mv_output_index].clone()?;
                                column_ref(&mut memo.scalars, &mv_col)
                            }
                        };
                        items.push(project_item(
                            &mut memo.scalars,
                            expr,
                            oc.name.clone(),
                            oc.column_id,
                        ));
                    }
                    let mut required_columns = compensation_required_columns.clone();
                    collect_project_required_columns(&memo.scalars, &items, &mut required_columns);
                    set_mv_scan_required_columns(
                        memo,
                        scan_group,
                        &scan_columns,
                        &required_columns,
                    );
                    Some(NewExpr {
                        op: Operator::LogicalProject(ProjectOp {
                            items,
                            output_qualifier: None,
                        }),
                        children: vec![child_group],
                    })
                }
                RollupKind::Rollup => {
                    let group_by = original_agg
                        .group_by
                        .iter()
                        .map(|expr| col_map.rewrite(&mut memo.scalars, *expr, &q_names))
                        .collect::<Option<Vec<_>>>()?;
                    let needs_coalesce = plan.items.iter().any(|i| i.needs_coalesce);
                    // Aggregate outputs: reuse original ids directly unless a
                    // COALESCE wrapper project is needed (then mint fresh ids
                    // for the aggregate and bind originals in the project).
                    let mut aggregate_columns =
                        original_agg.output_layout.aggregate_columns.clone();
                    if needs_coalesce {
                        for oc in &mut aggregate_columns {
                            oc.column_id = memo.factory.create(
                                None,
                                oc.name.clone(),
                                oc.data_type.clone(),
                                oc.nullable,
                            );
                        }
                    }
                    let output_layout = AggregateOutputLayout::new(
                        original_agg.output_layout.group_key_columns.clone(),
                        aggregate_columns,
                    );
                    let aggregates = plan
                        .items
                        .iter()
                        .enumerate()
                        .map(|(idx, item)| {
                            let mv_col = agg_cols[item.mv_output_index].clone()?;
                            Some(ScalarAggregateSpec {
                                output_column_id: output_layout.aggregate_columns[idx].column_id,
                                name: item.rollup_fn.to_string(),
                                args: vec![column_ref(&mut memo.scalars, &mv_col)],
                                distinct: false,
                                order_by: vec![],
                            })
                        })
                        .collect::<Option<Vec<_>>>()?;
                    let mut required_columns = compensation_required_columns.clone();
                    for expr in &group_by {
                        collect_required_columns(&memo.scalars, *expr, &mut required_columns);
                    }
                    for aggregate in &aggregates {
                        collect_aggregate_required_columns(
                            &memo.scalars,
                            aggregate,
                            &mut required_columns,
                        );
                    }
                    set_mv_scan_required_columns(
                        memo,
                        scan_group,
                        &scan_columns,
                        &required_columns,
                    );
                    let aggregate_visible_outputs = remap_visible_outputs_to_layout(
                        &original_agg.output_columns,
                        &original_agg.output_layout,
                        &output_layout,
                    )?;
                    let agg_op = Operator::LogicalAggregate(LogicalAggregateOp::single(
                        group_by,
                        aggregates,
                        output_layout.clone(),
                        aggregate_visible_outputs,
                    ));
                    if !needs_coalesce {
                        return Some(NewExpr {
                            op: agg_op,
                            children: vec![child_group],
                        });
                    }
                    // Scalar COUNT rollup: wrap with COALESCE(sum, 0).
                    let agg_group = memo.new_group(MExpr {
                        id: memo.next_expr_id(),
                        op: agg_op,
                        children: vec![child_group],
                    });
                    let items: Vec<ScalarProjectItem> = original_agg
                        .output_columns
                        .iter()
                        .map(|oc| {
                            let expr = match aggregate_output_position(
                                &original_agg.output_layout,
                                oc.column_id,
                            )? {
                                AggregateOutputPosition::GroupKey(idx) => column_ref(
                                    &mut memo.scalars,
                                    &output_layout.group_key_columns[idx],
                                ),
                                AggregateOutputPosition::Aggregate(idx) => {
                                    let inner = column_ref(
                                        &mut memo.scalars,
                                        &output_layout.aggregate_columns[idx],
                                    );
                                    if plan.items[idx].needs_coalesce {
                                        coalesce_zero(&mut memo.scalars, inner, oc)
                                    } else {
                                        inner
                                    }
                                }
                            };
                            Some(project_item(
                                &mut memo.scalars,
                                expr,
                                oc.name.clone(),
                                oc.column_id,
                            ))
                        })
                        .collect::<Option<Vec<_>>>()?;
                    Some(NewExpr {
                        op: Operator::LogicalProject(ProjectOp {
                            items,
                            output_qualifier: None,
                        }),
                        children: vec![agg_group],
                    })
                }
            }
        }
    }
}

fn column_ref(arena: &mut ScalarArena, c: &OutputColumn) -> ScalarId {
    arena.remember_project_output_display(c.column_id, None, c.name.clone());
    arena.intern(
        ScalarNode::ColumnRef(c.column_id),
        c.data_type.clone(),
        c.nullable,
    )
}

fn project_item(
    arena: &mut ScalarArena,
    expr: ScalarId,
    output_name: String,
    output_column_id: crate::sql::column_id::ColumnId,
) -> ScalarProjectItem {
    let expr_display = column_display(arena, expr);
    arena.remember_project_output_display(output_column_id, None, output_name.clone());
    ScalarProjectItem {
        expr,
        output_name,
        output_column_id,
        expr_display,
    }
}

fn column_display(arena: &ScalarArena, expr: ScalarId) -> Option<ColumnDisplay> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => arena.column_display(*column_id).cloned(),
        _ => None,
    }
}

fn collect_required_columns(arena: &ScalarArena, expr: ScalarId, out: &mut HashSet<ColumnId>) {
    out.extend(crate::sql::optimizer::topn_proof::collect_column_ids(arena, expr).iter());
}

fn collect_project_required_columns(
    arena: &ScalarArena,
    items: &[ScalarProjectItem],
    out: &mut HashSet<ColumnId>,
) {
    for item in items {
        collect_required_columns(arena, item.expr, out);
    }
}

fn collect_aggregate_required_columns(
    arena: &ScalarArena,
    aggregate: &ScalarAggregateSpec,
    out: &mut HashSet<ColumnId>,
) {
    for arg in &aggregate.args {
        collect_required_columns(arena, *arg, out);
    }
    for key in &aggregate.order_by {
        collect_required_columns(arena, key.expr, out);
    }
}

fn set_mv_scan_required_columns(
    memo: &mut Memo,
    scan_group: GroupId,
    scan_columns: &[OutputColumn],
    required_column_ids: &HashSet<ColumnId>,
) {
    let required_columns = scan_columns
        .iter()
        .filter(|column| required_column_ids.contains(&column.column_id))
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if required_columns.is_empty() {
        return;
    }
    let Some(expr) = memo.groups[scan_group].logical_exprs.first_mut() else {
        return;
    };
    let Operator::LogicalScan(scan) = &mut expr.op else {
        return;
    };
    scan.required_columns = Some(required_columns);
}

fn rewrite_aggregate_to_mv(
    arena: &mut ScalarArena,
    call: &ScalarAggregateSpec,
    col_map: &MvColumnMap,
    query_base_names: &std::collections::HashMap<crate::sql::column_id::ColumnId, String>,
) -> Option<ScalarAggregateSpec> {
    Some(ScalarAggregateSpec {
        output_column_id: call.output_column_id,
        name: call.name.clone(),
        args: call
            .args
            .iter()
            .map(|arg| col_map.rewrite(arena, *arg, query_base_names))
            .collect::<Option<Vec<_>>>()?,
        distinct: call.distinct,
        order_by: call
            .order_by
            .iter()
            .map(|key| rewrite_sort_key(arena, key, col_map, query_base_names))
            .collect::<Option<Vec<_>>>()?,
    })
}

fn rewrite_sort_key(
    arena: &mut ScalarArena,
    key: &SortKey,
    col_map: &MvColumnMap,
    query_base_names: &std::collections::HashMap<crate::sql::column_id::ColumnId, String>,
) -> Option<SortKey> {
    Some(SortKey {
        expr: col_map.rewrite(arena, key.expr, query_base_names)?,
        asc: key.asc,
        nulls_first: key.nulls_first,
        display: key.display.clone(),
    })
}

fn coalesce_zero(arena: &mut ScalarArena, value: ScalarId, output: &OutputColumn) -> ScalarId {
    let zero = arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(0))),
        output.data_type.clone(),
        false,
    );
    arena.intern(
        ScalarNode::FunctionCall {
            name: "coalesce".to_string(),
            args: vec![value, zero],
            distinct: false,
        },
        output.data_type.clone(),
        false,
    )
}

/// Identity match on `(catalog, namespace, table)` only. `table_uuid` and the
/// snapshot binding are deliberately ignored at this layer: this rule cannot
/// validate MV freshness. Freshness/uuid safety (so a stale MV or a
/// drop+recreate of the base table cannot match) is the responsibility of the
/// engine-side candidate preparation, which only hands the optimizer
/// candidates whose base snapshots match the MV's refresh pins.
fn same_iceberg_table(
    a: &crate::sql::planner::table::TableDef,
    b: &crate::sql::planner::table::TableDef,
) -> bool {
    use crate::sql::planner::table::ScanSource;
    match (&a.source, &b.source) {
        (
            ScanSource::IcebergDataFiles { table: ta, .. },
            ScanSource::IcebergDataFiles { table: tb, .. },
        ) => ta.catalog == tb.catalog && ta.namespace == tb.namespace && ta.table == tb.table,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::scan_model::{
        IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo,
    };
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::cascades_rules::mv_rewrite::descriptor::{
        EquiEdge, JoinInput, JoinShape,
    };
    use crate::sql::optimizer::memo::{GroupId, Memo};
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::logical::{
        LogicalAggregateNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode,
    };
    use crate::sql::planner::optimizer_bridge::scalar::materialize;
    use crate::sql::planner::payload::{AggregateCall, PlanFilterNode, PlanScanNode};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

    // --- fixture helpers --------------------------------------------------

    fn logical_plan_to_memo_for_test(plan: &LogicalPlanNode, memo: &mut Memo) -> GroupId {
        let opt_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
            plan,
            &mut memo.scalars,
        )
        .expect("logical plan to opt expr");
        crate::sql::optimizer::memo_copy::opt_expr_to_memo(&opt_expr, memo)
    }

    fn spjg_descriptor_for_test(plan: &LogicalPlanNode) -> (SpjgDescriptor, ScalarArena) {
        let mut arena = ScalarArena::new();
        let opt_expr =
            crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(plan, &mut arena)
                .expect("logical plan to opt expr");
        let descriptor = SpjgDescriptor::from_opt_expr(&opt_expr, &mut arena).expect("mv spjg");
        (descriptor, arena)
    }

    fn col(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn col_ref(c: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: c.column_id,
                qualifier: None,
                column: c.name.clone(),
            },
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn ge(left: TypedExpr, v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Ge,
                right: Box::new(int_lit(v)),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn gt(left: TypedExpr, v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Gt,
                right: Box::new(int_lit(v)),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn lt(left: TypedExpr, v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Lt,
                right: Box::new(int_lit(v)),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn or(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn iceberg_info(catalog: &str, ns: &str, tbl: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: catalog.to_string(),
            namespace: ns.to_string(),
            table: tbl.to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 0,
            location: String::new(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    /// A `TableDef` over `ScanSource::IcebergDataFiles` with the given identity
    /// and column names (all Int64). `same_iceberg_table` keys only on the
    /// `(catalog, namespace, table)` triple, so the base table and the MV
    /// target table differ ONLY in the `table` component.
    fn iceberg_table(catalog: &str, ns: &str, tbl: &str, columns: &[&str]) -> TableDef {
        TableDef {
            name: tbl.to_string(),
            columns: columns
                .iter()
                .map(|n| ColumnDef {
                    name: n.to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg_info(catalog, ns, tbl),
                files: vec![],
                cloud_properties: Default::default(),
                binding: IcebergDataFileBinding::CurrentSnapshot,
            },
        }
    }

    /// `Scan` over the base table identity `cat.ns.t` exposing `columns`.
    fn base_scan(columns: &[OutputColumn]) -> LogicalPlanNode {
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "ns".to_string(),
                table: iceberg_table("cat", "ns", "t", &names),
                alias: None,
                columns: columns.to_vec(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn join_plan(
        kind: crate::sql::common::expr::JoinKind,
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        on: Option<TypedExpr>,
    ) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: kind,
                condition: on,
            }),
            vec![left, right],
            None,
        )
    }

    fn sum_call(arg: &OutputColumn, out: &OutputColumn) -> AggregateCall {
        AggregateCall {
            name: "sum".to_string(),
            args: vec![col_ref(arg)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: out.column_id,
        }
    }

    fn count_star(out: &OutputColumn) -> AggregateCall {
        AggregateCall {
            name: "count".to_string(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: out.column_id,
        }
    }

    /// Advance `memo.factory` past id `up_to` so that freshly minted MV-scan
    /// column ids never collide with the test's hardcoded query/MV ids. This
    /// mirrors production, where the factory is shared and already advanced
    /// past every analyzer-minted id by the time MvRewrite runs.
    fn advance_factory(memo: &mut Memo, up_to: u32) {
        while memo
            .factory
            .create(None, "pad".to_string(), DataType::Int64, true)
            .0
            <= up_to
        {}
    }

    /// MV defining plan `SELECT a, b, sum(v) AS s FROM t WHERE a >= mv_low
    /// GROUP BY a, b`, over the SAME base identity but a DISTINCT id range
    /// (100..=110). Returned as a built `SpjgDescriptor` via the already-tested
    /// `from_opt_expr`.
    fn mv_descriptor(mv_low: i64) -> (SpjgDescriptor, ScalarArena) {
        let a = col(100, "a");
        let b = col(101, "b");
        let v = col(102, "v");
        let s = col(110, "s");
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a), col_ref(&b)],
                aggregates: vec![sum_call(&v, &s)],
                output_columns: vec![a.clone(), b.clone(), s.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&a), mv_low),
                }),
                vec![base_scan(&[a.clone(), b.clone(), v.clone()])],
                None,
            )],
            None,
        );
        spjg_descriptor_for_test(&plan)
    }

    /// Candidate over MV `agg_mv(a, b, s)` materializing `mv_descriptor`.
    fn agg_candidate(mv_low: i64) -> MvRewriteCandidate {
        let (mv, mv_scalars) = mv_descriptor(mv_low);
        MvRewriteCandidate {
            mv_name: "agg_mv".to_string(),
            mv,
            mv_scalars,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "agg_mv", &["a", "b", "s"]),
            target_stats_ref: stats_ref_for_test(700),
        }
    }

    fn direct_agg_candidate(mv_low: i64) -> MvRewriteCandidate {
        let a = col(100, "a");
        let v = col(102, "v");
        let s = col(110, "s");
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![sum_call(&v, &s)],
                output_columns: vec![a.clone(), s.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&a), mv_low),
                }),
                vec![base_scan(&[a.clone(), v.clone()])],
                None,
            )],
            None,
        );
        let (mv, mv_scalars) = spjg_descriptor_for_test(&plan);
        MvRewriteCandidate {
            mv_name: "direct_agg_mv".to_string(),
            mv,
            mv_scalars,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "direct_agg_mv", &["a", "s"]),
            target_stats_ref: stats_ref_for_test(703),
        }
    }

    fn stats_ref_for_test(value: u32) -> crate::sql::optimizer::stats_input::StatsRef {
        crate::sql::optimizer::stats_input::StatsRef::new(value)
    }

    fn prune_root_aggregate_outputs(
        memo: &mut Memo,
        root: GroupId,
        output_columns: Vec<OutputColumn>,
    ) {
        let Operator::LogicalAggregate(aggregate) = &mut memo.groups[root].logical_exprs[0].op
        else {
            panic!("expected root aggregate");
        };
        aggregate.output_columns = output_columns;
    }

    /// Walk a child-group chain from `gid`, following first logical expr, and
    /// return the first `LogicalScan` op reached (panics if none).
    fn find_scan(memo: &Memo, gid: usize) -> &ScanOp {
        let expr = &memo.groups[gid].logical_exprs[0];
        match &expr.op {
            Operator::LogicalScan(s) => s,
            Operator::LogicalFilter(_)
            | Operator::LogicalProject(_)
            | Operator::LogicalAggregate(_) => find_scan(memo, expr.children[0]),
            other => panic!("unexpected op while walking to scan: {other:?}"),
        }
    }

    /// True if the chain from `gid` contains a `LogicalFilter`.
    fn has_filter(memo: &Memo, gid: usize) -> bool {
        let expr = &memo.groups[gid].logical_exprs[0];
        match &expr.op {
            Operator::LogicalFilter(_) => true,
            Operator::LogicalScan(_) => false,
            _ => has_filter(memo, expr.children[0]),
        }
    }

    // --- tests ------------------------------------------------------------

    #[test]
    fn injects_rollup_alternative() {
        // Query: SELECT a, sum(v) FROM t WHERE a >= 10 GROUP BY a.
        // MV:    SELECT a, b, sum(v) s FROM t WHERE a >= 0 GROUP BY a, b.
        // Query group-by {a} ⊂ MV {a, b}  -> Rollup; a>=10 ⊃ a>=0 compensation.
        let a = col(1, "a");
        let v = col(2, "v");
        let s = col(3, "s"); // original aggregate sum output id
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![sum_call(&v, &s)],
                output_columns: vec![a.clone(), s.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&a), 10),
                }),
                vec![base_scan(&[a.clone(), v.clone()])],
                None,
            )],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![agg_candidate(0)]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1, "exactly one rollup alternative");

        // Top must be a LogicalAggregate reusing the ORIGINAL output ids.
        let Operator::LogicalAggregate(agg) = &alts[0].op else {
            panic!("expected rollup aggregate, got {:?}", alts[0].op);
        };
        assert_eq!(agg.output_columns[0].column_id, a.column_id);
        assert_eq!(agg.output_columns[1].column_id, s.column_id);
        // The rollup aggregate re-aggregates with SUM over the MV's `s` column.
        assert_eq!(agg.aggregates.len(), 1);
        assert_eq!(agg.aggregates[0].name, "sum");

        // Child chain: a compensation Filter (a >= 10) over Scan(agg_mv).
        let child = alts[0].children[0];
        assert!(has_filter(&memo, child), "compensation filter expected");
        let scan = find_scan(&memo, child);
        assert_eq!(scan.table.name, "agg_mv");
        assert_eq!(scan.stats_ref, Some(stats_ref_for_test(700)));
        assert_eq!(scan.mv_rewritten_from.as_deref(), Some("agg_mv"));

        // Idempotency: a second apply on the same expr injects nothing.
        assert!(
            rule.apply(&root_expr, &mut memo).is_empty(),
            "second apply must be a no-op"
        );
    }

    #[test]
    fn direct_rewrite_uses_layout_when_group_key_output_pruned() {
        // Query: SELECT sum(v) FROM t WHERE a >= 0 GROUP BY a.
        // The optimizer has pruned public aggregate outputs down to [sum],
        // but the internal aggregate layout still contains group key [a].
        let a = col(1, "a");
        let v = col(2, "v");
        let s = col(3, "s");
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![sum_call(&v, &s)],
                output_columns: vec![a.clone(), s.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&a), 0),
                }),
                vec![base_scan(&[a.clone(), v.clone()])],
                None,
            )],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        prune_root_aggregate_outputs(&mut memo, root, vec![s.clone()]);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![direct_agg_candidate(0)]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1);

        let Operator::LogicalProject(project) = &alts[0].op else {
            panic!("expected direct rewrite project, got {:?}", alts[0].op);
        };
        assert_eq!(project.items.len(), 1);
        assert_eq!(project.items[0].output_column_id, s.column_id);
        let expr = materialize(&memo.scalars, project.items[0].expr);
        let ExprKind::ColumnRef { column, .. } = &expr.kind else {
            panic!("expected aggregate output column ref, got {:?}", expr.kind);
        };
        assert_eq!(
            column, "s",
            "direct rewrite must bind visible aggregate output to MV aggregate column"
        );
    }

    #[test]
    fn rollup_rewrite_uses_layout_when_group_key_output_pruned() {
        // Query: SELECT sum(v) FROM t WHERE a >= 10 GROUP BY a.
        // MV:    SELECT a, b, sum(v) s FROM t WHERE a >= 0 GROUP BY a, b.
        // The query's public aggregate outputs have been pruned to [sum].
        let a = col(1, "a");
        let v = col(2, "v");
        let s = col(3, "s");
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![sum_call(&v, &s)],
                output_columns: vec![a.clone(), s.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&a), 10),
                }),
                vec![base_scan(&[a.clone(), v.clone()])],
                None,
            )],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        prune_root_aggregate_outputs(&mut memo, root, vec![s.clone()]);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![agg_candidate(0)]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1);

        let Operator::LogicalAggregate(aggregate) = &alts[0].op else {
            panic!("expected rollup aggregate, got {:?}", alts[0].op);
        };
        assert_eq!(
            aggregate
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![s.column_id],
            "rollup visible outputs should preserve the pruned public contract"
        );
        assert_eq!(
            aggregate.output_layout.group_key_columns[0].column_id,
            a.column_id
        );
        assert_eq!(
            aggregate.output_layout.aggregate_columns[0].column_id,
            s.column_id
        );
        assert_eq!(aggregate.aggregates[0].output_column_id, s.column_id);
    }

    #[test]
    fn rejects_multitable_mv_descriptor_candidate() {
        // Until memo-side multi-table descriptor matching exists, a candidate
        // that carries MV-side join shape must fail closed.
        let a = col(1, "a");
        let v = col(2, "v");
        let s = col(3, "s");
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![sum_call(&v, &s)],
                output_columns: vec![a.clone(), s.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&a), 10),
                }),
                vec![base_scan(&[a.clone(), v.clone()])],
                None,
            )],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let mut candidate = agg_candidate(0);
        candidate.mv.joins = Some(JoinShape {
            inputs: vec![JoinInput {
                table: iceberg_table("cat", "ns", "t2", &["c"]),
                scan_columns: vec![col(300, "c")],
            }],
            equi_edges: vec![EquiEdge {
                left: ColumnId(1),
                right: ColumnId(300),
            }],
        });

        let rule = MvRewriteRule::new(vec![candidate]);
        assert!(
            rule.apply(&root_expr, &mut memo).is_empty(),
            "multi-table MV descriptor must not rewrite before join matching exists"
        );
    }

    #[test]
    fn multi_table_query_descriptor_does_not_rewrite_against_single_table_candidate() {
        use crate::sql::common::expr::JoinKind;

        // Aggregate(Filter(Join(...))) is important: MvRewriteRule::matches
        // accepts Aggregate, not a bare LogicalJoin root.
        let a = col(1, "a");
        let v = col(2, "v");
        let c = col(3, "c");
        let s = col(4, "s");
        let join = join_plan(
            JoinKind::Inner,
            base_scan(&[a.clone(), v.clone()]),
            LogicalPlanNode::new(
                LogicalPlanKind::Scan(PlanScanNode {
                    database: "ns".to_string(),
                    table: iceberg_table("cat", "ns", "t2", &["c"]),
                    alias: None,
                    columns: vec![c.clone()],
                    predicates: vec![],
                    required_columns: None,
                    variant_columns: vec![],
                    mv_rewritten_from: None,
                }),
                vec![],
                None,
            ),
            Some(eq(col_ref(&a), col_ref(&c))),
        );
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![sum_call(&v, &s)],
                output_columns: vec![a.clone(), s.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&a), 10),
                }),
                vec![join],
                None,
            )],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![agg_candidate(0)]);
        assert!(
            rule.matches(&root_expr.op),
            "test must exercise a production rule entry op"
        );
        let (query, _) =
            SpjgDescriptor::from_memo(&root_expr, &mut memo).expect("query descriptor");
        assert!(
            query.joins.is_some(),
            "test must exercise the query-side join descriptor path"
        );
        assert!(
            rule.apply(&root_expr, &mut memo).is_empty(),
            "multi-table query descriptor must not rewrite against a single-table candidate yet"
        );
    }

    #[test]
    fn no_injection_when_predicate_not_contained() {
        // MV WHERE a >= 100; query WHERE a >= 10 reads rows the MV dropped.
        let a = col(1, "a");
        let v = col(2, "v");
        let s = col(3, "s");
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&a)],
                aggregates: vec![sum_call(&v, &s)],
                output_columns: vec![a.clone(), s.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&a), 10),
                }),
                vec![base_scan(&[a.clone(), v.clone()])],
                None,
            )],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![agg_candidate(100)]);
        assert!(
            rule.apply(&root_expr, &mut memo).is_empty(),
            "predicate not contained -> no alternative"
        );
    }

    #[test]
    fn spj_query_on_spj_mv_injects_project() {
        // SPJ MV: SELECT a, b, v FROM t WHERE a >= 0  (no aggregate).
        let mv_a = col(100, "a");
        let mv_b = col(101, "b");
        let mv_v = col(102, "v");
        let mv_plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: ge(col_ref(&mv_a), 0),
            }),
            vec![base_scan(&[mv_a.clone(), mv_b.clone(), mv_v.clone()])],
            None,
        );
        let (mv, mv_scalars) = spjg_descriptor_for_test(&mv_plan);
        let candidate = MvRewriteCandidate {
            mv_name: "spj_mv".to_string(),
            mv,
            mv_scalars,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "spj_mv", &["a", "b", "v"]),
            target_stats_ref: stats_ref_for_test(701),
        };

        // SPJ query: SELECT a, b FROM t WHERE a >= 10. (top = Filter(Scan))
        let a = col(1, "a");
        let b = col(2, "b");
        let v = col(3, "v");
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: ge(col_ref(&a), 10),
            }),
            vec![base_scan(&[a.clone(), b.clone(), v.clone()])],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![candidate]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1);

        // Top must be a LogicalProject binding the ORIGINAL scan output ids.
        let Operator::LogicalProject(p) = &alts[0].op else {
            panic!("expected project, got {:?}", alts[0].op);
        };
        let ids: Vec<ColumnId> = p.items.iter().map(|i| i.output_column_id).collect();
        assert_eq!(ids, vec![a.column_id, b.column_id, v.column_id]);

        // Child chain: compensation Filter (a >= 10) over Scan(spj_mv).
        let child = alts[0].children[0];
        assert!(has_filter(&memo, child));
        let scan = find_scan(&memo, child);
        assert_eq!(scan.table.name, "spj_mv");
        assert_eq!(scan.stats_ref, Some(stats_ref_for_test(701)));
        assert_eq!(scan.mv_rewritten_from.as_deref(), Some("spj_mv"));
    }

    #[test]
    fn spj_query_on_wider_spj_mv_prunes_injected_scan_columns() {
        // SPJ MV: SELECT a, b, v FROM t WHERE a >= 0.
        let mv_a = col(100, "a");
        let mv_b = col(101, "b");
        let mv_v = col(102, "v");
        let mv_plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: ge(col_ref(&mv_a), 0),
            }),
            vec![base_scan(&[mv_a.clone(), mv_b.clone(), mv_v.clone()])],
            None,
        );
        let (mv, mv_scalars) = spjg_descriptor_for_test(&mv_plan);
        let candidate = MvRewriteCandidate {
            mv_name: "wide_spj_mv".to_string(),
            mv,
            mv_scalars,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "wide_spj_mv", &["a", "b", "v"]),
            target_stats_ref: stats_ref_for_test(704),
        };

        // Query only needs a,b; the injected MV scan must not cost/read v.
        let a = col(1, "a");
        let b = col(2, "b");
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: ge(col_ref(&a), 0),
            }),
            vec![base_scan(&[a.clone(), b.clone()])],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![candidate]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1);

        let scan = find_scan(&memo, alts[0].children[0]);
        assert_eq!(
            scan.required_columns.as_deref(),
            Some(&["a".to_string(), "b".to_string()][..])
        );
    }

    #[test]
    fn spj_query_on_exact_or_spj_mv_injects_alternative() {
        // SPJ MV: SELECT a, b, v FROM t WHERE a > 10 OR b < 3.
        let mv_a = col(100, "a");
        let mv_b = col(101, "b");
        let mv_v = col(102, "v");
        let mv_predicate = or(gt(col_ref(&mv_a), 10), lt(col_ref(&mv_b), 3));
        let mv_plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: mv_predicate,
            }),
            vec![base_scan(&[mv_a.clone(), mv_b.clone(), mv_v.clone()])],
            None,
        );
        let (mv, mv_scalars) = spjg_descriptor_for_test(&mv_plan);
        let candidate = MvRewriteCandidate {
            mv_name: "or_mv".to_string(),
            mv,
            mv_scalars,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "or_mv", &["a", "b", "v"]),
            target_stats_ref: stats_ref_for_test(705),
        };

        // Query uses the same OR arms in the opposite order; normalization must
        // still match and require no compensation filter.
        let a = col(1, "a");
        let b = col(2, "b");
        let query_predicate = or(lt(col_ref(&b), 3), gt(col_ref(&a), 10));
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: query_predicate,
            }),
            vec![base_scan(&[a.clone(), b.clone()])],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![candidate]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1);
        assert!(
            !has_filter(&memo, alts[0].children[0]),
            "exact OR predicate should be fully implied by the MV"
        );
        let scan = find_scan(&memo, alts[0].children[0]);
        assert_eq!(scan.table.name, "or_mv");
        assert_eq!(scan.mv_rewritten_from.as_deref(), Some("or_mv"));
        assert_eq!(
            scan.required_columns.as_deref(),
            Some(&["a".to_string(), "b".to_string()][..])
        );
    }

    #[test]
    fn spj_scan_root_with_pushed_or_predicate_injects_alternative() {
        // Production query rewrite has already pushed the OR into the scan and
        // pruned the required scan output list before Cascades MV rewrite runs.
        let mv_a = col(100, "a");
        let mv_b = col(101, "b");
        let mv_v = col(102, "v");
        let mv_plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: or(gt(col_ref(&mv_a), 10), lt(col_ref(&mv_b), 3)),
            }),
            vec![base_scan(&[mv_a.clone(), mv_b.clone(), mv_v.clone()])],
            None,
        );
        let (mv, mv_scalars) = spjg_descriptor_for_test(&mv_plan);
        let candidate = MvRewriteCandidate {
            mv_name: "or_mv".to_string(),
            mv,
            mv_scalars,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "or_mv", &["a", "b", "v"]),
            target_stats_ref: stats_ref_for_test(706),
        };

        let a = col(1, "a");
        let b = col(2, "b");
        let v = col(3, "v");
        let query_scan = LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "ns".to_string(),
                table: iceberg_table("cat", "ns", "t", &["a", "b", "v"]),
                alias: None,
                columns: vec![a.clone(), b.clone(), v],
                predicates: vec![or(lt(col_ref(&b), 3), gt(col_ref(&a), 10))],
                required_columns: Some(vec!["a".to_string(), "b".to_string()]),
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_scan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![candidate]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1);
        let scan = find_scan(&memo, alts[0].children[0]);
        assert_eq!(scan.table.name, "or_mv");
        assert_eq!(
            scan.required_columns.as_deref(),
            Some(&["a".to_string(), "b".to_string()][..])
        );
    }

    #[test]
    fn scalar_count_rollup_wraps_with_coalesce() {
        // MV:    SELECT a, count(*) c FROM t WHERE a >= 0 GROUP BY a.
        // Query: SELECT count(*) FROM t WHERE a >= 0  (scalar, no group-by).
        // {} ⊂ {a} -> Rollup; count -> SUM over MV `c`; scalar count over an
        // empty MV result is NULL where COUNT must be 0 -> COALESCE(sum, 0).
        let mv_a = col(100, "a");
        let mv_c = col(110, "c");
        let mv_plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(&mv_a)],
                aggregates: vec![count_star(&mv_c)],
                output_columns: vec![mv_a.clone(), mv_c.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&mv_a), 0),
                }),
                vec![base_scan(std::slice::from_ref(&mv_a))],
                None,
            )],
            None,
        );
        let (mv, mv_scalars) = spjg_descriptor_for_test(&mv_plan);
        let candidate = MvRewriteCandidate {
            mv_name: "cnt_mv".to_string(),
            mv,
            mv_scalars,
            target_database: "ns".to_string(),
            target_table: iceberg_table("cat", "ns", "cnt_mv", &["a", "c"]),
            target_stats_ref: stats_ref_for_test(702),
        };

        let a = col(1, "a");
        let cnt = col(3, "cnt"); // original scalar count output id
        let query_plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![count_star(&cnt)],
                output_columns: vec![cnt.clone()],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: ge(col_ref(&a), 0),
                }),
                vec![base_scan(std::slice::from_ref(&a))],
                None,
            )],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&query_plan, &mut memo);
        advance_factory(&mut memo, 200);
        let root_expr = memo.groups[root].logical_exprs[0].clone();

        let rule = MvRewriteRule::new(vec![candidate]);
        let alts = rule.apply(&root_expr, &mut memo);
        assert_eq!(alts.len(), 1);

        // Top must be a LogicalProject whose sole item is COALESCE(_, 0) bound
        // to the ORIGINAL count output id.
        let Operator::LogicalProject(p) = &alts[0].op else {
            panic!("expected coalesce project, got {:?}", alts[0].op);
        };
        assert_eq!(p.items.len(), 1);
        assert_eq!(p.items[0].output_column_id, cnt.column_id);
        let project_expr = materialize(&memo.scalars, p.items[0].expr);
        let ExprKind::FunctionCall { name, args, .. } = &project_expr.kind else {
            panic!("expected coalesce call, got {:?}", project_expr.kind);
        };
        assert_eq!(name, "coalesce");
        assert_eq!(args.len(), 2);
        // arg0 references the inner aggregate output (a freshly-minted id, NOT
        // the original cnt id — the original id is reused only at the project).
        let ExprKind::ColumnRef { column_id, .. } = &args[0].kind else {
            panic!("coalesce arg0 must be a column ref to the inner sum");
        };
        assert_ne!(*column_id, cnt.column_id);
        // arg1 is the literal 0.
        assert!(matches!(
            &args[1].kind,
            ExprKind::Literal(LiteralValue::Int(0))
        ));

        // The child group is the inner rollup aggregate: SUM over MV `c`.
        let agg_group = alts[0].children[0];
        let Operator::LogicalAggregate(inner) = &memo.groups[agg_group].logical_exprs[0].op else {
            panic!("expected inner rollup aggregate");
        };
        assert!(inner.group_by.is_empty());
        assert_eq!(inner.aggregates.len(), 1);
        assert_eq!(inner.aggregates[0].name, "sum");
        // The inner aggregate's output id is the freshly-minted one used by the
        // coalesce arg, confirming the original id is not duplicated mid-tree.
        assert_eq!(inner.output_columns[0].column_id, *column_id);

        // Scan(cnt_mv) at the bottom (no compensation: predicates identical).
        let scan = find_scan(&memo, agg_group);
        assert_eq!(scan.table.name, "cnt_mv");
        assert_eq!(scan.mv_rewritten_from.as_deref(), Some("cnt_mv"));
    }
}
