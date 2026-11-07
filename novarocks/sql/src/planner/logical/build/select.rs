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

use crate::analysis::cte::CTERegistry;
use crate::analysis::*;
use crate::column_id::{ColumnId, ColumnRefFactory};
use crate::compiler::SqlCompileError;
use crate::planner::logical::*;
use crate::planner::payload::*;
use novarocks_type_contract::PureCompileControl;

use super::aggregate::{
    collect_non_agg_column_refs, dedup_group_by_exprs, expr_column_id, prepare_repeat_input,
    split_projection_for_aggregate,
};
use super::relation::plan_relation_scoped;
use super::subquery::{wrap_predicate_applies, wrap_scalar_applies};
use super::window::build_window_and_project;

/// The planner consumes value labels after SQL has frozen presentation.
pub(super) struct PlanningSelect {
    pub from: Option<Relation>,
    pub filter: Option<TypedExpr>,
    pub group_by: Vec<TypedExpr>,
    pub having: Option<TypedExpr>,
    pub projection: Vec<ProjectItem>,
    pub has_aggregation: bool,
    pub distinct: bool,
    pub repeat: Option<RepeatInfo>,
    pub apply_specs: Vec<ApplyScalarSpec>,
    pub predicate_apply_specs: Vec<ApplyPredicateSpec>,
}

impl From<ResolvedSelect> for PlanningSelect {
    fn from(select: ResolvedSelect) -> Self {
        Self {
            from: select.from,
            filter: select.filter,
            group_by: select.group_by,
            having: select.having,
            projection: select
                .projection
                .into_iter()
                .map(AnalyzedProjectItem::into_planner)
                .collect(),
            has_aggregation: select.has_aggregation,
            distinct: select.distinct,
            repeat: select.repeat,
            apply_specs: select.apply_specs,
            predicate_apply_specs: select.predicate_apply_specs,
        }
    }
}

// ---------------------------------------------------------------------------
// SELECT planning
// ---------------------------------------------------------------------------

pub(super) fn plan_select_scoped(
    select: ResolvedSelect,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    plan_select_scoped_with_source(select, None, cte_registry, factory, control)
}

pub(super) fn plan_select_scoped_with_source(
    select: ResolvedSelect,
    source: Option<LogicalPlanNode>,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    let mut select = PlanningSelect::from(select);
    const REPEAT_GROUP_QUALIFIER: &str = "__repeat_group";

    // Keep the analyzer-owned occurrence identities before Repeat/Aggregate
    // substitute their same-value runtime symbols.
    let analyzed_projection_facts: Vec<_> = select
        .projection
        .iter()
        .map(|item| (item.output_column_id, item.expr.value_type.clone()))
        .collect();

    // Take ownership of all apply specs up-front. The wrap points below consume
    // them clause by clause.
    let mut apply_specs = std::mem::take(&mut select.apply_specs);
    let mut predicate_apply_specs = std::mem::take(&mut select.predicate_apply_specs);

    let mut current = match source {
        Some(source) => source,
        None => match select.from.take() {
            Some(relation) => plan_relation_scoped(relation, cte_registry, factory, control)?,
            None => LogicalPlanNode::new(
                LogicalPlanKind::Values(PlanValuesNode {
                    rows: vec![vec![]],
                    columns: vec![],
                }),
                vec![],
                None,
            ),
        },
    };

    // WHERE placement: Apply nodes for WHERE-clause scalar subqueries are
    // inserted between the FROM plan and the WHERE Filter so the output column
    // is visible when the filter expression evaluates.
    current = wrap_scalar_applies(
        current,
        &mut apply_specs,
        ApplyClause::Where,
        cte_registry,
        factory,
        control,
    )?;
    current = wrap_predicate_applies(
        current,
        &mut predicate_apply_specs,
        ApplyClause::Where,
        cte_registry,
        factory,
        control,
    )?;

    if let Some(predicate) = select.filter.take() {
        current = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode { predicate }),
            vec![current],
            None,
        );
    }

    // Scalar subqueries used by aggregate arguments must run below the
    // Aggregate. A projection Apply would be placed above it and leave the
    // aggregate argument's synthetic column absent from the child layout.
    current = wrap_scalar_applies(
        current,
        &mut apply_specs,
        ApplyClause::AggregateInput,
        cte_registry,
        factory,
        control,
    )?;

    if let Some(mut repeat_info) = select.repeat.take() {
        let grouping_key_aliases = prepare_repeat_input(
            &mut current,
            &mut select,
            &mut repeat_info,
            REPEAT_GROUP_QUALIFIER,
            factory,
            control,
        )?;
        current = LogicalPlanNode::new(
            LogicalPlanKind::Repeat(PlanRepeatNode {
                repeat_column_ref_list: repeat_info.repeat_column_ref_list,
                repeat_column_ref_ids: repeat_info.repeat_column_ref_ids,
                grouping_ids: repeat_info.grouping_ids,
                all_rollup_columns: repeat_info.all_rollup_columns,
                all_rollup_column_ids: repeat_info.all_rollup_column_ids,
                grouping_key_aliases,
                grouping_fn_args: repeat_info.grouping_fn_args,
                grouping_fn_arg_ids: repeat_info.grouping_fn_arg_ids,
                grouping_fn_ids: repeat_info.grouping_fn_ids,
                virtual_tuple_id: None,
            }),
            vec![current],
            None,
        );
    }

    if select.has_aggregation || !select.group_by.is_empty() {
        if let Some(ref having_expr) = select.having {
            // Collect the output column ids of HAVING apply specs so they are
            // not mistakenly promoted into the GROUP BY list. Those columns are
            // produced by Apply nodes that sit ABOVE the Aggregate, so the
            // Aggregate must not try to pass them through as group keys.
            let mut having_apply_col_ids: std::collections::HashSet<ColumnId> = apply_specs
                .iter()
                .filter(|s| s.clause == ApplyClause::Having)
                .map(|s| s.output_column.column_id)
                .collect();
            having_apply_col_ids.extend(
                predicate_apply_specs
                    .iter()
                    .filter(|s| s.clause == ApplyClause::Having)
                    .map(|s| s.output_column.column_id),
            );
            let mut extra_gb = Vec::new();
            collect_non_agg_column_refs(having_expr, &select.group_by, &mut extra_gb, control)?;
            for col in extra_gb {
                // Skip output columns of HAVING apply specs — they are
                // provided by the Apply node above the Aggregate, not below.
                if let ExprKind::ColumnRef { column_id, .. } = &col.kind
                    && having_apply_col_ids.contains(column_id)
                {
                    continue;
                }
                select.group_by.push(col);
            }
        }

        // Mark probes belong to the post-aggregate row domain. Include their
        // roots in the ordinary aggregate extraction, then retain the rewritten
        // expression for the owning mark join instead of projecting it early.
        let projected_count = select.projection.len();
        let mut mark_spec_indices = Vec::new();
        let mut aggregate_projection = select.projection.clone();
        for (index, spec) in apply_specs.iter().enumerate() {
            if matches!(spec.clause, ApplyClause::Having | ApplyClause::Projection)
                && let Some(probe) = &spec.mark_probe
            {
                let id = factory.create(
                    None,
                    format!("__mark_aggregate_probe_{index}"),
                    probe.value_type.clone(),
                );
                aggregate_projection.push(ProjectItem {
                    expr: probe.clone(),
                    output_name: format!("__mark_aggregate_probe_{index}"),
                    output_column_id: id,
                });
                mark_spec_indices.push(index);
                let mut extra = Vec::new();
                collect_non_agg_column_refs(probe, &select.group_by, &mut extra, control)?;
                select.group_by.extend(extra);
            }
        }
        let aggregate_group_by = dedup_group_by_exprs(&select.group_by, control)?;
        let (mut project_items, agg_calls, output_columns, rewritten_having) =
            split_projection_for_aggregate(
                &aggregate_projection,
                &aggregate_group_by,
                select.having.as_ref(),
                factory,
                control,
            )?;

        for (index, item) in mark_spec_indices
            .into_iter()
            .zip(project_items.drain(projected_count..))
        {
            apply_specs[index].mark_probe = Some(item.expr);
        }
        current = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: aggregate_group_by,
                aggregates: agg_calls,
                output_columns,
                already_pushed: false,
            }),
            vec![current],
            None,
        );

        // HAVING placement: Apply nodes for HAVING-clause scalar subqueries
        // are inserted above the Aggregate and below the HAVING Filter.
        current = wrap_scalar_applies(
            current,
            &mut apply_specs,
            ApplyClause::Having,
            cte_registry,
            factory,
            control,
        )?;
        current = wrap_predicate_applies(
            current,
            &mut predicate_apply_specs,
            ApplyClause::Having,
            cte_registry,
            factory,
            control,
        )?;

        if let Some(having) = rewritten_having {
            current = LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode { predicate: having }),
                vec![current],
                None,
            );
        }

        // Projection placement (aggregated branch): Apply nodes for
        // Projection-clause scalar subqueries are inserted before the window
        // and project so the output column is available for the SELECT list.
        current = wrap_scalar_applies(
            current,
            &mut apply_specs,
            ApplyClause::Projection,
            cte_registry,
            factory,
            control,
        )?;

        transfer_projection_provenance(&analyzed_projection_facts, &mut project_items, factory)?;
        current = build_window_and_project(current, project_items, factory)?;
    } else {
        // Projection placement (non-aggregated branch).
        current = wrap_scalar_applies(
            current,
            &mut apply_specs,
            ApplyClause::Projection,
            cte_registry,
            factory,
            control,
        )?;

        current = build_window_and_project(current, select.projection.clone(), factory)?;
    }

    debug_assert!(
        apply_specs.is_empty() && predicate_apply_specs.is_empty(),
        "unplaced apply specs: scalar={:?} predicate={:?}",
        apply_specs.iter().map(|s| s.clause).collect::<Vec<_>>(),
        predicate_apply_specs
            .iter()
            .map(|s| s.clause)
            .collect::<Vec<_>>()
    );

    // SELECT DISTINCT → Aggregate on all output columns (deduplication)
    if select.distinct {
        current = build_distinct(current, &select.projection, factory);
    }

    Ok(current)
}

/// The rewrite functions preserve each complete projected value and ordinal.
/// Carry its analyzed facts to the replacement symbol before duplicate output
/// IDs are separated. Repeated symbols must agree even when one fact is None.
fn transfer_projection_provenance(
    analyzed_facts: &[(ColumnId, novarocks_type_contract::FunctionValueType)],
    rewritten: &mut [ProjectItem],
    factory: &mut ColumnRefFactory,
) -> Result<(), String> {
    if analyzed_facts.len() != rewritten.len() {
        return Err("same-value projection rewrite changed its occurrence count".into());
    }
    let mut sources_by_target = std::collections::HashMap::new();
    for ((source, value_type), item) in analyzed_facts.iter().zip(rewritten.iter()) {
        let source = *source;
        if source == ColumnId::UNSET || item.output_column_id == ColumnId::UNSET {
            // Legacy construction tests may omit analyzer IDs; there is no
            // source fact to transfer from an unset identity.
            continue;
        }
        if value_type.data_type() != item.expr.value_type.data_type() {
            return Err("same-value projection rewrite changed its declared carrier".into());
        }
        if !value_type
            .same_physical_binding_ignoring_root_nullability_observed(&item.expr.value_type, || {
                Ok::<(), std::convert::Infallible>(())
            })
            .expect("infallible checked type comparison")
        {
            return Err("same-value projection rewrite changed its complete declared type".into());
        }
        // A symbol can be nullable at an outer-join boundary while its
        // producing expression is non-null (for example a match indicator).
        // Compare the expressions at this rewrite boundary; symbol transfer
        // and output adaptation separately check their declared contracts.
        if value_type.nullable() && !item.expr.value_type.nullable() {
            return Err("same-value projection rewrite narrowed its declared nullability".into());
        }
        if let Some(previous) = sources_by_target.insert(item.output_column_id, source)
            && (factory.borrowed_logical_type(previous) != factory.borrowed_logical_type(source)
                || factory.has_json_list_provenance(previous)
                    != factory.has_json_list_provenance(source))
        {
            return Err("same-value projection rewrite merged conflicting source domains".into());
        }
    }
    for ((source, _), item) in analyzed_facts.iter().zip(rewritten) {
        let source = *source;
        if source != ColumnId::UNSET && item.output_column_id != ColumnId::UNSET {
            if !factory.same_value_origin(source, item.output_column_id) {
                // Repeat can materialize one group key for several distinct
                // analyzed outputs. Keep each projected occurrence separate;
                // its expression still reads the shared runtime group value.
                item.output_column_id =
                    factory.create(None, item.output_name.clone(), item.expr.value_type.clone());
            }
            factory
                .transfer_value_occurrence(source, item.output_column_id)
                .map_err(str::to_owned)?;
        }
    }
    Ok(())
}

/// Build a deduplication Aggregate for SELECT DISTINCT.
/// Uses all projection columns as GROUP BY keys with no aggregate functions.
fn build_distinct(
    input: LogicalPlanNode,
    projection: &[ProjectItem],
    factory: &mut ColumnRefFactory,
) -> LogicalPlanNode {
    let mut group_by = Vec::new();
    let mut output_columns = Vec::new();
    for item in projection {
        // Prefer the pre-assigned output_column_id (e.g. synthetic __match_N
        // columns from IN/EXISTS subquery rewrites). Falling back to
        // expr_column_id would mint a fresh id, disconnecting the column from
        // any downstream reference that already holds the original id.
        let cid = if item.output_column_id != ColumnId::UNSET {
            item.output_column_id
        } else {
            expr_column_id(&item.expr, &item.output_name, factory)
        };
        group_by.push(TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: cid,
                qualifier: None,
                column: item.output_name.clone(),
            },
            value_type: item.expr.value_type.clone(),
        });
        output_columns.push(OutputColumn {
            column_id: cid,
            name: item.output_name.clone(),
            value_type: item.expr.value_type.clone(),

            is_internal: false,
        });
    }
    LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(LogicalAggregateNode {
            group_by,
            aggregates: vec![],
            output_columns,
            already_pushed: false,
        }),
        vec![input],
        None,
    )
}
