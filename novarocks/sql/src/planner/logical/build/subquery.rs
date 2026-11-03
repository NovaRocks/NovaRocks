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
use crate::column_id::ColumnRefFactory;
use crate::common::ApplyKind;
use crate::compiler::SqlCompileError;
use crate::planner::logical::*;
use crate::planner::payload::PlanProjectNode;
use novarocks_type_contract::PureCompileControl;

use super::output::plan_output_columns;
use super::query::plan_scoped_query;

/// Project an already materialized JSON value using its original hidden binding.
/// The RHS query has finished its own DISTINCT/ORDER/LIMIT semantics first.
fn project_mark_key(
    input: LogicalPlanNode,
    source: TypedExpr,
    binding: crate::binding::SqlFunctionBinding,
    name: String,
    factory: &mut ColumnRefFactory,
    preserve_input: bool,
) -> Result<(LogicalPlanNode, TypedExpr), SqlCompileError> {
    let novarocks_functions::FunctionResultType::Scalar(value_type) = &binding.selected.result_type
    else {
        return Err("JSON key normalization binding is not scalar".into());
    };
    let value_type = value_type.clone();
    let column_id = factory.create(None, name.clone(), value_type.clone());
    let mut items = if preserve_input {
        plan_output_columns(&input)?
            .into_iter()
            .map(|column| ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: column.column_id,
                        qualifier: None,
                        column: column.name.clone(),
                    },
                    value_type: column.value_type,
                },
                output_name: column.name,
                output_column_id: column.column_id,
            })
            .collect()
    } else {
        Vec::new()
    };
    items.push(ProjectItem {
        expr: TypedExpr {
            value_type: value_type.clone(),
            kind: ExprKind::FunctionCall {
                name: novarocks_functions::json_canonical::FUNCTION_NAME.into(),
                volatility: binding.semantics.volatility,
                binding,
                args: vec![source],
                distinct: false,
            },
        },
        output_name: name.clone(),
        output_column_id: column_id,
    });
    let plan = LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: None,
        }),
        vec![input],
        None,
    );
    Ok((
        plan,
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: name,
            },
            value_type,
        },
    ))
}

// ---------------------------------------------------------------------------
// Apply spec wrapping helpers
// ---------------------------------------------------------------------------

/// Wrap `input` in a left-deep chain of `LogicalPlanKind::Apply` nodes, one per
/// spec whose clause matches `clause`. Each Apply's right child is the planned
/// inner subquery. Matching specs are consumed (removed) from `specs`; the
/// remaining specs are preserved for the other clause insertion points.
pub(super) fn wrap_scalar_applies(
    input: LogicalPlanNode,
    specs: &mut Vec<ApplyScalarSpec>,
    clause: ApplyClause,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    let mut current = input;
    let mut remaining = Vec::new();
    for spec in specs.drain(..) {
        if spec.clause != clause {
            remaining.push(spec);
            continue;
        }
        let right = plan_scoped_query(spec.inner, cte_registry, factory, control)?;

        if let Some(probe) = spec.mark_probe {
            let mut items: Vec<ProjectItem> = plan_output_columns(&current)?
                .into_iter()
                .map(|c| ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: c.column_id,
                            qualifier: None,
                            column: c.name.clone(),
                        },
                        value_type: c.value_type,
                    },
                    output_name: c.name,
                    output_column_id: c.column_id,
                })
                .collect();
            let probe_type = probe.value_type.clone();
            let probe_name = format!("__in_probe_{}", spec.subquery_id);
            let probe_id = factory.create(None, probe_name.clone(), probe_type.clone());
            items.push(ProjectItem {
                expr: probe,
                output_name: probe_name.clone(),
                output_column_id: probe_id,
            });
            current = LogicalPlanNode::new(
                LogicalPlanKind::Project(PlanProjectNode {
                    items,
                    output_qualifier: None,
                }),
                vec![current],
                None,
            );
            let rhs = plan_output_columns(&right)?
                .into_iter()
                .next()
                .ok_or("JSON mark build has no value")?;
            let [probe_binding, build_binding] = spec
                .mark_key_bindings
                .ok_or("JSON mark join has no authored normalization bindings")?;
            let decimal_overflow_policy = probe_binding.decimal_overflow_policy();
            let (probe_plan, probe_key) = project_mark_key(
                current,
                TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: probe_id,
                        qualifier: None,
                        column: probe_name,
                    },
                    value_type: probe_type,
                },
                probe_binding,
                format!("__in_probe_key_{}", spec.subquery_id),
                factory,
                true,
            )?;
            let (build_plan, build_key) = project_mark_key(
                right,
                TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: rhs.column_id,
                        qualifier: None,
                        column: rhs.name,
                    },
                    value_type: rhs.value_type,
                },
                build_binding,
                format!("__in_build_key_{}", spec.subquery_id),
                factory,
                false,
            )?;
            let predicate = TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(probe_key),
                    op: BinOp::Eq,
                    right: Box::new(build_key),
                    decimal_overflow_policy,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Boolean,
                    true,
                ),
            };
            current = LogicalPlanNode::new(
                LogicalPlanKind::Join(LogicalJoinNode {
                    naa_author: Some(Box::new(crate::common::naa::NaaAuthorFacts {
                        comparison: predicate.clone(),
                        eligibility: None,
                    })),
                    join_type: JoinKind::LeftMark {
                        mark: spec.output_column.column_id,
                    },
                    condition: Some(predicate),
                }),
                vec![probe_plan, build_plan],
                None,
            );
            continue;
        }
        // Capture the inner's single scalar output column id before right is
        // moved into the LogicalApplyNode. This id is stable across M1b pushdown rules
        // (which may add group-by keys), so it is the reliable way to find the
        // scalar result in ScalarApplyToJoin (Task 3).
        let inner_output_column_id = plan_output_columns(&right)?
            .first()
            .map(|c| c.column_id)
            .ok_or_else(|| "scalar subquery inner has no output column".to_string())?;
        // Copy output-column fields before spec.output_column is moved into the LogicalApplyNode.
        let col_id = spec.output_column.column_id;
        let col_name = spec.output_column.name.clone();
        let col_type = spec.output_column.value_type.data_type().clone();
        current = LogicalPlanNode::new(
            LogicalPlanKind::apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                inner_output_column_id,
                subquery_expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: col_id,
                        qualifier: None,
                        column: col_name,
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(col_type, true),
                },
                output_column: spec.output_column,
                correlation_column_ids: spec.correlation_column_ids,
                correlation_conjuncts: Vec::new(),
                residual_predicate: None,
                need_check_max_rows: spec.need_check_max_rows,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: std::collections::HashSet::new(),
            }),
            vec![current, right],
            None,
        );
    }
    *specs = remaining;
    Ok(current)
}

/// Wrap `input` in a left-deep chain of `LogicalPlanKind::Apply` nodes for each
/// EXISTS/IN predicate spec whose clause matches `clause`. Mirrors
/// `wrap_scalar_applies` but builds `ApplyKind::Exists` / `ApplyKind::In`
/// semi/anti-collapsing applies. The M3 to-join rules read correlation and
/// residual predicates directly from the inner Filter, so construction leaves
/// `correlation_conjuncts` empty.
pub(super) fn wrap_predicate_applies(
    input: LogicalPlanNode,
    specs: &mut Vec<ApplyPredicateSpec>,
    clause: ApplyClause,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    use crate::analysis::SubqueryKind;

    let mut current = input;
    let mut remaining = Vec::new();
    for spec in specs.drain(..) {
        if spec.clause != clause {
            remaining.push(spec);
            continue;
        }
        let right = plan_scoped_query(spec.inner, cte_registry, factory, control)?;
        let inner_output_column_id = plan_output_columns(&right)?
            .first()
            .map(|c| c.column_id)
            .ok_or_else(|| "EXISTS/IN subquery inner has no output column".to_string())?;

        let kind = match spec.kind {
            SubqueryKind::Exists { negated } => ApplyKind::Exists { negated },
            SubqueryKind::InSubquery { negated } => ApplyKind::In { negated },
            SubqueryKind::Scalar => {
                return Err(SqlCompileError::Compilation(
                    "scalar spec routed to wrap_predicate_applies".to_string(),
                ));
            }
        };

        let subquery_expr = match (&kind, spec.in_lhs.clone()) {
            (ApplyKind::In { .. }, Some(lhs)) => lhs,
            (ApplyKind::In { .. }, None) => {
                return Err(SqlCompileError::Compilation(
                    "IN spec missing analyzed LHS".to_string(),
                ));
            }
            _ => TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: spec.output_column.column_id,
                    qualifier: None,
                    column: spec.output_column.name.clone(),
                },
                value_type: spec.output_column.value_type.clone(),
            },
        };

        current = LogicalPlanNode::new(
            LogicalPlanKind::apply(LogicalApplyNode {
                kind,
                subquery_expr,
                output_column: spec.output_column,
                inner_output_column_id,
                correlation_column_ids: spec.correlation_column_ids,
                correlation_conjuncts: Vec::new(),
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: spec.use_semi_anti,
                uncorrelated_outer_predicate_columns: std::collections::HashSet::new(),
            }),
            vec![current, right],
            None,
        );
    }
    *specs = remaining;
    Ok(current)
}

// ---------------------------------------------------------------------------
// SELECT planning
// ---------------------------------------------------------------------------
