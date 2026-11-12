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

//! Immutable aggregate-state vocabulary used by SQL MV refresh planning.
//!
//! This deliberately models only the SQL shape required to construct the
//! first-refresh state projection.  Physical aggregate-state codecs and merge
//! execution remain application/runtime concerns.

use super::{AggregateFunctionKind, VisibleAggregateOutput};
use novarocks_parser::printer;
use novarocks_parser::{ast, build};

pub(crate) const SQL_MV_ROW_ID_COLUMN: &str = "__row_id__";
pub(crate) const SQL_MV_AGG_STATE_PREFIX: &str = "__agg_state_";
pub(crate) const SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN: &str = "__agg_state___ivm_row_count";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlAggregateGroupKey {
    pub(crate) output_name: String,
    pub(crate) expr: ast::Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SqlAggregateInput {
    Star,
    Expr(Box<ast::Expr>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlAggregateCall {
    pub(crate) output_name: String,
    pub(crate) function: AggregateFunctionKind,
    pub(crate) input: SqlAggregateInput,
}

/// The SQL-only aggregate contract for one MV SELECT.  It is immutable by
/// convention: application adapters construct it from already-frozen facts,
/// and SQL planning only reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlAggregateCalls {
    pub(crate) group_keys: Vec<SqlAggregateGroupKey>,
    pub(crate) aggregates: Vec<SqlAggregateCall>,
    pub(crate) visible_outputs: Vec<VisibleAggregateOutput>,
}

impl SqlAggregateCalls {
    pub(crate) fn extract(query: &ast::Query) -> Result<Self, String> {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            return Err("extract_aggregate_sql_calls: expected a plain SELECT body".to_string());
        };
        let group_by_exprs = match &select.group_by {
            ast::GroupBy::Expressions { expressions, .. } if matches!(expressions.as_slice(), [ast::Expr::Identifier(ident)] if !ident.quoted && ident.value.eq_ignore_ascii_case("all")) =>
            {
                return Err("incremental aggregate MV requires an explicit non-empty GROUP BY; GROUP BY ALL is unsupported".to_string());
            }
            ast::GroupBy::Expressions { expressions, .. } if !expressions.is_empty() => expressions,
            ast::GroupBy::Expressions { .. } | ast::GroupBy::None => {
                return Err("incremental aggregate MV requires a non-empty GROUP BY".to_string());
            }
            ast::GroupBy::Rollup { .. }
            | ast::GroupBy::Cube { .. }
            | ast::GroupBy::GroupingSets { .. } => {
                return Err("incremental aggregate MV requires an explicit non-empty GROUP BY; GROUP BY ALL is unsupported".to_string());
            }
        };

        let mut group_keys = group_by_exprs
            .iter()
            .cloned()
            .map(|expr| SqlAggregateGroupKey {
                output_name: String::new(),
                expr,
            })
            .collect::<Vec<_>>();
        let mut aggregates = Vec::new();
        let mut visible_outputs = Vec::with_capacity(select.projection.len());
        let mut projected_group_keys = vec![false; group_keys.len()];

        for item in &select.projection {
            let (expr, output_name) = projection_expr_and_output_name(item)?;
            if let Some(group_key_index) = group_keys.iter().position(|group_key| {
                printer::print_expr(&group_key.expr) == printer::print_expr(expr)
            }) {
                if group_keys[group_key_index].output_name.is_empty() {
                    group_keys[group_key_index].output_name = output_name;
                }
                projected_group_keys[group_key_index] = true;
                visible_outputs.push(VisibleAggregateOutput::GroupKey(group_key_index));
                continue;
            }
            let aggregate_index = aggregates.len();
            aggregates.push(classify_aggregate_call(expr, output_name)?);
            visible_outputs.push(VisibleAggregateOutput::Aggregate(aggregate_index));
        }
        if projected_group_keys.iter().any(|projected| !projected) {
            return Err(
                "incremental aggregate MV projection must include every GROUP BY key".to_string(),
            );
        }
        if aggregates.is_empty() {
            return Err(
                "incremental aggregate MV requires at least one aggregate output".to_string(),
            );
        }
        Ok(Self {
            group_keys,
            aggregates,
            visible_outputs,
        })
    }

    pub(crate) fn needs_retraction_count_state(&self) -> bool {
        !self.aggregates.iter().any(|aggregate| {
            aggregate.function == AggregateFunctionKind::Count
                && matches!(aggregate.input, SqlAggregateInput::Star)
        })
    }
}

pub(crate) fn state_column_name(output_name: &str) -> String {
    format!(
        "{SQL_MV_AGG_STATE_PREFIX}{}",
        sanitize_state_column_name(output_name)
    )
}

pub(crate) fn avg_sum_state_column_name(output_name: &str) -> String {
    format!("{}_avg_sum", state_column_name(output_name))
}

pub(crate) fn avg_count_state_column_name(output_name: &str) -> String {
    format!("{}_avg_count", state_column_name(output_name))
}

pub(crate) fn sanitize_state_column_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "agg".to_string()
    } else {
        sanitized
    }
}

/// Rewrite a validated aggregate SELECT into the state-shaped projection that
/// the distributed first-refresh write uses.  The SQL planner owns this text
/// shaping; runtime state codecs never enter this boundary.
pub(crate) fn rewrite_select_sql_for_state(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
) -> Result<ast::Query, String> {
    let mut query = select_query.clone();
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rewrite_select_sql_for_state: expected SELECT body".to_string());
    };

    let mut projection = Vec::with_capacity(
        calls.visible_outputs.len()
            + calls.aggregates.len()
            + usize::from(calls.needs_retraction_count_state()),
    );
    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(index) => {
                let key = calls.group_keys.get(*index).ok_or_else(|| {
                    format!("rewrite_select_sql_for_state: group key index {index} out of range")
                })?;
                projection.push(build::item(
                    key.expr.clone(),
                    Some(select_alias_ident(&key.output_name)),
                    true,
                ));
            }
            VisibleAggregateOutput::Aggregate(index) => {
                let aggregate = calls.aggregates.get(*index).ok_or_else(|| {
                    format!("rewrite_select_sql_for_state: aggregate index {index} out of range")
                })?;
                projection.extend(state_combinator_select_items(aggregate)?);
            }
        }
    }
    if calls.needs_retraction_count_state() {
        projection.push(count_star_select_item(
            SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN,
        ));
    }
    select.projection = projection;
    Ok(query)
}

fn projection_expr_and_output_name(item: &ast::SelectItem) -> Result<(&ast::Expr, String), String> {
    match item {
        ast::SelectItem::UnnamedExpr(expr) => Ok((expr, printer::print_expr(expr))),
        ast::SelectItem::ExprWithAlias { expr, alias, .. } => Ok((expr, alias.value.clone())),
        ast::SelectItem::QualifiedWildcard { .. } | ast::SelectItem::Wildcard { .. } => Err(
            "incremental aggregate MV projection can only contain expressions or aliases"
                .to_string(),
        ),
    }
}

fn classify_aggregate_call(
    expr: &ast::Expr,
    output_name: String,
) -> Result<SqlAggregateCall, String> {
    let ast::Expr::FunctionCall(function) = expr else {
        return Err(
            "incremental aggregate MV scalar projection must be a GROUP BY key or aggregate call"
                .to_string(),
        );
    };
    if function.name.parts.len() != 1
        || function.null_treatment.is_some()
        || function.over.is_some()
        || function.filter.is_some()
        || !function.order_by.is_empty()
    {
        return Err(aggregate_error());
    }
    let name = function.name.parts[0].value.to_ascii_lowercase();
    let args = &function.arguments;
    let (function, input) = match name.as_str() {
        "count" if matches!(function.quantifier, ast::FunctionQuantifier::Distinct) => (
            AggregateFunctionKind::CountDistinct,
            single_expression_input(args, "COUNT(DISTINCT)")?,
        ),
        "count" if matches!(function.quantifier, ast::FunctionQuantifier::None) => {
            count_input(args)?
        }
        "count_distinct" | "multi_distinct_count"
            if matches!(function.quantifier, ast::FunctionQuantifier::None) =>
        {
            (
                AggregateFunctionKind::CountDistinct,
                single_expression_input(args, "COUNT(DISTINCT)")?,
            )
        }
        "approx_count_distinct" | "ndv" | "hll_ndv"
            if matches!(function.quantifier, ast::FunctionQuantifier::None) =>
        {
            (
                AggregateFunctionKind::ApproxCountDistinct,
                single_expression_input(args, "APPROX_COUNT_DISTINCT")?,
            )
        }
        "sum" if matches!(function.quantifier, ast::FunctionQuantifier::None) => (
            AggregateFunctionKind::Sum,
            single_expression_input(args, "SUM")?,
        ),
        "avg" if matches!(function.quantifier, ast::FunctionQuantifier::None) => (
            AggregateFunctionKind::Avg,
            single_expression_input(args, "AVG")?,
        ),
        "min" if matches!(function.quantifier, ast::FunctionQuantifier::None) => (
            AggregateFunctionKind::Min,
            single_expression_input(args, "MIN/MAX")?,
        ),
        "max" if matches!(function.quantifier, ast::FunctionQuantifier::None) => (
            AggregateFunctionKind::Max,
            single_expression_input(args, "MIN/MAX")?,
        ),
        "bool_or" | "boolor_agg"
            if matches!(function.quantifier, ast::FunctionQuantifier::None) =>
        {
            (
                AggregateFunctionKind::BoolOr,
                single_expression_input(args, "BOOL_OR/BOOL_AND")?,
            )
        }
        "bool_and" | "booland_agg"
            if matches!(function.quantifier, ast::FunctionQuantifier::None) =>
        {
            (
                AggregateFunctionKind::BoolAnd,
                single_expression_input(args, "BOOL_OR/BOOL_AND")?,
            )
        }
        _ => return Err(aggregate_error()),
    };
    Ok(SqlAggregateCall {
        output_name,
        function,
        input,
    })
}

fn count_input(args: &[ast::Expr]) -> Result<(AggregateFunctionKind, SqlAggregateInput), String> {
    let [arg] = args else {
        return Err(aggregate_error());
    };
    match arg {
        ast::Expr::Identifier(ident) if ident.value == "*" => {
            Ok((AggregateFunctionKind::Count, SqlAggregateInput::Star))
        }
        expr => Ok((
            AggregateFunctionKind::Count,
            SqlAggregateInput::Expr(Box::new(expr.clone())),
        )),
    }
}

fn single_expression_input(args: &[ast::Expr], label: &str) -> Result<SqlAggregateInput, String> {
    let [arg] = args else {
        return Err(format!("{label} requires exactly one column expression"));
    };
    if matches!(arg, ast::Expr::Identifier(ident) if ident.value == "*") {
        return Err(format!("{label}(*) is not supported"));
    }
    Ok(SqlAggregateInput::Expr(Box::new(arg.clone())))
}

fn aggregate_error() -> String {
    "incremental aggregate MV query must be a single-table SELECT with non-empty GROUP BY and only supported aggregate outputs".to_string()
}

fn state_combinator_select_items(
    aggregate: &SqlAggregateCall,
) -> Result<Vec<ast::SelectItem>, String> {
    let argument = match &aggregate.input {
        SqlAggregateInput::Star if aggregate.function == AggregateFunctionKind::Count => {
            build::integer(1)
        }
        SqlAggregateInput::Star => {
            return Err(format!(
                "rewrite_select_sql_for_state: {} requires an expression input",
                aggregate_label(aggregate.function)
            ));
        }
        SqlAggregateInput::Expr(expr) => expr.as_ref().clone(),
    };
    if aggregate.function == AggregateFunctionKind::Avg {
        return Ok(vec![
            aggregate_select_item(
                "sum_state",
                argument.clone(),
                &avg_sum_state_column_name(&aggregate.output_name),
            )?,
            aggregate_select_item(
                "count_state",
                argument,
                &avg_count_state_column_name(&aggregate.output_name),
            )?,
        ]);
    }
    Ok(vec![aggregate_select_item(
        state_combinator_name(aggregate.function),
        argument,
        &state_column_name(&aggregate.output_name),
    )?])
}

fn aggregate_label(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "COUNT",
        AggregateFunctionKind::Sum => "SUM",
        AggregateFunctionKind::Avg => "AVG",
        AggregateFunctionKind::Min => "MIN",
        AggregateFunctionKind::Max => "MAX",
        AggregateFunctionKind::BoolOr => "BOOL_OR",
        AggregateFunctionKind::BoolAnd => "BOOL_AND",
        AggregateFunctionKind::CountDistinct => "COUNT_DISTINCT",
        AggregateFunctionKind::ApproxCountDistinct => "APPROX_COUNT_DISTINCT",
    }
}

fn state_combinator_name(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "count_state",
        AggregateFunctionKind::Sum => "sum_state",
        AggregateFunctionKind::Avg => unreachable!("AVG expands to sum_state and count_state"),
        AggregateFunctionKind::Min => "min_state",
        AggregateFunctionKind::Max => "max_state",
        AggregateFunctionKind::BoolOr => "bool_or_state",
        AggregateFunctionKind::BoolAnd => "bool_and_state",
        AggregateFunctionKind::CountDistinct => "count_distinct_state",
        AggregateFunctionKind::ApproxCountDistinct => "approx_count_distinct_state",
    }
}

fn select_alias_ident(alias: &str) -> ast::Ident {
    let mut chars = alias.chars();
    let plain = chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric());
    if plain {
        build::ident(alias, None)
    } else {
        build::ident(alias, Some('`'))
    }
}

fn aggregate_select_item(
    function_name: &str,
    argument: ast::Expr,
    alias: &str,
) -> Result<ast::SelectItem, String> {
    Ok(build::item(
        build::function(
            build::object_name(vec![build::ident(function_name, None)])
                .map_err(|error| error.to_string())?,
            vec![argument],
        ),
        Some(build::ident(alias, None)),
        true,
    ))
}

fn count_star_select_item(alias: &str) -> ast::SelectItem {
    build::item(build::count_star(), Some(build::ident(alias, None)), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        query.clone()
    }

    #[test]
    fn sqlx2_mv_aggregate_shape_is_sql_owned_and_rewrites_state() {
        let query = parse("SELECT k, sum(v) AS total FROM ice.db.fact GROUP BY k");
        let calls = SqlAggregateCalls::extract(&query).unwrap();
        assert_eq!(calls.aggregates.len(), 1);
        assert_eq!(state_column_name("total"), "__agg_state_total");
        let sql = printer::print_query(&rewrite_select_sql_for_state(&query, &calls).unwrap());
        assert!(sql.contains("sum_state(v) AS __agg_state_total"), "{sql}");
        assert!(
            sql.contains("COUNT(*) AS __agg_state___ivm_row_count"),
            "{sql}"
        );
    }

    #[test]
    fn sqlx2_mv_aggregate_shape_count_star_omits_retraction_count() {
        let calls = SqlAggregateCalls::extract(&parse(
            "SELECT k, count(*) AS total FROM ice.db.fact GROUP BY k",
        ))
        .unwrap();
        assert!(!calls.needs_retraction_count_state());
    }
}
