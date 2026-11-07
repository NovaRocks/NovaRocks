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

//! Temporary old-text references for the AST migration; removed with old paths.

use super::aggregate_shape::{
    SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN, SQL_MV_ROW_ID_COLUMN, SqlAggregateCalls,
    avg_count_state_column_name, avg_sum_state_column_name, state_column_name,
};
use super::first_refresh::SqlMvSnapshotPin;
use super::{AggregateFunctionKind, VisibleAggregateOutput};
use crate::planner::vocabulary::BRANCH_ID_COLUMN_NAME;
use arrow::datatypes::{DataType, Schema};
use legacy_shape::{branch_union_queries, pin_state_sql};
pub(crate) use legacy_shape::{
    prepare_projection_full_read_sql, prepare_union_projection_full_read_sql,
};
use legacy_state_shape::rewrite_select_sql_for_state;
use novarocks_parser::{
    ast::{self, SyntaxEq},
    printer,
};

pub(crate) fn require(
    site: &str,
    built: &ast::Query,
    legacy: impl FnOnce() -> Result<String, String>,
) -> Result<(), String> {
    let sql = match legacy() {
        Ok(sql) => sql,
        Err(reason) => {
            tracing::warn!("TRC-5b equivalence skipped at {site}: {reason}");
            return Ok(());
        }
    };
    let statements = match novarocks_parser::parse(&sql) {
        Ok(statements) => statements,
        Err(reason) => {
            tracing::warn!("TRC-5b equivalence skipped at {site}: {reason}");
            return Ok(());
        }
    };
    let [ast::Statement::Query(parsed)] = statements.as_slice() else {
        return Err(format!(
            "TRC-5b legacy reference at {site} did not produce one query"
        ));
    };
    if !built.syntax_eq(parsed) {
        return Err(format!(
            "TRC-5b equivalence mismatch at {site}: built={}, legacy={sql}",
            printer::print_query(built)
        ));
    }
    Ok(())
}

pub(crate) fn prepare_aggregate_first_refresh_state_sql(
    select_query: &ast::Query,
    calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let state_query = rewrite_select_sql_for_state(select_query, calls)?;
    pin_state_sql(&state_query, pin, current_catalog, current_database)
}

pub(crate) fn prepare_branch_union_aggregate_first_refresh_state_sqls(
    select_query: &ast::Query,
    branch_count: usize,
    first_branch_calls: &SqlAggregateCalls,
    pin: &SqlMvSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Vec<(SqlAggregateCalls, String)>, String> {
    branch_union_queries(
        select_query,
        branch_count,
        pin,
        current_catalog,
        current_database,
    )?
    .into_iter()
    .enumerate()
    .map(|(branch_index, (branch_query, _branch_sql))| {
        let branch_calls = SqlAggregateCalls::extract(&branch_query)?;
        if branch_index == 0 && &branch_calls != first_branch_calls {
            return Err(
                "branch UNION ALL aggregate first branch calls drifted from the validated contract"
                    .to_string(),
            );
        }
        let state_query = rewrite_select_sql_for_state(&branch_query, &branch_calls)?;
        let state_sql = novarocks_parser::printer::print_query(&state_query);
        Ok((branch_calls, state_sql))
    })
    .collect()
}

pub(crate) fn aggregate_physical_sql(
    state_sql: &str,
    calls: &SqlAggregateCalls,
    branch_id: Option<i32>,
    target_schema: Option<&Schema>,
    aggregate_input_types: Option<&[Option<DataType>]>,
) -> Result<String, String> {
    let mut projection = Vec::with_capacity(
        1 + calls.visible_outputs.len()
            + calls.aggregates.len()
            + calls
                .aggregates
                .iter()
                .filter(|aggregate| aggregate.function == AggregateFunctionKind::Avg)
                .count()
            + usize::from(branch_id.is_some()),
    );
    let group_key_refs = calls
        .group_keys
        .iter()
        .map(|key| qualified_column("state", &key.output_name))
        .collect::<Vec<_>>();
    projection.push(format!(
        "mv_group_row_id({}) AS {}",
        group_key_refs.join(", "),
        quote_sql_identifier(SQL_MV_ROW_ID_COLUMN),
    ));

    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let key = calls.group_keys.get(*group_key_index).ok_or_else(|| {
                    format!("MV first-refresh group key index {group_key_index} out of range")
                })?;
                projection.push(format!(
                    "{} AS {}",
                    qualified_column("state", &key.output_name),
                    quote_sql_identifier(&key.output_name),
                ));
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = calls.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!("MV first-refresh aggregate index {aggregate_index} out of range")
                })?;
                let state_name = state_column_name(&aggregate.output_name);
                let witness = if matches!(
                    aggregate.function,
                    AggregateFunctionKind::Sum
                        | AggregateFunctionKind::Min
                        | AggregateFunctionKind::Max
                ) {
                    target_schema
                        .and_then(|schema| {
                            schema
                                .fields()
                                .iter()
                                .find(|field| field.name() == &aggregate.output_name)
                        })
                        .map(|field| aggregate_visible_type_witness(field.data_type()))
                        .transpose()?
                } else {
                    None
                };
                let args = if aggregate.function == AggregateFunctionKind::Avg {
                    let sum_state_name = avg_sum_state_column_name(&aggregate.output_name);
                    let count_state_name = avg_count_state_column_name(&aggregate.output_name);
                    let state_args = format!(
                        "{}, {}",
                        qualified_column("state", &sum_state_name),
                        qualified_column("state", &count_state_name),
                    );
                    let input_type = aggregate_input_types
                        .and_then(|types| types.get(*aggregate_index))
                        .and_then(Option::as_ref);
                    let output_witness = target_schema
                        .and_then(|schema| {
                            schema
                                .fields()
                                .iter()
                                .find(|field| field.name() == &aggregate.output_name)
                        })
                        .map(|field| aggregate_visible_type_witness(field.data_type()))
                        .transpose()?;
                    match output_witness {
                        Some(witness) => {
                            let input_scale = match input_type {
                                Some(DataType::Decimal128(_, scale)) => i64::from(*scale),
                                _ => -1,
                            };
                            format!("{state_args}, CAST({input_scale} AS BIGINT), {witness}",)
                        }
                        None => state_args,
                    }
                } else {
                    match witness {
                        Some(witness) => {
                            format!("{}, {witness}", qualified_column("state", &state_name))
                        }
                        None => qualified_column("state", &state_name),
                    }
                };
                projection.push(format!(
                    "{}({args}) AS {}",
                    aggregate_visible_function(aggregate.function),
                    quote_sql_identifier(&aggregate.output_name),
                ));
            }
        }
    }

    for aggregate in &calls.aggregates {
        let state_names = if aggregate.function == AggregateFunctionKind::Avg {
            vec![
                avg_sum_state_column_name(&aggregate.output_name),
                avg_count_state_column_name(&aggregate.output_name),
            ]
        } else {
            vec![state_column_name(&aggregate.output_name)]
        };
        for state_name in state_names {
            projection.push(format!(
                "{} AS {}",
                qualified_column("state", &state_name),
                quote_sql_identifier(&state_name),
            ));
        }
    }
    if calls.needs_retraction_count_state() {
        projection.push(format!(
            "{} AS {}",
            qualified_column("state", SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN),
            quote_sql_identifier(SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN),
        ));
    }
    if let Some(branch_id) = branch_id {
        projection.push(format!(
            "CAST({branch_id} AS INT) AS {}",
            quote_sql_identifier(BRANCH_ID_COLUMN_NAME),
        ));
    }

    Ok(format!(
        "SELECT {} FROM ({state_sql}) AS state",
        projection.join(", "),
    ))
}

fn aggregate_visible_type_witness(data_type: &DataType) -> Result<String, String> {
    let sql_type = match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "DATETIME".to_string(),
        DataType::Decimal128(precision, scale) => format!("DECIMAL({precision},{scale})"),
        other => {
            return Err(format!(
                "unsupported MV aggregate visible target type {other:?}"
            ));
        }
    };
    Ok(format!("CAST(NULL AS {sql_type})"))
}

fn aggregate_visible_function(kind: AggregateFunctionKind) -> &'static str {
    match kind {
        AggregateFunctionKind::Count => "count_state_visible",
        AggregateFunctionKind::Sum => "sum_state_visible",
        AggregateFunctionKind::Avg => "avg_state_visible",
        AggregateFunctionKind::Min => "min_state_visible",
        AggregateFunctionKind::Max => "max_state_visible",
        AggregateFunctionKind::BoolOr => "bool_or_state_visible",
        AggregateFunctionKind::BoolAnd => "bool_and_state_visible",
        AggregateFunctionKind::CountDistinct => "count_distinct_state_visible",
        AggregateFunctionKind::ApproxCountDistinct => "approx_count_distinct_state_visible",
    }
}

fn qualified_column(qualifier: &str, column: &str) -> String {
    format!(
        "{}.{}",
        quote_sql_identifier(qualifier),
        quote_sql_identifier(column)
    )
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

mod legacy_state_shape {
    use super::super::aggregate_shape::{
        SQL_MV_AGG_RETRACTION_COUNT_STATE_COLUMN, SqlAggregateCall, SqlAggregateCalls,
        SqlAggregateInput, avg_count_state_column_name, avg_sum_state_column_name,
        state_column_name,
    };
    use super::super::{AggregateFunctionKind, VisibleAggregateOutput};
    use novarocks_parser::{Span, ast};
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
                        format!(
                            "rewrite_select_sql_for_state: group key index {index} out of range"
                        )
                    })?;
                    projection.push(ast::SelectItem::ExprWithAlias {
                        expr: key.expr.clone(),
                        alias: select_alias_ident(&key.output_name),
                        explicit_as: true,
                        span: Span::new(0, 0),
                    });
                }
                VisibleAggregateOutput::Aggregate(index) => {
                    let aggregate = calls.aggregates.get(*index).ok_or_else(|| {
                        format!(
                            "rewrite_select_sql_for_state: aggregate index {index} out of range"
                        )
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

    fn state_combinator_select_items(
        aggregate: &SqlAggregateCall,
    ) -> Result<Vec<ast::SelectItem>, String> {
        let argument = match &aggregate.input {
            SqlAggregateInput::Star if aggregate.function == AggregateFunctionKind::Count => {
                number_literal("1")
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
            synthetic_ident(alias, false)
        } else {
            synthetic_ident(alias, true)
        }
    }

    fn aggregate_select_item(
        function_name: &str,
        argument: ast::Expr,
        alias: &str,
    ) -> Result<ast::SelectItem, String> {
        let function = ast::FunctionCall {
            name: ast::ObjectName {
                parts: vec![synthetic_ident(function_name, false)],
                span: Span::new(0, 0),
            },
            arguments: vec![argument],
            quantifier: ast::FunctionQuantifier::None,
            order_by: vec![],
            separator: None,
            filter: None,
            null_treatment: None,
            over: None,
            substring_from_syntax: false,
            span: Span::new(0, 0),
        };
        Ok(ast::SelectItem::ExprWithAlias {
            expr: ast::Expr::FunctionCall(function),
            alias: synthetic_ident(alias, false),
            explicit_as: true,
            span: Span::new(0, 0),
        })
    }

    fn count_star_select_item(alias: &str) -> ast::SelectItem {
        let function = ast::FunctionCall {
            name: ast::ObjectName {
                parts: vec![synthetic_ident("COUNT", false)],
                span: Span::new(0, 0),
            },
            arguments: vec![ast::Expr::Identifier(synthetic_ident("*", false))],
            quantifier: ast::FunctionQuantifier::None,
            order_by: vec![],
            separator: None,
            filter: None,
            null_treatment: None,
            over: None,
            substring_from_syntax: false,
            span: Span::new(0, 0),
        };
        ast::SelectItem::ExprWithAlias {
            expr: ast::Expr::FunctionCall(function),
            alias: synthetic_ident(alias, false),
            explicit_as: true,
            span: Span::new(0, 0),
        }
    }

    fn synthetic_ident(value: &str, quoted: bool) -> ast::Ident {
        ast::Ident {
            value: value.to_string(),
            quoted,
            quote_style: quoted.then_some('`'),
            span: Span::new(0, 0),
        }
    }

    fn number_literal(value: &str) -> ast::Expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(value.to_string()),
            span: Span::new(0, 0),
        })
    }
}
mod legacy_shape {
    use super::super::first_refresh::SqlMvSnapshotPin;
    use crate::planner::vocabulary::{BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME};
    use novarocks_parser::{Span, ast, printer};
    use novarocks_types::naming::TableIdentity;
    use std::collections::HashSet;

    pub(crate) fn prepare_projection_full_read_sql(
        select_query: &ast::Query,
        pin: &SqlMvSnapshotPin,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<String, String> {
        let mut query = select_query.clone();
        inject_pin_as_version_as_of(&mut query, pin, current_catalog, current_database)?;
        append_physical_apply_key(query)
    }

    pub(crate) fn prepare_union_projection_full_read_sql(
        select_query: &ast::Query,
        branch_count: usize,
        pin: &SqlMvSnapshotPin,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<String, String> {
        if branch_count < 2 {
            return Err(
                "iceberg UNION ALL MV full refresh requires at least 2 branches".to_string(),
            );
        }
        let branch_count_i32 = i32::try_from(branch_count).map_err(|_| {
            format!(
                "iceberg UNION ALL MV full refresh branch count {branch_count} does not fit in i32"
            )
        })?;
        let mut query = select_query.clone();
        inject_pin_as_version_as_of(&mut query, pin, current_catalog, current_database)?;

        let mut validated_branch_count = 0;
        let mut saw_union_all = false;
        validate_union_projection_set_expr(
            query.body.as_ref(),
            branch_count,
            &mut validated_branch_count,
            &mut saw_union_all,
        )?;
        if !saw_union_all {
            return Err(
                "iceberg UNION ALL MV full refresh requires an actual UNION ALL".to_string(),
            );
        }
        if validated_branch_count != branch_count {
            return Err(format!(
                "iceberg UNION ALL MV full refresh expected {branch_count} branches, rewrote {validated_branch_count}"
            ));
        }

        let mut next_branch_id = 0;
        append_union_projection_hidden_columns(query.body.as_mut(), &mut next_branch_id)?;
        debug_assert_eq!(next_branch_id, branch_count_i32);
        Ok(printer::print_query(&query))
    }

    pub(crate) fn pin_state_sql(
        state_query: &ast::Query,
        pin: &SqlMvSnapshotPin,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<String, String> {
        let mut query = state_query.clone();
        inject_pin_as_version_as_of(&mut query, pin, current_catalog, current_database)?;
        Ok(printer::print_query(&query))
    }

    pub(crate) fn branch_union_queries(
        select_query: &ast::Query,
        branch_count: usize,
        pin: &SqlMvSnapshotPin,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<Vec<(ast::Query, String)>, String> {
        let mut query = select_query.clone();
        inject_pin_as_version_as_of(&mut query, pin, current_catalog, current_database)?;
        let mut branch_bodies = Vec::new();
        flatten_branch_union_all_set_expr(query.body.as_ref(), &mut branch_bodies)?;
        if branch_bodies.len() != branch_count {
            return Err(format!(
                "iceberg branch UNION ALL aggregate first refresh expected {branch_count} branches, found {}",
                branch_bodies.len()
            ));
        }
        branch_bodies
            .into_iter()
            .map(|body| {
                let mut branch_query = query.clone();
                branch_query.body = Box::new(body);
                let branch_sql = printer::print_query(&branch_query);
                Ok((branch_query, branch_sql))
            })
            .collect()
    }

    fn synthetic_ident(value: &str) -> ast::Ident {
        ast::Ident {
            value: value.to_string(),
            quoted: false,
            quote_style: None,
            span: Span::new(0, 0),
        }
    }

    fn number_literal(value: String) -> ast::Expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Number(value),
            span: Span::new(0, 0),
        })
    }

    fn int_type_name() -> ast::TypeName {
        ast::TypeName {
            name: ast::ObjectName {
                parts: vec![synthetic_ident("INT")],
                span: Span::new(0, 0),
            },
            arguments: vec![],
            argument_separator_spaces: vec![],
            span: Span::new(0, 0),
        }
    }

    fn append_physical_apply_key(mut query: ast::Query) -> Result<String, String> {
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            return Err("iceberg MV physical SELECT expects a SELECT body".to_string());
        };
        validate_reserved_projection_output_names(
            select,
            &[(HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key")],
        )?;
        for item in &select.projection {
            if matches!(
                item,
                ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. }
            ) {
                return Err(
                    "iceberg MV physical SELECT requires explicit projection columns".to_string(),
                );
            }
        }
        select.projection.push(ast::SelectItem::ExprWithAlias {
            expr: ast::Expr::Identifier(synthetic_ident("_row_id")),
            alias: synthetic_ident(HIDDEN_APPLY_KEY_COLUMN_NAME),
            explicit_as: true,
            span: Span::new(0, 0),
        });
        Ok(printer::print_query(&query))
    }

    fn validate_reserved_projection_output_names(
        select: &ast::Select,
        reserved: &[(&str, &str)],
    ) -> Result<(), String> {
        for item in &select.projection {
            let output_name = match item {
                ast::SelectItem::UnnamedExpr(expr) => Some(printer::print_expr(expr)),
                ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => {
                    None
                }
            };
            let Some(output_name) = output_name else {
                continue;
            };
            for (reserved_name, purpose) in reserved {
                if output_name.eq_ignore_ascii_case(reserved_name) {
                    return Err(format!(
                        "Iceberg MV output column name {reserved_name} is reserved for internal {purpose}"
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_union_projection_set_expr(
        set_expr: &ast::SetExpr,
        branch_count: usize,
        validated_branch_count: &mut usize,
        saw_union_all: &mut bool,
    ) -> Result<(), String> {
        match set_expr {
            ast::SetExpr::SetOperation(ast::SetOperation {
                operator,
                quantifier,
                left,
                right,
                ..
            }) => {
                if *operator != ast::SetOperator::Union || *quantifier != ast::SetQuantifier::All {
                    return Err(
                        "iceberg UNION ALL MV full refresh supports UNION ALL only".to_string()
                    );
                }
                *saw_union_all = true;
                validate_union_projection_set_expr(
                    left,
                    branch_count,
                    validated_branch_count,
                    saw_union_all,
                )?;
                validate_union_projection_set_expr(
                    right,
                    branch_count,
                    validated_branch_count,
                    saw_union_all,
                )
            }
            ast::SetExpr::Query(query) => validate_union_projection_set_expr(
                query.body.as_ref(),
                branch_count,
                validated_branch_count,
                saw_union_all,
            ),
            ast::SetExpr::Select(select) => {
                if *validated_branch_count >= branch_count {
                    return Err(format!(
                        "iceberg UNION ALL MV full refresh found more than {branch_count} branches"
                    ));
                }
                validate_reserved_projection_output_names(
                    select,
                    &[
                        (HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key"),
                        (BRANCH_ID_COLUMN_NAME, "branch id"),
                    ],
                )?;
                *validated_branch_count += 1;
                Ok(())
            }
            _ => Err("iceberg UNION ALL MV full refresh expects SELECT branches".to_string()),
        }
    }

    fn append_union_projection_hidden_columns(
        set_expr: &mut ast::SetExpr,
        next_branch_id: &mut i32,
    ) -> Result<(), String> {
        match set_expr {
            ast::SetExpr::SetOperation(ast::SetOperation { left, right, .. }) => {
                append_union_projection_hidden_columns(left.as_mut(), next_branch_id)?;
                append_union_projection_hidden_columns(right.as_mut(), next_branch_id)
            }
            ast::SetExpr::Query(query) => {
                append_union_projection_hidden_columns(query.body.as_mut(), next_branch_id)
            }
            ast::SetExpr::Select(select) => {
                let branch_id = *next_branch_id;
                *next_branch_id = next_branch_id
                    .checked_add(1)
                    .ok_or_else(|| "iceberg UNION ALL MV branch id overflow".to_string())?;
                select.projection.push(ast::SelectItem::ExprWithAlias {
                    expr: ast::Expr::Identifier(synthetic_ident("_row_id")),
                    alias: synthetic_ident(HIDDEN_APPLY_KEY_COLUMN_NAME),
                    explicit_as: true,
                    span: Span::new(0, 0),
                });
                select.projection.push(ast::SelectItem::ExprWithAlias {
                    expr: ast::Expr::Cast(ast::CastExpr {
                        kind: ast::CastKind::Cast,
                        expr: Box::new(number_literal(branch_id.to_string())),
                        data_type: int_type_name(),
                        format: None,
                        span: Span::new(0, 0),
                    }),
                    alias: synthetic_ident(BRANCH_ID_COLUMN_NAME),
                    explicit_as: true,
                    span: Span::new(0, 0),
                });
                Ok(())
            }
            _ => Err("iceberg UNION ALL MV full refresh expects SELECT branches".to_string()),
        }
    }

    fn inject_pin_as_version_as_of(
        query: &mut ast::Query,
        pin: &SqlMvSnapshotPin,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<usize, String> {
        let mut state = InjectState {
            pin,
            current_catalog,
            current_database,
            next_occurrence: 0,
            count: 0,
            first_error: None,
        };
        walk_query(query, &HashSet::new(), &mut state);
        if let Some(error) = state.first_error {
            return Err(error);
        }
        if state.next_occurrence != pin.len() {
            let next = &pin.occurrences()[state.next_occurrence];
            return Err(format!(
                "refresh SELECT did not contain definition occurrence {} ({})",
                next.occurrence_id().get(),
                next.table().fqn()
            ));
        }
        Ok(state.count)
    }

    struct InjectState<'a> {
        pin: &'a SqlMvSnapshotPin,
        current_catalog: Option<&'a str>,
        current_database: &'a str,
        next_occurrence: usize,
        count: usize,
        first_error: Option<String>,
    }

    fn walk_query(
        query: &mut ast::Query,
        outer_ctes: &HashSet<String>,
        state: &mut InjectState<'_>,
    ) {
        let mut body_ctes = outer_ctes.clone();
        if let Some(with) = &mut query.with {
            let local_names = with
                .ctes
                .iter()
                .map(|cte| cte.name.value.to_ascii_lowercase())
                .collect::<Vec<_>>();
            body_ctes.extend(local_names.iter().cloned());
            for cte in &mut with.ctes {
                walk_query(cte.query.as_mut(), &body_ctes, state);
            }
        }
        walk_set_expr(query.body.as_mut(), &body_ctes, state);
    }

    fn walk_set_expr(
        expr: &mut ast::SetExpr,
        visible_ctes: &HashSet<String>,
        state: &mut InjectState<'_>,
    ) {
        if state.first_error.is_some() {
            return;
        }
        match expr {
            ast::SetExpr::Select(select) => {
                for table_with_joins in &mut select.from {
                    walk_table_with_joins(table_with_joins, visible_ctes, state);
                }
            }
            ast::SetExpr::SetOperation(ast::SetOperation { left, right, .. }) => {
                walk_set_expr(left.as_mut(), visible_ctes, state);
                walk_set_expr(right.as_mut(), visible_ctes, state);
            }
            ast::SetExpr::Query(query) => walk_query(query.as_mut(), visible_ctes, state),
            _ => {}
        }
    }

    fn walk_table_with_joins(
        table_with_joins: &mut ast::TableWithJoins,
        visible_ctes: &HashSet<String>,
        state: &mut InjectState<'_>,
    ) {
        walk_factor(&mut table_with_joins.relation, visible_ctes, state);
        for join in &mut table_with_joins.joins {
            walk_factor(&mut join.relation, visible_ctes, state);
        }
    }

    fn walk_factor(
        factor: &mut ast::TableFactor,
        visible_ctes: &HashSet<String>,
        state: &mut InjectState<'_>,
    ) {
        if state.first_error.is_some() {
            return;
        }
        match factor {
            ast::TableFactor::Table { name, version, .. } => {
                let parts = name
                    .parts
                    .iter()
                    .map(|part| part.value.to_ascii_lowercase())
                    .collect::<Vec<_>>();
                if parts.len() == 1 && visible_ctes.contains(&parts[0]) {
                    return;
                }
                let Some(base_ref) =
                    resolve_table_factor(&parts, state.current_catalog, state.current_database)
                else {
                    state.first_error = Some(
                        "refresh SELECT contains an unsupported base relation identity".to_string(),
                    );
                    return;
                };
                let Some(pinned) = state.pin.occurrences().get(state.next_occurrence) else {
                    state.first_error = Some(format!(
                        "refresh SELECT contains an unpinned relation occurrence {}",
                        base_ref.fqn()
                    ));
                    return;
                };
                if !same_table_identity(pinned.table(), &base_ref) {
                    state.first_error = Some(format!(
                        "refresh SELECT occurrence {} resolved to {}, expected {}",
                        pinned.occurrence_id().get(),
                        base_ref.fqn(),
                        pinned.table().fqn()
                    ));
                    return;
                }
                if version.is_some() {
                    state.first_error = Some(format!(
                        "refresh SELECT must not write explicit FOR VERSION AS OF for occurrence {} ({}); refresh pin would conflict",
                        pinned.occurrence_id().get(),
                        base_ref.fqn(),
                    ));
                    return;
                }
                *version = Some(ast::TableVersion {
                    kind: ast::TableVersionKind::ForVersionAsOf,
                    value: number_literal(pinned.snapshot_id().to_string()),
                    span: Span::new(0, 0),
                });
                state.next_occurrence += 1;
                state.count += 1;
            }
            ast::TableFactor::Derived { subquery, .. } => {
                walk_query(subquery.as_mut(), visible_ctes, state)
            }
            ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => walk_table_with_joins(table_with_joins.as_mut(), visible_ctes, state),
            _ => {}
        }
    }

    fn same_table_identity(left: &TableIdentity, right: &TableIdentity) -> bool {
        left.catalog.eq_ignore_ascii_case(&right.catalog)
            && left.namespace.eq_ignore_ascii_case(&right.namespace)
            && left.table.eq_ignore_ascii_case(&right.table)
    }

    fn resolve_table_factor(
        parts: &[String],
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Option<TableIdentity> {
        let current_database = current_database.to_ascii_lowercase();
        let current_catalog = current_catalog.map(str::to_ascii_lowercase);
        match parts {
            [table] => current_catalog.map(|catalog| TableIdentity {
                catalog,
                namespace: current_database,
                table: table.clone(),
            }),
            [database, table] => current_catalog.map(|catalog| TableIdentity {
                catalog,
                namespace: database.clone(),
                table: table.clone(),
            }),
            [catalog, database, table] => Some(TableIdentity {
                catalog: catalog.clone(),
                namespace: database.clone(),
                table: table.clone(),
            }),
            _ => None,
        }
    }

    fn flatten_branch_union_all_set_expr(
        body: &ast::SetExpr,
        out: &mut Vec<ast::SetExpr>,
    ) -> Result<(), String> {
        match body {
            ast::SetExpr::SetOperation(ast::SetOperation {
                operator,
                quantifier,
                left,
                right,
                ..
            }) => {
                if !matches!(operator, ast::SetOperator::Union)
                    || !matches!(quantifier, ast::SetQuantifier::All)
                {
                    return Err(
                        "iceberg branch UNION ALL aggregate first refresh supports UNION ALL only"
                            .to_string(),
                    );
                }
                flatten_branch_union_all_set_expr(left, out)?;
                flatten_branch_union_all_set_expr(right, out)
            }
            ast::SetExpr::Query(query) => {
                flatten_branch_union_all_set_expr(query.body.as_ref(), out)
            }
            ast::SetExpr::Select(_) => {
                out.push(body.clone());
                Ok(())
            }
            _ => Err(
                "iceberg branch UNION ALL aggregate first refresh expects SELECT branches"
                    .to_string(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f11_layout_inputs_equal_the_original_print_and_parse_path() {
        let statements = novarocks_parser::parse(
            "SELECT k, sum(v) AS total, avg(v) AS mean FROM fact GROUP BY k",
        )
        .unwrap();
        let [ast::Statement::Query(direct)] = statements.as_slice() else {
            panic!("query fixture");
        };
        let printed = printer::print_query(direct);
        let old_statements = novarocks_parser::parse(&printed).unwrap();
        let [ast::Statement::Query(reparsed)] = old_statements.as_slice() else {
            panic!("query fixture");
        };
        assert!(direct.syntax_eq(reparsed));
        let outputs = vec![
            crate::planning::mv::SqlMvOutputColumnFacts {
                name: "k".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            },
            crate::planning::mv::SqlMvOutputColumnFacts {
                name: "total".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            },
            crate::planning::mv::SqlMvOutputColumnFacts {
                name: "mean".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Float64,
                    true,
                ),
            },
        ];
        let layout = |query| {
            let calls = crate::planning::mv::extract_aggregate_sql_calls(query).unwrap();
            let facts =
                crate::planning::mv::SqlMvAggregateLayoutFacts::from_aggregate_calls_and_outputs(
                    &calls,
                    &outputs,
                    &[Some(DataType::Int64), Some(DataType::Int64)],
                )
                .unwrap();
            crate::planning::mv_aggregate_layout::build_sql_mv_aggregate_physical_layout(&facts)
                .unwrap()
        };
        assert_eq!(layout(direct), layout(reparsed));
    }
}
