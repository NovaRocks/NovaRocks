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

//! Materialized-view query-analysis vocabulary and pure staged contract.

pub(crate) mod rebind;
pub(crate) mod refresh_property;
#[cfg(test)]
mod refresh_property_contract_tests;

use std::collections::HashSet;

use crate::mv::aggregate_state::mv_shape::AggregateMvShape;
use crate::mv::aggregate_state::sql_type::arrow_data_type_to_sql_type;
use crate::sql::analysis::{OutputColumn, QueryBody, ResolvedQuery};
use crate::sql::column_id::ColumnId;
use crate::sql::parser::ast::{
    IcebergPartitionFieldExpr, MaterializedViewDistribution, ObjectName, TableColumnDef,
};
use novarocks_catalog::identifier::normalize_identifier;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedTableRef {
    Iceberg {
        catalog: String,
        namespace: String,
        table: String,
    },
    StarRocks {
        database: String,
        table: String,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct MvAnalysis {
    pub resolved_refs: Vec<ResolvedTableRef>,
    pub output_columns: Vec<OutputColumn>,
    pub resolved_query: ResolvedQuery,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMvSelect {
    resolved_refs: Vec<ResolvedTableRef>,
    query_for_analysis: sqlparser::ast::Query,
}

impl PreparedMvSelect {
    pub(crate) fn resolved_refs(&self) -> &[ResolvedTableRef] {
        &self.resolved_refs
    }

    pub(crate) fn query_for_analysis(&self) -> &sqlparser::ast::Query {
        &self.query_for_analysis
    }
}

pub(crate) fn prepare_mv_select(
    query: &sqlparser::ast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<PreparedMvSelect, String> {
    validate_mv_select_raw_query_clauses(query)?;
    let resolved_refs = collect_table_refs_from_query(query, current_catalog, current_database);
    let mut query_for_analysis = query.clone();
    if has_three_part_refs(&resolved_refs) {
        crate::sql::parser::query_refs::strip_catalog_from_three_part_names(
            &mut query_for_analysis,
        );
    }
    Ok(PreparedMvSelect {
        resolved_refs,
        query_for_analysis,
    })
}

pub(crate) fn finish_mv_analysis(
    prepared: PreparedMvSelect,
    resolved_query: ResolvedQuery,
) -> MvAnalysis {
    let mut output_columns = resolved_query.output_columns.clone();
    if output_columns.is_empty() {
        output_columns = resolved_output_columns_from_body(&resolved_query);
    }
    MvAnalysis {
        resolved_refs: prepared.resolved_refs,
        output_columns,
        resolved_query,
    }
}

pub(crate) fn analyze_mv_select_with<Register, Analyze>(
    query: &sqlparser::ast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
    register: Register,
    analyze: Analyze,
) -> Result<MvAnalysis, String>
where
    Register: FnOnce(&[ResolvedTableRef]) -> Result<(), String>,
    Analyze: FnOnce(&sqlparser::ast::Query) -> Result<ResolvedQuery, String>,
{
    let prepared = prepare_mv_select(query, current_catalog, current_database)?;
    register(prepared.resolved_refs())?;
    let resolved_query = analyze(prepared.query_for_analysis())?;
    Ok(finish_mv_analysis(prepared, resolved_query))
}

fn validate_mv_select_raw_query_clauses(query: &sqlparser::ast::Query) -> Result<(), String> {
    if query.with.is_some() {
        return Err(unsupported_mv_query_clause("WITH"));
    }
    if query.order_by.is_some() {
        return Err(unsupported_mv_query_clause("ORDER BY"));
    }
    if query.limit_clause.is_some() {
        return Err(unsupported_mv_query_clause("LIMIT or OFFSET"));
    }
    if query.fetch.is_some() {
        return Err(unsupported_mv_query_clause("FETCH"));
    }
    if !query.locks.is_empty() {
        return Err(unsupported_mv_query_clause("locking clauses"));
    }
    if query.for_clause.is_some() {
        return Err(unsupported_mv_query_clause("FOR clauses"));
    }
    if query.settings.is_some() {
        return Err(unsupported_mv_query_clause("SETTINGS"));
    }
    if query.format_clause.is_some() {
        return Err(unsupported_mv_query_clause("FORMAT"));
    }
    if !query.pipe_operators.is_empty() {
        return Err(unsupported_mv_query_clause("pipe operators"));
    }
    validate_mv_select_raw_clauses_in_set_expr(query.body.as_ref())
}

fn validate_mv_select_raw_clauses_in_set_expr(
    expr: &sqlparser::ast::SetExpr,
) -> Result<(), String> {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            validate_mv_select_raw_select_clauses(select)?;
            for from in &select.from {
                validate_mv_select_raw_clauses_in_table_with_joins(from)?;
            }
            Ok(())
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            validate_mv_select_raw_clauses_in_set_expr(left.as_ref())?;
            validate_mv_select_raw_clauses_in_set_expr(right.as_ref())
        }
        sqlparser::ast::SetExpr::Query(query) => validate_mv_select_raw_query_clauses(query),
        sqlparser::ast::SetExpr::Values(_)
        | sqlparser::ast::SetExpr::Insert(_)
        | sqlparser::ast::SetExpr::Update(_)
        | sqlparser::ast::SetExpr::Delete(_)
        | sqlparser::ast::SetExpr::Merge(_)
        | sqlparser::ast::SetExpr::Table(_) => Ok(()),
    }
}

fn validate_mv_select_raw_select_clauses(select: &sqlparser::ast::Select) -> Result<(), String> {
    if select.select_modifiers.is_some() {
        return Err(unsupported_mv_select_clause("SELECT modifiers"));
    }
    if select.top.is_some() {
        return Err(unsupported_mv_select_clause("TOP"));
    }
    if select.exclude.is_some() {
        return Err(unsupported_mv_select_clause("EXCLUDE"));
    }
    if select.into.is_some() {
        return Err(unsupported_mv_select_clause("SELECT INTO"));
    }
    if !select.lateral_views.is_empty() {
        return Err(unsupported_mv_select_clause("LATERAL VIEW"));
    }
    if select.prewhere.is_some() {
        return Err(unsupported_mv_select_clause("PREWHERE"));
    }
    if !select.connect_by.is_empty() {
        return Err(unsupported_mv_select_clause("CONNECT BY"));
    }
    if !select.cluster_by.is_empty() {
        return Err(unsupported_mv_select_clause("CLUSTER BY"));
    }
    if !select.distribute_by.is_empty() {
        return Err(unsupported_mv_select_clause("DISTRIBUTE BY"));
    }
    if !select.sort_by.is_empty() {
        return Err(unsupported_mv_select_clause("SORT BY"));
    }
    if !select.named_window.is_empty() {
        return Err(unsupported_mv_select_clause("named WINDOW clauses"));
    }
    if select.qualify.is_some() {
        return Err(unsupported_mv_select_clause("QUALIFY"));
    }
    if select.value_table_mode.is_some() {
        return Err(unsupported_mv_select_clause("SELECT AS VALUE or STRUCT"));
    }
    Ok(())
}

fn validate_mv_select_raw_clauses_in_table_with_joins(
    table: &sqlparser::ast::TableWithJoins,
) -> Result<(), String> {
    validate_mv_select_raw_clauses_in_factor(&table.relation)?;
    for join in &table.joins {
        validate_mv_select_raw_clauses_in_factor(&join.relation)?;
    }
    Ok(())
}

fn validate_mv_select_raw_clauses_in_factor(
    factor: &sqlparser::ast::TableFactor,
) -> Result<(), String> {
    match factor {
        sqlparser::ast::TableFactor::Table {
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } => {
            if args.is_some() {
                return Err(unsupported_mv_from_clause("table function arguments"));
            }
            if !with_hints.is_empty() {
                return Err(unsupported_mv_from_clause("table hints"));
            }
            if version.is_some() {
                return Err(unsupported_mv_from_clause("table version qualifiers"));
            }
            if *with_ordinality {
                return Err(unsupported_mv_from_clause("WITH ORDINALITY"));
            }
            if !partitions.is_empty() {
                return Err(unsupported_mv_from_clause("partition selection"));
            }
            if json_path.is_some() {
                return Err(unsupported_mv_from_clause("JSON path table access"));
            }
            if sample.is_some() {
                return Err(unsupported_mv_from_clause("TABLESAMPLE"));
            }
            if !index_hints.is_empty() {
                return Err(unsupported_mv_from_clause("index hints"));
            }
            Ok(())
        }
        sqlparser::ast::TableFactor::Derived {
            lateral,
            subquery,
            sample,
            ..
        } => {
            if *lateral {
                return Err(unsupported_mv_from_clause("LATERAL derived tables"));
            }
            if sample.is_some() {
                return Err(unsupported_mv_from_clause("TABLESAMPLE"));
            }
            validate_mv_select_raw_query_clauses(subquery)
        }
        sqlparser::ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => validate_mv_select_raw_clauses_in_table_with_joins(table_with_joins),
        sqlparser::ast::TableFactor::Pivot { table, .. }
        | sqlparser::ast::TableFactor::Unpivot { table, .. }
        | sqlparser::ast::TableFactor::MatchRecognize { table, .. } => {
            validate_mv_select_raw_clauses_in_factor(table)
        }
        sqlparser::ast::TableFactor::TableFunction { .. }
        | sqlparser::ast::TableFactor::Function { .. }
        | sqlparser::ast::TableFactor::UNNEST { .. }
        | sqlparser::ast::TableFactor::JsonTable { .. }
        | sqlparser::ast::TableFactor::OpenJsonTable { .. }
        | sqlparser::ast::TableFactor::XmlTable { .. }
        | sqlparser::ast::TableFactor::SemanticView { .. } => {
            Err(unsupported_mv_from_clause("table functions"))
        }
    }
}

fn unsupported_mv_query_clause(clause: &str) -> String {
    format!("materialized view SELECT does not support {clause}")
}

fn unsupported_mv_select_clause(clause: &str) -> String {
    format!("materialized view SELECT does not support {clause}")
}

fn unsupported_mv_from_clause(clause: &str) -> String {
    format!("materialized view SELECT does not support {clause} in FROM")
}

pub(crate) fn canonicalize_iceberg_mv_select_query(
    query: &sqlparser::ast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> sqlparser::ast::Query {
    let mut query = query.clone();
    let Some(catalog) = current_catalog else {
        return query;
    };
    qualify_current_catalog_refs_in_query(
        &mut query,
        &catalog.to_ascii_lowercase(),
        &current_database.to_ascii_lowercase(),
    );
    query
}

fn qualify_current_catalog_refs_in_query(
    query: &mut sqlparser::ast::Query,
    catalog: &str,
    current_database: &str,
) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            qualify_current_catalog_refs_in_set_expr(
                cte.query.body.as_mut(),
                catalog,
                current_database,
            );
        }
    }
    qualify_current_catalog_refs_in_set_expr(query.body.as_mut(), catalog, current_database);
}

fn qualify_current_catalog_refs_in_set_expr(
    expr: &mut sqlparser::ast::SetExpr,
    catalog: &str,
    current_database: &str,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &mut select.from {
                qualify_current_catalog_refs_in_factor(
                    &mut from.relation,
                    catalog,
                    current_database,
                );
                for join in &mut from.joins {
                    qualify_current_catalog_refs_in_factor(
                        &mut join.relation,
                        catalog,
                        current_database,
                    );
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            qualify_current_catalog_refs_in_set_expr(left.as_mut(), catalog, current_database);
            qualify_current_catalog_refs_in_set_expr(right.as_mut(), catalog, current_database);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            qualify_current_catalog_refs_in_set_expr(
                query.body.as_mut(),
                catalog,
                current_database,
            );
        }
        _ => {}
    }
}

fn qualify_current_catalog_refs_in_factor(
    factor: &mut sqlparser::ast::TableFactor,
    catalog: &str,
    current_database: &str,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let qualified = match parts.as_slice() {
                [table] => Some((
                    catalog.to_string(),
                    current_database.to_string(),
                    table.clone(),
                )),
                [namespace, table] => Some((catalog.to_string(), namespace.clone(), table.clone())),
                _ => None,
            };
            if let Some((catalog, namespace, table)) = qualified {
                name.0 = vec![
                    sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(catalog)),
                    sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                        namespace,
                    )),
                    sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(table)),
                ];
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            qualify_current_catalog_refs_in_set_expr(
                subquery.body.as_mut(),
                catalog,
                current_database,
            );
        }
        _ => {}
    }
}

fn resolved_output_columns_from_body(resolved: &ResolvedQuery) -> Vec<OutputColumn> {
    match &resolved.body {
        QueryBody::Select(select) => select
            .projection
            .iter()
            .map(|item| OutputColumn {
                column_id: ColumnId::UNSET,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
            })
            .collect(),
        _ => resolved.output_columns.clone(),
    }
}

pub(crate) fn validate_distribution_columns(
    distribution: &MaterializedViewDistribution,
    output_columns: &[OutputColumn],
) -> Result<(), String> {
    for column in &distribution.hash_columns {
        let exists = output_columns
            .iter()
            .any(|output| output.name.eq_ignore_ascii_case(column));
        if !exists {
            return Err(format!(
                "DISTRIBUTED BY column `{column}` not in MV output schema"
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_aggregate_distribution_columns(
    distribution: &MaterializedViewDistribution,
    shape: &AggregateMvShape,
) -> Result<(), String> {
    let group_key_outputs = shape
        .group_keys
        .iter()
        .map(|group_key| normalize_identifier(&group_key.output_name))
        .collect::<Result<HashSet<_>, _>>()?;
    for column in &distribution.hash_columns {
        let normalized = normalize_identifier(column)?;
        if !group_key_outputs.contains(&normalized) {
            return Err(format!(
                "aggregate MV distribution column `{column}` must be a GROUP BY key output column; DISTRIBUTED BY HASH for aggregate MV can only reference GROUP BY keys"
            ));
        }
    }
    Ok(())
}

pub(crate) fn resolve_mv_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<(String, String), String> {
    match name.parts.as_slice() {
        [table] => Ok((
            normalize_identifier(current_database)?,
            normalize_identifier(table)?,
        )),
        [database, table] => Ok((
            normalize_identifier(database)?,
            normalize_identifier(table)?,
        )),
        [catalog, database, table] => {
            let catalog = normalize_identifier(catalog)?;
            if catalog != "default_catalog" {
                return Err(format!(
                    "materialized view name catalog must be `default_catalog`, got `{catalog}`"
                ));
            }
            Ok((
                normalize_identifier(database)?,
                normalize_identifier(table)?,
            ))
        }
        _ => Err(format!(
            "materialized view name must be `<name>`, `<db>.<name>`, or `default_catalog.<db>.<name>`; got `{}`",
            name.parts.join(".")
        )),
    }
}

pub(crate) fn validate_mv_partition_columns(
    partition_by: Option<&[IcebergPartitionFieldExpr]>,
    output_columns: &[OutputColumn],
) -> Result<(), String> {
    let Some(partition_by) = partition_by else {
        return Ok(());
    };
    let output_names = output_columns
        .iter()
        .map(|column| normalize_identifier(&column.name))
        .collect::<Result<HashSet<_>, _>>()?;
    for field in partition_by {
        let column = mv_partition_source_column(field);
        let normalized = normalize_identifier(column)?;
        if !output_names.contains(&normalized) {
            return Err(format!(
                "materialized view PARTITION BY column `{column}` must be an output column"
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_starrocks_mv_partition_columns(
    partition_by: Option<&[IcebergPartitionFieldExpr]>,
    output_columns: &[OutputColumn],
) -> Result<(), String> {
    if let Some(fields) = partition_by {
        for field in fields {
            if !matches!(field, IcebergPartitionFieldExpr::Identity { .. }) {
                return Err(
                    "StarRocks table materialized view PARTITION BY only supports identity columns"
                        .to_string(),
                );
            }
        }
    }
    validate_mv_partition_columns(partition_by, output_columns)
}

fn mv_partition_source_column(field: &IcebergPartitionFieldExpr) -> &str {
    match field {
        IcebergPartitionFieldExpr::Identity { column }
        | IcebergPartitionFieldExpr::Year { column }
        | IcebergPartitionFieldExpr::Month { column }
        | IcebergPartitionFieldExpr::Day { column }
        | IcebergPartitionFieldExpr::Hour { column }
        | IcebergPartitionFieldExpr::Bucket { column, .. }
        | IcebergPartitionFieldExpr::Truncate { column, .. }
        | IcebergPartitionFieldExpr::Void { column } => column,
    }
}

fn collect_table_refs_from_query(
    query: &sqlparser::ast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Vec<ResolvedTableRef> {
    let mut refs = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_table_refs_from_set_expr(
                cte.query.body.as_ref(),
                current_catalog,
                current_database,
                &mut refs,
            );
        }
    }
    collect_table_refs_from_set_expr(
        query.body.as_ref(),
        current_catalog,
        current_database,
        &mut refs,
    );
    refs
}

fn collect_table_refs_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    current_catalog: Option<&str>,
    current_database: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                collect_table_refs_from_factor(
                    &from.relation,
                    current_catalog,
                    current_database,
                    refs,
                );
                for join in &from.joins {
                    collect_table_refs_from_factor(
                        &join.relation,
                        current_catalog,
                        current_database,
                        refs,
                    );
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            collect_table_refs_from_set_expr(left, current_catalog, current_database, refs);
            collect_table_refs_from_set_expr(right, current_catalog, current_database, refs);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            collect_table_refs_from_set_expr(
                query.body.as_ref(),
                current_catalog,
                current_database,
                refs,
            );
        }
        _ => {}
    }
}

fn collect_table_refs_from_factor(
    factor: &sqlparser::ast::TableFactor,
    current_catalog: Option<&str>,
    current_database: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect();
            let resolved = match parts.as_slice() {
                [catalog, namespace, table] => ResolvedTableRef::Iceberg {
                    catalog: catalog.clone(),
                    namespace: namespace.clone(),
                    table: table.clone(),
                },
                [table] => match current_catalog {
                    Some(catalog) => ResolvedTableRef::Iceberg {
                        catalog: catalog.to_ascii_lowercase(),
                        namespace: current_database.to_ascii_lowercase(),
                        table: table.clone(),
                    },
                    None => ResolvedTableRef::StarRocks {
                        database: current_database.to_ascii_lowercase(),
                        table: table.clone(),
                    },
                },
                [database, table] => match current_catalog {
                    Some(catalog) => ResolvedTableRef::Iceberg {
                        catalog: catalog.to_ascii_lowercase(),
                        namespace: database.clone(),
                        table: table.clone(),
                    },
                    None => ResolvedTableRef::StarRocks {
                        database: database.clone(),
                        table: table.clone(),
                    },
                },
                _ => {
                    let rendered = parts.join(".");
                    ResolvedTableRef::StarRocks {
                        database: current_database.to_ascii_lowercase(),
                        table: rendered,
                    }
                }
            };
            if !refs.contains(&resolved) {
                refs.push(resolved);
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            if let Some(with) = &subquery.with {
                for cte in &with.cte_tables {
                    collect_table_refs_from_set_expr(
                        cte.query.body.as_ref(),
                        current_catalog,
                        current_database,
                        refs,
                    );
                }
            }
            collect_table_refs_from_set_expr(
                subquery.body.as_ref(),
                current_catalog,
                current_database,
                refs,
            );
        }
        _ => {}
    }
}

fn has_three_part_refs(resolved_refs: &[ResolvedTableRef]) -> bool {
    resolved_refs
        .iter()
        .any(|table_ref| matches!(table_ref, ResolvedTableRef::Iceberg { .. }))
}

pub(crate) fn output_column_to_table_column(
    column: &OutputColumn,
) -> Result<TableColumnDef, String> {
    Ok(TableColumnDef {
        name: column.name.clone(),
        data_type: arrow_data_type_to_sql_type(&column.data_type)?,
        nullable: column.nullable,
        aggregation: None,
        default: None,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::ResolvedQuery;
    use crate::sql::catalog::local::PlannerMemoryCatalog;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize query");
        let statement =
            crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query")
        };
        *query
    }

    fn analyze_literal_query(sql: &str) -> ResolvedQuery {
        let query = parse_query(sql);
        let catalog = PlannerMemoryCatalog::default();
        let (resolved, _, _) = crate::sql::analyzer::analyze(&query, &catalog, "default")
            .expect("analyze literal query");
        resolved
    }

    #[test]
    fn prepare_mv_select_rejects_existing_unsupported_clause_with_exact_error() {
        let query = parse_query("SELECT 1 ORDER BY 1");

        let error = prepare_mv_select(&query, None, "default").expect_err("reject ORDER BY");

        assert_eq!(error, "materialized view SELECT does not support ORDER BY");
    }

    #[test]
    fn prepare_mv_select_preserves_ref_order_and_strips_three_part_catalog_for_analysis() {
        let query = parse_query(
            "SELECT a.id, b.id FROM Ice.Sales.First a JOIN ICE.Sales.Second b ON a.id = b.id",
        );

        let prepared = prepare_mv_select(&query, Some("ice"), "sales").expect("prepare query");

        assert_eq!(
            prepared.resolved_refs(),
            &[
                ResolvedTableRef::Iceberg {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "first".to_string(),
                },
                ResolvedTableRef::Iceberg {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "second".to_string(),
                },
            ]
        );
        let analysis_sql = prepared
            .query_for_analysis()
            .to_string()
            .to_ascii_lowercase();
        assert!(analysis_sql.contains("sales.first"), "{analysis_sql}");
        assert!(analysis_sql.contains("sales.second"), "{analysis_sql}");
        assert!(!analysis_sql.contains("ice.sales"), "{analysis_sql}");
    }

    #[test]
    fn finish_mv_analysis_uses_resolved_output_columns_when_present() {
        let query = parse_query("SELECT 1 AS projected_name");
        let prepared = prepare_mv_select(&query, None, "default").expect("prepare query");
        let mut resolved = analyze_literal_query("SELECT 1 AS projected_name");
        resolved.output_columns[0].name = "resolved_name".to_string();

        let analysis = finish_mv_analysis(prepared, resolved);

        assert_eq!(analysis.output_columns[0].name, "resolved_name");
        assert_eq!(analysis.output_columns[0].data_type, DataType::Int64);
    }

    #[test]
    fn finish_mv_analysis_falls_back_to_projection_outputs_when_analyzer_output_is_empty() {
        let query = parse_query("SELECT 1 AS projection_name");
        let prepared = prepare_mv_select(&query, None, "default").expect("prepare query");
        let mut resolved = analyze_literal_query("SELECT 1 AS projection_name");
        resolved.output_columns.clear();

        let analysis = finish_mv_analysis(prepared, resolved);

        assert_eq!(analysis.output_columns.len(), 1);
        assert_eq!(analysis.output_columns[0].name, "projection_name");
        assert_eq!(analysis.output_columns[0].data_type, DataType::Int64);
    }

    #[test]
    fn resolve_mv_name_accepts_supported_forms_and_rejects_non_default_catalog() {
        assert_eq!(
            resolve_mv_name(
                &crate::sql::parser::ast::ObjectName {
                    parts: vec!["Orders".to_string()],
                },
                "Sales",
            ),
            Ok(("sales".to_string(), "orders".to_string()))
        );
        assert_eq!(
            resolve_mv_name(
                &crate::sql::parser::ast::ObjectName {
                    parts: vec!["Marketing".to_string(), "Orders".to_string()],
                },
                "Sales",
            ),
            Ok(("marketing".to_string(), "orders".to_string()))
        );
        assert_eq!(
            resolve_mv_name(
                &crate::sql::parser::ast::ObjectName {
                    parts: vec![
                        "default_catalog".to_string(),
                        "Marketing".to_string(),
                        "Orders".to_string(),
                    ],
                },
                "Sales",
            ),
            Ok(("marketing".to_string(), "orders".to_string()))
        );
        assert_eq!(
            resolve_mv_name(
                &crate::sql::parser::ast::ObjectName {
                    parts: vec![
                        "ice".to_string(),
                        "Marketing".to_string(),
                        "Orders".to_string(),
                    ],
                },
                "Sales",
            ),
            Err("materialized view name catalog must be `default_catalog`, got `ice`".to_string())
        );
    }

    #[test]
    fn canonicalize_iceberg_mv_select_query_qualifies_persisted_refs() {
        let query =
            parse_query("SELECT o.id FROM Orders o JOIN Marketing.Customers c ON o.id = c.id");

        let canonical =
            canonicalize_iceberg_mv_select_query(&query, Some("ICE"), "Sales").to_string();
        let canonical = canonical.to_ascii_lowercase();

        assert!(canonical.contains("ice.sales.orders"), "{canonical}");
        assert!(canonical.contains("ice.marketing.customers"), "{canonical}");
    }

    #[test]
    fn analysis_orchestration_preserves_staged_contract() {
        let query = parse_query(
            "SELECT a.id, b.id FROM Ice.Sales.First a JOIN ICE.Sales.Second b ON a.id = b.id",
        );
        let events = RefCell::new(Vec::new());
        let registered_refs = RefCell::new(Vec::new());
        let analyzer_sql = RefCell::new(String::new());
        let mut resolved = analyze_literal_query("SELECT 1 AS projection_name");
        resolved.output_columns[0].name = "resolved_name".to_string();

        let analysis = analyze_mv_select_with(
            &query,
            Some("ice"),
            "sales",
            |refs: &[ResolvedTableRef]| {
                events.borrow_mut().push("register");
                registered_refs.borrow_mut().extend_from_slice(refs);
                Ok(())
            },
            |query_for_analysis: &sqlparser::ast::Query| {
                events.borrow_mut().push("analyze");
                *analyzer_sql.borrow_mut() = query_for_analysis.to_string();
                Ok(resolved)
            },
        )
        .expect("analyze through shared orchestration");

        assert_eq!(&*events.borrow(), &["register", "analyze"]);
        assert_eq!(
            &*registered_refs.borrow(),
            &[
                ResolvedTableRef::Iceberg {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "first".to_string(),
                },
                ResolvedTableRef::Iceberg {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "second".to_string(),
                },
            ]
        );
        let analyzer_sql = analyzer_sql.borrow().to_ascii_lowercase();
        assert!(analyzer_sql.contains("sales.first"), "{analyzer_sql}");
        assert!(analyzer_sql.contains("sales.second"), "{analyzer_sql}");
        assert!(!analyzer_sql.contains("ice.sales"), "{analyzer_sql}");
        assert_eq!(analysis.output_columns[0].name, "resolved_name");

        let fallback_events = RefCell::new(Vec::new());
        let mut fallback_resolved = analyze_literal_query("SELECT 1 AS projection_name");
        fallback_resolved.output_columns.clear();
        let fallback = analyze_mv_select_with(
            &query,
            Some("ice"),
            "sales",
            |_: &[ResolvedTableRef]| {
                fallback_events.borrow_mut().push("register");
                Ok(())
            },
            |_: &sqlparser::ast::Query| {
                fallback_events.borrow_mut().push("analyze");
                Ok(fallback_resolved)
            },
        )
        .expect("analyze fallback through shared orchestration");

        assert_eq!(&*fallback_events.borrow(), &["register", "analyze"]);
        assert_eq!(fallback.output_columns[0].name, "projection_name");
    }
}
