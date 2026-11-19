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

pub mod refresh_property;

use novarocks_parser::ast::{Ident, Query, Relation, SetExpr};
use novarocks_sql::planning::mv::{
    SqlMvOutputColumnFacts, SqlResolvedMvRefreshInput, SqlResolvedMvRefreshInputSource,
    strip_catalog_from_three_part_names,
};
use novarocks_sql::semantic::{ObjectName, TableColumnDef};
use novarocks_types::naming::normalize_identifier;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedTableRef {
    Iceberg {
        catalog: String,
        namespace: String,
        table: String,
    },
    UnsupportedNative {
        display_name: String,
    },
}

#[derive(Clone, Debug)]
pub struct MvAnalysis {
    pub(crate) resolved_refs: Vec<ResolvedTableRef>,
    pub output_columns: Vec<SqlMvOutputColumnFacts>,
    /// Opaque SQL-owned carrier for refresh-property planning. Core retains it
    /// only to hand it back to SQL; it cannot inspect the analyzer tree.
    pub refresh_input: SqlResolvedMvRefreshInput,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMvSelect {
    resolved_refs: Vec<ResolvedTableRef>,
    query_for_analysis: Query,
}

impl PreparedMvSelect {
    pub(crate) fn resolved_refs(&self) -> &[ResolvedTableRef] {
        &self.resolved_refs
    }

    pub(crate) fn query_for_analysis(&self) -> &Query {
        &self.query_for_analysis
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn prepare_mv_select(
    query: &Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<PreparedMvSelect, String> {
    prepare_mv_select_with_catalog_paths(query, current_catalog, current_database, false)
}

/// Prepare a materialized-view query for an analyzer that resolves external
/// tables through the application catalog facade.  Unlike the legacy
/// local-catalog adapter, this retains three-part identities so each relation
/// is materialized under its own request-local connector binding.
pub(crate) fn prepare_mv_select_for_catalog_provider(
    query: &Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<PreparedMvSelect, String> {
    prepare_mv_select_with_catalog_paths(query, current_catalog, current_database, true)
}

fn prepare_mv_select_with_catalog_paths(
    query: &Query,
    current_catalog: Option<&str>,
    current_database: &str,
    retain_catalog_paths: bool,
) -> Result<PreparedMvSelect, String> {
    validate_mv_select_raw_query_clauses(query)?;
    let resolved_refs = collect_table_refs_from_query(query, current_catalog, current_database);
    let mut query_for_analysis = query.clone();
    if !retain_catalog_paths && has_three_part_refs(&resolved_refs) {
        strip_catalog_from_three_part_names(&mut query_for_analysis);
    }
    Ok(PreparedMvSelect {
        resolved_refs,
        query_for_analysis,
    })
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn finish_mv_analysis<T>(
    prepared: PreparedMvSelect,
    source: T,
) -> Result<MvAnalysis, novarocks_sql::compiler::SqlCompileError>
where
    T: SqlResolvedMvRefreshInputSource,
{
    let refresh_input = SqlResolvedMvRefreshInput::from_analysis(source)?;
    let output_columns = refresh_input.analysis_facts().output_columns;
    Ok(MvAnalysis {
        resolved_refs: prepared.resolved_refs,
        output_columns,
        refresh_input,
    })
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn analyze_mv_select_with<Register, Analyze, T>(
    query: &Query,
    current_catalog: Option<&str>,
    current_database: &str,
    register: Register,
    analyze: Analyze,
) -> Result<MvAnalysis, novarocks_sql::compiler::SqlCompileError>
where
    Register: FnOnce(&[ResolvedTableRef]) -> Result<(), String>,
    Analyze: FnOnce(&Query) -> Result<T, String>,
    T: SqlResolvedMvRefreshInputSource,
{
    let prepared = prepare_mv_select(query, current_catalog, current_database)?;
    register(prepared.resolved_refs())?;
    let source = analyze(prepared.query_for_analysis())?;
    finish_mv_analysis(prepared, source)
}

fn validate_mv_select_raw_query_clauses(query: &Query) -> Result<(), String> {
    if !query.functions.is_empty() {
        return Err(unsupported_mv_query_clause("inline functions"));
    }
    if query.with.is_some() {
        return Err(unsupported_mv_query_clause("WITH"));
    }
    validate_mv_query_modifiers(&query.modifiers)?;
    validate_mv_select_raw_clauses_in_set_expr(query.body.as_ref())
}

fn validate_mv_query_modifiers(
    modifiers: &novarocks_parser::ast::QueryModifiers,
) -> Result<(), String> {
    if modifiers.order_by.is_some() {
        return Err(unsupported_mv_query_clause("ORDER BY"));
    }
    if matches!(
        &modifiers.limit,
        Some(novarocks_parser::ast::QueryLimit::Limit(_))
    ) || modifiers.offset.is_some()
    {
        return Err(unsupported_mv_query_clause("LIMIT or OFFSET"));
    }
    if matches!(
        &modifiers.limit,
        Some(novarocks_parser::ast::QueryLimit::FetchFirst(_))
    ) {
        return Err(unsupported_mv_query_clause("FETCH"));
    }
    Ok(())
}

fn validate_mv_select_raw_clauses_in_set_expr(expr: &SetExpr) -> Result<(), String> {
    match expr {
        SetExpr::Select(select) => {
            validate_mv_select_raw_select_clauses(select)?;
            if let Some(from) = &select.from {
                validate_mv_select_raw_clauses_in_factor(from)?;
            }
            Ok(())
        }
        SetExpr::SetOperation(operation) => {
            for input in &operation.inputs {
                validate_mv_select_raw_clauses_in_set_expr(input)?;
            }
            Ok(())
        }
        SetExpr::Query(subquery) => {
            let query = &subquery.query;
            validate_mv_select_raw_query_clauses(query)
        }
        SetExpr::Values(_) => Ok(()),
        SetExpr::Table(_) => Err(unsupported_mv_from_clause("TABLE query bodies")),
    }
}

fn validate_mv_select_raw_select_clauses(
    select: &novarocks_parser::ast::Select,
) -> Result<(), String> {
    validate_mv_query_modifiers(&select.modifiers)?;
    if !select.windows.is_empty() {
        return Err(unsupported_mv_select_clause("named WINDOW clauses"));
    }
    Ok(())
}

fn validate_mv_select_raw_clauses_in_factor(factor: &Relation) -> Result<(), String> {
    match factor {
        Relation::Table(table) => {
            if table.query_period.is_some() || table.branch.is_some() {
                return Err(unsupported_mv_from_clause("table version qualifiers"));
            }
            Ok(())
        }
        Relation::Aliased(alias) => validate_mv_select_raw_clauses_in_factor(&alias.relation),
        Relation::Join(join) => {
            validate_mv_select_raw_clauses_in_factor(&join.left)?;
            validate_mv_select_raw_clauses_in_factor(&join.right)
        }
        Relation::TableSubquery(node) => validate_mv_select_raw_query_clauses(&node.query),
        Relation::Lateral(_) => Err(unsupported_mv_from_clause("LATERAL derived tables")),
        Relation::JsonTable(_) => Err(unsupported_mv_from_clause("JSON_TABLE")),
        Relation::TableFunction(_) | Relation::Unnest(_) => {
            Err(unsupported_mv_from_clause("table functions"))
        }
        _ => Err(unsupported_mv_from_clause("relation modifiers")),
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

fn synthetic_ident(value: impl Into<String>, span: novarocks_parser::Span) -> Ident {
    Ident {
        value: value.into(),
        quoted: false,
        span,
    }
}

pub fn canonicalize_iceberg_mv_select_query(
    query: &Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Query {
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

fn qualify_current_catalog_refs_in_query(query: &mut Query, catalog: &str, current_database: &str) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.ctes {
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
    expr: &mut SetExpr,
    catalog: &str,
    current_database: &str,
) {
    match expr {
        SetExpr::Select(select) => {
            if let Some(from) = &mut select.from {
                qualify_current_catalog_refs_in_factor(from, catalog, current_database);
            }
        }
        SetExpr::SetOperation(operation) => {
            for input in &mut operation.inputs {
                qualify_current_catalog_refs_in_set_expr(input, catalog, current_database);
            }
        }
        SetExpr::Query(subquery) => {
            let query = &mut subquery.query;
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
    factor: &mut Relation,
    catalog: &str,
    current_database: &str,
) {
    match factor {
        Relation::Table(table) => {
            let name = &mut table.name;
            let parts = name
                .parts
                .iter()
                .map(|ident| ident.value.to_ascii_lowercase())
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
                name.parts = vec![
                    synthetic_ident(catalog, name.span),
                    synthetic_ident(namespace, name.span),
                    synthetic_ident(table, name.span),
                ];
            }
        }
        Relation::TableSubquery(node) => {
            let subquery = &mut node.query;
            qualify_current_catalog_refs_in_set_expr(
                subquery.body.as_mut(),
                catalog,
                current_database,
            );
        }
        Relation::Lateral(node) => qualify_current_catalog_refs_in_set_expr(
            node.query.body.as_mut(),
            catalog,
            current_database,
        ),
        Relation::Aliased(alias) => {
            qualify_current_catalog_refs_in_factor(&mut alias.relation, catalog, current_database)
        }
        Relation::Join(join) => {
            qualify_current_catalog_refs_in_factor(&mut join.left, catalog, current_database);
            qualify_current_catalog_refs_in_factor(&mut join.right, catalog, current_database);
        }
        _ => {}
    }
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

fn collect_table_refs_from_query(
    query: &Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Vec<ResolvedTableRef> {
    let mut refs = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.ctes {
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
    expr: &SetExpr,
    current_catalog: Option<&str>,
    current_database: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match expr {
        SetExpr::Select(select) => {
            if let Some(from) = &select.from {
                collect_table_refs_from_factor(from, current_catalog, current_database, refs);
            }
        }
        SetExpr::SetOperation(operation) => {
            for input in &operation.inputs {
                collect_table_refs_from_set_expr(input, current_catalog, current_database, refs);
            }
        }
        SetExpr::Query(subquery) => {
            let query = &subquery.query;
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
    factor: &Relation,
    current_catalog: Option<&str>,
    current_database: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match factor {
        Relation::Table(table) => {
            let name = &table.name;
            let parts: Vec<String> = novarocks_sql::TableNameView::new(name)
                .parts()
                .map(str::to_ascii_lowercase)
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
                    None => ResolvedTableRef::UnsupportedNative {
                        display_name: format!(
                            "{}.{}",
                            current_database.to_ascii_lowercase(),
                            table
                        ),
                    },
                },
                [database, table] => match current_catalog {
                    Some(catalog) => ResolvedTableRef::Iceberg {
                        catalog: catalog.to_ascii_lowercase(),
                        namespace: database.clone(),
                        table: table.clone(),
                    },
                    None => ResolvedTableRef::UnsupportedNative {
                        display_name: format!("{database}.{table}"),
                    },
                },
                _ => ResolvedTableRef::UnsupportedNative {
                    display_name: parts.join("."),
                },
            };
            if !refs.contains(&resolved) {
                refs.push(resolved);
            }
        }
        Relation::TableSubquery(node) => {
            let subquery = &node.query;
            if let Some(with) = &subquery.with {
                for cte in &with.ctes {
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
        Relation::Lateral(node) => {
            for item in
                collect_table_refs_from_query(&node.query, current_catalog, current_database)
            {
                if !refs.contains(&item) {
                    refs.push(item);
                }
            }
        }
        Relation::Aliased(alias) => {
            collect_table_refs_from_factor(&alias.relation, current_catalog, current_database, refs)
        }
        Relation::Join(join) => {
            collect_table_refs_from_factor(&join.left, current_catalog, current_database, refs);
            collect_table_refs_from_factor(&join.right, current_catalog, current_database, refs);
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
    column: &SqlMvOutputColumnFacts,
) -> Result<TableColumnDef, String> {
    Ok(TableColumnDef {
        name: column.name.clone(),
        data_type: novarocks_types::sql_type_from_value_type(&column.value_type)
            .map_err(|error| error.to_string())?,
        nullable: column.value_type.nullable(),
        default: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::printer::print_query;

    fn parse_query(sql: &str) -> Query {
        novarocks_parser::parser::parse_query(sql).expect("parse query")
    }

    fn render_query(query: &Query) -> String {
        print_query(query).expect("print fixture query")
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
        let analysis_sql = render_query(prepared.query_for_analysis()).to_ascii_lowercase();
        assert!(analysis_sql.contains("sales.first"), "{analysis_sql}");
        assert!(analysis_sql.contains("sales.second"), "{analysis_sql}");
        assert!(!analysis_sql.contains("ice.sales"), "{analysis_sql}");
    }

    #[test]
    fn sqlx2_mv_catalog_provider_prep_keeps_three_part_identities() {
        let query = parse_query(
            "SELECT a.id, b.id FROM Ice.Sales.First a JOIN Other.Sales.Second b ON a.id = b.id",
        );

        let prepared = prepare_mv_select_for_catalog_provider(&query, Some("ice"), "sales")
            .expect("prepare query");
        let analysis_sql = render_query(prepared.query_for_analysis()).to_ascii_lowercase();

        assert!(analysis_sql.contains("ice.sales.first"), "{analysis_sql}");
        assert!(
            analysis_sql.contains("other.sales.second"),
            "{analysis_sql}"
        );
    }

    #[test]
    fn resolve_mv_name_accepts_supported_forms_and_rejects_non_default_catalog() {
        assert_eq!(
            resolve_mv_name(
                &novarocks_sql::semantic::ObjectName {
                    parts: vec!["Orders".to_string()],
                },
                "Sales",
            ),
            Ok(("sales".to_string(), "orders".to_string()))
        );
        assert_eq!(
            resolve_mv_name(
                &novarocks_sql::semantic::ObjectName {
                    parts: vec!["Marketing".to_string(), "Orders".to_string()],
                },
                "Sales",
            ),
            Ok(("marketing".to_string(), "orders".to_string()))
        );
        assert_eq!(
            resolve_mv_name(
                &novarocks_sql::semantic::ObjectName {
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
                &novarocks_sql::semantic::ObjectName {
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

        let canonical = render_query(&canonicalize_iceberg_mv_select_query(
            &query,
            Some("ICE"),
            "Sales",
        ));
        let canonical = canonical.to_ascii_lowercase();

        assert!(canonical.contains("ice.sales.orders"), "{canonical}");
        assert!(canonical.contains("ice.marketing.customers"), "{canonical}");
    }
}
