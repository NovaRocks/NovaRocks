use crate::sql::analyzer::iceberg_metadata::split_metadata_suffix;

/// Extract table names from a query AST, using the last object-name part and
/// ignoring catalog/database qualifiers.
pub(crate) fn extract_table_names_from_query(query: &sqlparser::ast::Query) -> Vec<String> {
    let mut names = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_table_names_from_subquery(&cte.query, &mut names);
        }
    }
    extract_table_names_from_set_expr(query.body.as_ref(), &mut names);
    names.sort();
    names.dedup();
    names
}

fn extract_table_names_from_set_expr(expr: &sqlparser::ast::SetExpr, names: &mut Vec<String>) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                extract_table_names_from_table_factor(&from.relation, names);
                for join in &from.joins {
                    extract_table_names_from_table_factor(&join.relation, names);
                }
            }
            extract_table_names_from_expr_opt(select.selection.as_ref(), names);
            extract_table_names_from_expr_opt(select.having.as_ref(), names);
            for projection in &select.projection {
                match projection {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                        extract_table_names_from_expr(expr, names);
                    }
                    _ => {}
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            extract_table_names_from_set_expr(left, names);
            extract_table_names_from_set_expr(right, names);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            extract_table_names_from_set_expr(query.body.as_ref(), names);
        }
        _ => {}
    }
}

fn extract_table_names_from_table_factor(
    factor: &sqlparser::ast::TableFactor,
    names: &mut Vec<String>,
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
            // Skip synthetic iceberg-metadata factors; they are handled by
            // extract_three_part_refs (which strips the trailing
            // __nr_meta_*__ suffix and routes to the iceberg backend).
            let (_, metadata_suffix) = split_metadata_suffix(&parts);
            if metadata_suffix.is_some() {
                return;
            }
            if let Some(last) = parts.last() {
                names.push(last.clone());
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            extract_table_names_from_set_expr(subquery.body.as_ref(), names);
        }
        _ => {}
    }
}

fn extract_table_names_from_expr_opt(expr: Option<&sqlparser::ast::Expr>, names: &mut Vec<String>) {
    if let Some(expr) = expr {
        extract_table_names_from_expr(expr, names);
    }
}

fn extract_table_names_from_expr(expr: &sqlparser::ast::Expr, names: &mut Vec<String>) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => {
            extract_table_names_from_subquery(query, names);
        }
        Expr::InSubquery { subquery, expr, .. } => {
            extract_table_names_from_subquery(subquery, names);
            extract_table_names_from_expr(expr, names);
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_table_names_from_expr(left, names);
            extract_table_names_from_expr(right, names);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            extract_table_names_from_expr(expr, names);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            extract_table_names_from_expr(expr, names);
            extract_table_names_from_expr(low, names);
            extract_table_names_from_expr(high, names);
        }
        _ => {}
    }
}

fn extract_table_names_from_subquery(query: &sqlparser::ast::Query, names: &mut Vec<String>) {
    extract_table_names_from_set_expr(query.body.as_ref(), names);
}

/// Extract `(catalog, database, table)` triples from 3-part table references
/// in a query AST.
pub(crate) fn extract_three_part_table_refs(
    query: &sqlparser::ast::Query,
) -> Vec<(String, String, String)> {
    let mut refs = extract_three_part_table_ref_occurrences(query);
    refs.sort();
    refs.dedup();
    refs
}

/// Extract every `(catalog, database, table)` occurrence from 3-part table
/// references in a query AST. Unlike `extract_three_part_table_refs`, this
/// preserves duplicates so callers can enforce cardinality before rewrites.
pub(crate) fn extract_three_part_table_ref_occurrences(
    query: &sqlparser::ast::Query,
) -> Vec<(String, String, String)> {
    let mut refs = Vec::new();
    extract_three_part_refs_from_query(query, &mut refs);
    refs
}

fn extract_three_part_refs_from_query(
    query: &sqlparser::ast::Query,
    refs: &mut Vec<(String, String, String)>,
) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_three_part_refs_from_query(&cte.query, refs);
        }
    }
    extract_three_part_refs_from_set_expr(query.body.as_ref(), refs);
}

fn extract_three_part_refs_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    refs: &mut Vec<(String, String, String)>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                extract_three_part_refs_from_factor(&from.relation, refs);
                for join in &from.joins {
                    extract_three_part_refs_from_factor(&join.relation, refs);
                }
            }
            if let Some(selection) = &select.selection {
                extract_three_part_refs_from_expr(selection, refs);
            }
            if let Some(having) = &select.having {
                extract_three_part_refs_from_expr(having, refs);
            }
            for projection in &select.projection {
                match projection {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                        extract_three_part_refs_from_expr(expr, refs);
                    }
                    _ => {}
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            extract_three_part_refs_from_set_expr(left, refs);
            extract_three_part_refs_from_set_expr(right, refs);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            extract_three_part_refs_from_query(query, refs);
        }
        _ => {}
    }
}

fn extract_three_part_refs_from_expr(
    expr: &sqlparser::ast::Expr,
    refs: &mut Vec<(String, String, String)>,
) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => {
            extract_three_part_refs_from_query(query, refs);
        }
        Expr::InSubquery { subquery, expr, .. } => {
            extract_three_part_refs_from_query(subquery, refs);
            extract_three_part_refs_from_expr(expr, refs);
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_three_part_refs_from_expr(left, refs);
            extract_three_part_refs_from_expr(right, refs);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            extract_three_part_refs_from_expr(expr, refs);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            extract_three_part_refs_from_expr(expr, refs);
            extract_three_part_refs_from_expr(low, refs);
            extract_three_part_refs_from_expr(high, refs);
        }
        Expr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &function.args {
                for arg in &arg_list.args {
                    let inner = match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => Some(e),
                        sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                            ..
                        } => Some(e),
                        sqlparser::ast::FunctionArg::ExprNamed {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                            ..
                        } => Some(e),
                        _ => None,
                    };
                    if let Some(e) = inner {
                        extract_three_part_refs_from_expr(e, refs);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                extract_three_part_refs_from_expr(op, refs);
            }
            for case_when in conditions {
                extract_three_part_refs_from_expr(&case_when.condition, refs);
                extract_three_part_refs_from_expr(&case_when.result, refs);
            }
            if let Some(else_expr) = else_result {
                extract_three_part_refs_from_expr(else_expr, refs);
            }
        }
        Expr::Cast { expr, .. } => {
            extract_three_part_refs_from_expr(expr, refs);
        }
        _ => {}
    }
}

fn extract_three_part_refs_from_factor(
    factor: &sqlparser::ast::TableFactor,
    refs: &mut Vec<(String, String, String)>,
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
            // Strip a trailing `__nr_meta_*__` suffix so a 4-part metadata-table
            // reference (cat.db.tbl.__nr_meta_*__) is treated as a fully-qualified
            // 3-part base reference for registration.
            let (base_parts, _) = split_metadata_suffix(&parts);
            if base_parts.len() == 3 {
                refs.push((
                    base_parts[0].clone(),
                    base_parts[1].clone(),
                    base_parts[2].clone(),
                ));
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            extract_three_part_refs_from_query(subquery, refs);
        }
        _ => {}
    }
}

/// Rewrite a query AST in-place: convert all 3-part table references
/// `catalog.database.table` to 2-part `database.table` by stripping the
/// leading catalog element.
pub(crate) fn strip_catalog_from_three_part_names(query: &mut sqlparser::ast::Query) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            strip_catalog_in_set_expr(cte.query.body.as_mut());
        }
    }
    strip_catalog_in_set_expr(query.body.as_mut());
}

fn strip_catalog_in_set_expr(expr: &mut sqlparser::ast::SetExpr) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &mut select.from {
                strip_catalog_in_factor(&mut from.relation);
                for join in &mut from.joins {
                    strip_catalog_in_factor(&mut join.relation);
                }
            }
            if let Some(selection) = &mut select.selection {
                strip_catalog_in_expr(selection);
            }
            if let Some(having) = &mut select.having {
                strip_catalog_in_expr(having);
            }
            for projection in &mut select.projection {
                match projection {
                    sqlparser::ast::SelectItem::UnnamedExpr(expr)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                        strip_catalog_in_expr(expr);
                    }
                    _ => {}
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            strip_catalog_in_set_expr(left.as_mut());
            strip_catalog_in_set_expr(right.as_mut());
        }
        sqlparser::ast::SetExpr::Query(query) => {
            strip_catalog_in_set_expr(query.body.as_mut());
        }
        _ => {}
    }
}

fn strip_catalog_in_factor(factor: &mut sqlparser::ast::TableFactor) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            // Count parts logically (excluding any trailing __nr_meta_*__).
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
            let (base_parts, _) = split_metadata_suffix(&parts);
            if base_parts.len() == 3 {
                // Drop the leading catalog identifier; keep the remaining
                // 2-part base name plus the (preserved) metadata suffix part.
                name.0.remove(0);
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            strip_catalog_in_set_expr(subquery.body.as_mut());
        }
        _ => {}
    }
}

fn strip_catalog_in_expr(expr: &mut sqlparser::ast::Expr) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => {
            strip_catalog_in_set_expr(query.body.as_mut());
        }
        Expr::InSubquery { subquery, expr, .. } => {
            strip_catalog_in_set_expr(subquery.body.as_mut());
            strip_catalog_in_expr(expr);
        }
        Expr::BinaryOp { left, right, .. } => {
            strip_catalog_in_expr(left);
            strip_catalog_in_expr(right);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            strip_catalog_in_expr(expr);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            strip_catalog_in_expr(expr);
            strip_catalog_in_expr(low);
            strip_catalog_in_expr(high);
        }
        Expr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &mut function.args {
                for arg in &mut arg_list.args {
                    let inner = match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => Some(e),
                        sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                            ..
                        } => Some(e),
                        sqlparser::ast::FunctionArg::ExprNamed {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(e),
                            ..
                        } => Some(e),
                        _ => None,
                    };
                    if let Some(e) = inner {
                        strip_catalog_in_expr(e);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                strip_catalog_in_expr(op);
            }
            for case_when in conditions {
                strip_catalog_in_expr(&mut case_when.condition);
                strip_catalog_in_expr(&mut case_when.result);
            }
            if let Some(else_expr) = else_result {
                strip_catalog_in_expr(else_expr);
            }
        }
        Expr::Cast { expr, .. } => {
            strip_catalog_in_expr(expr);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::{parse_sql_raw, query_refs};

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let stmt = parse_sql_raw(sql).expect("parse sql");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        *query
    }

    #[test]
    fn extracts_three_part_table_refs_from_joins_and_subqueries() {
        let query = parse_query("SELECT * FROM c1.db1.t1 JOIN (SELECT * FROM c2.db2.t2) d ON true");

        assert_eq!(
            query_refs::extract_three_part_table_refs(&query),
            vec![
                ("c1".to_string(), "db1".to_string(), "t1".to_string()),
                ("c2".to_string(), "db2".to_string(), "t2".to_string()),
            ],
        );
    }

    #[test]
    fn strips_catalog_from_three_part_table_refs() {
        let mut query = parse_query("SELECT * FROM c1.db1.t1 JOIN c2.db2.t2 ON true");

        query_refs::strip_catalog_from_three_part_names(&mut query);

        assert_eq!(
            query.to_string(),
            "SELECT * FROM db1.t1 JOIN db2.t2 ON true"
        );
    }

    #[test]
    fn extracts_table_names_from_ctes_and_having_subqueries() {
        let query = parse_query(
            "WITH x AS (SELECT * FROM seed) \
             SELECT k FROM t1 GROUP BY k HAVING EXISTS (SELECT 1 FROM db2.t2)",
        );

        assert_eq!(
            query_refs::extract_table_names_from_query(&query),
            vec!["seed".to_string(), "t1".to_string(), "t2".to_string()],
        );
    }

    #[test]
    fn extracts_three_part_refs_for_4part_metadata_table_factor() {
        let query = parse_query("SELECT * FROM ice.db.t.__nr_meta_snapshots__");

        assert_eq!(
            query_refs::extract_three_part_table_refs(&query),
            vec![("ice".to_string(), "db".to_string(), "t".to_string())],
        );
    }

    #[test]
    fn ignores_3part_metadata_table_factor_in_three_part_extractor() {
        // base.len() == 2 — not a fully-qualified 3-part ref, must not be emitted.
        let query = parse_query("SELECT * FROM db.t.__nr_meta_history__");

        assert_eq!(
            query_refs::extract_three_part_table_refs(&query),
            Vec::<(String, String, String)>::new(),
        );
    }

    #[test]
    fn extracts_three_part_refs_from_projection_subquery() {
        // Mirrors the iceberg_in_list_predicate failure pattern: outer SELECT has
        // no FROM; the 3-part reference lives inside a COALESCE(SELECT ...) item.
        let query = parse_query("SELECT COALESCE((SELECT count(*) FROM c1.db1.t1), 0) AS a");

        assert_eq!(
            query_refs::extract_three_part_table_refs(&query),
            vec![("c1".to_string(), "db1".to_string(), "t1".to_string())],
        );
    }

    #[test]
    fn extracts_three_part_refs_from_where_in_subquery() {
        let query = parse_query("SELECT 1 FROM dual WHERE x IN (SELECT y FROM c2.db2.t2)");

        assert_eq!(
            query_refs::extract_three_part_table_refs(&query),
            vec![("c2".to_string(), "db2".to_string(), "t2".to_string())],
        );
    }

    #[test]
    fn extracts_three_part_refs_from_exists_subquery() {
        let query = parse_query("SELECT 1 FROM dual WHERE EXISTS (SELECT * FROM c3.db3.t3)");

        assert_eq!(
            query_refs::extract_three_part_table_refs(&query),
            vec![("c3".to_string(), "db3".to_string(), "t3".to_string())],
        );
    }

    #[test]
    fn extracts_three_part_refs_from_having_subquery() {
        let query = parse_query(
            "SELECT k, count(*) FROM dual GROUP BY k \
             HAVING count(*) > (SELECT avg(v) FROM c4.db4.t4)",
        );

        assert_eq!(
            query_refs::extract_three_part_table_refs(&query),
            vec![("c4".to_string(), "db4".to_string(), "t4".to_string())],
        );
    }

    #[test]
    fn extracts_three_part_refs_from_cte_body() {
        let query = parse_query("WITH x AS (SELECT * FROM c5.db5.t5) SELECT * FROM x");

        assert_eq!(
            query_refs::extract_three_part_table_refs(&query),
            vec![("c5".to_string(), "db5".to_string(), "t5".to_string())],
        );
    }

    #[test]
    fn strips_catalog_from_4part_metadata_table_factor() {
        let mut query = parse_query("SELECT * FROM ice.db.t.__nr_meta_snapshots__");

        query_refs::strip_catalog_from_three_part_names(&mut query);

        assert_eq!(
            query.to_string(),
            "SELECT * FROM db.t.__nr_meta_snapshots__"
        );
    }

    #[test]
    fn leaves_3part_metadata_table_factor_alone() {
        // base.len() == 2 — already not catalog-qualified; must not be touched.
        let mut query = parse_query("SELECT * FROM db.t.__nr_meta_history__");

        query_refs::strip_catalog_from_three_part_names(&mut query);

        assert_eq!(query.to_string(), "SELECT * FROM db.t.__nr_meta_history__");
    }

    #[test]
    fn strips_catalog_inside_projection_subquery() {
        let mut query = parse_query("SELECT COALESCE((SELECT count(*) FROM c1.db1.t1), 0) AS a");

        query_refs::strip_catalog_from_three_part_names(&mut query);

        assert_eq!(
            query.to_string(),
            "SELECT COALESCE((SELECT count(*) FROM db1.t1), 0) AS a"
        );
    }

    #[test]
    fn strips_catalog_inside_where_in_subquery() {
        let mut query = parse_query("SELECT 1 FROM dual WHERE x IN (SELECT y FROM c2.db2.t2)");

        query_refs::strip_catalog_from_three_part_names(&mut query);

        assert_eq!(
            query.to_string(),
            "SELECT 1 FROM dual WHERE x IN (SELECT y FROM db2.t2)"
        );
    }

    #[test]
    fn strips_catalog_inside_cte_body() {
        let mut query = parse_query("WITH x AS (SELECT * FROM c5.db5.t5) SELECT * FROM x");

        query_refs::strip_catalog_from_three_part_names(&mut query);

        assert_eq!(
            query.to_string(),
            "WITH x AS (SELECT * FROM db5.t5) SELECT * FROM x"
        );
    }

    #[test]
    fn one_part_extractor_skips_nr_meta_last_part() {
        // 4-part metadata factor must not surface as a 1-part name (the last part
        // is a synthetic suffix, not a real table name). The 3-part extractor
        // handles the real registration.
        let query = parse_query("SELECT * FROM ice.db.t.__nr_meta_snapshots__");

        assert_eq!(
            query_refs::extract_table_names_from_query(&query),
            Vec::<String>::new(),
        );
    }
}
