#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IncrementalMvShape {
    pub(crate) base_table: sqlparser::ast::ObjectName,
}

pub(crate) fn classify_incremental_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    reject_unsupported_query_clauses(query)?;

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(projection_filter_error());
    };
    reject_unsupported_select_clauses(select)?;

    let [from] = select.from.as_slice() else {
        return Err(single_base_table_error());
    };
    if !from.joins.is_empty() {
        return Err(single_base_table_error());
    }

    let sqlparser::ast::TableFactor::Table {
        name,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &from.relation
    else {
        return Err(projection_filter_error());
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(single_base_table_error());
    }
    if !is_three_part_object_name(name) {
        return Err(single_base_table_error());
    }

    reject_unsupported_projection_filter_exprs(select)?;

    Ok(IncrementalMvShape {
        base_table: name.clone(),
    })
}

fn reject_unsupported_query_clauses(query: &sqlparser::ast::Query) -> Result<(), String> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn reject_unsupported_select_clauses(select: &sqlparser::ast::Select) -> Result<(), String> {
    if select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !is_empty_group_by(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(projection_filter_error());
    }
    Ok(())
}

fn reject_unsupported_projection_filter_exprs(
    select: &sqlparser::ast::Select,
) -> Result<(), String> {
    let mut rendered_exprs: Vec<String> = select
        .projection
        .iter()
        .map(|item| item.to_string().to_ascii_lowercase())
        .collect();
    if let Some(selection) = &select.selection {
        rendered_exprs.push(selection.to_string().to_ascii_lowercase());
    }

    for expr in rendered_exprs {
        if contains_non_deterministic_function(&expr) {
            return Err(
                "incremental MV projection/filter query contains non-deterministic function"
                    .to_string(),
            );
        }
        if contains_unsupported_projection_filter_expr(&expr) {
            return Err(projection_filter_error());
        }
    }
    Ok(())
}

fn contains_non_deterministic_function(expr: &str) -> bool {
    contains_function_call(expr, "now")
        || contains_function_call(expr, "current_timestamp")
        || contains_query_keyword(expr, "current_timestamp")
        || contains_function_call(expr, "random")
        || contains_function_call(expr, "rand")
        || contains_function_call(expr, "uuid")
}

fn contains_unsupported_projection_filter_expr(expr: &str) -> bool {
    contains_query_keyword(expr, "select")
        || contains_query_keyword(expr, "over")
        || ["sum", "count", "avg", "min", "max"]
            .iter()
            .any(|name| contains_function_call(expr, name))
}

fn contains_function_call(expr: &str, name: &str) -> bool {
    let mut rest = expr;
    while let Some(pos) = rest.find(name) {
        let before = rest[..pos].chars().next_back();
        let after = rest[pos + name.len()..].chars().next();
        let has_boundary_before = before.is_none_or(|ch| !is_identifier_char(ch));
        let has_call_after = after.is_some_and(|ch| ch == '(' || !is_identifier_char(ch));
        if has_boundary_before && has_call_after {
            return true;
        }
        rest = &rest[pos + name.len()..];
    }
    false
}

fn contains_query_keyword(expr: &str, keyword: &str) -> bool {
    let mut rest = expr;
    while let Some(pos) = rest.find(keyword) {
        let before = rest[..pos].chars().next_back();
        let after = rest[pos + keyword.len()..].chars().next();
        if before.is_none_or(|ch| !is_identifier_char(ch))
            && after.is_none_or(|ch| !is_identifier_char(ch))
        {
            return true;
        }
        rest = &rest[pos + keyword.len()..];
    }
    false
}

fn is_identifier_char(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn is_empty_group_by(group_by: &sqlparser::ast::GroupByExpr) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, modifiers) => {
            exprs.is_empty() && modifiers.is_empty()
        }
        sqlparser::ast::GroupByExpr::All(modifiers) => modifiers.is_empty(),
    }
}

fn is_three_part_object_name(name: &sqlparser::ast::ObjectName) -> bool {
    name.0.len() == 3
        && name
            .0
            .iter()
            .all(|part| matches!(part, sqlparser::ast::ObjectNamePart::Identifier(_)))
}

fn single_base_table_error() -> String {
    "incremental MV query must reference a single Iceberg base table".to_string()
}

fn projection_filter_error() -> String {
    "incremental MV query must be a projection/filter SELECT".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("not a query: {stmt:?}");
        };
        *query
    }

    fn classify_sql(sql: &str) -> Result<IncrementalMvShape, String> {
        let query = parse_query(sql);
        classify_incremental_mv_query(&query)
    }

    fn assert_rejects_with(sql: &str, needle: &str) {
        let err = classify_sql(sql).expect_err("query should be rejected");
        assert!(
            err.contains(needle),
            "expected error to contain `{needle}`, got `{err}`"
        );
    }

    #[test]
    fn accepts_single_table_projection_filter() {
        let shape = classify_sql("select k1, v2 + 1 as v3 from ice.ns.orders where v2 > 10")
            .expect("query should be accepted");
        assert_eq!(shape.base_table.to_string(), "ice.ns.orders");
    }

    #[test]
    fn rejects_multi_table_join() {
        assert_rejects_with(
            "select o.k1 from ice.ns.orders o join ice.ns.items i on o.k1 = i.k1",
            "single Iceberg base table",
        );
    }

    #[test]
    fn rejects_aggregation() {
        assert_rejects_with(
            "select k1, sum(v2) from ice.ns.orders group by k1",
            "projection/filter",
        );
    }

    #[test]
    fn rejects_distinct_window_limit_and_subquery() {
        assert_rejects_with("select distinct k1 from ice.ns.orders", "projection/filter");
        assert_rejects_with(
            "select k1, row_number() over (partition by k1) from ice.ns.orders",
            "projection/filter",
        );
        assert_rejects_with("select k1 from ice.ns.orders limit 1", "projection/filter");
        assert_rejects_with(
            "select k1 from (select k1 from ice.ns.orders) t",
            "projection/filter",
        );
    }

    #[test]
    fn rejects_non_deterministic_now() {
        assert_rejects_with("select k1, now() from ice.ns.orders", "non-deterministic");
        assert_rejects_with(
            "select k1, current_timestamp from ice.ns.orders",
            "non-deterministic",
        );
    }
}
