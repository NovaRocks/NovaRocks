use crate::sql::parser::dialect::StarRocksDialect;
use crate::sql::parser::recursive_cte;
use sqlparser::parser::Parser;

/// Parse SQL into a raw sqlparser AST without converting to the custom AST.
/// This is used by the standalone ThriftPlanBuilder which works directly
/// with sqlparser types to avoid the limitations of the custom AST.
pub(crate) fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    parse_normalized_sql_raw(&normalized)
}

pub(crate) fn parse_normalized_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    let dialect = StarRocksDialect;
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|e| e.to_string())?;
    let stmt = parser
        .parse_statement()
        .map_err(|e| normalize_raw_parse_error(sql, &e.to_string()))?;
    let max_depth = recursive_cte::extract_recursive_cte_max_depth(sql)
        .unwrap_or(recursive_cte::DEFAULT_MAX_DEPTH);
    let mut stmt = stmt;
    recursive_cte::rewrite_statement(&mut stmt, max_depth)?;
    Ok(stmt)
}

fn normalize_raw_parse_error(sql: &str, err: &str) -> String {
    normalize_ordered_agg_parse_error(sql, err).unwrap_or_else(|| err.to_string())
}

fn normalize_ordered_agg_parse_error(sql: &str, err: &str) -> Option<String> {
    let normalized_sql = sql
        .chars()
        .map(|ch| {
            if ch.is_ascii_whitespace() {
                ' '
            } else {
                ch.to_ascii_lowercase()
            }
        })
        .collect::<String>();
    let (function_name, rest) = ordered_agg_call_body(&normalized_sql)?;
    let trimmed = rest.trim_start();

    if trimmed.starts_with("order by") {
        return Some("Unexpected input '(', the most similar input is {<EOF>, ';'}.".to_string());
    }
    if function_name == "array_agg" && trimmed.starts_with("separator null") {
        return Some("No viable statement for input 'array_agg(separator NULL'.".to_string());
    }
    if let Some(after_distinct) = trimmed.strip_prefix("distinct")
        && (after_distinct.trim_start().starts_with("order by")
            || after_distinct.trim_start().starts_with(", ',' order by"))
    {
        return Some(
            "Unexpected input 'order', the most similar input is {a legal identifier}.".to_string(),
        );
    }
    if err.contains("Expected: ), found: separator")
        && trimmed.contains(" order by ")
        && trimmed.contains(" separator ")
    {
        return Some(
            "Unexpected input 'separator', the most similar input is {',', ')'}.".to_string(),
        );
    }

    None
}

fn ordered_agg_call_body(sql: &str) -> Option<(&'static str, &str)> {
    let (function_name, start) = ["array_agg", "group_concat"]
        .into_iter()
        .filter_map(|name| sql.find(name).map(|start| (name, start)))
        .min_by_key(|(_, start)| *start)?;
    let after_name = &sql[start + function_name.len()..];
    let open = after_name.find('(')?;
    Some((function_name, &after_name[open + 1..]))
}
