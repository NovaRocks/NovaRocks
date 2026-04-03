use crate::sql::parser::dialect::StarRocksDialect;
use sqlparser::parser::Parser;

/// Parse SQL into a raw sqlparser AST without converting to the custom AST.
/// This is used by the standalone ThriftPlanBuilder which works directly
/// with sqlparser types to avoid the limitations of the custom AST.
pub(crate) fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let dialect = StarRocksDialect;
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&normalized)
        .map_err(|e| e.to_string())?;
    let stmt = parser.parse_statement().map_err(|e| e.to_string())?;
    Ok(stmt)
}
