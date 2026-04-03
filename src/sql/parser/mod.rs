#![allow(dead_code)]

mod raw;
pub(crate) mod dialect;
pub(crate) mod ast;

/// Parse SQL into a raw sqlparser AST (no custom AST conversion).
/// Used by the standalone ThriftPlanBuilder.
pub(crate) fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    raw::parse_sql_raw(sql)
}
