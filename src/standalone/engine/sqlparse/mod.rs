//! SQL parsing: sqlparser AST → internal Expr/Literal conversion, DDL/DML handlers,
//! materialized-view recognition, generate_series helpers.
//!
//! Populated incrementally during the PR1 refactor.

pub(crate) mod expr;
pub(crate) mod generate_series;
pub(crate) mod statement;
