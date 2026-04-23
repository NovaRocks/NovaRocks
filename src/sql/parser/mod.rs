#![allow(dead_code)]

pub(crate) mod ast;
pub(crate) mod dialect;
mod raw;

use sqlparser::parser::Parser;

use crate::sql::parser::ast::Statement;
use crate::sql::parser::dialect::StarRocksDialect;

/// Parse SQL into a raw sqlparser AST (no custom AST conversion).
/// Used by the standalone ThriftPlanBuilder.
pub(crate) fn parse_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    raw::parse_sql_raw(sql)
}

pub(crate) fn parse_normalized_sql_raw(sql: &str) -> Result<sqlparser::ast::Statement, String> {
    raw::parse_normalized_sql_raw(sql)
}

/// Parse SQL through the custom StarRocks dialect into a `Vec<Statement>`.
///
/// Phase 1 only recognizes materialized-view DDL (CREATE/DROP/REFRESH/SHOW
/// MATERIALIZED VIEW[S]). All other statements return an explicit error so
/// callers know to fall back to `parse_sql_raw` for the legacy path.
pub(crate) fn parse_sql(sql: &str) -> Result<Vec<Statement>, String> {
    let normalized = dialect::normalize_for_raw_parse(sql)?;
    let sr_dialect = StarRocksDialect;
    let mut parser = Parser::new(&sr_dialect)
        .try_with_sql(&normalized)
        .map_err(|e| e.to_string())?;

    // MV probes MUST come BEFORE any generic CREATE TABLE / DROP TABLE /
    // SHOW TABLES / REFRESH dispatch we may add later: the `MATERIALIZED`
    // token is what distinguishes these from their plain-table counterparts,
    // and the generic paths would happily swallow `CREATE MATERIALIZED VIEW`
    // as a failed `CREATE TABLE`. Keep these four probes first.
    if dialect::materialized_view::looks_like_create_materialized_view(&parser) {
        let stmt = dialect::materialized_view::parse_create_materialized_view(&mut parser)?;
        return Ok(vec![stmt]);
    }
    if dialect::materialized_view::looks_like_drop_materialized_view(&parser) {
        let stmt = dialect::materialized_view::parse_drop_materialized_view(&mut parser)?;
        return Ok(vec![stmt]);
    }
    if dialect::materialized_view::looks_like_refresh_materialized_view(&parser) {
        let stmt = dialect::materialized_view::parse_refresh_materialized_view(&mut parser)?;
        return Ok(vec![stmt]);
    }
    if dialect::materialized_view::looks_like_show_materialized_views(&parser) {
        let stmt = dialect::materialized_view::parse_show_materialized_views(&mut parser)?;
        return Ok(vec![stmt]);
    }

    Err("parse_sql: only materialized-view DDL is recognized in Phase 1".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sql_raw_rewrites_typed_array_literals() {
        let stmt = parse_sql_raw("SELECT array<double>[0.25, 0.5]").expect("parse should succeed");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let sqlparser::ast::SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
            panic!("expected unnamed projection");
        };
        let sqlparser::ast::Expr::Cast {
            expr: inner,
            data_type,
            ..
        } = expr
        else {
            panic!("expected CAST wrapper, got {expr:?}");
        };
        let sqlparser::ast::Expr::Array(array) = inner.as_ref() else {
            panic!("expected array literal, got {inner:?}");
        };
        assert_eq!(array.elem.len(), 2);
        assert!(matches!(
            data_type,
            sqlparser::ast::DataType::Array(sqlparser::ast::ArrayElemTypeDef::AngleBracket(inner))
                if matches!(inner.as_ref(), sqlparser::ast::DataType::Double(_) | sqlparser::ast::DataType::DoublePrecision)
        ));
    }

    #[test]
    fn parse_sql_raw_normalizes_array_agg_separator_error() {
        let err =
            parse_sql_raw(r#"SELECT array_agg("中国" order by 2, id separator NULL) from ss"#)
                .expect_err("malformed array_agg should fail");
        assert_eq!(
            err,
            "Unexpected input 'separator', the most similar input is {',', ')'}.",
        );
    }

    #[test]
    fn parse_sql_raw_normalizes_array_agg_missing_argument_error() {
        let err =
            parse_sql_raw("SELECT array_agg(order by 1 separator '')").expect_err("should fail");
        assert_eq!(
            err,
            "Unexpected input '(', the most similar input is {<EOF>, ';'}.",
        );
    }

    #[test]
    fn parse_sql_raw_normalizes_array_agg_distinct_missing_argument_error() {
        let err = parse_sql_raw("SELECT array_agg(distinct  order by score) from ss order by 1")
            .expect_err("should fail");
        assert_eq!(
            err,
            "Unexpected input 'order', the most similar input is {a legal identifier}.",
        );
    }

    #[test]
    fn parse_sql_raw_parses_array_sortby_lambda_argument_shape() {
        let stmt =
            parse_sql_raw("SELECT array_sortby((x) -> x.item, x)").expect("parse should succeed");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(func)) =
            &select.projection[0]
        else {
            panic!("expected function call projection");
        };
        let sqlparser::ast::FunctionArguments::List(args) = &func.args else {
            panic!("expected list arguments");
        };
        let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(first_arg)) =
            &args.args[0]
        else {
            panic!("expected first function argument expr");
        };
        assert!(
            matches!(
                first_arg,
                sqlparser::ast::Expr::BinaryOp {
                    left,
                    op: sqlparser::ast::BinaryOperator::Arrow,
                    right,
                } if matches!(
                    left.as_ref(),
                    sqlparser::ast::Expr::Nested(inner)
                        if matches!(
                            inner.as_ref(),
                            sqlparser::ast::Expr::Identifier(ident) if ident.value == "x"
                        )
                ) && matches!(
                    right.as_ref(),
                    sqlparser::ast::Expr::CompoundIdentifier(parts)
                        if parts.len() == 2
                            && parts[0].value == "x"
                            && parts[1].value == "item"
                )
            ),
            "unexpected lambda arg shape: {first_arg:?}"
        );
    }

    #[test]
    fn parse_sql_raw_parses_cast_null_as_map_type() {
        let stmt = parse_sql_raw("SELECT CAST(NULL AS MAP<INT, INT>)").expect("parse should work");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Cast {
            data_type, ..
        }) = &select.projection[0]
        else {
            panic!("expected cast projection");
        };
        assert!(
            matches!(
                data_type,
                sqlparser::ast::DataType::Map(key_type, value_type)
                    if matches!(key_type.as_ref(), sqlparser::ast::DataType::Int(_))
                        && matches!(value_type.as_ref(), sqlparser::ast::DataType::Int(_))
            ) || matches!(
                data_type,
                sqlparser::ast::DataType::Custom(name, modifiers)
                    if name.to_string().eq_ignore_ascii_case("map")
                        && modifiers.len() == 2
                        && modifiers[0].eq_ignore_ascii_case("int")
                        && modifiers[1].eq_ignore_ascii_case("int")
            )
        );
    }
}
