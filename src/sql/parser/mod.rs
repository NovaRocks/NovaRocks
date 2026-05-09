#![allow(dead_code)]

pub(crate) mod ast;
pub(crate) mod dialect;
pub(crate) mod query_refs;
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

    if dialect::alter_iceberg_ref::looks_like_alter_iceberg_ref(&parser) {
        let stmt = dialect::alter_iceberg_ref::parse_alter_iceberg_ref(&mut parser)?;
        return Ok(vec![stmt]);
    }

    if dialect::truncate::looks_like_truncate_table(&parser) {
        let stmt = dialect::truncate::parse_truncate_table(&mut parser)?;
        return Ok(vec![stmt]);
    }

    Err("parse_sql: only materialized-view DDL is recognized in Phase 1".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sql_raw_parses_for_version_as_of_string() {
        // `FOR VERSION AS OF '<string>'` is normalizer-rewritten to
        // `FOR SYSTEM_TIME AS OF '__nr_ref:<string>'` before parsing so
        // sqlparser (which only allows numerics for VERSION AS OF) can handle it.
        let stmt = parse_sql_raw("SELECT id FROM t FOR VERSION AS OF 'main'")
            .expect("FOR VERSION AS OF string must parse after normalizer rewrite");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let tw = &select.from[0];
        let sqlparser::ast::TableFactor::Table { version, .. } = &tw.relation else {
            panic!("expected table factor");
        };
        assert!(version.is_some(), "version clause must be present");
        // Should have been normalized to ForSystemTimeAsOf('__nr_ref:main').
        match version.as_ref().unwrap() {
            sqlparser::ast::TableVersion::ForSystemTimeAsOf(sqlparser::ast::Expr::Value(v)) => {
                match &v.value {
                    sqlparser::ast::Value::SingleQuotedString(s) => {
                        assert_eq!(
                            s, "__nr_ref:main",
                            "normalizer must produce __nr_ref: prefix"
                        );
                    }
                    other => panic!("expected single-quoted string, got: {other:?}"),
                }
            }
            other => panic!("expected ForSystemTimeAsOf after normalization, got: {other:?}"),
        }
    }

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
    fn parse_sql_raw_normalizes_group_concat_missing_argument_error() {
        let err = parse_sql_raw("SELECT group_concat(  order by score) from ss order by 1")
            .expect_err("should fail");
        assert_eq!(
            err,
            "Unexpected input '(', the most similar input is {<EOF>, ';'}.",
        );
    }

    #[test]
    fn parse_sql_raw_normalizes_group_concat_distinct_missing_argument_error() {
        let err = parse_sql_raw("SELECT group_concat(distinct  order by score) from ss order by 1")
            .expect_err("should fail");
        assert_eq!(
            err,
            "Unexpected input 'order', the most similar input is {a legal identifier}.",
        );
    }

    #[test]
    fn parse_sql_raw_normalizes_group_concat_missing_argument_with_separator_error() {
        let err =
            parse_sql_raw("SELECT group_concat(order by 1 separator '')").expect_err("should fail");
        assert_eq!(
            err,
            "Unexpected input '(', the most similar input is {<EOF>, ';'}.",
        );
    }

    #[test]
    fn parse_sql_raw_normalizes_group_concat_separator_without_argument_error() {
        let err = parse_sql_raw("SELECT group_concat(separator NULL)").expect_err("should fail");
        assert_eq!(
            err,
            "No viable statement for input 'group_concat(separator NULL'.",
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

    #[test]
    fn parse_sql_raw_recognizes_ignore_nulls_inside_window_function_args() {
        // `first_value(v IGNORE NULLS)` puts the null-treatment clause
        // inside the function's argument list. sqlparser exposes that as
        // FunctionArgumentClause::IgnoreOrRespectNulls; this requires
        // StarRocksDialect to opt in via supports_window_function_null_treatment_arg().
        let stmt = parse_sql_raw(
            "SELECT first_value(v IGNORE NULLS) OVER (ORDER BY x) AS w FROM t",
        )
        .expect("IGNORE NULLS inside function args must parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = &select.projection[0] else {
            panic!("expected aliased projection, got: {:?}", select.projection);
        };
        let sqlparser::ast::Expr::Function(func) = expr else {
            panic!("expected function call projection, got: {expr:?}");
        };
        let sqlparser::ast::FunctionArguments::List(args) = &func.args else {
            panic!("expected list arguments, got: {:?}", func.args);
        };
        let found = args.clauses.iter().any(|c| {
            matches!(
                c,
                sqlparser::ast::FunctionArgumentClause::IgnoreOrRespectNulls(
                    sqlparser::ast::NullTreatment::IgnoreNulls,
                ),
            )
        });
        assert!(
            found,
            "expected IGNORE NULLS clause inside function args, got: {:?}",
            args.clauses,
        );
        assert!(func.over.is_some(), "OVER clause must still be parsed");
    }

    #[test]
    fn parse_sql_raw_recognizes_ignore_nulls_between_lead_args() {
        // StarRocks dialect: `LEAD(v IGNORE NULLS, 3)` — IGNORE NULLS sits
        // between the value and the offset, *before* the comma. sqlparser only
        // parses null-treatment after the last arg, so this needs an explicit
        // normalizer rewrite.
        let stmt = parse_sql_raw(
            "SELECT LEAD(v IGNORE NULLS, 3) OVER (ORDER BY x) AS w FROM t",
        )
        .expect("LEAD(v IGNORE NULLS, 3) must parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = &select.projection[0] else {
            panic!("expected aliased projection");
        };
        let sqlparser::ast::Expr::Function(func) = expr else {
            panic!("expected function call");
        };
        let sqlparser::ast::FunctionArguments::List(args) = &func.args else {
            panic!("expected list arguments");
        };
        // Two unnamed args (v, 3) and one IgnoreOrRespectNulls clause.
        assert_eq!(args.args.len(), 2, "expected 2 args, got: {:?}", args.args);
        let found = args.clauses.iter().any(|c| {
            matches!(
                c,
                sqlparser::ast::FunctionArgumentClause::IgnoreOrRespectNulls(
                    sqlparser::ast::NullTreatment::IgnoreNulls,
                ),
            )
        });
        assert!(
            found,
            "expected IGNORE NULLS clause in args.clauses, got: {:?}",
            args.clauses,
        );
    }

    #[test]
    fn parse_sql_raw_recognizes_respect_nulls_inside_window_function_args() {
        let stmt = parse_sql_raw(
            "SELECT lead(v, 1) RESPECT NULLS OVER (ORDER BY x) AS w FROM t",
        )
        .expect("RESPECT NULLS after function call must parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select body");
        };
        let sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = &select.projection[0] else {
            panic!("expected aliased projection, got: {:?}", select.projection);
        };
        let sqlparser::ast::Expr::Function(func) = expr else {
            panic!("expected function call projection, got: {expr:?}");
        };
        // Post-args form lands in Function.null_treatment.
        assert!(
            matches!(
                func.null_treatment,
                Some(sqlparser::ast::NullTreatment::RespectNulls)
            ),
            "expected null_treatment=RespectNulls, got: {:?}",
            func.null_treatment,
        );
        assert!(func.over.is_some(), "OVER clause must still be parsed");
    }
}
