// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{Expr, SyntaxEq, TrinoStatement};
use novarocks_parser::{SYNTHETIC, parser, printer};

#[test]
fn views_and_managed_storage_roundtrip_without_semantic_admission() {
    for sql in [
        "CREATE VIEW IF NOT EXISTS v COMMENT 'text' SECURITY INVOKER WITH (a = DEFAULT) AS SELECT 1",
        "CREATE OR REPLACE VIEW v SECURITY DEFINER AS VALUES 1",
        "CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS v GRACE PERIOD INTERVAL - '2' DAY WHEN STALE INLINE WITH (x=?) AS SELECT ?",
        "CREATE MATERIALIZED VIEW v WHEN STALE FAIL COMMENT '' AS SELECT 1",
        "DROP VIEW IF EXISTS ice.s.v",
        "DROP MATERIALIZED VIEW IF EXISTS ice.s.v",
        "ALTER VIEW a RENAME TO cat.s.b",
        "ALTER VIEW a REFRESH",
        "ALTER MATERIALIZED VIEW IF EXISTS a RENAME TO cat.s.b",
        "ALTER MATERIALIZED VIEW a SET PROPERTIES x = NULL, y = DEFAULT",
        "ALTER MATERIALIZED VIEW a EXECUTE v",
        "ALTER MATERIALIZED VIEW a EXECUTE v() WHERE 1 = 1",
        "ALTER MATERIALIZED VIEW a EXECUTE v(first => ?, ?) WHERE id = ?",
        "REFRESH MATERIALIZED VIEW cat.s.v",
        "EXPLAIN REFRESH MATERIALIZED VIEW v",
    ] {
        let ast = parser::parse_statement(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(ast.span().end(), sql.len());
        let rendered = printer::print_statement(&ast).unwrap();
        assert!(
            ast.syntax_eq(&parser::parse_statement(&rendered).unwrap()),
            "{sql}: {rendered}"
        );
    }
}

#[test]
fn view_query_span_retains_actual_definition_and_properties_use_real_parameters() {
    let sql = "CREATE MATERIALIZED VIEW a WITH (x = ?) AS /* outside */ WITH t AS (VALUES 1) SELECT /* inside */ ? FROM t /* outside */";
    let ast = parser::parse_statement(sql).unwrap();
    let TrinoStatement::CreateMaterializedView(view) = ast else {
        panic!("materialized view")
    };
    assert_eq!(
        &sql[view.query.span.start()..view.query.span.end()],
        "WITH t AS (VALUES 1) SELECT /* inside */ ? FROM t"
    );
    let Some(Expr::Parameter(property)) = &view.properties[0].value else {
        panic!("property parameter")
    };
    assert_eq!(property.position, 0);
    let ast = parser::parse_statement("REFRESH MATERIALIZED VIEW cat.s.v").unwrap();
    let TrinoStatement::RefreshMaterializedView(refresh) = ast else {
        panic!("materialized view refresh")
    };
    assert_eq!(refresh.table.span, SYNTHETIC);
    assert_eq!(refresh.table.name.parts[0].span.start(), 26);
}

#[test]
fn legacy_view_syntax_and_invalid_clause_order_are_refused() {
    for sql in [
        "CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT 1",
        "CREATE MATERIALIZED VIEW v DISTRIBUTED BY HASH(a) BUCKETS 3 AS SELECT 1",
        "CREATE MATERIALIZED VIEW v GRACE PERIOD INTERVAL '2' AS SELECT 1",
        "CREATE MATERIALIZED VIEW v WHEN STALE ERROR AS SELECT 1",
        "CREATE MATERIALIZED VIEW v COMMENT 'c' WHEN STALE FAIL AS SELECT 1",
        "CREATE VIEW v AS WITH SESSION x = 1 SELECT 1",
        "ALTER MATERIALIZED VIEW IF EXISTS v SET PROPERTIES k=1",
        "ALTER MATERIALIZED VIEW v SET PROPERTIES (x = 1)",
        "REFRESH MATERIALIZED VIEW v FULL",
        "DROP MATERIALIZED VIEW v FORCE",
    ] {
        assert!(parser::parse_statement(sql).is_err(), "{sql}");
    }
}
