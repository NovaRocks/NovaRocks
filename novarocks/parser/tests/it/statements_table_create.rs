// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{
    Expr, LikePropertiesOption, Literal, LiteralKind, StatementSaveMode, SyntaxEq, TrinoStatement,
    TrinoTableElement,
};
use novarocks_parser::{ParseError, ParserError, parser, printer};

fn roundtrip(source: &str) -> TrinoStatement {
    let original =
        parser::parse_statement(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    let printed =
        printer::print_statement(&original).unwrap_or_else(|error| panic!("{original:?}: {error}"));
    let reparsed =
        parser::parse_statement(&printed).unwrap_or_else(|error| panic!("{printed}: {error}"));
    assert!(original.syntax_eq(&reparsed), "{source}\n{printed}");
    original
}

#[test]
fn create_table_preserves_columns_likes_and_builder_parameter_order() {
    let source = "  CREATE TABLE IF NOT EXISTS cat.s.t (\"A.B\" bigint DEFAULT NULL NOT NULL COMMENT 'col' WITH (x = ?), LIKE cat.s.source EXCLUDING PROPERTIES, b DATE DEFAULT DATE '2024-01-01') COMMENT 'tbl' WITH (y = ?, z = DEFAULT)";
    let TrinoStatement::CreateTable(table) = roundtrip(source) else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.save_mode, StatementSaveMode::Ignore);
    assert_eq!(table.name.parts.len(), 3);
    assert_eq!(table.comment.as_deref(), Some("tbl"));
    assert_eq!(table.span.start(), 2);
    assert_eq!(table.span.end(), source.len());

    let [
        TrinoTableElement::Column(first),
        TrinoTableElement::Like(like),
        TrinoTableElement::Column(last),
    ] = table.elements.as_slice()
    else {
        panic!("expected column, LIKE, column");
    };
    assert_eq!(first.name.parts[0].value, "A.B");
    assert!(first.name.parts[0].quoted);
    assert!(!first.nullable);
    assert!(last.nullable);
    assert_eq!(first.comment.as_deref(), Some("col"));
    assert!(matches!(
        &first.default_value,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        }))
    ));
    assert!(matches!(
        &last.default_value,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Generic(_),
            ..
        }))
    ));
    assert_eq!(
        like.properties_option,
        Some(LikePropertiesOption::Excluding)
    );
    assert_eq!(
        &source[like.span.start()..like.span.end()],
        "LIKE cat.s.source EXCLUDING PROPERTIES"
    );
    assert!(
        matches!(&first.properties[0].value, Some(Expr::Parameter(value)) if value.position == 1)
    );
    assert!(
        matches!(&table.properties[0].value, Some(Expr::Parameter(value)) if value.position == 0)
    );
    assert!(table.properties[1].value.is_none());
}

#[test]
fn table_statement_modes_and_like_options_remain_distinct() {
    let omit = roundtrip("CREATE TABLE IF (LIKE source)");
    let excluding = roundtrip("CREATE TABLE IF (LIKE source EXCLUDING PROPERTIES)");
    let including = roundtrip("CREATE TABLE IF (LIKE source INCLUDING PROPERTIES)");
    assert!(!omit.syntax_eq(&excluding));
    assert!(!excluding.syntax_eq(&including));
    let TrinoStatement::CreateTable(table) = omit else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(table.name.parts[0].value, "IF");
    assert_eq!(table.save_mode, StatementSaveMode::Fail);
    assert!(matches!(
        &table.elements[0],
        TrinoTableElement::Like(node) if node.properties_option.is_none()
    ));

    assert!(matches!(
        roundtrip("CREATE OR REPLACE TABLE t (id bigint)"),
        TrinoStatement::CreateTable(node) if node.save_mode == StatementSaveMode::Replace
    ));
    let source = "  CREATE OR REPLACE TABLE IF NOT EXISTS t (id bigint)";
    let error = parser::parse_statement(source).unwrap_err();
    let ParserError::Parse(ParseError::UnexpectedToken { span, found, .. }) = error else {
        panic!("expected a save-mode syntax error");
    };
    assert_eq!(&source[span.start()..span.end()], "CREATE");
    assert_eq!(found, "OR REPLACE and IF NOT EXISTS");
}

#[test]
fn drop_and_analyze_preserve_original_shape_and_reject_legacy_suffixes() {
    let TrinoStatement::DropTable(drop) = roundtrip("DROP TABLE IF EXISTS cat.s.\"A.B\"") else {
        panic!("expected DROP TABLE");
    };
    assert!(drop.exists);
    assert_eq!(drop.table_name.parts.len(), 3);
    assert!(drop.table_name.parts[2].quoted);

    let TrinoStatement::Analyze(analyze) =
        roundtrip("ANALYZE cat.s.t WITH (columns = ARRAY['id'], p = ?)")
    else {
        panic!("expected ANALYZE");
    };
    assert_eq!(analyze.table_name.parts.len(), 3);
    assert_eq!(analyze.properties[0].name.value, "columns");
    assert!(
        matches!(&analyze.properties[0].value, Some(Expr::Array(array)) if array.values.len() == 1)
    );
    assert!(
        matches!(&analyze.properties[1].value, Some(Expr::Parameter(value)) if value.position == 0)
    );

    for source in [
        "CREATE TABLE t ()",
        "CREATE TABLE t (id bigint DEFAULT 1 + 2)",
        "CREATE TABLE t (id bigint NOT NULL DEFAULT 5)",
        "CREATE TABLE t (id bigint) DISTRIBUTED BY HASH(id)",
        "CREATE TABLE t (id bigint);",
        "DROP TABLE t FORCE",
        "ANALYZE TABLE t (id)",
    ] {
        assert!(parser::parse_statement(source).is_err(), "{source}");
    }
}
