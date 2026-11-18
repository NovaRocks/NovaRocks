// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{Expr, Parameter, SyntaxEq, TrinoStatement, Visit};
use novarocks_parser::{ParserError, trino_parser, trino_printer};

fn roundtrip(source: &str) -> TrinoStatement {
    let statement =
        trino_parser::parse_statement(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    let rendered = trino_printer::print_statement(&statement).unwrap();
    let reparsed = trino_parser::parse_statement(&rendered).unwrap();
    assert!(statement.syntax_eq(&reparsed), "{source}\n{rendered}");
    statement
}

#[test]
fn metadata_show_preserves_pinned_targets_and_decoded_filters() {
    use TrinoStatement::*;

    let source = r#"  SHOW TABLES IN lake."s.c" LIKE U&'caf\00E9%' ESCAPE 'xy'"#;
    let ShowTables(node) = roundtrip(source) else {
        panic!("expected SHOW TABLES")
    };
    let schema = node.schema.unwrap();
    assert_eq!(schema.parts[1].value, "s.c");
    assert!(schema.parts[1].quoted);
    assert_eq!(node.like_pattern.as_deref(), Some("café%"));
    assert_eq!(node.escape.as_deref(), Some("xy"));
    assert_eq!(node.span.start(), 2);
    assert_eq!(node.span.end(), source.len());
    assert_eq!(
        &source[schema.span.start()..schema.span.end()],
        r#"lake."s.c""#
    );
    let ShowSchemas(node) = roundtrip(r#"SHOW SCHEMAS FROM "Lake.Id" LIKE 'A%'"#) else {
        panic!("expected SHOW SCHEMAS")
    };
    let catalog = node.catalog.unwrap();
    assert!(catalog.quoted);
    assert_eq!(catalog.value, "Lake.Id");
    assert_eq!(node.like_pattern.as_deref(), Some("A%"));
    assert_eq!(node.escape, None);

    let ShowCatalogs(node) = roundtrip("SHOW CATALOGS LIKE '' ESCAPE ''") else {
        panic!("expected SHOW CATALOGS")
    };
    assert_eq!(node.like_pattern.as_deref(), Some(""));
    assert_eq!(node.escape.as_deref(), Some(""));

    let ShowColumns(node) = roundtrip("SHOW COLUMNS IN lake.s.t LIKE 'foo''bar' ESCAPE '!'") else {
        panic!("expected SHOW COLUMNS")
    };
    assert_eq!(node.table.parts.len(), 3);
    assert_eq!(node.like_pattern.as_deref(), Some("foo'bar"));
    assert_eq!(node.escape.as_deref(), Some("!"));

    let ShowFunctions(node) = roundtrip("SHOW FUNCTIONS FROM lake.s LIKE 'my%' ESCAPE '_'") else {
        panic!("expected SHOW FUNCTIONS")
    };
    assert_eq!(node.schema.unwrap().parts.len(), 2);
    assert_eq!(node.like_pattern.as_deref(), Some("my%"));
    assert_eq!(node.escape.as_deref(), Some("_"));

    let ShowSession(node) = roundtrip("SHOW SESSION LIKE 'my!_%' ESCAPE '!'") else {
        panic!("expected SHOW SESSION")
    };
    assert_eq!(node.like_pattern.as_deref(), Some("my!_%"));
    assert_eq!(node.escape.as_deref(), Some("!"));

    assert!(matches!(roundtrip("SHOW TABLES"), ShowTables(node)
        if node.schema.is_none() && node.like_pattern.is_none() && node.escape.is_none()));
    assert!(matches!(roundtrip("SHOW SCHEMAS"), ShowSchemas(node)
        if node.catalog.is_none() && node.like_pattern.is_none() && node.escape.is_none()));
    assert!(matches!(roundtrip("SHOW CATALOGS"), ShowCatalogs(node)
        if node.like_pattern.is_none() && node.escape.is_none()));
    assert!(matches!(roundtrip("SHOW COLUMNS FROM t"), ShowColumns(node)
        if node.like_pattern.is_none() && node.escape.is_none()));
    assert!(matches!(roundtrip("SHOW FUNCTIONS"), ShowFunctions(node)
        if node.schema.is_none() && node.like_pattern.is_none() && node.escape.is_none()));
    assert!(matches!(roundtrip("SHOW SESSION"), ShowSession(node)
        if node.like_pattern.is_none() && node.escape.is_none()));
}

#[test]
fn describe_synonyms_preserve_the_show_columns_shape_and_original_table_spans() {
    let columns = roundtrip(r#"SHOW COLUMNS FROM lake."t.1""#);
    for source in [r#"DESCRIBE lake."t.1""#, r#"DESC lake."t.1""#] {
        let statement = roundtrip(source);
        assert!(statement.syntax_eq(&columns));
        let TrinoStatement::ShowColumns(node) = &statement else {
            panic!("expected columns")
        };
        assert_eq!(
            &source[node.table.span.start()..node.table.span.end()],
            r#"lake."t.1""#
        );
        assert_eq!(node.span.end(), source.len());
        assert_eq!(
            trino_printer::print_statement(&statement).unwrap(),
            r#"SHOW COLUMNS FROM lake."t.1""#
        );
    }
    for source in [
        "DESCRIBE INPUT",
        "DESCRIBE OUTPUT",
        "DESCRIBE input.t",
        "DESCRIBE output.t",
    ] {
        assert!(
            matches!(roundtrip(source), TrinoStatement::ShowColumns(_)),
            "{source}"
        );
    }
}

#[test]
fn set_path_preserves_ordered_catalog_and_schema_identifiers() {
    let source = r#"  SET PATH "ca.t"."s""1", local, "sc.hema""#;
    let TrinoStatement::SetPath(node) = roundtrip(source) else {
        panic!("expected SET PATH")
    };
    let path = node.path_specification;
    assert_eq!(path.path.len(), 3);
    assert_eq!(path.path[0].catalog.as_ref().unwrap().value, "ca.t");
    assert_eq!(path.path[0].schema.value, "s\"1");
    assert!(path.path[0].schema.quoted);
    assert_eq!(
        &source[path.path[0].span.start()..path.path[0].span.end()],
        r#""ca.t"."s""1""#
    );
    assert!(path.path[1].catalog.is_none());
    assert_eq!(path.path[1].schema.value, "local");
    assert!(!path.path[1].schema.quoted);
    assert!(path.path[2].catalog.is_none());
    assert!(path.path[2].schema.quoted);
    assert_eq!(path.path[2].schema.value, "sc.hema");
    assert_eq!(
        &source[path.span.start()..path.span.end()],
        r#""ca.t"."s""1", local, "sc.hema""#
    );
    assert_eq!(node.span.start(), 2);
    assert!(!roundtrip("SET PATH a, b").syntax_eq(&roundtrip("SET PATH b, a")));
}

#[test]
fn set_time_zone_keeps_the_local_choice_and_original_expression_parameters() {
    let local = roundtrip("SET TIME ZONE LOCAL");
    let TrinoStatement::SetTimeZone(node) = &local else {
        panic!("expected SET TIME ZONE")
    };
    assert!(node.time_zone.is_none());
    let identifier = roundtrip("SET TIME ZONE (LOCAL)");
    assert!(!local.syntax_eq(&identifier));
    let TrinoStatement::SetTimeZone(node) = &identifier else {
        panic!("expected SET TIME ZONE")
    };
    assert!(matches!(&node.time_zone, Some(Expr::Identifier(ident)) if !ident.quoted));
    assert_eq!(
        trino_printer::print_statement(&identifier).unwrap(),
        "SET TIME ZONE (LOCAL)"
    );

    assert!(
        matches!(roundtrip(r#"SET TIME ZONE "LOCAL""#), TrinoStatement::SetTimeZone(node)
        if matches!(&node.time_zone, Some(Expr::Identifier(ident)) if ident.quoted))
    );
    let expression = roundtrip("SET TIME ZONE LOCAL + ?");
    let TrinoStatement::SetTimeZone(node) = &expression else {
        panic!("expected SET TIME ZONE")
    };
    assert!(matches!(&node.time_zone, Some(Expr::Binary(_))));
    roundtrip("SET TIME ZONE INTERVAL '05:30' HOUR TO MINUTE");
    roundtrip("SET TIME ZONE 'America/Los_Angeles'");

    #[derive(Default)]
    struct Parameters(Vec<usize>);
    impl Visit for Parameters {
        fn visit_parameter(&mut self, parameter: &Parameter) {
            self.0.push(parameter.position);
        }
    }
    let mut params = Parameters::default();
    let statement = roundtrip("SET TIME ZONE ? + ?");
    params.visit_trino_statement(&statement);
    assert_eq!(params.0, [0, 1]);
}

#[test]
fn malformed_metadata_and_session_text_is_a_syntax_error() {
    for source in [
        "SHOW TABLES FROM",
        "SHOW SCHEMAS IN lake.schema_name",
        "SHOW COLUMNS lake.s.t",
        "SHOW CATALOGS FROM lake",
        "SHOW FUNCTIONS ESCAPE 'x'",
        "SHOW SESSION LIKE 'x' ESCAPE",
        "DESCRIBE t LIKE 'x'",
        "DESC input x",
        "SET PATH",
        "SET PATH a.b.c",
        "SET PATH a,",
        "SET TIME ZONE",
        "SET TIME ZONE LOCAL +",
        "SET TIME ZONE LOCAL;",
        "SHOW COLUMNS FROM t;",
    ] {
        assert!(
            matches!(
                trino_parser::parse_statement(source),
                Err(ParserError::Parse(_))
            ),
            "{source}"
        );
    }
}
