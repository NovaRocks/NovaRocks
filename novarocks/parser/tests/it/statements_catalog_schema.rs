// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{
    Expr, Literal, LiteralKind, StatementPrincipalKind, SyntaxEq, TrinoStatement,
};
use novarocks_parser::{trino_parser, trino_printer};

fn roundtrip(source: &str) -> TrinoStatement {
    let original =
        trino_parser::parse_statement(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    let printed = trino_printer::print_statement(&original)
        .unwrap_or_else(|error| panic!("{original:?}: {error}"));
    let reparsed = trino_parser::parse_statement(&printed)
        .unwrap_or_else(|error| panic!("{printed}: {error}"));
    assert!(original.syntax_eq(&reparsed), "{source}\n{printed}");
    original
}

#[test]
fn catalog_creation_keeps_connector_principal_comments_and_ordered_properties() {
    let source = " -- omitted leading trivia\nCREATE CATALOG IF NOT EXISTS \"Lake\" USING ice COMMENT 'a''b' AUTHORIZATION USER \"Owner\" WITH (\"Some.Option\" = ?, d = DEFAULT, n = NULL, x = DEFAULT + ?, y = (DEFAULT)) -- trailing";
    let TrinoStatement::CreateCatalog(catalog) = roundtrip(source) else {
        panic!("expected CREATE CATALOG");
    };
    assert!(catalog.not_exists);
    assert_eq!(catalog.catalog_name.value, "Lake");
    assert!(catalog.catalog_name.quoted);
    assert_eq!(catalog.connector_name.value, "ice");
    assert_eq!(catalog.comment.as_deref(), Some("a'b"));
    assert_eq!(catalog.span.start(), source.find("CREATE").unwrap());
    assert_eq!(catalog.span.end(), source.rfind(')').unwrap() + 1);
    let principal = catalog.principal.as_ref().unwrap();
    assert_eq!(principal.kind, StatementPrincipalKind::User);
    assert!(principal.name.quoted);
    assert_eq!(principal.name.value, "Owner");

    let [first, default, null, add, identifier] = catalog.properties.as_slice() else {
        panic!("expected five properties");
    };
    assert!(first.name.quoted);
    assert_eq!(first.name.value, "Some.Option");
    assert_eq!(
        &source[first.span.start()..first.span.end()],
        "\"Some.Option\" = ?"
    );
    assert!(matches!(&first.value, Some(Expr::Parameter(parameter)) if parameter.position == 0));
    assert!(default.value.is_none());
    assert_eq!(
        &source[default.span.start()..default.span.end()],
        "d = DEFAULT"
    );
    assert!(matches!(
        &null.value,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        }))
    ));
    let Some(Expr::Binary(add)) = &add.value else {
        panic!("DEFAULT + ? must be an expression");
    };
    assert!(matches!(add.left.as_ref(), Expr::Identifier(name) if name.value == "DEFAULT"));
    assert!(matches!(add.right.as_ref(), Expr::Parameter(parameter) if parameter.position == 1));
    assert!(
        matches!(&identifier.value, Some(Expr::Identifier(name)) if !name.quoted && name.value == "DEFAULT")
    );
}

#[test]
fn schema_flags_and_principal_prefixes_are_only_recognized_in_their_productions() {
    let TrinoStatement::CreateSchema(schema) =
        roundtrip("CREATE SCHEMA IF AUTHORIZATION ROLE WITH (location = 'a')")
    else {
        panic!("expected CREATE SCHEMA");
    };
    assert!(!schema.not_exists);
    assert_eq!(schema.schema_name.parts[0].value, "IF");
    assert_eq!(schema.properties.len(), 1);
    let principal = schema.principal.as_ref().unwrap();
    assert_eq!(principal.kind, StatementPrincipalKind::Unspecified);
    assert_eq!(principal.name.value, "ROLE");

    let TrinoStatement::CreateSchema(schema) =
        roundtrip("CREATE SCHEMA IF NOT EXISTS lake.\"schema.part\" AUTHORIZATION ROLE base")
    else {
        panic!("expected CREATE SCHEMA");
    };
    assert!(schema.not_exists);
    assert_eq!(schema.schema_name.parts.len(), 2);
    assert!(schema.schema_name.parts[1].quoted);
    assert_eq!(schema.schema_name.parts[1].value, "schema.part");
    assert_eq!(
        schema.principal.as_ref().unwrap().kind,
        StatementPrincipalKind::Role
    );
    assert!(schema.properties.is_empty());

    let TrinoStatement::DropCatalog(catalog) = roundtrip("DROP CATALOG IF") else {
        panic!("expected DROP CATALOG");
    };
    assert!(!catalog.exists);
    assert!(!catalog.cascade);
    assert_eq!(catalog.catalog_name.value, "IF");
    assert!(roundtrip("DROP CATALOG c").syntax_eq(&roundtrip("DROP CATALOG c RESTRICT")));
    assert!(
        !roundtrip("DROP SCHEMA IF EXISTS lake.ns RESTRICT")
            .syntax_eq(&roundtrip("DROP SCHEMA IF EXISTS lake.ns CASCADE"))
    );
    let TrinoStatement::RenameSchema(rename) =
        roundtrip("ALTER SCHEMA lake.old RENAME TO \"new.schema\"")
    else {
        panic!("expected RENAME SCHEMA");
    };
    assert_eq!(rename.source.parts.len(), 2);
    assert!(rename.target.quoted);
    assert_eq!(rename.target.value, "new.schema");
}

#[test]
fn catalog_and_schema_reject_non_grammar_statement_suffixes() {
    for source in [
        "CREATE EXTERNAL CATALOG c PROPERTIES ('type' = 'iceberg')",
        "CREATE CATALOG cat.name USING ice",
        "CREATE CATALOG c USING cat.ice",
        "CREATE CATALOG c USING ice WITH ()",
        "CREATE CATALOG c USING ice WITH (x = NULL,)",
        "CREATE CATALOG c USING ice WITH (dot.key = 'x')",
        "CREATE CATALOG c USING ice WITH (x = 'a') COMMENT 'out of order'",
        "CREATE SCHEMA s COMMENT 'unsupported syntax'",
        "ALTER SCHEMA IF EXISTS a RENAME TO b",
        "ALTER SCHEMA a RENAME TO b.c",
        "DROP SCHEMA a FORCE",
        "DROP CATALOG c; DROP CATALOG d",
        "CREATE SCHEMA s;",
    ] {
        assert!(trino_parser::parse_statement(source).is_err(), "{source}");
    }
}
