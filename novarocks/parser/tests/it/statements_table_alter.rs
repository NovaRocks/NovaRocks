// SPDX-License-Identifier: Apache-2.0

use novarocks_parser::ast::{
    CommentKind, Expr, Literal, LiteralKind, Parameter, SetPropertiesKind, SyntaxEq,
    TrinoColumnPosition, TrinoStatement, Visit,
};
use novarocks_parser::{ParserError, Span, parser, printer};

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
fn alter_column_alternatives_keep_their_distinct_guard_name_and_position_facts() {
    let source = "ALTER TABLE IF EXISTS lake.ns.t ADD COLUMN IF NOT EXISTS a.\"b.c\" bigint DEFAULT NULL NOT NULL COMMENT 'child' WITH (p = ?) AFTER \"q.r\"";
    let TrinoStatement::AddColumn(add) = roundtrip(source) else {
        panic!("expected ADD COLUMN");
    };
    assert!(add.table_exists && add.column_not_exists);
    assert_eq!(add.name.parts.len(), 3);
    assert_eq!(add.column.name.parts[1].value, "b.c");
    assert!(add.column.name.parts[1].quoted);
    assert!(!add.column.nullable);
    assert_eq!(add.column.comment.as_deref(), Some("child"));
    assert!(matches!(
        &add.column.default_value,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        }))
    ));
    assert!(matches!(&add.column.properties[0].value,
        Some(Expr::Parameter(value)) if value.position == 0));
    let Some(TrinoColumnPosition::After(after)) = &add.position else {
        panic!("expected AFTER");
    };
    assert_eq!(after.value, "q.r");
    assert!(after.quoted);
    assert_eq!(&source[after.span.start()..after.span.end()], "\"q.r\"");

    let absent = roundtrip("ALTER TABLE t ADD COLUMN i bigint");
    let first = roundtrip("ALTER TABLE t ADD COLUMN i bigint FIRST");
    let last = roundtrip("ALTER TABLE t ADD COLUMN i bigint LAST");
    assert!(!absent.syntax_eq(&last));
    assert!(!first.syntax_eq(&last));

    let TrinoStatement::RenameColumn(rename) =
        roundtrip("ALTER TABLE t RENAME COLUMN IF EXISTS a.b TO \"New.Label\"")
    else {
        panic!("expected RENAME COLUMN");
    };
    assert!(!rename.table_exists && rename.column_exists);
    assert_eq!(rename.source.parts.len(), 2);
    assert!(rename.target.quoted);
    assert_eq!(rename.target.value, "New.Label");

    let TrinoStatement::DropColumn(drop) =
        roundtrip("ALTER TABLE IF EXISTS t DROP COLUMN a.\"b.c\"")
    else {
        panic!("expected DROP COLUMN");
    };
    assert!(drop.table_exists && !drop.column_exists);
    assert_eq!(drop.field.parts[1].value, "b.c");
    assert!(drop.field.parts[1].quoted);
    assert!(matches!(
        roundtrip("ALTER TABLE IF EXISTS lake.s.t RENAME TO lake.s.u"),
        TrinoStatement::RenameTable(rename) if rename.exists && rename.target.parts[2].value == "u",
    ));
}

#[test]
fn alter_column_literal_set_drop_type_and_constraint_are_separate_nodes() {
    assert!(matches!(
        roundtrip("ALTER TABLE IF EXISTS t ALTER COLUMN a.b SET DEFAULT NULL"),
        TrinoStatement::SetDefaultValue(node)
            if node.table_exists && node.column_name.parts.len() == 2
                && matches!(&node.default_value, Expr::Literal(Literal { kind: LiteralKind::Null, .. })),
    ));
    assert!(matches!(
        roundtrip("ALTER TABLE t ALTER COLUMN a.b DROP DEFAULT"),
        TrinoStatement::DropDefaultValue(node) if !node.table_exists && node.column_name.parts.len() == 2,
    ));
    assert!(matches!(
        roundtrip("ALTER TABLE IF EXISTS t ALTER COLUMN a.b SET DATA TYPE decimal(10, 2)"),
        TrinoStatement::SetColumnType(node)
            if node.table_exists && node.column_name.parts.len() == 2
                && node.data_type.syntax_eq(&parser::parse_type("decimal(10, 2)").unwrap()),
    ));
    assert!(matches!(
        roundtrip("ALTER TABLE IF EXISTS t ALTER COLUMN \"a.b\" DROP NOT NULL"),
        TrinoStatement::DropNotNullConstraint(node)
            if node.table_exists && node.column.quoted && node.column.value == "a.b",
    ));
}

#[test]
fn table_properties_and_execute_preserve_actual_expression_and_parameter_slots() {
    let TrinoStatement::SetProperties(set) =
        roundtrip("ALTER TABLE t SET PROPERTIES \"key.part\" = DEFAULT, n = NULL, p = (DEFAULT)")
    else {
        panic!("expected SET PROPERTIES");
    };
    assert_eq!(set.kind, SetPropertiesKind::Table);
    assert!(set.properties[0].name.quoted);
    assert_eq!(set.properties[0].name.value, "key.part");
    assert!(set.properties[0].value.is_none());
    assert!(matches!(
        &set.properties[1].value,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        }))
    ));
    assert!(matches!(&set.properties[2].value,
        Some(Expr::Identifier(name)) if name.value == "DEFAULT" && !name.quoted));

    let source = "ALTER TABLE lake.ns.t EXECUTE \"p\"(?, \"named\" => ?) WHERE id = ?";
    let statement = roundtrip(source);
    let TrinoStatement::TableExecute(execute) = &statement else {
        panic!("expected ALTER TABLE EXECUTE");
    };
    assert_eq!(
        &source[execute.table.span.start()..execute.table.span.end()],
        "TABLE lake.ns.t"
    );
    assert!(execute.table.query_period.is_none() && execute.table.branch.is_none());
    assert!(execute.procedure_name.quoted);
    assert!(execute.arguments[0].name.is_none());
    assert_eq!(execute.arguments[1].name.as_ref().unwrap().value, "named");
    assert!(execute.arguments[1].name.as_ref().unwrap().quoted);
    assert!(matches!(&execute.selection, Some(Expr::Predicated(_))));

    struct Parameters(Vec<(Span, usize)>);
    impl Visit for Parameters {
        fn visit_parameter(&mut self, value: &Parameter) {
            self.0.push((value.span, value.position));
        }
    }
    let mut actual = Parameters(Vec::new());
    actual.visit_trino_statement(&statement);
    let expected: Vec<_> = source
        .match_indices('?')
        .enumerate()
        .map(|(position, (offset, _))| (Span::new(offset, offset + 1), position))
        .collect();
    assert_eq!(actual.0, expected);
    assert!(
        roundtrip("ALTER TABLE t EXECUTE p").syntax_eq(&roundtrip("ALTER TABLE t EXECUTE p()"))
    );
}

#[test]
fn truncate_and_comment_keep_qualified_names_and_null_distinct_from_empty_text() {
    assert!(matches!(
        roundtrip("TRUNCATE TABLE lake.ns.\"table.name\""),
        TrinoStatement::TruncateTable(node)
            if node.table_name.parts.len() == 3 && node.table_name.parts[2].quoted,
    ));
    let clear = roundtrip("COMMENT ON COLUMN lake.ns.t.\"a.b\" IS NULL");
    let empty = roundtrip("COMMENT ON COLUMN lake.ns.t.\"a.b\" IS ''");
    assert!(!clear.syntax_eq(&empty));
    let TrinoStatement::Comment(clear) = clear else {
        panic!("expected COMMENT");
    };
    assert_eq!(clear.kind, CommentKind::Column);
    assert_eq!(clear.name.parts[3].value, "a.b");
    assert!(clear.name.parts[3].quoted);
    assert!(clear.comment.is_none());

    for (source, kind) in [
        ("COMMENT ON TABLE ns.t IS 'x''y'", CommentKind::Table),
        ("COMMENT ON VIEW ns.v IS 'x''y'", CommentKind::View),
        (
            "COMMENT ON MATERIALIZED VIEW ns.m IS 'x''y'",
            CommentKind::MaterializedView,
        ),
    ] {
        let TrinoStatement::Comment(comment) = roundtrip(source) else {
            panic!("expected COMMENT");
        };
        assert_eq!(comment.kind, kind);
        assert_eq!(comment.comment.as_deref(), Some("x'y"));
    }
}

#[test]
fn legacy_or_misshaped_alter_forms_are_rejected_as_syntax() {
    for source in [
        "ALTER TABLE t SET TBLPROPERTIES ('k' = 'v')",
        "ALTER TABLE t SET PROPERTIES (k = 'v')",
        "ALTER TABLE IF EXISTS t SET PROPERTIES k = 'v'",
        "ALTER TABLE IF EXISTS t EXECUTE p",
        "ALTER TABLE t EXECUTE p(1,)",
        "ALTER TABLE t EXECUTE a.b",
        "ALTER TABLE t ADD COLUMN c bigint BEFORE d",
        "ALTER TABLE t ADD COLUMN c bigint AFTER a.b",
        "ALTER TABLE t ALTER COLUMN a.b DROP NOT NULL",
        "ALTER TABLE t ALTER COLUMN c SET NOT NULL",
        "ALTER TABLE t ALTER COLUMN c SET DEFAULT 1 + 2",
        "ALTER TABLE t RENAME COLUMN c TO a.b",
        "TRUNCATE TABLE lake.ns.t @b",
        "COMMENT ON COLUMN t.c IS concat('a','b')",
        "TRUNCATE TABLE t;",
    ] {
        assert!(
            matches!(parser::parse_statement(source), Err(ParserError::Parse(_))),
            "{source}",
        );
    }
}
