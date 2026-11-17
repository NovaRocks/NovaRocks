// SPDX-License-Identifier: Apache-2.0
use novarocks_parser::ast::*;
use novarocks_parser::{trino_parser, trino_printer};

fn roundtrip(source: &str) -> (Expr, String) {
    let original = trino_parser::parse_expression(source).unwrap();
    let printed = trino_printer::print_expression(&original);
    let reparsed = trino_parser::parse_expression(&printed).unwrap();
    assert!(original.syntax_eq(&reparsed), "{source:?} -> {printed:?}");
    (original, printed)
}
fn literal(value: &Expr) -> &LiteralKind {
    let Expr::Literal(value) = value else {
        panic!("expected the real literal child")
    };
    &value.kind
}

#[test]
fn special_printer_preserves_fields_qualified_names_and_optional_children() {
    let (extract, _) = roundtrip("EXTRACT(DAY_OF_WEEK FROM x)");
    assert!(matches!(extract, Expr::Extract(value) if value.field == ExtractField::DayOfWeek));
    assert!(matches!(roundtrip("x AT LOCAL").0, Expr::AtLocal(_)));
    assert!(matches!(
        roundtrip("x AT TIME ZONE 'UTC'").0,
        Expr::AtTimeZone(_)
    ));
    roundtrip("x AT TIME ZONE INTERVAL '02:00' HOUR TO MINUTE");

    let (grouping, _) = roundtrip("GROUPING(T.\"MiX\", \"Two Words\")");
    let Expr::GroupingOperation(grouping) = grouping else {
        panic!("not a grouping operation")
    };
    let first = identifier_chain_observed(&grouping.grouping_columns[0], |_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();
    assert_eq!(first.first().value, "t");
    assert_eq!(first.last().value, "mix");
    assert!(!first.last().quoted);
    let Expr::Identifier(second) = &grouping.grouping_columns[1] else {
        panic!("not the second name")
    };
    assert_eq!(second.value, "two words");
    assert!(second.quoted);

    let (none, _) = roundtrip("TRIM(x)");
    let Expr::Trim(none) = none else {
        panic!("not trim")
    };
    assert_eq!(none.specification, TrimSpecification::Both);
    assert!(none.trim_character.is_none());
    let (empty, _) = roundtrip("TRIM(x, '')");
    let Expr::Trim(empty) = empty else {
        panic!("not trim")
    };
    assert!(
        matches!(literal(empty.trim_character.as_ref().unwrap()), LiteralKind::String(value) if value.is_empty())
    );
    roundtrip("TRIM(TRAILING 'x' FROM y)");

    let (none, _) = roundtrip("OVERLAY(x PLACING y FROM 2)");
    let Expr::Overlay(none) = none else {
        panic!("not overlay")
    };
    assert!(none.length.is_none());
    let (null, _) = roundtrip("OVERLAY(x PLACING y FROM 2 FOR NULL)");
    let Expr::Overlay(null) = null else {
        panic!("not overlay")
    };
    assert!(matches!(
        literal(null.length.as_ref().unwrap()),
        LiteralKind::Null
    ));
}

#[test]
fn listagg_printer_inverts_real_constructor_defaults_overflow_and_metadata() {
    for (source, error, filler, count) in [
        ("LISTAGG(x) WITHIN GROUP (ORDER BY y)", true, "...", false),
        (
            "LISTAGG(DISTINCT x, ';' ON OVERFLOW TRUNCATE 'cut' WITH COUNT) WITHIN GROUP (ORDER BY y DESC NULLS FIRST) FILTER (WHERE x IS NOT NULL) OVER (PARTITION BY z)",
            false,
            "cut",
            true,
        ),
        (
            "LISTAGG(x, '' ON OVERFLOW TRUNCATE '' WITHOUT COUNT) WITHIN GROUP (ORDER BY y)",
            false,
            "",
            false,
        ),
        (
            "LISTAGG(x ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY y) OVER w",
            false,
            "...",
            false,
        ),
    ] {
        let (value, printed) = roundtrip(source);
        let Expr::FunctionCall(value) = value else {
            panic!("LISTAGG did not use its real call class")
        };
        assert_eq!(value.name.parts[0].value, "LISTAGG");
        assert!(!value.name.parts[0].quoted);
        assert_eq!(value.arguments.len(), 5);
        assert!(
            matches!(literal(&value.arguments[2].value), LiteralKind::Boolean(value) if *value == error)
        );
        assert!(
            matches!(literal(&value.arguments[3].value), LiteralKind::String(value) if value == filler)
        );
        assert!(
            matches!(literal(&value.arguments[4].value), LiteralKind::Boolean(value) if *value == count)
        );
        assert!(printed.contains(" WITHIN GROUP (ORDER BY "));
    }
    let (ordinary, printed) = roundtrip("\"LISTAGG\"(x, ',', TRUE, '...', FALSE)");
    let Expr::FunctionCall(ordinary) = ordinary else {
        panic!("not an ordinary call")
    };
    assert!(ordinary.name.parts[0].quoted);
    assert!(ordinary.order_by.is_none());
    assert!(!printed.contains("WITHIN GROUP"));
}

#[test]
fn method_and_interval_printer_keep_real_classes_and_prevent_day_alias_capture() {
    // These inputs also require Root's subsequent real grammar producers.
    // This is a candidate guard, not a claim that those producers already ran.
    assert!(matches!(
        roundtrip("(x).SELECT(a => 1)").0,
        Expr::MethodCall(_)
    ));
    assert!(matches!(
        roundtrip("\"Cat\".type_name::\"m\"(x => 1)").0,
        Expr::StaticMethodCall(_)
    ));
    let (difference, _) = roundtrip("((a - b) DAY(2) TO SECOND(3))");
    assert!(matches!(difference, Expr::IntervalValueExpression(_)));

    let original = trino_parser::parse_query("SELECT ((a - b) DAY)").unwrap();
    let SetExpr::Select(select) = original.body.as_ref() else {
        panic!("not select")
    };
    let SelectItem::SingleColumn(column) = &select.projection[0] else {
        panic!("not a single column")
    };
    assert!(column.alias.is_none());
    assert!(matches!(
        &column.expression,
        Expr::IntervalValueExpression(_)
    ));
    let printed = trino_printer::print_query(&original);
    let reparsed = trino_parser::parse_query(&printed).unwrap();
    assert!(original.syntax_eq(&reparsed), "{printed}");
}
