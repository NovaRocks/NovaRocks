// SPDX-License-Identifier: Apache-2.0
use novarocks_parser::ast::SyntaxEq;
use novarocks_parser::{parser, printer};

#[test]
fn trino_printer_literal_and_type_facts_survive_public_roundtrip() {
    for source in [
        "-0e0",
        "1e999",
        "-1e999",
        "- /* original sign */ 0x8000000000000000",
        "DECIMAL '1_2.300'",
        r"'a\b''中'",
        r"U&'a\0062\+01F600'",
        "X'00ff'",
        "DATE '2026-11-18'",
        "DOUBLE PRECISION 'NaN'",
        "\"decimal\" '1.25'",
        "INTERVAL - '2.3' SECOND(02,09)",
    ] {
        let original = parser::parse_expression(source).unwrap();
        let printed = printer::print_expression(&original).unwrap();
        let reparsed = parser::parse_expression(&printed).unwrap();
        assert!(original.syntax_eq(&reparsed), "{source:?} -> {printed:?}");
    }
    for source in [
        "ROW(\"x\" INTEGER, ARRAY(DECIMAL(020,04)), TIME(custom_precision) WITH TIME ZONE)",
        "TIMESTAMP",
        "TIMESTAMP(6)",
        "ROW(time WITHOUT, interval DAY(1,2))",
        "TIME(1,2)",
        "INTERVAL DAY(2) TO SECOND(9)",
        "INTEGER ARRAY[184467440737095516160] ARRAY",
    ] {
        let original = parser::parse_type(source).unwrap();
        let printed = printer::print_type(&original).unwrap();
        let reparsed = parser::parse_type(&printed).unwrap();
        assert!(original.syntax_eq(&reparsed), "{source:?} -> {printed:?}");
    }
}

#[test]
fn trino_printer_expression_children_and_optional_facts_survive_public_roundtrip() {
    for source in [
        "-(1)",
        "+1",
        "(a AND b) AND c",
        "a AND (b OR c)",
        "((1 + 2) * 3) + (4 / (5 - 1))",
        "x NOT BETWEEN ASYMMETRIC 1 AND 2",
        "x IS NOT DISTINCT FROM y",
        "x NOT IN (1, 2)",
        "x LIKE 'a%' ESCAPE '!'",
        "IF(x, 1)",
        "IF(x, 1, NULL)",
        "COALESCE(x, NULLIF(y, 2))",
        "TRY(FORMAT('%s', x))",
        "CASE x WHEN > 1 THEN 2 ELSE 3 END",
        "CASE WHEN x IS NULL THEN 2 END",
        "TRY_CAST(ROW(1 AS \"f\", ARRAY[2]) AS ROW(x BIGINT, y ARRAY(BIGINT)))",
        "(x, y) -> x + y",
        "EXISTS (SELECT 1)",
        "CURRENT_TIMESTAMP(3)",
        "\"if\"(x, y)",
        "RUNNING f(DISTINCT x, a => y ORDER BY z DESC NULLS FIRST) FILTER (WHERE TRUE) IGNORE NULLS OVER (PARTITION BY x ORDER BY y ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)",
    ] {
        let original = parser::parse_expression(source).unwrap();
        let printed = printer::print_expression(&original).unwrap();
        let reparsed = parser::parse_expression(&printed).unwrap();
        assert!(original.syntax_eq(&reparsed), "{source:?} -> {printed:?}");
    }
}

#[test]
fn trino_printer_query_wrappers_counts_and_ordering_survive_public_roundtrip() {
    for source in [
        "SELECT DISTINCT -0e0 AS \"Z\", ROW(1 AS \"f\"), ARRAY[2]",
        "WITH q(a) AS (VALUES ROW(1)) SELECT q.* FROM q",
        "SELECT t.a FROM t INNER JOIN u USING (a) LEFT JOIN v ON u.a = v.a",
        "SELECT a FROM t, u WHERE t.a = u.a GROUP BY GROUPING SETS ((a), ()) HAVING count(*) > 0 ORDER BY a NULLS LAST OFFSET 2 ROWS FETCH FIRST ROWS ONLY",
        "VALUES 1, ROW(2), ROW(ROW(3))",
        "TABLE t UNION ALL VALUES ROW(1)",
        "SELECT 1 UNION ALL (SELECT 2 EXCEPT SELECT 3)",
        "SELECT sum(x) OVER win FROM t WINDOW win AS (ORDER BY y)",
        "SELECT sum(x) OVER (ORDER BY y ROWS CURRENT ROW) FROM t LIMIT ALL",
    ] {
        let original = parser::parse_query(source).unwrap();
        let printed = printer::print_query(&original).unwrap();
        let reparsed = parser::parse_query(&printed).unwrap();
        assert!(original.syntax_eq(&reparsed), "{source:?} -> {printed:?}");
    }
    let absent = parser::parse_query("SELECT 1 FETCH FIRST ROWS ONLY").unwrap();
    let one = parser::parse_query("SELECT 1 FETCH FIRST 1 ROWS ONLY").unwrap();
    assert!(!absent.syntax_eq(&one));
    for value in [absent, one] {
        let printed = printer::print_query(&value).unwrap();
        let reparsed = parser::parse_query(&printed).unwrap();
        assert!(value.syntax_eq(&reparsed), "{printed}");
    }
}

fn parameter_query_roundtrip(source: &str) -> String {
    let original = parser::parse_query(source).unwrap();
    let printed = printer::print_query(&original).unwrap();
    let reparsed = parser::parse_query(&printed).unwrap();
    assert!(
        original.syntax_eq(&reparsed),
        "{source} -> {printed}\n{original:?}\n{reparsed:?}"
    );
    printed
}

#[test]
fn parameter_positions_choose_inverse_position_without_reversing_ordinary_strpos() {
    assert!(parameter_query_roundtrip("SELECT POSITION(? IN ?)").contains("POSITION("));
    assert!(parameter_query_roundtrip("SELECT strpos(?, ?)").contains("strpos("));
    for source in [
        "SELECT POSITION(POSITION(? IN ?) IN POSITION(? IN ?))",
        "SELECT POSITION((? + ?) IN (? * ?))",
        "SELECT strpos((? + ?), (? * ?))",
        "SELECT cat.strpos(?, ?)",
        "SELECT STRPOS(?, ?)",
        "SELECT strpos(DISTINCT ?, ?)",
        "SELECT strpos(haystack => ?, needle => ?)",
        "SELECT \"strpos\"(?, ?)",
        "SELECT strpos(?, ? ORDER BY ?) FILTER (WHERE ?) OVER (PARTITION BY ?)",
    ] {
        parameter_query_roundtrip(source);
    }
}

#[test]
fn parameter_gaps_duplicates_and_component_offsets_have_typed_source_errors() {
    use novarocks_parser::Span;
    use novarocks_parser::ast::*;
    use novarocks_parser::printer::PrintError;
    for source in [
        "SELECT IF(?, ? ORDER BY ?), ?",
        "SELECT COALESCE(?, ? ORDER BY ?), ?",
    ] {
        let original = parser::parse_query(source).unwrap();
        let first = source.find('?').unwrap();
        assert!(matches!(printer::print_query(&original),
            Err(PrintError::ParameterPosition { expected: 0, actual: 1, span })
                if span == Span::new(first, first + 1)));
        let SetExpr::Select(select) = original.body.as_ref() else {
            panic!()
        };
        let SelectItem::SingleColumn(column) = &select.projection[0] else {
            panic!()
        };
        let first = match &column.expression {
            Expr::If(value) => {
                assert!(value.false_value.is_none());
                &value.condition
            }
            Expr::Coalesce(value) => &value.operands[0],
            _ => panic!(),
        };
        assert!(matches!(first, Expr::Parameter(value) if value.position == 1));
    }
    let source = "SELECT ?, ? + ?";
    let query = parser::parse_query(source).unwrap();
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!()
    };
    let SelectItem::SingleColumn(column) = &select.projection[1] else {
        panic!()
    };
    assert!(matches!(
        printer::print_expression(&column.expression),
        Err(PrintError::ParameterPosition {
            expected: 0,
            actual: 1,
            ..
        })
    ));

    let source = "SELECT ? + ?";
    let mut query = parser::parse_query(source).unwrap();
    let SetExpr::Select(select) = query.body.as_mut() else {
        panic!()
    };
    let SelectItem::SingleColumn(column) = &mut select.projection[0] else {
        panic!()
    };
    let Expr::Binary(binary) = &mut column.expression else {
        panic!()
    };
    let Expr::Parameter(right) = binary.right.as_mut() else {
        panic!()
    };
    right.position = 0;
    let last = source.rfind('?').unwrap();
    assert!(matches!(printer::print_query(&query),
        Err(PrintError::ParameterPosition { expected: 1, actual: 0, span })
            if span == Span::new(last, last + 1)));

    // This discarded visit is after every retained parameter (there are none).
    // No total-counter field is part of the final AST.
    parameter_query_roundtrip("SELECT IF(1, 2 ORDER BY ?)");
}

#[test]
fn construction_ledger_rejects_permutations_that_a_global_set_cannot_detect() {
    use novarocks_parser::ast::*;
    use novarocks_parser::printer::PrintError;
    let mut query = parser::parse_query("SELECT ? + ?").unwrap();
    let SetExpr::Select(select) = query.body.as_mut() else {
        panic!()
    };
    let SelectItem::SingleColumn(column) = &mut select.projection[0] else {
        panic!()
    };
    let Expr::Binary(binary) = &mut column.expression else {
        panic!()
    };
    let Expr::Parameter(left) = binary.left.as_mut() else {
        panic!()
    };
    left.position = 1;
    let Expr::Parameter(right) = binary.right.as_mut() else {
        panic!()
    };
    right.position = 0;
    assert!(matches!(
        printer::print_query(&query),
        Err(PrintError::ParameterPosition {
            expected: 0,
            actual: 1,
            ..
        })
    ));

    let source = "SELECT strpos((? + ?), (? * ?))";
    let mut interleaved = parser::parse_query(source).unwrap();
    let SetExpr::Select(select) = interleaved.body.as_mut() else {
        panic!()
    };
    let SelectItem::SingleColumn(column) = &mut select.projection[0] else {
        panic!()
    };
    let Expr::FunctionCall(call) = &mut column.expression else {
        panic!()
    };
    let Expr::Binary(first) = &mut call.arguments[0].value else {
        panic!()
    };
    let Expr::Parameter(value) = first.right.as_mut() else {
        panic!()
    };
    value.position = 2;
    let Expr::Binary(second) = &mut call.arguments[1].value else {
        panic!()
    };
    let Expr::Parameter(value) = second.left.as_mut() else {
        panic!()
    };
    value.position = 1;
    let start = source.find("? + ?").unwrap();
    assert!(matches!(printer::print_query(&interleaved),
        Err(PrintError::ParameterOrder { span })
            if span == novarocks_parser::Span::new(start, start + "? + ?".len())));

    let mut query = parser::parse_query("SELECT f(?) FILTER (WHERE ?)").unwrap();
    let SetExpr::Select(select) = query.body.as_mut() else {
        panic!()
    };
    let SelectItem::SingleColumn(column) = &mut select.projection[0] else {
        panic!()
    };
    let Expr::FunctionCall(call) = &mut column.expression else {
        panic!()
    };
    let Expr::Parameter(argument) = &mut call.arguments[0].value else {
        panic!()
    };
    argument.position = 0;
    let Expr::Parameter(filter) = call.filter.as_mut().unwrap() else {
        panic!()
    };
    filter.position = 1;
    assert!(matches!(
        printer::print_query(&query),
        Err(PrintError::ParameterPosition {
            expected: 0,
            actual: 1,
            ..
        })
    ));
}

#[test]
fn construction_ledger_preserves_real_nontext_builder_orders() {
    for source in [
        "WITH SESSION s=? WITH FUNCTION f() RETURNS BIGINT WITH (p=?) RETURN ? WITH c AS (SELECT ?) SELECT ? ORDER BY ? OFFSET ? ROWS LIMIT ?",
        "SELECT f(? ORDER BY ?) FILTER (WHERE ?) OVER (PARTITION BY ? ORDER BY ? ROWS BETWEEN ? PRECEDING AND ? FOLLOWING), ?",
        "SELECT LISTAGG(?) WITHIN GROUP (ORDER BY ?) FILTER (WHERE ?) OVER (PARTITION BY ?)",
        "SELECT TRIM(? FROM ?), TRIM(?, ?)",
        "SELECT * FROM t MATCH_RECOGNIZE(PARTITION BY ? ORDER BY ? MEASURES ? AS m PATTERN(A) DEFINE A AS ?)",
        "SELECT f(?) OVER(MEASURES ? AS m ROWS BETWEEN ? PRECEDING AND ? FOLLOWING PATTERN(A) DEFINE A AS ?)",
    ] {
        parameter_query_roundtrip(source);
    }
}

#[test]
fn row_pattern_component_keeps_quantifier_questions_out_of_parameter_events() {
    let original = parser::parse_row_pattern("A?? B{01,2} | PERMUTE(C,D)").unwrap();
    let printed = printer::print_row_pattern(&original).unwrap();
    let reparsed = parser::parse_row_pattern(&printed).unwrap();
    assert!(original.syntax_eq(&reparsed), "{printed}");
}

#[test]
fn non_grammar_empty_and_all_rows_facts_return_typed_errors() {
    use novarocks_parser::SYNTHETIC;
    use novarocks_parser::ast::*;
    use novarocks_parser::printer::PrintError;
    let all = Expr::AllRows(AllRows { span: SYNTHETIC });
    assert!(matches!(
        printer::print_expression(&all),
        Err(PrintError::Unrepresentable { .. })
    ));
    let row = Expr::Row(RowExpr {
        fields: AstVec::new(),
        span: SYNTHETIC,
    });
    assert!(matches!(
        printer::print_expression(&row),
        Err(PrintError::Unrepresentable { .. })
    ));
    let mut query = parser::parse_query("VALUES 1").unwrap();
    let SetExpr::Values(values) = query.body.as_mut() else {
        panic!()
    };
    values.rows.clear();
    assert!(matches!(
        printer::print_query(&query),
        Err(PrintError::Unrepresentable { .. })
    ));
    parameter_query_roundtrip("SELECT 1 LIMIT ALL");
}
