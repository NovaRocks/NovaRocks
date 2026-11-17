// SPDX-License-Identifier: Apache-2.0
use novarocks_parser::ast::SyntaxEq;
use novarocks_parser::{trino_parser, trino_printer};

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
        let original = trino_parser::parse_expression(source).unwrap();
        let printed = trino_printer::print_expression(&original);
        let reparsed = trino_parser::parse_expression(&printed).unwrap();
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
        let original = trino_parser::parse_type(source).unwrap();
        let printed = trino_printer::print_type(&original);
        let reparsed = trino_parser::parse_type(&printed).unwrap();
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
        let original = trino_parser::parse_expression(source).unwrap();
        let printed = trino_printer::print_expression(&original);
        let reparsed = trino_parser::parse_expression(&printed).unwrap();
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
        let original = trino_parser::parse_query(source).unwrap();
        let printed = trino_printer::print_query(&original);
        let reparsed = trino_parser::parse_query(&printed).unwrap();
        assert!(original.syntax_eq(&reparsed), "{source:?} -> {printed:?}");
    }
    let absent = trino_parser::parse_query("SELECT 1 FETCH FIRST ROWS ONLY").unwrap();
    let one = trino_parser::parse_query("SELECT 1 FETCH FIRST 1 ROWS ONLY").unwrap();
    assert!(!absent.syntax_eq(&one));
    for value in [absent, one] {
        let printed = trino_printer::print_query(&value);
        let reparsed = trino_parser::parse_query(&printed).unwrap();
        assert!(value.syntax_eq(&reparsed), "{printed}");
    }
}
