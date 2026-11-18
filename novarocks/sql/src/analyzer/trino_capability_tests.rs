// SPDX-License-Identifier: Apache-2.0

use super::analyze;
use crate::analyze_error::AnalyzeErrorKind;
use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable, SqlViewDefinition};
use arrow::datatypes::DataType;
use novarocks_parser::ast::SyntaxEq;
use novarocks_parser::trino_parser::parse_query;
use novarocks_type_contract::FunctionValueType;
use novarocks_user_error::ErrorPhase;

struct NoCatalog;

impl PlannerTableProvider for NoCatalog {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        _: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        panic!("a capability rejection must not require a table lookup")
    }

    fn resolve_view_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        _: &str,
    ) -> Result<Option<&SqlViewDefinition>, crate::analyze_error::AnalyzeError> {
        panic!("a capability rejection must not require a view lookup")
    }
}

fn assert_analyzer_capability(source: &str, owner: &str, reason: &str) {
    let query = parse_query(source).unwrap_or_else(|error| panic!("{owner}: {source}: {error:?}"));
    let error = match analyze(&query, &NoCatalog, "default") {
        Ok(_) => panic!("{owner}: unsupported query reached analysis success: {source}"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        AnalyzeErrorKind::UnsupportedExpression,
        "{owner}: {source}: {error}",
    );
    assert!(
        error.message().contains(reason),
        "{owner}: {source}: expected {reason:?}, got {error}",
    );

    let span = error
        .span()
        .expect("the analyzer keeps the rejecting AST node's source range");
    assert!(
        source
            .get(span.start()..span.end())
            .is_some_and(|text| !text.trim().is_empty()),
        "{owner}: {source}: {span:?}",
    );
    let user_error = error.to_user_error(Some(source));
    assert_eq!(user_error.phase(), ErrorPhase::Analyze);
    assert_eq!(
        user_error.code().as_str(),
        "sql.analyze.unsupported_expression",
    );
    assert_eq!(
        user_error.location(),
        Some(&span.to_user_error_location(source)),
    );
}

fn reason(gate: &str) -> &str {
    match gate {
        "array_wildcard" => "array wildcard subscripts",
        "explicit_array_type" => "explicit ARRAY lengths",
        "function_processing_mode" => "function processing modes",
        "interval_literal" => "INTERVAL literals",
        "interval_type" => "INTERVAL data types",
        "json_constructor" => "SQL/JSON constructor",
        "json_query_profile" => "JSON_QUERY only supports its plain two-argument profile",
        "json_table" => "JSON_TABLE is not supported",
        "match" => "MATCH predicates",
        "overlaps" => "OVERLAPS predicates",
        "special_root" => "expression profile is not supported",
        "standalone_lambda" => {
            "lambda expressions are only allowed inside higher-order function calls"
        }
        "unique" => "UNIQUE subquery predicates",
        "window_operation" => "window operations",
        _ => panic!("unknown grammar coverage gate: {gate}"),
    }
}

fn assert_row_field_dereference(source: &str) {
    let query = parse_query(source).unwrap();
    let delimited = parse_query("VALUES ? . \"x\"").unwrap();
    assert!(query.syntax_eq(&delimited), "{source}");

    // Supply a concrete ROW in place of the grammar sample's unbound parameter.
    // Both spellings must resolve the same existing field binding.
    for sql in [
        "VALUES (CAST(NULL AS ROW(x INTEGER))) . 'x'",
        "VALUES (CAST(NULL AS ROW(x INTEGER))) . \"x\"",
    ] {
        let query = parse_query(sql).unwrap();
        let (resolved, _, _) =
            analyze(&query, &NoCatalog, "default").unwrap_or_else(|error| panic!("{sql}: {error}"));
        let [field] = resolved.output_columns.as_slice() else {
            panic!("expected one resolved ROW field: {sql}");
        };
        assert_eq!(
            field.value_type,
            FunctionValueType::new(DataType::Int32, true),
            "{sql}"
        );
    }
}

#[test]
fn parsed_trino_queries_reach_their_declared_analyzer_capability_gate() {
    let mut covered = std::collections::BTreeSet::new();
    for line in include_str!("trino_capability_cases.tsv").lines().skip(1) {
        let columns = line.split('\t').collect::<Vec<_>>();
        let [row, rule, _, _, _, source, gate] = columns.as_slice() else {
            panic!("invalid grammar coverage row: {line}");
        };
        assert!(covered.insert(*row), "duplicate grammar coverage row {row}");
        if *gate == "row_field_dereference" {
            assert_row_field_dereference(source);
        } else {
            assert_analyzer_capability(
                source,
                &format!("grammar row {row} ({rule})"),
                reason(gate),
            );
        }
    }
    assert_eq!(covered.len(), 186);
}

#[test]
fn values_inputs_expose_the_nested_pattern_and_set_operation_gates() {
    // The canonical pattern samples use a window operation, which has a
    // separate outer gate. A plain function reaches the actual frame gate.
    for (source, reason) in [
        (
            "VALUES sum(1) OVER (ROWS CURRENT ROW PATTERN (A))",
            "pattern recognition in window frames is not supported",
        ),
        (
            "VALUES 1 UNION CORRESPONDING BY (x) VALUES 2",
            "CORRESPONDING set operations are not supported",
        ),
        (
            "SELECT * FROM (VALUES 1) AS facts(x) MATCH_RECOGNIZE (PATTERN (A) DEFINE A AS TRUE)",
            "relation profile is not supported",
        ),
    ] {
        assert_analyzer_capability(source, "table-free capability witness", reason);
    }
}

#[test]
fn adjacent_values_with_supported_type_and_plain_window_still_analyze() {
    for source in [
        "VALUES CAST(NULL AS ARRAY(INTEGER))",
        "SELECT SUM(x) OVER (ROWS CURRENT ROW) FROM (VALUES 1) AS facts(x)",
    ] {
        let query = parse_query(source).unwrap();
        assert!(analyze(&query, &NoCatalog, "default").is_ok(), "{source}");
    }
}

#[test]
fn canonical_default_json_query_examples_retain_the_plain_profile() {
    use novarocks_parser::ast::{Expr, SetExpr};

    for source in [
        "VALUES JSON_QUERY ( ? , 'x' )",
        "VALUES JSON_QUERY ( ? , 'x' WITHOUT ARRAY WRAPPER )",
        "VALUES JSON_QUERY ( ? , 'x' WITHOUT WRAPPER )",
        "VALUES JSON_QUERY ( ? , 'x' NULL ON EMPTY )",
    ] {
        let query = parse_query(source).unwrap();
        let SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES: {source}");
        };
        let Expr::JsonQuery(expression) = &values.rows[0] else {
            panic!("expected JSON_QUERY: {source}");
        };
        assert!(super::is_plain_json_query(expression), "{source}");
    }
}
