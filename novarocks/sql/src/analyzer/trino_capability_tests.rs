// SPDX-License-Identifier: Apache-2.0

use super::analyze;
use crate::analyze_error::{AnalyzeError, AnalyzeErrorKind};
use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable, SqlViewDefinition};
use arrow::datatypes::DataType;
use novarocks_parser::Span;
use novarocks_parser::ast::{Query, Relation, SetExpr, SyntaxEq, TableFunctionArgumentValue};
use novarocks_parser::parser::parse_query;
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

fn assert_analyzer_rejection(
    query: &Query,
    source: &str,
    owner: &str,
    kind: AnalyzeErrorKind,
    descriptor: &str,
) -> (AnalyzeError, Span) {
    let error = match analyze(query, &NoCatalog, "default") {
        Ok(_) => panic!("{owner}: query reached analysis success: {source}"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), kind, "{owner}: {source}: {error}");

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
    assert_eq!(user_error.code().as_str(), descriptor);
    assert_eq!(
        user_error.location(),
        Some(&span.to_user_error_location(source)),
    );
    (error, span)
}

fn assert_analyzer_capability(source: &str, owner: &str, reason: &str) {
    let query = parse_query(source).unwrap_or_else(|error| panic!("{owner}: {source}: {error:?}"));
    let (error, _) = assert_analyzer_rejection(
        &query,
        source,
        owner,
        AnalyzeErrorKind::UnsupportedExpression,
        "sql.analyze.unsupported_expression",
    );
    assert!(
        error.message().contains(reason),
        "{owner}: {source}: expected {reason:?}, got {error}",
    );
}

fn reason(gate: &str) -> &str {
    match gate {
        "array_wildcard" => "array wildcard subscripts",
        "explicit_array_type" => "explicit ARRAY lengths",
        "function_processing_mode" => "function processing modes",
        "json_exists" => "JSON_EXISTS is not supported",
        "json_value" => "JSON_VALUE is not supported",
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
fn temporal_sql_reaches_its_selected_types_after_query_preanalysis() {
    use novarocks_type_contract::{
        IntervalField, IntervalQualifier, TemporalPrecision, TemporalType,
    };

    let p12 = TemporalPrecision::PICOSECONDS;
    let interval =
        IntervalQualifier::new(IntervalField::Second, IntervalField::Second, 4, p12).unwrap();
    let cases = [
        (
            "SELECT CAST(TIMESTAMP '2024-01-01 00:00:00 UTC' AS TIMESTAMP(12) WITH TIME ZONE)",
            TemporalType::TimestampWithTimeZone(p12),
        ),
        (
            "SELECT CAST(TIME '12:00:00 +05:30' AS TIME(12) WITH TIME ZONE)",
            TemporalType::TimeWithTimeZone(p12),
        ),
        (
            "SELECT CAST(INTERVAL '1.123456789012' SECOND(2, 12) AS INTERVAL SECOND(4, 12))",
            TemporalType::Interval(interval),
        ),
    ];
    for (source, expected) in cases {
        let query = super::query_prepass::preanalyze(parse_query(source).unwrap()).unwrap();
        let (resolved, _, _) = analyze(&query, &NoCatalog, "default")
            .unwrap_or_else(|error| panic!("{source}: {error}"));
        let [column] = resolved.output_columns.as_slice() else {
            panic!("expected one temporal column for {source}");
        };
        assert_eq!(
            column.value_type.value_type().temporal(),
            Some(expected),
            "{source}"
        );
    }
}

#[test]
fn parsed_trino_queries_reach_their_declared_analyzer_capability_gate() {
    let parser_ledger: Vec<_> = include_str!("../../../parser/tests/it/grammar-coverage.tsv")
        .lines()
        .collect();
    let mut covered = std::collections::BTreeSet::new();
    for line in include_str!("trino_capability_cases.tsv").lines().skip(1) {
        let columns = line.split('\t').collect::<Vec<_>>();
        let [row, rule, alternative, label, clause, source, gate] = columns.as_slice() else {
            panic!("invalid grammar coverage row: {line}");
        };
        let row: usize = row.parse().unwrap();
        assert!(covered.insert(row), "duplicate grammar coverage row {row}");
        let fields: Vec<_> = parser_ledger[row - 1].split('\t').collect();
        assert_eq!(
            &fields[1..6],
            [*rule, *alternative, *label, *clause, *source]
        );
        let owner = format!("grammar row {row} ({rule})");
        match *gate {
            "row_field_dereference" => assert_row_field_dereference(source),
            "interval_literal" => {
                let query = parse_query(source).unwrap();
                assert_analyzer_rejection(
                    &query,
                    source,
                    &owner,
                    AnalyzeErrorKind::InvalidLiteral,
                    "sql.analyze.invalid_literal",
                );
            }
            "interval_type" => assert_interval_type(source),
            "json_constructor" => {
                let input = if source.contains("ENCODING") {
                    "CAST(NULL AS VARBINARY)"
                } else {
                    "CAST(NULL AS VARCHAR)"
                };
                let ty = FunctionValueType::try_with_logical_type(
                    DataType::LargeBinary,
                    true,
                    novarocks_type_contract::ValueLogicalType::Json,
                )
                .unwrap();
                assert_single_analyzed_type(&source.replacen('?', input, 1), ty);
            }
            "json_serialize" => assert_single_analyzed_type(
                &source.replacen('?', "CAST(NULL AS VARCHAR)", 1),
                FunctionValueType::new(DataType::Utf8, true),
            ),
            "interval_value_expression" => {
                let typed = source.replacen('?', "DATE '2000-01-02'", 1).replacen(
                    '?',
                    "DATE '2000-01-01'",
                    1,
                );
                if source.contains("YEAR") || source.contains("MONTH") {
                    assert_analyzer_capability(&typed, &owner, "year-month interval difference");
                } else {
                    assert_interval_type(&typed);
                }
            }
            _ => assert_analyzer_capability(source, &owner, reason(gate)),
        }
    }
    assert_eq!(covered.len(), 186);
}

fn assert_interval_type(source: &str) {
    let query = parse_query(source).unwrap_or_else(|error| panic!("{source}: {error:?}"));
    let (resolved, _, _) =
        analyze(&query, &NoCatalog, "default").unwrap_or_else(|error| panic!("{source}: {error}"));
    let [field] = resolved.output_columns.as_slice() else {
        panic!("{source}: expected one interval output");
    };
    assert!(
        matches!(
            field.value_type.value_type().temporal(),
            Some(novarocks_type_contract::TemporalType::Interval(qualifier)) if !qualifier.is_year_month()
        ),
        "{source}"
    );
}

fn unaliased_select_relation(query: &Query) -> &Relation {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a query with a FROM relation");
    };
    let mut relation = select.from.as_ref().expect("a FROM relation");
    while let Relation::Aliased(aliased) = relation {
        relation = aliased.relation.as_ref();
    }
    relation
}

fn assert_relation_capability(source: &str, owner: &str, gate: &str) {
    let query = parse_query(source).unwrap_or_else(|error| panic!("{owner}: {source}: {error:?}"));
    let (reason, owner_span) = if gate == "corresponding" {
        let SetExpr::SetOperation(operation) = query.body.as_ref() else {
            panic!("{owner}: expected a set operation");
        };
        assert!(operation.corresponding.is_some(), "{owner}");
        (
            "CORRESPONDING set operations are not supported",
            operation.span,
        )
    } else {
        match (gate, unaliased_select_relation(&query)) {
            ("pivot", Relation::Pivot(node)) => ("PIVOT is not supported", node.span),
            ("match_recognize", Relation::PatternRecognition(node)) => {
                ("MATCH_RECOGNIZE is not supported", node.span)
            }
            ("nearest", Relation::Nearest(node)) => ("NEAREST is not supported", node.span),
            ("table_function_argument_profile", Relation::TableFunction(node)) => {
                assert!(
                    !node.copartitioning.is_empty()
                        || node.arguments.iter().any(|argument| {
                            !matches!(argument.value, TableFunctionArgumentValue::Expression(_))
                        }),
                    "{owner}: expected table arguments, descriptors or copartitioning",
                );
                (
                    "table function argument profile is not supported",
                    node.span,
                )
            }
            _ => panic!("{owner}: expected a {gate} syntax node"),
        }
    };
    let (error, actual_span) = assert_analyzer_rejection(
        &query,
        source,
        owner,
        AnalyzeErrorKind::UnsupportedExpression,
        "sql.analyze.unsupported_expression",
    );
    assert_eq!(error.message(), reason, "{owner}");
    assert_eq!(actual_span, owner_span, "{owner}: {source}");
}

fn assert_unknown_table_function(source: &str, owner: &str) {
    let query = parse_query(source).unwrap_or_else(|error| panic!("{owner}: {source}: {error:?}"));
    let Relation::TableFunction(function) = unaliased_select_relation(&query) else {
        panic!("{owner}: expected a table function");
    };
    assert!(function.copartitioning.is_empty(), "{owner}");
    assert!(
        function
            .arguments
            .iter()
            .all(|argument| matches!(argument.value, TableFunctionArgumentValue::Expression(_))),
        "{owner}"
    );
    let (error, actual_span) = assert_analyzer_rejection(
        &query,
        source,
        owner,
        AnalyzeErrorKind::UnknownFunction,
        "sql.analyze.unknown_function",
    );
    assert_eq!(
        error.message(),
        "Table function 'x' not registered",
        "{owner}"
    );
    assert_eq!(actual_span, function.name.span, "{owner}");
}

fn assert_single_analyzed_type(source: &str, value_type: FunctionValueType) {
    let query = parse_query(source).unwrap_or_else(|error| panic!("{source}: {error:?}"));
    let (resolved, _, _) =
        analyze(&query, &NoCatalog, "default").unwrap_or_else(|error| panic!("{source}: {error}"));
    let [field] = resolved.output_columns.as_slice() else {
        panic!("{source}: expected one resolved field");
    };
    assert_eq!(field.value_type, value_type, "{source}");
}

#[test]
fn relation_grammar_cases_use_the_first_actual_table_free_analyzer_owner() {
    let parser_ledger: Vec<_> = include_str!("../../../parser/tests/it/grammar-coverage.tsv")
        .lines()
        .collect();
    let mut covered = std::collections::BTreeSet::new();
    let (mut capability, mut unknown, mut absent) = (0, 0, 0);
    for line in include_str!("trino_relation_capability_cases.tsv")
        .lines()
        .skip(1)
    {
        let columns: Vec<_> = line.split('\t').collect();
        let [row, rule, alternative, label, clause, authored, gate] = columns.as_slice() else {
            panic!("invalid relation grammar coverage row: {line}");
        };
        let row: usize = row.parse().unwrap();
        assert!(covered.insert(row), "duplicate grammar coverage row {row}");
        let fields: Vec<_> = parser_ledger[row - 1].split('\t').collect();
        assert_eq!(
            &fields[1..6],
            [*rule, *alternative, *label, *clause, *authored]
        );
        let source = format!("WITH x(x) AS (VALUES 1) {authored}");
        let owner = format!("grammar row {row} ({rule})");
        match *gate {
            "unknown_table_function" => {
                assert_unknown_table_function(&source, &owner);
                unknown += 1;
            }
            "feature_absent" => {
                assert_single_analyzed_type(&source, FunctionValueType::new(DataType::Int32, true));
                absent += 1;
            }
            gate => {
                assert_relation_capability(&source, &owner, gate);
                capability += 1;
            }
        }
    }
    assert_eq!((capability, unknown, absent), (103, 10, 2));
}

#[test]
fn admitted_table_function_rejects_unimplemented_argument_profiles() {
    for source in [
        "WITH x(x) AS (VALUES 1) SELECT * FROM TABLE (sequence(TABLE(x)))",
        "SELECT * FROM TABLE (sequence(DESCRIPTOR(x)))",
        "SELECT * FROM TABLE (sequence(1, 2 COPARTITION (x, x)))",
    ] {
        assert_relation_capability(
            source,
            "existing table function",
            "table_function_argument_profile",
        );
    }
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
            "MATCH_RECOGNIZE is not supported",
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
