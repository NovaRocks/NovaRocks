// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use novarocks_parser::ast::*;
use novarocks_parser::{trino_parser, trino_printer};

fn query(source: &str) -> Query {
    trino_parser::parse_query(source).expect(source)
}

fn select(query: &Query) -> &Select {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected SELECT")
    };
    select
}

fn recognition(query: &Query) -> &PatternRecognitionRelation {
    let mut relation = select(query).from.as_ref().unwrap();
    loop {
        match relation {
            Relation::PatternRecognition(pattern) => return pattern,
            Relation::Aliased(alias) => relation = &alias.relation,
            Relation::Sampled(sample) => relation = &sample.relation,
            _ => panic!("expected MATCH_RECOGNIZE"),
        }
    }
}

fn pattern_query(pattern: &str) -> Query {
    query(&format!(
        "SELECT * FROM t MATCH_RECOGNIZE (PATTERN ({pattern}) DEFINE A AS TRUE)"
    ))
}

fn parameter(expression: &Expr) -> usize {
    let Expr::Parameter(value) = expression else {
        panic!("expected parameter")
    };
    value.position
}

fn first_expression(query: &Query) -> &Expr {
    let SelectItem::SingleColumn(column) = &select(query).projection[0] else {
        panic!()
    };
    &column.expression
}

#[test]
fn trino_pattern_binary_contexts_and_grouping_are_preserved() {
    let parsed = pattern_query("A B C | D | E");
    let RowPattern::Alternation { patterns, .. } = &recognition(&parsed).pattern else {
        panic!()
    };
    assert_eq!(patterns.len(), 2);
    let RowPattern::Alternation { patterns: left, .. } = &patterns[0] else {
        panic!()
    };
    let RowPattern::Concatenation {
        patterns: sequence, ..
    } = &left[0]
    else {
        panic!()
    };
    assert_eq!(sequence.len(), 2);
    assert!(
        matches!(&sequence[0], RowPattern::Concatenation { patterns, .. } if patterns.len()==2)
    );
    let grouped = pattern_query("A (B C)");
    let RowPattern::Concatenation { patterns, .. } = &recognition(&grouped).pattern else {
        panic!()
    };
    assert!(matches!(patterns[0], RowPattern::Variable { .. }));
    assert!(matches!(patterns[1], RowPattern::Concatenation { .. }));
    let left = pattern_query("(A B) C");
    let plain = pattern_query("A B C");
    assert!(
        recognition(&left)
            .pattern
            .syntax_eq(&recognition(&plain).pattern)
    );
}

#[test]
fn trino_pattern_primary_forms_keep_actual_source_spans() {
    let empty = pattern_query("()");
    assert!(matches!(
        recognition(&empty).pattern,
        RowPattern::Empty { .. }
    ));
    let permutation = pattern_query("PERMUTE(A)");
    assert!(
        matches!(&recognition(&permutation).pattern, RowPattern::Permutation { patterns, .. } if patterns.len()==1)
    );
    let anchors = pattern_query("^ {- A -} $");
    let RowPattern::Concatenation { patterns, .. } = &recognition(&anchors).pattern else {
        panic!()
    };
    assert!(matches!(
        patterns[1],
        RowPattern::Anchor {
            kind: PatternAnchorKind::PartitionEnd,
            ..
        }
    ));
    let RowPattern::Concatenation {
        patterns: first, ..
    } = &patterns[0]
    else {
        panic!()
    };
    assert!(matches!(
        first[0],
        RowPattern::Anchor {
            kind: PatternAnchorKind::PartitionStart,
            ..
        }
    ));
    assert!(matches!(first[1], RowPattern::Excluded { .. }));

    let source = "SELECT * FROM (t) MATCH_RECOGNIZE (PATTERN (\"文?\") DEFINE A AS TRUE) AS r(x)";
    let parsed = query(source);
    let node = recognition(&parsed);
    assert_eq!(node.span.start(), source.find("(t)").unwrap());
    assert!(node.input.span().start() > node.span.start());
    assert_eq!(node.span.end(), source.find(" AS r(x)").unwrap());
    let RowPattern::Variable { name, span } = &node.pattern else {
        panic!()
    };
    assert!(name.quoted);
    assert_eq!(name.value, "文?");
    assert_eq!(&source[span.start()..span.end()], "\"文?\"");
    let Relation::Aliased(alias) = select(&parsed).from.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(alias.span.end(), source.len());
    assert_eq!(alias.column_names.as_ref().unwrap()[0].value, "x");
}

#[test]
fn trino_pattern_quantifiers_preserve_spelling_bounds_and_greediness() {
    for (source, kind, greedy) in [
        ("A*", 0, true),
        ("A*?", 0, false),
        ("A+", 1, true),
        ("A+?", 1, false),
        ("A?", 2, true),
        ("A??", 2, false),
    ] {
        let parsed = pattern_query(source);
        let RowPattern::Quantified { quantifier, .. } = &recognition(&parsed).pattern else {
            panic!()
        };
        let actual = match quantifier {
            PatternQuantifier::ZeroOrMore { greedy, .. } => (0, *greedy),
            PatternQuantifier::OneOrMore { greedy, .. } => (1, *greedy),
            PatternQuantifier::ZeroOrOne { greedy, .. } => (2, *greedy),
            _ => panic!(),
        };
        assert_eq!(actual, (kind, greedy), "{source}");
    }
    for (source, lower, upper, greedy) in [
        ("A{01}", Some(("01", 1)), Some(("01", 1)), true),
        ("A{1,}", Some(("1", 1)), None, true),
        ("A{,2}?", None, Some(("2", 2)), false),
        ("A{,}", None, None, true),
        ("A{2,1}", Some(("2", 2)), Some(("1", 1)), true),
        ("A{0xA,0xF}", Some(("0xA", 10)), Some(("0xF", 15)), true),
    ] {
        let parsed = pattern_query(source);
        let RowPattern::Quantified {
            quantifier:
                PatternQuantifier::Range {
                    greedy: actual_greedy,
                    at_least,
                    at_most,
                    ..
                },
            ..
        } = &recognition(&parsed).pattern
        else {
            panic!()
        };
        assert_eq!(
            at_least
                .as_ref()
                .map(|x| (x.lexeme.as_str(), x.parsed_value)),
            lower
        );
        assert_eq!(
            at_most
                .as_ref()
                .map(|x| (x.lexeme.as_str(), x.parsed_value)),
            upper
        );
        assert_eq!(*actual_greedy, greedy);
        if source == "A{01}" {
            assert_eq!(
                at_least.as_ref().unwrap().span,
                at_most.as_ref().unwrap().span
            );
        }
    }
}

#[test]
fn trino_pattern_recognition_defaults_and_rows_per_match_are_exact() {
    for (clause, rows) in [
        ("", None),
        ("ONE ROW PER MATCH", Some(RowsPerMatch::One)),
        ("ALL ROWS PER MATCH", Some(RowsPerMatch::AllShowEmpty)),
        (
            "ALL ROWS PER MATCH SHOW EMPTY MATCHES",
            Some(RowsPerMatch::AllShowEmpty),
        ),
        (
            "ALL ROWS PER MATCH OMIT EMPTY MATCHES",
            Some(RowsPerMatch::AllOmitEmpty),
        ),
        (
            "ALL ROWS PER MATCH WITH UNMATCHED ROWS",
            Some(RowsPerMatch::AllWithUnmatched),
        ),
    ] {
        let parsed = query(&format!(
            "SELECT * FROM t MATCH_RECOGNIZE ({clause} PATTERN (A) DEFINE A AS TRUE)"
        ));
        let node = recognition(&parsed);
        assert_eq!(node.rows_per_match, rows);
        assert!(node.order_by.is_none());
        assert!(node.partition_by.is_empty());
        assert!(node.measures.is_empty());
        assert!(node.after_match_skip_to.is_none());
        assert!(node.pattern_search_mode.is_none());
        assert!(node.subsets.is_empty());
        assert_eq!(node.variable_definitions.len(), 1);
    }
}

#[test]
fn trino_pattern_skip_to_variants_and_keyword_identifier_remain_distinct() {
    for (clause, expected) in [
        ("SKIP PAST LAST ROW", 0),
        ("SKIP TO NEXT ROW", 1),
        ("SKIP TO FIRST A", 2),
        ("SKIP TO LAST A", 3),
        ("SKIP TO A", 3),
    ] {
        let parsed = query(&format!(
            "SELECT * FROM t MATCH_RECOGNIZE (AFTER MATCH {clause} PATTERN (A) DEFINE A AS TRUE)"
        ));
        let actual = match recognition(&parsed).after_match_skip_to.as_ref().unwrap() {
            SkipTo::PastLast { .. } => 0,
            SkipTo::Next { .. } => 1,
            SkipTo::First { .. } => 2,
            SkipTo::Last { .. } => 3,
        };
        assert_eq!(actual, expected, "{clause}");
    }
    for name in ["FIRST", "LAST", "NEXT"] {
        let parsed = query(&format!(
            "SELECT * FROM t MATCH_RECOGNIZE (AFTER MATCH SKIP TO {name} PATTERN(A) DEFINE A AS TRUE)"
        ));
        let SkipTo::Last { identifier, .. } =
            recognition(&parsed).after_match_skip_to.as_ref().unwrap()
        else {
            panic!()
        };
        assert_eq!(identifier.value, name);
    }
}

#[test]
fn trino_pattern_parameter_visits_order_before_partition_and_ignore_quantifier_marks() {
    let parsed = query(
        "SELECT ? FROM (SELECT ?) t MATCH_RECOGNIZE (PARTITION BY ? ORDER BY ? MEASURES ? AS m PATTERN (A?? B*? C{,}?) DEFINE A AS ?) WHERE ?",
    );
    assert_eq!(parameter(first_expression(&parsed)), 0);
    let node = recognition(&parsed);
    let Relation::Aliased(input) = node.input.as_ref() else {
        panic!()
    };
    let Relation::TableSubquery(input) = input.relation.as_ref() else {
        panic!()
    };
    assert_eq!(parameter(first_expression(&input.query)), 1);
    assert_eq!(
        parameter(&node.order_by.as_ref().unwrap().items[0].sort_key),
        2
    );
    assert_eq!(parameter(&node.partition_by[0]), 3);
    assert_eq!(parameter(&node.measures[0].expression), 4);
    assert_eq!(parameter(&node.variable_definitions[0].expression), 5);
    assert_eq!(parameter(select(&parsed).selection.as_ref().unwrap()), 6);
}

#[test]
fn trino_pattern_window_frame_retains_all_six_pattern_facts_and_visit_order() {
    let parsed = query(
        "SELECT sum(?) OVER (PARTITION BY ? ORDER BY ? MEASURES ? AS m ROWS BETWEEN ? PRECEDING AND ? FOLLOWING EXCLUDE CURRENT ROW AFTER MATCH SKIP TO A INITIAL PATTERN(A??) SUBSET S=(A) DEFINE A AS ?)",
    );
    let Expr::FunctionCall(call) = first_expression(&parsed) else {
        panic!()
    };
    assert_eq!(parameter(&call.arguments[0].value), 6);
    let Window::Specification(window) = call.window.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!(parameter(&window.partition_by[0]), 0);
    assert_eq!(
        parameter(&window.order_by.as_ref().unwrap().items[0].sort_key),
        1
    );
    let frame = window.window_frame.as_ref().unwrap();
    assert_eq!(frame.units, WindowFrameUnits::Rows);
    let WindowFrameBound::Preceding(Some(start), _) = &frame.start_bound else {
        panic!()
    };
    let WindowFrameBound::Following(Some(end), _) = frame.end_bound.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!((parameter(start), parameter(end)), (2, 3));
    assert_eq!(parameter(&frame.measures[0].expression), 4);
    assert_eq!(frame.exclusion, WindowFrameExclusion::CurrentRow);
    assert!(
        matches!(&frame.after_match_skip_to, Some(SkipTo::Last { identifier, .. }) if identifier.value=="A")
    );
    assert_eq!(
        frame.pattern_search_mode.as_ref().unwrap().mode,
        PatternSearchModeKind::Initial
    );
    assert!(frame.pattern.is_some());
    assert_eq!(frame.subsets[0].name.value, "S");
    assert_eq!(frame.subsets[0].identifiers[0].value, "A");
    assert_eq!(parameter(&frame.variable_definitions[0].expression), 5);
    let plain = query("SELECT sum(1) OVER (ROWS CURRENT ROW)");
    let Expr::FunctionCall(call) = first_expression(&plain) else {
        panic!()
    };
    let Window::Specification(window) = call.window.as_ref().unwrap() else {
        panic!()
    };
    let frame = window.window_frame.as_ref().unwrap();
    assert!(
        frame.measures.is_empty()
            && frame.subsets.is_empty()
            && frame.variable_definitions.is_empty()
    );
    assert!(
        frame.after_match_skip_to.is_none()
            && frame.pattern_search_mode.is_none()
            && frame.pattern.is_none()
    );
    assert_eq!(frame.exclusion, WindowFrameExclusion::NoOthers);
}

#[test]
fn trino_pattern_grammar_and_constructor_failures_remain_errors() {
    for pattern in [
        "",
        "A|",
        "PERMUTE()",
        "A{}",
        "A{1,,2}",
        "A***",
        "A{9223372036854775808}",
    ] {
        let source =
            format!("SELECT * FROM t MATCH_RECOGNIZE (PATTERN ({pattern}) DEFINE A AS TRUE)");
        assert!(trino_parser::parse_query(&source).is_err(), "{source}");
    }
    for source in [
        "SELECT * FROM t MATCH_RECOGNIZE (PATTERN(A))",
        "SELECT * FROM t MATCH_RECOGNIZE (PATTERN(A) DEFINE)",
        "SELECT * FROM t MATCH_RECOGNIZE (PATTERN(A) SUBSET S=() DEFINE A AS TRUE)",
    ] {
        assert!(trino_parser::parse_query(source).is_err(), "{source}");
    }
}

#[test]
fn trino_pattern_related_coverage_rows_use_the_actual_new_query_entry() {
    let rules = [
        "patternRecognition",
        "measureDefinition",
        "rowsPerMatch",
        "emptyMatchHandling",
        "skipTo",
        "subsetDefinition",
        "variableDefinition",
        "windowFrame",
        "frameExtent",
        "frameExclusion",
        "frameBound",
        "rowPattern",
        "patternPrimary",
        "patternQuantifier",
    ];
    let mut count = 0;
    for (index, line) in include_str!("grammar-coverage.tsv").lines().enumerate() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let columns: Vec<_> = line.split('\t').collect();
        if columns.len() != 10 || !rules.contains(&columns[1]) || columns[7] != "accept" {
            continue;
        }
        assert!(
            !columns[5].is_empty(),
            "coverage line {} has no actual source",
            index + 1
        );
        assert!(
            trino_parser::parse_query(&super::coverage::sample(columns[5])).is_ok(),
            "coverage line {}: {}",
            index + 1,
            columns[5]
        );
        count += 1;
    }
    assert!(count > 0);
}

#[test]
fn trino_pattern_deep_parser_and_owning_cycles_use_the_ordinary_test_thread() {
    let grouped = format!("{}A{}", "(".repeat(2000), ")".repeat(2000));
    let parsed = pattern_query(&grouped);
    assert!(matches!(
        recognition(&parsed).pattern,
        RowPattern::Variable { .. }
    ));
    drop(parsed);
    let excluded = format!("{}A{}", "{-".repeat(2000), "-}".repeat(2000));
    let parsed = pattern_query(&excluded);
    struct Counter(usize);
    impl Visit for Counter {
        fn visit_row_pattern(&mut self, pattern: &RowPattern) {
            self.0 += 1;
            walk_row_pattern(self, pattern);
        }
    }
    let mut counter = Counter(0);
    counter.visit_query(&parsed);
    assert_eq!(counter.0, 2001);
    let copied = parsed.clone();
    assert!(syntax_eq_query(&parsed, &copied));
    drop(copied);
    drop(parsed);
}

#[test]
fn trino_pattern_new_printer_roundtrips_every_retained_fact() {
    for source in [
        "SELECT * FROM t MATCH_RECOGNIZE (PATTERN (A (B C)|PERMUTE(D,())) DEFINE A AS TRUE)",
        "SELECT * FROM t MATCH_RECOGNIZE (ALL ROWS PER MATCH WITH UNMATCHED ROWS AFTER MATCH SKIP TO FIRST A SEEK PATTERN(^ {- A*? -} B{,} $) SUBSET S=(A,B) DEFINE A AS ?, B AS ?) AS r(x)",
        "SELECT sum(?) OVER (MEASURES ? AS m ROWS BETWEEN ? PRECEDING AND CURRENT ROW EXCLUDE TIES AFTER MATCH SKIP PAST LAST ROW PATTERN(A??) DEFINE A AS ?)",
    ] {
        let parsed = query(source);
        let rendered = trino_printer::print_query(&parsed);
        let reparsed = query(&rendered);
        assert!(syntax_eq_query(&parsed, &reparsed), "{source}\n{rendered}");
    }
}
