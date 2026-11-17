// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use novarocks_parser::ast::*;
use novarocks_parser::{trino_parser, trino_printer};

fn query(source: &str) -> Query {
    trino_parser::parse_query(source).expect(source)
}

fn roundtrip(source: &str) -> (Query, String) {
    let original = query(source);
    let printed = trino_printer::print_query(&original).unwrap();
    let reparsed = query(&printed);
    assert!(syntax_eq_query(&original, &reparsed), "{source}\n{printed}");
    (reparsed, printed)
}

fn select(query: &Query) -> &Select {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected SELECT")
    };
    select
}

fn recognition(query: &Query) -> &PatternRecognitionRelation {
    let Relation::PatternRecognition(pattern) = select(query).from.as_ref().unwrap() else {
        panic!("expected recognition")
    };
    pattern
}

fn recognition_mut(query: &mut Query) -> &mut PatternRecognitionRelation {
    let SetExpr::Select(select) = query.body.as_mut() else {
        panic!("expected SELECT")
    };
    let Relation::PatternRecognition(pattern) = select.from.as_mut().unwrap() else {
        panic!("expected recognition")
    };
    pattern
}

fn parameter(expression: &Expr) -> usize {
    let Expr::Parameter(value) = expression else {
        panic!("expected parameter")
    };
    value.position
}

#[test]
fn printer_pattern_preserves_binary_grouping_and_all_primary_kinds() {
    for pattern in [
        "A B C | D | E",
        "A (B C)",
        "(A B) C",
        "A | (B | C)",
        "(A | B) | C",
        "PERMUTE(A)",
        "PERMUTE(A B,C | D)",
        "()",
        "^ A $",
        "{- (A | {- B -}) C -}",
        "\"a\"\"b\" \"space label\"",
    ] {
        roundtrip(&format!(
            "SELECT * FROM t MATCH_RECOGNIZE(PATTERN({pattern}) DEFINE A AS TRUE)"
        ));
    }
    let (parsed, _) =
        roundtrip("SELECT * FROM t MATCH_RECOGNIZE(PATTERN(A | (B | C)) DEFINE A AS TRUE)");
    let RowPattern::Alternation { patterns, .. } = &recognition(&parsed).pattern else {
        panic!()
    };
    assert_eq!(patterns.len(), 2);
    assert!(matches!(patterns[0], RowPattern::Variable { .. }));
    assert!(matches!(patterns[1], RowPattern::Alternation { .. }));
}

#[test]
fn printer_pattern_preserves_quantifier_spelling_none_bounds_and_greediness() {
    for pattern in [
        "A*",
        "A*?",
        "A+",
        "A+?",
        "A?",
        "A??",
        "A{01}",
        "A{0xA,0xF}?",
        "A{0b1,0o7}",
        "A{1_0,20}",
        "A{1,}",
        "A{,2}?",
        "A{,}",
        "A{2,1}",
        "(A*)+",
    ] {
        roundtrip(&format!(
            "SELECT * FROM t MATCH_RECOGNIZE(PATTERN({pattern}) DEFINE A AS TRUE)"
        ));
    }
    let (parsed, printed) =
        roundtrip("SELECT * FROM t MATCH_RECOGNIZE(PATTERN(A{01,0xF}?) DEFINE A AS TRUE)");
    assert!(printed.contains("{01,0xF}?"));
    let RowPattern::Quantified {
        quantifier:
            PatternQuantifier::Range {
                at_least,
                at_most,
                greedy,
                ..
            },
        ..
    } = &recognition(&parsed).pattern
    else {
        panic!()
    };
    assert!(!greedy);
    assert_eq!(at_least.as_ref().unwrap().lexeme, "01");
    assert_eq!(at_least.as_ref().unwrap().parsed_value, 1);
    assert_eq!(at_most.as_ref().unwrap().lexeme, "0xF");
    assert_eq!(at_most.as_ref().unwrap().parsed_value, 15);
}

#[test]
fn printer_pattern_relation_preserves_none_and_all_rows_variants() {
    for (clause, expected) in [
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
        let (parsed, printed) = roundtrip(&format!(
            "SELECT * FROM t MATCH_RECOGNIZE({clause} PATTERN(A) DEFINE A AS TRUE)"
        ));
        assert_eq!(recognition(&parsed).rows_per_match, expected);
        if expected.is_none() {
            assert!(!printed.contains("ROW PER MATCH"));
            assert!(!printed.contains("ROWS PER MATCH"));
        }
    }
}

#[test]
fn printer_pattern_skip_search_subsets_and_quoted_labels_are_explicit() {
    for skip in [
        "SKIP PAST LAST ROW",
        "SKIP TO NEXT ROW",
        "SKIP TO FIRST A",
        "SKIP TO LAST A",
        "SKIP TO A",
        "SKIP TO LAST \"PATTERN\"",
    ] {
        for mode in ["", "INITIAL", "SEEK"] {
            roundtrip(&format!(
                "SELECT * FROM t MATCH_RECOGNIZE(AFTER MATCH {skip} {mode} PATTERN(A) SUBSET \"s\"=(A,\"b\"),T=(A) DEFINE A AS TRUE,\"b\" AS FALSE)"
            ));
        }
    }
}

#[test]
fn printer_pattern_relation_parameters_keep_builder_order_without_quantifier_events() {
    let (parsed, printed) = roundtrip(
        "SELECT * FROM (SELECT ?) AS t(x) MATCH_RECOGNIZE(PARTITION BY ? ORDER BY ? MEASURES ? AS m AFTER MATCH SKIP TO A SEEK PATTERN(A?? B{,}?) SUBSET S=(A,B) DEFINE A AS ?,B AS ?) AS out(x,y)",
    );
    let Relation::Aliased(outer) = select(&parsed).from.as_ref().unwrap() else {
        panic!()
    };
    let Relation::PatternRecognition(pattern) = outer.relation.as_ref() else {
        panic!()
    };
    let Relation::Aliased(input) = pattern.input.as_ref() else {
        panic!()
    };
    let Relation::TableSubquery(input) = input.relation.as_ref() else {
        panic!()
    };
    let SelectItem::SingleColumn(column) = &select(&input.query).projection[0] else {
        panic!()
    };
    assert_eq!(parameter(&column.expression), 0);
    assert_eq!(
        parameter(&pattern.order_by.as_ref().unwrap().items[0].sort_key),
        1
    );
    assert_eq!(parameter(&pattern.partition_by[0]), 2);
    assert_eq!(parameter(&pattern.measures[0].expression), 3);
    assert_eq!(parameter(&pattern.variable_definitions[0].expression), 4);
    assert_eq!(parameter(&pattern.variable_definitions[1].expression), 5);
    assert!(printed.contains("??") && printed.contains("{,}?"));
    struct Count(usize);
    impl Visit for Count {
        fn visit_parameter(&mut self, _: &Parameter) {
            self.0 += 1;
        }
    }
    let mut count = Count(0);
    count.visit_query(&parsed);
    assert_eq!(count.0, 6);
}

#[test]
fn printer_pattern_window_reuses_extent_exclusion_and_preserves_all_six_facts() {
    let (parsed, _) = roundtrip(
        "SELECT sum(?) OVER(PARTITION BY ? ORDER BY ? MEASURES ? AS m ROWS BETWEEN ? PRECEDING AND ? FOLLOWING EXCLUDE CURRENT ROW AFTER MATCH SKIP TO FIRST A INITIAL PATTERN(A??) SUBSET S=(A) DEFINE A AS ?)",
    );
    let SelectItem::SingleColumn(column) = &select(&parsed).projection[0] else {
        panic!()
    };
    let Expr::FunctionCall(call) = &column.expression else {
        panic!()
    };
    assert_eq!(parameter(&call.arguments[0].value), 6);
    let Window::Specification(window) = call.window.as_ref().unwrap() else {
        panic!()
    };
    let frame = window.window_frame.as_ref().unwrap();
    assert_eq!(frame.exclusion, WindowFrameExclusion::CurrentRow);
    assert_eq!(frame.units, WindowFrameUnits::Rows);
    let WindowFrameBound::Preceding(Some(start), _) = &frame.start_bound else {
        panic!()
    };
    let WindowFrameBound::Following(Some(end), _) = frame.end_bound.as_ref().unwrap() else {
        panic!()
    };
    assert_eq!((parameter(start), parameter(end)), (2, 3));
    assert_eq!(parameter(&frame.measures[0].expression), 4);
    assert_eq!(parameter(&frame.variable_definitions[0].expression), 5);
    assert!(
        frame.after_match_skip_to.is_some()
            && frame.pattern_search_mode.is_some()
            && frame.pattern.is_some()
    );
    assert_eq!(frame.subsets[0].name.value, "S");
    for extent in [
        "ROWS CURRENT ROW",
        "RANGE UNBOUNDED PRECEDING",
        "GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
    ] {
        for exclusion in [
            "",
            "EXCLUDE CURRENT ROW",
            "EXCLUDE GROUP",
            "EXCLUDE TIES",
            "EXCLUDE NO OTHERS",
        ] {
            roundtrip(&format!(
                "SELECT m OVER({extent} {exclusion} AFTER MATCH SKIP TO A SEEK PATTERN(A) DEFINE A AS TRUE)"
            ));
        }
    }
}

#[test]
fn printer_pattern_window_optional_facts_remain_independently_optional() {
    for tail in [
        "",
        "AFTER MATCH SKIP TO A",
        "INITIAL",
        "PATTERN(A)",
        "SUBSET S=(A)",
        "DEFINE A AS TRUE",
        "MEASURES 1 AS m",
    ] {
        let source = if tail.starts_with("MEASURES") {
            format!("SELECT m OVER({tail} ROWS CURRENT ROW)")
        } else {
            format!("SELECT m OVER(ROWS CURRENT ROW {tail})")
        };
        roundtrip(&source);
    }
    let (parsed, _) = roundtrip("SELECT m OVER(ROWS CURRENT ROW)");
    let SelectItem::SingleColumn(column) = &select(&parsed).projection[0] else {
        panic!()
    };
    let Expr::WindowOperation(operation) = &column.expression else {
        panic!()
    };
    let Window::Specification(window) = &operation.window else {
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
    assert!(frame.end_bound.is_none());
}

#[test]
fn printer_pattern_relation_input_grouping_preserves_real_nested_nodes() {
    for source in [
        "SELECT * FROM (t INNER JOIN u ON TRUE) MATCH_RECOGNIZE(PATTERN(A) DEFINE A AS TRUE)",
        "SELECT * FROM (t TABLESAMPLE SYSTEM(5)) MATCH_RECOGNIZE(PATTERN(A) DEFINE A AS TRUE)",
        "SELECT * FROM (t MATCH_RECOGNIZE(PATTERN(A) DEFINE A AS TRUE)) MATCH_RECOGNIZE(PATTERN(B) DEFINE B AS TRUE)",
        "SELECT * FROM ((SELECT 1) AS t(x)) MATCH_RECOGNIZE(PATTERN(A) DEFINE A AS TRUE)",
    ] {
        roundtrip(source);
    }
}

#[test]
fn printer_pattern_refuses_non_binary_typed_trees_without_flattening() {
    let mut parsed = query("SELECT * FROM t MATCH_RECOGNIZE(PATTERN(A B) DEFINE A AS TRUE)");
    let pattern = &mut recognition_mut(&mut parsed).pattern;
    let RowPattern::Concatenation { patterns, .. } = pattern else {
        panic!()
    };
    patterns.push(RowPattern::Empty {
        span: novarocks_parser::SYNTHETIC,
    });
    assert!(matches!(
        trino_printer::print_query(&parsed),
        Err(novarocks_parser::trino_printer::PrintError::Unrepresentable { .. })
    ));
}

#[test]
fn printer_pattern_reference_coverage_all_queries_strictly_roundtrip() {
    let rules = [
        "rowPattern",
        "patternPrimary",
        "patternQuantifier",
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
    ];
    let mut count = 0;
    for line in include_str!("grammar-coverage.tsv").lines() {
        let columns = line.split('\t').collect::<Vec<_>>();
        if columns.len() == 10 && columns[7] == "accept" && rules.contains(&columns[1]) {
            roundtrip(&super::coverage::sample(columns[5]));
            count += 1;
        }
    }
    assert!(
        count >= 100,
        "all pattern reference coverage rows must remain present"
    );
}

#[test]
fn printer_pattern_deep_lifecycle_uses_the_ordinary_thread() {
    let pattern = format!("{}A{}", "{- ".repeat(2000), " -}".repeat(2000));
    let source = format!("SELECT * FROM t MATCH_RECOGNIZE(PATTERN({pattern}) DEFINE A AS TRUE)");
    let original = query(&source);
    let printed = trino_printer::print_query(&original).unwrap();
    let reparsed = query(&printed);
    let cloned = reparsed.clone();
    assert!(syntax_eq_query(&original, &reparsed));
    assert!(syntax_eq_query(&reparsed, &cloned));
    struct Count(usize);
    impl Visit for Count {
        fn visit_row_pattern(&mut self, node: &RowPattern) {
            self.0 += 1;
            walk_row_pattern(self, node);
        }
    }
    let mut count = Count(0);
    count.visit_query(&reparsed);
    assert_eq!(count.0, 2001);
    drop(cloned);
    drop(reparsed);
    drop(original);
}
