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

use super::coverage::sample;
use novarocks_parser::ast::*;
use novarocks_parser::{SYNTHETIC, trino_parser, trino_printer};

fn roundtrip_function(source: &str) -> FunctionSpecification {
    let original = trino_parser::parse_function_specification(source)
        .unwrap_or_else(|error| panic!("{source:?}: {error:?}"));
    let printed = trino_printer::print_function_specification(&original);
    let reparsed = trino_parser::parse_function_specification(&printed)
        .unwrap_or_else(|error| panic!("{source:?} -> {printed:?}: {error:?}"));
    assert!(
        original.syntax_eq(&reparsed),
        "{source:?} -> {printed:?}\n{original:?}\n{reparsed:?}"
    );
    reparsed
}

#[test]
fn standalone_routine_header_and_all_ordered_characteristics_roundtrip() {
    let source = r#"FUNCTION cat."f"(BIGINT, x ARRAY(BIGINT), "Q" ROW(a BIGINT)) RETURNS BIGINT LANGUAGE SQL LANGUAGE JAVA DETERMINISTIC NOT DETERMINISTIC RETURNS NULL ON NULL INPUT CALLED ON NULL INPUT SECURITY INVOKER SECURITY DEFINER COMMENT 'it''s\raw' WITH (p = DEFAULT, p = NULL) RETURN 7"#;
    let function = roundtrip_function(source);
    assert_eq!(function.name.parts[1].value, "f");
    assert!(function.name.parts[1].quoted);
    assert_eq!(function.parameters.len(), 3);
    assert!(function.parameters[0].name.is_none());
    assert_eq!(function.parameters[1].name.as_ref().unwrap().value, "x");
    assert!(function.parameters[2].name.as_ref().unwrap().quoted);
    assert_eq!(function.characteristics.len(), 10);
    assert!(
        matches!(&function.characteristics[0], RoutineCharacteristic::Language { language, .. } if language.value == "SQL")
    );
    assert!(
        matches!(&function.characteristics[1], RoutineCharacteristic::Language { language, .. } if language.value == "JAVA")
    );
    assert!(
        matches!(&function.characteristics[8], RoutineCharacteristic::Comment { comment, .. } if comment == "it's\\raw")
    );
    let RoutineCharacteristic::Properties { properties, .. } = &function.characteristics[9] else {
        panic!()
    };
    assert_eq!(properties[0].name.value, properties[1].name.value);
    assert!(properties[0].value.is_none());
    assert!(matches!(
        properties[1].value,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        }))
    ));
}

#[test]
fn all_control_statements_and_optional_clauses_roundtrip_without_scope_checks() {
    let source = "FUNCTION f() RETURNS BIGINT BEGIN DECLARE x, \"Y\" BIGINT DEFAULT 3; DECLARE z DOUBLE; SET x = 1; CASE x WHEN 1 THEN RETURN 2; WHEN 2 THEN RETURN 3; ELSE RETURN 4; END CASE; CASE WHEN TRUE THEN RETURN 5; END CASE; IF TRUE THEN RETURN 6; ELSEIF FALSE THEN RETURN 7; ELSE RETURN 8; END IF; l: LOOP ITERATE missing; LEAVE missing; END LOOP; WHILE FALSE DO RETURN 9; END WHILE; r: REPEAT RETURN 10; UNTIL TRUE END REPEAT; RETURN x; END";
    let function = roundtrip_function(source);
    let RoutineBody::Sql(ControlStatement::Compound(body)) = &function.body else {
        panic!()
    };
    assert_eq!(body.variable_declarations.len(), 2);
    assert_eq!(body.variable_declarations[0].names[1].value, "Y");
    assert!(body.variable_declarations[0].names[1].quoted);
    assert!(body.variable_declarations[1].default_value.is_none());
    assert_eq!(body.statements.len(), 8);
    let ControlStatement::Case(simple) = &body.statements[1] else {
        panic!()
    };
    assert!(simple.expression.is_some());
    assert_eq!(simple.when_clauses.len(), 2);
    assert!(simple.else_clause.is_some());
    let ControlStatement::Case(searched) = &body.statements[2] else {
        panic!()
    };
    assert!(searched.expression.is_none());
    assert!(searched.else_clause.is_none());
    let ControlStatement::If(branch) = &body.statements[3] else {
        panic!()
    };
    assert_eq!(branch.else_if_clauses.len(), 1);
    assert!(branch.else_clause.is_some());
    let ControlStatement::Loop(repeated) = &body.statements[4] else {
        panic!()
    };
    assert_eq!(repeated.label.as_ref().unwrap().value, "l");
    let ControlStatement::Iterate(iterate) = &repeated.statements[0] else {
        panic!()
    };
    assert_eq!(iterate.label.value, "missing");
    let ControlStatement::While(conditional) = &body.statements[5] else {
        panic!()
    };
    assert!(conditional.label.is_none());
    for source in [
        "FUNCTION f() RETURNS BIGINT BEGIN END",
        "FUNCTION f() RETURNS BIGINT BEGIN DECLARE x BIGINT; END",
        "FUNCTION f() RETURNS BIGINT BEGIN IF TRUE THEN RETURN 1; END IF; END",
    ] {
        roundtrip_function(source);
    }
}

#[test]
fn query_prefixes_keep_body_first_parameter_numbers_and_function_list_order() {
    let source = "WITH SESSION cat.p = ? WITH FUNCTION f() RETURNS BIGINT WITH (p = ?) BEGIN SET x = ?; RETURN ?; END, FUNCTION g() RETURNS BIGINT RETURN ? WITH c AS (SELECT ?) SELECT ?";
    let original = trino_parser::parse_query(source).unwrap();
    assert_eq!(original.functions.len(), 2);
    assert_eq!(original.functions[0].name.parts[0].value, "f");
    assert_eq!(original.functions[1].name.parts[0].value, "g");
    struct Parameters(Vec<(novarocks_parser::Span, usize)>);
    impl Visit for Parameters {
        fn visit_parameter(&mut self, value: &Parameter) {
            self.0.push((value.span, value.position));
        }
    }
    let mut parameters = Parameters(Vec::new());
    parameters.visit_query(&original);
    parameters.0.sort_by_key(|(span, _)| span.start());
    let offsets = source
        .match_indices('?')
        .map(|(offset, _)| offset)
        .collect::<Vec<_>>();
    assert_eq!(
        parameters
            .0
            .iter()
            .map(|(span, _)| span.start())
            .collect::<Vec<_>>(),
        offsets
    );
    assert_eq!(
        parameters
            .0
            .iter()
            .map(|(_, position)| *position)
            .collect::<Vec<_>>(),
        vec![6, 4, 2, 3, 5, 1, 0]
    );
    let printed = trino_printer::print_query(&original);
    assert!(printed.starts_with("WITH SESSION "));
    let reparsed = trino_parser::parse_query(&printed).unwrap();
    assert!(
        original.syntax_eq(&reparsed),
        "{printed}\n{original:?}\n{reparsed:?}"
    );
}

#[test]
fn routine_property_expression_default_is_distinct_from_the_default_marker() {
    let source = "FUNCTION f() RETURNS BIGINT WITH (p = (DEFAULT), q = DEFAULT, r = NULL) RETURN 1";
    let function = roundtrip_function(source);
    let RoutineCharacteristic::Properties { properties, .. } = &function.characteristics[0] else {
        panic!()
    };
    assert!(
        matches!(&properties[0].value, Some(Expr::Identifier(name)) if name.value == "DEFAULT")
    );
    assert!(properties[1].value.is_none());
    assert!(matches!(
        properties[2].value,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        }))
    ));
    assert!(trino_printer::print_function_specification(&function).contains("p = (DEFAULT)"));
}

#[test]
fn dollar_definitions_keep_raw_content_newlines_and_question_marks_outside_parameters() {
    for newline in ["\n", "\r", "\r\n"] {
        let source = format!(
            "FUNCTION f() RETURNS BIGINT LANGUAGE JAVA WITH (p = ?) AS $${newline}? 'raw'\\n\n$$"
        );
        let function = roundtrip_function(&source);
        let RoutineBody::External { definition, .. } = &function.body else {
            panic!()
        };
        assert_eq!(definition, "? 'raw'\\n\n");
        let RoutineCharacteristic::Properties { properties, .. } = &function.characteristics[1]
        else {
            panic!()
        };
        let Some(Expr::Parameter(parameter)) = &properties[0].value else {
            panic!()
        };
        assert_eq!(parameter.position, 0);
        let printed = trino_printer::print_function_specification(&function);
        assert!(printed.ends_with("AS $$\n? 'raw'\\n\n$$"));
    }
    let empty = roundtrip_function("FUNCTION f() RETURNS BIGINT AS $$\n$$");
    assert!(
        matches!(&empty.body, RoutineBody::External { definition, .. } if definition.is_empty())
    );
    let source = "WITH FUNCTION f() RETURNS BIGINT AS $$\n?\n$$ SELECT ?";
    let original = trino_parser::parse_query(source).unwrap();
    let reparsed = trino_parser::parse_query(&trino_printer::print_query(&original)).unwrap();
    assert!(original.syntax_eq(&reparsed));
}

#[test]
fn unrepresentable_programmatic_routine_facts_are_refused_not_rewritten() {
    for definition in ["unescaped$$body", "trailing$"] {
        let mut function = trino_parser::parse_function_specification(
            "FUNCTION f() RETURNS BIGINT AS $$\nvalid$$",
        )
        .unwrap();
        function.body = RoutineBody::External {
            definition: definition.into(),
            span: SYNTHETIC,
        };
        assert!(
            std::panic::catch_unwind(|| trino_printer::print_function_specification(&function))
                .is_err()
        );
    }
    let mut function =
        trino_parser::parse_function_specification("FUNCTION f() RETURNS BIGINT RETURN 1").unwrap();
    function.parameters.push(FunctionParameter {
        name: None,
        data_type: trino_parser::parse_type("INTERVAL DAY").unwrap(),
        span: SYNTHETIC,
    });
    assert!(
        std::panic::catch_unwind(|| trino_printer::print_function_specification(&function))
            .is_err()
    );
}

#[test]
fn original_routine_coverage_rows_roundtrip_through_their_actual_component_entry() {
    let mut exercised = false;
    for line in include_str!("grammar-coverage.tsv")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 10
            || !matches!(
                fields[1],
                "standaloneFunctionSpecification"
                    | "functionSpecification"
                    | "functionDefinition"
                    | "functionDeclaration"
                    | "parameterDeclaration"
                    | "returnsClause"
                    | "routineCharacteristic"
                    | "controlStatement"
                    | "caseStatementWhenClause"
                    | "elseIfClause"
                    | "elseClause"
                    | "variableDeclaration"
                    | "sqlStatementList"
            )
        {
            continue;
        }
        assert!(
            !fields[5].is_empty(),
            "{} has no actual routine sample",
            fields[1]
        );
        let source = sample(fields[5]);
        if fields[1] == "standaloneFunctionSpecification" {
            roundtrip_function(source.as_ref());
        } else {
            let original = trino_parser::parse_query(source.as_ref())
                .unwrap_or_else(|error| panic!("{}: {source:?}: {error:?}", fields[1]));
            assert!(!original.functions.is_empty());
            let printed = trino_printer::print_query(&original);
            let reparsed = trino_parser::parse_query(&printed)
                .unwrap_or_else(|error| panic!("{} -> {printed:?}: {error:?}", fields[1]));
            assert!(
                original.syntax_eq(&reparsed),
                "{}: {source:?} -> {printed:?}",
                fields[1]
            );
        }
        exercised = true;
    }
    assert!(exercised);
}

#[test]
fn actual_deep_routine_control_prints_and_reparses_on_the_ordinary_test_thread() {
    let depth = 2048;
    let mut source = String::from("FUNCTION f() RETURNS BIGINT BEGIN ");
    for _ in 0..depth {
        source.push_str("IF TRUE THEN ");
    }
    source.push_str("RETURN ?;");
    for _ in 0..depth {
        source.push_str(" END IF;");
    }
    source.push_str(" END");
    let original = trino_parser::parse_function_specification(&source).unwrap();
    let printed = trino_printer::print_function_specification(&original);
    let reparsed = trino_parser::parse_function_specification(&printed).unwrap();
    assert!(original.syntax_eq(&reparsed));
    let RoutineBody::Sql(ControlStatement::Compound(body)) = &reparsed.body else {
        panic!()
    };
    let mut cursor = &body.statements[0];
    for _ in 0..depth {
        let ControlStatement::If(branch) = cursor else {
            panic!("lost nested IF")
        };
        assert_eq!(branch.statements.len(), 1);
        cursor = &branch.statements[0];
    }
    let ControlStatement::Return(returned) = cursor else {
        panic!()
    };
    let Expr::Parameter(parameter) = &returned.value else {
        panic!()
    };
    assert_eq!(parameter.position, 0);
}
