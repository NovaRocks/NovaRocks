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
use novarocks_parser::parser::{
    parse_function_specification as parse_standalone_function, parse_query,
};
use novarocks_parser::{ParseError, ParserError, Span};

fn specification(source: &str) -> FunctionSpecification {
    let mut query = parse_query(source).unwrap_or_else(|error| panic!("{source:?}: {error:?}"));
    assert_eq!(query.functions.len(), 1);
    query.functions.remove(0)
}
fn inline(fragment: &str) -> FunctionSpecification {
    specification(&format!("WITH {fragment} SELECT 0"))
}
fn sql_body(function: &FunctionSpecification) -> &ControlStatement {
    let RoutineBody::Sql(body) = &function.body else {
        panic!("not SQL: {function:?}")
    };
    body
}
fn compound(function: &FunctionSpecification) -> &CompoundStatement {
    let ControlStatement::Compound(body) = sql_body(function) else {
        panic!("not BEGIN: {function:?}")
    };
    body
}
fn parameter(expression: &Expr) -> &Parameter {
    let Expr::Parameter(value) = expression else {
        panic!("not parameter: {expression:?}")
    };
    value
}
fn long(expression: &Expr) -> i64 {
    let Expr::Literal(Literal {
        kind: LiteralKind::Long { parsed_value, .. },
        ..
    }) = expression
    else {
        panic!("not long: {expression:?}")
    };
    *parsed_value
}
fn generic_name(data_type: &TypeName) -> &str {
    let TypeKind::Generic(value) = &data_type.kind else {
        panic!("not generic: {data_type:?}")
    };
    &value.name.value
}
fn unexpected(source: &str) -> (String, Span) {
    let ParserError::Parse(ParseError::UnexpectedToken { found, span, .. }) =
        parse_query(source).unwrap_err()
    else {
        panic!("expected parse error: {source}")
    };
    (found, span)
}

#[test]
fn routine_parameters_preserve_optional_names_and_complete_types() {
    let source = "WITH FUNCTION cat.\"f\"(BIGINT, x ARRAY(BIGINT), \"Q\" ROW(a BIGINT), TIMESTAMP(3) WITH TIME ZONE) RETURNS BIGINT RETURN 1 SELECT 0";
    let function = specification(source);
    assert_eq!(
        function
            .name
            .parts
            .iter()
            .map(|part| (part.value.as_str(), part.quoted))
            .collect::<Vec<_>>(),
        vec![("cat", false), ("f", true)]
    );
    assert_eq!(function.parameters.len(), 4);
    assert!(function.parameters[0].name.is_none());
    assert_eq!(generic_name(&function.parameters[0].data_type), "BIGINT");
    assert_eq!(function.parameters[1].name.as_ref().unwrap().value, "x");
    assert_eq!(generic_name(&function.parameters[1].data_type), "ARRAY");
    assert!(function.parameters[2].name.as_ref().unwrap().quoted);
    let TypeKind::Row(row) = &function.parameters[2].data_type.kind else {
        panic!()
    };
    assert_eq!(row.fields.len(), 1);
    assert_eq!(row.fields[0].name.as_ref().unwrap().value, "a");
    assert!(function.parameters[3].name.is_none());
    let TypeKind::DateTime(value) = &function.parameters[3].data_type.kind else {
        panic!()
    };
    assert!(value.with_time_zone);
    assert!(value.precision.is_some());
    assert!(function.characteristics.is_empty());
    assert_eq!(
        function.span,
        Span::new(5, source.find(" SELECT 0").unwrap())
    );
    let ControlStatement::Return(body) = sql_body(&function) else {
        panic!()
    };
    assert_eq!(long(&body.value), 1);
    assert_eq!(
        &source[body.value.span().start()..body.value.span().end()],
        "1"
    );

    // The optional name precedes type, so a complete named alternative wins.
    let ambiguous = inline("FUNCTION f(foo ARRAY, INTERVAL DAY) RETURNS BIGINT RETURN 1");
    assert_eq!(ambiguous.parameters[0].name.as_ref().unwrap().value, "foo");
    assert_eq!(generic_name(&ambiguous.parameters[0].data_type), "ARRAY");
    assert_eq!(
        ambiguous.parameters[1].name.as_ref().unwrap().value,
        "INTERVAL"
    );
    assert_eq!(generic_name(&ambiguous.parameters[1].data_type), "DAY");
}

#[test]
fn routine_characteristics_keep_order_duplicates_and_property_defaults() {
    let function = inline(
        "FUNCTION f() RETURNS BIGINT LANGUAGE SQL DETERMINISTIC NOT DETERMINISTIC RETURNS NULL ON NULL INPUT CALLED ON NULL INPUT SECURITY DEFINER SECURITY INVOKER COMMENT U&'a\\0062' WITH (p = DEFAULT, p = NULL, q = DEFAULT + 1) RETURN 7",
    );
    assert_eq!(function.characteristics.len(), 9);
    assert!(
        matches!(&function.characteristics[0], RoutineCharacteristic::Language { language, .. } if language.value == "SQL")
    );
    assert!(matches!(
        function.characteristics[1],
        RoutineCharacteristic::Deterministic {
            deterministic: true,
            ..
        }
    ));
    assert!(matches!(
        function.characteristics[2],
        RoutineCharacteristic::Deterministic {
            deterministic: false,
            ..
        }
    ));
    assert!(matches!(
        function.characteristics[3],
        RoutineCharacteristic::NullInput {
            called_on_null: false,
            ..
        }
    ));
    assert!(matches!(
        function.characteristics[4],
        RoutineCharacteristic::NullInput {
            called_on_null: true,
            ..
        }
    ));
    assert!(matches!(
        function.characteristics[5],
        RoutineCharacteristic::Security {
            security: RoutineSecurity::Definer,
            ..
        }
    ));
    assert!(matches!(
        function.characteristics[6],
        RoutineCharacteristic::Security {
            security: RoutineSecurity::Invoker,
            ..
        }
    ));
    assert!(
        matches!(&function.characteristics[7], RoutineCharacteristic::Comment { comment, .. } if comment == "ab")
    );
    let RoutineCharacteristic::Properties { properties, .. } = &function.characteristics[8] else {
        panic!()
    };
    assert_eq!(properties.len(), 3);
    assert_eq!(properties[0].name.value, properties[1].name.value);
    assert!(properties[0].value.is_none());
    assert!(matches!(
        properties[1].value,
        Some(Expr::Literal(Literal {
            kind: LiteralKind::Null,
            ..
        }))
    ));
    assert!(matches!(properties[2].value, Some(Expr::Binary(_))));
}

#[test]
fn external_definitions_strip_one_initial_line_break_and_never_visit_raw_questions() {
    for newline in ["\n", "\r", "\r\n"] {
        let source = format!(
            "WITH FUNCTION f() RETURNS BIGINT LANGUAGE JAVA WITH (p = ?) AS $${newline}?\\nraw\n$$ SELECT ?"
        );
        let query = parse_query(&source).unwrap();
        let function = &query.functions[0];
        let RoutineBody::External { definition, span } = &function.body else {
            panic!()
        };
        assert_eq!(definition, "?\\nraw\n");
        assert_eq!(
            &source[span.start()..span.end()],
            format!("$${newline}?\\nraw\n$$")
        );
        let RoutineCharacteristic::Properties { properties, .. } = &function.characteristics[1]
        else {
            panic!()
        };
        // rootQuery is visited before functionSpecification. The Dollar body
        // is raw definition content and contributes no parameter event.
        assert_eq!(parameter(properties[0].value.as_ref().unwrap()).position, 1);
        struct Positions(Vec<usize>);
        impl Visit for Positions {
            fn visit_parameter(&mut self, value: &Parameter) {
                self.0.push(value.position);
            }
        }
        let mut positions = Positions(Vec::new());
        positions.visit_query(&query);
        positions.0.sort_unstable();
        assert_eq!(positions.0, vec![0, 1]);
    }
    for body in ["$$$$", "$$not-a-newline$$"] {
        let source = format!("WITH FUNCTION f() RETURNS BIGINT AS {body} SELECT 0");
        let (found, span) = unexpected(&source);
        assert_eq!(found, body);
        assert_eq!(&source[span.start()..span.end()], body);
    }
}

#[test]
fn compound_declarations_assignment_and_return_keep_original_value_spans() {
    let source = "WITH FUNCTION f() RETURNS BIGINT BEGIN DECLARE x, \"Y\" BIGINT DEFAULT 3; DECLARE z BIGINT; SET x = ?; RETURN x; END SELECT 0";
    let function = specification(source);
    let body = compound(&function);
    assert_eq!(body.variable_declarations.len(), 2);
    let declaration = &body.variable_declarations[0];
    assert_eq!(
        declaration
            .names
            .iter()
            .map(|name| (name.value.as_str(), name.quoted))
            .collect::<Vec<_>>(),
        vec![("x", false), ("Y", true)]
    );
    assert_eq!(long(declaration.default_value.as_ref().unwrap()), 3);
    assert!(body.variable_declarations[1].default_value.is_none());
    assert_eq!(
        &source[declaration.span.start()..declaration.span.end()],
        "DECLARE x, \"Y\" BIGINT DEFAULT 3"
    );
    assert_eq!(body.statements.len(), 2);
    let ControlStatement::Assignment(set) = &body.statements[0] else {
        panic!()
    };
    assert_eq!(set.target.value, "x");
    let value = parameter(&set.value);
    assert_eq!(value.position, 0);
    assert_eq!(
        value.span,
        Span::new(source.find('?').unwrap(), source.find('?').unwrap() + 1)
    );
    assert_eq!(&source[set.span.start()..set.span.end()], "SET x = ?");
    assert!(matches!(body.statements[1], ControlStatement::Return(_)));
    let empty = inline("FUNCTION f() RETURNS BIGINT BEGIN END");
    assert!(compound(&empty).statements.is_empty());
    assert!(compound(&empty).variable_declarations.is_empty());
    let declarations_only = inline("FUNCTION f() RETURNS BIGINT BEGIN DECLARE x BIGINT; END");
    assert!(compound(&declarations_only).statements.is_empty());
    assert_eq!(compound(&declarations_only).variable_declarations.len(), 1);
}

#[test]
fn case_and_if_controls_keep_distinct_clauses_and_optional_else() {
    let source = "WITH FUNCTION f() RETURNS BIGINT BEGIN CASE ? WHEN ? THEN RETURN ?; ELSE RETURN ?; END CASE; CASE WHEN ? THEN RETURN ?; END CASE; IF ? THEN RETURN ?; ELSEIF ? THEN RETURN ?; ELSE RETURN ?; END IF; END SELECT 0";
    let function = specification(source);
    let statements = &compound(&function).statements;
    assert_eq!(statements.len(), 3);
    let ControlStatement::Case(simple) = &statements[0] else {
        panic!()
    };
    assert_eq!(parameter(simple.expression.as_ref().unwrap()).position, 0);
    assert_eq!(parameter(&simple.when_clauses[0].expression).position, 1);
    assert!(simple.else_clause.is_some());
    let ControlStatement::Case(searched) = &statements[1] else {
        panic!()
    };
    assert!(searched.expression.is_none());
    assert_eq!(parameter(&searched.when_clauses[0].expression).position, 4);
    assert!(searched.else_clause.is_none());
    let ControlStatement::If(branch) = &statements[2] else {
        panic!()
    };
    assert_eq!(parameter(&branch.expression).position, 6);
    assert_eq!(branch.else_if_clauses.len(), 1);
    assert_eq!(parameter(&branch.else_if_clauses[0].expression).position, 8);
    assert!(branch.else_clause.is_some());
    assert_eq!(
        &source[branch.else_if_clauses[0].span.start()..branch.else_if_clauses[0].span.end()],
        "ELSEIF ? THEN RETURN ?;"
    );
    assert_eq!(
        &source[simple.when_clauses[0].span.start()..simple.when_clauses[0].span.end()],
        "WHEN ? THEN RETURN ?;"
    );
}

#[test]
fn loop_labels_and_repeat_event_order_are_syntax_facts_not_scope_checks() {
    let function = inline(
        "FUNCTION f() RETURNS BIGINT BEGIN outer_loop: LOOP ITERATE missing; LEAVE missing; END LOOP; WHILE ? DO RETURN ?; END WHILE; r: REPEAT RETURN ?; UNTIL ? END REPEAT; RETURN 0; END",
    );
    let statements = &compound(&function).statements;
    let ControlStatement::Loop(value) = &statements[0] else {
        panic!()
    };
    assert_eq!(value.label.as_ref().unwrap().value, "outer_loop");
    let ControlStatement::Iterate(iterate) = &value.statements[0] else {
        panic!()
    };
    let ControlStatement::Leave(leave) = &value.statements[1] else {
        panic!()
    };
    assert_eq!(iterate.label.value, "missing");
    assert_eq!(leave.label.value, "missing");
    let ControlStatement::While(value) = &statements[1] else {
        panic!()
    };
    assert!(value.label.is_none());
    assert_eq!(parameter(&value.expression).position, 0);
    let ControlStatement::Repeat(value) = &statements[2] else {
        panic!()
    };
    assert_eq!(value.label.as_ref().unwrap().value, "r");
    let ControlStatement::Return(body) = &value.statements[0] else {
        panic!()
    };
    assert_eq!(parameter(&body.value).position, 2);
    assert_eq!(parameter(&value.condition).position, 3);
}

#[test]
fn routine_body_precedes_earlier_properties_in_the_single_parameter_ledger() {
    let source =
        "WITH FUNCTION f() RETURNS BIGINT WITH (p = ?) BEGIN SET x = ?; RETURN ?; END SELECT ?";
    let query = parse_query(source).unwrap();
    let function = &query.functions[0];
    let RoutineCharacteristic::Properties { properties, .. } = &function.characteristics[0] else {
        panic!()
    };
    assert_eq!(parameter(properties[0].value.as_ref().unwrap()).position, 3);
    let body = compound(function);
    let ControlStatement::Assignment(set) = &body.statements[0] else {
        panic!()
    };
    let ControlStatement::Return(returned) = &body.statements[1] else {
        panic!()
    };
    assert_eq!(parameter(&set.value).position, 1);
    assert_eq!(parameter(&returned.value).position, 2);
    assert_eq!(
        parameter(&set.value).span.start(),
        source.find("SET x = ?").unwrap() + 8
    );
    // A discarded special-call ORDER BY parameter still occupies its visit slot.
    let holes = inline("FUNCTION f() RETURNS BIGINT WITH (p = ?) RETURN IF(?, ? ORDER BY ?)");
    let ControlStatement::Return(returned) = sql_body(&holes) else {
        panic!()
    };
    let Expr::If(value) = &returned.value else {
        panic!()
    };
    assert_eq!(parameter(&value.condition).position, 1);
    assert_eq!(parameter(&value.true_value).position, 2);
    let RoutineCharacteristic::Properties { properties, .. } = &holes.characteristics[0] else {
        panic!()
    };
    assert_eq!(parameter(properties[0].value.as_ref().unwrap()).position, 3);
}

#[test]
fn routine_grammar_and_matched_constructor_failures_are_not_scope_or_capability_checks() {
    for fragment in [
        "FUNCTION f() RETURNS BIGINT BEGIN RETURN 1 END",
        "FUNCTION f() RETURNS BIGINT BEGIN SET x = 1; DECLARE y BIGINT; END",
        "FUNCTION f() RETURNS BIGINT BEGIN IF TRUE THEN END IF; END",
        "FUNCTION f() RETURNS BIGINT BEGIN CASE WHEN > 1 THEN RETURN 1; END CASE; END",
        "FUNCTION f() RETURNS BIGINT BEGIN x: BEGIN END; END",
        "FUNCTION f() RETURNS BIGINT BEGIN LOOP RETURN 1; END LOOP x; END",
        "FUNCTION f() RETURNS BIGINT WITH () RETURN 1",
        "FUNCTION f() RETURNS BIGINT RETURN 1 = 1",
        "FUNCTION f() RETURNS BIGINT AS 'not dollar'",
    ] {
        let source = format!("WITH {fragment} SELECT 0");
        assert!(
            matches!(parse_query(&source), Err(ParserError::Parse(_))),
            "{source}"
        );
    }
    let source = "WITH FUNCTION f() RETURNS BIGINT LOOP RETURN 1; END LOOP SELECT 0";
    let (found, span) = unexpected(source);
    assert_eq!(found, "LOOP RETURN 1; END LOOP");
    assert_eq!(&source[span.start()..span.end()], found);
    let source = "WITH FUNCTION f(x BIGINT) RETURNS BIGINT RETURN 9223372036854775808 SELECT 0";
    let (found, span) = unexpected(source);
    assert_eq!(found, "9223372036854775808");
    assert_eq!(&source[span.start()..span.end()], found);
    let source = "WITH FUNCTION f(x DECIMAL(9223372036854775808)) RETURNS BIGINT RETURN 0 SELECT 0";
    let (found, span) = unexpected(source);
    assert_eq!(found, "9223372036854775808");
    assert_eq!(&source[span.start()..span.end()], found);
}

#[test]
fn deeply_nested_control_source_keeps_the_real_return_and_parameter() {
    let depth = 2048;
    let mut source = String::from("WITH FUNCTION f() RETURNS BIGINT BEGIN ");
    for _ in 0..depth {
        source.push_str("IF TRUE THEN ");
    }
    let parameter_offset = source.len() + "RETURN ".len();
    source.push_str("RETURN ?;");
    for _ in 0..depth {
        source.push_str(" END IF;");
    }
    source.push_str(" END SELECT 0");
    let function = specification(&source);
    let mut statement = &compound(&function).statements[0];
    for _ in 0..depth {
        let ControlStatement::If(branch) = statement else {
            panic!("lost nested IF")
        };
        assert_eq!(branch.statements.len(), 1);
        assert!(branch.else_if_clauses.is_empty());
        assert!(branch.else_clause.is_none());
        statement = &branch.statements[0];
    }
    let ControlStatement::Return(returned) = statement else {
        panic!()
    };
    assert_eq!(
        parameter(&returned.value),
        &Parameter {
            position: 0,
            span: Span::new(parameter_offset, parameter_offset + 1)
        }
    );
    assert!(function.syntax_eq(&function.clone()));
}

#[test]
fn routine_coverage_fixtures_keep_multiline_definition_bytes() {
    for source in [
        include_str!("coverage-sql/routine-return.sql"),
        include_str!("coverage-sql/routine-parameters.sql"),
        include_str!("coverage-sql/routine-characteristics.sql"),
        include_str!("coverage-sql/routine-external.sql"),
        include_str!("coverage-sql/routine-assignment.sql"),
        include_str!("coverage-sql/routine-simple-case.sql"),
        include_str!("coverage-sql/routine-searched-case.sql"),
        include_str!("coverage-sql/routine-if.sql"),
        include_str!("coverage-sql/routine-declarations.sql"),
        include_str!("coverage-sql/routine-loop.sql"),
        include_str!("coverage-sql/routine-while.sql"),
        include_str!("coverage-sql/routine-repeat.sql"),
    ] {
        let function = specification(source);
        assert!(!function.name.parts.is_empty());
    }
    let external = specification(include_str!("coverage-sql/routine-external.sql"));
    let RoutineBody::External { definition, .. } = external.body else {
        panic!()
    };
    assert_eq!(definition, "return ?;\n");
}

#[test]
fn standalone_function_entry_uses_real_eof_and_the_same_parameter_visitor_order() {
    let source = include_str!("coverage-sql/routine-standalone.sql");
    let function = parse_standalone_function(source).unwrap();
    assert_eq!(function.name.parts[0].value, "f");
    assert_eq!(function.parameters[0].name.as_ref().unwrap().value, "x");
    assert_eq!(function.span, Span::new(0, source.trim_end().len()));
    let standalone = "FUNCTION f() RETURNS BIGINT WITH (p = ?) RETURN ?";
    let function = parse_standalone_function(standalone).unwrap();
    let ControlStatement::Return(returned) = sql_body(&function) else {
        panic!()
    };
    assert_eq!(parameter(&returned.value).position, 0);
    let RoutineCharacteristic::Properties { properties, .. } = &function.characteristics[0] else {
        panic!()
    };
    assert_eq!(parameter(properties[0].value.as_ref().unwrap()).position, 1);
    for trailing in [" SELECT 1", ";"] {
        assert!(parse_standalone_function(&format!("{standalone}{trailing}")).is_err());
    }
}
