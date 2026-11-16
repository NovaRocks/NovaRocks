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

use novarocks_parser::{
    ParserError, Span, ValidateError,
    ast::{
        NamedWindow, Query, Select, SelectItem, SelectQuantifier, SetExpr, Statement, Values,
        WindowFrame, WindowFrameBound, WindowFrameExclusion, WindowFrameUnits, WindowSpec,
        validate_statement,
    },
    parse,
};
use novarocks_user_error::ErrorPhase;

const SPAN: Span = Span::new(0, 1);

#[test]
fn public_parse_runs_validation_after_syntax_build() {
    let source = "WITH c AS (SELECT 1), C AS (SELECT 2) SELECT * FROM c";
    let error = parse(source).expect_err("duplicate CTE names are a Validate error");
    let ParserError::Validate(ValidateError::DuplicateCteName { name, span }) = error else {
        panic!("expected typed duplicate CTE validation error");
    };
    assert_eq!(name, "C");
    assert_eq!(span, Span::new(22, 23));
    let user_error =
        ParserError::Validate(ValidateError::DuplicateCteName { name, span }).to_user_error(source);
    assert_eq!(
        user_error.code().as_str(),
        "sql.validate.duplicate_cte_name"
    );
    assert_eq!(user_error.phase(), ErrorPhase::Validate);
    assert_eq!(user_error.location().expect("location").column(), 23);
}

#[test]
fn parser_failures_remain_in_parse_phase_before_validation() {
    let error = parse("SELECT FROM t").expect_err("missing projection is syntax-invalid");
    let ParserError::Parse(_) = error else {
        panic!("expected Parse error, got {error:?}");
    };
    assert_eq!(
        error.to_user_error("SELECT FROM t").phase(),
        ErrorPhase::Parse
    );
}

#[test]
fn duplicate_named_windows_have_a_stable_typed_error() {
    let statement = select_statement(vec![
        named_window("w", false, Span::new(7, 8), None),
        named_window("W", false, Span::new(17, 18), None),
    ]);
    let error = validate_statement(&statement).expect_err("case-insensitive duplicate window");
    assert!(matches!(
        error,
        ValidateError::DuplicateWindowName { ref name, span: Span { .. } } if name == "W"
    ));
    let user_error = ParserError::from(error).to_user_error("SELECT 1 WINDOW w AS (), W AS ()");
    assert_eq!(
        user_error.code().as_str(),
        "sql.validate.duplicate_window_name"
    );
    assert_eq!(user_error.phase(), ErrorPhase::Validate);
    assert_eq!(user_error.location().expect("location").column(), 18);
}

#[test]
fn quoted_window_names_keep_their_syntax_identity() {
    let statement = select_statement(vec![
        named_window("w", false, Span::new(7, 8), None),
        named_window("w", true, Span::new(17, 20), None),
    ]);
    validate_statement(&statement).expect("quoted and unquoted syntax names remain distinct here");
}

#[test]
fn construction_only_empty_lists_retain_the_generic_structure_code() {
    let statement = Statement::Query(Query {
        session_properties: Vec::new(),
        functions: Vec::new(),
        with: None,
        body: Box::new(SetExpr::Values(Values {
            rows: Vec::new(),
            explicit_row: false,
            span: SPAN,
        })),
        modifiers: Default::default(),
        span: SPAN,
    });
    let error = validate_statement(&statement).expect_err("empty VALUES rows are invalid AST");
    assert!(matches!(error, ValidateError::InvalidStructure { .. }));
    let user_error = ParserError::from(error).to_user_error("VALUES");
    assert_eq!(user_error.code().as_str(), "sql.validate.invalid_structure");
    assert_eq!(user_error.phase(), ErrorPhase::Validate);
    assert_eq!(user_error.location().expect("location").column(), 1);
}

#[test]
fn contradictory_window_frame_bounds_are_rejected_without_analyze() {
    let frame = WindowFrame {
        units: WindowFrameUnits::Rows,
        start_bound: WindowFrameBound::Following(Some(number("1")), Span::new(20, 31)),
        end_bound: Some(WindowFrameBound::CurrentRow(Span::new(36, 47))),
        exclusion: WindowFrameExclusion::NoOthers,
        span: Span::new(15, 47),
    };
    let statement = select_statement(vec![named_window("w", false, Span::new(7, 8), Some(frame))]);
    let error = validate_statement(&statement).expect_err("frame start follows its end");
    assert!(matches!(
        error,
        ValidateError::InvalidWindowFrameBounds { .. }
    ));
    let user_error = ParserError::from(error)
        .to_user_error("SELECT 1 WINDOW w AS (ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW)");
    assert_eq!(
        user_error.code().as_str(),
        "sql.validate.invalid_window_frame_bounds"
    );
    assert_eq!(user_error.phase(), ErrorPhase::Validate);
}

#[test]
fn impossible_unbounded_window_frame_endpoints_are_rejected() {
    for frame in [
        WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Following(None, Span::new(20, 39)),
            end_bound: None,
            exclusion: WindowFrameExclusion::NoOthers,
            span: Span::new(15, 39),
        },
        WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::CurrentRow(Span::new(20, 31)),
            end_bound: Some(WindowFrameBound::Preceding(None, Span::new(36, 55))),
            exclusion: WindowFrameExclusion::NoOthers,
            span: Span::new(15, 55),
        },
    ] {
        let statement = select_statement(vec![named_window("w", false, SPAN, Some(frame))]);
        assert!(matches!(
            validate_statement(&statement),
            Err(ValidateError::InvalidWindowFrameBounds { .. })
        ));
    }
}

fn select_statement(windows: Vec<NamedWindow>) -> Statement {
    Statement::Query(Query {
        session_properties: Vec::new(),
        functions: Vec::new(),
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            quantifier: SelectQuantifier::None,
            projection: vec![SelectItem::UnnamedExpr(number("1"))],
            from: Vec::new(),
            selection: None,
            group_by: novarocks_parser::ast::GroupBy::None,
            having: None,
            qualify: None,
            windows,
            modifiers: Default::default(),
            span: SPAN,
        }))),
        modifiers: Default::default(),
        span: SPAN,
    })
}

fn named_window(
    name: &str,
    quoted: bool,
    name_span: Span,
    frame: Option<WindowFrame>,
) -> NamedWindow {
    NamedWindow {
        name: novarocks_parser::ast::Ident {
            value: name.to_owned(),
            quoted,
            span: name_span,
        },
        specification: WindowSpec {
            existing_window_name: None,
            partition_by: Vec::new(),
            order_by: Vec::new(),
            window_frame: frame,
            span: SPAN,
        },
        span: SPAN,
    }
}

fn number(value: &str) -> novarocks_parser::ast::Expr {
    novarocks_parser::ast::Expr::Literal(novarocks_parser::ast::Literal {
        kind: novarocks_parser::ast::LiteralKind::Long {
            spelling: value.to_owned(),
            parsed_value: (value.to_owned()).parse::<i64>().unwrap(),
        },
        span: SPAN,
    })
}
