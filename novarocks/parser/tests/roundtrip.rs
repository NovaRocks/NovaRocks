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

//! Seed AST contracts for the canonical printer.
//!
//! T6 wires `parse()` and extends this test module with
//! `parse(print(ast))` structural-equivalence assertions. Keeping these
//! printer-only contracts independent from that pending public API lets the
//! canonical rendering rules stay covered before parser dispatch exists.

use novarocks_parser::{
    Span,
    ast::{
        BinaryExpr, BinaryOperator, Expr, Ident, Literal, LiteralKind, ShowProcessList, Statement,
    },
    printer::{print_expr, print_statement},
};

fn span() -> Span {
    Span::new(0, 0)
}

fn ident(value: &str) -> Expr {
    Expr::Identifier(Ident {
        value: value.to_owned(),
        quoted: false,
        quote_style: None,
        span: span(),
    })
}

fn binary(left: Expr, operator: BinaryOperator, right: Expr) -> Expr {
    Expr::Binary(BinaryExpr {
        left: Box::new(left),
        operator,
        right: Box::new(right),
        span: span(),
    })
}

#[test]
fn roundtrip_seed_ast_printer_contracts_cover_vertical_slice_and_expressions() {
    let show_processlist = Statement::ShowProcessList(ShowProcessList {
        full: false,
        span: span(),
    });
    assert_eq!(print_statement(&show_processlist), "SHOW PROCESSLIST");

    let expression = binary(
        ident("a"),
        BinaryOperator::Multiply,
        binary(
            Expr::Literal(Literal {
                kind: LiteralKind::Number("01.5e+2".to_owned()),
                span: span(),
            }),
            BinaryOperator::Add,
            ident("c"),
        ),
    );
    assert_eq!(print_expr(&expression), "a * (01.5e+2 + c)");
}
