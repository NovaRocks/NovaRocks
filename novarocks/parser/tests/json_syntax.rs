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

//! Public typed syntax dispatch; target-parser roundtrips belong to P05.
use novarocks_parser::{
    SYNTHETIC, Span, StructuralViolation, ValidateError, ast::*, build, parse, printer,
    trino_parser, trino_printer,
};

fn invocation() -> JsonPathInvocation {
    JsonPathInvocation::plain(
        build::identifier(build::ident("doc", false)),
        JsonPathString::new("lax $".into(), SYNTHETIC),
        SYNTHETIC,
    )
}
fn expression_query(expression: Expr) -> Statement {
    Statement::Query(build::query(SetExpr::Select(
        novarocks_parser::ast::AstBox::new(
            build::select(vec![build::item(expression, None)], None, None, None).unwrap(),
        ),
    )))
}
fn table_query(table: JsonTable) -> Statement {
    let from = build::alias(
        Relation::JsonTable(AstBox::new(table)),
        build::ident("jt", false),
        Some(vec![build::ident("x", false)]),
    );
    Statement::Query(build::query(SetExpr::Select(
        novarocks_parser::ast::AstBox::new(
            build::select(vec![build::wildcard()], Some(from), None, None).unwrap(),
        ),
    )))
}

#[test]
fn complete_legacy_json_object_pairs_share_standard_member_syntax() {
    for (legacy_sql, trino_sql) in [
        (
            "SELECT json_object('2:3', NULL)",
            "SELECT JSON_OBJECT('2:3': NULL)",
        ),
        (
            "SELECT json_object('k', json_object('n', 1))",
            "SELECT JSON_OBJECT('k': JSON_OBJECT('n': 1))",
        ),
        (
            "SELECT array_agg(DISTINCT [json_object('2:3', NULL)]) FROM ss",
            "SELECT array_agg(DISTINCT ARRAY[JSON_OBJECT('2:3': NULL)]) FROM ss",
        ),
    ] {
        let statement = parse(legacy_sql).unwrap().pop().unwrap();
        let Statement::Query(legacy) = &statement else {
            panic!("expected an ordinary query");
        };
        let standard = trino_parser::parse_query(trino_sql).unwrap();
        assert!(legacy.syntax_eq(&standard), "{legacy_sql}");
        assert_eq!(printer::print_statement(&statement), legacy_sql);
        let printed = trino_printer::print_query(&standard).unwrap();
        assert!(standard.syntax_eq(&trino_parser::parse_query(&printed).unwrap()));
    }
}

#[test]
fn other_legacy_json_object_calls_keep_their_original_function_shape() {
    for source in [
        "json_object('k')",
        "json_object()",
        "json_object(DISTINCT 'k', 1)",
        "json_object('k', 1 ORDER BY k)",
        "json_object('k', 1) FILTER (WHERE true)",
        "json_object('k', 1) OVER ()",
        "`json_object`('k', 1)",
        "s.json_object('k', 1)",
        "JSON_OBJECT('k', 1)",
    ] {
        let statement = parse(&format!("SELECT {source}")).unwrap().pop().unwrap();
        let Statement::Query(query) = statement else {
            panic!("expected an ordinary query");
        };
        let SetExpr::Select(select) = &*query.body else {
            panic!("expected SELECT");
        };
        let SelectItem::SingleColumn(column) = &select.projection[0] else {
            panic!("expected a scalar projection");
        };
        assert!(
            matches!(&column.expression, Expr::FunctionCall(_)),
            "{source}"
        );
    }
}

#[test]
fn every_json_expression_reaches_the_public_printer_and_validator() {
    let doc = build::identifier(build::ident("doc", false));
    let roots = vec![
        (
            Expr::JsonConstructor(novarocks_parser::ast::AstBox::new(JsonConstructor {
                expression: doc.clone(),
                format: JsonFormat::Json,
                span: SYNTHETIC,
            })),
            "JSON(doc FORMAT JSON)",
        ),
        (
            Expr::JsonExists(novarocks_parser::ast::AstBox::new(JsonExists {
                path_invocation: invocation(),
                error_behavior: JsonExistsErrorBehavior::False,
                span: SYNTHETIC,
            })),
            "JSON_EXISTS(doc FORMAT JSON, 'lax $' FALSE ON ERROR)",
        ),
        (
            Expr::JsonValue(novarocks_parser::ast::AstBox::new(
                JsonValue::new(
                    invocation(),
                    None,
                    JsonValueBehavior::Null,
                    None,
                    JsonValueBehavior::Null,
                    None,
                    SYNTHETIC,
                )
                .unwrap(),
            )),
            "JSON_VALUE(doc FORMAT JSON, 'lax $' NULL ON EMPTY NULL ON ERROR)",
        ),
        (
            Expr::JsonQuery(AstBox::new(JsonQuery::plain(invocation(), SYNTHETIC))),
            "JSON_QUERY(doc FORMAT JSON, 'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)",
        ),
        (
            Expr::JsonSerialize(novarocks_parser::ast::AstBox::new(JsonSerialize {
                expression: doc,
                input_format: JsonFormat::Json,
                returned_type: None,
                output_format: None,
                error_behavior: JsonSerializeErrorBehavior::Error,
                span: SYNTHETIC,
            })),
            "JSON_SERIALIZE(doc FORMAT JSON)",
        ),
        (
            Expr::JsonObject(novarocks_parser::ast::AstBox::new(JsonObject {
                members: vec![],
                null_on_null: true,
                unique_keys: false,
                returned_type: None,
                output_format: None,
                span: SYNTHETIC,
            })),
            "JSON_OBJECT()",
        ),
        (
            Expr::JsonArray(novarocks_parser::ast::AstBox::new(JsonArray {
                elements: vec![],
                null_on_null: false,
                returned_type: None,
                output_format: None,
                span: SYNTHETIC,
            })),
            "JSON_ARRAY()",
        ),
    ];
    for (expression, expected) in roots {
        assert_eq!(printer::print_expr(&expression), expected);
        validate_statement(&expression_query(expression)).unwrap();
    }
}

#[test]
fn json_table_public_relation_dispatch_keeps_alias_columns_and_child_facts() {
    let table = JsonTable::new(
        invocation(),
        vec![JsonTableColumn::Ordinality(JsonTableOrdinalityColumn {
            name: build::ident("ord", false),
            span: SYNTHETIC,
        })],
        None,
        None,
        SYNTHETIC,
    )
    .unwrap();
    let statement = table_query(table);
    validate_statement(&statement).unwrap();
    assert_eq!(
        printer::print_statement(&statement),
        "SELECT * FROM JSON_TABLE(doc FORMAT JSON, 'lax $' COLUMNS (ord FOR ORDINALITY)) AS jt(x)"
    );
    #[derive(Default)]
    struct Names(Vec<String>);
    impl Visit for Names {
        fn visit_ident(&mut self, ident: &Ident) {
            self.0.push(ident.value.clone());
        }
    }
    let mut names = Names::default();
    names.visit_statement(&statement);
    assert_eq!(names.0, ["doc", "ord", "jt", "x"]);
    struct Rename;
    impl Fold for Rename {
        fn fold_ident(&mut self, mut ident: Ident) -> Ident {
            ident.value = format!("renamed_{}", ident.value);
            ident
        }
    }
    let renamed = Rename.fold_statement(statement.clone());
    let (Statement::Query(original), Statement::Query(rebuilt)) = (&statement, &renamed) else {
        panic!("query");
    };
    assert!(!original.syntax_eq(rebuilt));
    let mut names = Names::default();
    names.visit_statement(&renamed);
    assert_eq!(
        names.0,
        ["renamed_doc", "renamed_ord", "renamed_jt", "renamed_x"]
    );
}

#[test]
fn public_validation_consumes_json_constructor_invariants_with_original_spans() {
    let bad_span = Span::new(17, 31);
    let mut value = JsonValue::new(
        invocation(),
        None,
        JsonValueBehavior::Null,
        None,
        JsonValueBehavior::Null,
        None,
        bad_span,
    )
    .unwrap();
    value.error_behavior = JsonValueBehavior::Default;
    let error = validate_statement(&expression_query(Expr::JsonValue(
        novarocks_parser::ast::AstBox::new(value),
    )))
    .unwrap_err();
    assert_eq!(
        error,
        ValidateError::InvalidStructure {
            violation: StructuralViolation::JsonConstructorInvariant(
                JsonAstInvariantError::ErrorDefaultMismatch
            ),
            span: bad_span
        }
    );
    let table = JsonTable {
        path_invocation: invocation(),
        columns: vec![],
        plan: None,
        error_behavior: None,
        span: bad_span,
    };
    let error = validate_statement(&table_query(table)).unwrap_err();
    assert_eq!(
        error,
        ValidateError::InvalidStructure {
            violation: StructuralViolation::JsonConstructorInvariant(
                JsonAstInvariantError::EmptyTableColumns
            ),
            span: bad_span
        }
    );
}
