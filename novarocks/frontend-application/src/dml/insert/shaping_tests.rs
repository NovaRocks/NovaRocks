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

use super::{omitted_insert_expr, shape_insert_source};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use novarocks_parser::ast::SyntaxEq;
use novarocks_parser::{Span, ast, printer};
use novarocks_types::schema::{ColumnDef, ColumnDefault};
use std::sync::Arc;

fn column(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        data_type,
        nullable: true,
        write_default: None,
        logical_type: None,
    }
}

fn parse_query(sql: &str) -> ast::Query {
    let statements = novarocks_parser::parse(sql).expect("test query should parse");
    let [ast::Statement::Query(query)] = statements.as_slice() else {
        panic!("expected one query");
    };
    query.clone()
}

fn shape(sql: &str, columns: &[ColumnDef]) -> ast::Query {
    shape_insert_source(&parse_query(sql), &[], columns, columns)
        .expect("literal source should shape")
        .expect("literal source should use target shaping")
}

fn rows(query: &ast::Query) -> &[Vec<ast::Expr>] {
    let ast::SetExpr::Values(values) = query.body.as_ref() else {
        panic!("expected VALUES");
    };
    &values.rows
}

fn select(body: &ast::SetExpr) -> &ast::Select {
    match body {
        ast::SetExpr::Select(select) => select,
        ast::SetExpr::Query(query) => select(&query.body),
        _ => panic!("expected SELECT leaf"),
    }
}

fn projection_expr(select: &ast::Select, index: usize) -> &ast::Expr {
    match &select.projection[index] {
        ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => panic!("expected expression projection"),
    }
}

fn cast_input<'a>(expr: &'a ast::Expr, target: &str) -> &'a ast::Expr {
    let ast::Expr::Cast(cast) = expr else {
        panic!("expected target CAST, got {expr:?}");
    };
    assert_eq!(printer::print_type_name(&cast.data_type), target);
    &cast.expr
}

fn number_text(expr: &ast::Expr) -> &str {
    let ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Number(text),
        ..
    }) = expr
    else {
        panic!("expected exact numeric literal, got {expr:?}");
    };
    text
}

fn literal_bytes(expr: &ast::Expr) -> Vec<u8> {
    let ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::HexString(text),
        ..
    }) = expr
    else {
        panic!("expected binary literal, got {expr:?}");
    };
    hex::decode(text).expect("binary literal must have valid hex")
}

fn decimal_type() -> DataType {
    DataType::Decimal128(18, 9)
}

fn array_type(inner: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", inner, true)))
}

fn map_type(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("key", key, false)),
                Arc::new(Field::new("value", value, true)),
            ])),
            false,
        )),
        false,
    )
}

#[test]
fn values_constrain_each_decimal_before_mixed_row_type_inference() {
    let query = shape(
        "VALUES (123456789.123456789), (1.25e2), (NULL)",
        &[column("d", decimal_type())],
    );
    let exact = number_text(cast_input(&rows(&query)[0][0], "DECIMAL(18, 9)"));
    assert_eq!(exact, "123456789.123456789");
    assert_eq!(
        exact.replace('.', "").parse::<i128>().unwrap(),
        123456789123456789
    );
    assert_eq!(
        number_text(cast_input(&rows(&query)[1][0], "DECIMAL(18, 9)")),
        "1.25e2"
    );
    assert!(matches!(
        cast_input(&rows(&query)[2][0], "DECIMAL(18, 9)"),
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        })
    ));
}

#[test]
fn constant_union_branches_are_typed_before_their_common_type() {
    let query = shape(
        "SELECT 123456789.123456789 UNION ALL SELECT 1.25e2",
        &[column("d", decimal_type())],
    );
    let ast::SetExpr::SetOperation(operation) = query.body.as_ref() else {
        panic!("expected retained UNION");
    };
    assert_eq!(operation.quantifier, ast::SetQuantifier::All);
    for (body, expected) in [
        (&operation.left, "123456789.123456789"),
        (&operation.right, "1.25e2"),
    ] {
        let expr = projection_expr(select(body), 0);
        assert_eq!(number_text(cast_input(expr, "DECIMAL(18, 9)")), expected);
    }
}

#[test]
fn recursive_array_map_and_struct_literals_keep_exact_decimal_children() {
    let decimal_array = array_type(decimal_type());
    let structure = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("d", decimal_type(), true)),
        Arc::new(Field::new("a", decimal_array.clone(), true)),
    ]));
    let columns = [
        column("a", decimal_array.clone()),
        column("m", map_type(decimal_type(), decimal_array)),
        column("s", structure),
    ];
    let query = shape(
        "VALUES ([123456789.123456789, 1.25e2, NULL], map(123456789.123456789, [123456789.123456789, NULL]), row(123456789.123456789, [123456789.123456789]))",
        &columns,
    );
    let row = &rows(&query)[0];
    let ast::Expr::Array(array) = cast_input(&row[0], "ARRAY<DECIMAL(18, 9)>") else {
        panic!("expected direct ARRAY");
    };
    assert_eq!(
        number_text(cast_input(&array.elements[0], "DECIMAL(18, 9)")),
        "123456789.123456789"
    );
    assert_eq!(
        number_text(cast_input(&array.elements[1], "DECIMAL(18, 9)")),
        "1.25e2"
    );
    assert!(matches!(
        cast_input(&array.elements[2], "DECIMAL(18, 9)"),
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        })
    ));

    let ast::Expr::FunctionCall(map) =
        cast_input(&row[1], "MAP<DECIMAL(18, 9), ARRAY<DECIMAL(18, 9)>>")
    else {
        panic!("expected MAP constructor");
    };
    assert_eq!(
        number_text(cast_input(&map.arguments[0], "DECIMAL(18, 9)")),
        "123456789.123456789"
    );
    let ast::Expr::Array(map_array) = cast_input(&map.arguments[1], "ARRAY<DECIMAL(18, 9)>") else {
        panic!("expected MAP array value");
    };
    assert_eq!(
        number_text(cast_input(&map_array.elements[0], "DECIMAL(18, 9)")),
        "123456789.123456789"
    );

    let ast::Expr::FunctionCall(structure) = cast_input(
        &row[2],
        "STRUCT<`d` DECIMAL(18, 9), `a` ARRAY<DECIMAL(18, 9)>>",
    ) else {
        panic!("expected ROW constructor");
    };
    assert_eq!(
        number_text(cast_input(&structure.arguments[0], "DECIMAL(18, 9)")),
        "123456789.123456789"
    );
    let ast::Expr::Array(struct_array) =
        cast_input(&structure.arguments[1], "ARRAY<DECIMAL(18, 9)>")
    else {
        panic!("expected STRUCT array field");
    };
    assert_eq!(
        number_text(cast_input(&struct_array.elements[0], "DECIMAL(18, 9)")),
        "123456789.123456789"
    );
}

#[test]
fn explicit_casts_keep_their_original_expression_and_declared_type() {
    for (sql, data_type, target) in [
        (
            "VALUES (CAST(123456789.123456789 AS DOUBLE))",
            decimal_type(),
            "DECIMAL(18, 9)",
        ),
        (
            "VALUES (CAST([123456789.123456789, 1.25e2] AS ARRAY<DOUBLE>))",
            array_type(decimal_type()),
            "ARRAY<DECIMAL(18, 9)>",
        ),
    ] {
        let source = parse_query(sql);
        let columns = [column("d", data_type)];
        let shaped = shape_insert_source(&source, &[], &columns, &columns)
            .unwrap()
            .unwrap();
        assert_eq!(
            cast_input(&rows(&shaped)[0][0], target),
            &rows(&source)[0][0]
        );
    }
}

#[test]
fn variant_direct_and_explicit_cast_inputs_use_canonical_payloads() {
    // Published VARIANT layout: u32 little-endian total size, sorted empty
    // metadata dictionary (11 00 00), then primitive INT8 (0C) and value 42.
    let expected = [5, 0, 0, 0, 0x11, 0, 0, 0x0c, 42];
    for sql in [
        "VALUES (parse_json('42'))",
        "SELECT parse_json(('42'))",
        "VALUES (CAST(parse_json('42') AS VARIANT))",
        "SELECT CAST(parse_json('42') AS VARIANT)",
        "VALUES (parse_json(CAST('42' AS VARCHAR)))",
        "SELECT parse_json(CAST('42' AS VARCHAR))",
    ] {
        let query = shape(sql, &[column("v", DataType::LargeBinary)]);
        let expr = match query.body.as_ref() {
            ast::SetExpr::Values(values) => &values.rows[0][0],
            body => projection_expr(select(body), 0),
        };
        let mut input = cast_input(expr, "VARIANT");
        if matches!(input, ast::Expr::Cast(_)) {
            input = cast_input(input, "VARIANT");
        }
        let bytes = literal_bytes(input);
        assert_eq!(bytes, expected, "{sql}");
        let value = novarocks_types::value::variant::VariantValue::from_serialized(&bytes).unwrap();
        assert_eq!(
            novarocks_types::value::variant::variant_to_i64(&value).unwrap(),
            42
        );
    }
}

#[test]
fn binary_literals_and_defaults_preserve_every_latin1_byte() {
    let expected: Vec<u8> = (0..=255).collect();
    let text: String = expected.iter().map(|&byte| char::from(byte)).collect();
    let mut source = parse_query("VALUES (NULL), (X'FF00')");
    let ast::SetExpr::Values(values) = source.body.as_mut() else {
        panic!("expected VALUES");
    };
    values.rows[0][0] = ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::String(text),
        span: Span::new(0, 0),
    });
    let columns = [column("b", DataType::Binary)];
    let query = shape_insert_source(&source, &[], &columns, &columns)
        .unwrap()
        .unwrap();
    assert_eq!(
        literal_bytes(cast_input(&rows(&query)[0][0], "VARBINARY")),
        expected
    );
    assert_eq!(
        literal_bytes(cast_input(&rows(&query)[1][0], "VARBINARY")),
        [255, 0]
    );
    let mut default = columns[0].clone();
    default.write_default = Some(ColumnDefault::Binary(expected.clone()));
    assert_eq!(
        literal_bytes(&omitted_insert_expr(&default).unwrap()),
        expected
    );
    assert_eq!(
        literal_bytes(cast_input(
            &rows(&shape("VALUES ('ÿ')", &columns))[0][0],
            "VARBINARY"
        )),
        [255]
    );
    assert!(shape_insert_source(&parse_query("VALUES ('€')"), &[], &columns, &columns).is_err());
}

#[test]
fn recursive_nonempty_defaults_preserve_decimal_coefficients_and_nulls() {
    let decimal_default = ColumnDefault::Decimal {
        unscaled: -123456789123456789,
        precision: 18,
        scale: 9,
    };
    let data_type = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("a", array_type(decimal_type()), true)),
        Arc::new(Field::new(
            "m",
            map_type(DataType::Int32, decimal_type()),
            true,
        )),
    ]));
    let mut nested = column("s", data_type);
    nested.write_default = Some(ColumnDefault::Struct(vec![
        (
            "a".into(),
            ColumnDefault::Array(vec![decimal_default.clone(), ColumnDefault::Null]),
        ),
        (
            "m".into(),
            ColumnDefault::Map(vec![(ColumnDefault::Int32(7), decimal_default)]),
        ),
    ]));
    let ast::Expr::Tuple(structure) = omitted_insert_expr(&nested).unwrap() else {
        panic!("expected structured default");
    };
    let ast::Expr::Array(array) = &structure.expressions[0] else {
        panic!("expected nonempty ARRAY default");
    };
    assert_eq!(number_text(&array.elements[0]), "-123456789.123456789");
    assert!(matches!(
        &array.elements[1],
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        })
    ));
    let ast::Expr::Map(map) = &structure.expressions[1] else {
        panic!("expected nonempty MAP default");
    };
    assert_eq!(number_text(&map.entries[0].key), "7");
    assert_eq!(number_text(&map.entries[0].value), "-123456789.123456789");

    let columns = [column("id", DataType::Int32), nested];
    let shaped = shape_insert_source(
        &parse_query("VALUES (1)"),
        &["id".into()],
        &columns,
        &columns,
    )
    .unwrap()
    .unwrap();
    let ast::Expr::Tuple(structure) = cast_input(
        &rows(&shaped)[0][1],
        "STRUCT<`a` ARRAY<DECIMAL(18, 9)>, `m` MAP<INT, DECIMAL(18, 9)>>",
    ) else {
        panic!("expected target-shaped structured default");
    };
    let ast::Expr::Array(array) = cast_input(&structure.expressions[0], "ARRAY<DECIMAL(18, 9)>")
    else {
        panic!("expected target-shaped ARRAY default");
    };
    assert_eq!(
        number_text(cast_input(&array.elements[0], "DECIMAL(18, 9)")),
        "-123456789.123456789"
    );
}

#[test]
fn decimal_defaults_reject_precision_or_scale_mismatch() {
    for (precision, scale) in [(19, 9), (18, 8)] {
        let mut decimal = column("d", decimal_type());
        decimal.write_default = Some(ColumnDefault::Decimal {
            unscaled: 123456789123456789,
            precision,
            scale,
        });
        assert!(
            omitted_insert_expr(&decimal).is_err(),
            "default DECIMAL({precision}, {scale}) must match target DECIMAL(18, 9)"
        );
    }
}

#[test]
fn supported_temporal_defaults_keep_their_original_supported_values() {
    for (data_type, default, expected) in [
        (
            DataType::Date32,
            ColumnDefault::Date {
                days_since_epoch: 1,
            },
            "1970-01-02",
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            ColumnDefault::TimestampMicros {
                micros_since_epoch: 1_000_000,
            },
            "1970-01-01 00:00:01",
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            ColumnDefault::TimestampNanos {
                nanos_since_epoch: 1_123_456_789,
            },
            "1970-01-01 00:00:01.123456789",
        ),
    ] {
        let mut temporal = column("t", data_type);
        temporal.write_default = Some(default);
        let ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::String(actual),
            ..
        }) = omitted_insert_expr(&temporal).unwrap()
        else {
            panic!("expected temporal default literal");
        };
        assert_eq!(actual, expected);
    }
}

#[test]
fn column_mapping_reorders_and_fills_only_omitted_columns() {
    let mut b = column("b", decimal_type());
    b.write_default = Some(ColumnDefault::Decimal {
        unscaled: 123456789123456789,
        precision: 18,
        scale: 9,
    });
    let columns = [
        column("a", DataType::Int32),
        b,
        column("c", DataType::Int32),
    ];
    let query = shape_insert_source(
        &parse_query("VALUES (30, 10)"),
        &["C".into(), "a".into()],
        &columns,
        &columns,
    )
    .unwrap()
    .unwrap();
    let rendered: Vec<_> = rows(&query)[0].iter().map(printer::print_expr).collect();
    assert_eq!(
        rendered,
        [
            "CAST(10 AS INT)",
            "CAST(123456789.123456789 AS DECIMAL(18, 9))",
            "CAST(30 AS INT)"
        ]
    );

    let source_columns = [columns[0].clone(), columns[2].clone()];
    let query = shape_insert_source(
        &parse_query("VALUES (10, 30)"),
        &[],
        &source_columns,
        &columns,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        rows(&query)[0]
            .iter()
            .map(printer::print_expr)
            .collect::<Vec<_>>(),
        rendered
    );
    for names in [["a", "A"], ["a", "missing"]] {
        assert!(
            shape_insert_source(
                &parse_query("VALUES (1, 2)"),
                &names.map(String::from),
                &columns,
                &columns
            )
            .is_err()
        );
    }
    assert!(
        shape_insert_source(
            &parse_query("VALUES (1)"),
            &["a".into(), "c".into()],
            &columns,
            &columns
        )
        .is_err()
    );
    let mut required = column("required", DataType::Int32);
    required.nullable = false;
    assert!(
        omitted_insert_expr(&required)
            .unwrap_err()
            .contains("omits required column")
    );
    let nullable = omitted_insert_expr(&column("optional", DataType::Int32)).unwrap();
    assert!(matches!(
        nullable,
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        })
    ));
}

#[test]
fn source_filters_aliases_distinct_and_general_query_controls_are_retained() {
    let columns = [column("d", decimal_type())];
    let source = parse_query("SELECT DISTINCT 123456789.123456789 AS d WHERE false HAVING d > 0");
    let shaped = shape_insert_source(&source, &[], &columns, &columns)
        .unwrap()
        .unwrap();
    let outer = select(&shaped.body);
    let expr = projection_expr(outer, 0);
    assert_eq!(
        number_text(cast_input(expr, "DECIMAL(18, 9)")),
        "123456789.123456789"
    );
    let [relation] = outer.from.as_slice() else {
        panic!("source controls must precede the target projection");
    };
    let ast::TableFactor::Derived { subquery, .. } = &relation.relation else {
        panic!("expected preserved source query");
    };
    assert!(subquery.body.syntax_eq(&source.body));
    assert!(
        matches!(&select(&subquery.body).projection[0], ast::SelectItem::ExprWithAlias { alias, .. } if alias.value == "d")
    );
    for sql in [
        "SELECT 123456789.123456789 ORDER BY 1 LIMIT 0",
        "WITH c AS (SELECT 123456789.123456789 AS d) SELECT d FROM c",
        "SELECT d FROM source_table WHERE d > 0",
        "VALUES (123456789.123456789) ORDER BY 1 LIMIT 0",
    ] {
        let query = parse_query(sql);
        assert!(
            shape_insert_source(&query, &[], &columns, &columns)
                .unwrap()
                .is_none(),
            "{sql}"
        );
    }
}
