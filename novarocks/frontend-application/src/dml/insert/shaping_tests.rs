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
use novarocks_parser::{SYNTHETIC, Span, ast, printer};
use novarocks_sql::analyze_error::AnalyzeErrorKind;
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
    let query = novarocks_parser::parser::parse_query(sql).expect("test query should parse");
    query.clone()
}

fn analyze_shaped_query(query: ast::Query) -> Result<(), novarocks_sql::compiler::SqlCompileError> {
    use novarocks_sql::compiler::{
        SessionOptimizerSettings, SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent,
        SqlCompiler, SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext,
        SqlStatementInput,
    };
    let catalog = novarocks_sql::planning::catalog::PlannerMemoryCatalog::default();
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let functions = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(Box::new(query)),
        SqlCompileIntent::Query,
        SqlSessionContext {
            current_catalog: None,
            current_database: "db".into(),
            optimizer_settings: SessionOptimizerSettings::default(),
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        },
        SqlPlanningEnvironment::Distributed,
        &snapshot,
        &functions,
        crate::query_execution::constant_eval::constant_evaluator(),
        None,
        crate::application::test_constant_policy(),
        SqlCompileControl::unbounded(),
    ))?
    .into_pending()
    .map(|_| ())
}

// Replay the c8890b1dd CAST author: parse only the type template, then install
// the original typed expression without printing or reparsing that expression.
fn baseline_target_cast(expr: ast::Expr, type_sql: &str) -> ast::Expr {
    let template = parse_query(&format!("SELECT CAST(NULL AS {type_sql})"));
    let ast::Expr::Cast(cast) = projection_expr(select(&template.body), 0) else {
        panic!("baseline type template");
    };
    let mut cast = cast.clone();
    cast.span = expr.span();
    cast.expression = expr;
    ast::Expr::Cast(cast)
}

fn baseline_default_values(default: ast::Expr, default_type_sql: &str) -> ast::Query {
    let mut query = parse_query("VALUES (1)");
    let ast::SetExpr::Values(values) = query.body.as_mut() else {
        panic!("baseline VALUES source");
    };
    let span = values.rows[0].span();
    let original = ast::ValuesRowCells::new(&values.rows[0])
        .get(0)
        .unwrap()
        .clone();
    values.rows[0] = ast::Expr::Row(ast::RowExpr {
        fields: vec![
            baseline_target_cast(original, "INT"),
            baseline_target_cast(default, default_type_sql),
        ]
        .into_iter()
        .map(|expression| ast::RowField {
            name: None,
            span: expression.span(),
            expression,
        })
        .collect(),
        span,
    });
    query
}

fn assert_existing_default_rejection(
    shaped: ast::Query,
    baseline: ast::Query,
    kind: AnalyzeErrorKind,
    message: &str,
) {
    assert!(shaped.syntax_eq(&baseline));
    let novarocks_sql::compiler::SqlCompileError::Analyze(original) =
        analyze_shaped_query(baseline).expect_err("the original AST has this analysis boundary")
    else {
        panic!("baseline must retain its analysis error");
    };
    let novarocks_sql::compiler::SqlCompileError::Analyze(current) =
        analyze_shaped_query(shaped).expect_err("the migrated AST keeps the original rejection")
    else {
        panic!("migrated AST must retain its analysis error");
    };
    assert_eq!(original.kind(), kind);
    assert_eq!(original.message(), message);
    assert_eq!(original.span(), Some(SYNTHETIC));
    assert_eq!(current.kind(), original.kind());
    assert_eq!(current.code(), original.code());
    assert_eq!(current.message(), original.message());
    assert_eq!(current.span(), original.span());
}

fn shape(sql: &str, columns: &[ColumnDef]) -> ast::Query {
    shape_insert_source(&parse_query(sql), &[], columns, columns)
        .expect("literal source should shape")
        .expect("literal source should use target shaping")
}

fn rows(query: &ast::Query) -> Vec<Vec<&ast::Expr>> {
    let ast::SetExpr::Values(values) = query.body.as_ref() else {
        panic!("expected VALUES");
    };
    values
        .rows
        .iter()
        .map(|row| {
            let cells = ast::ValuesRowCells::new(row);
            assert!(!cells.has_named_fields());
            cells.iter().collect()
        })
        .collect()
}

fn select(body: &ast::SetExpr) -> &ast::Select {
    match body {
        ast::SetExpr::Select(select) => select,
        ast::SetExpr::Query(subquery) => {
            let query = &subquery.query;
            select(&query.body)
        }
        _ => panic!("expected SELECT leaf"),
    }
}

fn projection_expr(select: &ast::Select, index: usize) -> &ast::Expr {
    match &select.projection[index] {
        ast::SelectItem::SingleColumn(column) => &column.expression,
        _ => panic!("expected expression projection"),
    }
}

fn cast_input<'a>(expr: &'a ast::Expr, target: &str) -> &'a ast::Expr {
    let ast::Expr::Cast(cast) = expr else {
        panic!("expected target CAST, got {expr:?}");
    };
    assert_eq!(printer::print_type(&cast.data_type).unwrap(), target);
    &cast.expression
}

fn number_text(expr: &ast::Expr) -> String {
    let ast::Expr::Literal(literal) = expr else {
        panic!("expected exact numeric literal, got {expr:?}");
    };
    match &literal.kind {
        ast::LiteralKind::Long { spelling, .. } | ast::LiteralKind::Decimal(spelling) => {
            spelling.clone()
        }
        ast::LiteralKind::Double(value) => format!("{:?}", value.get()),
        _ => panic!("expected exact numeric literal, got {expr:?}"),
    }
}

fn literal_bytes(expr: &ast::Expr) -> Vec<u8> {
    let ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Binary(bytes),
        ..
    }) = expr
    else {
        panic!("expected binary literal, got {expr:?}");
    };
    bytes.clone()
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
    let source = parse_query(
        "VALUES ([123456789.123456789, 1.25e2, NULL], map(123456789.123456789, [123456789.123456789, NULL]), row(123456789.123456789, [123456789.123456789]))",
    );
    let query = shape_insert_source(&source, &[], &columns, &columns)
        .expect("literal source should shape")
        .expect("literal source should use target shaping");
    let row = &rows(&query)[0];
    let ast::Expr::Array(array) = cast_input(row[0], "ARRAY<DECIMAL(18, 9)>") else {
        panic!("expected direct ARRAY");
    };
    assert_eq!(
        number_text(cast_input(&array.values[0], "DECIMAL(18, 9)")),
        "123456789.123456789"
    );
    let exponent = cast_input(&array.values[1], "DECIMAL(18, 9)");
    let ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Double(value),
        ..
    }) = exponent
    else {
        panic!("source exponent must retain its Double category");
    };
    assert_eq!(value.get().to_bits(), 125.0_f64.to_bits());
    let ast::Expr::Array(source_array) = rows(&source)[0][0] else {
        panic!("source ARRAY");
    };
    assert_eq!(exponent, &source_array.values[1]);
    assert_eq!(exponent.span(), source_array.values[1].span());
    assert!(matches!(
        cast_input(&array.values[2], "DECIMAL(18, 9)"),
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        })
    ));

    let ast::Expr::FunctionCall(map) =
        cast_input(row[1], "MAP<DECIMAL(18, 9), ARRAY<DECIMAL(18, 9)>>")
    else {
        panic!("expected MAP constructor");
    };
    assert_eq!(
        number_text(cast_input(&map.arguments[0].value, "DECIMAL(18, 9)")),
        "123456789.123456789"
    );
    let ast::Expr::Array(map_array) = cast_input(&map.arguments[1].value, "ARRAY<DECIMAL(18, 9)>")
    else {
        panic!("expected MAP array value");
    };
    assert_eq!(
        number_text(cast_input(&map_array.values[0], "DECIMAL(18, 9)")),
        "123456789.123456789"
    );

    let ast::Expr::Row(structure) = cast_input(
        &row[2],
        "STRUCT<`d` DECIMAL(18, 9), `a` ARRAY<DECIMAL(18, 9)>>",
    ) else {
        panic!("expected ROW constructor");
    };
    assert_eq!(
        number_text(cast_input(
            &structure.fields[0].expression,
            "DECIMAL(18, 9)"
        )),
        "123456789.123456789"
    );
    let ast::Expr::Array(struct_array) =
        cast_input(&structure.fields[1].expression, "ARRAY<DECIMAL(18, 9)>")
    else {
        panic!("expected STRUCT array field");
    };
    assert_eq!(
        number_text(cast_input(&struct_array.values[0], "DECIMAL(18, 9)")),
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
        assert_eq!(cast_input(rows(&shaped)[0][0], target), rows(&source)[0][0]);
    }
}

#[test]
fn explicit_safe_cast_keeps_original_child_type_and_source_positions() {
    let mut source = parse_query("VALUES (CAST(123456789.123456789 AS DOUBLE))");
    let ast::SetExpr::Values(values) = source.body.as_mut() else {
        panic!("VALUES");
    };
    let ast::Expr::Cast(cast) = super::values_cell_mut(&mut values.rows[0], 0)
        .unwrap()
        .unwrap()
    else {
        panic!("explicit CAST");
    };
    cast.safe = true;
    let original = ast::ValuesRowCells::new(&values.rows[0])
        .get(0)
        .unwrap()
        .clone();
    let columns = [column("d", decimal_type())];
    let shaped = shape_insert_source(&source, &[], &columns, &columns)
        .unwrap()
        .unwrap();
    let preserved = cast_input(rows(&shaped)[0][0], "DECIMAL(18, 9)");
    assert_eq!(preserved, &original);
    let ast::Expr::Cast(cast) = preserved else {
        panic!("original safe CAST");
    };
    assert!(cast.safe);
    assert_eq!(preserved.span(), original.span());
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
        let variant = ColumnDef::from_value_type(
            "v".into(),
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                novarocks_type_contract::ValueLogicalType::Variant,
            )
            .unwrap(),
            None,
        )
        .unwrap();
        let query = shape(sql, &[variant]);
        let expr = match query.body.as_ref() {
            ast::SetExpr::Values(values) => {
                ast::ValuesRowCells::new(&values.rows[0]).get(0).unwrap()
            }
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
    *super::values_cell_mut(&mut values.rows[0], 0)
        .unwrap()
        .unwrap() = ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::String(text),
        span: Span::new(0, 0),
    });
    let columns = [column("b", DataType::Binary)];
    let query = shape_insert_source(&source, &[], &columns, &columns)
        .unwrap()
        .unwrap();
    assert_eq!(
        literal_bytes(cast_input(rows(&query)[0][0], "VARBINARY")),
        expected
    );
    assert_eq!(
        literal_bytes(cast_input(rows(&query)[1][0], "VARBINARY")),
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
            rows(&shape("VALUES ('ÿ')", &columns))[0][0],
            "VARBINARY"
        )),
        [255]
    );
    assert!(shape_insert_source(&parse_query("VALUES ('€')"), &[], &columns, &columns).is_err());
}

#[test]
fn typed_decimal_defaults_keep_coefficients_beyond_source_integer_width() {
    let coefficient = 12345678901234567890123456789012345678_i128;
    let mut decimal = column("d", DataType::Decimal128(38, 0));
    decimal.write_default = Some(ColumnDefault::Decimal {
        unscaled: coefficient,
        precision: 38,
        scale: 0,
    });
    assert_eq!(
        number_text(&omitted_insert_expr(&decimal).unwrap()),
        coefficient.to_string()
    );
    let columns = [column("id", DataType::Int32), decimal];
    let shaped = shape_insert_source(
        &parse_query("VALUES (1)"),
        &["id".into()],
        &columns,
        &columns,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        number_text(cast_input(rows(&shaped)[0][1], "DECIMAL(38, 0)")),
        coefficient.to_string()
    );
    // Typed decimal defaults use their declared numeric category, even when
    // their exact coefficient exceeds the source integer literal width.
    let baseline = baseline_default_values(
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Decimal(coefficient.to_string()),
            span: SYNTHETIC,
        }),
        "DECIMAL(38, 0)",
    );
    assert!(shaped.syntax_eq(&baseline));
    analyze_shaped_query(baseline).expect("typed decimal coefficient should analyze");
    analyze_shaped_query(shaped).expect("shaped typed decimal coefficient should analyze");
}

#[test]
fn typed_single_field_struct_defaults_keep_their_values_row() {
    let mut structure = column(
        "s",
        DataType::Struct(Fields::from(vec![Arc::new(Field::new(
            "only",
            DataType::Int64,
            true,
        ))])),
    );
    structure.write_default = Some(ColumnDefault::Struct(vec![(
        "only".into(),
        ColumnDefault::Int64(7),
    )]));
    let original = omitted_insert_expr(&structure).unwrap();
    let ast::Expr::Row(tuple) = &original else {
        panic!("typed Struct value");
    };
    assert_eq!(tuple.fields.len(), 1);
    assert_eq!(number_text(&tuple.fields[0].expression), "7");
    let columns = [column("id", DataType::Int32), structure];
    let shaped = shape_insert_source(
        &parse_query("VALUES (1)"),
        &["id".into()],
        &columns,
        &columns,
    )
    .unwrap()
    .unwrap();
    let ast::Expr::Cast(cast) = rows(&shaped)[0][1] else {
        panic!("Struct target cast");
    };
    assert!(matches!(&cast.expression, ast::Expr::Row(tuple) if tuple.fields.len() == 1));
    let baseline = baseline_default_values(
        ast::Expr::Row(ast::RowExpr {
            fields: vec![ast::RowField {
                name: None,
                expression: baseline_target_cast(
                    ast::Expr::Literal(ast::Literal {
                        kind: ast::LiteralKind::Long {
                            spelling: "7".into(),
                            parsed_value: 7,
                        },
                        span: SYNTHETIC,
                    }),
                    "BIGINT",
                ),
                span: SYNTHETIC,
            }]
            .into(),
            span: SYNTHETIC,
        }),
        "STRUCT<`only` BIGINT>",
    );
    assert!(shaped.syntax_eq(&baseline));
    // This input originally failed at SQL's unsupported Row branch, not at
    // the VALUES wrapper. Preserve the exact default value as an independent
    // literal oracle while the original row binding now owns scalar analysis.
    let value = novarocks_sql::literal::expr_to_literal(rows(&shaped)[0][1]).unwrap();
    assert_eq!(
        value,
        novarocks_sql::semantic::Literal::Struct(vec![novarocks_sql::semantic::Literal::Int(7)])
    );
    let ast::Expr::Cast(cast) = rows(&shaped)[0][1] else {
        panic!("Struct target cast");
    };
    let ast::Expr::Row(row) = &cast.expression else {
        panic!("singleton value row");
    };
    assert_eq!(row.fields.len(), 1);
    assert_eq!(
        number_text(cast_input(&row.fields[0].expression, "BIGINT")),
        "7"
    );
    assert_eq!(
        columns[1].declared_value_type().unwrap().data_type(),
        &DataType::Struct(Fields::from(vec![Arc::new(Field::new(
            "only",
            DataType::Int64,
            true
        ))]))
    );
    // Both actual source forms now reach the original row overload. Its
    // installed-implementation boundary still rejects the default; preserving
    // literal values above does not imply successful compilation or execution.
    assert_existing_default_rejection(
        shaped,
        baseline,
        AnalyzeErrorKind::UnavailableImplementation,
        "selected overload `builtin.scalar/row/dynamic-v1` has no installed implementation for the selected carrier",
    );
}

#[test]
fn select_wrapper_normalizes_generated_defaults_and_retains_user_positions() {
    let mut decimal = column("d", DataType::Decimal128(5, 2));
    decimal.write_default = Some(ColumnDefault::Decimal {
        unscaled: -12345,
        precision: 5,
        scale: 2,
    });
    let columns = [column("id", DataType::Int32), decimal];
    let source = parse_query("SELECT\n  1 AS original");
    let shaped = shape_insert_source(&source, &["id".into()], &columns, &columns)
        .unwrap()
        .unwrap();
    let expected = parse_query(
        "SELECT CAST(1 AS INT), CAST(DECIMAL '-123.45' AS DECIMAL(5, 2)) FROM (SELECT 1 AS original) AS __nr_insert_literal_src",
    );
    let ast::SetExpr::Query(wrapper) = shaped.body.as_ref() else {
        panic!("target wrapper");
    };
    assert!(wrapper.query.as_ref().syntax_eq(&expected));
    let ast::SetExpr::Select(inner) = source.body.as_ref() else {
        panic!("original SELECT");
    };
    let outer = select(&shaped.body);
    let ast::Expr::Cast(cast) = projection_expr(outer, 0) else {
        panic!("target cast");
    };
    assert_eq!(cast.span, projection_expr(inner, 0).span());
    assert_eq!(cast.expression.span(), projection_expr(inner, 0).span());
    analyze_shaped_query(shaped).expect("the original generated default keeps its typed value");
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
    let ast::Expr::Row(structure) = omitted_insert_expr(&nested).unwrap() else {
        panic!("expected structured default");
    };
    let ast::Expr::Array(array) = &structure.fields[0].expression else {
        panic!("expected nonempty ARRAY default");
    };
    assert_eq!(number_text(&array.values[0]), "-123456789.123456789");
    assert!(matches!(
        &array.values[1],
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        })
    ));
    let ast::Expr::FunctionCall(map) = &structure.fields[1].expression else {
        panic!("expected nonempty ordinary MAP default");
    };
    assert_eq!(number_text(&map.arguments[0].value), "7");
    assert_eq!(number_text(&map.arguments[1].value), "-123456789.123456789");

    let columns = [column("id", DataType::Int32), nested];
    let shaped = shape_insert_source(
        &parse_query("VALUES (1)"),
        &["id".into()],
        &columns,
        &columns,
    )
    .unwrap()
    .unwrap();
    let ast::Expr::Row(structure) = cast_input(
        rows(&shaped)[0][1],
        "STRUCT<`a` ARRAY<DECIMAL(18, 9)>, `m` MAP<INT, DECIMAL(18, 9)>>",
    ) else {
        panic!("expected target-shaped structured default");
    };
    let ast::Expr::Array(array) =
        cast_input(&structure.fields[0].expression, "ARRAY<DECIMAL(18, 9)>")
    else {
        panic!("expected target-shaped ARRAY default");
    };
    assert_eq!(
        number_text(cast_input(&array.values[0], "DECIMAL(18, 9)")),
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
    let rendered: Vec<_> = rows(&query)[0]
        .iter()
        .map(|expr| printer::print_expression(expr).unwrap())
        .collect();
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
            .map(|expr| printer::print_expression(expr).unwrap())
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
    let Some(ast::Relation::Aliased(alias)) = &outer.from else {
        panic!("source controls must precede the target projection");
    };
    let ast::Relation::TableSubquery(node) = alias.relation.as_ref() else {
        panic!("expected preserved source query");
    };
    let subquery = &node.query;
    assert!(subquery.body.syntax_eq(&source.body));
    assert!(
        matches!(&select(&subquery.body).projection[0], ast::SelectItem::SingleColumn(column) if column.alias.as_ref().is_some_and(|alias| alias.value == "d"))
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

#[test]
fn literal_target_casts_keep_original_bitmap_and_hll_declarations() {
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    for (domain, source, target_name) in [
        // The test owns opaque cast preservation, independently of the
        // unavailable legacy to_bitmap overload. This real aggregate-backed
        // scalar subquery preserves a concrete Bitmap producer and its domain.
        (
            ValueLogicalType::Bitmap,
            "(SELECT bitmap_agg(10))",
            "BITMAP",
        ),
        (ValueLogicalType::Hll, "hll_hash(10)", "HLL"),
    ] {
        let declared =
            FunctionValueType::try_with_logical_type(DataType::Binary, true, domain).unwrap();
        let target = ColumnDef::from_value_type("v".into(), declared, None).unwrap();
        let query = shape(&format!("VALUES (({source}))"), &[target]);
        let original_call = cast_input(rows(&query)[0][0], target_name);
        assert!(ast::SyntaxEq::syntax_eq(
            original_call,
            &novarocks_parser::parser::parse_expression(source).unwrap(),
        ));
        analyze_shaped_query(query)
            .expect("the actual compiler must accept the original complete opaque cast");
        let error =
            analyze_shaped_query(parse_query(&format!("SELECT CAST(X'00' AS {target_name})")))
                .unwrap_err();
        assert!(
            error.to_string().contains("no matching declared overload"),
            "{error}"
        );
    }
}

#[test]
fn typed_float_defaults_keep_double_category_and_signed_zero() {
    for value in [-0.0_f64, 1.0, f64::MAX] {
        let mut column = column("f", DataType::Float64);
        column.write_default = Some(ColumnDefault::Float64 {
            bits: value.to_bits(),
        });
        let ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Double(actual),
            ..
        }) = omitted_insert_expr(&column).unwrap()
        else {
            panic!("typed floating default must remain a double");
        };
        assert_eq!(actual.get().to_bits(), value.to_bits());
    }
}

#[test]
fn select_projection_keeps_singleton_source_and_default_row_values() {
    let mut structure = column(
        "s",
        DataType::Struct(Fields::from(vec![Arc::new(Field::new(
            "only",
            DataType::Int64,
            true,
        ))])),
    );
    structure.write_default = Some(ColumnDefault::Struct(vec![(
        "only".into(),
        ColumnDefault::Int64(7),
    )]));
    let source = parse_query("SELECT row(7)");
    let shaped = shape_insert_source(
        &source,
        &["s".into()],
        &[structure.clone()],
        &[structure.clone()],
    )
    .unwrap()
    .unwrap();
    let expression = projection_expr(select(&shaped.body), 0);
    let ast::Expr::Row(row) = cast_input(expression, "STRUCT<`only` BIGINT>") else {
        panic!("source singleton Row must keep its value shape");
    };
    assert_eq!(row.fields.len(), 1);
    assert_eq!(
        novarocks_sql::literal::expr_to_literal(expression).unwrap(),
        novarocks_sql::semantic::Literal::Struct(vec![novarocks_sql::semantic::Literal::Int(7)])
    );
    let columns = [column("id", DataType::Int32), structure];
    let shaped = shape_insert_source(
        &parse_query("SELECT 1 AS id"),
        &["id".into()],
        &columns,
        &columns,
    )
    .unwrap()
    .unwrap();
    let expression = projection_expr(select(&shaped.body), 1);
    assert!(
        matches!(cast_input(expression, "STRUCT<`only` BIGINT>"), ast::Expr::Row(row) if row.fields.len() == 1)
    );
    assert_eq!(
        novarocks_sql::literal::expr_to_literal(expression).unwrap(),
        novarocks_sql::semantic::Literal::Struct(vec![novarocks_sql::semantic::Literal::Int(7)])
    );
}

#[test]
fn literal_values_shaping_preserves_outer_row_span_and_single_struct_cell() {
    let source = parse_query("VALUES (ROW(7))");
    let ast::SetExpr::Values(original) = source.body.as_ref() else {
        panic!("VALUES");
    };
    let outer_span = original.rows[0].span();
    let source_cells = ast::ValuesRowCells::new(&original.rows[0]);
    assert_eq!(source_cells.len(), 1);
    let ast::Expr::Row(inner) = source_cells.get(0).unwrap() else {
        panic!("inner Struct cell");
    };
    assert_eq!(inner.fields.len(), 1);
    let target = column(
        "s",
        DataType::Struct(Fields::from(vec![Arc::new(Field::new(
            "only",
            DataType::Int64,
            true,
        ))])),
    );
    let shaped = shape_insert_source(
        &source,
        &[],
        std::slice::from_ref(&target),
        std::slice::from_ref(&target),
    )
    .unwrap()
    .unwrap();
    let ast::SetExpr::Values(values) = shaped.body.as_ref() else {
        panic!("shaped VALUES");
    };
    assert_eq!(values.rows[0].span(), outer_span);
    let ast::Expr::Row(outer) = &values.rows[0] else {
        panic!("real outer row");
    };
    assert_eq!(outer.fields.len(), 1);
    assert!(outer.fields[0].name.is_none());
    assert_eq!(outer.fields[0].span, outer.fields[0].expression.span());
    let ast::Expr::Row(shaped_inner) =
        cast_input(&outer.fields[0].expression, "STRUCT<`only` BIGINT>")
    else {
        panic!("Struct value must remain one cell");
    };
    assert_eq!(shaped_inner.span, inner.span);
    assert_eq!(shaped_inner.fields.len(), 1);
    assert_eq!(shaped_inner.fields[0].name, inner.fields[0].name);
    assert_eq!(shaped_inner.fields[0].span, inner.fields[0].span);
    assert_eq!(
        cast_input(&shaped_inner.fields[0].expression, "BIGINT"),
        &inner.fields[0].expression
    );

    let mut named = source.clone();
    let ast::SetExpr::Values(values) = named.body.as_mut() else {
        unreachable!()
    };
    let ast::Expr::Row(outer) = &mut values.rows[0] else {
        unreachable!()
    };
    outer.fields[0].name = Some(novarocks_parser::build::ident_at(
        "named cell",
        true,
        Span::new(7, 12),
    ));
    let before = named.clone();
    assert_eq!(
        shape_insert_source(
            &named,
            &[],
            std::slice::from_ref(&target),
            std::slice::from_ref(&target)
        )
        .unwrap_err(),
        "INSERT literal VALUES source does not support named row fields"
    );
    assert_eq!(named, before);
}
