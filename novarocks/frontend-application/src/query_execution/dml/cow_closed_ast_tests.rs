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

use super::*;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_types::schema::SqlType;
use std::sync::Arc;

fn declared(declaration: &SqlType, carrier: &DataType) -> FunctionValueType {
    FunctionValueType::try_from_typed_binding(
        novarocks_types::execution_value_type_from_declaration(
            declaration,
            carrier,
            true,
            ValueTypeStorageLifetime::pure(),
        )
        .expect("explicit COW declaration"),
    )
    .expect("complete COW type")
}

fn old_cell(
    column: &ArrayRef,
    row: usize,
    declaration: &SqlType,
) -> std::result::Result<String, String> {
    let literal = novarocks_sql::literal::literal_from_batch(column, row)?;
    let text = crate::query_execution::dml::iceberg_writer::literal_to_sql_for_arrow_type(
        &literal,
        column.data_type(),
    )?;
    let field = novarocks_types::schema::ColumnDef {
        name: "value".into(),
        data_type: column.data_type().clone(),
        nullable: true,
        write_default: None,
        logical_type: Some(declaration.clone()),
    };
    let sql = crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(&text, &field)?;
    let query = novarocks_parser::parse(&format!("SELECT {sql}")).map_err(|e| e.to_string())?;
    let [Statement::Query(q)] = query.as_slice() else {
        panic!("single old query")
    };
    let SetExpr::Select(select) = q.body.as_ref() else {
        panic!("single SELECT")
    };
    let [SelectItem::UnnamedExpr(e)] = select.projection.as_slice() else {
        panic!("single expression")
    };
    Ok(novarocks_parser::printer::print_expr(e))
}
fn new_cell(column: &ArrayRef, row: usize, declaration: &FunctionValueType) -> Result<String> {
    let e = checked_value(column.as_ref(), row, &|| Ok(()))?;
    Ok(novarocks_parser::printer::print_expr(&cast(
        e,
        declaration,
        &|| Ok(()),
    )?))
}

#[test]
fn small_values_keep_original_literal_caster_and_parser_intersection() {
    let list = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>([
        Some(vec![Some(-2), None, Some(3)]),
        None,
    ]);
    let structure = StructArray::from(vec![
        (
            Arc::new(arrow::datatypes::Field::new("a`b", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(-4), None])) as ArrayRef,
        ),
        (
            Arc::new(arrow::datatypes::Field::new("text", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("a'\\b"), None])) as ArrayRef,
        ),
    ]);
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    map.keys().append_value("key");
    map.values().append_value(-7);
    map.append(true).unwrap();
    map.append(false).unwrap();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(map.finish()),
        Arc::new(BooleanArray::from(vec![Some(true), None])),
        Arc::new(Int8Array::from(vec![Some(-128), None])),
        Arc::new(Int16Array::from(vec![Some(-32768), None])),
        Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
        Arc::new(Float32Array::from(vec![Some(-0.1), None])),
        Arc::new(Float64Array::from(vec![Some(-0.0), None])),
        Arc::new(
            Decimal128Array::from(vec![Some(-1001), None])
                .with_precision_and_scale(12, 3)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec![Some("a'\\b"), None])),
        Arc::new(BinaryArray::from(vec![Some(&b"\x00\xFF\x27"[..]), None])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(&b"\x00\xFF\x27"[..]),
            None,
        ])),
        Arc::new(Date32Array::from(vec![Some(-1), None])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(1_234_567), None])),
        Arc::new(list),
        Arc::new(structure),
    ];
    let declarations = [
        SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::BigInt)),
        SqlType::Boolean,
        SqlType::TinyInt,
        SqlType::SmallInt,
        SqlType::Int,
        SqlType::BigInt,
        SqlType::Float,
        SqlType::Double,
        SqlType::Decimal {
            precision: 12,
            scale: 3,
        },
        SqlType::String,
        SqlType::Binary,
        SqlType::Variant,
        SqlType::Date,
        SqlType::DateTime,
        SqlType::Array(Box::new(SqlType::BigInt)),
        SqlType::Struct(vec![
            ("a`b".into(), SqlType::BigInt),
            ("text".into(), SqlType::String),
        ]),
    ];
    assert_eq!(columns.len(), declarations.len());
    for (column, declaration) in columns.into_iter().zip(declarations) {
        let complete = declared(&declaration, column.data_type());
        for row in 0..column.len() {
            assert_eq!(
                new_cell(&column, row, &complete).unwrap(),
                old_cell(&column, row, &declaration).unwrap(),
                "type {:?} row {row}",
                column.data_type()
            );
        }
    }
}

#[test]
fn unsupported_nonnull_values_stay_unsupported_but_null_first_is_retained() {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(LargeStringArray::from(vec![Some("x"), None])),
        Arc::new(Time64MicrosecondArray::from(vec![Some(1), None])),
        Arc::new(TimestampNanosecondArray::from(vec![Some(1), None])),
    ];
    for (column, declaration) in
        columns
            .into_iter()
            .zip([SqlType::String, SqlType::Time, SqlType::DateTimeNs])
    {
        let complete = declared(&declaration, column.data_type());
        assert!(old_cell(&column, 0, &declaration).is_err());
        assert!(matches!(
            new_cell(&column, 0, &complete),
            Err(BuildError::OriginalSemantic(_))
        ));
        assert_eq!(
            new_cell(&column, 1, &complete).unwrap(),
            old_cell(&column, 1, &declaration).unwrap()
        );
    }
    for scale in [-1, 0] {
        let value = if scale == 0 { i128::MAX } else { 1 };
        let a: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(value), None])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        );
        let declaration = SqlType::Decimal {
            precision: 38,
            scale,
        };
        let complete = declared(&declaration, a.data_type());
        assert!(old_cell(&a, 0, &declaration).is_err());
        assert!(new_cell(&a, 0, &complete).is_err());
        if scale < 0 {
            assert!(old_cell(&a, 1, &declaration).is_err());
            assert!(new_cell(&a, 1, &complete).is_err());
        }
    }
    let empty_struct = DataType::Struct(Vec::<arrow::datatypes::Field>::new().into());
    assert!(matches!(
        type_name(&declared(&SqlType::Struct(vec![]), &empty_struct), &|| Ok(
            ()
        )),
        Err(BuildError::OriginalSemantic(_))
    ));
    assert!(matches!(
        finite_float(f64::INFINITY),
        Err(BuildError::OriginalSemantic(_))
    ));
}

#[test]
fn invalid_borrowed_row_is_rejected_before_nested_ast_constructor_entry() {
    let mut list = ListBuilder::new(BooleanBuilder::new());
    for _ in 0..4096 {
        list.values().append_value(true);
    }
    list.append(true);
    let array = list.finish();
    let entered = std::cell::Cell::new(0_u64);
    let result = checked_value(&array, array.len(), &|| {
        entered.set(entered.get() + 1);
        Ok(())
    });
    assert!(matches!(
        result,
        Err(BuildError::InvalidSource("COW value row is out of bounds"))
    ));
    assert_eq!(entered.get(), 0);
}

#[test]
fn nested_constructor_still_checks_original_scope_during_construction() {
    let array = ListArray::from_iter_primitive::<arrow::datatypes::Int8Type, _, _>([Some(vec![
            Some(1);
            600
        ])]);
    let checks = std::cell::Cell::new(0_u64);
    let result = checked_value(&array, 0, &|| {
        checks.set(checks.get() + 1);
        if checks.get() == 2 {
            Err(BuildError::InvalidSource("original scope closed"))
        } else {
            Ok(())
        }
    });
    assert!(matches!(
        result,
        Err(BuildError::InvalidSource("original scope closed"))
    ));
    assert_eq!(checks.get(), 2);
}

#[test]
fn every_generated_nonempty_slot_is_exact_and_empty_values_are_rejected() {
    let a: ArrayRef = Arc::new(BinaryArray::from(vec![&b"abc"[..]]));
    let e = checked_value(a.as_ref(), 0, &|| Ok(())).unwrap();
    let Expr::Literal(Literal {
        kind: LiteralKind::HexString(s),
        ..
    }) = e
    else {
        panic!("hex")
    };
    assert_eq!((s.as_str(), s.len(), s.capacity()), ("616263", 6, 6));
    let mut row = exact_vec(1).unwrap();
    row.push(
        cast(
            bool_value(true),
            &declared(&SqlType::Boolean, &DataType::Boolean),
            &|| Ok(()),
        )
        .unwrap(),
    );
    let mut rows = exact_vec(1).unwrap();
    rows.push(row);
    let mut aliases = exact_vec(1).unwrap();
    aliases.push(ident("__nr_v_0", true).unwrap());
    let from = values(rows, "__nr_values", aliases).unwrap();
    let mut projection = exact_vec(1).unwrap();
    projection.push(item(column("__nr_values", "__nr_v_0").unwrap(), "value").unwrap());
    let query = select(projection, from, None, None).unwrap();
    assert!(!novarocks_parser::printer::print_query(&query).is_empty());
    assert!(values(Vec::new(), "x", Vec::new()).is_err());
}

fn cast_sql(value: &FunctionValueType) -> String {
    novarocks_parser::printer::print_expr(&cast(null(), value, &|| Ok(())).unwrap())
}
#[test]
fn complete_domains_determine_same_carrier_cast_spelling() {
    let text = declared(&SqlType::String, &DataType::Utf8);
    let json = declared(&SqlType::Json, &DataType::Utf8);
    assert_eq!(cast_sql(&text), "CAST(NULL AS STRING)");
    assert_eq!(cast_sql(&json), "CAST(NULL AS JSON)");
    let binary = declared(&SqlType::Binary, &DataType::LargeBinary);
    let variant = declared(&SqlType::Variant, &DataType::LargeBinary);
    assert_eq!(cast_sql(&binary), "CAST(NULL AS VARBINARY)");
    assert_eq!(cast_sql(&variant), "CAST(NULL AS VARIANT)");
    let carrier = DataType::FixedSizeBinary(16);
    let plain_fixed = declared(&SqlType::Binary, &carrier);
    assert!(matches!(
        type_name(&plain_fixed, &|| Ok(())),
        Err(BuildError::OriginalSemantic(_))
    ));
    assert_eq!(
        cast_sql(&declared(&SqlType::LargeInt, &carrier)),
        "CAST(NULL AS LARGEINT)"
    );
    // UUID exists in the immutable vocabulary, but has no admitted CAST name.
    assert!(matches!(
        type_name(&declared(&SqlType::Uuid, &carrier), &|| Ok(())),
        Err(BuildError::OriginalSemantic(_))
    ));
    let fixed: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([&[0_u8; 16][..]].into_iter()).unwrap());
    assert!(matches!(
        new_cell(&fixed, 0, &declared(&SqlType::LargeInt, &carrier)),
        Err(BuildError::OriginalSemantic(_))
    ));
    let null_fixed = new_null_array(&carrier, 1);
    assert_eq!(
        new_cell(&null_fixed, 0, &declared(&SqlType::LargeInt, &carrier)).unwrap(),
        "CAST(NULL AS LARGEINT)"
    );
    assert!(matches!(
        new_cell(&null_fixed, 0, &plain_fixed),
        Err(BuildError::OriginalSemantic(_))
    ));
}
#[test]
fn nested_declared_domains_preserve_actual_cast_spelling() {
    use arrow::datatypes::Field;
    let leaf = Field::new("format_item", DataType::Utf8, false).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "nrLogicalType".into(),
            "opaque-source-label".into(),
        )]),
    );
    let carrier = DataType::Struct(
        vec![
            Field::new("j`son", DataType::List(Arc::new(leaf)), true),
            Field::new("amount", DataType::Decimal128(12, 3), false),
        ]
        .into(),
    );
    let declaration = SqlType::Struct(vec![
        ("j`son".into(), SqlType::Array(Box::new(SqlType::Json))),
        (
            "amount".into(),
            SqlType::Decimal {
                precision: 12,
                scale: 3,
            },
        ),
    ]);
    let complete = declared(&declaration, &carrier);
    type_name(&complete, &|| Ok(())).unwrap();
    let sql = cast_sql(&complete);
    assert!(sql.contains("ARRAY<JSON>"), "{sql}");
    assert!(sql.contains("j``son"), "{sql}");
    let parsed = novarocks_parser::parse(&format!("SELECT {sql}")).unwrap();
    assert_eq!(parsed.len(), 1);
    // Both original carrier child promises remain in the borrowed authority;
    // the SQL TypeName grammar does not contain a child-NULL annotation.
    let novarocks_type_contract::ValueKind::Struct(fields) = complete.value_type().kind() else {
        panic!("struct");
    };
    assert!(fields[0].value_type().nullable());
    let novarocks_type_contract::ValueKind::List(item) = fields[0].value_type().kind() else {
        panic!("list");
    };
    assert!(!item.nullable());
    assert!(!fields[1].value_type().nullable());
    for (declaration, carrier) in [
        (SqlType::Json, DataType::Utf8),
        (SqlType::Variant, DataType::LargeBinary),
        (
            SqlType::Decimal {
                precision: 38,
                scale: 9,
            },
            DataType::Decimal128(38, 9),
        ),
    ] {
        let complete = declared(&declaration, &carrier);
        assert!(type_name(&complete, &|| Ok(())).is_ok());
    }
}
#[test]
fn type_construction_preserves_first_original_caller_without_tail() {
    use novarocks_failure_contract::FailureProjection;
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorStopOwner, OriginalResultCheckClass,
    };
    use std::error::Error;

    let (control, root, binding, _capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let cancellation = ConnectorStopOwner::new();
    let context = crate::connector::connector_request_context(None, cancellation.view()).unwrap();
    let scope = crate::query_execution::original_cow_result_scope::from_original_binding(
        &binding, &context,
    )
    .unwrap();
    scope.check_active().expect("the original caller is active");
    cancellation.request_stop();
    let complete = declared(&SqlType::Json, &DataType::Utf8);
    let calls = std::cell::Cell::new(0);
    let failure = type_name(&complete, &|| {
        calls.set(calls.get() + 1);
        scope.check_active().map_err(BuildError::Original)
    })
    .unwrap_err();
    let BuildError::Original(original) = failure else {
        panic!("original caller");
    };
    assert_eq!(original.class(), OriginalResultCheckClass::Control);
    assert_eq!(
        original.failure_fact(),
        ConnectorErrorKind::Cancelled.failure_fact()
    );
    let source = original
        .source()
        .unwrap()
        .downcast_ref::<ConnectorError>()
        .unwrap();
    assert_eq!(source.kind(), ConnectorErrorKind::Cancelled);
    assert_eq!(source.message(), "connector operation was cancelled");
    assert_eq!(calls.get(), 1);
    drop(original);
    drop(scope);
    drop(binding);
    root.owner.complete();
    root.business.release();
    control.close_admission();
}

#[test]
fn authored_variant_fields_keep_exact_cast_identity() {
    for (declaration, expected) in [
        (SqlType::Binary, "VARBINARY"),
        (SqlType::Variant, "VARIANT"),
    ] {
        let complete = declared(&declaration, &DataType::LargeBinary);
        let column: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(&b"\x00\xFF\x27"[..]),
            None,
        ]));
        for row in 0..column.len() {
            let old = old_cell(&column, row, &declaration).unwrap();
            let new = new_cell(&column, row, &complete).unwrap();
            assert_eq!(new, old);
            assert!(old.ends_with(&format!(" AS {expected})")));
        }
        type_name(&complete, &|| Ok(())).unwrap();
        assert_eq!(
            novarocks_parser::printer::print_expr(&cast(null(), &complete, &|| Ok(())).unwrap()),
            format!("CAST(NULL AS {expected})")
        );
    }
    // A mismatched declaration/carrier cannot become a signed complete author.
    assert!(
        novarocks_types::execution_value_type_from_declaration(
            &SqlType::Variant,
            &DataType::Int64,
            true,
            ValueTypeStorageLifetime::pure(),
        )
        .is_err()
    );
}

#[test]
fn declared_cow_field_types_preserve_original_caster_supported_intersection() {
    let cases = [
        (DataType::FixedSizeBinary(16), SqlType::LargeInt),
        (DataType::Utf8, SqlType::Json),
        (DataType::Binary, SqlType::Hll),
        (DataType::Binary, SqlType::Bitmap),
        (DataType::FixedSizeBinary(16), SqlType::Uuid),
        (DataType::LargeBinary, SqlType::Object),
        (DataType::Binary, SqlType::Percentile),
        (
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::LargeBinary,
                true,
            ))),
            SqlType::Array(Box::new(SqlType::Variant)),
        ),
    ];
    for (carrier, declaration) in cases {
        let complete = declared(&declaration, &carrier);
        let old = crate::query_execution::dml::iceberg_writer::target_cast_expr_sql(
            "NULL",
            &novarocks_types::schema::ColumnDef::from_value_type(
                "value".into(),
                complete.clone(),
                None,
            )
            .unwrap(),
        );
        let new = cast(null(), &complete, &|| Ok(()));
        match (old, new) {
            (Ok(old), Ok(new)) => {
                assert_eq!(novarocks_parser::printer::print_expr(&new), old);
                assert!(type_name(&complete, &|| Ok(())).is_ok());
            }
            (Err(_), Err(_)) => assert!(type_name(&complete, &|| Ok(())).is_err()),
            (old, new) => {
                panic!("original/new COW cast support differs for {complete:?}: {old:?}, {new:?}")
            }
        }
    }
    // Fixed-width Binary has no lossless SQL CAST spelling. Do not restore the
    // old carrier-only VARBINARY downgrade while merging the upstream test.
    let fixed = declared(&SqlType::Binary, &DataType::FixedSizeBinary(16));
    assert!(type_name(&fixed, &|| Ok(())).is_err());
}
