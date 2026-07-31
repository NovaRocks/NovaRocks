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

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks::engine::insert_engine::{
    InsertOverwriteMode, InsertValue, QueryInsertBatch, QueryInsertColumn, parse_insert_statement,
};
use novarocks_catalog::schema::{ColumnDef, ColumnDefault};
use novarocks_frontend::dml::{
    InsertCommandSource, align_query_batch_to_target, convert_insert_command, reorder_insert_rows,
};

fn parse_insert(sql: &str) -> sqlparser::ast::Insert {
    parse_insert_statement(sql)
        .expect("statement should parse")
        .expect("statement should be INSERT")
}

fn column(
    name: &str,
    data_type: DataType,
    nullable: bool,
    write_default: Option<ColumnDefault>,
) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type,
        nullable,
        write_default,
        logical_type: None,
    }
}

fn query_batch(columns: Vec<QueryInsertColumn>, batch: RecordBatch) -> QueryInsertBatch {
    QueryInsertBatch {
        columns,
        batches: vec![batch],
    }
}

fn query_column(name: &str, data_type: DataType, nullable: bool) -> QueryInsertColumn {
    QueryInsertColumn {
        name: name.to_string(),
        data_type,
        nullable,
    }
}

#[test]
fn values_become_literal_rows() {
    let command =
        convert_insert_command(&parse_insert("INSERT INTO db.t VALUES (1, 'a'), (2, NULL)"))
            .expect("convert command");
    assert_eq!(command.target.parts, vec!["db", "t"]);
    assert_eq!(
        command.source,
        InsertCommandSource::Values(vec![
            vec![InsertValue::Int(1), InsertValue::String("a".to_string())],
            vec![InsertValue::Int(2), InsertValue::Null],
        ])
    );
}

#[test]
fn select_without_from_becomes_literal_row() {
    let command = convert_insert_command(&parse_insert("INSERT INTO t SELECT 40 + 2, 'x'"))
        .expect("convert command");
    assert_eq!(
        command.source,
        InsertCommandSource::SelectLiteralRow(vec![
            InsertValue::Int(42),
            InsertValue::String("x".to_string()),
        ])
    );
}

#[test]
fn select_with_from_uses_query_pipeline() {
    let command = convert_insert_command(&parse_insert("INSERT INTO t SELECT id FROM src"))
        .expect("convert command");
    assert!(matches!(command.source, InsertCommandSource::FromQuery(_)));
}

#[test]
fn non_constant_projection_uses_query_pipeline() {
    let command = convert_insert_command(&parse_insert("INSERT INTO t SELECT value + 1"))
        .expect("convert command");
    assert!(matches!(command.source, InsertCommandSource::FromQuery(_)));
}

#[test]
fn non_literal_values_function_uses_query_pipeline() {
    let command = convert_insert_command(&parse_insert(
        "INSERT INTO t VALUES (to_bitmap(11), hll_hash(5))",
    ))
    .expect("convert command");
    assert!(matches!(command.source, InsertCommandSource::FromQuery(_)));
}

#[test]
fn parse_json_values_fold_to_packed_variant_literal() {
    let command = convert_insert_command(&parse_insert(
        r#"INSERT INTO t VALUES (1, parse_json('{"a":1}'))"#,
    ))
    .expect("convert command");
    let InsertCommandSource::Values(rows) = command.source else {
        panic!("constant parse_json must stay on the literal VALUES path");
    };
    let InsertValue::String(packed) = &rows[0][1] else {
        panic!("parse_json must produce packed variant bytes");
    };
    assert!(
        packed.chars().all(|ch| u32::from(ch) <= 0xff),
        "packed variant must preserve every byte through the Latin-1 bridge"
    );
    let unpacked = packed.chars().map(|ch| ch as u8).collect::<Vec<_>>();
    let expected = novarocks::engine::insert_engine::encode_insert_variant_json(r#"{"a":1}"#)
        .expect("encode expected variant");
    assert_eq!(unpacked, expected);
}

#[test]
fn union_all_flattens_in_source_order() {
    let command = convert_insert_command(&parse_insert(
        "INSERT INTO t SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
    ))
    .expect("convert command");
    let InsertCommandSource::UnionAll(parts) = command.source else {
        panic!("expected UNION ALL source");
    };
    assert_eq!(
        parts,
        vec![
            InsertCommandSource::SelectLiteralRow(vec![InsertValue::Int(1)]),
            InsertCommandSource::SelectLiteralRow(vec![InsertValue::Int(2)]),
            InsertCommandSource::SelectLiteralRow(vec![InsertValue::Int(3)]),
        ]
    );
}

#[test]
fn union_distinct_is_rejected() {
    let error =
        convert_insert_command(&parse_insert("INSERT INTO t SELECT 1 UNION SELECT 2")).unwrap_err();
    assert!(error.contains("requires UNION ALL"), "{error}");
}

#[test]
fn dynamic_overwrite_marker_is_removed() {
    let command = convert_insert_command(&parse_insert(
        "INSERT OVERWRITE PARTITIONS TABLE db.t VALUES (1)",
    ))
    .expect("convert dynamic overwrite");
    assert_eq!(command.target.parts, vec!["db", "t"]);
    assert_eq!(
        command.overwrite_mode,
        InsertOverwriteMode::DynamicPartitions
    );
}

#[test]
fn unsupported_insert_target_is_rejected() {
    let mut statement = parse_insert("INSERT INTO t SELECT remote('localhost')");
    let source = statement.source.as_ref().expect("INSERT source");
    let sqlparser::ast::SetExpr::Select(select) = source.body.as_ref() else {
        panic!("expected SELECT source");
    };
    let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(function)) =
        &select.projection[0]
    else {
        panic!("expected function projection");
    };
    statement.table = sqlparser::ast::TableObject::TableFunction(function.clone());
    let error = convert_insert_command(&statement).unwrap_err();
    assert!(error.contains("unsupported INSERT target"), "{error}");
}

#[test]
fn reorders_explicit_columns_and_fills_missing_nullable_with_null() {
    let target = vec![
        column("a", DataType::Int64, true, None),
        column("b", DataType::Int64, false, None),
    ];
    let rows = reorder_insert_rows(&[vec![InsertValue::Int(7)]], &["b".to_string()], &target)
        .expect("reorder rows");
    assert_eq!(rows, vec![vec![InsertValue::Null, InsertValue::Int(7)]]);
}

#[test]
fn rejects_duplicate_insert_columns() {
    let target = vec![column("a", DataType::Int64, true, None)];
    let error = reorder_insert_rows(
        &[vec![InsertValue::Int(1), InsertValue::Int(2)]],
        &["a".to_string(), "a".to_string()],
        &target,
    )
    .unwrap_err();
    assert!(error.contains("duplicate INSERT column"), "{error}");
}

#[test]
fn rejects_unknown_insert_column() {
    let target = vec![column("a", DataType::Int64, true, None)];
    let error = reorder_insert_rows(
        &[vec![InsertValue::Int(1)]],
        &["missing".to_string()],
        &target,
    )
    .unwrap_err();
    assert!(error.contains("unknown INSERT column"), "{error}");
}

#[test]
fn rejects_missing_required_column() {
    let target = vec![
        column("required", DataType::Int64, false, None),
        column("provided", DataType::Int64, false, None),
    ];
    let error = reorder_insert_rows(
        &[vec![InsertValue::Int(1)]],
        &["provided".to_string()],
        &target,
    )
    .unwrap_err();
    assert!(
        error.contains("omits required column `required`"),
        "{error}"
    );
}

#[test]
fn preserves_array_map_struct_literals() {
    let values = vec![
        InsertValue::Array(vec![InsertValue::Int(1), InsertValue::Null]),
        InsertValue::Map(vec![(
            InsertValue::String("key".to_string()),
            InsertValue::Float(5.5),
        )]),
        InsertValue::Struct(vec![
            InsertValue::Int(100),
            InsertValue::String("abc".to_string()),
        ]),
    ];
    let target = vec![
        column(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
            None,
        ),
        column(
            "map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            Arc::new(Field::new("value", DataType::Float64, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
            None,
        ),
        column(
            "row",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("id", DataType::Int64, false)),
                    Arc::new(Field::new("name", DataType::Utf8, true)),
                ]
                .into(),
            ),
            true,
            None,
        ),
    ];
    assert_eq!(
        reorder_insert_rows(&[values.clone()], &[], &target).expect("preserve values"),
        vec![values]
    );
}

#[test]
fn aligns_query_batch_by_target_column_order() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("a", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![20])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .expect("source batch");
    let result = query_batch(
        vec![
            query_column("b", DataType::Int64, false),
            query_column("a", DataType::Int64, false),
        ],
        batch,
    );
    let target = vec![
        column("a", DataType::Int64, false, None),
        column("b", DataType::Int64, false, None),
    ];

    let aligned =
        align_query_batch_to_target(&result, &["b".to_string(), "a".to_string()], &target)
            .expect("align batch");
    assert_eq!(
        aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    assert_eq!(
        aligned
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        20
    );
}

#[test]
fn casts_query_batch_to_target_types() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![Some("42"), None]))],
    )
    .expect("source batch");
    let result = query_batch(vec![query_column("value", DataType::Utf8, true)], batch);
    let aligned = align_query_batch_to_target(
        &result,
        &[],
        &[column("value", DataType::Int64, true, None)],
    )
    .expect("cast batch");
    let values = aligned
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 output");
    assert_eq!(values.value(0), 42);
    assert!(values.is_null(1));
}

#[test]
fn rejects_query_output_arity_mismatch() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("source batch");
    let result = query_batch(vec![query_column("value", DataType::Int64, false)], batch);
    let error = align_query_batch_to_target(
        &result,
        &[],
        &[
            column("a", DataType::Int64, false, None),
            column("b", DataType::Int64, false, None),
        ],
    )
    .unwrap_err();
    assert!(error.contains("column count mismatch"), "{error}");
}

#[test]
fn applies_write_defaults() {
    let target = vec![
        column("a", DataType::Int64, false, Some(ColumnDefault::Int64(99))),
        column("b", DataType::Int64, false, None),
    ];
    let reordered = reorder_insert_rows(&[vec![InsertValue::Int(7)]], &["b".to_string()], &target)
        .expect("apply row default");
    assert_eq!(
        reordered,
        vec![vec![InsertValue::Int(99), InsertValue::Int(7)]]
    );

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![7, 8]))],
    )
    .expect("source batch");
    let result = query_batch(vec![query_column("b", DataType::Int64, false)], batch);
    let aligned = align_query_batch_to_target(&result, &["b".to_string()], &target)
        .expect("apply batch default");
    let defaults = aligned
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 defaults");
    assert_eq!(defaults.values(), &[99, 99]);
}

#[test]
fn supports_largeint_and_integral_float_array_inputs() {
    let values = vec![
        InsertValue::String("-170141183460469231731687303715884105728".to_string()),
        InsertValue::Array(vec![InsertValue::Float(1.0), InsertValue::Float(2.0)]),
    ];
    let target = vec![
        column("large", DataType::FixedSizeBinary(16), false, None),
        column(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
            None,
        ),
    ];
    assert_eq!(
        reorder_insert_rows(&[values.clone()], &[], &target).expect("preserve inputs"),
        vec![values]
    );
}

#[test]
fn concatenates_multiple_query_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let result = QueryInsertBatch {
        columns: vec![query_column("value", DataType::Float64, false)],
        batches: vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Float64Array::from(vec![1.0]))],
            )
            .unwrap(),
            RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![2.0, 3.0]))])
                .unwrap(),
        ],
    };
    let aligned = align_query_batch_to_target(
        &result,
        &[],
        &[column("value", DataType::Float64, false, None)],
    )
    .expect("concat batches");
    assert_eq!(aligned.num_rows(), 3);
}
