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

use arrow::datatypes::{DataType, Field};
use novarocks::engine::insert_engine::{InsertOverwriteMode, InsertValue, parse_insert_statement};
use novarocks_catalog::schema::{ColumnDef, ColumnDefault};
use novarocks_frontend::dml::{InsertCommandSource, convert_insert_command, reorder_insert_rows};

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
    let InsertCommandSource::Values(rows) = command.source else {
        panic!("expected UNION ALL literals to normalize into one VALUES source");
    };
    assert_eq!(
        rows,
        vec![
            vec![InsertValue::Int(1)],
            vec![InsertValue::Int(2)],
            vec![InsertValue::Int(3)],
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
