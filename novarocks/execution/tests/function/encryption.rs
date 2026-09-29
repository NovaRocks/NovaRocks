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
#![allow(unused_imports)]

use crate::common;
use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::DataType;
use novarocks_execution::exec::expr::ExprId;
use novarocks_execution::exec::expr::function::FunctionKind;
use novarocks_execution::exec::expr::function::encryption::eval_encryption_function;
use novarocks_execution::exec::expr::{ExprArena, ExprNode, LiteralValue};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Tests migrated from encryption/dispatch.rs
// ---------------------------------------------------------------------------

#[test]
fn test_register_encryption_functions() {
    use novarocks_execution::exec::expr::function::encryption::register;
    let mut m = HashMap::new();
    register(&mut m);
    assert_eq!(m.get("md5"), Some(&FunctionKind::Encryption("md5")));
    assert_eq!(
        m.get("encode_fingerprint_sha256"),
        Some(&FunctionKind::Encryption("encode_fingerprint_sha256"))
    );
    assert_eq!(
        m.get("encode_sort_key"),
        Some(&FunctionKind::Encryption("encode_sort_key"))
    );
    assert_eq!(
        m.get("base64_decode_binary"),
        Some(&FunctionKind::Encryption("from_base64"))
    );
}

// ---------------------------------------------------------------------------
// Tests migrated from encryption/from_base64.rs
// ---------------------------------------------------------------------------

#[test]
fn test_from_base64_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let input = common::literal_string(&mut arena, "SGVsbG8=");

    let out = eval_encryption_function(
        "from_base64",
        &arena,
        expr,
        &[input],
        &common::chunk_len_1(),
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "Hello");
}

// ---------------------------------------------------------------------------
// Tests migrated from encryption/to_base64.rs
// ---------------------------------------------------------------------------

#[test]
fn test_to_base64_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let input = common::literal_string(&mut arena, "Hello");

    let out = eval_encryption_function("to_base64", &arena, expr, &[input], &common::chunk_len_1())
        .unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "SGVsbG8=");
}

// ---------------------------------------------------------------------------
// Tests migrated from encryption/md5.rs
// ---------------------------------------------------------------------------

#[test]
fn test_md5_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let input = common::literal_string(&mut arena, "abc");

    let out =
        eval_encryption_function("md5", &arena, expr, &[input], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "900150983cd24fb0d6963f7d28e17f72");
}

// ---------------------------------------------------------------------------
// Tests migrated from encryption/md5sum.rs
// ---------------------------------------------------------------------------

#[test]
fn test_md5sum_concat_and_skip_null() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let a = common::literal_string(&mut arena, "a");
    let b = common::literal_string(&mut arena, "b");
    let c = common::typed_null(&mut arena, DataType::Utf8);

    let out = eval_encryption_function("md5sum", &arena, expr, &[a, b, c], &common::chunk_len_1())
        .unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(out.value(0), "187ef4436122d1cc2f40dc2b92f0eba0");
}

// ---------------------------------------------------------------------------
// Tests migrated from encryption/md5sum_numeric.rs
// ---------------------------------------------------------------------------

#[test]
fn test_md5sum_numeric_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let a = common::literal_string(&mut arena, "ab");

    let out =
        eval_encryption_function("md5sum_numeric", &arena, expr, &[a], &common::chunk_len_1())
            .unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();

    let expected_u128 = u128::from_str_radix("187ef4436122d1cc2f40dc2b92f0eba0", 16).unwrap();
    assert_eq!(out.value(0), expected_u128 as i64);
}

#[test]
fn test_md5sum_numeric_skips_null_arguments() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::FixedSizeBinary(16));
    let null_arg = common::typed_null(&mut arena, DataType::Utf8);

    let out = eval_encryption_function(
        "md5sum_numeric",
        &arena,
        expr,
        &[null_arg],
        &common::chunk_len_1(),
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
    // The value should be the MD5 of empty string (all nulls are skipped)
    assert!(!out.is_null(0));
    assert_eq!(out.value(0).len(), 16);
}

// ---------------------------------------------------------------------------
// Tests migrated from encryption/sha2.rs
// ---------------------------------------------------------------------------

#[test]
fn test_sha2_256_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let s = common::literal_string(&mut arena, "abc");
    let len = common::literal_i64(&mut arena, 256);

    let out =
        eval_encryption_function("sha2", &arena, expr, &[s, len], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        out.value(0),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

// ---------------------------------------------------------------------------
// Tests migrated from encryption/sm3.rs
// ---------------------------------------------------------------------------

#[test]
fn test_sm3_basic() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Utf8);
    let input = common::literal_string(&mut arena, "abc");

    let out =
        eval_encryption_function("sm3", &arena, expr, &[input], &common::chunk_len_1()).unwrap();
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        out.value(0),
        "66c7f0f4 62eeedd9 d1f2d46b dc10e4e2 4167c487 5cf2f7a2 297da02b 8f4ba8e0"
    );
}

// ---------------------------------------------------------------------------
// Tests migrated from encryption/to_binary.rs
// ---------------------------------------------------------------------------

#[test]
fn test_to_binary_hex_default() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Binary);
    let input = common::literal_string(&mut arena, "4142");

    let out = eval_encryption_function("to_binary", &arena, expr, &[input], &common::chunk_len_1())
        .unwrap();
    let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(out.value(0), b"AB");
}

#[test]
fn test_to_binary_utf8_format() {
    let mut arena = ExprArena::default();
    let expr = common::typed_null(&mut arena, DataType::Binary);
    let input = common::literal_string(&mut arena, "AB");
    let format = common::literal_string(&mut arena, "utf8");

    let out = eval_encryption_function(
        "to_binary",
        &arena,
        expr,
        &[input, format],
        &common::chunk_len_1(),
    )
    .unwrap();
    let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(out.value(0), b"AB");
}

#[test]
fn fingerprint_alias_ignores_real_list_columns_without_ignoring_scalar_nulls() {
    use arrow::array::ListArray;
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_execution::exec::chunk::{Chunk, ChunkSchema};
    use novarocks_types::SlotId;
    use std::sync::Arc;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(7), None])),
        Arc::new(StringArray::from(vec![Some("x"), None])),
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ])),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, columns.clone()).unwrap();
    let slots = [SlotId::new(1), SlotId::new(2), SlotId::new(3)];
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
    let mut arena = ExprArena::default();
    let args: Vec<_> = slots
        .iter()
        .zip(&columns)
        .map(|(slot, a)| arena.push_typed(ExprNode::SlotId(*slot), a.data_type().clone()))
        .collect();
    let output = common::typed_null(&mut arena, DataType::Binary);
    // Independent SHA256 of [INT64 marker4, LE64(7), STRING marker8, b"x"]
    // and of two scalar NULL marker bytes. The list contributes no bytes.
    let expected = [
        "4E3BB87E1137337244E0B6B279F59DBC01109E9DECD81D33283D5F06CCC7E11C",
        "96A296D224F285C67BEE93C30F8A309157F0DAA35DC5B87E410B78630A09CFC7",
    ];
    for name in ["encode_row_id", "encode_fingerprint_sha256"] {
        let with_list = eval_encryption_function(name, &arena, output, &args, &chunk).unwrap();
        let without_list =
            eval_encryption_function(name, &arena, output, &args[..2], &chunk).unwrap();
        let list_only = eval_encryption_function(name, &arena, output, &args[2..], &chunk).unwrap();
        let mixed = with_list.as_any().downcast_ref::<BinaryArray>().unwrap();
        let plain = without_list.as_any().downcast_ref::<BinaryArray>().unwrap();
        let ignored = list_only.as_any().downcast_ref::<BinaryArray>().unwrap();
        for row in 0..2 {
            assert!(!mixed.is_null(row));
            assert_eq!(mixed.value(row), plain.value(row));
            let hex: String = mixed
                .value(row)
                .iter()
                .map(|b| format!("{b:02X}"))
                .collect();
            assert_eq!(hex, expected[row]);
            let empty_hex: String = ignored
                .value(row)
                .iter()
                .map(|b| format!("{b:02X}"))
                .collect();
            assert_eq!(
                empty_hex,
                "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"
            );
        }
    }
    let err =
        eval_encryption_function("encode_sort_key", &arena, output, &args, &chunk).unwrap_err();
    assert!(err.contains("unsupported argument type"), "{err}");
}
