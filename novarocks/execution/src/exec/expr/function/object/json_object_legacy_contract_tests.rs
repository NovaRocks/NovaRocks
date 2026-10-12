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

//! Independent direct and Arena original contracts; production bodies are unchanged.
use super::json_object::eval_json_object;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer, i256};
use novarocks_types::SlotId;
use serde_json::{Value, json};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

fn setup(mut columns: Vec<ArrayRef>, rows: usize) -> (ExprArena, Vec<ExprId>, Chunk) {
    let empty = columns.is_empty();
    if empty {
        columns.push(Arc::new(Int32Array::from(vec![0; rows])));
    }
    let slots = (0..columns.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect::<Vec<_>>();
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let mut arena = ExprArena::default();
    let ids = if empty {
        Vec::new()
    } else {
        columns
            .iter()
            .enumerate()
            .map(|(i, a)| arena.push_typed(ExprNode::SlotId(slots[i]), a.data_type().clone()))
            .collect()
    };
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    (arena, ids, Chunk::new_with_chunk_schema(batch, schema))
}
fn outcome(columns: Vec<ArrayRef>, rows: usize) -> Value {
    let (arena, ids, chunk) = setup(columns, rows);
    match catch_unwind(AssertUnwindSafe(|| {
        eval_json_object(&arena, ExprId(usize::MAX), &ids, &chunk)
    })) {
        Ok(Ok(a)) => {
            assert_eq!(a.data_type(), &DataType::Utf8);
            let strings = a.as_any().downcast_ref::<StringArray>().unwrap();
            json!({"kind":"success","type":"Utf8","values":(0..a.len()).map(|r| (!a.is_null(r)).then(|| strings.value(r))).collect::<Vec<_>>()})
        }
        Ok(Err(e)) => json!({"kind":"whole_data","message":e}),
        Err(panic) => {
            json!({"kind":"panic","message":crate::exec::expr::pure_differential::panic_message(&panic)})
        }
    }
}
fn map() -> ArrayRef {
    let fields = vec![
        Arc::new(Field::new("key", DataType::Utf8, false)),
        Arc::new(Field::new("value", DataType::Int64, true)),
    ];
    let entries = StructArray::new(
        fields.into(),
        vec![
            Arc::new(StringArray::from(vec!["é", "b"])),
            Arc::new(Int64Array::from(vec![Some(4), None])),
        ],
        None,
    );
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 2, 2].into()),
        entries,
        Some(NullBuffer::from(vec![true, false])),
        false,
    ))
}
fn carriers() -> Vec<(&'static str, ArrayRef)> {
    let mut carriers: Vec<(&str, ArrayRef)> = vec![
        ("null", Arc::new(NullArray::new(2))),
        ("bool", Arc::new(BooleanArray::from(vec![Some(true), None]))),
        ("i8", Arc::new(Int8Array::from(vec![Some(i8::MIN), None]))),
        (
            "i16",
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None])),
        ),
        (
            "i32",
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
        ),
        (
            "i64",
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
        ),
        ("f32", Arc::new(Float32Array::from(vec![Some(-0.0), None]))),
        ("f64", Arc::new(Float64Array::from(vec![Some(0.5), None]))),
        (
            "utf8",
            Arc::new(StringArray::from(vec![
                Some("[1,{\"z\":true,\"a\":\"é中\"}]"),
                None,
            ])),
        ),
        (
            "binary",
            Arc::new(BinaryArray::from(vec![Some(&[0, 255][..]), None])),
        ),
        (
            "large_binary",
            Arc::new(LargeBinaryArray::from(vec![Some(&[0, 255][..]), None])),
        ),
        ("date", Arc::new(Date32Array::from(vec![Some(0), None]))),
        (
            "decimal128",
            Arc::new(
                Decimal128Array::from(vec![Some(-500), None])
                    .with_precision_and_scale(18, 4)
                    .unwrap(),
            ),
        ),
        (
            "decimal256",
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from_i128(i128::MAX) * i256::from_i128(10)),
                    None,
                ])
                .with_precision_and_scale(76, 4)
                .unwrap(),
            ),
        ),
        (
            "largeint",
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(i128::MIN.to_be_bytes()), None].into_iter(),
                    16,
                )
                .unwrap(),
            ),
        ),
        (
            "list",
            Arc::new(ListArray::from_iter_primitive::<
                arrow::datatypes::Int64Type,
                _,
                _,
            >([Some(vec![Some(2), None]), None])),
        ),
        (
            "struct",
            Arc::new(StructArray::new(
                vec![Arc::new(Field::new("s", DataType::Utf8, true))].into(),
                vec![Arc::new(StringArray::from(vec![Some("x"), None]))],
                Some(NullBuffer::from(vec![true, false])),
            )),
        ),
        ("map", map()),
    ];
    macro_rules! timestamp {
        ($name:literal,$array:ty) => {
            carriers.push((
                $name,
                Arc::new(<$array>::from(vec![Some(-12345), None]).with_timezone("Asia/Shanghai")),
            ));
        };
    }
    timestamp!("timestamp_s", TimestampSecondArray);
    timestamp!("timestamp_ms", TimestampMillisecondArray);
    timestamp!("timestamp_us", TimestampMicrosecondArray);
    timestamp!("timestamp_ns", TimestampNanosecondArray);
    carriers
}
fn freeze(name: &str, value: Value) {
    let expected: Value = serde_json::from_str(ORIGINAL_RAW_OUTCOMES).unwrap();
    assert_eq!(value, expected[name], "complete original outcome {name}");
    if let Ok(directory) = std::env::var("C16_ORIGINAL_RAW_OUT") {
        std::fs::write(
            std::path::Path::new(&directory).join(format!("{name}.json")),
            serde_json::to_string_pretty(&value).unwrap() + "\n",
        )
        .unwrap();
    }
}
#[test]
fn json_object_legacy_contract_complete_reader_carrier_domain() {
    let mut records = Vec::new();
    for (name, a) in carriers() {
        let key: ArrayRef = Arc::new(StringArray::from(vec!["v", "v"]));
        let value = outcome(vec![key, Arc::clone(&a)], 2);
        assert_eq!(value["kind"], "success", "{name}: {value}");
        let key = outcome(
            vec![Arc::clone(&a), Arc::new(Int64Array::from(vec![1, 2]))],
            2,
        );
        if matches!(
            a.data_type(),
            DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
        ) {
            assert_eq!(
                key["message"],
                "json_object key does not support complex type"
            );
        } else {
            assert_eq!(key["kind"], "success");
        }
        records.push(
            json!({"carrier":name,"type":format!("{:?}",a.data_type()),"value":value,"key":key}),
        );
    }
    freeze("reader-carrier-domain", json!(records));
}
#[test]
fn json_object_legacy_contract_unsupported_even_when_null() {
    let types = vec![
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::LargeUtf8,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Second),
        DataType::FixedSizeBinary(3),
        DataType::LargeList(Arc::new(Field::new("x", DataType::Int64, true))),
        DataType::FixedSizeList(Arc::new(Field::new("x", DataType::Int64, true)), 2),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
    ];
    let mut records = Vec::new();
    for ty in types {
        let a = new_null_array(&ty, 1);
        let key: ArrayRef = Arc::new(StringArray::from(vec!["v"]));
        for arguments in [
            vec![Arc::clone(&key), Arc::clone(&a)],
            vec![Arc::clone(&a), Arc::clone(&key)],
        ] {
            let result = outcome(arguments, 1);
            assert_eq!(result["kind"], "whole_data");
            assert_eq!(
                result["message"],
                format!("unsupported scalar type: {ty:?}")
            );
            records.push(json!({"type":format!("{ty:?}"),"outcome":result}));
        }
    }
    freeze("unsupported-null-carriers", json!(records));
}
#[test]
fn json_object_legacy_contract_original_special_values_and_date_frontier() {
    let mut records = Vec::new();
    for (name, a) in [
        (
            "nonfinite",
            Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ])) as ArrayRef,
        ),
        (
            "ordinary-and-escaped",
            Arc::new(StringArray::from(vec![
                "ordinary text",
                "\"\\\n\té中",
                "null",
            ])),
        ),
        (
            "invalid-date",
            Arc::new(Date32Array::from(vec![-1_000_000_000])),
        ),
        ("date-overflow", Arc::new(Date32Array::from(vec![i32::MAX]))),
    ] {
        let count = a.len();
        let key: ArrayRef = Arc::new(StringArray::from(vec!["v"; count]));
        records.push(json!({"case":name,"value":outcome(vec![key, Arc::clone(&a)], count),"key":outcome(vec![a], count)}));
    }
    assert_eq!(
        records[0]["value"]["values"],
        json!(["{\"v\": null}", "{\"v\": null}", "{\"v\": null}"])
    );
    assert_eq!(
        records[2]["value"]["message"],
        "json_object invalid date32 day value: -1000000000"
    );
    assert_eq!(records[3]["value"]["kind"], "panic");
    assert_eq!(
        records[3]["value"]["message"],
        "attempt to add with overflow"
    );
    freeze("special-values-and-date-frontier", json!(records));
}
#[test]
fn json_object_legacy_contract_eager_children_null_key_empty_chunk_and_zero_layers() {
    let mut records = Vec::new();
    for rows in [0, 1] {
        let (mut arena, mut ids, chunk) = setup(vec![new_null_array(&DataType::Utf8, rows)], rows);
        ids.push(arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Int64));
        let error = eval_json_object(&arena, ExprId(usize::MAX), &ids, &chunk).unwrap_err();
        assert!(error.contains("999"));
        records.push(json!({"rows":rows,"eager_error":error}));
    }
    let direct = outcome(vec![], 1);
    assert_eq!(direct["message"], "json_object expects at least 1 argument");
    let (mut arena, _, chunk) = setup(vec![], 1);
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: crate::exec::expr::function::lookup_function("json_object").unwrap(),
            args: vec![],
        },
        DataType::Utf8,
    );
    let error = arena.eval(call, &chunk).unwrap_err();
    assert_ne!(error, direct["message"].as_str().unwrap());
    records.push(json!({"direct_zero":direct,"arena_zero_error":error}));
    freeze("eager-and-zero-layers", json!(records));
}

// Complete original outcomes captured before any production extraction.
const ORIGINAL_RAW_OUTCOMES: &str = r###"{"reader-carrier-domain":[{"carrier":"null","key":{"kind":"success","type":"Utf8","values":[null,null]},"type":"Null","value":{"kind":"success","type":"Utf8","values":["{\"v\": null}","{\"v\": null}"]}},{"carrier":"bool","key":{"kind":"success","type":"Utf8","values":["{\"true\": 1}",null]},"type":"Boolean","value":{"kind":"success","type":"Utf8","values":["{\"v\": true}","{\"v\": null}"]}},{"carrier":"i8","key":{"kind":"success","type":"Utf8","values":["{\"-128\": 1}",null]},"type":"Int8","value":{"kind":"success","type":"Utf8","values":["{\"v\": -128}","{\"v\": null}"]}},{"carrier":"i16","key":{"kind":"success","type":"Utf8","values":["{\"-32768\": 1}",null]},"type":"Int16","value":{"kind":"success","type":"Utf8","values":["{\"v\": -32768}","{\"v\": null}"]}},{"carrier":"i32","key":{"kind":"success","type":"Utf8","values":["{\"-2147483648\": 1}",null]},"type":"Int32","value":{"kind":"success","type":"Utf8","values":["{\"v\": -2147483648}","{\"v\": null}"]}},{"carrier":"i64","key":{"kind":"success","type":"Utf8","values":["{\"-9223372036854775808\": 1}",null]},"type":"Int64","value":{"kind":"success","type":"Utf8","values":["{\"v\": -9223372036854775808}","{\"v\": null}"]}},{"carrier":"f32","key":{"kind":"success","type":"Utf8","values":["{\"-0\": 1}",null]},"type":"Float32","value":{"kind":"success","type":"Utf8","values":["{\"v\": -0}","{\"v\": null}"]}},{"carrier":"f64","key":{"kind":"success","type":"Utf8","values":["{\"0.5\": 1}",null]},"type":"Float64","value":{"kind":"success","type":"Utf8","values":["{\"v\": 0.5}","{\"v\": null}"]}},{"carrier":"utf8","key":{"kind":"success","type":"Utf8","values":["{\"[1,{\\\"z\\\":true,\\\"a\\\":\\\"é中\\\"}]\": 1}",null]},"type":"Utf8","value":{"kind":"success","type":"Utf8","values":["{\"v\": [1, {\"a\": \"é中\", \"z\": true}]}","{\"v\": null}"]}},{"carrier":"binary","key":{"kind":"success","type":"Utf8","values":["{\"00ff\": 1}",null]},"type":"Binary","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"00ff\"}","{\"v\": null}"]}},{"carrier":"large_binary","key":{"kind":"success","type":"Utf8","values":["{\"00ff\": 1}",null]},"type":"LargeBinary","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"00ff\"}","{\"v\": null}"]}},{"carrier":"date","key":{"kind":"success","type":"Utf8","values":["{\"1970-01-01\": 1}",null]},"type":"Date32","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"1970-01-01\"}","{\"v\": null}"]}},{"carrier":"decimal128","key":{"kind":"success","type":"Utf8","values":["{\"-500\": 1}",null]},"type":"Decimal128(18, 4)","value":{"kind":"success","type":"Utf8","values":["{\"v\": -500}","{\"v\": null}"]}},{"carrier":"decimal256","key":{"kind":"success","type":"Utf8","values":["{\"1701411834604692317316873037158841057270\": 1}",null]},"type":"Decimal256(76, 4)","value":{"kind":"success","type":"Utf8","values":["{\"v\": 1701411834604692317316873037158841057270}","{\"v\": null}"]}},{"carrier":"largeint","key":{"kind":"success","type":"Utf8","values":["{\"-170141183460469231731687303715884105728\": 1}",null]},"type":"FixedSizeBinary(16)","value":{"kind":"success","type":"Utf8","values":["{\"v\": -170141183460469231731687303715884105728}","{\"v\": null}"]}},{"carrier":"list","key":{"kind":"whole_data","message":"json_object key does not support complex type"},"type":"List(Field { data_type: Int64, nullable: true })","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"[Some(Int64(2)), None]\"}","{\"v\": null}"]}},{"carrier":"struct","key":{"kind":"whole_data","message":"json_object key does not support complex type"},"type":"Struct([Field { name: \"s\", data_type: Utf8, nullable: true }])","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"[Some(Utf8(\\\"x\\\"))]\"}","{\"v\": null}"]}},{"carrier":"map","key":{"kind":"whole_data","message":"json_object key does not support complex type"},"type":"Map(Field { name: \"entries\", data_type: Struct([Field { name: \"key\", data_type: Utf8 }, Field { name: \"value\", data_type: Int64, nullable: true }]) }, false)","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"[(Some(Utf8(\\\"é\\\")), Some(Int64(4))), (Some(Utf8(\\\"b\\\")), None)]\"}","{\"v\": null}"]}},{"carrier":"timestamp_s","key":{"kind":"success","type":"Utf8","values":["{\"-12345\": 1}",null]},"type":"Timestamp(Second, Some(\"Asia/Shanghai\"))","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"-12345\"}","{\"v\": null}"]}},{"carrier":"timestamp_ms","key":{"kind":"success","type":"Utf8","values":["{\"-12345\": 1}",null]},"type":"Timestamp(Millisecond, Some(\"Asia/Shanghai\"))","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"-12345\"}","{\"v\": null}"]}},{"carrier":"timestamp_us","key":{"kind":"success","type":"Utf8","values":["{\"-12345\": 1}",null]},"type":"Timestamp(Microsecond, Some(\"Asia/Shanghai\"))","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"-12345\"}","{\"v\": null}"]}},{"carrier":"timestamp_ns","key":{"kind":"success","type":"Utf8","values":["{\"-12345\": 1}",null]},"type":"Timestamp(Nanosecond, Some(\"Asia/Shanghai\"))","value":{"kind":"success","type":"Utf8","values":["{\"v\": \"-12345\"}","{\"v\": null}"]}}],"eager-and-zero-layers":[{"eager_error":"slot id 999 not found in chunk (num_columns=1, slot_ids=[SlotId(1)])","rows":0},{"eager_error":"slot id 999 not found in chunk (num_columns=1, slot_ids=[SlotId(1)])","rows":1},{"arena_zero_error":"json_object expects 1 to 18446744073709551615 arguments, got 0","direct_zero":{"kind":"whole_data","message":"json_object expects at least 1 argument"}}],"special-values-and-date-frontier":[{"case":"nonfinite","key":{"kind":"success","type":"Utf8","values":["{\"NaN\": null}","{\"inf\": null}","{\"-inf\": null}"]},"value":{"kind":"success","type":"Utf8","values":["{\"v\": null}","{\"v\": null}","{\"v\": null}"]}},{"case":"ordinary-and-escaped","key":{"kind":"success","type":"Utf8","values":["{\"ordinary text\": null}","{\"\\\"\\\\\\n\\té中\": null}","{\"null\": null}"]},"value":{"kind":"success","type":"Utf8","values":["{\"v\": \"ordinary text\"}","{\"v\": \"\\\"\\\\\\n\\té中\"}","{\"v\": null}"]}},{"case":"invalid-date","key":{"kind":"whole_data","message":"json_object invalid date32 day value: -1000000000"},"value":{"kind":"whole_data","message":"json_object invalid date32 day value: -1000000000"}},{"case":"date-overflow","key":{"kind":"panic","message":"attempt to add with overflow"},"value":{"kind":"panic","message":"attempt to add with overflow"}}],"unsupported-null-carriers":[{"outcome":{"kind":"whole_data","message":"unsupported scalar type: UInt8"},"type":"UInt8"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: UInt8"},"type":"UInt8"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: UInt16"},"type":"UInt16"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: UInt16"},"type":"UInt16"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: UInt32"},"type":"UInt32"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: UInt32"},"type":"UInt32"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: UInt64"},"type":"UInt64"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: UInt64"},"type":"UInt64"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: LargeUtf8"},"type":"LargeUtf8"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: LargeUtf8"},"type":"LargeUtf8"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Date64"},"type":"Date64"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Date64"},"type":"Date64"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Time32(Second)"},"type":"Time32(Second)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Time32(Second)"},"type":"Time32(Second)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Time64(Nanosecond)"},"type":"Time64(Nanosecond)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Time64(Nanosecond)"},"type":"Time64(Nanosecond)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Duration(Second)"},"type":"Duration(Second)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Duration(Second)"},"type":"Duration(Second)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: FixedSizeBinary(3)"},"type":"FixedSizeBinary(3)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: FixedSizeBinary(3)"},"type":"FixedSizeBinary(3)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: LargeList(Field { name: \"x\", data_type: Int64, nullable: true })"},"type":"LargeList(Field { name: \"x\", data_type: Int64, nullable: true })"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: LargeList(Field { name: \"x\", data_type: Int64, nullable: true })"},"type":"LargeList(Field { name: \"x\", data_type: Int64, nullable: true })"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: FixedSizeList(Field { name: \"x\", data_type: Int64, nullable: true }, 2)"},"type":"FixedSizeList(Field { name: \"x\", data_type: Int64, nullable: true }, 2)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: FixedSizeList(Field { name: \"x\", data_type: Int64, nullable: true }, 2)"},"type":"FixedSizeList(Field { name: \"x\", data_type: Int64, nullable: true }, 2)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Dictionary(Int8, Utf8)"},"type":"Dictionary(Int8, Utf8)"},{"outcome":{"kind":"whole_data","message":"unsupported scalar type: Dictionary(Int8, Utf8)"},"type":"Dictionary(Int8, Utf8)"}]}"###;
