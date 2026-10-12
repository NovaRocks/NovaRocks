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

//! Original Arena-only producer. No pure preparation or candidate calculation.
use super::*;
use arrow::array::{
    BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array, ListArray, StringArray,
};
use arrow::datatypes::Field;
use chrono::NaiveDate;
use serde_json::{Value, json};

// Recorded by the unchanged parent on Native 1FE+3BE before these test leaves.
const NATIVE_INPUT_SCAN: &str = r#"id	k	v	n	flag	day	amount
1	b	{"z":2,"a":1}	42	1	2020-02-29	12.3400
2	a	ordinary text	-7	0	1970-01-01	-0.0500
3	NULL	[1,{"b":false,"a":"é中"}]	NULL	NULL	NULL	NULL
4	é中	null	0	1	2000-01-01	0.0000
5			9223372036854775807	0	1969-12-31	999.9999
"#;

fn native_columns() -> Vec<ArrayRef> {
    if let Ok(path) = std::env::var("C16_PARENT_SCAN") {
        assert_eq!(std::fs::read_to_string(path).unwrap(), NATIVE_INPUT_SCAN);
    }
    let rows = NATIVE_INPUT_SCAN
        .lines()
        .skip(1)
        .map(|s| s.split('\t').collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|r| r.len() == 7));
    let text = |col: usize| {
        rows.iter()
            .map(|r| (r[col] != "NULL").then_some(r[col]))
            .collect::<Vec<_>>()
    };
    let ints = |col: usize| {
        text(col)
            .into_iter()
            .map(|v| v.map(|s| s.parse::<i64>().unwrap()))
            .collect::<Vec<_>>()
    };
    let date = text(5)
        .into_iter()
        .map(|v| {
            v.map(|s| {
                let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
                i32::try_from(
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days(),
                )
                .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let decimals = text(6)
        .into_iter()
        .map(|v| {
            v.map(|s| {
                let (integer, fraction) = s.split_once('.').unwrap();
                assert_eq!(fraction.len(), 4);
                let magnitude = integer.trim_start_matches('-').parse::<i128>().unwrap() * 10_000
                    + fraction.parse::<i128>().unwrap();
                if integer.starts_with('-') {
                    -magnitude
                } else {
                    magnitude
                }
            })
        })
        .collect::<Vec<_>>();
    vec![
        Arc::new(Int32Array::from(
            ints(0)
                .into_iter()
                .map(|v| v.map(|i| i32::try_from(i).unwrap()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(text(1))),
        Arc::new(StringArray::from(text(2))),
        Arc::new(Int64Array::from(ints(3))),
        Arc::new(BooleanArray::from(
            text(4)
                .into_iter()
                .map(|v| {
                    v.map(|s| match s {
                        "1" => true,
                        "0" => false,
                        _ => panic!("original boolean transport"),
                    })
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(Date32Array::from(date)),
        Arc::new(
            Decimal128Array::from(decimals)
                .with_precision_and_scale(18, 4)
                .unwrap(),
        ),
    ]
}

fn setup(rows: &[usize]) -> (ExprArena, Vec<ExprId>, Chunk) {
    let input = native_columns();
    let indices = UInt64Array::from_iter_values(rows.iter().map(|r| *r as u64));
    let columns = input
        .iter()
        .map(|a| {
            if rows.is_empty() {
                a.slice(0, 0)
            } else {
                take(a.as_ref(), &indices, None).unwrap()
            }
        })
        .collect::<Vec<_>>();
    let names = ["id", "k", "v", "n", "flag", "day", "amount"];
    let slots = (1..=7).map(SlotId::new).collect::<Vec<_>>();
    let fields = columns
        .iter()
        .zip(names)
        .map(|(a, n)| Field::new(n, a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let mut arena = ExprArena::default();
    let ids = slots
        .iter()
        .zip(batch.columns())
        .map(|(s, a)| arena.push_typed(ExprNode::SlotId(*s), a.data_type().clone()))
        .collect();
    (
        arena,
        ids,
        Chunk::new_with_chunk_schema(batch, chunk_schema),
    )
}
fn literal(arena: &mut ExprArena, text: &str) -> ExprId {
    arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(text.into())),
        DataType::Utf8,
    )
}
fn list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
}
fn array(arena: &mut ExprArena, ids: &[ExprId]) -> ExprId {
    arena.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![ids[0], ids[3]],
        },
        list_type(),
    )
}
fn if_key(arena: &mut ExprArena, ids: &[ExprId]) -> ExprId {
    let two = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(2)), DataType::Int32);
    let equal = arena.push_typed(ExprNode::Eq(ids[0], two), DataType::Boolean);
    let null = arena.push_typed(ExprNode::Literal(LiteralValue::Null), list_type());
    let array = array(arena, ids);
    arena.push_typed(
        ExprNode::FunctionCall {
            kind: LegacyKind::If,
            args: vec![equal, null, array],
        },
        list_type(),
    )
}
fn child_values(a: &ArrayRef) -> Value {
    let lists = a.as_any().downcast_ref::<ListArray>().unwrap();
    Value::Array(
        (0..a.len())
            .map(|row| {
                if a.is_null(row) {
                    Value::Null
                } else {
                    let values = lists.value(row);
                    let ints = values.as_any().downcast_ref::<Int64Array>().unwrap();
                    json!(
                        (0..ints.len())
                            .map(|i| (!ints.is_null(i)).then(|| ints.value(i)))
                            .collect::<Vec<_>>()
                    )
                }
            })
            .collect(),
    )
}
fn type_metadata(ty: &FunctionValueType) -> Value {
    let field = match &ty.data_type {
        DataType::List(field) => {
            json!({"name":field.name(), "nullable":field.is_nullable(), "data_type":format!("{:?}",field.data_type()), "metadata":field.metadata()})
        }
        _ => Value::Null,
    };
    json!({"physical_type":format!("{:?}",ty.data_type),"nullable":ty.nullable,"logical_type":format!("{:?}",ty.logical_type),"field":field})
}
fn original_query(query: usize, rows: &[usize]) -> Value {
    let (mut arena, ids, chunk) = setup(rows);
    let mut children = Vec::new();
    let arguments = match query {
        4 => {
            let mut args = vec![ids[1], ids[2]];
            for (key, value) in [
                ("n", ids[3]),
                ("b", ids[4]),
                ("day", ids[5]),
                ("amount", ids[6]),
                ("dup", ids[2]),
                ("dup", ids[3]),
            ] {
                args.push(literal(&mut arena, key));
                args.push(value);
            }
            args
        }
        5 => vec![ids[1]],
        6 => {
            let child = array(&mut arena, &ids);
            children.push(child);
            vec![ids[1], child]
        }
        7 => {
            let key = literal(&mut arena, "n");
            vec![ids[1], ids[2], key, ids[3]]
        }
        8 => vec![ids[1], ids[2]],
        9..=11 => {
            let child = if_key(&mut arena, &ids);
            children.push(child);
            vec![child, ids[3]]
        }
        _ => panic!("fixed original SQL query"),
    };
    // Child outcomes come from their original ArrayExpr/IF authors, not fixtures.
    let children = children.into_iter().map(|child| {
        let output = arena.eval(child, &chunk).unwrap();
        assert!(novarocks_type_contract::arrow_data_types_exact(output.data_type(), &list_type()));
        json!({"type":type_metadata(&FunctionValueType::new(output.data_type().clone(), query != 6)), "values":child_values(&output), "offsets":output.as_any().downcast_ref::<ListArray>().unwrap().value_offsets()})
    }).collect::<Vec<_>>();
    let arrays = arguments
        .iter()
        .map(|id| arena.eval(*id, &chunk).unwrap())
        .collect::<Vec<_>>();
    let spec =
        arrays
            .iter()
            .zip(&arguments)
            .fold(ScalarDiffSpec::new("json_object"), |s, (a, id)| {
                let nullable = !matches!(
                    arena.node(*id).unwrap(),
                    ExprNode::Literal(_) | ExprNode::ArrayExpr { .. }
                );
                s.typed_column(
                    FunctionValueType::new(a.data_type().clone(), nullable),
                    Arc::clone(a),
                )
            });
    let binding = resolve_like_sql(
        builtin_engine_function_catalog(),
        "json_object",
        CatalogKind::Scalar,
        &spec.arguments,
    )
    .unwrap();
    let FunctionResultType::Scalar(result_type) = binding.selected.result_type else {
        panic!("scalar result")
    };
    assert_eq!(result_type.data_type, DataType::Utf8);
    assert_eq!(result_type.logical_type, ValueLogicalType::Json);
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: lookup_function("json_object").unwrap(),
            args: arguments,
        },
        result_type.data_type.clone(),
    );
    let result = arena.eval(call, &chunk);
    let outcome = match result {
        Ok(values) => {
            assert_eq!(values.data_type(), &DataType::Utf8);
            assert_eq!(values.len(), rows.len());
            let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
            json!({"kind":"success", "physical_type":format!("{:?}", values.data_type()), "values":(0..values.len()).map(|i| (!values.is_null(i)).then(|| strings.value(i))).collect::<Vec<_>>()})
        }
        Err(error) => {
            assert_eq!(error, "json_object key does not support complex type");
            json!({"kind":"whole_data", "message":error})
        }
    };
    json!({"query":query, "rows":rows, "ids":rows.iter().map(|r| r + 1).collect::<Vec<_>>(), "selected_result_type":type_metadata(&result_type), "selected_argument_types":spec.arguments.iter().map(|a| type_metadata(a.value_type())).collect::<Vec<_>>(), "children":children, "outcome":outcome})
}
#[test]
fn json_object_legacy_native_oracle_executes_original_arena() {
    let queries = (4..=11)
        .map(|q| {
            let rows: &[usize] = match q {
                7 => &[0, 2, 4],
                8 => &[],
                9 => &[0],
                _ => &[0, 1, 2, 3, 4],
            };
            original_query(q, rows)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        queries
            .iter()
            .filter(|q| q["outcome"]["kind"] == "success")
            .count(),
        5
    );
    assert_eq!(
        queries
            .iter()
            .filter(|q| q["outcome"]["kind"] == "whole_data")
            .count(),
        3
    );
    assert_eq!(
        json!(queries),
        serde_json::from_str::<Value>(ORIGINAL_ARENA_QUERIES).unwrap()
    );
    let receipt = json!({"producer":"unchanged original ExprArena; actual ArrayExpr and IF children", "native_input_scan":NATIVE_INPUT_SCAN, "queries":queries});
    if let Ok(path) = std::env::var("C16_ORIGINAL_ORACLE_OUT") {
        std::fs::write(path, serde_json::to_string_pretty(&receipt).unwrap() + "\n").unwrap();
    }
}

const ORIGINAL_SQL: &[(usize, &str)] = &[
    (
        4,
        r#"SELECT id, json_object(k, v, 'n', n, 'b', flag, 'day', day, 'amount', amount, 'dup', v, 'dup', n) AS object_value
FROM t ORDER BY id;"#,
    ),
    (
        5,
        r#"SELECT id, json_object(k) AS odd_tail FROM t ORDER BY id;"#,
    ),
    (
        6,
        r#"SELECT id, json_object(k, [id, n]) AS debug_value FROM t ORDER BY id;"#,
    ),
    (
        7,
        r#"SELECT id, json_object(k, v, 'n', n) AS selected_value FROM t WHERE id IN (1, 3, 5) ORDER BY id;"#,
    ),
    (
        8,
        r#"SELECT id, json_object(k, v) AS empty_value FROM t WHERE id < 0 ORDER BY id;"#,
    ),
    (
        9,
        r#"SELECT json_object(IF(id = 2, NULL, [id, n]), n) AS complex_key FROM t WHERE id = 1;"#,
    ),
    (
        10,
        r#"SELECT id FROM (SELECT id, json_object(IF(id = 2, NULL, [id, n]), n) AS unused_error FROM t) s ORDER BY id;"#,
    ),
    (
        11,
        r#"SELECT id FROM (SELECT id, json_object(IF(id = 2, NULL, [id, n]), n) AS unused_error FROM t) s WHERE id = 2 ORDER BY id;"#,
    ),
];

fn ancestors(
    plan: &novarocks_physical_plan::PhysicalPlan,
    start: (
        novarocks_physical_plan::FragmentId,
        novarocks_physical_plan::NodeId,
    ),
) -> std::collections::BTreeSet<(
    novarocks_physical_plan::FragmentId,
    novarocks_physical_plan::NodeId,
)> {
    use novarocks_physical_plan::NodeKind;
    let mut seen = std::collections::BTreeSet::new();
    let mut pending = vec![start];
    while let Some((fragment, node)) = pending.pop() {
        if !seen.insert((fragment, node)) {
            continue;
        }
        let current = &plan.fragments()[&fragment].nodes()[&node];
        pending.extend(current.inputs.iter().map(|id| (fragment, *id)));
        if let NodeKind::ExchangeSource { edge, .. } = &current.kind {
            let source = plan.edges()[edge].source.fragment;
            pending.push((source, plan.fragments()[&source].root()));
        }
    }
    seen
}
#[test]
fn json_object_legacy_native_oracle_original_sql_metadata_and_error_preservation() {
    use novarocks_physical_plan::{ExprKind, NodeKind};
    use novarocks_sql::compiler::SqlPhysicalEmissionMode;
    let fields = native_columns()
        .iter()
        .zip(["id", "k", "v", "n", "flag", "day", "amount"])
        .map(|(a, n)| Field::new(n, a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let mut records = Vec::new();
    for (query, sql) in ORIGINAL_SQL {
        let source = super::super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_declared_fields(sql, &fields, SqlPhysicalEmissionMode::OriginalNativeV1);
        let plan = source.plan();
        let result = plan.result_port().unwrap();
        let live = ancestors(plan, (result.fragment, result.output.node));
        let mut owners = Vec::new();
        let mut call_metadata = Vec::new();
        for fragment in plan.fragments().values() {
            for (id, expr) in fragment.expressions().iter() {
                let ExprKind::FunctionCall { function, .. } = &expr.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/json_object/v1" {
                    continue;
                }
                assert_eq!(expr.ty.logical_type, ValueLogicalType::Json);
                assert_eq!(expr.ty.data_type, DataType::Utf8);
                assert_eq!(function.result_type, expr.ty);
                assert!(
                    live.contains(&(fragment.id(), expr.owner)),
                    "original throwing use remains root-reachable"
                );
                let NodeKind::Project { expressions } = &fragment.nodes()[&expr.owner].kind else {
                    panic!("original throwing projection owner")
                };
                assert!(expressions.iter().any(|(root, _)| root == id));
                call_metadata.push(json!({"result_type":type_metadata(&function.result_type),"argument_types":function.argument_types.iter().map(|a| match a { FunctionArgumentType::Value(v) => type_metadata(v), _ => panic!("original value channel") }).collect::<Vec<_>>()}));
                owners.push((fragment.id(), expr.owner));
            }
        }
        assert!(!owners.is_empty(), "original JSON call is not pruned");
        if *query == 11 {
            let filters = live
                .iter()
                .filter(|(f, n)| {
                    matches!(plan.fragments()[f].nodes()[n].kind, NodeKind::Filter { .. })
                })
                .collect::<Vec<_>>();
            assert!(!filters.is_empty());
            assert!(
                filters.iter().any(|site| owners
                    .iter()
                    .all(|owner| ancestors(plan, **site).contains(owner))),
                "outer filter remains above the throwing project"
            );
            for (f, n) in &live {
                if let NodeKind::Scan {
                    relation,
                    residuals,
                    ..
                } = &plan.fragments()[f].nodes()[n].kind
                {
                    assert!(residuals.is_empty());
                    assert!(
                        relation.predicate_guarantees().is_empty(),
                        "no provider predicate hides a pushed filter"
                    );
                }
            }
        }
        let headers = result
            .fields
            .iter()
            .map(|f| f.alias.as_deref().unwrap_or(&f.name))
            .collect::<Vec<_>>();
        let expected: &[&str] = match query {
            4 => &["id", "object_value"],
            5 => &["id", "odd_tail"],
            6 => &["id", "debug_value"],
            7 => &["id", "selected_value"],
            8 => &["id", "empty_value"],
            9 => &["complex_key"],
            10 | 11 => &["id"],
            _ => unreachable!(),
        };
        assert_eq!(headers, expected);
        records.push(json!({"query":query,"sql":sql,"headers":headers,"fields":format!("{:?}",result.fields),"full_plan":format!("{plan:#?}"),"reachable_json_project_count":owners.len(),"call_metadata":call_metadata}));
    }
    if let Ok(path) = std::env::var("C16_ORIGINAL_SQL_OUT") {
        std::fs::write(path, serde_json::to_string_pretty(&json!({"mode":"OriginalNativeV1","fold":"NoOp original source fixture","queries":records})).unwrap()+"\n").unwrap();
    }
}

#[test]
fn c16_p1_json_object_nested_index_child_matches_frozen_original_if_child() {
    // Only unchanged child authors run here. Original JSON expectations remain
    // the captured parent outcomes; current JSON never generates this oracle.
    let original: Value = serde_json::from_str(ORIGINAL_ARENA_QUERIES).unwrap();
    let mut records = Vec::new();
    for (query, rows) in [
        (9, vec![0]),
        (10, vec![0, 1, 2, 3, 4]),
        (11, vec![0, 1, 2, 3, 4]),
        (10, vec![1]),
    ] {
        let (mut arena, ids, chunk) = setup(&rows);
        assert_eq!(arena.data_type(ids[0]), Some(&DataType::Int32));
        assert_eq!(arena.data_type(ids[3]), Some(&DataType::Int64));
        let id = arena.push_typed(
            ExprNode::Cast(ids[0], DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );
        let inner = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![id, ids[3]],
            },
            list_type(),
        );
        let outer_type = DataType::List(Arc::new(Field::new("item", list_type(), true)));
        let outer = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![inner],
            },
            outer_type.clone(),
        );
        let two = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let one = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let equal = arena.push_typed(ExprNode::Eq(id, two), DataType::Boolean);
        let null = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Int64);
        let index64 = arena.push_typed(
            ExprNode::FunctionCall {
                kind: LegacyKind::If,
                args: vec![equal, null, one],
            },
            DataType::Int64,
        );
        let index = arena.push_typed(
            ExprNode::Cast(index64, DecimalOverflowPolicy::OutputNull),
            DataType::Int32,
        );
        let selected = resolve_like_sql(
            builtin_engine_function_catalog(),
            "__array_element_at",
            CatalogKind::Scalar,
            &ScalarDiffSpec::new("__array_element_at")
                .typed_column(
                    FunctionValueType::new(outer_type, false),
                    arena.eval(outer, &chunk).unwrap(),
                )
                .typed_column(
                    FunctionValueType::new(DataType::Int32, true),
                    arena.eval(index, &chunk).unwrap(),
                )
                .arguments,
        )
        .unwrap();
        let FunctionResultType::Scalar(result_type) = selected.selected.result_type else {
            panic!("original subscript scalar result");
        };
        let child = arena.push_typed(
            ExprNode::FunctionCall {
                kind: lookup_function("__array_element_at").unwrap(),
                args: vec![outer, index],
            },
            result_type.data_type.clone(),
        );
        let index64_values = arena.eval(index64, &chunk).unwrap();
        let index_values = arena.eval(index, &chunk).unwrap();
        assert_eq!(index64_values.data_type(), &DataType::Int64);
        assert_eq!(index_values.data_type(), &DataType::Int32);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(index64_values.is_null(i), *row == 1);
            assert_eq!(index_values.is_null(i), *row == 1);
        }
        let indices = index_values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            (0..rows.len())
                .map(|i| (!indices.is_null(i)).then(|| indices.value(i)))
                .collect::<Vec<_>>(),
            rows.iter()
                .map(|r| (*r != 1).then_some(1))
                .collect::<Vec<_>>(),
        );
        let output = arena.eval(child, &chunk).unwrap();
        assert_eq!(output.len(), rows.len());
        let frozen = original
            .as_array()
            .unwrap()
            .iter()
            .find(|q| q["query"] == query)
            .unwrap();
        let expected = &frozen["children"][0];
        assert_eq!(type_metadata(&result_type), expected["type"]);
        let expected_values = if rows == [1] {
            json!([expected["values"][1].clone()])
        } else {
            expected["values"].clone()
        };
        assert_eq!(child_values(&output), expected_values);
        for (i, value) in expected_values.as_array().unwrap().iter().enumerate() {
            assert_eq!(output.is_null(i), value.is_null());
        }
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        let backing = lists
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        records.push(json!({
            "query":query,"rows":rows,"type":type_metadata(&result_type),
            "values":child_values(&output),"offsets":lists.value_offsets(),
            "backing":(0..backing.len()).map(|i| (!backing.is_null(i)).then(|| backing.value(i))).collect::<Vec<_>>(),
            "original_offsets":expected["offsets"],
            "index_input":"SQL nullable Int64 IF followed by nullable Int32 cast",
            "index64_type":type_metadata(&FunctionValueType::new(DataType::Int64,true)),
            "index32_type":type_metadata(&FunctionValueType::new(DataType::Int32,true)),
            "index64_null_bitmap":(0..rows.len()).map(|i| index64_values.is_null(i)).collect::<Vec<_>>(),
            "index32_null_bitmap":(0..rows.len()).map(|i| indices.is_null(i)).collect::<Vec<_>>(), "index_values":(0..rows.len()).map(|i| (!indices.is_null(i)).then(|| indices.value(i))).collect::<Vec<_>>(),
            "representation_rule":"Offsets/backing are recorded separately; NULL slots consume no values; observable type, NULL bitmap and recursively selected values must equal the frozen parent",
        }));
    }
    if let Ok(path) = std::env::var("C16_CHILD_EQUIVALENCE_OUT") {
        std::fs::write(path, serde_json::to_string_pretty(&json!({"producer":"unchanged original child authors only; no JSON calculation", "records":records})).unwrap()+"\n").unwrap();
    }
}

// Complete parent Arena outcomes, including selected types, real child arrays, and whole errors.
const ORIGINAL_ARENA_QUERIES: &str = r###"[{"children":[],"ids":[1,2,3,4,5],"outcome":{"kind":"success","physical_type":"Utf8","values":["{\"b\": {\"a\": 1, \"z\": 2},\"n\": 42,\"b\": true,\"day\": \"2020-02-29\",\"amount\": 123400,\"dup\": {\"a\": 1, \"z\": 2},\"dup\": 42}","{\"a\": \"ordinary text\",\"n\": -7,\"b\": false,\"day\": \"1970-01-01\",\"amount\": -500,\"dup\": \"ordinary text\",\"dup\": -7}",null,"{\"é中\": null,\"n\": 0,\"b\": true,\"day\": \"2000-01-01\",\"amount\": 0,\"dup\": null,\"dup\": 0}","{\"\": \"\",\"n\": 9223372036854775807,\"b\": false,\"day\": \"1969-12-31\",\"amount\": 9999999,\"dup\": \"\",\"dup\": 9223372036854775807}"]},"query":4,"rows":[0,1,2,3,4],"selected_argument_types":[{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":false,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Int64"},{"field":null,"logical_type":"Physical","nullable":false,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Boolean"},{"field":null,"logical_type":"Physical","nullable":false,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Date32"},{"field":null,"logical_type":"Physical","nullable":false,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Decimal128(18, 4)"},{"field":null,"logical_type":"Physical","nullable":false,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":false,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Int64"}],"selected_result_type":{"field":null,"logical_type":"Json","nullable":true,"physical_type":"Utf8"}},{"children":[],"ids":[1,2,3,4,5],"outcome":{"kind":"success","physical_type":"Utf8","values":["{\"b\": null}","{\"a\": null}",null,"{\"é中\": null}","{\"\": null}"]},"query":5,"rows":[0,1,2,3,4],"selected_argument_types":[{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Utf8"}],"selected_result_type":{"field":null,"logical_type":"Json","nullable":true,"physical_type":"Utf8"}},{"children":[{"offsets":[0,2,4,6,8,10],"type":{"field":{"data_type":"Int64","metadata":{},"name":"item","nullable":true},"logical_type":"Physical","nullable":false,"physical_type":"List(Field { data_type: Int64, nullable: true })"},"values":[[1,42],[2,-7],[3,null],[4,0],[5,9223372036854775807]]}],"ids":[1,2,3,4,5],"outcome":{"kind":"success","physical_type":"Utf8","values":["{\"b\": \"[Some(Int64(1)), Some(Int64(42))]\"}","{\"a\": \"[Some(Int64(2)), Some(Int64(-7))]\"}",null,"{\"é中\": \"[Some(Int64(4)), Some(Int64(0))]\"}","{\"\": \"[Some(Int64(5)), Some(Int64(9223372036854775807))]\"}"]},"query":6,"rows":[0,1,2,3,4],"selected_argument_types":[{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Utf8"},{"field":{"data_type":"Int64","metadata":{},"name":"item","nullable":true},"logical_type":"Physical","nullable":false,"physical_type":"List(Field { data_type: Int64, nullable: true })"}],"selected_result_type":{"field":null,"logical_type":"Json","nullable":true,"physical_type":"Utf8"}},{"children":[],"ids":[1,3,5],"outcome":{"kind":"success","physical_type":"Utf8","values":["{\"b\": {\"a\": 1, \"z\": 2},\"n\": 42}",null,"{\"\": \"\",\"n\": 9223372036854775807}"]},"query":7,"rows":[0,2,4],"selected_argument_types":[{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":false,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Int64"}],"selected_result_type":{"field":null,"logical_type":"Json","nullable":true,"physical_type":"Utf8"}},{"children":[],"ids":[],"outcome":{"kind":"success","physical_type":"Utf8","values":[]},"query":8,"rows":[],"selected_argument_types":[{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Utf8"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Utf8"}],"selected_result_type":{"field":null,"logical_type":"Json","nullable":true,"physical_type":"Utf8"}},{"children":[{"offsets":[0,2],"type":{"field":{"data_type":"Int64","metadata":{},"name":"item","nullable":true},"logical_type":"Physical","nullable":true,"physical_type":"List(Field { data_type: Int64, nullable: true })"},"values":[[1,42]]}],"ids":[1],"outcome":{"kind":"whole_data","message":"json_object key does not support complex type"},"query":9,"rows":[0],"selected_argument_types":[{"field":{"data_type":"Int64","metadata":{},"name":"item","nullable":true},"logical_type":"Physical","nullable":true,"physical_type":"List(Field { data_type: Int64, nullable: true })"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Int64"}],"selected_result_type":{"field":null,"logical_type":"Json","nullable":true,"physical_type":"Utf8"}},{"children":[{"offsets":[0,2,2,4,6,8],"type":{"field":{"data_type":"Int64","metadata":{},"name":"item","nullable":true},"logical_type":"Physical","nullable":true,"physical_type":"List(Field { data_type: Int64, nullable: true })"},"values":[[1,42],null,[3,null],[4,0],[5,9223372036854775807]]}],"ids":[1,2,3,4,5],"outcome":{"kind":"whole_data","message":"json_object key does not support complex type"},"query":10,"rows":[0,1,2,3,4],"selected_argument_types":[{"field":{"data_type":"Int64","metadata":{},"name":"item","nullable":true},"logical_type":"Physical","nullable":true,"physical_type":"List(Field { data_type: Int64, nullable: true })"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Int64"}],"selected_result_type":{"field":null,"logical_type":"Json","nullable":true,"physical_type":"Utf8"}},{"children":[{"offsets":[0,2,2,4,6,8],"type":{"field":{"data_type":"Int64","metadata":{},"name":"item","nullable":true},"logical_type":"Physical","nullable":true,"physical_type":"List(Field { data_type: Int64, nullable: true })"},"values":[[1,42],null,[3,null],[4,0],[5,9223372036854775807]]}],"ids":[1,2,3,4,5],"outcome":{"kind":"whole_data","message":"json_object key does not support complex type"},"query":11,"rows":[0,1,2,3,4],"selected_argument_types":[{"field":{"data_type":"Int64","metadata":{},"name":"item","nullable":true},"logical_type":"Physical","nullable":true,"physical_type":"List(Field { data_type: Int64, nullable: true })"},{"field":null,"logical_type":"Physical","nullable":true,"physical_type":"Int64"}],"selected_result_type":{"field":null,"logical_type":"Json","nullable":true,"physical_type":"Utf8"}}]"###;
