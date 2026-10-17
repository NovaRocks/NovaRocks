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

//! Independent original ExprArena constructor and raw map access baselines.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, ListArray, MapArray, NullArray,
    StringArray, StructArray, new_empty_array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::{NullBuffer, OffsetBuffer};
use novarocks_types::SlotId;
use std::{collections::HashMap, sync::Arc};
fn setup(
    mut columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<(ExprArena, Vec<ExprId>, Chunk), String> {
    if columns.is_empty() {
        columns.push(Arc::new(Int32Array::from(vec![0; rows])))
    }
    let slots = (0..columns.len())
        .map(|i| SlotId::new(i as u32 + 1))
        .collect::<Vec<_>>();
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let mut arena = ExprArena::default();
    let ids = columns
        .iter()
        .enumerate()
        .map(|(i, a)| arena.push_typed(ExprNode::SlotId(slots[i]), a.data_type().clone()))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots)?;
    Ok((arena, ids, Chunk::new_with_chunk_schema(batch, schema)))
}
fn array(
    columns: Vec<ArrayRef>,
    rows: usize,
    output: Option<DataType>,
) -> Result<ArrayRef, String> {
    let empty = columns.is_empty();
    let (mut arena, mut ids, chunk) = setup(columns, rows)?;
    if empty {
        ids.clear()
    }
    let node = ExprNode::ArrayExpr {
        elements: ids.clone(),
    };
    let id = match output {
        Some(t) => arena.push_typed(node, t),
        None => arena.push(node),
    };
    super::array_expr::eval_array_expr(&arena, id, &ids, &chunk)
}
fn map(keys: ArrayRef, values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    let fs = vec![
        Arc::new(Field::new(
            "key",
            keys.data_type().clone(),
            keys.logical_null_count() > 0,
        )),
        Arc::new(Field::new("value", values.data_type().clone(), true)),
    ];
    let entries = StructArray::new(fs.into(), vec![keys, values], None);
    Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(offsets.into()),
        entries,
        valid.map(NullBuffer::from),
        false,
    ))
}
fn lookup(columns: Vec<ArrayRef>, output: Option<DataType>) -> Result<ArrayRef, String> {
    let rows = columns[0].len();
    let (mut arena, ids, chunk) = setup(columns, rows)?;
    let id = match output {
        Some(t) => arena.push_typed(ExprNode::SlotId(SlotId::new(999)), t),
        None => ExprId(usize::MAX),
    };
    super::function::map::eval_element_at(&arena, id, &ids, &chunk)
}
fn ints(a: &ArrayRef) -> Vec<Option<i32>> {
    a.as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn original_array_literal_empty_preserves_item_metadata_and_nonnull_parent() {
    let field = Arc::new(
        Field::new("exact-item", DataType::Int32, false)
            .with_metadata(HashMap::from([("author".into(), "frozen".into())])),
    );
    for rows in [0, 3] {
        let out = array(vec![], rows, Some(DataType::List(field.clone()))).unwrap();
        let l = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.data_type(), &DataType::List(field.clone()));
        assert_eq!(l.null_count(), 0);
        assert_eq!(l.value_offsets(), vec![0; rows + 1]);
        assert_eq!(l.values().len(), 0)
    }
}
#[test]
fn original_array_literal_row_major_null_elements_and_raw_coercion() {
    let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
    let b: ArrayRef = Arc::new(NullArray::new(3));
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let out = array(vec![a, b], 3, Some(DataType::List(field))).unwrap();
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(l.value_offsets(), &[0, 2, 4, 6]);
    assert_eq!(
        ints(l.values()),
        vec![Some(1), None, None, None, Some(3), None]
    );
    assert_eq!(l.null_count(), 0);
    let out = array(
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        3,
        Some(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float64,
            true,
        )))),
    )
    .unwrap();
    let l = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        l.values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1., 2., 3.]
    );
}
#[test]
fn original_array_literal_inferred_null_output_errors_precede_child_eval() {
    assert_eq!(
        array(vec![], 2, None).unwrap_err(),
        "array_expr output type must be List, got Null"
    );
    assert_eq!(
        array(vec![], 2, Some(DataType::Int32)).unwrap_err(),
        "array_expr output type must be List, got Int32"
    );
    // ExprArena::push infers Null; that output is read before the invalid child id.
    let (mut arena, _, chunk) = setup(vec![], 2).unwrap();
    let ids = vec![ExprId(usize::MAX)];
    let id = arena.push(ExprNode::ArrayExpr {
        elements: ids.clone(),
    });
    assert_eq!(
        super::array_expr::eval_array_expr(&arena, id, &ids, &chunk).unwrap_err(),
        "array_expr output type must be List, got Null"
    );
}
#[test]
fn map_null_probe_duplicate_key_parent_null_and_invalid_key_refusal() {
    let m = map(
        Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            None,
            Some(2),
            Some(2),
        ])),
        Arc::new(Int32Array::from(vec![10, 11, 12, 20, 21])),
        vec![0, 3, 5, 5],
        Some(vec![true, true, false]),
    );
    assert_eq!(
        lookup(
            vec![m, Arc::new(Int32Array::from(vec![None, Some(2), None]))],
            None,
        )
        .unwrap_err(),
        "map key type is nullable"
    );
    let valid = map(
        Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2])),
        Arc::new(Int32Array::from(vec![10, 11, 12, 20, 21])),
        vec![0, 3, 5, 5],
        Some(vec![true, true, false]),
    );
    let out = lookup(
        vec![valid, Arc::new(Int32Array::from(vec![None, Some(2), None]))],
        None,
    )
    .unwrap();
    assert_eq!(ints(&out), vec![None, Some(21), None]);
}
#[test]
fn original_map_ieee_equality_nan_does_not_match_signed_zero_last_wins() {
    let m = map(
        Arc::new(Float64Array::from(vec![
            -0.,
            0.,
            f64::NAN,
            -0.,
            0.,
            f64::NAN,
        ])),
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
        vec![0, 3, 6],
        None,
    );
    let out = lookup(
        vec![m, Arc::new(Float64Array::from(vec![0., f64::NAN]))],
        None,
    )
    .unwrap();
    assert_eq!(ints(&out), vec![Some(2), None]);
}
#[test]
fn original_map_largeint_empty_and_miss_keep_null_bitmap() {
    let v = novarocks_types::largeint::array_from_i128(&[Some(i128::MIN)]).unwrap();
    let m = map(Arc::new(Int32Array::from(vec![1])), v, vec![0, 1, 1], None);
    let out = lookup(vec![m, Arc::new(Int32Array::from(vec![2, 1]))], None).unwrap();
    assert_eq!(out.null_count(), 2);
    let m = map(
        new_empty_array(&DataType::Int32),
        new_empty_array(&DataType::FixedSizeBinary(16)),
        vec![0, 0, 0],
        None,
    );
    let out = lookup(vec![m, Arc::new(Int32Array::from(vec![1, 2]))], None).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out.null_count(), 2);
}
#[test]
fn original_map_raw_third_flag_and_full_carrier_errors() {
    let m = map(
        Arc::new(Int32Array::from(vec![1])),
        Arc::new(Int32Array::from(vec![5])),
        vec![0, 1, 1],
        Some(vec![true, false]),
    );
    assert_eq!(
        lookup(
            vec![
                m.clone(),
                Arc::new(Int32Array::from(vec![2, 2])),
                Arc::new(BooleanArray::from(vec![true, true]))
            ],
            None
        )
        .unwrap_err(),
        "Key not present in map"
    );
    assert_eq!(
        ints(
            &lookup(
                vec![
                    m.clone(),
                    Arc::new(Int32Array::from(vec![2, 2])),
                    Arc::new(BooleanArray::from(vec![None, Some(true)]))
                ],
                None
            )
            .unwrap()
        ),
        vec![None, None]
    );
    assert_eq!(
        lookup(
            vec![
                m,
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![1, 1]))
            ],
            None
        )
        .unwrap_err(),
        "element_at check flag must be BOOLEAN"
    );
    assert_eq!(
        lookup(
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![1]))
            ],
            None
        )
        .unwrap_err(),
        "element_at expects MapArray, got Int32"
    );
}
#[test]
fn map_null_keys_are_rejected_and_value_key_type_mismatch_remains_explicit() {
    let m = map(
        Arc::new(NullArray::new(1)),
        Arc::new(Int32Array::from(vec![7])),
        vec![0, 1],
        None,
    );
    assert_eq!(
        lookup(vec![m, Arc::new(NullArray::new(1))], None).unwrap_err(),
        "map key type is nullable"
    );
    let m = map(
        Arc::new(StringArray::from(vec!["k"])),
        Arc::new(Int32Array::from(vec![7])),
        vec![0, 1],
        None,
    );
    assert_eq!(
        lookup(vec![m, Arc::new(Int32Array::from(vec![1]))], None).unwrap_err(),
        "map key type mismatch: Utf8 vs Int32"
    );
}

#[test]
fn original_map_null_values_keep_null_array_storage_for_hits_misses_and_null_maps() {
    let source = map(
        Arc::new(Int32Array::from(vec![1, 1, 1])),
        Arc::new(NullArray::new(3)),
        vec![0, 1, 2, 3, 3],
        Some(vec![true, true, false, true]),
    );
    let result = lookup(
        vec![source, Arc::new(Int32Array::from(vec![1, 2, 1, 1]))],
        Some(DataType::Null),
    )
    .unwrap();
    assert_eq!(result.data_type(), &DataType::Null);
    assert_eq!(result.len(), 4);
    assert_eq!(result.logical_null_count(), 4);
    assert!(result.to_data().nulls().is_none());
}

#[test]
fn original_map_constructor_rejects_null_keys_without_widening_its_output_contract() {
    use super::function::FunctionKind;
    for keys in [
        Arc::new(Int32Array::from(vec![None, Some(1)])) as ArrayRef,
        Arc::new(NullArray::new(2)) as ArrayRef,
    ] {
        let key_type = keys.data_type().clone();
        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let (mut arena, ids, chunk) = setup(vec![keys, values], 2).unwrap();
        let target = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", key_type, false),
                        Field::new("value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let id = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Map("map"),
                args: ids,
            },
            target,
        );
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| arena.eval(id, &chunk)))
                .expect("a NULL map key must return an explicit failure instead of panicking");
        let error =
            result.expect_err("a NULL map key cannot widen the frozen non-null key contract");
        assert!(
            error.to_ascii_lowercase().contains("key")
                && error.to_ascii_lowercase().contains("null"),
            "{error}"
        );
    }
}

#[test]
fn original_map_constructor_checks_only_the_real_if_branch_invocation_domain() {
    use super::function::FunctionKind;
    let condition: ArrayRef = Arc::new(BooleanArray::from(vec![false, true]));
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(1)]));
    let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
    let (mut arena, ids, chunk) = setup(vec![condition, keys, values], 2).unwrap();
    let target = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let taken = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Map("map"),
            args: vec![ids[1], ids[2]],
        },
        target.clone(),
    );
    let key = arena.push_typed(
        ExprNode::Literal(super::LiteralValue::Int32(7)),
        DataType::Int32,
    );
    let value = arena.push_typed(
        ExprNode::Literal(super::LiteralValue::Int32(8)),
        DataType::Int32,
    );
    let otherwise = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Map("map"),
            args: vec![key, value],
        },
        target.clone(),
    );
    let choice = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::If,
            args: vec![ids[0], taken, otherwise],
        },
        target.clone(),
    );
    let output = arena
        .eval(choice, &chunk)
        .expect("inactive row NULL keys must not enter the actual map invocation");
    assert_eq!(output.data_type(), &target);
    let output = output.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 1, 2]);
    assert_eq!(ints(output.keys()), vec![Some(7), Some(1)]);
    assert_eq!(ints(output.values()), vec![Some(8), Some(20)]);
}

#[test]
fn original_map_constructor_keeps_eager_child_failure_before_own_key_failure() {
    use super::function::FunctionKind;
    let (mut arena, _, chunk) = setup(vec![], 1).unwrap();
    let key = arena.push_typed(
        ExprNode::Literal(super::LiteralValue::Null),
        DataType::Int32,
    );
    let value = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Int32);
    let original = arena.eval(value, &chunk).unwrap_err();
    let target = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Map("map"),
            args: vec![key, value],
        },
        target,
    );
    assert_eq!(arena.eval(call, &chunk).unwrap_err(), original);
}

#[test]
fn original_map_constructor_zero_rows_and_empty_maps_have_no_null_key_obligation() {
    use super::function::FunctionKind;
    for (rows, with_pair, leaf) in [
        (0, true, DataType::Int32),
        (0, false, DataType::Int32),
        (3, false, DataType::Int32),
        (3, false, DataType::Null),
    ] {
        let (mut arena, _, chunk) = setup(vec![], rows).unwrap();
        let key = arena.push_typed(ExprNode::Literal(super::LiteralValue::Null), leaf.clone());
        let value = arena.push_typed(ExprNode::Literal(super::LiteralValue::Null), leaf.clone());
        let target = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", leaf.clone(), false),
                        Field::new("value", leaf.clone(), true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let args = if with_pair { vec![key, value] } else { vec![] };
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Map("map"),
                args,
            },
            target.clone(),
        );
        let output = arena.eval(call, &chunk).unwrap();
        let output = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(output.len(), rows);
        assert_eq!(output.data_type(), &target);
        assert_eq!(output.keys().len(), 0);
        assert_eq!(output.keys().logical_null_count(), 0);
        assert_eq!(output.value_offsets(), vec![0; rows + 1]);
    }
}
