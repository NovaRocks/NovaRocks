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
use crate::source_custody_direct_tests::{Control, Host};
use arrow_array::{ArrayRef, MapArray, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::Field;

fn json_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        nullable,
        ValueLogicalType::Json,
    )
    .unwrap()
}

fn cast(
    source: &FunctionValueType,
    target: &FunctionValueType,
    array: &ArrayRef,
    mode: TrinoCastMode,
    host: &Arc<dyn AggregateStateAllocator>,
) -> Result<SelectedValues<'static>, KernelFailure> {
    let recipe =
        PreparedCastRecipe::try_new_trino(source, target, mode, crate::binding_test_control())
            .expect("declared CAST recipe");
    recipe.evaluate_selected_scoped(
        EvaluatedArgument::Column(array),
        Selection::all(array.len()),
        &[],
        &Control::default(),
        Some(host),
    )
}

#[test]
fn casts_native_json_scalars_on_demand_with_original_data_boundary() {
    let host = Arc::new(Host::default());
    let allocator: Arc<dyn AggregateStateAllocator> = host.clone();
    let data: ArrayRef = Arc::new(StringArray::from(vec![Some("null"), Some("réf"), None]));
    let encoded = cast(
        &FunctionValueType::new(DataType::Utf8, true),
        &json_type(true),
        &data,
        TrinoCastMode::Explicit,
        &allocator,
    )
    .unwrap();
    let values = encoded
        .values()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert!(matches!(
        Node::from_root(values.value(0)).unwrap().kind().unwrap(),
        Kind::Scalar(Scalar::String("null"))
    ));
    assert!(values.is_null(2));
    assert!(host.ledger.lock().unwrap().opaque > 0);
    let alias = Arc::clone(encoded.values());
    drop(encoded);
    assert!(host.ledger.lock().unwrap().opaque > 0);
    let decoded = cast(
        &json_type(true),
        &FunctionValueType::new(DataType::Utf8, true),
        &alias,
        TrinoCastMode::Explicit,
        &allocator,
    )
    .unwrap();
    assert_eq!(
        decoded
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("null"), Some("réf"), None]
    );
    drop(decoded);
    drop(alias);
    host.zero();
}

#[test]
fn json_null_and_own_row_errors_have_separate_cast_outcomes() {
    let host: Arc<dyn AggregateStateAllocator> = Arc::new(Host::default());
    let rows: Vec<Option<&[u8]>> = vec![
        Some(&[1, 0x20]),       // JSON null
        Some(&[1, 0x42, 1]),    // BOOLEAN true
        Some(&[1, 0x60, 1, 0]), // empty ARRAY
        None,                   // SQL null
    ];
    let source: ArrayRef = Arc::new(LargeBinaryArray::from(rows));
    let target = FunctionValueType::new(DataType::Int32, true);
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        let output = cast(&json_type(true), &target, &source, mode, &host).unwrap();
        let values = output
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![None, Some(1), None, None]
        );
        let errors = output.errors();
        if mode == TrinoCastMode::Explicit {
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].selected_ordinal(), 2);
        } else {
            assert!(errors.is_empty());
        }
    }
}

#[test]
fn container_casts_preserve_row_order_and_use_trino_utf16_map_key_order() {
    let host: Arc<dyn AggregateStateAllocator> = Arc::new(Host::default());
    let fields = vec![
        Arc::new(Field::new("z", DataType::Int64, false)),
        Arc::new(Field::new("a", DataType::Utf8, true)),
    ];
    let rows: ArrayRef = Arc::new(StructArray::new(
        fields.into(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("p")])),
        ],
        None,
    ));
    let output = cast(
        &FunctionValueType::new(rows.data_type().clone(), false),
        &json_type(false),
        &rows,
        TrinoCastMode::Explicit,
        &host,
    )
    .unwrap();
    let array = output
        .values()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let Kind::Object(object) = Node::from_root(array.value(0)).unwrap().kind().unwrap() else {
        panic!("named ROW as object");
    };
    assert_eq!(
        object
            .entries()
            .map(|entry| entry.unwrap().key.unwrap())
            .collect::<Vec<_>>(),
        vec!["z", "a"]
    );

    let keys = Arc::new(StringArray::from(vec!["\u{f900}", "\u{10000}"]));
    let vals: ArrayRef = Arc::new(
        Decimal128Array::from(vec![150, 200])
            .with_precision_and_scale(3, 2)
            .unwrap(),
    );
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", vals.data_type().clone(), true)),
        ]
        .into(),
        vec![keys, vals],
        None,
    );
    let maps: ArrayRef = Arc::new(MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
        entries,
        None,
        false,
    ));
    let output = cast(
        &FunctionValueType::new(maps.data_type().clone(), false),
        &json_type(false),
        &maps,
        TrinoCastMode::Explicit,
        &host,
    )
    .unwrap();
    let array = output
        .values()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let Kind::Object(object) = Node::from_root(array.value(0)).unwrap().kind().unwrap() else {
        panic!("object");
    };
    let observed = object
        .entries()
        .map(|entry| {
            let entry = entry.unwrap();
            let Kind::Scalar(Scalar::Decimal {
                scale, coefficient, ..
            }) = entry.value.kind().unwrap()
            else {
                panic!("decimal member");
            };
            (entry.key.unwrap(), scale, coefficient)
        })
        .collect::<Vec<_>>();
    assert_eq!(observed, vec![("\u{10000}", 2, 200), ("\u{f900}", 2, 150)]);
}
