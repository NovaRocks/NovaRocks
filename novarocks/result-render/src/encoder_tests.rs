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
use arrow::datatypes::{Field, Schema};
use novarocks_result_contract::{RenderColumn, RenderPresentation as P};
fn make(value: String, with_float: bool) -> ArrowClientRowEncoder {
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![value.as_str()]))];
    let mut fields = vec![RenderField {
        native_type: N::String,
        presentation: P::TrinoValue,
        nullable: false,
    }];
    if with_float {
        arrays.push(Arc::new(Float64Array::from(vec![1.25])));
        fields.push(RenderField {
            native_type: N::Float64,
            presentation: P::TrinoValue,
            nullable: false,
        });
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(
            arrays
                .iter()
                .map(|a| Field::new("x", a.data_type().clone(), false))
                .collect::<Vec<_>>(),
        )),
        arrays,
    )
    .unwrap();
    let cols = fields
        .into_iter()
        .enumerate()
        .map(|(i, field)| RenderColumn {
            source_ordinal: i as u32,
            source_slot: None,
            name: "x".into(),
            field,
        })
        .collect();
    ArrowClientRowEncoder::try_new(
        Arc::new(
            ClientRenderSchema::try_new(
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(Default::default()),
                cols,
                batch.num_columns(),
            )
            .unwrap(),
        ),
        batch,
    )
    .unwrap()
}
#[test]
fn exact_threshold_path_and_float_once() {
    for (n, large) in [(65531, false), (65532, false), (65533, true)] {
        let mut e = make("x".repeat(n), false);
        let mut out = [0; 65536];
        let mut counted = false;
        loop {
            let t = e.step(&mut out).unwrap();
            counted |= e.phase == Phase::Count || e.phase == Phase::Emit;
            if t.status == RenderTurnStatus::InputComplete {
                break;
            }
        }
        assert_eq!(counted, large, "{n}");
    }
    FLOAT_FORMATS.with(|n| n.set(0));
    let mut e = make("small".into(), true);
    let mut out = [0; 65536];
    loop {
        if e.step(&mut out).unwrap().status == RenderTurnStatus::InputComplete {
            break;
        }
    }
    assert_eq!(FLOAT_FORMATS.with(std::cell::Cell::get), 1);
}
#[test]
fn u32_prefix_not_published_on_exhausted_cell_budget() {
    let mut e = make("x".repeat(65534), false);
    let mut out = [0; 100];
    while e.phase != Phase::Emit {
        e.step(&mut out).unwrap();
    }
    e.reset_row(Phase::Emit);
    e.row_length = 65538;
    e.lengths[0] = 65537;
    let mut budget = Budget::new();
    budget.cells = V::CELLS_PER_TURN;
    assert!(!e.start_cell(&mut budget).unwrap());
    assert_eq!(e.prefix_at, 0);
    assert!(!e.cell_started);
    let t = e.step(&mut out[..5]).unwrap();
    assert_eq!(t.emitted_bytes, 5);
    assert_eq!(out[4], b'[');
}

#[test]
fn actual_scratch_capacity_layout() {
    let e = make("x".into(), false);
    let capacity = e.scratch_capacity_bytes();
    eprintln!(
        "SCRATCH capacity_bytes={capacity} staging_bytes={} cell_lengths_bytes={} task_slots={} task_size={} stack_capacity_bytes={} control_bytes={}",
        size_of::<[u8; 65536]>(),
        size_of::<[u32; 4096]>(),
        e.cursor.stack.capacity(),
        size_of::<EncodeStep>(),
        e.cursor.stack.capacity() * size_of::<EncodeStep>(),
        size_of::<ArrowClientRowEncoder>()
    );
    assert_eq!(
        capacity,
        size_of::<ArrowClientRowEncoder>() + 65536 + 16384 + 196 * size_of::<EncodeStep>()
    );
    assert!(capacity < 2 * 1024 * 1024);
}

fn single_cell_body(array: ArrayRef, native_type: N, name: &str) -> Vec<u8> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            name,
            array.data_type().clone(),
            false,
        )])),
        vec![array],
    )
    .unwrap();
    let schema = ClientRenderSchema::try_new(
        novarocks_result_contract::ClientPresentationFamily::TrinoJson(Default::default()),
        vec![RenderColumn {
            source_ordinal: 0,
            source_slot: None,
            name: name.into(),
            field: RenderField {
                native_type,
                presentation: P::TrinoValue,
                nullable: false,
            },
        }],
        1,
    )
    .unwrap();
    let mut encoder = ArrowClientRowEncoder::try_new(Arc::new(schema), batch).unwrap();
    let mut body = Vec::new();
    for _ in 0..1024 {
        let mut segment = [0; 4096];
        let turn = encoder.step(&mut segment).unwrap();
        body.extend_from_slice(&segment[..turn.emitted_bytes]);
        if turn.status == RenderTurnStatus::InputComplete {
            return body;
        }
    }
    panic!("tiny fixture did not finish within bounded turns");
}

#[test]
fn plain_binary_names_do_not_infer_opaque_domains() {
    for name in ["hll", "bitmap", "object", "percentile"] {
        let body = single_cell_body(
            Arc::new(BinaryArray::from(vec![b"external state".as_slice()])),
            N::Binary,
            name,
        );
        let mut expected = 24u32.to_le_bytes().to_vec();
        expected.extend_from_slice(br#"["ZXh0ZXJuYWwgc3RhdGU="]"#);
        assert_eq!(body, expected);
    }
}

#[test]
fn typed_root_cursor_keeps_the_actual_input_type_owner_and_rejects_another_schema() {
    use novarocks_type_contract::{
        ClientInputTypeBindings, TypedValueBinding, ValidatedClientInputTypes, ValueKind,
        ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageHold,
        ValueTypeStorageLifetime,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Input {
        schema: Arc<Schema>,
        binding: TypedValueBinding,
    }
    impl ClientInputTypeBindings for Input {
        fn binding_at(&self, ordinal: usize) -> Option<&TypedValueBinding> {
            (ordinal == 0).then_some(&self.binding)
        }
        fn slot_at(&self, ordinal: usize) -> Option<u32> {
            (ordinal == 0).then_some(7)
        }
        fn matches_schema(&self, schema: &Arc<Schema>) -> bool {
            Arc::ptr_eq(&self.schema, schema)
        }
    }
    let released = Arc::new(AtomicUsize::new(0));
    let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(released.clone())));
    let binding = TypedValueBinding::try_new(
        ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            false,
            storage.clone(),
        )
        .unwrap(),
        ValueLayout::try_new(&DataType::Utf8, storage.clone()).unwrap(),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "incoming",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["{}"]))],
    )
    .unwrap();
    let contract = Arc::new(RootOutputContract::new(
        novarocks_result_contract::RootProfileId::V1,
        FrozenRootOutput::ClientRows(
            ClientRenderSchema::try_new(
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(Default::default()),
                vec![RenderColumn {
                    source_ordinal: 0,
                    source_slot: Some(7),
                    name: "published_name".into(),
                    field: RenderField {
                        native_type: N::Json,
                        presentation: P::TrinoValue,
                        nullable: false,
                    },
                }],
                1,
            )
            .unwrap(),
        ),
    ));
    let owner: Arc<dyn ClientInputTypeBindings> = Arc::new(Input {
        schema: schema.clone(),
        binding,
    });
    let proof = ValidatedClientInput::try_new(
        ValidatedClientInputTypes::try_new(contract, owner.clone()).unwrap(),
    )
    .unwrap();
    let other_batch =
        RecordBatch::try_new(Arc::new(schema.as_ref().clone()), batch.columns().to_vec()).unwrap();
    assert!(
        ArrowClientRowEncoder::try_new_typed_root(proof.clone(), other_batch).is_err(),
        "a different physical schema cannot borrow a previously established proof"
    );
    let mut encoder = ArrowClientRowEncoder::try_new_typed_root(proof.clone(), batch).unwrap();
    drop(storage);
    drop(owner);
    drop(proof);
    drop(schema);
    assert_eq!(released.load(Ordering::SeqCst), 0);
    let mut output = [0; 64];
    let mut emitted = Vec::new();
    for _ in 0..10 {
        let turn = encoder.step(&mut output).unwrap();
        emitted.extend_from_slice(&output[..turn.emitted_bytes]);
        if turn.status == RenderTurnStatus::InputComplete {
            break;
        }
    }
    assert_eq!(emitted, b"\x06\0\0\0[\"{}\"]");
    assert_eq!(released.load(Ordering::SeqCst), 0);
    drop(encoder);
    assert_eq!(released.load(Ordering::SeqCst), 1);
}
