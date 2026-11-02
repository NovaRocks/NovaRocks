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
//! Actual Rust allocation oracle. Input/schema/output are admitted before the
//! probe; no iterator turn may allocate a complete cell or row of any size.
use arrow::array::*;
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_result_contract::{
    ClientRenderSchema, NativeRenderType as N, RenderColumn, RenderField, RenderPresentation as P,
    RenderTimeUnit as U, RootProfileV1 as V,
};
use novarocks_result_render::{ArrowMysqlTextEncoder, BoundedClientRowEncoder, RenderTurnStatus};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
struct Probe;
// The test harness runs independent probes concurrently. Both admission and
// counters belong to the calling thread; process counters would contaminate
// an exact allocation receipt with another encoder's legitimate allocation.
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static COUNTERS: Cell<(usize, usize, usize)> = const { Cell::new((0, 0, 0)) };
}
#[global_allocator]
static ALLOC: Probe = Probe;
fn record(size: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = COUNTERS.try_with(|counter| {
            let (bytes, calls, maximum) = counter.get();
            counter.set((
                bytes.saturating_add(size),
                calls.saturating_add(1),
                maximum.max(size),
            ));
        });
    }
}
// SAFETY: Every allocator operation is forwarded unchanged to System; the
// probe records only numeric layout sizes and never dereferences allocations.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            record(layout.size());
        }
        p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            record(layout.size());
        }
        p
    }
    unsafe fn realloc(&self, p: *mut u8, old: Layout, size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(p, old, size) };
        if !p.is_null() {
            record(size);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) };
    }
}
fn start() {
    COUNTERS.with(|counter| counter.set((0, 0, 0)));
    TRACK.with(|t| t.set(true));
}
fn stop() -> (usize, usize, usize) {
    TRACK.with(|t| t.set(false));
    COUNTERS.with(Cell::get)
}
fn prepare(array: ArrayRef, render: RenderField) -> (Arc<ClientRenderSchema>, RecordBatch) {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap();
    let schema = Arc::new(
        ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: None,
                name: "v".into(),
                field: render,
            }],
            1,
        )
        .unwrap(),
    );
    (schema, batch)
}
fn f(n: N, p: P) -> RenderField {
    RenderField {
        native_type: n,
        presentation: p,
        nullable: true,
    }
}
#[test]
fn root_contract_schema_is_borrowed_through_actual_encoder_exit() {
    use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
    let text = "x".repeat(128 * 1024);
    let (schema, batch) = prepare(
        Arc::new(StringArray::from(vec![text])),
        f(N::String, P::ScalarText),
    );
    let schema = Arc::try_unwrap(schema).unwrap();
    let contract = Arc::new(RootOutputContract::new(
        RootProfileId::V1,
        FrozenRootOutput::ClientRows(schema),
    ));
    let weak = Arc::downgrade(&contract);
    let mut output = [0u8; V::EMIT_BYTES_PER_TURN];
    start();
    let encoded = ArrowMysqlTextEncoder::try_new_root(Arc::clone(&contract), batch);
    let (allocated, calls, _) = stop();
    let mut encoder = encoded.unwrap();
    assert_eq!(
        calls, 3,
        "starting a BE cursor must not clone schema Vec/String backings"
    );
    assert!(allocated <= encoder.scratch_capacity_bytes());
    drop(contract);
    assert!(weak.upgrade().is_some());
    loop {
        start();
        let result = encoder.step(&mut output);
        let (allocated, calls, _) = stop();
        assert_eq!((allocated, calls), (0, 0));
        let turn = result.unwrap();
        assert!(turn.emitted_bytes + turn.examined_bytes <= V::EMIT_BYTES_PER_TURN);
        if turn.status == RenderTurnStatus::InputComplete {
            break;
        }
    }
    encoder.cancel();
    assert!(weak.upgrade().is_some(), "cancel is not schema-owner exit");
    drop(encoder);
    assert!(weak.upgrade().is_none());
}
fn probe(schema: Arc<ClientRenderSchema>, batch: RecordBatch) {
    let mut out = vec![0; V::SEGMENT_BYTES];
    start();
    let result = ArrowMysqlTextEncoder::try_new(schema, batch);
    let (allocated, calls, maximum) = stop();
    let mut e = result.unwrap();
    let cap = e.scratch_capacity_bytes();
    assert!(cap < 2 * 1024 * 1024);
    assert!(allocated <= cap);
    assert_eq!(calls, 3);
    assert!(maximum <= cap);
    let initial = cap;
    start();
    let mut rows = 0;
    loop {
        let t = e.step(&mut out).unwrap();
        rows += t.completed_rows;
        assert!(t.emitted_bytes + t.examined_bytes <= 65536);
        assert!(t.visited_cells <= 1024);
        if t.status == RenderTurnStatus::InputComplete {
            break;
        }
    }
    let (allocated, calls, _) = stop();
    assert_eq!(calls, 0);
    assert_eq!(allocated, 0);
    assert_eq!(rows, 1);
    assert_eq!(e.scratch_capacity_bytes(), initial);
    e.cancel();
    assert_eq!(e.scratch_capacity_bytes(), initial);
}
#[test]
fn fixed_actual_capacity_and_zero_turn_allocations() {
    let value = "x".repeat(4 * 1024 * 1024);
    let (schema, batch) = prepare(
        Arc::new(StringArray::from(vec![value.as_str()])),
        f(N::String, P::ScalarText),
    );
    probe(schema, batch);
    let text = format!("{{\"k\":\"{}\"}}", "a\\\"".repeat(30000));
    let (schema, batch) = prepare(
        Arc::new(StringArray::from(vec![text.as_str()])),
        f(N::Json, P::JsonText),
    );
    probe(schema, batch);
    let array = Arc::new(BinaryArray::from(vec![vec![0xff; 200000].as_slice()])) as ArrayRef;
    let child = Arc::new(Field::new("item", array.data_type().clone(), true));
    let array = ListArray::new(
        child,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        array,
        None,
    );
    let (schema, batch) = prepare(
        Arc::new(array),
        f(
            N::List(Box::new(f(N::Binary, P::ScalarText))),
            P::MysqlContainer,
        ),
    );
    probe(schema, batch);
}
#[test]
fn temporal_and_variant_frozen_offset_do_not_allocate() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![-719561])),
        Arc::new(TimestampNanosecondArray::from(vec![-1]).with_timezone("UTC")),
    ];
    let fields = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Arc::new(Field::new(i.to_string(), a.data_type().clone(), true)))
        .collect::<Vec<_>>();
    let array = StructArray::new(fields.into(), arrays, None);
    let render = f(
        N::Struct(vec![
            novarocks_result_contract::NamedRenderField {
                name: "0".into(),
                field: f(N::Date, P::ScalarText),
            },
            novarocks_result_contract::NamedRenderField {
                name: "1".into(),
                field: f(
                    N::Timestamp {
                        unit: U::Nanosecond,
                        timezone: Some("UTC".into()),
                    },
                    P::TimestampContainerText,
                ),
            },
        ]),
        P::MysqlContainer,
    );
    let (schema, batch) = prepare(Arc::new(array), render);
    probe(schema, batch);
    for offset in [-86399, -59, -30, 30, 59, 86399] {
        let mut value = vec![12 << 2];
        value.extend_from_slice(&1i64.to_le_bytes());
        let mut data = ((3 + value.len()) as u32).to_le_bytes().to_vec();
        data.extend_from_slice(&[1, 0, 0]);
        data.extend(value);
        let (schema, batch) = prepare(
            Arc::new(LargeBinaryArray::from(vec![data.as_slice()])),
            f(
                N::Variant,
                P::VariantJson {
                    timezone_offset_seconds: offset,
                },
            ),
        );
        probe(schema, batch);
    }
}
#[test]
fn input_backing_remains_until_actual_encoder_exit() {
    let array = Arc::new(StringArray::from(vec!["x".repeat(200000)])) as ArrayRef;
    let weak = Arc::downgrade(&array);
    let (schema, batch) = prepare(array, f(N::String, P::ScalarText));
    let mut e = ArrowMysqlTextEncoder::try_new(schema, batch).unwrap();
    e.step(&mut [0; 100]).unwrap();
    e.cancel();
    assert!(weak.upgrade().is_some(), "cancel is not physical release");
    drop(e);
    assert!(weak.upgrade().is_none());
}

#[test]
fn maximum_native_depth_keeps_fixed_capacity() {
    let mut array = Arc::new(Int8Array::from(vec![0])) as ArrayRef;
    let mut render = f(N::SignedInteger(8), P::ScalarText);
    for _ in 0..63 {
        let child = Arc::new(Field::new("item", array.data_type().clone(), true));
        array = Arc::new(ListArray::new(
            child,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            array,
            None,
        ));
        render = f(N::List(Box::new(render)), P::MysqlContainer);
    }
    let (schema, batch) = prepare(array, render);
    probe(schema, batch);
}

#[test]
fn trino_large_strings_temporal_and_variant_turns_allocate_nothing() {
    use novarocks_result_contract::{ClientPresentationFamily, TrinoJsonFacts};
    use novarocks_result_render::ClientRowEncoder;
    let text = "\n中文\"".repeat(100_000);
    let mut variant = vec![16 << 2];
    let raw = text.as_bytes();
    variant.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    variant.extend_from_slice(raw);
    let mut variant_bytes = ((3 + variant.len()) as u32).to_le_bytes().to_vec();
    variant_bytes.extend_from_slice(&[1, 0, 0]);
    variant_bytes.extend_from_slice(&variant);
    let inputs: Vec<(ArrayRef, N)> = vec![
        (Arc::new(StringArray::from(vec![text.as_str()])), N::String),
        (
            Arc::new(
                TimestampNanosecondArray::from(vec![-123_456_789]).with_timezone("Asia/Shanghai"),
            ),
            N::Timestamp {
                unit: U::Nanosecond,
                timezone: Some("Asia/Shanghai".into()),
            },
        ),
        (
            Arc::new(LargeBinaryArray::from(vec![variant_bytes.as_slice()])),
            N::Variant,
        ),
    ];
    for (array, native) in inputs {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap();
        let schema = Arc::new(
            ClientRenderSchema::try_new(
                ClientPresentationFamily::TrinoJson(TrinoJsonFacts::default()),
                vec![RenderColumn {
                    source_ordinal: 0,
                    source_slot: None,
                    name: "v".into(),
                    field: f(native, P::TrinoValue),
                }],
                1,
            )
            .unwrap(),
        );
        let mut output = vec![0; V::SEGMENT_BYTES];
        start();
        let cursor = ClientRowEncoder::try_new(schema, batch);
        let (allocated, calls, _) = stop();
        let mut cursor = cursor.unwrap();
        let capacity = cursor.scratch_capacity_bytes();
        assert_eq!(calls, 3);
        assert!(allocated <= capacity);
        assert!(capacity < 2 * 1024 * 1024);
        loop {
            start();
            let turn = cursor.step(&mut output);
            let (allocated, calls, _) = stop();
            assert_eq!((allocated, calls), (0, 0));
            let turn = turn.unwrap();
            assert!(turn.emitted_bytes + turn.examined_bytes <= V::EMIT_BYTES_PER_TURN);
            assert!(turn.visited_cells <= V::CELLS_PER_TURN);
            assert_eq!(cursor.scratch_capacity_bytes(), capacity);
            if turn.status == RenderTurnStatus::InputComplete {
                break;
            }
        }
    }
}
