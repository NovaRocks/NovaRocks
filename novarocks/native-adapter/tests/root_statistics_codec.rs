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

//! Module-only StatisticsArtifactV1 codec evidence, not Native/FE/source admission.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Builder, ListBuilder, MapBuilder, MapFieldNames,
    RecordBatch, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_native_adapter::root_statistics_codec::*;
use novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
struct Probe;
fn allocated() {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCATIONS.try_with(|calls| calls.set(calls.get() + 1));
    }
}
// SAFETY: All requests are forwarded unchanged to System; observation uses
// allocation-free per-thread scalar cells and never allocation contents.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct StopTracking;
impl Drop for StopTracking {
    fn drop(&mut self) {
        TRACK.with(|track| track.set(false));
    }
}
fn no_allocation<T>(operation: impl FnOnce() -> T) -> T {
    ALLOCATIONS.with(|calls| calls.set(0));
    TRACK.with(|track| track.set(true));
    let stop = StopTracking;
    let value = operation();
    drop(stop);
    assert_eq!(ALLOCATIONS.with(Cell::get), 0, "codec operation allocated");
    value
}

struct Row<'a> {
    fields: &'a [i32],
    blob: &'a str,
    body: Option<&'a [u8]>,
    properties: &'a [(&'a str, &'a str)],
}
fn batch(rows: &[Row<'_>]) -> RecordBatch {
    let mut fields = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Int32,
        false,
    )));
    let mut properties = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
    .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
    for row in rows {
        fields.values().append_slice(row.fields);
        fields.append(true);
        for (key, value) in row.properties {
            properties.keys().append_value(key);
            properties.values().append_value(value);
        }
        properties.append(true).unwrap();
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(fields.finish()),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.blob),
        )),
        Arc::new(BinaryArray::from_iter(rows.iter().map(|row| row.body))),
        Arc::new(properties.finish()),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("input_fields", columns[0].data_type().clone(), false),
        Field::new("blob_type", DataType::Utf8, false),
        Field::new("body", DataType::Binary, true),
        Field::new("properties", columns[3].data_type().clone(), false),
    ]));
    RecordBatch::try_new(schema, columns).unwrap()
}
fn one(fields: &[i32], blob: &str, body: Option<&[u8]>) -> RecordBatch {
    batch(&[Row {
        fields,
        blob,
        body,
        properties: &[],
    }])
}

#[test]
fn sliced_record_uses_actual_list_string_binary_and_map_offsets() {
    let batch = batch(&[
        Row {
            fields: &[-1],
            blob: "",
            body: None,
            properties: &[("ignored", "before")],
        },
        Row {
            fields: &[11, 13],
            blob: "theta",
            body: Some(&[0xff, 0x80, 0]),
            properties: &[],
        },
        Row {
            fields: &[2, 2],
            blob: "",
            body: None,
            properties: &[("ignored", "after")],
        },
    ]);
    let (encoded, totals, _) = encode(batch.slice(1, 1), 1);
    assert_eq!(
        encoded,
        [
            b'S', b'T', b'A', b'1', 40, 0, 0, 0, 2, 0, 0, 0, 5, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            11, 0, 0, 0, 13, 0, 0, 0, b't', b'h', b'e', b't', b'a', 0xff, 0x80, 0,
        ],
    );
    assert_eq!(totals.rows, 1);
    assert_eq!(totals.body_bytes, 3);
    assert_eq!(totals.metadata_bytes, 37);
}
fn encode(batch: RecordBatch, output_size: usize) -> (Vec<u8>, StatisticsCodecTotals, usize) {
    let mut encoder = no_allocation(|| {
        StatisticsArtifactEncoder::try_new(batch, StatisticsCodecTotals::default())
    })
    .unwrap();
    let mut output = vec![0; output_size];
    let mut bytes = Vec::new();
    let mut rows = 0;
    let mut turns = 0;
    loop {
        turns += 1;
        assert!(turns <= 100_000, "cursor failed to make bounded progress");
        let turn = no_allocation(|| encoder.step(&mut output)).unwrap();
        assert!(turn.examined_bytes <= STATISTICS_TURN_BYTES);
        assert!(turn.work <= STATISTICS_TURN_WORK);
        assert!(turn.emitted_bytes <= turn.examined_bytes);
        bytes.extend_from_slice(&output[..turn.emitted_bytes]);
        rows += turn.completed_rows;
        if turn.status == StatisticsCodecStatus::InputComplete {
            break;
        }
        assert!(turn.work > 0 || turn.emitted_bytes > 0);
    }
    assert_eq!(rows as usize, encoder.totals().rows);
    (bytes, encoder.totals(), turns)
}

#[test]
fn literal_golden_preserves_composite_order_utf8_and_binary() {
    let (bytes, totals, _) = encode(
        batch(&[
            Row {
                fields: &[9, 7],
                blob: "é",
                body: Some(&[0, 255, 65]),
                properties: &[],
            },
            Row {
                fields: &[3],
                blob: "x",
                body: Some(&[]),
                properties: &[],
            },
        ]),
        128,
    );
    let expected = [
        83, 84, 65, 49, 37, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 7,
        0, 0, 0, 195, 169, 0, 255, 65, 83, 84, 65, 49, 29, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 120,
    ];
    assert_eq!(bytes, expected);
    assert_eq!(
        totals,
        StatisticsCodecTotals {
            rows: 2,
            body_bytes: 3,
            metadata_bytes: 63
        }
    );
    let header = no_allocation(|| StatisticsArtifactHeader::parse(&bytes)).unwrap();
    assert_eq!(
        (
            header.field_count(),
            header.blob_type_bytes(),
            header.body_bytes(),
            header.record_bytes()
        ),
        (2, 2, 3, 37)
    );
    assert_eq!(header.validate_record_bytes(37), Ok(()));
    assert_eq!(
        header.validate_record_bytes(36),
        Err(StatisticsCodecError::TruncatedRecord)
    );
    assert_eq!(
        header.validate_record_bytes(38),
        Err(StatisticsCodecError::TrailingRecordBytes)
    );
}

#[test]
fn every_short_header_and_invalid_declaration_rejects_without_allocation() {
    let valid: [u8; 24] = [
        83, 84, 65, 49, 29, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    for len in 0..24 {
        assert_eq!(
            no_allocation(|| StatisticsArtifactHeader::parse(&valid[..len])),
            Err(StatisticsCodecError::TruncatedHeader)
        );
    }
    for (at, value, expected) in [
        (
            0,
            u32::from_le_bytes(*b"STA2"),
            StatisticsCodecError::HeaderVersion,
        ),
        (4, 28, StatisticsCodecError::RecordLength),
        (4, u32::MAX, StatisticsCodecError::RecordLength),
        (8, 0, StatisticsCodecError::FieldIds),
        (8, 1025, StatisticsCodecError::FieldIds),
        (12, 0, StatisticsCodecError::BlobType),
        (12, 65537, StatisticsCodecError::BlobType),
        (16, 16777217, StatisticsCodecError::BodyLimit),
        (20, 1, StatisticsCodecError::Properties),
    ] {
        let mut bytes = valid;
        bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
        assert_eq!(
            no_allocation(|| StatisticsArtifactHeader::parse(&bytes)),
            Err(expected)
        );
    }
}

#[test]
fn one_byte_outputs_split_header_field_utf8_and_body_without_padding() {
    let input = one(&[257, 7], "é", Some(b"abc"));
    let (normal, _, _) = encode(input.clone(), 4096);
    let (split, totals, turns) = encode(input, 1);
    assert_eq!(split, normal);
    assert_eq!(turns, split.len());
    assert_eq!(totals.rows, 1);
}

#[test]
fn sixteen_mib_body_crosses_segments_and_turns_without_record_materialization() {
    let body = vec![0xa5; MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES];
    let input = one(&[7], "b", Some(&body));
    let mut encoder = no_allocation(|| {
        StatisticsArtifactEncoder::try_new(input, StatisticsCodecTotals::default())
    })
    .unwrap();
    let mut output = [0; 64 * 1024];
    let mut offset = 0;
    let mut completed = 0;
    let mut turns = 0;
    while completed == 0 {
        turns += 1;
        let turn = no_allocation(|| encoder.step(&mut output)).unwrap();
        assert!(turn.work <= 1024 && turn.examined_bytes <= 65536);
        for (at, byte) in output[..turn.emitted_bytes].iter().enumerate() {
            if offset + at >= 29 {
                assert_eq!(*byte, 0xa5);
            }
        }
        offset += turn.emitted_bytes;
        completed += turn.completed_rows;
        if turn.status == StatisticsCodecStatus::InputComplete {
            break;
        }
        assert!(turns < 1024);
    }
    assert_eq!(offset, body.len() + 29);
    assert!(turns > 256);
    assert_eq!(encoder.totals().body_bytes, body.len());
}

#[test]
fn constructor_does_not_validate_rows_and_invalid_rows_emit_no_prefix() {
    let large = vec![0; MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES + 1];
    let fields_1025 = vec![1; 1025];
    let long_blob = "x".repeat(65537);
    let cases = [
        (one(&[], "x", Some(b"")), StatisticsCodecError::FieldIds),
        (one(&[0], "x", Some(b"")), StatisticsCodecError::FieldIds),
        (one(&[-1], "x", Some(b"")), StatisticsCodecError::FieldIds),
        (
            one(&[7, 9, 7], "x", Some(b"")),
            StatisticsCodecError::FieldIds,
        ),
        (
            one(&fields_1025, "x", Some(b"")),
            StatisticsCodecError::FieldIds,
        ),
        (one(&[7], "", Some(b"")), StatisticsCodecError::BlobType),
        (
            one(&[7], &long_blob, Some(b"")),
            StatisticsCodecError::BlobType,
        ),
        (
            one(&[7], "x", Some(&large)),
            StatisticsCodecError::BodyLimit,
        ),
        (one(&[7], "x", None), StatisticsCodecError::NullValue),
        (
            batch(&[Row {
                fields: &[7],
                blob: "x",
                body: Some(b""),
                properties: &[("k", "v")],
            }]),
            StatisticsCodecError::Properties,
        ),
    ];
    for (input, expected) in cases {
        let mut encoder = no_allocation(|| {
            StatisticsArtifactEncoder::try_new(input, StatisticsCodecTotals::default())
        })
        .unwrap();
        let mut output = [0xab; 128];
        assert_eq!(no_allocation(|| encoder.step(&mut output)), Err(expected));
        assert_eq!(output, [0xab; 128]);
        assert_eq!(encoder.totals(), StatisticsCodecTotals::default());
        assert_eq!(encoder.step(&mut output), Err(StatisticsCodecError::Failed));
    }
}

#[test]
fn adversarial_duplicate_probes_obey_work_quantum_before_any_emission() {
    // Multiples of 2048 share the same low hash bits. The worst half-full
    // linear-probe table remains finite, and EVERY comparison is charged.
    let ids: Vec<i32> = (0..1024).map(|index| 1 + index * 2048).collect();
    let input = one(&ids, "x", Some(b""));
    let mut encoder = no_allocation(|| {
        StatisticsArtifactEncoder::try_new(input, StatisticsCodecTotals::default())
    })
    .unwrap();
    let mut output = [0; 8192];
    let mut work = 0;
    let mut zero_output_turns = 0;
    let mut completed = 0;
    for _ in 0..1024 {
        let turn = no_allocation(|| encoder.step(&mut output)).unwrap();
        assert!(turn.work <= 1024 && turn.examined_bytes <= 65536);
        work += turn.work;
        zero_output_turns += usize::from(turn.emitted_bytes == 0);
        completed += turn.completed_rows;
        if turn.status == StatisticsCodecStatus::InputComplete {
            break;
        }
    }
    assert_eq!(completed, 1);
    assert!(zero_output_turns >= 512);
    assert!(work >= 1024 * 1025 / 2);
    assert_eq!(encoder.totals().rows, 1);
}

#[test]
fn only_cumulative_metadata_limits_artifact_emission() {
    let prior = StatisticsCodecTotals {
        rows: 4096,
        body_bytes: 128 * 1024 * 1024,
        metadata_bytes: STATISTICS_METADATA_BYTES - 29,
    };
    let input = one(&[7], "x", Some(b"a"));
    let mut encoder = no_allocation(|| StatisticsArtifactEncoder::try_new(input, prior)).unwrap();
    let turn = no_allocation(|| encoder.step(&mut [0; 30])).unwrap();
    assert_eq!(turn.status, StatisticsCodecStatus::InputComplete);
    assert_eq!(turn.completed_rows, 1);
    assert_eq!(
        encoder.totals(),
        StatisticsCodecTotals {
            rows: prior.rows + 1,
            body_bytes: prior.body_bytes + 1,
            metadata_bytes: STATISTICS_METADATA_BYTES
        }
    );
    let next = one(&[7], "x", Some(b""));
    let mut encoder =
        no_allocation(|| StatisticsArtifactEncoder::try_new(next, encoder.totals())).unwrap();
    let mut output = [0xab; 64];
    assert_eq!(
        no_allocation(|| encoder.step(&mut output)),
        Err(StatisticsCodecError::MetadataLimit)
    );
    assert_eq!(output, [0xab; 64]);
}

#[test]
fn empty_input_empty_output_completion_and_cancel_keep_exact_state() {
    let input = batch(&[]);
    let mut encoder = no_allocation(|| {
        StatisticsArtifactEncoder::try_new(input, StatisticsCodecTotals::default())
    })
    .unwrap();
    assert_eq!(
        no_allocation(|| encoder.step(&mut [])).unwrap(),
        StatisticsCodecTurn {
            emitted_bytes: 0,
            examined_bytes: 0,
            work: 0,
            completed_rows: 0,
            status: StatisticsCodecStatus::InputComplete
        }
    );
    assert!(StatisticsArtifactEncoder::inline_capacity_bytes() >= 2048 * 8);

    let input = one(&[7], "x", Some(b"abc"));
    let weak = Arc::downgrade(input.column(2));
    let mut encoder = no_allocation(|| {
        StatisticsArtifactEncoder::try_new(input, StatisticsCodecTotals::default())
    })
    .unwrap();
    let turn = no_allocation(|| encoder.step(&mut [])).unwrap();
    assert_eq!(turn.status, StatisticsCodecStatus::NeedsOutput);
    assert_eq!((turn.work, turn.emitted_bytes), (0, 0));
    no_allocation(|| encoder.step(&mut [0; 1])).unwrap();
    assert_eq!(encoder.totals(), StatisticsCodecTotals::default());
    assert!(weak.upgrade().is_some());
    no_allocation(|| encoder.cancel());
    assert!(weak.upgrade().is_none());
    assert_eq!(
        encoder.step(&mut [0; 64]),
        Err(StatisticsCodecError::Cancelled)
    );
}

#[test]
fn exact_schema_order_names_and_nested_nullability_are_required() {
    let input = one(&[7], "x", Some(b"a"));
    for variant in 0..3 {
        let mut fields = input
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        match variant {
            0 => fields[0] = fields[0].clone().with_name("renamed"),
            1 => fields[0] = fields[0].clone().with_nullable(true),
            _ => fields[3] = fields[3].clone().with_nullable(true),
        }
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), input.columns().to_vec()).unwrap();
        assert!(matches!(
            no_allocation(|| StatisticsArtifactEncoder::try_new(
                batch,
                StatisticsCodecTotals::default()
            )),
            Err(StatisticsCodecError::Schema)
        ));
    }
}

#[test]
fn an_input_batch_with_more_than_4096_artifacts_is_emitted() {
    let count = 4097;
    let rows = (0..count)
        .map(|_| Row {
            fields: &[7],
            blob: "x",
            body: Some(b""),
            properties: &[],
        })
        .collect::<Vec<_>>();
    let (bytes, totals, turns) = encode(batch(&rows), 64 * 1024);
    assert_eq!(bytes.len(), count * 29);
    assert_eq!(totals.rows, count);
    assert!(turns > 1);
}

#[test]
fn maximal_blob_and_header_declarations_fit_exactly() {
    let blob = "x".repeat(65536);
    let (bytes, totals, turns) = encode(one(&[7], &blob, Some(b"")), 64 * 1024);
    assert_eq!(bytes.len(), 65536 + 28);
    assert_eq!(totals.metadata_bytes, bytes.len());
    assert!(turns > 1);
    let mut header = [0; 24];
    header[..4].copy_from_slice(b"STA1");
    for (at, value) in [
        (4, 24_u32 + 4096 + 65536 + 16777216),
        (8, 1024),
        (12, 65536),
        (16, 16777216),
    ] {
        header[at..at + 4].copy_from_slice(&value.to_le_bytes());
    }
    let parsed = no_allocation(|| StatisticsArtifactHeader::parse(&header)).unwrap();
    assert_eq!(parsed.field_count(), 1024);
    assert_eq!(parsed.body_bytes(), 16777216);
    assert_eq!(parsed.metadata_bytes(), 24 + 4096 + 65536);
}
