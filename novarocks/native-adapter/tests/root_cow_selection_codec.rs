// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Module-only CowSelectionArrowV1 codec evidence: exact Arrow round trips
//! for the closed type set (including sliced inputs), refusals before any
//! record byte, bounded turns and no allocation during emission.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
    Int8Array, Int32Array, Int64Array, LargeStringArray, ListArray, MapArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema, SchemaRef};
use novarocks_native_adapter::root_cow_selection_codec::*;
use novarocks_result_render::RenderTurnStatus;
use novarocks_spi::connector::MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES;

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

mod support {
    include!("common/cow_recipe.rs");
}

const UNLIMITED: usize = usize::MAX;

#[test]
fn record_above_256_mib_uses_the_prepaid_cursor_without_a_codec_size_quota() {
    // Alias one real payload across independent columns. The serialized record
    // exceeds 256 MiB while the source has only one MiB of physical backing.
    let payload = "x".repeat(1024 * 1024);
    let array: ArrayRef = Arc::new(StringArray::from(vec![payload.as_str()]));
    let columns = 257;
    let schema = Arc::new(Schema::new(
        (0..columns)
            .map(|index| Field::new(format!("value_{index}"), DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let source = RecordBatch::try_new(schema, vec![array; columns]).unwrap();
    let recipe = support::recipe(source.schema_ref());
    let source = support::normalized(&source, &recipe);
    let scratch = CowSelectionEncoder::scratch_bytes(&source, &recipe).unwrap();
    assert!(matches!(
        CowSelectionEncoder::try_new(&source, &recipe, CowSelectionTotals::default(), scratch - 1),
        Err(CowSelectionCodecError::ScratchLimit)
    ));
    let mut encoder =
        CowSelectionEncoder::try_new(&source, &recipe, CowSelectionTotals::default(), scratch)
            .unwrap();
    let mut output = [0u8; COW_SELECTION_TURN_BYTES];
    let mut emitted = 0u64;
    let mut turns = 0;
    let mut declared = None;
    loop {
        TRACK.with(|track| track.set(true));
        ALLOCATIONS.with(|calls| calls.set(0));
        let turn = encoder.step(&mut output);
        TRACK.with(|track| track.set(false));
        assert_eq!(ALLOCATIONS.with(Cell::get), 0);
        if turns == 0 {
            let header = CowSelectionRecordHeader::parse(&output[..turn.emitted_bytes]).unwrap();
            // Each Utf8 column has one node, three buffer descriptors, no
            // validity allocation, two i32 offsets and the one MiB payload.
            let expected = COW_SELECTION_HEADER_BYTES as u64
                + columns as u64 * (8 * 4 + 8 + payload.len() as u64);
            assert_eq!(header.record_bytes(), expected);
            assert!(header.record_bytes() > 256 * 1024 * 1024);
            declared = Some(header.record_bytes());
        }
        emitted += turn.emitted_bytes as u64;
        turns += 1;
        assert!(turn.examined_bytes <= COW_SELECTION_TURN_BYTES);
        assert!(turn.visited_cells <= COW_SELECTION_TURN_WORK);
        if turn.status == RenderTurnStatus::InputComplete {
            break;
        }
    }
    assert!(turns > 4096);
    assert_eq!(Some(emitted), declared);
    assert_eq!(encoder.totals().rows(), 1);
}

/// Encode one input; every step is measured for allocation and bounds.
fn encode(
    batch: &RecordBatch,
    totals: CowSelectionTotals,
    output: usize,
) -> (Vec<u8>, CowSelectionTotals, u64) {
    let recipe = support::recipe(batch.schema_ref());
    let batch = support::normalized(batch, &recipe);
    let mut encoder = CowSelectionEncoder::try_new(&batch, &recipe, totals, UNLIMITED).unwrap();
    let mut buffer = vec![0_u8; output];
    let (mut bytes, mut rows) = (Vec::new(), 0);
    loop {
        TRACK.with(|track| track.set(true));
        ALLOCATIONS.with(|calls| calls.set(0));
        let turn = encoder.step(&mut buffer);
        TRACK.with(|track| track.set(false));
        assert_eq!(ALLOCATIONS.with(Cell::get), 0, "cursor step allocated");
        assert!(turn.examined_bytes <= COW_SELECTION_TURN_BYTES);
        assert!(turn.visited_cells <= COW_SELECTION_TURN_WORK);
        bytes.extend_from_slice(&buffer[..turn.emitted_bytes]);
        rows += turn.completed_rows;
        if turn.status == RenderTurnStatus::InputComplete {
            return (bytes, encoder.totals(), rows);
        }
    }
}
fn split(mut bytes: &[u8]) -> Vec<(CowSelectionRecordHeader, &[u8])> {
    let mut records = Vec::new();
    while !bytes.is_empty() {
        let header = CowSelectionRecordHeader::parse(bytes).unwrap();
        let length = header.record_bytes() as usize;
        records.push((header, &bytes[..length]));
        bytes = &bytes[length..];
    }
    records
}
/// Decode a complete stream back to its batches.
fn decode(bytes: &[u8], recipe: &CowSelectionRecipe) -> (SchemaRef, Vec<RecordBatch>) {
    let batches = split(bytes)
        .iter()
        .map(|(header, record)| {
            assert_eq!(header.kind(), CowSelectionRecordKind::Batch);
            CowSelectionDecoder::decode_batch(recipe, record).unwrap()
        })
        .collect();
    (Arc::clone(recipe.schema()), batches)
}

fn wide_batch() -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let tags = ListArray::new(
        Arc::clone(&item),
        OffsetBuffer::from_lengths([2, 0, 1, 3, 1, 0, 2, 1, 1, 2, 0]),
        Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
            None,
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
        ])),
        Some(
            vec![
                true, true, false, true, true, true, true, true, true, true, true,
            ]
            .into(),
        ),
    );
    let point_fields = Fields::from(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("label", DataType::LargeUtf8, true),
    ]);
    let points = StructArray::new(
        point_fields.clone(),
        vec![
            Arc::new(Int32Array::from((0..11).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![
                Some("a"),
                None,
                Some("ccc"),
                Some(""),
                Some("e"),
                Some("ff"),
                None,
                Some("h"),
                Some("i"),
                Some("jj"),
                Some("k"),
            ])) as ArrayRef,
        ],
        Some(
            vec![
                true, true, true, false, true, true, true, true, true, true, true,
            ]
            .into(),
        ),
    );
    let keys = Int32Array::from(vec![
        Some(1),
        Some(0),
        None,
        Some(2),
        Some(1),
        Some(1),
        Some(0),
        Some(2),
        None,
        Some(0),
        Some(1),
    ]);
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        keys,
        Arc::new(StringArray::from(vec![Some("x"), Some("yy"), None])),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("_file", DataType::Utf8, false),
        Field::new("_pos", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, true),
        Field::new("amount", DataType::Decimal128(12, 2), true),
        Field::new(
            "at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("uuid", DataType::FixedSizeBinary(4), true),
        Field::new("tags", DataType::List(item), true),
        Field::new("point", DataType::Struct(point_fields), true),
        Field::new("city", dictionary.data_type().clone(), true),
        Field::new("effect", DataType::Int8, false),
    ]));
    let rows = 11;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|row| format!("s3://f/{row}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (100..100 + rows as i64).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
                Some(true),
                Some(false),
                None,
                Some(true),
                Some(false),
                Some(true),
                Some(true),
            ])),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(1),
                    Some(-2),
                    None,
                    Some(400),
                    Some(5),
                    Some(6),
                    Some(7),
                    None,
                    Some(9),
                    Some(10),
                    Some(11),
                ])
                .with_precision_and_scale(12, 2)
                .unwrap(),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(
                    (0..rows as i64)
                        .map(|row| (row % 3 != 0).then_some(row * 1000))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    (0..rows as u8).map(|row| (row != 5).then_some([row, row, row, row])),
                    4,
                )
                .unwrap(),
            ),
            Arc::new(tags),
            Arc::new(points),
            Arc::new(dictionary),
            Arc::new(Int8Array::from(vec![2; rows])),
        ],
    )
    .unwrap()
}

#[test]
fn every_supported_carrier_round_trips_exactly() {
    let batch = wide_batch();
    let (bytes, totals, rows) = encode(&batch, CowSelectionTotals::default(), 1 << 20);
    assert_eq!(rows, 11);
    assert_eq!((totals.batches(), totals.rows()), (1, 11));
    let recipe = support::recipe(batch.schema_ref());
    let (schema, batches) = decode(&bytes, &recipe);
    assert_eq!(&schema, recipe.schema());
    assert_eq!(batches, [support::normalized(&batch, &recipe)]);
}

#[test]
fn sliced_inputs_rebase_bitmaps_and_offsets() {
    let batch = wide_batch();
    for (offset, len) in [(3, 5), (1, 9), (7, 4), (10, 1)] {
        let slice = batch.slice(offset, len);
        let (bytes, _, rows) = encode(&slice, CowSelectionTotals::default(), 1 << 20);
        assert_eq!(rows, len as u64);
        let recipe = support::recipe(slice.schema_ref());
        let (_, batches) = decode(&bytes, &recipe);
        assert_eq!(
            batches,
            [support::normalized(&slice, &recipe)],
            "slice {offset}+{len}"
        );
    }
}

#[test]
fn tiny_outputs_produce_identical_bytes_and_large_values_split_turns() {
    let big = "z".repeat(150 * 1024);
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            Some(big.as_str()),
            None,
            Some("t"),
        ]))],
    )
    .unwrap();
    let (whole, ..) = encode(&batch, CowSelectionTotals::default(), 4 << 20);
    let (tiny, ..) = encode(&batch, CowSelectionTotals::default(), 5);
    assert_eq!(whole, tiny);
    let recipe = support::recipe(batch.schema_ref());
    assert_eq!(
        decode(&whole, &recipe).1,
        [support::normalized(&batch, &recipe)]
    );
}

#[test]
fn installed_recipe_requires_no_schema_and_empty_inputs_emit_no_batch() {
    let batch = wide_batch();
    let empty = batch.slice(0, 0);
    let (first, totals, _) = encode(&empty, CowSelectionTotals::default(), 1 << 20);
    assert!(first.is_empty());
    assert_eq!(totals.batches(), 0);
    let (second, totals, _) = encode(&batch, totals, 1 << 20);
    let records = split(&second);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0.kind(), CowSelectionRecordKind::Batch);
    let (third, totals, _) = encode(&empty, totals, 1 << 20);
    assert!(third.is_empty());
    assert_eq!(totals.batches(), 1);
    let mut stream = first;
    stream.extend_from_slice(&second);
    let recipe = support::recipe(batch.schema_ref());
    assert_eq!(
        decode(&stream, &recipe).1,
        [support::normalized(&batch, &recipe)]
    );
}

#[test]
fn schema_change_map_batch_count_and_scratch_are_refused_at_construction() {
    let batch = wide_batch();
    let (_, totals, _) = encode(&batch, CowSelectionTotals::default(), 1 << 20);
    let recipe = support::recipe(batch.schema_ref());
    let batch = support::normalized(&batch, &recipe);
    let other = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "_file",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["x"]))],
    )
    .unwrap();
    assert!(matches!(
        CowSelectionEncoder::try_new(&other, &recipe, totals.clone(), UNLIMITED),
        Err(CowSelectionCodecError::SchemaChanged)
    ));
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        false,
    ));
    let DataType::Struct(entry_fields) = entries.data_type().clone() else {
        unreachable!()
    };
    let map = MapArray::new(
        entries,
        OffsetBuffer::from_lengths([0]),
        StructArray::new(
            entry_fields,
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
                Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            ],
            None,
        ),
        None,
        false,
    );
    let with_map = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "m",
            map.data_type().clone(),
            true,
        )])),
        vec![Arc::new(map)],
    )
    .unwrap();
    assert!(matches!(
        CowSelectionRecipe::validate_supported(with_map.schema_ref()),
        Err(CowSelectionCodecError::UnsupportedType)
    ));
    let needed = CowSelectionEncoder::scratch_bytes(&batch, &recipe).unwrap();
    assert!(matches!(
        CowSelectionEncoder::try_new(&batch, &recipe, CowSelectionTotals::default(), needed - 1),
        Err(CowSelectionCodecError::ScratchLimit)
    ));
    assert!(
        CowSelectionEncoder::try_new(&batch, &recipe, CowSelectionTotals::default(), needed)
            .is_ok()
    );
    let mut totals = totals;
    let one = batch.slice(0, 1);
    for _ in 1..MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES {
        totals = encode(&one, totals, 1 << 20).1;
    }
    assert_eq!(
        totals.batches(),
        MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES
    );
    assert!(matches!(
        CowSelectionEncoder::try_new(&one, &recipe, totals.clone(), UNLIMITED),
        Err(CowSelectionCodecError::BatchLimit)
    ));
    // An empty input still passes once the batch count is reached.
    assert!(CowSelectionEncoder::try_new(&batch.slice(0, 0), &recipe, totals, UNLIMITED).is_ok());
}

#[test]
fn forged_headers_and_payloads_are_refused() {
    let batch = wide_batch();
    let (bytes, ..) = encode(&batch, CowSelectionTotals::default(), 1 << 20);
    let records = split(&bytes);
    let batch_record = records[0].1.to_vec();
    let recipe = support::recipe(batch.schema_ref());
    let forged = |record: &[u8], at: usize, value: u8| {
        let mut copy = record.to_vec();
        copy[at] = value;
        copy
    };
    assert_eq!(
        CowSelectionRecordHeader::parse(&forged(&batch_record, 0, b'X')),
        Err(CowSelectionCodecError::HeaderVersion)
    );
    assert_eq!(
        CowSelectionRecordHeader::parse(&forged(&batch_record, 4, 9)),
        Err(CowSelectionCodecError::HeaderVersion)
    );
    assert_eq!(
        CowSelectionRecordHeader::parse(&batch_record[..COW_SELECTION_HEADER_BYTES - 1]),
        Err(CowSelectionCodecError::TruncatedHeader)
    );
    assert_eq!(
        CowSelectionDecoder::decode_batch(&recipe, &batch_record[..batch_record.len() - 1]),
        Err(CowSelectionCodecError::TruncatedRecord)
    );
    // A batch record decoded under a different schema is refused.
    let narrow = Arc::new(Schema::new(vec![Field::new(
        "_file",
        DataType::Utf8,
        false,
    )]));
    let narrow = support::recipe(&narrow);
    assert_eq!(
        CowSelectionDecoder::decode_batch(&narrow, &batch_record),
        Err(CowSelectionCodecError::RecipeMismatch)
    );
    assert!(CowSelectionDecoder::decode_batch(&recipe, &batch_record).is_ok());
    // Legacy Schema records are not a decoding fallback.
    let mut legacy = batch_record.clone();
    legacy[4] = 1;
    assert_eq!(
        CowSelectionRecordHeader::parse(&legacy),
        Err(CowSelectionCodecError::HeaderVersion)
    );
}

#[test]
fn rebased_string_offsets_past_their_data_are_refused() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["ab", "c"]))],
    )
    .unwrap();
    let (bytes, ..) = encode(&batch, CowSelectionTotals::default(), 1 << 20);
    let records = split(&bytes);
    let recipe = support::recipe(batch.schema_ref());
    let mut record = records[0].1.to_vec();
    // Directory: one node length, three buffer sizes; then validity (0
    // bytes), offsets (12 bytes), data (3 bytes). Corrupt the last offset.
    let offsets = COW_SELECTION_HEADER_BYTES + 8 * 4;
    record[offsets + 8] = 9;
    assert_eq!(
        CowSelectionDecoder::decode_batch(&recipe, &record),
        Err(CowSelectionCodecError::MalformedBatch)
    );
}

#[test]
fn stream_decoder_uses_installed_recipe_for_every_independent_batch() {
    let batch = wide_batch();
    let (first, totals, _) = encode(&batch.slice(0, 4), CowSelectionTotals::default(), 1 << 20);
    let (second, _, _) = encode(&batch.slice(4, 7), totals, 1 << 20);
    let mut stream = first.clone();
    stream.extend_from_slice(&second);
    let recipe = support::recipe(batch.schema_ref());
    let mut decoder = CowSelectionStreamDecoder::new(Arc::clone(&recipe));
    let mut batches = Vec::new();
    for (_, record) in split(&stream) {
        batches.extend(decoder.apply_record(record).unwrap());
    }
    let schema = decoder.finish();
    assert_eq!(&schema, recipe.schema());
    assert_eq!(
        batches,
        vec![
            support::normalized(&batch.slice(0, 4), &recipe),
            support::normalized(&batch.slice(4, 7), &recipe)
        ]
    );
    // A complete first record is independently decodable; no prior data may
    // establish or change its declaration.
    let records = split(&stream);
    let mut first = CowSelectionStreamDecoder::new(Arc::clone(&recipe));
    assert!(first.apply_record(records[0].1).unwrap().is_some());
    let mut wrong = records[0].1.to_vec();
    wrong[32] ^= 1;
    assert!(matches!(
        first.apply_record(&wrong),
        Err(CowSelectionCodecError::RecipeMismatch)
    ));
    assert_eq!(
        CowSelectionStreamDecoder::new(Arc::clone(&recipe)).finish(),
        *recipe.schema()
    );
}
