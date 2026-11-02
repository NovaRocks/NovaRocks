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
use crate::{
    ipc_flat_batch_v2::FlatBatchProjectionLimits,
    ipc_flat_stream_v2::{
        FlatReaderProjectionLimits, FlatStreamProjectionLimits, preflight_flat_constant_stream,
    },
};
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, TimeUnit},
    ipc::{
        self,
        reader::StreamReader,
        writer::{IpcWriteOptions, StreamWriter},
    },
    record_batch::RecordBatch,
};
use arrow_buffer::{
    BooleanBuffer, Buffer, IntervalDayTime, IntervalMonthDayNano, NullBuffer, ScalarBuffer, i256,
};
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, NR_LOGICAL_TYPE_KEY, PureCompileControl,
    ValueLogicalType,
};
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, Mutex},
};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct Control {
    phase: CompilePhase,
    refusal: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl Control {
    fn good(phase: CompilePhase) -> Self {
        Self {
            phase,
            refusal: None,
            trace: Mutex::new(Vec::new()),
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            phase: CompilePhase::Encode,
            refusal: Some((at, cause)),
            trace: Mutex::new(Vec::new()),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, self.phase);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((refusal, _)) = self.refusal {
            assert!(at <= refusal, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((refusal, cause)) if at == refusal => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 64,
        max_logical_elements: 16384,
        max_retained_buffer_bytes: 4 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 64,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 16 * 1024 * 1024,
        max_library_validation_bytes: 16 * 1024 * 1024,
    }
}
fn schema_limits() -> IpcSchemaProjectionLimits {
    IpcSchemaProjectionLimits {
        max_field_occurrences: 64,
        max_type_occurrences: 64,
        max_string_bytes: 65536,
        max_flatbuffer_bytes: 1024 * 1024,
    }
}
fn limits() -> FlatPoolWriteLimits {
    FlatPoolWriteLimits {
        max_rows: 4096,
        max_buffer_descriptors: 4096,
        max_body_bytes: 4 * 1024 * 1024,
        max_encoded_stream_bytes: 8 * 1024 * 1024,
        schema: schema_limits(),
        max_new_allocation_request_bytes: 128 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
        max_cumulative_library_work: 512 * 1024 * 1024,
    }
}
fn stream_limits() -> FlatStreamProjectionLimits {
    FlatStreamProjectionLimits {
        max_input_bytes: 8 * 1024 * 1024,
        schema: schema_limits(),
        batch: FlatBatchProjectionLimits {
            max_metadata_bytes: 1024 * 1024,
            max_body_bytes: 4 * 1024 * 1024,
            max_rows: 4096,
            max_buffer_descriptors: 4096,
            max_view_validation_bytes: 4 * 1024 * 1024,
        },
    }
}
fn verifier() -> VerifierOptions {
    VerifierOptions {
        max_depth: 67,
        max_tables: 4096,
        max_apparent_size: 16 * 1024 * 1024,
        ignore_missing_null_terminator: false,
    }
}
fn pool(array: ArrayRef) -> ConstantPool {
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let field = Arc::new(Field::new(
        "original",
        ty.data_type().clone(),
        ty.nullable(),
    ));
    pool_with(array, field, ty)
}
fn pool_with(array: ArrayRef, field: Arc<Field>, ty: FunctionValueType) -> ConstantPool {
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Decode,
        &Control::good(CompilePhase::Decode),
    )
    .unwrap()
}
fn invoice(pool: &ConstantPool) -> usize {
    // Explicit conservative fixture retention, not a global allocator or MEM receipt.
    32768
        + pool.resource_facts().retained_buffer_capacity_bytes as usize
        + novarocks_type_contract::owned_resources::arrow_metadata::retained_backing_upper_bound(
            pool.field().metadata().len(),
        )
        .unwrap()
        + pool
            .field()
            .metadata()
            .iter()
            .map(|(k, v)| k.capacity() + v.capacity())
            .sum::<usize>()
}
fn standard(pool: &ConstantPool) -> Vec<u8> {
    let schema = Arc::new(Schema::new([Arc::clone(pool.field_ref())]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(pool.array())]).unwrap();
    let options = IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5).unwrap();
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    bytes
}
fn batch_parts(bytes: &[u8]) -> (ipc::RecordBatch<'_>, &[u8]) {
    assert_eq!(&bytes[..4], &[255; 4]);
    let schema_end = 8 + u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let body_start = schema_end
        + 8
        + u32::from_le_bytes(bytes[schema_end + 4..schema_end + 8].try_into().unwrap()) as usize;
    let message = ipc::root_as_message(&bytes[schema_end + 8..body_start]).unwrap();
    let end = body_start + message.bodyLength() as usize;
    assert_eq!(&bytes[end..], &[255, 255, 255, 255, 0, 0, 0, 0]);
    (
        message.header_as_record_batch().unwrap(),
        &bytes[body_start..end],
    )
}
fn buffer<'a>(body: &'a [u8], descriptor: &ipc::Buffer) -> &'a [u8] {
    &body[descriptor.offset() as usize..(descriptor.offset() + descriptor.length()) as usize]
}
fn compare_standard_batch(output: &[u8], standard: &[u8], ty: &DataType) {
    let (actual, actual_body) = batch_parts(output);
    let (expected, expected_body) = batch_parts(standard);
    assert_eq!(actual.length(), expected.length());
    let a = actual.nodes().unwrap();
    let e = expected.nodes().unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(e.len(), 1);
    assert_eq!(a.get(0).length(), e.get(0).length());
    assert_eq!(a.get(0).null_count(), e.get(0).null_count());
    let a = actual.buffers().unwrap();
    let e = expected.buffers().unwrap();
    assert_eq!(a.len(), e.len());
    for index in 0..a.len() {
        let actual = buffer(actual_body, a.get(index));
        let expected = buffer(expected_body, e.get(index));
        assert_eq!(actual.len(), expected.len());
        if index == 0 || (ty == &DataType::Boolean && index == 1) {
            // Bitmap trailing bits/padding do not represent selected rows.
            for row in 0..actual_rows(output) {
                assert_eq!(
                    (actual[row / 8] >> (row % 8)) & 1,
                    (expected[row / 8] >> (row % 8)) & 1
                );
            }
        } else {
            assert_eq!(actual, expected);
        }
    }
}
fn actual_rows(output: &[u8]) -> usize {
    batch_parts(output).0.length() as usize
}
fn encoded(pool: &ConstantPool) -> Vec<u8> {
    encode_flat_pool(
        pool,
        invoice(pool),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap()
}
fn materialize(bytes: &[u8], original: &ConstantPool) -> ConstantPool {
    let stream = preflight_flat_constant_stream(
        bytes,
        original.field(),
        stream_limits(),
        &verifier(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    let result = stream
        .materialize_pool(
            Arc::clone(original.field_ref()),
            original.value_type().clone(),
            bytes.len() + invoice(original),
            policy(),
            FlatReaderProjectionLimits {
                max_new_allocation_request_bytes: 128 * 1024 * 1024,
                max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
                max_cumulative_library_work: 512 * 1024 * 1024,
            },
            &Control::good(CompilePhase::Decode),
        )
        .unwrap();
    assert!(Arc::ptr_eq(result.field_ref(), original.field_ref()));
    assert_eq!(result.value_type(), original.value_type());
    result
}
fn standard_read(bytes: &[u8]) -> RecordBatch {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    batch
}
fn assert_prefixes(
    pool: &ConstantPool,
    retention: usize,
    bound: FlatPoolWriteLimits,
    trace: &[(CompilePhase, u32)],
    positions: impl IntoIterator<Item = usize>,
) {
    for at in positions {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(matches!(encode_flat_pool(pool, retention, bound, &control),
                Err(TypeCodecError::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn actual_flat_writer_fixed_bits_and_interval_match_independent_standard_reader() {
    let f32_bits = [0x80000000, 0x7fc01234];
    let f64_bits = [0x8000000000000000, 0x7ff8000000001234];
    let mut wide = [0u8; 32];
    wide[6] = 1; // 2^200, exact legal Decimal256 raw value.
    let mdn = IntervalMonthDayNano::new(i32::MIN, i32::MAX, i64::MIN);
    let half = make_array(
        arrow::array::ArrayData::builder(DataType::Float16)
            .len(3)
            .add_buffer(Buffer::from_slice_ref([0x8000u16, 0x7e15, 0]))
            .nulls(Some(NullBuffer::from(vec![true, true, false])))
            .build()
            .unwrap(),
    );
    let arrays: Vec<ArrayRef> = vec![
        half,
        Arc::new(Date32Array::from(vec![Some(-1), Some(1), None])),
        Arc::new(Date64Array::from(vec![
            Some(-86400000),
            Some(86400000),
            None,
        ])),
        Arc::new(Time32SecondArray::from(vec![Some(0), Some(86399), None])),
        Arc::new(Time32MillisecondArray::from(vec![
            Some(0),
            Some(86399999),
            None,
        ])),
        Arc::new(Time64MicrosecondArray::from(vec![
            Some(0),
            Some(86399999999),
            None,
        ])),
        Arc::new(Time64NanosecondArray::from(vec![
            Some(0),
            Some(86399999999999),
            None,
        ])),
        Arc::new(DurationSecondArray::from(vec![
            Some(i64::MIN),
            Some(i64::MAX),
            None,
        ])),
        Arc::new(DurationMillisecondArray::from(vec![
            Some(-1),
            Some(1),
            None,
        ])),
        Arc::new(DurationMicrosecondArray::from(vec![
            Some(-1),
            Some(1),
            None,
        ])),
        Arc::new(DurationNanosecondArray::from(vec![Some(-1), Some(1), None])),
        Arc::new(TimestampSecondArray::from(vec![Some(-1), Some(1), None])),
        Arc::new(TimestampMillisecondArray::from(vec![
            Some(-1),
            Some(1),
            None,
        ])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-1),
            Some(1),
            None,
        ])),
        Arc::new(TimestampNanosecondArray::from(vec![
            Some(-1),
            Some(1),
            None,
        ])),
        Arc::new(IntervalYearMonthArray::from(vec![
            Some(i32::MIN),
            Some(i32::MAX),
            None,
        ])),
        Arc::new(IntervalDayTimeArray::from(vec![
            Some(IntervalDayTime::new(i32::MIN, i32::MAX)),
            Some(IntervalDayTime::new(i32::MAX, i32::MIN)),
            None,
        ])),
        Arc::new(
            Decimal32Array::from(vec![Some(-123), Some(999), None])
                .with_precision_and_scale(3, -128)
                .unwrap(),
        ),
        Arc::new(
            Decimal64Array::from(vec![Some(-123), Some(999), None])
                .with_precision_and_scale(3, -128)
                .unwrap(),
        ),
        Arc::new(Int8Array::from(vec![Some(i8::MIN), Some(i8::MAX), None])),
        Arc::new(Int16Array::from(vec![Some(i16::MIN), Some(i16::MAX), None])),
        Arc::new(Int32Array::from(vec![Some(i32::MIN), Some(i32::MAX), None])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
        Arc::new(UInt8Array::from(vec![Some(0), Some(u8::MAX), None])),
        Arc::new(UInt16Array::from(vec![Some(0), Some(u16::MAX), None])),
        Arc::new(UInt32Array::from(vec![Some(0), Some(u32::MAX), None])),
        Arc::new(UInt64Array::from(vec![Some(0), Some(u64::MAX), None])),
        Arc::new(Float32Array::from(vec![
            Some(f32::from_bits(f32_bits[0])),
            Some(f32::from_bits(f32_bits[1])),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(f64::from_bits(f64_bits[0])),
            Some(f64::from_bits(f64_bits[1])),
            None,
        ])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(mdn),
            None,
            Some(IntervalMonthDayNano::new(-7, 8, 9)),
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(-12345), Some(99999), None])
                .with_precision_and_scale(5, -2)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_be_bytes(wide)),
                Some(i256::from_i128(-1)),
                None,
            ])
            .with_precision_and_scale(76, -2)
            .unwrap(),
        ),
    ];
    let mut arrays = arrays;
    for width in [0, 3, 16] {
        let values: Vec<u8> = (0..3 * width).map(|n| (n * 7) as u8).collect();
        arrays.push(make_array(
            arrow::array::ArrayData::builder(DataType::FixedSizeBinary(width))
                .len(3)
                .add_buffer(Buffer::from(values))
                .nulls(Some(NullBuffer::from(vec![true, true, false])))
                .build()
                .unwrap(),
        ));
    }
    for array in arrays {
        let original = pool(array);
        let bytes = encoded(&original);
        compare_standard_batch(&bytes, &standard(&original), original.array().data_type());
        let batch = standard_read(&bytes);
        let decoded = materialize(&bytes, &original);
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.column(0).logical_null_count(), 1);
        assert_eq!(decoded.array().to_data(), original.data().clone());
        if let Some(a) = batch.column(0).as_any().downcast_ref::<Float32Array>() {
            assert_eq!([a.value(0).to_bits(), a.value(1).to_bits()], f32_bits);
        }
        if let Some(a) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
            assert_eq!([a.value(0).to_bits(), a.value(1).to_bits()], f64_bits);
        }
        if let Some(a) = batch
            .column(0)
            .as_any()
            .downcast_ref::<IntervalDayTimeArray>()
        {
            assert_eq!(
                (a.value(0).days, a.value(0).milliseconds),
                (i32::MIN, i32::MAX)
            );
            assert_eq!(
                (a.value(1).days, a.value(1).milliseconds),
                (i32::MAX, i32::MIN)
            );
        }
        if let Some(a) = batch
            .column(0)
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
        {
            assert_eq!(
                (a.value(0).months, a.value(0).days, a.value(0).nanoseconds),
                (i32::MIN, i32::MAX, i64::MIN)
            );
        }
        if let Some(a) = batch.column(0).as_any().downcast_ref::<Decimal256Array>() {
            assert_eq!(a.value(0).to_be_bytes(), wide);
            assert_eq!(a.value(1), i256::from_i128(-1));
        }
        if let Some(a) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
            assert_eq!(a.value(1), u64::MAX);
        }
    }
}

#[test]
fn actual_flat_writer_day_time_slice_preserves_components_null_and_selected_ordinal() {
    // Arrow's native DayTime is a repr(C) pair of signed i32 components.
    // The source slice intentionally excludes both non-NULL sentinel rows.
    let source = IntervalDayTimeArray::from(vec![
        Some(IntervalDayTime::new(71, 72)),
        Some(IntervalDayTime::new(-7, -86400001)),
        Some(IntervalDayTime::new(i32::MIN, i32::MAX)),
        None,
        Some(IntervalDayTime::new(i32::MAX, i32::MIN)),
        Some(IntervalDayTime::new(73, 74)),
    ])
    .slice(1, 4);
    let field = Arc::new(
        Field::new("day_time_source", source.data_type().clone(), true).with_metadata(
            HashMap::from([("provider.field_id".into(), "2147483647".into())]),
        ),
    );
    let ty = FunctionValueType::new(source.data_type().clone(), true);
    let original = pool_with(Arc::new(source), Arc::clone(&field), ty.clone());
    let selected = original.value(1).unwrap();
    assert_eq!(selected.ordinal(), 1);
    assert_eq!(original.resource_facts().rows, 4);
    let source_invoice = invoice(&original);
    let facts = preflight_flat_pool_write(
        &original,
        source_invoice,
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert_eq!(facts.rows, 4);
    assert_eq!(facts.buffer_descriptors, 2);
    let mut exact = limits();
    exact.max_rows = facts.rows;
    exact.max_body_bytes = facts.body_bytes;
    let control = Control::good(CompilePhase::Encode);
    let bytes = encode_flat_pool(&original, source_invoice, exact, &control).unwrap();
    compare_standard_batch(&bytes, &standard(&original), field.data_type());
    let standard_batch = standard_read(&bytes);
    let decoded = materialize(&bytes, &original);
    assert_eq!(decoded.resource_facts().rows, 4);
    assert_eq!(decoded.value(1).unwrap().ordinal(), 1);
    assert_eq!(decoded.value_type(), &ty);
    assert!(Arc::ptr_eq(decoded.field_ref(), &field));
    assert_eq!(decoded.field().metadata(), field.metadata());
    let expected = [
        Some((-7, -86400001)),
        Some((i32::MIN, i32::MAX)),
        None,
        Some((i32::MAX, i32::MIN)),
    ];
    for array in [standard_batch.column(0), decoded.array(), original.array()] {
        let values = array
            .as_any()
            .downcast_ref::<IntervalDayTimeArray>()
            .unwrap();
        assert_eq!(values.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            let actual = (!values.is_null(row)).then(|| {
                let value = values.value(row);
                (value.days, value.milliseconds)
            });
            assert_eq!(actual, *expected);
        }
    }
    let trace = control.trace();
    assert_prefixes(&original, source_invoice, exact, &trace, 0..trace.len());
    exact.max_rows -= 1;
    let rejected = Control::good(CompilePhase::Encode);
    assert!(matches!(
        encode_flat_pool(&original, source_invoice, exact, &rejected),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let trace = rejected.trace();
    assert_prefixes(&original, source_invoice, exact, &trace, 0..trace.len());
}

#[test]
fn actual_flat_writer_sliced_offsets_and_hidden_null_boolean_bits_keep_ordinals() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(
            StringArray::from(vec![
                Some("unused-prefix"),
                None,
                Some("é😀"),
                Some("tail"),
                Some("unused-suffix"),
            ])
            .slice(1, 3),
        ),
        Arc::new(
            LargeStringArray::from(vec![
                Some("unused-prefix"),
                None,
                Some("é😀"),
                Some("tail"),
                Some("unused-suffix"),
            ])
            .slice(1, 3),
        ),
        Arc::new(
            BinaryArray::from(vec![
                Some(b"unused".as_slice()),
                None,
                Some(b"\0\xff".as_slice()),
                Some(b"tail".as_slice()),
                Some(b"suffix".as_slice()),
            ])
            .slice(1, 3),
        ),
        Arc::new(
            LargeBinaryArray::from(vec![
                Some(b"unused".as_slice()),
                None,
                Some(b"\0\xff".as_slice()),
                Some(b"tail".as_slice()),
                Some(b"suffix".as_slice()),
            ])
            .slice(1, 3),
        ),
    ];
    for array in arrays {
        let original = pool(array);
        let bytes = encoded(&original);
        compare_standard_batch(&bytes, &standard(&original), original.array().data_type());
        let decoded = materialize(&bytes, &original);
        assert!(
            decoded
                .value(0)
                .unwrap()
                .is_null_observed(CompilePhase::Decode, &Control::good(CompilePhase::Decode))
                .unwrap()
        );
        assert_eq!(decoded.value(1).unwrap().ordinal(), 1);
        if matches!(
            original.array().data_type(),
            DataType::Utf8 | DataType::LargeUtf8
        ) {
            assert_eq!(decoded.value(1).unwrap().try_utf8().unwrap(), Some("é😀"));
        } else {
            assert_eq!(
                decoded.value(1).unwrap().try_binary().unwrap(),
                Some(b"\0\xff".as_slice())
            );
        }
        let (metadata, body) = batch_parts(&bytes);
        let offsets = buffer(body, metadata.buffers().unwrap().get(1));
        match original.array().data_type() {
            DataType::Utf8 | DataType::Binary => {
                assert_eq!(i32::from_le_bytes(offsets[..4].try_into().unwrap()), 0)
            }
            _ => assert_eq!(i64::from_le_bytes(offsets[..8].try_into().unwrap()), 0),
        }
        assert_eq!(original.array().len(), 3);
    }
    let original = pool(Arc::new(
        BooleanArray::new(
            BooleanBuffer::from(vec![true, true, false, true, false, true]),
            Some(NullBuffer::from(vec![true, false, true, true, false, true])),
        )
        .slice(1, 4),
    ));
    let bytes = encoded(&original);
    compare_standard_batch(&bytes, &standard(&original), &DataType::Boolean);
    let decoded = materialize(&bytes, &original);
    let a = decoded
        .array()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        a.iter().collect::<Vec<_>>(),
        vec![None, Some(false), Some(true), None]
    );
}

#[test]
fn actual_flat_writer_exact_timezone_and_nominal_metadata_use_original_source() {
    for zone in [None, Some(""), Some("UTC")] {
        let a = TimestampNanosecondArray::from(vec![Some(-71), None, Some(99)]);
        let a = match zone {
            Some(z) => a.with_timezone(z),
            None => a,
        };
        let original = pool(Arc::new(a));
        let bytes = encoded(&original);
        let batch = standard_read(&bytes);
        assert_eq!(
            batch.column(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, zone.map(Into::into))
        );
        let decoded = materialize(&bytes, &original);
        assert_eq!(
            decoded.value(0).unwrap().try_timestamp().unwrap(),
            Some(-71)
        );
        if zone == Some("") {
            assert_eq!(
                standard_read(&standard(&original)).column(0).data_type(),
                &DataType::Timestamp(TimeUnit::Nanosecond, None)
            );
        }
    }
    for labelled in [false, true] {
        let ty =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let mut metadata = HashMap::from([
            ("provider-field-id".into(), "2147483647".into()),
            ("custom".into(), "unchanged".into()),
        ]);
        if labelled {
            metadata.insert(NR_LOGICAL_TYPE_KEY.into(), "json".into());
        }
        let field = Arc::new(Field::new("original", DataType::Utf8, true).with_metadata(metadata));
        let original = pool_with(
            Arc::new(StringArray::from(vec![Some("{\"v\":7}"), None])),
            field,
            ty,
        );
        let bytes = encoded(&original);
        let batch = standard_read(&bytes);
        assert_eq!(
            batch.schema().field(0).metadata(),
            original.field().metadata()
        );
        assert_eq!(batch.schema().field(0).name(), "original");
        let decoded = materialize(&bytes, &original);
        assert_eq!(decoded.value_type().logical_type(), ValueLogicalType::Json);
        assert_eq!(
            decoded.field().metadata().contains_key(NR_LOGICAL_TYPE_KEY),
            labelled
        );
        assert_eq!(
            decoded.value(0).unwrap().try_utf8().unwrap(),
            Some("{\"v\":7}")
        );
    }
}

fn long_view(index: u32, prefix: u8) -> u128 {
    let mut bytes = [prefix; 16];
    bytes[..4].copy_from_slice(&13u32.to_le_bytes());
    bytes[8..12].copy_from_slice(&index.to_le_bytes());
    bytes[12..].copy_from_slice(&0u32.to_le_bytes());
    u128::from_ne_bytes(bytes)
}
#[test]
fn actual_flat_writer_views_keep_all_backing_records_and_repeated_work() {
    let buffers = vec![
        Buffer::from(vec![b'a'; 13]),
        Buffer::from(vec![b'b'; 13]),
        Buffer::from(vec![b'x'; 79]),
        Buffer::from(vec![b'a'; 13]),
        Buffer::from(Vec::<u8>::new()),
    ];
    let records = ScalarBuffer::from(vec![
        long_view(0, b'a'),
        long_view(1, b'b'),
        long_view(0, b'a'),
        0,
        long_view(0, b'a'),
    ]);
    for array in [
        Arc::new(
            StringViewArray::new(
                records.clone(),
                buffers.clone(),
                Some(NullBuffer::from(vec![true, true, false, true, true])),
            )
            .slice(1, 4),
        ) as ArrayRef,
        Arc::new(
            BinaryViewArray::new(
                records.clone(),
                buffers.clone(),
                Some(NullBuffer::from(vec![true, true, false, true, true])),
            )
            .slice(1, 4),
        ) as ArrayRef,
    ] {
        let original = pool(array);
        let bytes = encoded(&original);
        compare_standard_batch(&bytes, &standard(&original), original.array().data_type());
        let (metadata, body) = batch_parts(&bytes);
        assert_eq!(metadata.variadicBufferCounts().unwrap().get(0), 5);
        let descriptors = metadata.buffers().unwrap();
        assert_eq!(descriptors.len(), 7);
        assert_eq!(descriptors.get(2 + 2).length(), 79);
        assert_eq!(descriptors.get(2 + 4).length(), 0);
        assert_eq!(buffer(body, descriptors.get(1)).len(), 64);
        let stream = preflight_flat_constant_stream(
            &bytes,
            original.field(),
            stream_limits(),
            &verifier(),
            &Control::good(CompilePhase::Decode),
        )
        .unwrap();
        assert_eq!(stream.geometry().view_validation_bytes, 39);
        let decoded = materialize(&bytes, &original);
        assert!(decoded.array().is_null(1));
        if let Some(a) = decoded.array().as_any().downcast_ref::<StringViewArray>() {
            assert_eq!(
                (a.value(0), a.value(2), a.value(3)),
                ("bbbbbbbbbbbbb", "", "aaaaaaaaaaaaa")
            );
        } else {
            let a = decoded
                .array()
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap();
            assert_eq!(a.value(0), b"bbbbbbbbbbbbb");
            assert_eq!(a.value(2), b"");
            assert_eq!(a.value(3), b"aaaaaaaaaaaaa");
        }
    }
}

#[test]
fn actual_flat_writer_compact_null_empty_offsets_and_unused_view_headers_are_real() {
    for rows in [0, 321] {
        let original = pool(Arc::new(NullArray::new(rows)));
        let bytes = encoded(&original);
        let (batch, body) = batch_parts(&bytes);
        assert_eq!(batch.nodes().unwrap().get(0).null_count(), rows as i64);
        assert!(body.is_empty());
        assert!(batch.buffers().unwrap().is_empty());
        let decoded = materialize(&bytes, &original);
        assert_eq!(decoded.array().len(), rows);
        assert_eq!(decoded.array().logical_null_count(), rows);
        if rows == 0 {
            assert!(decoded.value(0).is_err());
        }
    }
    for array in [
        Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        Arc::new(LargeStringArray::from(Vec::<&str>::new())) as ArrayRef,
    ] {
        let original = pool(array);
        let bytes = encoded(&original);
        let (batch, body) = batch_parts(&bytes);
        let expected = if original.array().data_type() == &DataType::Utf8 {
            4
        } else {
            8
        };
        assert_eq!(
            buffer(body, batch.buffers().unwrap().get(1)),
            vec![0; expected]
        );
        assert_eq!(materialize(&bytes, &original).array().len(), 0);
    }
    let original = pool(Arc::new(StringViewArray::new(
        ScalarBuffer::from(Vec::<u128>::new()),
        vec![Buffer::from(Vec::<u8>::new()); 32],
        None,
    )));
    let bytes = encoded(&original);
    let (batch, body) = batch_parts(&bytes);
    assert_eq!(batch.variadicBufferCounts().unwrap().get(0), 32);
    assert_eq!(batch.buffers().unwrap().len(), 34);
    assert!(body.is_empty());
    assert_eq!(materialize(&bytes, &original).array().len(), 0);
}

#[test]
fn actual_flat_writer_all_explicit_envelopes_have_exact_and_one_over_boundaries() {
    let original = pool(Arc::new(StringArray::from(vec![
        Some("prefix"),
        None,
        Some("last"),
    ])));
    let source = invoice(&original);
    let c = Control::good(CompilePhase::Encode);
    let facts = preflight_flat_pool_write(&original, source, limits(), &c).unwrap();
    let mut exact = limits();
    exact.max_rows = facts.rows;
    exact.max_buffer_descriptors = facts.buffer_descriptors;
    exact.max_body_bytes = facts.body_bytes;
    exact.max_encoded_stream_bytes = facts.encoded_stream_bytes_upper_bound;
    exact.max_new_allocation_request_bytes = facts.new_allocation_request_bytes_upper_bound;
    exact.max_coexisting_source_and_request_bytes =
        facts.coexisting_source_and_request_bytes_upper_bound;
    exact.max_cumulative_library_work = facts.cumulative_library_work_upper_bound;
    let bytes = encode_flat_pool(
        &original,
        source,
        exact,
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert!(bytes.len() <= facts.encoded_stream_bytes_upper_bound);
    assert_eq!(batch_parts(&bytes).1.len(), facts.body_bytes);
    let mut too_small = Vec::new();
    let mut x = exact;
    x.max_rows -= 1;
    too_small.push(x);
    let mut x = exact;
    x.max_buffer_descriptors -= 1;
    too_small.push(x);
    let mut x = exact;
    x.max_body_bytes -= 1;
    too_small.push(x);
    let mut x = exact;
    x.max_encoded_stream_bytes -= 1;
    too_small.push(x);
    let mut x = exact;
    x.max_new_allocation_request_bytes -= 1;
    too_small.push(x);
    let mut x = exact;
    x.max_coexisting_source_and_request_bytes -= 1;
    too_small.push(x);
    let mut x = exact;
    x.max_cumulative_library_work -= 1;
    too_small.push(x);
    for bound in too_small {
        let c = Control::good(CompilePhase::Encode);
        assert!(matches!(
            encode_flat_pool(&original, source, bound, &c),
            Err(TypeCodecError::InvalidShape(_))
        ));
        let trace = c.trace();
        assert!(!trace.is_empty());
        assert_prefixes(&original, source, bound, &trace, 0..trace.len());
    }
    assert!(matches!(
        preflight_flat_pool_write(&original, 0, limits(), &Control::good(CompilePhase::Encode)),
        Err(TypeCodecError::InvalidShape(_))
    ));
}

#[test]
fn actual_flat_writer_converted_metadata_keeps_source_invoice_independent_of_staging() {
    let mut metadata = HashMap::with_capacity(4096);
    for i in 0..4096 {
        metadata.insert(format!("key-{i}"), String::from("value"));
    }
    assert!(metadata.capacity() >= 4096);
    metadata.retain(|k, _| k == "key-0");
    assert_eq!(metadata.len(), 1);
    let field = Arc::new(Field::new("original", DataType::Int64, true).with_metadata(metadata));
    let original = pool_with(
        Arc::new(Int64Array::from(vec![71])),
        field,
        FunctionValueType::new(DataType::Int64, true),
    );
    // Arrow moved the surviving entry into a BTree/Arc; the staging table
    // has exited. The fixture invoice covers only the attached source.
    assert!(original.field().metadata().as_arc().is_some());
    let shared = original.field().metadata().clone();
    assert!(Arc::ptr_eq(
        original.field().metadata().as_arc().unwrap(),
        shared.as_arc().unwrap(),
    ));
    assert_eq!(shared.get("key-0").map(String::as_str), Some("value"));
    let retained = invoice(&original);
    let lower = preflight_flat_pool_write(
        &original,
        retained,
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let extra = 65536;
    let upper = preflight_flat_pool_write(
        &original,
        retained + extra,
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert_eq!(
        upper.source_retained_bytes - lower.source_retained_bytes,
        extra
    );
    assert_eq!(
        upper.coexisting_source_and_request_bytes_upper_bound
            - lower.coexisting_source_and_request_bytes_upper_bound,
        extra
    );
    assert_eq!(
        upper.new_allocation_request_bytes_upper_bound,
        lower.new_allocation_request_bytes_upper_bound
    );
    assert!(upper.cumulative_library_work_upper_bound > lower.cumulative_library_work_upper_bound);
    let mut bound = limits();
    bound.max_cumulative_library_work = lower.cumulative_library_work_upper_bound;
    let c = Control::good(CompilePhase::Encode);
    assert!(matches!(
        encode_flat_pool(&original, retained + extra, bound, &c),
        Err(TypeCodecError::InvalidShape(_))
    ));
    assert_prefixes(
        &original,
        retained + extra,
        bound,
        &c.trace(),
        0..c.trace().len(),
    );
    let bytes = encode_flat_pool(
        &original,
        retained + extra,
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert_eq!(
        materialize(&bytes, &original)
            .value(0)
            .unwrap()
            .try_i64()
            .unwrap(),
        Some(71)
    );
}

#[test]
fn actual_flat_writer_original_control_and_unsupported_tails_never_publish_partial_output() {
    let small = pool(Arc::new(Int64Array::from(vec![Some(7), None])));
    let c = Control::good(CompilePhase::Encode);
    encode_flat_pool(&small, invoice(&small), limits(), &c).unwrap();
    let trace = c.trace();
    assert_prefixes(&small, invoice(&small), limits(), &trace, 0..trace.len());
    let child = Arc::new(Field::new("item", DataType::Int64, true));
    let nested: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
        arrow::datatypes::Int64Type,
        _,
        _,
    >(vec![Some(vec![Some(1i64), None]), None]));
    assert_eq!(nested.data_type(), &DataType::List(child));
    let unsupported = pool(nested);
    let c = Control::good(CompilePhase::Encode);
    assert!(matches!(
        encode_flat_pool(&unsupported, invoice(&unsupported), limits(), &c),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let trace = c.trace();
    assert_prefixes(
        &unsupported,
        invoice(&unsupported),
        limits(),
        &trace,
        0..trace.len(),
    );
    let wide_sources: [ArrayRef; 2] = [
        Arc::new(StringArray::from(vec!["a".repeat(2048); 320])),
        Arc::new(StringViewArray::new(
            ScalarBuffer::from(Vec::<u128>::new()),
            vec![Buffer::from(Vec::<u8>::new()); 320],
            None,
        )),
    ];
    for array in wide_sources {
        let wide = pool(array);
        let c = Control::good(CompilePhase::Encode);
        encode_flat_pool(&wide, invoice(&wide), limits(), &c).unwrap();
        let trace = c.trace();
        let mut positions = vec![0, trace.len() - 1];
        positions.extend(
            trace
                .iter()
                .enumerate()
                .filter_map(|(i, (_, units))| (*units == 256).then_some(i)),
        );
        assert!(
            positions.len() > 2,
            "actual source copy or buffer header work must reach original quantum"
        );
        positions.sort_unstable();
        positions.dedup();
        assert_prefixes(&wide, invoice(&wide), limits(), &trace, positions);
    }
}

#[path = "prepared_tests.rs"]
mod prepared_tests;

#[path = "owned_tests.rs"]
mod owned_tests;

#[test]
fn pure_writer_without_source_invoice_uses_the_same_stream_body() {
    let pool = pool(Arc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some("λ"),
    ])));
    assert!(pool.source_retained_bytes().is_none());
    let control = Control::good(CompilePhase::Encode);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let pure = prepare_flat_pool_write_pure_in(&pool, &mut work)
        .unwrap()
        .emit_in(&mut work)
        .unwrap();
    work.finish().unwrap();
    let observed = encode_flat_pool(
        &pool,
        invoice(&pool),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert_eq!(pure, observed);
    let mut reader = StreamReader::try_new(Cursor::new(pure), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.column(0).as_ref(), pool.array().as_ref());
}

#[test]
fn pure_writer_preserves_each_original_control_refusal() {
    let pool = pool(Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])));
    let control = Control::good(CompilePhase::Encode);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    prepare_flat_pool_write_pure_in(&pool, &mut work)
        .unwrap()
        .emit_in(&mut work)
        .unwrap();
    work.finish().unwrap();
    for at in 0..control.trace().len() {
        for cause in CAUSES {
            let refusal = Control::refusing(at, cause.clone());
            let result = (|| {
                let mut work = CompileCheckpoints::try_new(&refusal, CompilePhase::Encode)?;
                let bytes =
                    prepare_flat_pool_write_pure_in(&pool, &mut work)?.emit_in(&mut work)?;
                work.finish()?;
                Ok::<_, TypeCodecError>(bytes)
            })();
            assert!(matches!(result, Err(TypeCodecError::Control(actual)) if actual == cause));
        }
    }
}

#[test]
fn pure_writer_refuses_nonempty_source_attributes_without_real_custody() {
    let array: ArrayRef = Arc::new(StringArray::from(vec!["text"]));
    let field = Arc::new(
        Field::new("source", DataType::Utf8, true).with_metadata(HashMap::from([(
            "provider.attribute".to_owned(),
            "actual source".to_owned(),
        )])),
    );
    let pool = pool_with(array, field, FunctionValueType::new(DataType::Utf8, true));
    let control = Control::good(CompilePhase::Encode);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    assert!(prepare_flat_pool_write_pure_in(&pool, &mut work).is_err());
    assert!(pool.source_owner().is_none());
}
