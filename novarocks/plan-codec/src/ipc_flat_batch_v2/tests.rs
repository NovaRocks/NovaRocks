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
use arrow::{
    array::*,
    datatypes::{Schema, TimeUnit},
    ipc::{
        self,
        writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    },
    record_batch::RecordBatch,
};
use arrow_buffer::{Buffer, IntervalDayTime, IntervalMonthDayNano, i256};
use flatbuffers::FlatBufferBuilder;
use novarocks_type_contract::CompileControlError;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            fail: None,
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            fail: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((refusal, _)) = self.fail {
            assert!(at <= refusal, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.fail.clone() {
            Some((refusal, cause)) if at == refusal => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> FlatBatchProjectionLimits {
    FlatBatchProjectionLimits {
        max_metadata_bytes: 1024 * 1024,
        max_body_bytes: 1024 * 1024,
        max_rows: 4096,
        max_buffer_descriptors: 4096,
        max_view_validation_bytes: 1024 * 1024,
    }
}
fn verifier() -> VerifierOptions {
    VerifierOptions {
        max_depth: 67,
        max_tables: 4096,
        max_apparent_size: 4 * 1024 * 1024,
        ignore_missing_null_terminator: false,
    }
}
fn preflight(
    meta: &[u8],
    body: &[u8],
    field: &Field,
    control: &Control,
) -> Result<FlatBatchGeometry, TypeCodecError> {
    preflight_flat_record_batch(meta, body, field, limits(), &verifier(), control)
}
fn prefixes(
    meta: &[u8],
    body: &[u8],
    field: &Field,
    bound: FlatBatchProjectionLimits,
    trace: &[(CompilePhase, u32)],
    positions: impl IntoIterator<Item = usize>,
) {
    for at in positions {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(preflight_flat_record_batch(meta, body, field, bound, &verifier(), &control),
                Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
fn ordinary(meta: &[u8], body: &[u8], field: &Field, bound: FlatBatchProjectionLimits) {
    let control = Control::good();
    assert!(matches!(
        preflight_flat_record_batch(meta, body, field, bound, &verifier(), &control),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let trace = control.trace();
    assert!(
        trace.len() >= 2,
        "ordinary malformed outcome omitted its tail"
    );
    prefixes(meta, body, field, bound, &trace, 0..trace.len());
}
#[allow(deprecated)]
fn encoded(array: ArrayRef) -> (Field, Vec<u8>, Vec<u8>) {
    let field = Field::new("source", array.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let (dictionaries, encoded) = IpcDataGenerator::default()
        .encoded_batch(
            &batch,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
        )
        .unwrap();
    assert!(dictionaries.is_empty());
    (field, encoded.ipc_message, encoded.arrow_data)
}
fn read(meta: &[u8], body: &[u8], field: &Field) -> RecordBatch {
    // Only callers with successful geometric preflight enter the actual reader.
    let message = ipc::root_as_message(meta).unwrap();
    ipc::reader::read_record_batch(
        &Buffer::from(body.to_vec()),
        message.header_as_record_batch().unwrap(),
        Arc::new(Schema::new(vec![field.clone()])),
        &HashMap::new(),
        None,
        &message.version(),
    )
    .unwrap()
}
#[derive(Clone)]
struct RawBatch {
    rows: i64,
    nodes: Option<Vec<ipc::FieldNode>>,
    buffers: Option<Vec<ipc::Buffer>>,
    variadic: Option<Vec<i64>>,
    body_length: i64,
    compression: bool,
    version: ipc::MetadataVersion,
    header: ipc::MessageHeader,
    metadata: bool,
}
impl RawBatch {
    fn fixed(rows: i64, width: i64) -> Self {
        Self {
            rows,
            nodes: Some(vec![ipc::FieldNode::new(rows, 0)]),
            buffers: Some(vec![
                ipc::Buffer::new(0, 0),
                ipc::Buffer::new(0, rows * width),
            ]),
            variadic: None,
            body_length: rows * width,
            compression: false,
            version: ipc::MetadataVersion::V5,
            header: ipc::MessageHeader::RecordBatch,
            metadata: false,
        }
    }
    fn encode(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        let nodes = self
            .nodes
            .as_ref()
            .map(|items| builder.create_vector(items));
        let buffers = self
            .buffers
            .as_ref()
            .map(|items| builder.create_vector(items));
        let variadic = self
            .variadic
            .as_ref()
            .map(|items| builder.create_vector(items));
        let compression = self.compression.then(|| {
            ipc::BodyCompression::create(
                &mut builder,
                &ipc::BodyCompressionArgs {
                    codec: ipc::CompressionType::LZ4_FRAME,
                    method: ipc::BodyCompressionMethod::BUFFER,
                },
            )
        });
        let batch = ipc::RecordBatch::create(
            &mut builder,
            &ipc::RecordBatchArgs {
                length: self.rows,
                nodes,
                buffers,
                compression,
                variadicBufferCounts: variadic,
            },
        );
        let custom_metadata = if self.metadata {
            let key = builder.create_string("unexpected");
            let value = builder.create_string("property");
            let pair = ipc::KeyValue::create(
                &mut builder,
                &ipc::KeyValueArgs {
                    key: Some(key),
                    value: Some(value),
                },
            );
            Some(builder.create_vector(&[pair]))
        } else {
            None
        };
        let message = ipc::Message::create(
            &mut builder,
            &ipc::MessageArgs {
                version: self.version,
                header_type: self.header,
                header: Some(batch.as_union_value()),
                bodyLength: self.body_length,
                custom_metadata,
            },
        );
        ipc::finish_message_buffer(&mut builder, message);
        builder.finished_data().to_vec()
    }
}

#[test]
fn actual_writer_flat_scalar_layouts_and_slices_have_checked_geometry() {
    let mut arrays: Vec<ArrayRef> = Vec::new();
    macro_rules! primitive {
        ($kind:ident, $a:expr, $b:expr) => {
            arrays.push(Arc::new($kind::from(vec![Some($a), None, Some($b)])));
        };
    }
    arrays.push(Arc::new(NullArray::new(3)));
    primitive!(BooleanArray, true, false);
    primitive!(Int8Array, i8::MIN, i8::MAX);
    primitive!(Int16Array, i16::MIN, i16::MAX);
    primitive!(Int32Array, i32::MIN, i32::MAX);
    primitive!(Int64Array, i64::MIN, i64::MAX);
    primitive!(UInt8Array, 0, u8::MAX);
    primitive!(UInt16Array, 0, u16::MAX);
    primitive!(UInt32Array, 0, u32::MAX);
    primitive!(UInt64Array, 0, u64::MAX);
    type F16 = <arrow::datatypes::Float16Type as arrow::datatypes::ArrowPrimitiveType>::Native;
    primitive!(Float16Array, F16::from_bits(0x8000), F16::from_bits(0x7e71));
    primitive!(
        Float32Array,
        f32::from_bits(0x80000000),
        f32::from_bits(0x7fc00071)
    );
    primitive!(
        Float64Array,
        f64::from_bits(0x8000000000000000),
        f64::from_bits(0x7ff8000000000071)
    );
    primitive!(Date32Array, -1, 1);
    primitive!(Date64Array, -86400000, 86400000);
    primitive!(Time32SecondArray, 0, 86399);
    primitive!(Time32MillisecondArray, 0, 86399000);
    primitive!(Time64MicrosecondArray, 0, 86399000000);
    primitive!(Time64NanosecondArray, 0, 86399000000000);
    primitive!(DurationSecondArray, -1, 1);
    primitive!(DurationMillisecondArray, -1, 1);
    primitive!(DurationMicrosecondArray, -1, 1);
    primitive!(DurationNanosecondArray, -1, 1);
    primitive!(TimestampSecondArray, -1, 1);
    primitive!(TimestampMillisecondArray, -1, 1);
    primitive!(TimestampMicrosecondArray, -1, 1);
    primitive!(TimestampNanosecondArray, -1, 1);
    arrays.push(Arc::new(
        TimestampMicrosecondArray::from(vec![Some(-1), None, Some(1)]).with_timezone(""),
    ));
    primitive!(IntervalYearMonthArray, -12, 13);
    primitive!(
        IntervalDayTimeArray,
        IntervalDayTime::new(-1, -5),
        IntervalDayTime::new(1, 5)
    );
    primitive!(
        IntervalMonthDayNanoArray,
        IntervalMonthDayNano::new(i32::MIN, 7, i64::MIN),
        IntervalMonthDayNano::new(i32::MAX, -9, i64::MAX)
    );
    arrays.push(Arc::new(
        Decimal32Array::from(vec![Some(-155), None, Some(155)])
            .with_precision_and_scale(9, -1)
            .unwrap(),
    ));
    arrays.push(Arc::new(
        Decimal64Array::from(vec![Some(-155), None, Some(155)])
            .with_precision_and_scale(18, 2)
            .unwrap(),
    ));
    arrays.push(Arc::new(
        Decimal128Array::from(vec![Some(-155), None, Some(155)])
            .with_precision_and_scale(38, -2)
            .unwrap(),
    ));
    arrays.push(Arc::new(
        Decimal256Array::from(vec![
            Some(-i256::from_i128(155)),
            None,
            Some(i256::from_i128(155)),
        ])
        .with_precision_and_scale(76, 38)
        .unwrap(),
    ));
    arrays.push(Arc::new(
        FixedSizeBinaryArray::try_from(vec![
            Some(b"abcd".as_slice()),
            None,
            Some(b"wxyz".as_slice()),
        ])
        .unwrap(),
    ));
    arrays.push(Arc::new(FixedSizeBinaryArray::new(
        0,
        Buffer::from(Vec::<u8>::new()),
        Some(arrow_buffer::NullBuffer::from(vec![true, false, true])),
    )));
    arrays.push(Arc::new(StringArray::from(vec![
        Some("a\0雪"),
        None,
        Some("🙂"),
    ])));
    arrays.push(Arc::new(LargeStringArray::from(vec![
        Some("a\0雪"),
        None,
        Some("🙂"),
    ])));
    arrays.push(Arc::new(BinaryArray::from(vec![
        Some(b"\0\xff".as_slice()),
        None,
        Some(b"abc".as_slice()),
    ])));
    arrays.push(Arc::new(LargeBinaryArray::from(vec![
        Some(b"\0\xff".as_slice()),
        None,
        Some(b"abc".as_slice()),
    ])));
    arrays.push(Arc::new(StringViewArray::from(vec![
        Some("abcdefghijklmnop"),
        None,
        Some("🙂"),
    ])));
    arrays.push(Arc::new(BinaryViewArray::from(vec![
        Some(b"abcdefghijklmnop".as_slice()),
        None,
        Some(b"\xff".as_slice()),
    ])));
    for original in arrays {
        for array in [original.clone(), original.slice(1, 2)] {
            let (field, meta, body) = encoded(array.clone());
            let control = Control::good();
            let geometry = preflight(&meta, &body, &field, &control).unwrap();
            assert_eq!(geometry.rows, array.len());
            // NullArray has no physical validity bitmap but every row is
            // logically null. Its IPC FieldNode correctly reports all rows.
            assert_eq!(geometry.null_count, array.logical_null_count());
            assert_eq!(geometry.body_bytes, body.len());
            let count = match field.data_type() {
                DataType::Null => 0,
                DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
                    3
                }
                DataType::Utf8View | DataType::BinaryView => 2 + geometry.variadic_buffers,
                _ => 2,
            };
            assert_eq!(geometry.buffer_descriptors, count);
            let decoded = read(&meta, &body, &field);
            assert_eq!(decoded.column(0).to_data(), array.to_data());
            let trace = control.trace();
            prefixes(&meta, &body, &field, limits(), &trace, 0..trace.len());
        }
    }
}

#[test]
fn actual_float_interval_decimal_payloads_and_empty_timezone_keep_source_identity() {
    let array: ArrayRef = Arc::new(Float32Array::from(vec![
        f32::from_bits(0x80000000),
        f32::from_bits(0x7fc00071),
    ]));
    let (field, meta, body) = encoded(array);
    preflight(&meta, &body, &field, &Control::good()).unwrap();
    let batch = read(&meta, &body, &field);
    let floats = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(floats.value(0).to_bits(), 0x80000000);
    assert_eq!(floats.value(1).to_bits(), 0x7fc00071);
    let interval = IntervalMonthDayNano::new(i32::MIN, i32::MAX, i64::MIN);
    let (field, meta, body) = encoded(Arc::new(IntervalMonthDayNanoArray::from(vec![interval])));
    preflight(&meta, &body, &field, &Control::good()).unwrap();
    let batch = read(&meta, &body, &field);
    let value = batch
        .column(0)
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .unwrap()
        .value(0);
    assert_eq!(
        (value.months, value.days, value.nanoseconds),
        (i32::MIN, i32::MAX, i64::MIN)
    );
    let decimal = i256::from_be_bytes([0x01; 32]);
    let (field, meta, body) = encoded(Arc::new(
        Decimal256Array::from(vec![decimal])
            .with_precision_and_scale(76, -2)
            .unwrap(),
    ));
    preflight(&meta, &body, &field, &Control::good()).unwrap();
    let batch = read(&meta, &body, &field);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap()
            .value(0)
            .to_be_bytes(),
        [0x01; 32]
    );
    let expected = Field::new(
        "source",
        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(""))),
        true,
    );
    let schema_limits = crate::ipc_schema_v2::IpcSchemaProjectionLimits {
        max_field_occurrences: 4,
        max_type_occurrences: 4,
        max_string_bytes: 128,
        max_flatbuffer_bytes: 4096,
    };
    let schema = crate::ipc_schema_v2::encode_single_field_schema(
        &expected,
        schema_limits,
        &Control::good(),
    )
    .unwrap();
    crate::ipc_schema_v2::verify_single_field_schema_message(
        &schema,
        &expected,
        schema_limits,
        &verifier(),
        &Control::good(),
    )
    .unwrap();
    let message = ipc::root_as_message(&schema).unwrap();
    let decoded = ipc::convert::try_fb_to_schema(message.header_as_schema().unwrap()).unwrap();
    assert_eq!(decoded.field(0).data_type(), expected.data_type());
}

#[test]
fn independent_signed_extents_nodes_descriptors_and_message_profiles_refuse() {
    let field = Field::new("source", DataType::Int64, true);
    let base = RawBatch::fixed(1, 8);
    let mut cases = Vec::new();
    macro_rules! altered {
        ($slot:ident, $value:expr) => {{
            let mut case = base.clone();
            case.$slot = $value;
            cases.push(case);
        }};
    }
    altered!(rows, -1);
    altered!(body_length, -1);
    altered!(body_length, i64::MAX);
    altered!(nodes, None);
    altered!(nodes, Some(vec![]));
    altered!(
        nodes,
        Some(vec![ipc::FieldNode::new(1, 0), ipc::FieldNode::new(1, 0)])
    );
    altered!(nodes, Some(vec![ipc::FieldNode::new(-1, 0)]));
    altered!(nodes, Some(vec![ipc::FieldNode::new(2, 0)]));
    altered!(nodes, Some(vec![ipc::FieldNode::new(1, -1)]));
    altered!(nodes, Some(vec![ipc::FieldNode::new(1, 2)]));
    altered!(buffers, None);
    altered!(buffers, Some(vec![]));
    altered!(buffers, Some(vec![ipc::Buffer::new(0, 0)]));
    altered!(
        buffers,
        Some(vec![
            ipc::Buffer::new(0, 0),
            ipc::Buffer::new(0, 8),
            ipc::Buffer::new(0, 0)
        ])
    );
    for descriptor in [
        ipc::Buffer::new(-1, 0),
        ipc::Buffer::new(0, -1),
        ipc::Buffer::new(1, 8),
        ipc::Buffer::new(i64::MAX, 1),
    ] {
        altered!(buffers, Some(vec![descriptor, ipc::Buffer::new(0, 8)]));
    }
    altered!(
        buffers,
        Some(vec![ipc::Buffer::new(0, 0), ipc::Buffer::new(0, 7)])
    );
    altered!(nodes, Some(vec![ipc::FieldNode::new(1, 1)])); // Required bitmap absent.
    altered!(variadic, Some(vec![0]));
    altered!(compression, true);
    altered!(version, ipc::MetadataVersion::V4);
    altered!(version, ipc::MetadataVersion(99));
    altered!(header, ipc::MessageHeader::NONE);
    altered!(metadata, true);
    for case in cases {
        ordinary(&case.encode(), &[0; 8], &field, limits());
    }
    for cutoff in [0, 3, base.encode().len() - 1] {
        let meta = base.encode();
        ordinary(&meta[..cutoff], &[0; 8], &field, limits());
    }
}

#[test]
fn exact_projection_envelopes_accept_near_and_refuse_one_over_with_typed_tails() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(-7)]));
    let (field, meta, body) = encoded(array);
    let control = Control::good();
    let exact = FlatBatchProjectionLimits {
        max_metadata_bytes: meta.len(),
        max_body_bytes: body.len(),
        max_rows: 3,
        max_buffer_descriptors: 2,
        max_view_validation_bytes: 0,
    };
    let geometry =
        preflight_flat_record_batch(&meta, &body, &field, exact, &verifier(), &control).unwrap();
    assert_eq!(
        (
            geometry.rows,
            geometry.null_count,
            geometry.buffer_descriptors
        ),
        (3, 1, 2)
    );
    let trace = control.trace();
    prefixes(&meta, &body, &field, exact, &trace, 0..trace.len());
    for bound in [
        FlatBatchProjectionLimits {
            max_metadata_bytes: meta.len() - 1,
            ..exact
        },
        FlatBatchProjectionLimits {
            max_body_bytes: body.len() - 1,
            ..exact
        },
        FlatBatchProjectionLimits {
            max_rows: 2,
            ..exact
        },
        FlatBatchProjectionLimits {
            max_buffer_descriptors: 1,
            ..exact
        },
    ] {
        ordinary(&meta, &body, &field, bound);
    }
}

#[test]
fn compact_null_huge_row_count_is_bounded_independently_of_zero_body() {
    let field = Field::new("source", DataType::Null, true);
    let rows = 1usize << 40;
    let raw = RawBatch {
        rows: rows as i64,
        nodes: Some(vec![ipc::FieldNode::new(rows as i64, rows as i64)]),
        buffers: Some(vec![]),
        body_length: 0,
        ..RawBatch::fixed(0, 0)
    };
    let meta = raw.encode();
    let exact = FlatBatchProjectionLimits {
        max_rows: rows,
        ..limits()
    };
    let control = Control::good();
    let geometry =
        preflight_flat_record_batch(&meta, &[], &field, exact, &verifier(), &control).unwrap();
    assert_eq!(
        (
            geometry.rows,
            geometry.null_count,
            geometry.body_bytes,
            geometry.buffer_descriptors
        ),
        (rows, rows, 0, 0)
    );
    let trace = control.trace();
    prefixes(&meta, &[], &field, exact, &trace, 0..trace.len());
    ordinary(
        &meta,
        &[],
        &field,
        FlatBatchProjectionLimits {
            max_rows: rows - 1,
            ..exact
        },
    );
    let mut invalid = raw;
    invalid.nodes = Some(vec![ipc::FieldNode::new(rows as i64, 0)]);
    ordinary(&invalid.encode(), &[], &field, exact);
}

#[test]
fn empty_offsets_and_many_empty_view_backings_are_valid_bounded_shapes() {
    for ty in [
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::LargeBinary,
    ] {
        let field = Field::new("source", ty.clone(), true);
        let width = if matches!(ty, DataType::Utf8 | DataType::Binary) {
            4
        } else {
            8
        };
        for offset_len in [0, width] {
            let raw = RawBatch {
                buffers: Some(vec![
                    ipc::Buffer::new(0, 0),
                    ipc::Buffer::new(0, offset_len),
                    ipc::Buffer::new(0, 0),
                ]),
                body_length: offset_len,
                ..RawBatch::fixed(0, 0)
            };
            let body = vec![0; offset_len as usize];
            let meta = raw.encode();
            let control = Control::good();
            let geometry = preflight(&meta, &body, &field, &control).unwrap();
            assert_eq!((geometry.rows, geometry.buffer_descriptors), (0, 3));
            assert_eq!(read(&meta, &body, &field).num_rows(), 0);
            let trace = control.trace();
            prefixes(&meta, &body, &field, limits(), &trace, 0..trace.len());
        }
        let short = RawBatch {
            buffers: Some(vec![
                ipc::Buffer::new(0, 0),
                ipc::Buffer::new(0, width - 1),
                ipc::Buffer::new(0, 0),
            ]),
            body_length: width - 1,
            ..RawBatch::fixed(0, 0)
        };
        ordinary(
            &short.encode(),
            &vec![0; (width - 1) as usize],
            &field,
            limits(),
        );
        let nonempty = RawBatch {
            rows: 1,
            nodes: Some(vec![ipc::FieldNode::new(1, 0)]),
            buffers: Some(vec![
                ipc::Buffer::new(0, 0),
                ipc::Buffer::new(0, width),
                ipc::Buffer::new(0, 0),
            ]),
            body_length: width,
            ..RawBatch::fixed(0, 0)
        };
        ordinary(
            &nonempty.encode(),
            &vec![0; width as usize],
            &field,
            limits(),
        );
    }
    for ty in [DataType::Utf8View, DataType::BinaryView] {
        let field = Field::new("source", ty, true);
        let raw = RawBatch {
            buffers: Some(vec![ipc::Buffer::new(0, 0); 7]),
            variadic: Some(vec![5]),
            ..RawBatch::fixed(0, 0)
        };
        let meta = raw.encode();
        let control = Control::good();
        let geometry = preflight(&meta, &[], &field, &control).unwrap();
        assert_eq!(
            (
                geometry.rows,
                geometry.variadic_buffers,
                geometry.buffer_descriptors
            ),
            (0, 5, 7)
        );
        let decoded = read(&meta, &[], &field);
        assert_eq!(decoded.num_rows(), 0);
        let trace = control.trace();
        prefixes(&meta, &[], &field, limits(), &trace, 0..trace.len());
        ordinary(
            &meta,
            &[],
            &field,
            FlatBatchProjectionLimits {
                max_buffer_descriptors: 6,
                ..limits()
            },
        );
    }
}

#[test]
fn view_variadic_shape_negative_counts_and_missing_records_refuse() {
    let field = Field::new("source", DataType::Utf8View, true);
    let base = RawBatch {
        buffers: Some(vec![ipc::Buffer::new(0, 0); 2]),
        variadic: Some(vec![0]),
        ..RawBatch::fixed(0, 0)
    };
    for counts in [
        None,
        Some(vec![]),
        Some(vec![-1]),
        Some(vec![i64::MAX]),
        Some(vec![0, 0]),
        Some(vec![1]),
    ] {
        let mut raw = base.clone();
        raw.variadic = counts;
        ordinary(&raw.encode(), &[], &field, limits());
    }
    let mut short = base;
    short.rows = 1;
    short.nodes = Some(vec![ipc::FieldNode::new(1, 0)]);
    ordinary(&short.encode(), &[], &field, limits());
}

fn external_view(length: u32, index: u32, offset: u32) -> [u8; 16] {
    let mut record = [b'a'; 16];
    record[..4].copy_from_slice(&length.to_le_bytes());
    record[8..12].copy_from_slice(&index.to_le_bytes());
    record[12..16].copy_from_slice(&offset.to_le_bytes());
    record
}
fn repeated_views(rows: usize, null: bool) -> (RawBatch, Vec<u8>) {
    let bitmap = if null { rows.div_ceil(8) } else { 0 };
    let mut body = vec![0; bitmap];
    for _ in 0..rows {
        body.extend_from_slice(&external_view(13, 0, 0));
    }
    let payload_start = body.len();
    body.extend_from_slice(b"aaaaaaaaaaaaa");
    (
        RawBatch {
            rows: rows as i64,
            nodes: Some(vec![ipc::FieldNode::new(
                rows as i64,
                if null { rows as i64 } else { 0 },
            )]),
            buffers: Some(vec![
                ipc::Buffer::new(0, bitmap as i64),
                ipc::Buffer::new(bitmap as i64, (rows * 16) as i64),
                ipc::Buffer::new(payload_start as i64, 13),
            ]),
            variadic: Some(vec![1]),
            body_length: body.len() as i64,
            ..RawBatch::fixed(0, 0)
        },
        body,
    )
}
#[test]
fn null_and_repeated_external_views_count_each_declared_byte_before_reading() {
    for ty in [DataType::Utf8View, DataType::BinaryView] {
        let field = Field::new("source", ty, true);
        for null in [false, true] {
            let (raw, body) = repeated_views(2, null);
            let meta = raw.encode();
            let exact = FlatBatchProjectionLimits {
                max_view_validation_bytes: 26,
                ..limits()
            };
            let control = Control::good();
            let geometry =
                preflight_flat_record_batch(&meta, &body, &field, exact, &verifier(), &control)
                    .unwrap();
            assert_eq!(geometry.view_validation_bytes, 26);
            assert_eq!(geometry.described_buffer_bytes, usize::from(null) + 32 + 13);
            assert_eq!(geometry.variadic_buffers, 1);
            let decoded = read(&meta, &body, &field);
            assert_eq!(decoded.column(0).null_count(), if null { 2 } else { 0 });
            let trace = control.trace();
            prefixes(&meta, &body, &field, exact, &trace, 0..trace.len());
            ordinary(
                &meta,
                &body,
                &field,
                FlatBatchProjectionLimits {
                    max_view_validation_bytes: 25,
                    ..exact
                },
            );
            for bad_record in [
                external_view(13, 1, 0),
                external_view(13, 0, 1),
                external_view(13, 0, u32::MAX),
            ] {
                let mut bad = body.clone();
                let at = usize::from(null);
                bad[at..at + 16].copy_from_slice(&bad_record);
                ordinary(&meta, &bad, &field, exact);
            }
        }
    }
}

#[test]
fn overlapping_unaligned_descriptors_remain_legal_and_real_reader_copies_if_needed() {
    let field = Field::new("source", DataType::Int64, true);
    let body = [99, 7, 0, 0, 0, 0, 0, 0, 0];
    let raw = RawBatch {
        buffers: Some(vec![ipc::Buffer::new(1, 1), ipc::Buffer::new(1, 8)]),
        body_length: 9,
        ..RawBatch::fixed(1, 8)
    };
    let meta = raw.encode();
    let control = Control::good();
    let geometry = preflight(&meta, &body, &field, &control).unwrap();
    assert_eq!(geometry.described_buffer_bytes, 9);
    assert_eq!(geometry.body_bytes, 9);
    let decoded = read(&meta, &body, &field);
    assert_eq!(
        decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    let trace = control.trace();
    prefixes(&meta, &body, &field, limits(), &trace, 0..trace.len());
}

#[test]
fn actual_300_view_records_reach_quantum_and_preserve_each_original_control_cause() {
    let field = Field::new("source", DataType::Utf8View, true);
    let (raw, body) = repeated_views(300, false);
    let meta = raw.encode();
    let control = Control::good();
    let geometry = preflight(&meta, &body, &field, &control).unwrap();
    assert_eq!(geometry.view_validation_bytes, 3900);
    assert_eq!(geometry.described_buffer_bytes, 4800 + 13);
    let decoded = read(&meta, &body, &field);
    assert_eq!(decoded.num_rows(), 300);
    let trace = control.trace();
    let mut positions: Vec<_> = trace
        .iter()
        .enumerate()
        .filter_map(|(at, (_, units))| (*units == 256).then_some(at))
        .collect();
    assert!(
        !positions.is_empty(),
        "real view traversal never reached its quantum"
    );
    positions.extend([0, trace.len() - 2, trace.len() - 1]);
    positions.sort_unstable();
    positions.dedup();
    prefixes(&meta, &body, &field, limits(), &trace, positions);
}

#[test]
fn complete_typed_offset_and_view_records_allow_extra_items_but_not_partial_suffixes() {
    let field = Field::new("source", DataType::Utf8, true);
    let mut body = Vec::new();
    for offset in [0i32, 1, 1] {
        body.extend_from_slice(&offset.to_le_bytes());
    }
    body.push(b'a');
    let raw = RawBatch {
        buffers: Some(vec![
            ipc::Buffer::new(0, 0),
            ipc::Buffer::new(0, 12),
            ipc::Buffer::new(12, 1),
        ]),
        body_length: 13,
        ..RawBatch::fixed(1, 0)
    };
    let meta = raw.encode();
    let control = Control::good();
    let geometry = preflight(&meta, &body, &field, &control).unwrap();
    assert_eq!(geometry.rows, 1);
    assert_eq!(geometry.described_buffer_bytes, 13);
    let decoded = read(&meta, &body, &field);
    assert_eq!(
        decoded
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
    let trace = control.trace();
    prefixes(&meta, &body, &field, limits(), &trace, 0..trace.len());
    let mut partial = raw.clone();
    partial.buffers = Some(vec![
        ipc::Buffer::new(0, 0),
        ipc::Buffer::new(0, 9),
        ipc::Buffer::new(12, 1),
    ]);
    ordinary(&partial.encode(), &body, &field, limits());
    let empty = RawBatch {
        nodes: Some(vec![ipc::FieldNode::new(0, 0)]),
        rows: 0,
        ..partial
    };
    ordinary(&empty.encode(), &body, &field, limits());

    for ty in [DataType::Utf8View, DataType::BinaryView] {
        let field = Field::new("source", ty, true);
        let mut body = external_view(13, 0, 0).to_vec();
        // A complete extra record is outside the actual node's one row. It must
        // neither inflate P nor be followed as a selected external reference.
        body.extend_from_slice(&external_view(u32::MAX, u32::MAX, u32::MAX));
        body.extend_from_slice(b"aaaaaaaaaaaaa");
        let raw = RawBatch {
            buffers: Some(vec![
                ipc::Buffer::new(0, 0),
                ipc::Buffer::new(0, 32),
                ipc::Buffer::new(32, 13),
            ]),
            variadic: Some(vec![1]),
            body_length: 45,
            ..RawBatch::fixed(1, 0)
        };
        let meta = raw.encode();
        let control = Control::good();
        let geometry = preflight(&meta, &body, &field, &control).unwrap();
        assert_eq!(geometry.view_validation_bytes, 13);
        assert_eq!(read(&meta, &body, &field).num_rows(), 1);
        let trace = control.trace();
        prefixes(&meta, &body, &field, limits(), &trace, 0..trace.len());
        let mut partial = raw;
        partial.buffers = Some(vec![
            ipc::Buffer::new(0, 0),
            ipc::Buffer::new(0, 17),
            ipc::Buffer::new(32, 13),
        ]);
        ordinary(&partial.encode(), &body, &field, limits());
        let empty = RawBatch {
            nodes: Some(vec![ipc::FieldNode::new(0, 0)]),
            rows: 0,
            ..partial
        };
        ordinary(&empty.encode(), &body, &field, limits());
    }
}
