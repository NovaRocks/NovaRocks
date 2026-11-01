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
    array::{
        Array, ArrayRef, FixedSizeBinaryArray, Float32Array, Int64Array, NullArray, StringArray,
        StringViewArray, TimestampNanosecondArray,
    },
    datatypes::{DataType, Schema, TimeUnit},
    ipc::{
        self,
        reader::StreamReader,
        writer::{IpcWriteOptions, StreamWriter},
    },
    record_batch::RecordBatch,
};
use flatbuffers::FlatBufferBuilder;
use novarocks_type_contract::{CompileControlError, NR_LOGICAL_TYPE_KEY};
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, Mutex},
};

const EOS: [u8; 8] = [255, 255, 255, 255, 0, 0, 0, 0];

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
fn limits() -> FlatStreamProjectionLimits {
    FlatStreamProjectionLimits {
        max_input_bytes: 1024 * 1024,
        schema: IpcSchemaProjectionLimits {
            max_field_occurrences: 4096,
            max_type_occurrences: 4096,
            max_string_bytes: 1024 * 1024,
            max_flatbuffer_bytes: 1024 * 1024,
        },
        batch: FlatBatchProjectionLimits {
            max_metadata_bytes: 1024 * 1024,
            max_body_bytes: 1024 * 1024,
            max_rows: 4096,
            max_buffer_descriptors: 4096,
            max_view_validation_bytes: 1024 * 1024,
        },
    }
}
fn verifier() -> VerifierOptions {
    VerifierOptions {
        max_depth: 67,
        max_tables: 16384,
        max_apparent_size: 4 * 1024 * 1024,
        ignore_missing_null_terminator: false,
    }
}
fn preflight<'a, 'f>(
    input: &'a [u8],
    field: &'f Field,
    control: &Control,
) -> Result<FlatConstantStream<'a, 'f>, TypeCodecError> {
    preflight_flat_constant_stream(input, field, limits(), &verifier(), control)
}
fn prefixes(
    input: &[u8],
    field: &Field,
    bound: FlatStreamProjectionLimits,
    trace: &[(CompilePhase, u32)],
    positions: impl IntoIterator<Item = usize>,
) {
    for at in positions {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(preflight_flat_constant_stream(input, field, bound, &verifier(), &control), Err(TypeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
fn ordinary(input: &[u8], field: &Field, bound: FlatStreamProjectionLimits) {
    let control = Control::good();
    assert!(matches!(
        preflight_flat_constant_stream(input, field, bound, &verifier(), &control),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let trace = control.trace();
    assert!(trace.len() >= 2, "ordinary refusal omitted its tail");
    prefixes(input, field, bound, &trace, 0..trace.len());
}
fn stream(array: ArrayRef, field: &Field, alignment: usize) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let options = IpcWriteOptions::try_new(alignment, false, ipc::MetadataVersion::V5).unwrap();
    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new_with_options(&mut output, &schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    output
}
fn read(input: &[u8]) -> RecordBatch {
    // Reader entry is only used after a complete successful stream preflight.
    let mut reader = StreamReader::try_new(Cursor::new(input), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    batch
}
fn frame(metadata: &[u8], body: &[u8]) -> Vec<u8> {
    let padded = metadata.len().next_multiple_of(8);
    let mut result = Vec::new();
    result.extend_from_slice(&[255; 4]);
    result.extend_from_slice(&(padded as u32).to_le_bytes());
    result.extend_from_slice(metadata);
    result.resize(8 + padded, 0);
    result.extend_from_slice(body);
    result
}
fn assembled(schema: &[u8], batch: &[u8], end: &[u8]) -> Vec<u8> {
    [schema, batch, end].concat()
}
fn frames(input: &[u8]) -> (&[u8], &[u8], &[u8]) {
    // Independently split the actual writer fixture using its declared frame
    // lengths; these helpers do not decide acceptance for adversarial input.
    assert_eq!(&input[..4], &[255; 4]);
    let schema_end = 8 + u32::from_le_bytes(input[4..8].try_into().unwrap()) as usize;
    assert_eq!(&input[schema_end..schema_end + 4], &[255; 4]);
    let batch_meta_end = schema_end
        + 8
        + u32::from_le_bytes(input[schema_end + 4..schema_end + 8].try_into().unwrap()) as usize;
    let message = ipc::root_as_message(&input[schema_end + 8..batch_meta_end]).unwrap();
    let batch_end = batch_meta_end + usize::try_from(message.bodyLength()).unwrap();
    (
        &input[..schema_end],
        &input[schema_end..batch_end],
        &input[batch_end..],
    )
}
#[derive(Clone, Copy)]
struct BatchProfile {
    body: i64,
    compressed: bool,
    metadata: bool,
    header: ipc::MessageHeader,
    version: ipc::MetadataVersion,
    offset: i64,
}
impl BatchProfile {
    fn ordinary() -> Self {
        Self {
            body: 8,
            compressed: false,
            metadata: false,
            header: ipc::MessageHeader::RecordBatch,
            version: ipc::MetadataVersion::V5,
            offset: 0,
        }
    }
}
fn batch_metadata(profile: BatchProfile) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(&[ipc::FieldNode::new(1, 0)]);
    let buffers =
        builder.create_vector(&[ipc::Buffer::new(0, 0), ipc::Buffer::new(profile.offset, 8)]);
    let compression = profile.compressed.then(|| {
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
            length: 1,
            nodes: Some(nodes),
            buffers: Some(buffers),
            compression,
            variadicBufferCounts: None,
        },
    );
    let header = if profile.header == ipc::MessageHeader::DictionaryBatch {
        ipc::DictionaryBatch::create(
            &mut builder,
            &ipc::DictionaryBatchArgs {
                id: 7,
                data: Some(batch),
                isDelta: false,
            },
        )
        .as_union_value()
    } else {
        batch.as_union_value()
    };
    let custom_metadata = if profile.metadata {
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
            version: profile.version,
            header_type: profile.header,
            header: Some(header),
            bodyLength: profile.body,
            custom_metadata,
        },
    );
    ipc::finish_message_buffer(&mut builder, message);
    builder.finished_data().to_vec()
}

#[test]
fn actual_writer_alignments_preserve_borrowed_input_field_and_scalar_bits() {
    let array: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(f32::from_bits(0x80000000)),
        None,
        Some(f32::from_bits(0x7fc00071)),
    ]));
    let field = Field::new("source", DataType::Float32, true)
        .with_metadata(HashMap::from([("provider_id".to_owned(), "7".to_owned())]));
    for alignment in [8, 16, 32, 64] {
        let input = stream(array.clone(), &field, alignment);
        let control = Control::good();
        let projected = preflight(&input, &field, &control).unwrap();
        assert!(std::ptr::eq(projected.field(), &field));
        assert_eq!(projected.input().as_ptr(), input.as_ptr());
        assert_eq!(projected.input().len(), input.len());
        let lower = input.as_ptr() as usize;
        let upper = lower + input.len();
        for borrowed in [projected.batch_metadata(), projected.batch_body()] {
            let start = borrowed.as_ptr() as usize;
            assert!(start >= lower && start + borrowed.len() <= upper);
        }
        assert_eq!(
            (projected.geometry().rows, projected.geometry().null_count),
            (3, 1)
        );
        let decoded = read(&input);
        let actual = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(actual.value(0).to_bits(), 0x80000000);
        assert!(actual.is_null(1));
        assert_eq!(actual.value(2).to_bits(), 0x7fc00071);
        let trace = control.trace();
        prefixes(&input, &field, limits(), &trace, 0..trace.len());
    }
}

// A small independent public-reader oracle, not production allocation admission.
fn read_checked_header(projected: &FlatConstantStream<'_, '_>, field: Arc<Field>) -> RecordBatch {
    assert!(std::ptr::eq(projected.field(), field.as_ref()));
    let schema = Arc::new(Schema::new([Arc::clone(&field)]));
    let body = arrow_buffer::Buffer::from(projected.batch_body());
    let batch = ipc::reader::read_record_batch(
        &body,
        projected.record_batch(),
        schema,
        &HashMap::new(),
        None,
        &projected.metadata_version(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&batch.schema().fields()[0], &field));
    batch
}

#[test]
fn verified_header_reuse_reads_exact_original_field_without_schema_conversion() {
    let field = Arc::new(
        Field::new("exact", DataType::Float32, true)
            .with_metadata(HashMap::from([("provider".into(), "untouched".into())])),
    );
    let array: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(f32::from_bits(0xffc00071)),
        None,
        Some(f32::from_bits(0x80000000)),
    ]));
    for alignment in [8, 16, 32, 64] {
        let input = stream(array.clone(), &field, alignment);
        let control = Control::good();
        let projected = preflight(&input, &field, &control).unwrap();
        let trace = control.trace();
        assert_eq!(projected.record_batch().length(), 3);
        assert_eq!(projected.metadata_version(), ipc::MetadataVersion::V5);
        let batch = read_checked_header(&projected, Arc::clone(&field));
        let actual = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(actual.value(0).to_bits(), 0xffc00071);
        assert!(actual.is_null(1));
        assert_eq!(actual.value(2).to_bits(), 0x80000000);
        // Accessing the retained safe header introduces no re-verification or
        // synthetic control callbacks into the completed stream projection.
        assert_eq!(control.trace(), trace);
    }
}

#[test]
fn verified_header_reuse_preserves_present_empty_timezone_on_original_field_arc() {
    let zone = Some(Arc::<str>::from(""));
    let field = Arc::new(Field::new(
        "exact",
        DataType::Timestamp(TimeUnit::Nanosecond, zone.clone()),
        true,
    ));
    let array: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(vec![Some(-1), None, Some(71)]).with_timezone_opt(zone),
    );
    let standard = stream(array, &field, 8);
    let (_, batch, eos) = frames(&standard);
    let schema =
        crate::ipc_schema_v2::encode_single_field_schema(&field, limits().schema, &Control::good())
            .unwrap();
    let input = assembled(&frame(&schema, &[]), batch, eos);
    let projected = preflight(&input, &field, &Control::good()).unwrap();
    let decoded = read_checked_header(&projected, Arc::clone(&field));
    assert_eq!(decoded.column(0).data_type(), field.data_type());
    let actual = decoded
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(actual.value(0), -1);
    assert!(actual.is_null(1));
    assert_eq!(actual.value(2), 71);
}

#[test]
fn exact_schema_replacement_preserves_none_and_present_empty_timezone() {
    for zone in [None, Some(Arc::<str>::from(""))] {
        let ty = DataType::Timestamp(TimeUnit::Nanosecond, zone.clone());
        let field = Field::new("source", ty, true);
        let array: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![Some(-1), None, Some(71)])
                .with_timezone_opt(zone.clone()),
        );
        let standard = stream(array, &field, 8);
        let (_, batch, eos) = frames(&standard);
        if zone.is_some() {
            ordinary(&standard, &field, limits());
        }
        let schema = crate::ipc_schema_v2::encode_single_field_schema(
            &field,
            limits().schema,
            &Control::good(),
        )
        .unwrap();
        let input = assembled(&frame(&schema, &[]), batch, eos);
        let control = Control::good();
        let projected = preflight(&input, &field, &control).unwrap();
        assert_eq!(projected.field().data_type(), field.data_type());
        let decoded = read(&input);
        assert_eq!(decoded.schema().field(0).data_type(), field.data_type());
        let values = decoded
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(values.value(0), -1);
        assert!(values.is_null(1));
        assert_eq!(values.value(2), 71);
        let trace = control.trace();
        prefixes(&input, &field, limits(), &trace, 0..trace.len());
    }
}

#[test]
fn complete_source_fields_are_exact_without_fabricating_logical_identity() {
    let plain = Field::new("source", DataType::Utf8, true)
        .with_metadata(HashMap::from([("provider_id".to_owned(), "7".to_owned())]));
    let json = plain.clone().with_metadata(HashMap::from([
        ("provider_id".to_owned(), "7".to_owned()),
        (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
    ]));
    let array: ArrayRef = Arc::new(StringArray::from(vec![Some("{\"v\":7}"), None]));
    for source in [&plain, &json] {
        let input = stream(array.clone(), source, 8);
        let control = Control::good();
        let projected = preflight(&input, source, &control).unwrap();
        assert!(std::ptr::eq(projected.field(), source));
        assert_eq!(projected.field().metadata(), source.metadata());
        assert_eq!(
            read(&input)
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "{\"v\":7}"
        );
        let trace = control.trace();
        prefixes(&input, source, limits(), &trace, 0..trace.len());
        for wrong in [
            Field::new("alias", DataType::Utf8, true),
            Field::new("source", DataType::Utf8, false),
            Field::new("source", DataType::Binary, true),
            source
                .clone()
                .with_metadata(HashMap::from([("provider_id".to_owned(), "8".to_owned())])),
        ] {
            ordinary(&input, &wrong, limits());
        }
    }
    ordinary(&stream(array.clone(), &plain, 8), &json, limits());
    ordinary(&stream(array, &json, 8), &plain, limits());
    let binary: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from(vec![b"0123456789abcdef".as_slice()]).unwrap());
    let field = Field::new("source", DataType::FixedSizeBinary(16), true);
    let input = stream(binary, &field, 8);
    let control = Control::good();
    let projected = preflight(&input, &field, &control).unwrap();
    let trace = control.trace();
    prefixes(&input, &field, limits(), &trace, 0..trace.len());
    assert!(
        !projected
            .field()
            .metadata()
            .contains_key(NR_LOGICAL_TYPE_KEY)
    );
    // Field identity is proven here; the later ConstantPool owner still has to
    // check the independently supplied complete FunctionValueType.
}

#[test]
fn required_schema_batch_eos_order_rejects_eof_trailing_and_extra_messages() {
    let field = Field::new("source", DataType::Int64, true);
    let input = stream(Arc::new(Int64Array::from(vec![7])), &field, 8);
    let (schema, batch, eos) = frames(&input);
    assert_eq!(eos, &EOS);
    for malformed in [
        Vec::new(),
        EOS.to_vec(),
        schema.to_vec(),
        assembled(schema, batch, &[]),
        assembled(batch, schema, eos),
        assembled(schema, schema, eos),
        assembled(schema, &[], eos),
        assembled(schema, batch, &[EOS.as_slice(), &[71]].concat()),
        assembled(schema, &[batch, batch].concat(), eos),
        assembled(schema, batch, &[schema, eos].concat()),
    ] {
        ordinary(&malformed, &field, limits());
    }
    // Bare EOF is accepted by the general reader, but it is not this complete
    // constant stream profile's explicit terminal fact.
    let no_eos = assembled(schema, batch, &[]);
    let mut reader = StreamReader::try_new(Cursor::new(&no_eos), None).unwrap();
    assert!(reader.next().unwrap().is_ok());
    assert!(reader.next().is_none());
}

#[test]
fn independent_batch_header_compression_body_extents_and_ranges_refuse() {
    let field = Field::new("source", DataType::Int64, true);
    let original = stream(Arc::new(Int64Array::from(vec![7])), &field, 8);
    let (schema, _, _) = frames(&original);
    let base = BatchProfile::ordinary();
    let good = assembled(
        schema,
        &frame(&batch_metadata(base), &7i64.to_le_bytes()),
        &EOS,
    );
    let control = Control::good();
    preflight(&good, &field, &control).unwrap();
    assert_eq!(
        read(&good)
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    let trace = control.trace();
    prefixes(&good, &field, limits(), &trace, 0..trace.len());
    for profile in [
        BatchProfile { body: -1, ..base },
        BatchProfile {
            body: i64::MAX,
            ..base
        },
        BatchProfile { body: 16, ..base },
        BatchProfile { body: 7, ..base },
        BatchProfile {
            compressed: true,
            ..base
        },
        BatchProfile {
            metadata: true,
            ..base
        },
        BatchProfile {
            header: ipc::MessageHeader::DictionaryBatch,
            ..base
        },
        BatchProfile {
            version: ipc::MetadataVersion::V4,
            ..base
        },
        BatchProfile { offset: 1, ..base },
        BatchProfile { offset: -1, ..base },
    ] {
        ordinary(
            &assembled(schema, &frame(&batch_metadata(profile), &[0; 8]), &EOS),
            &field,
            limits(),
        );
    }
}

#[test]
fn continuation_signed_lengths_alignment_and_truncation_refuse_before_reader() {
    let field = Field::new("source", DataType::Int64, true);
    let input = stream(Arc::new(Int64Array::from(vec![7])), &field, 8);
    let (schema, batch, _) = frames(&input);
    let mut wrong_marker = input.clone();
    wrong_marker[0] = 0;
    ordinary(&wrong_marker, &field, limits());
    ordinary(&input[4..], &field, limits());
    for length in [0x80000000u32, u32::MAX, 7, 8_388_608] {
        let mut bytes = input.clone();
        bytes[4..8].copy_from_slice(&length.to_le_bytes());
        ordinary(&bytes, &field, limits());
    }
    for cutoff in [
        1,
        7,
        schema.len() - 1,
        schema.len() + 7,
        schema.len() + batch.len() - 1,
        input.len() - 1,
    ] {
        ordinary(&input[..cutoff], &field, limits());
    }
    let schema_len = schema.len();
    let mut wrong_second = input;
    wrong_second[schema_len + 4..schema_len + 8].copy_from_slice(&0x80000000u32.to_le_bytes());
    ordinary(&wrong_second, &field, limits());
}

#[test]
fn explicit_input_schema_batch_envelopes_have_near_and_one_over_boundaries() {
    let field = Field::new("source", DataType::Int64, true);
    let input = stream(Arc::new(Int64Array::from(vec![Some(7), None])), &field, 8);
    let control = Control::good();
    let projected = preflight(&input, &field, &control).unwrap();
    let exact = FlatStreamProjectionLimits {
        max_input_bytes: input.len(),
        batch: FlatBatchProjectionLimits {
            max_metadata_bytes: projected.batch_metadata().len(),
            max_body_bytes: projected.batch_body().len(),
            max_rows: 2,
            max_buffer_descriptors: 2,
            max_view_validation_bytes: 0,
        },
        ..limits()
    };
    let control = Control::good();
    preflight_flat_constant_stream(&input, &field, exact, &verifier(), &control).unwrap();
    let trace = control.trace();
    prefixes(&input, &field, exact, &trace, 0..trace.len());
    for bound in [
        FlatStreamProjectionLimits {
            max_input_bytes: input.len() - 1,
            ..exact
        },
        FlatStreamProjectionLimits {
            batch: FlatBatchProjectionLimits {
                max_metadata_bytes: exact.batch.max_metadata_bytes - 1,
                ..exact.batch
            },
            ..exact
        },
        FlatStreamProjectionLimits {
            batch: FlatBatchProjectionLimits {
                max_body_bytes: exact.batch.max_body_bytes - 1,
                ..exact.batch
            },
            ..exact
        },
        FlatStreamProjectionLimits {
            batch: FlatBatchProjectionLimits {
                max_rows: 1,
                ..exact.batch
            },
            ..exact
        },
        FlatStreamProjectionLimits {
            batch: FlatBatchProjectionLimits {
                max_buffer_descriptors: 1,
                ..exact.batch
            },
            ..exact
        },
        FlatStreamProjectionLimits {
            schema: IpcSchemaProjectionLimits {
                max_field_occurrences: 0,
                ..exact.schema
            },
            ..exact
        },
        FlatStreamProjectionLimits {
            schema: IpcSchemaProjectionLimits {
                max_type_occurrences: 0,
                ..exact.schema
            },
            ..exact
        },
        FlatStreamProjectionLimits {
            schema: IpcSchemaProjectionLimits {
                max_string_bytes: 5,
                ..exact.schema
            },
            ..exact
        },
    ] {
        ordinary(&input, &field, bound);
    }
}

#[test]
fn zero_row_and_compact_null_streams_keep_exact_row_facts_without_payload() {
    for array in [
        Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        Arc::new(NullArray::new(4)),
    ] {
        let field = Field::new("source", array.data_type().clone(), true);
        let input = stream(array.clone(), &field, 8);
        let control = Control::good();
        let projected = preflight(&input, &field, &control).unwrap();
        assert_eq!(projected.geometry().rows, array.len());
        assert_eq!(projected.geometry().null_count, array.logical_null_count());
        assert_eq!(projected.batch_body().len(), 0);
        assert_eq!(read(&input).column(0).len(), array.len());
        let trace = control.trace();
        prefixes(&input, &field, limits(), &trace, 0..trace.len());
        if !array.is_empty() {
            ordinary(
                &input,
                &field,
                FlatStreamProjectionLimits {
                    batch: FlatBatchProjectionLimits {
                        max_rows: 3,
                        ..limits().batch
                    },
                    ..limits()
                },
            );
        }
    }
}

#[test]
fn real_view_stream_has_repeated_work_extent_and_original_quantum_control() {
    let array: ArrayRef = Arc::new(StringViewArray::from(vec!["aaaaaaaaaaaaa"; 300]));
    let field = Field::new("source", DataType::Utf8View, true);
    let input = stream(array, &field, 8);
    let control = Control::good();
    let projected = preflight(&input, &field, &control).unwrap();
    assert_eq!(projected.geometry().rows, 300);
    assert_eq!(projected.geometry().view_validation_bytes, 3900);
    assert_eq!(
        read(&input)
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(299),
        "aaaaaaaaaaaaa"
    );
    let trace = control.trace();
    let mut positions: Vec<_> = trace
        .iter()
        .enumerate()
        .filter_map(|(at, (_, units))| (*units == 256).then_some(at))
        .collect();
    assert!(!positions.is_empty());
    positions.extend([0, trace.len() - 2, trace.len() - 1]);
    positions.sort_unstable();
    positions.dedup();
    prefixes(&input, &field, limits(), &trace, positions);
    let bound = FlatStreamProjectionLimits {
        batch: FlatBatchProjectionLimits {
            max_view_validation_bytes: 3899,
            ..limits().batch
        },
        ..limits()
    };
    let error_control = Control::good();
    assert!(matches!(
        preflight_flat_constant_stream(&input, &field, bound, &verifier(), &error_control),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let trace = error_control.trace();
    let mut positions: Vec<_> = trace
        .iter()
        .enumerate()
        .filter_map(|(at, (_, units))| (*units == 256).then_some(at))
        .collect();
    positions.extend([0, trace.len() - 1]);
    prefixes(&input, &field, bound, &trace, positions);
}

fn constant_policy() -> novarocks_constant_contract::ConstantPolicy {
    novarocks_constant_contract::ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 16384,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}

fn pool_prefixes(
    projected: &FlatConstantStream<'_, '_>,
    ty: &novarocks_type_contract::FunctionValueType,
    policy: novarocks_constant_contract::ConstantPolicy,
    trace: &[(CompilePhase, u32)],
) {
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(matches!(
                projected.preflight_pool_resources(ty, policy, &control),
                Err(FlatPoolResourceError::Control(actual)) if actual == cause
            ));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn real_flat_stream_pool_projection_dominates_actual_owner_facts() {
    use novarocks_constant_contract::ConstantPool;
    use novarocks_type_contract::FunctionValueType;
    for array in [
        Arc::new(Float32Array::from(vec![Some(-0.0), None, Some(f32::NAN)])) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("first"), None, Some("last")])),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        Arc::new(StringViewArray::from(vec![
            Some("long repeated string"),
            None,
            Some("long repeated string"),
        ])),
        Arc::new(NullArray::new(4)),
    ] {
        let field = Arc::new(Field::new("exact", array.data_type().clone(), true));
        let ty = FunctionValueType::new(array.data_type().clone(), true);
        let input = stream(array, &field, 8);
        let checked = preflight(&input, &field, &Control::good()).unwrap();
        let control = Control::good();
        let bounds = checked
            .preflight_pool_resources(&ty, constant_policy(), &control)
            .unwrap();
        let decoded = read_checked_header(&checked, Arc::clone(&field));
        let pool = ConstantPool::try_new(
            Arc::clone(&field),
            ty.clone(),
            decoded.column(0).to_data(),
            constant_policy(),
            CompilePhase::Decode,
            &Control::good(),
        )
        .unwrap();
        let actual = pool.resource_facts();
        assert_eq!(actual.metadata_bytes, bounds.constant.metadata_bytes);
        assert!(
            actual.retained_buffer_capacity_bytes
                <= (bounds.owned_body_capacity_bytes
                    + bounds.alignment_repair_capacity_bytes_upper_bound
                    + bounds.empty_offset_capacity_bytes) as u64
        );
        assert!(
            actual.library_validation_work_upper_bound
                <= bounds.constant.library_validation_work_upper_bound
        );
        assert!(
            actual.library_validation_bytes_upper_bound
                <= bounds.constant.library_validation_bytes_upper_bound
        );
        assert!(
            actual.library_validation_temporary_bytes_upper_bound
                <= bounds
                    .constant
                    .library_validation_temporary_bytes_upper_bound
        );
        if matches!(field.data_type(), DataType::Null) {
            assert_eq!(actual.retained_buffer_capacity_bytes, 0);
        }
        if actual.rows > 1 {
            let value = pool.value(1).unwrap();
            assert_eq!(value.ordinal(), 1);
            assert!(std::ptr::eq(value.field(), field.as_ref()));
        }
        let trace = control.trace();
        assert!(
            trace
                .iter()
                .all(|(phase, units)| *phase == CompilePhase::Decode && *units <= 256)
        );
        pool_prefixes(&checked, &ty, constant_policy(), &trace);
    }
}

#[test]
fn pool_projection_checks_original_fvt_policy_and_ordinary_failure_tails() {
    use novarocks_type_contract::FunctionValueType;
    let field = Arc::new(Field::new("exact", DataType::Float32, true));
    let input = stream(Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])), &field, 8);
    let checked = preflight(&input, &field, &Control::good()).unwrap();
    let ty = FunctionValueType::new(DataType::Float32, true);
    let good = checked
        .preflight_pool_resources(&ty, constant_policy(), &Control::good())
        .unwrap();
    let exact = novarocks_constant_contract::ConstantPolicy {
        max_rows: 3,
        max_retained_buffer_bytes: good.owned_body_capacity_bytes as u64,
        max_library_validation_work: good.constant.library_validation_work_upper_bound,
        max_library_validation_bytes: good.constant.library_validation_bytes_upper_bound,
        ..constant_policy()
    };
    assert_eq!(
        checked
            .preflight_pool_resources(&ty, exact, &Control::good())
            .unwrap(),
        good
    );
    let wrong = FunctionValueType::new(DataType::Float32, false);
    for (value_type, policy) in [
        (&wrong, exact),
        (
            &ty,
            novarocks_constant_contract::ConstantPolicy {
                max_rows: 2,
                ..exact
            },
        ),
        (
            &ty,
            novarocks_constant_contract::ConstantPolicy {
                max_retained_buffer_bytes: exact.max_retained_buffer_bytes - 1,
                ..exact
            },
        ),
        (
            &ty,
            novarocks_constant_contract::ConstantPolicy {
                max_library_validation_work: exact.max_library_validation_work - 1,
                ..exact
            },
        ),
        (
            &ty,
            novarocks_constant_contract::ConstantPolicy {
                max_library_validation_bytes: exact.max_library_validation_bytes - 1,
                ..exact
            },
        ),
    ] {
        let control = Control::good();
        assert!(matches!(
            checked.preflight_pool_resources(value_type, policy, &control),
            Err(FlatPoolResourceError::Constant(_))
        ));
        let trace = control.trace();
        assert!(trace.len() >= 4);
        assert_eq!(trace.last(), Some(&(CompilePhase::Decode, 0)));
        pool_prefixes(&checked, value_type, policy, &trace);
    }
}

#[test]
fn intrinsic_null_false_reader_oracle_keeps_final_pool_value_obligation() {
    use novarocks_constant_contract::{ConstantError, ConstantPool};
    use novarocks_type_contract::FunctionValueType;
    let field = Arc::new(Field::new("exact", DataType::Null, false));
    let input = stream(Arc::new(NullArray::new(4)), &field, 8);
    let checked = preflight(&input, &field, &Control::good()).unwrap();
    let ty = FunctionValueType::new(DataType::Null, false);
    let projected = checked
        .preflight_pool_resources(&ty, constant_policy(), &Control::good())
        .unwrap();
    assert_eq!(projected.owned_body_capacity_bytes, 0);
    let decoded = read_checked_header(&checked, Arc::clone(&field));
    assert_eq!(decoded.column(0).null_count(), 0);
    assert_eq!(decoded.column(0).logical_null_count(), 4);
    assert!(matches!(
        ConstantPool::try_new(
            field,
            ty,
            decoded.column(0).to_data(),
            constant_policy(),
            CompilePhase::Decode,
            &Control::good(),
        ),
        Err(ConstantError::Invalid(
            "non-null constant contains SQL NULL"
        ))
    ));
}

fn raw_flat_batch(rows: i64, nulls: i64, buffers: &[(i64, i64)], body: &[u8]) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(&[ipc::FieldNode::new(rows, nulls)]);
    let buffers: Vec<_> = buffers
        .iter()
        .map(|&(offset, len)| ipc::Buffer::new(offset, len))
        .collect();
    let buffers = builder.create_vector(&buffers);
    let batch = ipc::RecordBatch::create(
        &mut builder,
        &ipc::RecordBatchArgs {
            length: rows,
            nodes: Some(nodes),
            buffers: Some(buffers),
            ..Default::default()
        },
    );
    let message = ipc::Message::create(
        &mut builder,
        &ipc::MessageArgs {
            version: ipc::MetadataVersion::V5,
            header_type: ipc::MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: body.len() as i64,
            ..Default::default()
        },
    );
    ipc::finish_message_buffer(&mut builder, message);
    frame(builder.finished_data(), body)
}

#[test]
fn pool_projection_includes_synthesized_empty_offset_backing_and_inspection() {
    use novarocks_constant_contract::ConstantPool;
    use novarocks_type_contract::FunctionValueType;
    for (ty, width) in [(DataType::Utf8, 4), (DataType::LargeBinary, 8)] {
        let field = Arc::new(Field::new("exact", ty.clone(), true));
        let schema = crate::ipc_schema_v2::encode_single_field_schema(
            &field,
            limits().schema,
            &Control::good(),
        )
        .unwrap();
        let batch = raw_flat_batch(0, 0, &[(0, 0), (0, 0), (0, 0)], &[]);
        let input = assembled(&frame(&schema, &[]), &batch, &EOS);
        let checked = preflight(&input, &field, &Control::good()).unwrap();
        let ty = FunctionValueType::new(ty, true);
        let control = Control::good();
        let bounds = checked
            .preflight_pool_resources(&ty, constant_policy(), &control)
            .unwrap();
        assert_eq!(bounds.owned_body_capacity_bytes, 0);
        assert_eq!(bounds.empty_offset_capacity_bytes, width);
        let decoded = read_checked_header(&checked, Arc::clone(&field));
        let pool = ConstantPool::try_new(
            Arc::clone(&field),
            ty.clone(),
            decoded.column(0).to_data(),
            constant_policy(),
            CompilePhase::Decode,
            &Control::good(),
        )
        .unwrap();
        let actual = pool.resource_facts();
        assert_eq!(actual.rows, 0);
        assert_eq!(actual.retained_buffer_capacity_bytes, width as u64);
        assert!(
            actual.library_validation_work_upper_bound
                <= bounds.constant.library_validation_work_upper_bound
        );
        assert!(
            actual.library_validation_bytes_upper_bound
                <= bounds.constant.library_validation_bytes_upper_bound
        );
        pool_prefixes(&checked, &ty, constant_policy(), &control.trace());
    }
}

#[test]
fn pool_projection_covers_full_unaligned_typed_descriptor_and_utf8_fallback() {
    use novarocks_constant_contract::ConstantPool;
    use novarocks_type_contract::FunctionValueType;
    for (ty, buffers, body, repair) in [
        (
            DataType::Int64,
            vec![(0, 0), (1, 80)],
            {
                let mut body = vec![0; 88];
                body[1..9].copy_from_slice(&71i64.to_le_bytes());
                body
            },
            128,
        ),
        (
            DataType::Utf8,
            vec![(0, 0), (0, 8), (8, 4)],
            {
                let mut body = vec![0; 16];
                body[4..8].copy_from_slice(&3i32.to_le_bytes());
                body[8..12].copy_from_slice(b"abc\xff");
                body
            },
            0,
        ),
    ] {
        let field = Arc::new(Field::new("exact", ty.clone(), true));
        let schema = crate::ipc_schema_v2::encode_single_field_schema(
            &field,
            limits().schema,
            &Control::good(),
        )
        .unwrap();
        let input = assembled(
            &frame(&schema, &[]),
            &raw_flat_batch(1, 0, &buffers, &body),
            &EOS,
        );
        let checked = preflight(&input, &field, &Control::good()).unwrap();
        let ty = FunctionValueType::new(ty, true);
        let control = Control::good();
        let bounds = checked
            .preflight_pool_resources(&ty, constant_policy(), &control)
            .unwrap();
        assert_eq!(bounds.alignment_repair_capacity_bytes_upper_bound, repair);
        let decoded = read_checked_header(&checked, Arc::clone(&field));
        let pool = ConstantPool::try_new(
            Arc::clone(&field),
            ty.clone(),
            decoded.column(0).to_data(),
            constant_policy(),
            CompilePhase::Decode,
            &Control::good(),
        )
        .unwrap();
        let actual = pool.resource_facts();
        assert!(
            actual.library_validation_work_upper_bound
                <= bounds.constant.library_validation_work_upper_bound
        );
        assert!(
            actual.library_validation_bytes_upper_bound
                <= bounds.constant.library_validation_bytes_upper_bound
        );
        if repair == 0 {
            assert_eq!(
                pool.array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "abc"
            );
        } else {
            assert!(bounds.alignment_repair_possible);
            assert_eq!(
                pool.array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                71
            );
        }
        pool_prefixes(&checked, &ty, constant_policy(), &control.trace());
    }
}
