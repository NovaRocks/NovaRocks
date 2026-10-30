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
        ArrayRef, Decimal128Array, Float64Array, Int64Array, IntervalMonthDayNanoArray, NullArray,
        StringArray, StringViewArray, TimestampNanosecondArray,
    },
    datatypes::{DataType, IntervalUnit, Schema, TimeUnit},
    ipc::{
        self,
        writer::{IpcWriteOptions, StreamWriter},
    },
    record_batch::RecordBatch,
};
use arrow_buffer::IntervalMonthDayNano;
use flatbuffers::FlatBufferBuilder;
use novarocks_constant_contract::{ConstantError, ConstantPolicy, ConstantPool};
use novarocks_type_contract::{
    CompileControlError, FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType,
};
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
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal: None,
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((refusal, _)) = self.refusal {
            assert!(at <= refusal, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((refusal, cause)) if refusal == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 1,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 1,
        max_dictionary_depth: 0,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}
fn schema_limits() -> IpcSchemaProjectionLimits {
    IpcSchemaProjectionLimits {
        max_field_occurrences: 1,
        max_type_occurrences: 1,
        max_string_bytes: 65536,
        max_flatbuffer_bytes: 65536,
    }
}
fn stream_limits() -> FlatStreamProjectionLimits {
    FlatStreamProjectionLimits {
        max_input_bytes: 1024 * 1024,
        schema: schema_limits(),
        batch: FlatBatchProjectionLimits {
            max_metadata_bytes: 65536,
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
        max_tables: 4096,
        max_apparent_size: 4 * 1024 * 1024,
        ignore_missing_null_terminator: false,
    }
}
fn reader_limits() -> FlatReaderProjectionLimits {
    FlatReaderProjectionLimits {
        max_new_allocation_request_bytes: 16 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 32 * 1024 * 1024,
        max_cumulative_library_work: 64 * 1024 * 1024,
    }
}
fn checked<'a, 'f>(input: &'a [u8], field: &'f Field) -> FlatConstantStream<'a, 'f> {
    preflight_flat_constant_stream(input, field, stream_limits(), &verifier(), &Control::good())
        .unwrap()
}
fn retained(input: &Vec<u8>, field: &Field) -> usize {
    // An explicit finite fixture allowance for the original Vec backing and
    // retained source owners. This is not a measured Account or MEM receipt.
    input.capacity()
        + 4096
        + field.name().len()
        + novarocks_type_contract::owned_resources::arrow_metadata::retained_backing_upper_bound(
            field.metadata().len(),
        )
        .unwrap()
        + field
            .metadata()
            .iter()
            .map(|(k, v)| k.capacity() + v.capacity())
            .sum::<usize>()
}
fn writer_stream(array: ArrayRef, field: &Arc<Field>) -> Vec<u8> {
    let schema = Arc::new(Schema::new([Arc::clone(field)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
    let options = IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5).unwrap();
    let mut input = Vec::new();
    {
        let mut writer = StreamWriter::try_new_with_options(&mut input, &schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    input
}
// Fixture schema construction has its own Encode observation, outside the
// materialization trace asserted by Control.
struct FixtureEncodeControl;
impl PureCompileControl for FixtureEncodeControl {
    fn checkpoint(&self, phase: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        Ok(())
    }
}
fn frame(metadata: &[u8], body: &[u8]) -> Vec<u8> {
    let padded = metadata.len().next_multiple_of(8);
    let mut bytes = vec![255; 4];
    bytes.extend_from_slice(&(padded as u32).to_le_bytes());
    bytes.extend_from_slice(metadata);
    bytes.resize(8 + padded, 0);
    bytes.extend_from_slice(body);
    bytes
}
fn exact_schema_stream(input: &[u8], field: &Field) -> Vec<u8> {
    let schema_end = 8 + u32::from_le_bytes(input[4..8].try_into().unwrap()) as usize;
    let schema = crate::ipc_schema_v2::encode_single_field_schema(
        field,
        schema_limits(),
        &FixtureEncodeControl,
    )
    .unwrap();
    [frame(&schema, &[]), input[schema_end..].to_vec()].concat()
}
fn raw_stream(
    field: &Field,
    rows: i64,
    nulls: i64,
    descriptors: &[(i64, i64)],
    variadic: Option<i64>,
    body: &[u8],
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(&[ipc::FieldNode::new(rows, nulls)]);
    let buffers: Vec<_> = descriptors
        .iter()
        .map(|&(offset, len)| ipc::Buffer::new(offset, len))
        .collect();
    let buffers = builder.create_vector(&buffers);
    let variadic = variadic.map(|count| builder.create_vector(&[count]));
    let batch = ipc::RecordBatch::create(
        &mut builder,
        &ipc::RecordBatchArgs {
            length: rows,
            nodes: Some(nodes),
            buffers: Some(buffers),
            compression: None,
            variadicBufferCounts: variadic,
        },
    );
    let message = ipc::Message::create(
        &mut builder,
        &ipc::MessageArgs {
            version: ipc::MetadataVersion::V5,
            header_type: ipc::MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: body.len() as i64,
            custom_metadata: None,
        },
    );
    ipc::finish_message_buffer(&mut builder, message);
    let schema = crate::ipc_schema_v2::encode_single_field_schema(
        field,
        schema_limits(),
        &FixtureEncodeControl,
    )
    .unwrap();
    [
        frame(&schema, &[]),
        frame(builder.finished_data(), body),
        vec![255, 255, 255, 255, 0, 0, 0, 0],
    ]
    .concat()
}
fn mutate_buffer(input: &mut [u8], index: usize, offset: usize, replacement: &[u8]) {
    // This independently locates a descriptor in an already valid writer
    // fixture. Its payload mutation deliberately bypasses no acceptance gate.
    let start = 8 + u32::from_le_bytes(input[4..8].try_into().unwrap()) as usize;
    let body =
        start + 8 + u32::from_le_bytes(input[start + 4..start + 8].try_into().unwrap()) as usize;
    let message = ipc::root_as_message(&input[start + 8..body]).unwrap();
    let descriptor = message
        .header_as_record_batch()
        .unwrap()
        .buffers()
        .unwrap()
        .get(index);
    let location = body + descriptor.offset() as usize + offset;
    assert!(offset + replacement.len() <= descriptor.length() as usize);
    input[location..location + replacement.len()].copy_from_slice(replacement);
}
fn materialize(
    stream: &FlatConstantStream<'_, '_>,
    field: &Arc<Field>,
    ty: &FunctionValueType,
    retention: usize,
    limits: FlatReaderProjectionLimits,
    control: &Control,
) -> Result<ConstantPool, FlatReaderError> {
    stream.materialize_pool(
        Arc::clone(field),
        ty.clone(),
        retention,
        policy(),
        limits,
        control,
    )
}
fn assert_materialize_prefixes(
    stream: &FlatConstantStream<'_, '_>,
    field: &Arc<Field>,
    ty: &FunctionValueType,
    retention: usize,
    limits: FlatReaderProjectionLimits,
    trace: &[(CompilePhase, u32)],
    positions: impl IntoIterator<Item = usize>,
) {
    for at in positions {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(materialize(stream,field,ty,retention,limits,&control),
            Err(FlatReaderError::Projection(FlatPoolResourceError::Control(actual))) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
fn ordinary_materialize(
    stream: &FlatConstantStream<'_, '_>,
    field: &Arc<Field>,
    ty: &FunctionValueType,
    retention: usize,
    limits: FlatReaderProjectionLimits,
) -> FlatReaderError {
    let control = Control::good();
    let error = materialize(stream, field, ty, retention, limits, &control).unwrap_err();
    assert!(!matches!(
        error,
        FlatReaderError::Projection(FlatPoolResourceError::Control(_))
    ));
    let trace = control.trace();
    assert!(trace.len() >= 2, "ordinary error omitted its tail");
    assert_materialize_prefixes(stream, field, ty, retention, limits, &trace, 0..trace.len());
    error
}

#[test]
fn actual_reader_scalar_bits_interval_and_nonzero_ordinal_preserve_source_arc() {
    let arrays: [ArrayRef; 3] = [
        Arc::new(Int64Array::from(vec![Some(-71), Some(i64::MAX), None])),
        Arc::new(Float64Array::from(vec![
            Some(f64::from_bits(0x8000000000000000)),
            Some(f64::from_bits(0xfff8000000000071)),
            None,
        ])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(1, -2, 3)),
            Some(IntervalMonthDayNano::new(i32::MIN, i32::MAX, i64::MIN)),
            None,
        ])),
    ];
    for array in arrays {
        let field = Arc::new(
            Field::new("source", array.data_type().clone(), true)
                .with_metadata(HashMap::from([("provider_id".into(), "71".into())])),
        );
        let ty = FunctionValueType::new(array.data_type().clone(), true);
        let input = writer_stream(array, &field);
        let stream = checked(&input, &field);
        let retention = retained(&input, &field);
        let control = Control::good();
        let pool = materialize(&stream, &field, &ty, retention, reader_limits(), &control).unwrap();
        assert!(std::ptr::eq(pool.field(), field.as_ref()));
        assert_eq!(pool.value_type(), &ty);
        assert_eq!(pool.resource_facts().rows, 3);
        let selected = pool.value(1).unwrap();
        assert_eq!(selected.ordinal(), 1);
        match field.data_type() {
            DataType::Int64 => {
                assert_eq!(selected.try_i64().unwrap(), Some(i64::MAX));
                assert_eq!(pool.value(2).unwrap().try_i64().unwrap(), None);
            }
            DataType::Float64 => {
                assert_eq!(
                    pool.value(0).unwrap().try_f64_bits().unwrap(),
                    Some(0x8000000000000000)
                );
                assert_eq!(selected.try_f64_bits().unwrap(), Some(0xfff8000000000071));
                assert_eq!(pool.value(2).unwrap().try_f64_bits().unwrap(), None);
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                assert_eq!(
                    selected.try_interval_month_day_nano().unwrap(),
                    Some((i32::MIN, i32::MAX, i64::MIN))
                );
                assert_eq!(
                    pool.value(2)
                        .unwrap()
                        .try_interval_month_day_nano()
                        .unwrap(),
                    None
                );
            }
            other => panic!("unexpected fixture {other:?}"),
        }
        assert!(pool.value(3).is_err());
        let trace = control.trace();
        assert_materialize_prefixes(
            &stream,
            &field,
            &ty,
            retention,
            reader_limits(),
            &trace,
            0..trace.len(),
        );
    }
}

#[test]
fn actual_reader_exact_empty_timezone_and_nominal_metadata_retain_source_author() {
    let cases: [(ArrayRef, FunctionValueType, HashMap<String, String>); 2] = [
        (
            Arc::new(TimestampNanosecondArray::from(vec![Some(-71), None]).with_timezone("")),
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
                true,
            ),
            HashMap::from([("provider".into(), "unchanged".into())]),
        ),
        (
            Arc::new(StringArray::from(vec![Some("{\"x\":1}"), None])),
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
            HashMap::from([
                (NR_LOGICAL_TYPE_KEY.into(), "json".into()),
                ("provider".into(), "unchanged".into()),
            ]),
        ),
    ];
    for (array, ty, metadata) in cases {
        let field =
            Arc::new(Field::new("source", ty.data_type().clone(), true).with_metadata(metadata));
        let input = exact_schema_stream(&writer_stream(array, &field), &field);
        let stream = checked(&input, &field);
        let retention = retained(&input, &field);
        let control = Control::good();
        let pool = materialize(&stream, &field, &ty, retention, reader_limits(), &control).unwrap();
        assert!(std::ptr::eq(pool.field(), field.as_ref()));
        assert_eq!(pool.value_type(), &ty);
        assert_eq!(pool.field().metadata(), field.metadata());
        if ty.logical_type() == ValueLogicalType::Json {
            assert_eq!(
                pool.value(0).unwrap().try_utf8().unwrap(),
                Some("{\"x\":1}")
            );
        } else {
            assert_eq!(
                pool.array().data_type(),
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("".into()))
            );
            assert_eq!(pool.value(0).unwrap().try_timestamp().unwrap(), Some(-71));
        }
        let trace = control.trace();
        assert_materialize_prefixes(
            &stream,
            &field,
            &ty,
            retention,
            reader_limits(),
            &trace,
            0..trace.len(),
        );
    }
}

#[test]
fn actual_reader_explicit_request_coexistence_and_work_envelopes_have_exact_boundaries() {
    let field = Arc::new(Field::new("source", DataType::Int64, true));
    let ty = FunctionValueType::new(DataType::Int64, true);
    let input = writer_stream(Arc::new(Int64Array::from(vec![Some(71), None])), &field);
    let stream = checked(&input, &field);
    let retention = retained(&input, &field);
    let control = Control::good();
    let facts = stream
        .preflight_reader_resources(&ty, retention, policy(), reader_limits(), &control)
        .unwrap();
    assert_eq!(facts.source_retained_bytes, retention);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        retention + facts.new_allocation_request_bytes_upper_bound
    );
    assert_eq!(
        facts.new_allocation_request_bytes_upper_bound,
        facts.payload_request_bytes_upper_bound
            + facts.structural_request_bytes_upper_bound
            + facts.diagnostic_request_bytes_upper_bound
    );
    assert!(
        facts.cumulative_library_work_upper_bound
            >= 4 * facts.pool.constant.library_validation_work_upper_bound as usize
    );
    let trace = control.trace();
    for at in 0..trace.len() {
        for cause in CAUSES {
            let refusal = Control::refusing(at, cause.clone());
            assert!(
                matches!(stream.preflight_reader_resources(&ty,retention,policy(),reader_limits(),&refusal),Err(FlatPoolResourceError::Control(actual)) if actual==cause)
            );
            assert_eq!(refusal.trace(), trace[..=at]);
        }
    }
    let exact = FlatReaderProjectionLimits {
        max_new_allocation_request_bytes: facts.new_allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_cumulative_library_work: facts.cumulative_library_work_upper_bound,
    };
    assert_eq!(
        materialize(&stream, &field, &ty, retention, exact, &Control::good())
            .unwrap()
            .value(0)
            .unwrap()
            .try_i64()
            .unwrap(),
        Some(71)
    );
    for cap in [
        FlatReaderProjectionLimits {
            max_new_allocation_request_bytes: exact.max_new_allocation_request_bytes - 1,
            ..exact
        },
        FlatReaderProjectionLimits {
            max_coexisting_source_and_request_bytes: exact.max_coexisting_source_and_request_bytes
                - 1,
            ..exact
        },
        FlatReaderProjectionLimits {
            max_cumulative_library_work: exact.max_cumulative_library_work - 1,
            ..exact
        },
    ] {
        assert!(matches!(
            ordinary_materialize(&stream, &field, &ty, retention, cap),
            FlatReaderError::Projection(FlatPoolResourceError::Shape(_))
        ));
    }
    assert!(matches!(
        ordinary_materialize(&stream, &field, &ty, input.len() - 1, reader_limits()),
        FlatReaderError::Projection(FlatPoolResourceError::Shape(_))
    ));
}

#[test]
fn actual_reader_requires_original_field_arc_and_exact_fvt_before_materialization() {
    let field = Arc::new(Field::new("source", DataType::Int64, true));
    let ty = FunctionValueType::new(DataType::Int64, true);
    let input = writer_stream(Arc::new(Int64Array::from(vec![71])), &field);
    let stream = checked(&input, &field);
    let retention = retained(&input, &field);
    let replacement = Arc::new(field.as_ref().clone());
    assert_eq!(&replacement, &field);
    assert!(!Arc::ptr_eq(&replacement, &field));
    assert!(matches!(
        ordinary_materialize(&stream, &replacement, &ty, retention, reader_limits()),
        FlatReaderError::Projection(FlatPoolResourceError::Shape(_))
    ));
    for wrong in [
        FunctionValueType::new(DataType::Float64, true),
        FunctionValueType::new(DataType::Int64, false),
    ] {
        assert!(matches!(
            ordinary_materialize(&stream, &field, &wrong, retention, reader_limits()),
            FlatReaderError::Projection(FlatPoolResourceError::Constant(ConstantError::Invalid(_)))
        ));
    }
}

#[test]
fn actual_reader_checked_geometry_does_not_hide_payload_validation_failures() {
    let mut fixtures = Vec::new();
    let strings = Arc::new(Field::new("source", DataType::Utf8, true));
    let mut offset_input = writer_stream(Arc::new(StringArray::from(vec!["a", "b"])), &strings);
    mutate_buffer(&mut offset_input, 1, 4, &(-1i32).to_le_bytes());
    fixtures.push((
        offset_input,
        Arc::clone(&strings),
        FunctionValueType::new(DataType::Utf8, true),
        false,
    ));
    let mut utf8_input = writer_stream(Arc::new(StringArray::from(vec!["a", "b"])), &strings);
    mutate_buffer(&mut utf8_input, 2, 0, &[0xff]);
    fixtures.push((
        utf8_input,
        strings,
        FunctionValueType::new(DataType::Utf8, true),
        false,
    ));
    let int = Arc::new(Field::new("source", DataType::Int64, true));
    let mut bitmap_input = writer_stream(Arc::new(Int64Array::from(vec![Some(71), None])), &int);
    mutate_buffer(&mut bitmap_input, 0, 0, &[3]);
    fixtures.push((
        bitmap_input,
        int,
        FunctionValueType::new(DataType::Int64, true),
        false,
    ));
    let view = Arc::new(Field::new("source", DataType::Utf8View, true));
    let mut view_input = writer_stream(
        Arc::new(StringViewArray::from(vec!["abcdefghijklm"])),
        &view,
    );
    mutate_buffer(&mut view_input, 1, 4, b"zzzz");
    fixtures.push((
        view_input,
        view,
        FunctionValueType::new(DataType::Utf8View, true),
        false,
    ));
    // Arrow's class-checked decimal array author validates p/s, but not these
    // raw value magnitudes. ConstantPool must reject them after safe reading.
    let decimal = Arc::new(Field::new("source", DataType::Decimal128(1, i8::MIN), true));
    let decimal_array = Decimal128Array::from(vec![123i128])
        .with_precision_and_scale(1, i8::MIN)
        .unwrap();
    fixtures.push((
        writer_stream(Arc::new(decimal_array), &decimal),
        decimal,
        FunctionValueType::new(DataType::Decimal128(1, i8::MIN), true),
        true,
    ));
    for (input, field, ty, owner_error) in fixtures {
        let stream = checked(&input, &field);
        let retention = retained(&input, &field);
        let error = ordinary_materialize(&stream, &field, &ty, retention, reader_limits());
        if owner_error {
            assert!(matches!(
                &error,
                FlatReaderError::Projection(FlatPoolResourceError::Constant(
                    ConstantError::Source(_)
                ))
            ));
        } else {
            assert!(matches!(&error, FlatReaderError::Arrow(_)));
        }
        let mut source: &(dyn std::error::Error + 'static) = &error;
        let mut saw_constant = false;
        loop {
            if let Some(constant) = source.downcast_ref::<ConstantError>() {
                use novarocks_failure_contract::{FailureClass, FailureProjection};
                assert_eq!(constant.failure_fact().class(), FailureClass::Data);
                assert_eq!(
                    constant.failure_fact().reason().domain(),
                    novarocks_failure_contract::domain::CONSTANT.get()
                );
                assert_eq!(constant.failure_fact().reason().code(), 5);
                saw_constant = true;
            }
            if source.is::<arrow::error::ArrowError>() {
                break;
            }
            source = source
                .source()
                .expect("actual reader/constant SDK refusal must remain reachable");
        }
        assert_eq!(saw_constant, owner_error);
    }
}

#[test]
fn actual_reader_compact_null_keeps_final_semantic_nullability_obligation() {
    for nullable in [true, false] {
        let field = Arc::new(Field::new("source", DataType::Null, nullable));
        let ty = FunctionValueType::new(DataType::Null, nullable);
        let input = writer_stream(Arc::new(NullArray::new(3)), &field);
        let stream = checked(&input, &field);
        assert_eq!(stream.geometry().null_count, 3);
        let retention = retained(&input, &field);
        if nullable {
            let control = Control::good();
            let pool =
                materialize(&stream, &field, &ty, retention, reader_limits(), &control).unwrap();
            assert_eq!(pool.resource_facts().rows, 3);
            assert_eq!(pool.resource_facts().retained_buffer_capacity_bytes, 0);
            assert_eq!(pool.array().data_type(), &DataType::Null);
            let trace = control.trace();
            assert_materialize_prefixes(
                &stream,
                &field,
                &ty,
                retention,
                reader_limits(),
                &trace,
                0..trace.len(),
            );
        } else {
            assert!(matches!(
                ordinary_materialize(&stream, &field, &ty, retention, reader_limits()),
                FlatReaderError::Projection(FlatPoolResourceError::Constant(
                    ConstantError::Invalid(_)
                ))
            ));
        }
    }
}

#[test]
fn actual_reader_empty_offsets_and_variadic_empty_headers_have_real_output_owners() {
    let strings = Arc::new(Field::new("source", DataType::Utf8, true));
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let input = raw_stream(&strings, 0, 0, &[(0, 0), (0, 0), (0, 0)], None, &[]);
    let stream = checked(&input, &strings);
    let retention = retained(&input, &strings);
    let facts = stream
        .preflight_reader_resources(&ty, retention, policy(), reader_limits(), &Control::good())
        .unwrap();
    assert_eq!(facts.pool.empty_offset_capacity_bytes, 4);
    let control = Control::good();
    let pool = materialize(&stream, &strings, &ty, retention, reader_limits(), &control).unwrap();
    assert_eq!(pool.array().len(), 0);
    assert!(pool.value(0).is_err());
    let trace = control.trace();
    assert_materialize_prefixes(
        &stream,
        &strings,
        &ty,
        retention,
        reader_limits(),
        &trace,
        0..trace.len(),
    );
    let view = Arc::new(Field::new("source", DataType::Utf8View, true));
    let view_ty = FunctionValueType::new(DataType::Utf8View, true);
    let mut header_bounds = Vec::new();
    for count in [0usize, 32] {
        let input = raw_stream(
            &view,
            0,
            0,
            &vec![(0, 0); count + 2],
            Some(count as i64),
            &[],
        );
        let stream = checked(&input, &view);
        let retention = retained(&input, &view);
        let facts = stream
            .preflight_reader_resources(
                &view_ty,
                retention,
                policy(),
                reader_limits(),
                &Control::good(),
            )
            .unwrap();
        assert_eq!(facts.payload_request_bytes_upper_bound, 0);
        let control = Control::good();
        let pool = materialize(
            &stream,
            &view,
            &view_ty,
            retention,
            reader_limits(),
            &control,
        )
        .unwrap();
        assert_eq!(pool.array().len(), 0);
        assert_eq!(pool.resource_facts().retained_buffer_capacity_bytes, 0);
        header_bounds.push(facts.structural_request_bytes_upper_bound);
        let trace = control.trace();
        assert_materialize_prefixes(
            &stream,
            &view,
            &view_ty,
            retention,
            reader_limits(),
            &trace,
            0..trace.len(),
        );
    }
    assert!(header_bounds[1] > header_bounds[0]);
    // A useful prefix plus a partial typed element remains a pre-reader refusal.
    let bad = raw_stream(&strings, 1, 0, &[(0, 0), (0, 9), (9, 0)], None, &[0; 16]);
    assert!(matches!(
        preflight_flat_constant_stream(
            &bad,
            &strings,
            stream_limits(),
            &verifier(),
            &Control::good()
        ),
        Err(TypeCodecError::InvalidShape(_))
    ));
}

#[test]
fn actual_reader_wide_repeated_view_work_reaches_original_quantum_without_replay() {
    let field = Arc::new(Field::new("source", DataType::Utf8View, true));
    let ty = FunctionValueType::new(DataType::Utf8View, true);
    let input = writer_stream(
        Arc::new(StringViewArray::from(vec!["abcdefghijklm"; 300])),
        &field,
    );
    let stream = checked(&input, &field);
    let retention = retained(&input, &field);
    assert_eq!(stream.geometry().view_validation_bytes, 3900);
    let control = Control::good();
    let pool = materialize(&stream, &field, &ty, retention, reader_limits(), &control).unwrap();
    assert_eq!(
        pool.value(299).unwrap().try_utf8().unwrap(),
        Some("abcdefghijklm")
    );
    let trace = control.trace();
    let mut positions: Vec<_> = trace
        .iter()
        .enumerate()
        .filter_map(|(at, (_, units))| (*units == 256).then_some(at))
        .collect();
    assert!(!positions.is_empty());
    positions.extend([0, trace.len() - 1]);
    positions.sort_unstable();
    positions.dedup();
    assert_materialize_prefixes(
        &stream,
        &field,
        &ty,
        retention,
        reader_limits(),
        &trace,
        positions,
    );
}

#[test]
fn actual_reader_sparse_staging_is_replaced_by_equivalent_shared_metadata() {
    let mut metadata = HashMap::with_capacity(1 << 16);
    metadata.insert("provider_id".to_owned(), "71".to_owned());
    assert!(metadata.capacity() >= 1 << 16);
    let sparse = Arc::new(Field::new("source", DataType::Int64, true).with_metadata(metadata));
    let dense = Arc::new(
        Field::new("source", DataType::Int64, true)
            .with_metadata(HashMap::from([("provider_id".to_owned(), "71".to_owned())])),
    );
    assert_eq!(sparse.as_ref(), dense.as_ref());
    assert!(sparse.metadata().as_arc().is_some());
    assert!(dense.metadata().as_arc().is_some());
    // Distinct conversions own distinct trees; immutable clones share one.
    assert!(!Arc::ptr_eq(
        sparse.metadata().as_arc().unwrap(),
        dense.metadata().as_arc().unwrap(),
    ));
    let shared = sparse.metadata().clone();
    assert!(Arc::ptr_eq(
        sparse.metadata().as_arc().unwrap(),
        shared.as_arc().unwrap(),
    ));
    assert_eq!(shared.get("provider_id").map(String::as_str), Some("71"));
    let generous = FlatReaderProjectionLimits {
        max_cumulative_library_work: 128 * 1024 * 1024,
        ..reader_limits()
    };
    let ty = FunctionValueType::new(DataType::Int64, true);
    let sparse_input = writer_stream(Arc::new(Int64Array::from(vec![71])), &sparse);
    let dense_input = writer_stream(Arc::new(Int64Array::from(vec![71])), &dense);
    let sparse_stream = checked(&sparse_input, &sparse);
    let dense_stream = checked(&dense_input, &dense);
    assert!(std::ptr::eq(sparse_stream.field(), sparse.as_ref()));
    // Both attached sources have the same public BTree/Arc shape and text;
    // their consumed HashMap staging capacity is no longer retained.
    assert_eq!(sparse_input, dense_input);
    let sparse_retention = retained(&sparse_input, &sparse);
    let dense_retention = retained(&dense_input, &dense);
    assert_eq!(sparse_retention, dense_retention);
    let dense_facts = dense_stream
        .preflight_reader_resources(&ty, dense_retention, policy(), generous, &Control::good())
        .unwrap();
    let low = FlatReaderProjectionLimits {
        max_cumulative_library_work: 0,
        ..reader_limits()
    };
    let control = Control::good();
    assert!(matches!(
        sparse_stream.preflight_reader_resources(&ty, sparse_retention, policy(), low, &control),
        Err(FlatPoolResourceError::Shape(TypeCodecError::InvalidShape(
            _
        )))
    ));
    let trace = control.trace();
    assert!(
        trace.len() >= 2,
        "ordinary source-work refusal omitted its tail"
    );
    assert!(
        trace.len() <= 3,
        "zero-work refusal reached the pool delegate"
    );
    for at in 0..trace.len() {
        for cause in CAUSES {
            let refusal = Control::refusing(at, cause.clone());
            assert!(matches!(
                sparse_stream.preflight_reader_resources(
                    &ty, sparse_retention, policy(), low, &refusal,
                ),
                Err(FlatPoolResourceError::Control(actual)) if actual == cause
            ));
            assert_eq!(refusal.trace(), trace[..=at]);
        }
    }
    let facts = sparse_stream
        .preflight_reader_resources(&ty, sparse_retention, policy(), generous, &Control::good())
        .unwrap();
    assert_eq!(facts, dense_facts);
    let pool = materialize(
        &sparse_stream,
        &sparse,
        &ty,
        sparse_retention,
        generous,
        &Control::good(),
    )
    .unwrap();
    assert!(std::ptr::eq(pool.field(), sparse.as_ref()));
    assert_eq!(pool.field().metadata().len(), 1);
    assert_eq!(pool.value(0).unwrap().try_i64().unwrap(), Some(71));
}

#[test]
fn actual_reader_converted_metadata_keeps_original_source_invoice_authority() {
    let mut metadata = HashMap::with_capacity(1 << 16);
    for ordinal in 0..(1 << 16) {
        metadata.insert(format!("provider_{ordinal}"), "71".to_owned());
    }
    let staging_capacity = metadata.capacity();
    for ordinal in 1..(1 << 16) {
        assert_eq!(
            metadata.remove(&format!("provider_{ordinal}")).as_deref(),
            Some("71")
        );
    }
    assert_eq!(metadata.len(), 1);
    assert!(metadata.capacity() <= staging_capacity);
    let field = Arc::new(Field::new("source", DataType::Int64, true).with_metadata(metadata));
    let ty = FunctionValueType::new(DataType::Int64, true);
    let input = writer_stream(Arc::new(Int64Array::from(vec![71])), &field);
    let stream = checked(&input, &field);
    assert!(std::ptr::eq(stream.field(), field.as_ref()));
    assert!(field.metadata().as_arc().is_some());
    assert_eq!(
        field.metadata().get("provider_0").map(String::as_str),
        Some("71")
    );
    // Historical staging buckets have exited; only attached BTree/Arc/text
    // backing belongs in the original source allowance.
    let invoice = retained(&input, &field);
    let delta = 65536;
    let enlarged_invoice = invoice + delta;
    let generous = FlatReaderProjectionLimits {
        max_cumulative_library_work: 128 * 1024 * 1024,
        ..reader_limits()
    };
    let original = stream
        .preflight_reader_resources(&ty, invoice, policy(), generous, &Control::good())
        .unwrap();
    let enlarged = stream
        .preflight_reader_resources(&ty, enlarged_invoice, policy(), generous, &Control::good())
        .unwrap();
    // Two complete source iterations and four tag lookups consume the same
    // trusted source backing bound. Only its invoice changes in this pair.
    assert_eq!(
        enlarged.cumulative_library_work_upper_bound - original.cumulative_library_work_upper_bound,
        6 * delta
    );
    assert_eq!(
        enlarged.coexisting_source_and_request_bytes_upper_bound
            - original.coexisting_source_and_request_bytes_upper_bound,
        delta
    );
    assert_eq!(
        enlarged.new_allocation_request_bytes_upper_bound,
        original.new_allocation_request_bytes_upper_bound
    );
    let low = FlatReaderProjectionLimits {
        max_cumulative_library_work: original.cumulative_library_work_upper_bound,
        ..generous
    };
    let control = Control::good();
    assert!(matches!(
        stream.preflight_reader_resources(&ty, enlarged_invoice, policy(), low, &control),
        Err(FlatPoolResourceError::Shape(TypeCodecError::InvalidShape(
            _
        )))
    ));
    let trace = control.trace();
    assert!(
        trace.len() >= 2,
        "ordinary backing refusal omitted its tail"
    );
    assert!(
        trace.len() <= 3,
        "backing refusal reached the pool delegate"
    );
    for at in 0..trace.len() {
        for cause in CAUSES {
            let refusal = Control::refusing(at, cause.clone());
            assert!(matches!(
                stream.preflight_reader_resources(&ty, enlarged_invoice, policy(), low, &refusal),
                Err(FlatPoolResourceError::Control(actual)) if actual == cause
            ));
            assert_eq!(refusal.trace(), trace[..=at]);
        }
    }
    let pool = materialize(
        &stream,
        &field,
        &ty,
        enlarged_invoice,
        generous,
        &Control::good(),
    )
    .unwrap();
    assert!(std::ptr::eq(pool.field(), field.as_ref()));
    assert_eq!(
        pool.field()
            .metadata()
            .get("provider_0")
            .map(String::as_str),
        Some("71")
    );
    assert_eq!(pool.value(0).unwrap().try_i64().unwrap(), Some(71));
}

mod borrowed_tests {
    include!("reader_tests/borrowed_tests.rs");
}

#[path = "prepared_tests.rs"]
mod prepared_tests;

#[path = "reader_tests/owner_ports.rs"]
mod owner_ports;
