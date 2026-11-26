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
    ipc_recursive_batch_v2::RecursiveBatchProjectionLimits,
    ipc_schema_v2::IpcSchemaProjectionLimits,
    physical_type_v2::{
        PackageTypeProjectionLimits, TypeProjectionLimits, decode_package_type_table_observed,
    },
};
use arrow::{
    array::*,
    datatypes::{Field, Schema, TimeUnit},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use novarocks_type_contract::{FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType};
use std::{collections::HashMap, sync::Mutex};

const SOURCE: usize = 1024 * 1024;
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
    fn refusing(phase: CompilePhase, at: usize, cause: CompileControlError) -> Self {
        Self {
            phase,
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
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 8 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 32 * 1024 * 1024,
        max_library_validation_bytes: 32 * 1024 * 1024,
    }
}
fn schema_limits() -> IpcSchemaProjectionLimits {
    IpcSchemaProjectionLimits {
        max_field_occurrences: 4096,
        max_type_occurrences: 4096,
        max_string_bytes: 65536,
        max_flatbuffer_bytes: 4 * 1024 * 1024,
    }
}
fn flat_write() -> FlatPoolWriteLimits {
    FlatPoolWriteLimits {
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_body_bytes: 8 * 1024 * 1024,
        max_encoded_stream_bytes: 16 * 1024 * 1024,
        schema: schema_limits(),
        max_new_allocation_request_bytes: 128 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
        max_cumulative_library_work: 512 * 1024 * 1024,
    }
}
fn write_limits() -> ConstantWriteProjectionLimits {
    ConstantWriteProjectionLimits {
        flat: flat_write(),
        recursive: RecursivePoolWriteLimits {
            flat: FlatPoolWriteLimits {
                max_cumulative_library_work: 1024 * 1024 * 1024,
                ..flat_write()
            },
            max_field_nodes: 4096,
            max_total_rows: 65536,
        },
    }
}
fn batch_limits() -> FlatBatchProjectionLimits {
    FlatBatchProjectionLimits {
        max_metadata_bytes: 1024 * 1024,
        max_body_bytes: 8 * 1024 * 1024,
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_view_validation_bytes: 8 * 1024 * 1024,
    }
}
fn decode_limits() -> ConstantDecodeProjectionLimits {
    ConstantDecodeProjectionLimits {
        flat_stream: FlatStreamProjectionLimits {
            max_input_bytes: 16 * 1024 * 1024,
            schema: schema_limits(),
            batch: batch_limits(),
        },
        flat_reader: FlatReaderProjectionLimits {
            max_new_allocation_request_bytes: 128 * 1024 * 1024,
            max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
            max_cumulative_library_work: 512 * 1024 * 1024,
        },
        recursive_stream: RecursiveStreamProjectionLimits {
            max_input_bytes: 16 * 1024 * 1024,
            schema: schema_limits(),
            batch: RecursiveBatchProjectionLimits {
                flat: batch_limits(),
                max_field_nodes: 4096,
                max_total_rows: 65536,
                max_geometry_request_bytes: 1024 * 1024,
            },
        },
        recursive_reader: RecursiveReaderProjectionLimits {
            max_new_allocation_request_bytes: 128 * 1024 * 1024,
            max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
            max_cumulative_library_work: 1024 * 1024 * 1024,
        },
    }
}
fn verifier() -> VerifierOptions {
    VerifierOptions {
        max_depth: 67,
        max_tables: 65536,
        max_apparent_size: 16 * 1024 * 1024,
        ignore_missing_null_terminator: false,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 16384,
        max_expanded_nodes: 65536,
        max_string_bytes: 65536,
    }
}
fn types(
    field: Arc<Field>,
    ty: FunctionValueType,
    value_id: u32,
    field_id: u32,
) -> DecodedTypeTable {
    types_many(&[(value_id, ty)], &[(field_id, field)])
}
fn fixture_type_limits() -> PackageTypeProjectionLimits {
    let old = type_limits();
    let reader = decode_limits().recursive_reader;
    PackageTypeProjectionLimits {
        max_definitions: old.max_definitions,
        max_expanded_nodes: old.max_expanded_nodes,
        max_string_bytes: old.max_string_bytes,
        max_allocation_requests: reader.max_new_allocation_request_bytes,
        max_allocation_request_bytes: reader.max_new_allocation_request_bytes,
        max_coexisting_source_and_request_bytes: reader.max_coexisting_source_and_request_bytes,
        max_work: reader.max_cumulative_library_work,
    }
}
fn types_many(
    values: &[(u32, FunctionValueType)],
    fields: &[(u32, Arc<Field>)],
) -> DecodedTypeTable {
    let value_refs = values
        .iter()
        .map(|(id, value)| (*id, value))
        .collect::<Vec<_>>();
    let field_refs = fields
        .iter()
        .map(|(id, field)| (*id, field))
        .collect::<Vec<_>>();
    let encoded = encoded_fixture_types(&value_refs, &field_refs);
    decode_fixture_types(&encoded, fields)
}
fn encoded_fixture_types<'s>(
    values: &'s [(u32, &'s FunctionValueType)],
    fields: &'s [(u32, &'s Arc<Field>)],
) -> crate::physical_type_v2::EncodedTypeTable<'s> {
    let control = Control::good(CompilePhase::Encode);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let table = crate::physical_type_v2::encode_borrowed_type_namespaces_pure_in(
        values,
        &[],
        fields,
        type_limits(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    table
}
fn decode_fixture_types(
    encoded: &crate::physical_type_v2::EncodedTypeTable<'_>,
    fields: &[(u32, Arc<Field>)],
) -> DecodedTypeTable {
    // These are the actual declared source roots of this type-only fixture.
    // IPC materialization uses its original records and the sole reader later.
    let package = wire::FragmentPackage {
        types: Some(encoded.as_wire().clone()),
        source_fields: Some(
            encoded
                .as_source_wire()
                .expect("actual source emission")
                .clone(),
        ),
        constants: fields
            .iter()
            .map(|(id, _)| wire::IpcConstantPool {
                source_field_id: Some(*id),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let control = Control::good(CompilePhase::Decode);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let result = decode_package_type_table_observed(
        &package,
        SOURCE,
        fixture_type_limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    result
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
fn pool(array: ArrayRef) -> ConstantPool {
    let field = Arc::new(
        Field::new("original", array.data_type().clone(), true).with_metadata(HashMap::from([(
            "provider".into(),
            "frozen-original".into(),
        )])),
    );
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    pool_with(array, field, ty)
}
fn encode(pool: &ConstantPool, id: u32, value_id: u32, field_id: u32) -> wire::IpcConstantPool {
    // This explicit fixture invoice covers the complete tiny pool/Field/FVT,
    // DTO capacities and type-table owners. It is not inferred from IPC length.
    encode_constant_record(
        ConstantPoolId::new(id),
        value_id,
        field_id,
        pool,
        SOURCE,
        write_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap()
}
fn decode(
    record: &wire::IpcConstantPool,
    types: &DecodedTypeTable,
) -> (ConstantPoolId, ConstantPool) {
    decode_constant_record(
        record,
        types,
        SOURCE,
        policy(),
        decode_limits(),
        &verifier(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap()
}
fn roundtrip(pool: &ConstantPool, id: u32, value_id: u32, field_id: u32) -> ConstantPool {
    let types = types(
        Arc::clone(pool.field_ref()),
        pool.value_type().clone(),
        value_id,
        field_id,
    );
    let record = encode(pool, id, value_id, field_id);
    assert_eq!(record.id, id);
    assert_eq!(record.value_type_id, Some(value_id));
    assert_eq!(record.source_field_id, Some(field_id));
    assert_eq!(
        record.compression,
        wire::IpcCompression::Uncompressed as i32
    );
    let (decoded_id, result) = decode(&record, &types);
    assert_eq!(decoded_id.get(), id);
    assert!(Arc::ptr_eq(
        result.field_ref(),
        types.source_field(field_id).unwrap()
    ));
    assert_eq!(result.array().len(), pool.array().len());
    for ordinal in 0..pool.array().len() {
        assert!(
            pool.value(ordinal as u32)
                .unwrap()
                .equals_observed(
                    &result.value(ordinal as u32).unwrap(),
                    CompilePhase::Decode,
                    &Control::good(CompilePhase::Decode),
                )
                .unwrap(),
            "selected ordinal {ordinal}"
        );
    }
    result
}

#[test]
fn record_flat_bits_sparse_ids_and_original_unlabelled_logical_field_roundtrip() {
    let bits = [0x7fc1_2345, 0x8000_0000, 0x0000_0000, 0x3f80_0000];
    let array: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(f32::from_bits(bits[0])),
        Some(f32::from_bits(bits[1])),
        None,
        Some(f32::from_bits(bits[3])),
    ]));
    let original = pool(array);
    let output = roundtrip(&original, u32::MAX, u32::MAX, 0);
    let actual = output
        .array()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(actual.value(0).to_bits(), bits[0]);
    assert_eq!(actual.value(1).to_bits(), bits[1]);
    assert!(actual.is_null(2));
    assert_eq!(actual.value(3).to_bits(), bits[3]);
    let array: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(b"\x01\x42\x01".as_slice()),
        None,
        Some(b"\x01\x20".as_slice()),
    ]));
    let field = Arc::new(
        Field::new("json", DataType::LargeBinary, true).with_metadata(HashMap::from([(
            "provider".into(),
            "not-a-logical-tag".into(),
        )])),
    );
    let ty = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let original = pool_with(array, field, ty);
    let output = roundtrip(&original, 0, 0, u32::MAX);
    assert_eq!(output.value_type().logical_type(), ValueLogicalType::Json);
    assert!(!output.field().metadata().contains_key(NR_LOGICAL_TYPE_KEY));
    assert_eq!(
        output.field().metadata().get("provider").unwrap(),
        "not-a-logical-tag"
    );
    let original = pool(Arc::new(NullArray::new(3)));
    let output = roundtrip(&original, 0, u32::MAX, u32::MAX);
    assert_eq!(output.field().data_type(), &DataType::Null);
    assert!(
        output
            .value(2)
            .unwrap()
            .is_null_observed(CompilePhase::Decode, &Control::good(CompilePhase::Decode),)
            .unwrap()
    );
}

#[test]
fn record_timestamp_none_and_present_empty_zone_remain_distinct() {
    for zone in [None, Some("")] {
        let array: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![Some(-1), None, Some(i64::MAX)])
                .with_timezone_opt(zone),
        );
        let original = pool(array);
        let result = roundtrip(&original, 0, 0, 0);
        assert_eq!(
            result.field().data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, zone.map(Into::into))
        );
    }
}

fn nested_arrays() -> Vec<ArrayRef> {
    let text = Arc::new(
        Field::new("text", DataType::Utf8, true)
            .with_metadata(HashMap::from([("nested-source".into(), "retained".into())])),
    );
    let values: ArrayRef = Arc::new(StructArray::new(
        vec![text, Arc::new(Field::new("number", DataType::Int32, true))].into(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("unused"),
                Some("a"),
                None,
                Some("z"),
            ])),
            Arc::new(Int32Array::from(vec![Some(0), None, Some(2), Some(3)])),
        ],
        None,
    ));
    let list: ArrayRef = Arc::new(
        ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 4])),
            Arc::clone(&values),
            Some(NullBuffer::from(vec![true, true, false])),
        )
        .slice(1, 2),
    );
    let large: ArrayRef = Arc::new(
        LargeListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 1, 3, 4])),
            values,
            None,
        )
        .slice(1, 2),
    );
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec!["unused", "a", "b", "c"])),
            Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)])),
        ],
        None,
    );
    let map: ArrayRef = Arc::new(
        MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 4])),
            entries,
            None,
            false,
        )
        .slice(1, 2),
    );
    let structure: ArrayRef = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("list", large.data_type().clone(), true)),
            Arc::new(Field::new("flag", DataType::Boolean, true)),
        ]
        .into(),
        vec![
            Arc::clone(&large),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
        ],
        None,
    ));
    vec![list, large, map, structure]
}
#[test]
fn record_all_four_recursive_roots_preserve_slices_full_fields_and_selected_ordinals() {
    for array in nested_arrays() {
        let original = pool(array);
        let result = roundtrip(&original, u32::MAX, 0, u32::MAX);
        assert_eq!(result.array().len(), 2);
        assert!(
            original
                .value(1)
                .unwrap()
                .equals_observed(
                    &result.value(1).unwrap(),
                    CompilePhase::Decode,
                    &Control::good(CompilePhase::Decode)
                )
                .unwrap()
        );
    }
}

#[test]
fn record_headers_reject_absence_unknown_compression_and_namespaces_before_ipc() {
    let original = pool(Arc::new(Int32Array::from(vec![1])));
    let types = types(
        Arc::clone(original.field_ref()),
        original.value_type().clone(),
        0,
        u32::MAX,
    );
    let valid = encode(&original, 0, 0, u32::MAX);
    for (value_id, field_id, compression) in [
        (
            None,
            Some(u32::MAX),
            wire::IpcCompression::Uncompressed as i32,
        ),
        (Some(0), None, wire::IpcCompression::Uncompressed as i32),
        (
            Some(1),
            Some(u32::MAX),
            wire::IpcCompression::Uncompressed as i32,
        ),
        (Some(0), Some(1), wire::IpcCompression::Uncompressed as i32),
        (Some(0), Some(u32::MAX), 0),
        (Some(0), Some(u32::MAX), i32::MAX),
    ] {
        let record = wire::IpcConstantPool {
            value_type_id: value_id,
            source_field_id: field_id,
            compression,
            arrow_ipc: vec![0xff],
            ..valid.clone()
        };
        let control = Control::good(CompilePhase::Decode);
        assert!(matches!(
            decode_constant_record(
                &record,
                &types,
                SOURCE,
                policy(),
                decode_limits(),
                &verifier(),
                &control
            ),
            Err(PhysicalConstantCodecError::InvalidShape(_))
        ));
        assert_eq!(control.trace().len(), 2); // Header owner entry/tail; IPC not entered.
    }
    let mut spare = valid;
    spare.arrow_ipc = Vec::with_capacity(SOURCE * 2);
    spare.arrow_ipc.push(0xff);
    let control = Control::good(CompilePhase::Decode);
    assert!(matches!(
        decode_constant_record(
            &spare,
            &types,
            SOURCE,
            policy(),
            decode_limits(),
            &verifier(),
            &control
        ),
        Err(PhysicalConstantCodecError::InvalidShape(
            "constant record source invoice omits original IPC backing"
        ))
    ));
    assert_eq!(control.trace().len(), 2);
}

#[test]
fn record_field_value_pair_and_ipc_schema_mismatches_do_not_retag_or_reauthor() {
    let original = pool(Arc::new(Int32Array::from(vec![Some(1), None])));
    let record = encode(&original, 0, 0, 0);
    for (field, ty) in [
        (
            Arc::clone(original.field_ref()),
            FunctionValueType::new(DataType::Int64, true),
        ),
        (
            Arc::clone(original.field_ref()),
            FunctionValueType::new(DataType::Int32, false),
        ),
        (
            Arc::new(original.field().clone().with_name("foreign")),
            original.value_type().clone(),
        ),
        (
            Arc::new(
                original
                    .field()
                    .clone()
                    .with_metadata(HashMap::from([("provider".into(), "foreign".into())])),
            ),
            original.value_type().clone(),
        ),
    ] {
        let types = types(field, ty, 0, 0);
        assert!(
            decode_constant_record(
                &record,
                &types,
                SOURCE,
                policy(),
                decode_limits(),
                &verifier(),
                &Control::good(CompilePhase::Decode)
            )
            .is_err()
        );
    }
    let array: ArrayRef = Arc::new(LargeBinaryArray::from(vec![b"\x01\x20".as_slice()]));
    let field = Arc::new(
        Field::new("json", DataType::LargeBinary, true)
            .with_metadata(HashMap::from([(NR_LOGICAL_TYPE_KEY.into(), "json".into())])),
    );
    let original = pool_with(
        array,
        Arc::clone(&field),
        FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            ValueLogicalType::Json,
        )
        .unwrap(),
    );
    let record = encode(&original, 0, 0, 0);
    let plain_types = types(
        Arc::clone(&field),
        FunctionValueType::new(DataType::LargeBinary, true),
        0,
        0,
    );
    let (_, plain) = decode_constant_record(
        &record,
        &plain_types,
        SOURCE,
        policy(),
        decode_limits(),
        &verifier(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    assert_eq!(
        plain.value_type().logical_type(),
        ValueLogicalType::Physical
    );
    assert_eq!(
        plain
            .field()
            .metadata()
            .get(NR_LOGICAL_TYPE_KEY)
            .map(String::as_str),
        Some("json")
    );
    assert_eq!(plain.value_type().data_type(), &DataType::LargeBinary);
    let json_types = types(field, original.value_type().clone(), 0, 0);
    let (_, json) = decode_constant_record(
        &record,
        &json_types,
        SOURCE,
        policy(),
        decode_limits(),
        &verifier(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    assert_eq!(json.value_type(), original.value_type());
    assert_eq!(plain.array().to_data(), json.array().to_data());
}

#[test]
fn record_uses_only_explicit_flat_or_recursive_profiles_without_failure_fallback() {
    let flat = pool(Arc::new(Int32Array::from(vec![1])));
    let recursive = pool(nested_arrays().remove(0));
    for original in [&flat, &recursive] {
        let is_recursive = super::recursive(original.field().data_type());
        let types = types(
            Arc::clone(original.field_ref()),
            original.value_type().clone(),
            0,
            0,
        );
        let mut limits = write_limits();
        if is_recursive {
            limits.flat.max_rows = 0;
        } else {
            limits.recursive.flat.max_rows = 0;
        }
        let record = encode_constant_record(
            ConstantPoolId::new(0),
            0,
            0,
            original,
            SOURCE,
            limits,
            &Control::good(CompilePhase::Encode),
        )
        .unwrap();
        let mut denied_reader = decode_limits();
        if is_recursive {
            denied_reader
                .recursive_reader
                .max_new_allocation_request_bytes = 0;
        } else {
            denied_reader.flat_reader.max_new_allocation_request_bytes = 0;
        }
        assert!(
            decode_constant_record(
                &record,
                &types,
                SOURCE,
                policy(),
                denied_reader,
                &verifier(),
                &Control::good(CompilePhase::Decode),
            )
            .is_err()
        );
        let mut decode_profile = decode_limits();
        if is_recursive {
            decode_profile.flat_stream.max_input_bytes = 0;
            decode_profile.flat_reader.max_new_allocation_request_bytes = 0;
        } else {
            decode_profile.recursive_stream.max_input_bytes = 0;
            decode_profile
                .recursive_reader
                .max_new_allocation_request_bytes = 0;
        }
        decode_constant_record(
            &record,
            &types,
            SOURCE,
            policy(),
            decode_profile,
            &verifier(),
            &Control::good(CompilePhase::Decode),
        )
        .unwrap();
        if is_recursive {
            limits.recursive.flat.max_rows = 0;
        } else {
            limits.flat.max_rows = 0;
        }
        assert!(
            encode_constant_record(
                ConstantPoolId::new(0),
                0,
                0,
                original,
                SOURCE,
                limits,
                &Control::good(CompilePhase::Encode)
            )
            .is_err()
        );
        if is_recursive {
            decode_profile.recursive_stream.max_input_bytes = 0;
        } else {
            decode_profile.flat_stream.max_input_bytes = 0;
        }
        assert!(
            decode_constant_record(
                &record,
                &types,
                SOURCE,
                policy(),
                decode_profile,
                &verifier(),
                &Control::good(CompilePhase::Decode)
            )
            .is_err()
        );
    }
}

#[test]
fn record_dictionary_and_fixed_list_are_explicitly_unsupported_in_both_routes() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<arrow::datatypes::Int8Type>::try_new(
            Int8Array::from(vec![0i8, 1]),
            Arc::new(StringArray::from(vec!["a", "b"])),
        )
        .unwrap(),
    );
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        1,
        Arc::new(Int32Array::from(vec![1, 2])),
        None,
    ));
    for array in [dictionary, fixed] {
        let original = pool(array);
        let types = types(
            Arc::clone(original.field_ref()),
            original.value_type().clone(),
            0,
            0,
        );
        assert!(matches!(
            encode_constant_record(
                ConstantPoolId::new(0),
                0,
                0,
                &original,
                SOURCE,
                write_limits(),
                &Control::good(CompilePhase::Encode)
            ),
            Err(PhysicalConstantCodecError::Type(_))
        ));
        let schema = Arc::new(Schema::new(vec![Arc::clone(original.field_ref())]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(original.array())]).unwrap();
        let mut arrow_ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut arrow_ipc, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let record = wire::IpcConstantPool {
            id: 0,
            value_type_id: Some(0),
            source_field_id: Some(0),
            compression: wire::IpcCompression::Uncompressed as i32,
            arrow_ipc,
        };
        assert!(
            decode_constant_record(
                &record,
                &types,
                SOURCE,
                policy(),
                decode_limits(),
                &verifier(),
                &Control::good(CompilePhase::Decode)
            )
            .is_err()
        );
    }
}

fn every_prefix<T>(
    phase: CompilePhase,
    call: impl Fn(&Control) -> Result<T, PhysicalConstantCodecError>,
    success: bool,
    positive_quantum: bool,
) {
    let good = Control::good(phase);
    let result = call(&good);
    assert_eq!(result.is_ok(), success);
    let trace = good.trace();
    assert_eq!(trace.first(), Some(&(phase, 0)));
    if positive_quantum {
        assert!(trace.iter().any(|(_, units)| *units == 256));
    }
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::refusing(phase, at, cause.clone());
            assert!(
                matches!(call(&control), Err(PhysicalConstantCodecError::Control(actual))
                if actual == cause),
                "phase {phase:?}, callback {at}, cause {cause:?}"
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
#[test]
fn record_every_success_and_ordinary_failure_callback_preserves_original_three_causes() {
    // Leave room in the explicit source invoice for builder spare capacity,
    // the encoded DTO, and both original Field/FVT/type-table owners.
    let long = "x".repeat(256 * 1024);
    let original = pool(Arc::new(StringArray::from(vec![
        Some(long.as_str()),
        None,
        Some("tail"),
    ])));
    let types = types(
        Arc::clone(original.field_ref()),
        original.value_type().clone(),
        0,
        0,
    );
    let record = encode(&original, 0, 0, 0);
    every_prefix(
        CompilePhase::Encode,
        |control| {
            encode_constant_record(
                ConstantPoolId::new(0),
                0,
                0,
                &original,
                SOURCE,
                write_limits(),
                control,
            )
        },
        true,
        true,
    );
    every_prefix(
        CompilePhase::Decode,
        |control| {
            decode_constant_record(
                &record,
                &types,
                SOURCE,
                policy(),
                decode_limits(),
                &verifier(),
                control,
            )
        },
        true,
        true,
    );
    let mut small = write_limits();
    small.flat.max_rows = 0;
    every_prefix(
        CompilePhase::Encode,
        |control| {
            encode_constant_record(
                ConstantPoolId::new(0),
                0,
                0,
                &original,
                SOURCE,
                small,
                control,
            )
        },
        false,
        false,
    );
    let mut missing = record.clone();
    missing.value_type_id = Some(1);
    every_prefix(
        CompilePhase::Decode,
        |control| {
            decode_constant_record(
                &missing,
                &types,
                SOURCE,
                policy(),
                decode_limits(),
                &verifier(),
                control,
            )
        },
        false,
        false,
    );
}

mod namespace_tests {
    use super::*;
    use crate::physical_constant_v2::namespace::*;
    include!("namespace/tests.rs");
}

#[test]
fn record_prepared_writer_uses_same_original_control_source_and_admitted_requests() {
    let mut arrays = nested_arrays();
    arrays.push(Arc::new(Float32Array::from(vec![
        Some(f32::from_bits(0x7fc0_1234)),
        None,
        Some(-0.0),
    ])));
    for array in arrays {
        let original = pool(array);
        let identity = original.backing_identity();
        let control = Control::good(CompilePhase::Encode);
        let prepared = prepare_constant_record_write(
            ConstantPoolId::new(u32::MAX),
            0,
            u32::MAX,
            &original,
            SOURCE,
            write_limits(),
            &control,
        )
        .unwrap();
        let facts = *prepared.facts();
        assert_eq!(facts.source_retained_bytes, SOURCE);
        assert!(facts.new_allocation_request_bytes_upper_bound > 0);
        let before_emission = control.trace().len();
        let record = prepared.emit().unwrap();
        assert!(control.trace().len() > before_emission);
        assert!(record.arrow_ipc.capacity() <= facts.encoded_stream_bytes_upper_bound);
        assert_eq!(original.backing_identity(), identity);
        let types = types(
            Arc::clone(original.field_ref()),
            original.value_type().clone(),
            0,
            u32::MAX,
        );
        let (id, decoded) = decode(&record, &types);
        assert_eq!(id, ConstantPoolId::new(u32::MAX));
        for ordinal in 0..original.array().len() as u32 {
            assert!(
                original
                    .value(ordinal)
                    .unwrap()
                    .equals_observed(
                        &decoded.value(ordinal).unwrap(),
                        CompilePhase::Decode,
                        &Control::good(CompilePhase::Decode)
                    )
                    .unwrap()
            );
        }
    }
}

mod write_namespace_tests {
    use super::*;
    include!("write_namespace_tests.rs");
}

mod caller_owned_tests;
