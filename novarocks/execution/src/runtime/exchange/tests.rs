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

// These fixtures explicitly declare ordinary physical values. Arrow
// annotations are source facts and do not author any nominal domain.
pub(super) fn plain_declaration(schema: &ChunkSchema) -> ChunkSchemaRef {
    use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
    let slots = schema
        .slots()
        .iter()
        .map(|slot| {
            let value = FunctionValueType::new(slot.data_type().clone(), slot.nullable());
            ChunkSlotSchema::try_new_typed(
                slot.slot_id(),
                Arc::new(Field::new(
                    slot.name(),
                    value.data_type().clone(),
                    value.nullable(),
                )),
                value.typed_binding().clone(),
                slot.unique_id(),
                ValueTypeStorageLifetime::pure(),
            )
            .expect("explicit plain fixture declaration")
        })
        .collect();
    Arc::new(ChunkSchema::try_new_typed(slots, ValueTypeStorageLifetime::pure()).unwrap())
}

use arrow::array::{
    Array, ArrayRef, Decimal128Array, DictionaryArray, Int32Array, Int64Array, MapArray,
    StringArray, StructArray, make_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use arrow::record_batch::RecordBatchOptions;
use std::collections::HashMap;
use std::sync::Arc;

use super::result_ipc_preflight::{
    ARROW_IPC_CONTINUATION_MARKER, MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES,
    MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS,
};
use super::{
    BoundedPayloadWriter, ExchangeKey, ExchangePopResult, ExchangeSenderIdentity, ExchangeWireMeta,
    ExecutionExchangeRegistry, cancel_exchange_key, decode_chunks, decode_chunks_for_sender,
    decode_root_result_chunks, encode_chunks, encode_chunks_bounded,
    encode_exchange_payload_envelope, preflight_typed_root_result_decode,
    register_expected_chunk_schema,
};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks_types::{SlotId, UniqueId};
use std::io::Write as _;

use crate::runtime::fragment::io::exchange_unit::{
    ExchangeAdmissionVerdict, ExchangeDemandEntry, ExchangeDemandOperation, ExchangeUnitDemand,
    ExchangeUnitItem, ExchangeUnitReceiverFrame,
};
use crate::runtime::runtime_state::RuntimeErrorState;
use crate::runtime::task_control::test_support::Owner;
use bytes::Bytes;
use novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitSequence;
use novarocks_execution_contract::{
    ExchangeSource, ExchangeStreamIdentity, FragmentNodeId, TaskConclusionPort, TaskFailurePhase,
    TaskIdentity,
};
use novarocks_types::{StageId, TaskId};

/// Explicit fixture declarations author these values; production never learns
/// semantics from a first payload or Arrow annotations.
pub(super) fn online_unit_as(chunk: &Chunk, expected: &ChunkSchemaRef) -> Vec<u8> {
    let factory = crate::runtime::fragment::exchange::test_receiver_creation_factory();
    let owner = factory.begin_typed_construction(expected).unwrap();
    let wire = ExchangeWireMeta {
        slot_ids_by_index: chunk
            .chunk_schema()
            .slots()
            .iter()
            .map(|slot| slot.slot_id())
            .collect(),
    };
    let chunk = super::materialize_chunk_for_wire_meta(
        expected,
        chunk.record_batch().clone(),
        &wire,
        false,
        super::DecodeCarrierContract::Exchange,
        &super::ExchangeHeaderCreation::Runtime(owner.as_ref()),
    )
    .unwrap();
    let recipe = super::online_unit::prepare_recipe(expected, &[], factory.as_ref()).unwrap();
    let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(Arc::new(
        RuntimeErrorState::default(),
    ));
    super::online_unit::encode(&recipe, &chunk, factory.as_ref(), &control).unwrap()
}
pub(super) fn online_unit(chunks: &[Chunk], _: bool) -> Result<Vec<u8>, super::ExchangeFailure> {
    assert_eq!(chunks.len(), 1, "one complete batch per online Unit");
    let schema = if chunks[0].chunk_schema().is_typed() {
        chunks[0].chunk_schema_ref()
    } else {
        plain_declaration(chunks[0].chunk_schema())
    };
    Ok(online_unit_as(&chunks[0], &schema))
}

/// One explicit receiver instance: no global registry, inferred stream or raw
/// queue publication. Every data item goes through its demand and decode lease.
struct UnitFixture {
    registry: Arc<ExecutionExchangeRegistry>,
    key: ExchangeKey,
    streams: Vec<ExchangeStreamIdentity>,
    task: Arc<Owner>,
}
impl UnitFixture {
    fn new(schema: ChunkSchemaRef, senders: usize) -> Self {
        Self::with_factory(
            schema,
            senders,
            crate::runtime::fragment::exchange::test_receiver_creation_factory(),
        )
    }
    fn with_factory(
        schema: ChunkSchemaRef,
        senders: usize,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> Self {
        // One first unit per source plus the metadata-conflict probe can be
        // live together. Keep this fixture independent of default calibration.
        let slots = senders + 1;
        let limits = crate::runtime::execution_runtime::ExchangeFlowLimits::try_new(
            slots,
            slots,
            0,
            2,
            std::time::Duration::from_secs(30),
        )
        .unwrap();
        let registry = Arc::new(ExecutionExchangeRegistry::with_flow_limits(limits));
        let key = ExchangeKey {
            finst_id_hi: 88001,
            finst_id_lo: 88002,
            node_id: 3,
        };
        registry
            .try_register_expected_chunk_schema_with_binding(
                key,
                senders,
                schema,
                super::ExchangeColumnBinding::Positional,
                factory,
            )
            .unwrap();
        Self::bind_registered(registry, key, senders)
    }
    fn bind_registered(
        registry: Arc<ExecutionExchangeRegistry>,
        key: ExchangeKey,
        senders: usize,
    ) -> Self {
        let task = Owner::new();
        let destination = task.identity();
        let streams = (0..senders)
            .map(|ordinal| {
                let source = TaskIdentity::new(
                    destination.query_execution_id(),
                    StageId::new(2).unwrap(),
                    TaskId::new(ordinal as u32 + 1).unwrap(),
                    destination.backend_process_id(),
                );
                ExchangeStreamIdentity::try_new(
                    ExchangeSource::new(
                        source,
                        UniqueId::new(88003, ordinal as i64 + 1),
                        ordinal as u32,
                    ),
                    destination,
                    UniqueId::new(key.finst_id_hi, key.finst_id_lo),
                    FragmentNodeId::new(key.node_id),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        registry
            .bind_unit_receiver(
                key,
                streams.clone(),
                Arc::new(RuntimeErrorState::for_task(Arc::new(
                    task.view(TaskFailurePhase::Execution),
                ))),
                novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64,
            )
            .unwrap();
        Self {
            registry,
            key,
            streams,
            task,
        }
    }
    fn frame(
        &self,
        sender: usize,
        sequence: u64,
        payload: Vec<u8>,
        end: bool,
    ) -> ExchangeUnitReceiverFrame {
        let payload = Bytes::from(payload);
        ExchangeUnitReceiverFrame {
            stream: self.streams[sender],
            sender_count: self.streams.len() as u32,
            sender_id: sender as i32,
            backend_number: 0,
            item: ExchangeUnitItem::Unit {
                demand: ExchangeUnitDemand {
                    sequence: ExchangeUnitSequence::try_new(sequence).unwrap(),
                    wire_bytes: payload.len() as u64,
                },
                end_after_unit: end,
            },
            payload,
            next_demand: None,
        }
    }
    fn end(&self, sender: usize, sequence: u64) -> ExchangeUnitReceiverFrame {
        ExchangeUnitReceiverFrame {
            stream: self.streams[sender],
            sender_count: self.streams.len() as u32,
            sender_id: sender as i32,
            backend_number: 0,
            item: ExchangeUnitItem::End(ExchangeUnitSequence::try_new(sequence).unwrap()),
            payload: Bytes::new(),
            next_demand: None,
        }
    }
    fn register(&self, frame: &ExchangeUnitReceiverFrame) {
        let ExchangeUnitItem::Unit { demand, .. } = frame.item else {
            panic!("data demand")
        };
        self.registry
            .register_unit_demand(
                self.key,
                ExchangeDemandEntry {
                    stream: frame.stream,
                    operation: ExchangeDemandOperation::Register(demand),
                },
            )
            .unwrap();
    }
    fn receive(
        &self,
        frame: ExchangeUnitReceiverFrame,
    ) -> Result<
        crate::runtime::fragment::io::exchange_unit::ExchangeUnitReceipt,
        super::ExchangeFailure,
    > {
        self.registry.receive_unit(self.key, frame)
    }
    fn handle(&self) -> super::ExchangeReceiverHandle {
        self.registry
            .get_receiver_handle(self.key, self.streams.len())
            .unwrap()
    }
    fn slots(&self) -> crate::runtime::exchange_slots::ExchangeSlotInstance {
        self.registry.receivers.lock().unwrap()[&self.key]
            .unit_owner()
            .unwrap()
            .slots
    }
}

impl Drop for UnitFixture {
    fn drop(&mut self) {
        self.registry.remove_exchange_key(self.key);
    }
}

const EXCHANGE_TEST_SLOT_IDS: [SlotId; 4] = [
    SlotId::new(33),
    SlotId::new(34),
    SlotId::new(31),
    SlotId::new(32),
];

fn exchange_test_chunk(last_name: &str) -> Chunk {
    let schema = Arc::new(Schema::new(vec![
        Field::new("v1", DataType::Int32, true),
        Field::new("v2", DataType::Int32, true),
        Field::new("k1", DataType::Int32, true),
        Field::new(last_name, DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(1)])),
        Arc::new(Int32Array::from(vec![Some(2)])),
        Arc::new(Int32Array::from(vec![Some(3)])),
        Arc::new(StringArray::from(vec![Some("x")])),
    ];
    let batch = RecordBatch::try_new(schema, columns).expect("record batch");
    {
        let batch = batch;
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &EXCHANGE_TEST_SLOT_IDS,
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }
}

fn exchange_test_chunk_without_metadata(last_name: &str) -> Chunk {
    let chunk = exchange_test_chunk(last_name);
    let schema = Arc::new(Schema::new(vec![
        Field::new("v1", DataType::Int32, true),
        Field::new("v2", DataType::Int32, true),
        Field::new("k1", DataType::Int32, true),
        Field::new(last_name, DataType::Utf8, true),
    ]));
    let batch =
        RecordBatch::try_new(schema, chunk.record_batch().columns().to_vec()).expect("batch");
    Chunk::try_new_with_chunk_schema(batch, chunk.chunk_schema_ref()).expect("chunk")
}

fn exchange_test_zero_column_chunk(row_count: usize) -> Chunk {
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let batch = RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
        .expect("empty batch");
    Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty())).unwrap()
}

fn decimal128_chunk_with_over_precision_value(value: i128) -> Chunk {
    let wide = Decimal128Array::from(vec![Some(value)])
        .with_precision_and_scale(38, 2)
        .expect("wide decimal");
    let data = wide
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal128(20, 2))
        .build()
        .expect("decimal20 array data");
    let array = make_array(data);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Decimal128(20, 2),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
    let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(55)],
    )
    .expect("chunk schema");
    Chunk::new_with_chunk_schema(batch, chunk_schema)
}

fn decimal_slot_schema(
    slot_id: SlotId,
    name: &str,
    nullable: bool,
    precision: u8,
    scale: i8,
    unique_id: Option<i32>,
) -> ChunkSlotSchema {
    ChunkSlotSchema::new_with_field(
        slot_id,
        Field::new(name, DataType::Decimal128(precision, scale), nullable),
        None,
        unique_id,
    )
}

fn dictionary_status_chunk(slot_id: SlotId, keys: Vec<Option<i32>>, values: Vec<&str>) -> Chunk {
    let dict = DictionaryArray::<Int32Type>::new(
        Int32Array::from(keys),
        Arc::new(StringArray::from(values)),
    );
    let schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("status", DataType::Utf8, true),
            None,
            None,
        )])
        .expect("chunk schema"),
    );
    Chunk::try_new_with_columns(schema, vec![Arc::new(dict) as ArrayRef]).expect("dictionary chunk")
}

fn logical_status_schema(slot_id: SlotId) -> ChunkSchemaRef {
    Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("status", DataType::Utf8, true),
            None,
            None,
        )])
        .expect("logical status schema"),
    )
}

fn dictionary_chunk_with_type(slot_id: SlotId, data_type: DataType, column: ArrayRef) -> Chunk {
    let schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            slot_id,
            Field::new("bad_dict", data_type, true),
            None,
            None,
        )])
        .expect("chunk schema"),
    );
    Chunk::try_new_with_columns(schema, vec![column]).expect("dictionary chunk")
}

fn logical_utf8_values(array: &ArrayRef) -> Vec<Option<String>> {
    let flat =
        arrow::compute::cast(array.as_ref(), &DataType::Utf8).expect("cast dictionary to utf8");
    let strings = flat
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 array");
    (0..strings.len())
        .map(|idx| {
            if strings.is_null(idx) {
                None
            } else {
                Some(strings.value(idx).to_string())
            }
        })
        .collect()
}

#[test]
fn encode_chunks_round_trips_dictionary_batches_with_replaced_values() {
    let first = dictionary_status_chunk(
        SlotId::new(71),
        vec![Some(0), Some(1), None, Some(0)],
        vec!["PAID", "NEW"],
    );
    let second = dictionary_status_chunk(
        SlotId::new(71),
        vec![Some(0), Some(1), Some(0), None],
        vec!["CANCELLED", "PAID"],
    );

    let bytes = encode_chunks(&[first, second], true).expect("encode dictionary chunks");
    let decoded = decode_chunks(&bytes).expect("decode dictionary chunks");

    assert_eq!(decoded.len(), 2);
    assert!(matches!(
        decoded[0].columns()[0].data_type(),
        DataType::Dictionary(_, _)
    ));
    assert!(matches!(
        decoded[1].columns()[0].data_type(),
        DataType::Dictionary(_, _)
    ));
    assert_eq!(
        logical_utf8_values(&decoded[0].columns()[0]),
        vec![
            Some("PAID".to_string()),
            Some("NEW".to_string()),
            None,
            Some("PAID".to_string()),
        ]
    );
    assert_eq!(
        logical_utf8_values(&decoded[1].columns()[0]),
        vec![
            Some("CANCELLED".to_string()),
            Some("PAID".to_string()),
            Some("CANCELLED".to_string()),
            None,
        ]
    );
}

#[test]
fn bounded_encode_uses_one_capped_backing_payload() {
    let chunk = exchange_test_chunk("name");
    let expected = encode_chunks(std::slice::from_ref(&chunk), true).expect("encode payload");
    let packet_cap = expected.len() + 1024;
    let encoded = encode_chunks_bounded(&[chunk], true, packet_cap).expect("bounded encode");
    assert!(!encoded.is_empty());
    let (payload, retained_bytes) = encoded.into_parts();

    assert_eq!(payload.as_ref(), expected.as_slice());
    assert!(retained_bytes >= payload.len());
    assert!(retained_bytes <= packet_cap);
}

#[test]
fn bounded_encode_reports_empty_payload() {
    let encoded = encode_chunks_bounded(&[], true, 0).expect("empty bounded encode");

    assert!(encoded.is_empty());
    assert_eq!(encoded.retained_bytes(), 0);
}

#[test]
fn bounded_encode_fails_while_crossing_packet_cap() {
    let error = encode_chunks_bounded(&[exchange_test_chunk("name")], true, 32)
        .expect_err("Arrow payload must exceed the bounded packet cap");

    assert!(error.to_string().contains("exceeds packet cap"), "{error}");
}

fn preflight_typed_test_payload(
    payload: &[u8],
) -> Result<super::TypedRootResultDecodeBounds, super::ExchangeFailure> {
    let limit = novarocks_execution_contract::ResultByteLimit::new(
        u64::try_from(payload.len()).expect("fixture payload length fits u64"),
    )
    .expect("fixture payload is nonempty");
    preflight_typed_root_result_decode(payload, limit)
}

#[test]
fn typed_root_result_preflight_bounds_encoder_round_trip_backing_bytes() {
    let cases = vec![
        exchange_test_chunk("name"),
        exchange_test_zero_column_chunk(7),
        decimal128_chunk_with_over_precision_value(123_456_789),
        dictionary_status_chunk(
            SlotId::new(91),
            vec![Some(0), Some(1), None, Some(0)],
            vec!["READY", "RUNNING"],
        ),
    ];

    for chunk in cases {
        let expected = plain_declaration(chunk.chunk_schema());
        let payload = encode_chunks(&[chunk], true).expect("encode typed root result");
        let bounds = preflight_typed_test_payload(&payload)
            .expect("current encoder output must pass preflight");
        let decoded = decode_root_result_chunks(&payload, &expected).expect("decode root result");
        let actual_backing = decoded.iter().fold(0usize, |total, chunk| {
            total.saturating_add(chunk.logical_bytes())
        });

        assert!(
            u64::try_from(actual_backing).unwrap() <= bounds.retained_backing_upper_bound(),
            "decoded backing {actual_backing} exceeds {:?}",
            bounds
        );
        assert!(bounds.decode_operation_upper_bound() >= bounds.retained_backing_upper_bound());
    }
}

#[test]
fn typed_root_result_preflight_rejects_non_current_stream_shapes() {
    let chunk = exchange_test_chunk("name");

    let mut missing_slot_flag = encode_chunks(std::slice::from_ref(&chunk), true).unwrap();
    missing_slot_flag[5] = 0;
    let error =
        preflight_typed_test_payload(&missing_slot_flag).expect_err("slot metadata is mandatory");
    assert!(
        error.to_string().contains("requires exactly slot-id flag"),
        "{error}"
    );

    let multiple_batches = encode_chunks(&[chunk.clone(), chunk.clone()], true).unwrap();
    let error = preflight_typed_test_payload(&multiple_batches)
        .expect_err("one fetch packet cannot contain multiple record batches");
    assert!(
        error.to_string().contains("exactly one record batch"),
        "{error}"
    );

    let mut trailing = encode_chunks(&[chunk], true).unwrap();
    trailing.push(0);
    let error =
        preflight_typed_test_payload(&trailing).expect_err("terminal marker must end the stream");
    assert!(
        error
            .to_string()
            .contains("terminal marker has trailing bytes"),
        "{error}"
    );
}

#[test]
fn typed_root_result_preflight_enforces_caller_payload_limit() {
    let payload = encode_chunks(&[exchange_test_chunk("name")], true).unwrap();
    let limit = novarocks_execution_contract::ResultByteLimit::new(
        u64::try_from(payload.len() - 1).unwrap(),
    )
    .unwrap();

    let error = preflight_typed_root_result_decode(&payload, limit)
        .expect_err("caller payload limit must remain authoritative");
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(&error);
    assert_eq!(
        fact.class(),
        novarocks_failure_contract::FailureClass::Representation
    );
    assert_eq!(fact.reason().code(), 115);
    let extent = fact.representation().unwrap();
    assert_eq!(extent.required, Some(payload.len() as u64));
    assert_eq!(extent.extent_limit, Some(limit.get()));
    assert_eq!(extent.representable_prefix, None);
    assert!(fact.resource().is_none());
}

#[test]
fn typed_root_result_preflight_rejects_compressed_stream() {
    let chunk = exchange_test_chunk("name");
    let schema = chunk.record_batch().schema();
    let options = arrow::ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
        .expect("compression support");
    let mut arrow_payload = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new_with_options(
        &mut arrow_payload,
        schema.as_ref(),
        options,
    )
    .expect("compressed stream writer");
    writer
        .write(chunk.record_batch())
        .expect("compressed batch");
    writer.finish().expect("finish compressed stream");
    let wire_meta = ExchangeWireMeta::from_chunks(std::slice::from_ref(&chunk))
        .expect("wire metadata")
        .expect("typed wire metadata");
    let payload = encode_exchange_payload_envelope(&arrow_payload, Some(&wire_meta))
        .expect("exchange envelope fixture");

    let error = preflight_typed_test_payload(&payload)
        .expect_err("compressed root result must fail closed");
    assert!(
        error.to_string().contains("compression is not allowed"),
        "{error}"
    );
}

#[test]
fn typed_root_result_preflight_rejects_exaggerated_and_negative_lengths() {
    let payload = encode_chunks(&[exchange_test_chunk("name")], true).unwrap();
    let arrow_offset = typed_test_arrow_payload_offset(&payload);

    let mut exaggerated_metadata = payload.clone();
    exaggerated_metadata[arrow_offset + 4..arrow_offset + 8].copy_from_slice(
        &u32::try_from(MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES + 64)
            .unwrap()
            .to_le_bytes(),
    );
    let error = preflight_typed_test_payload(&exaggerated_metadata)
        .expect_err("exaggerated metadata must fail closed");
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(&error);
    assert_eq!(fact.reason().code(), 117);
    let extent = fact.representation().unwrap();
    assert_eq!(
        extent.required,
        Some((MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES + 64) as u64)
    );
    assert_eq!(
        extent.extent_limit,
        Some(MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES as u64)
    );

    let mut negative_body = payload;
    mutate_record_batch_body_length(&mut negative_body, -1);
    let error = preflight_typed_test_payload(&negative_body)
        .expect_err("negative body length must fail closed");
    assert!(
        error.to_string().contains("body length is negative"),
        "{error}"
    );
    let error = decode_root_result_chunks(
        &negative_body,
        &plain_declaration(exchange_test_chunk("name").chunk_schema()),
    )
    .expect_err("the production decode entry must enforce preflight");
    assert!(
        error.to_string().contains("body length is negative"),
        "{error}"
    );

    let mut oversized_body =
        encode_chunks(&[exchange_test_chunk("name")], true).expect("fresh payload");
    let oversized_body_length = i64::try_from(oversized_body.len() + 64).unwrap();
    mutate_record_batch_body_length(&mut oversized_body, oversized_body_length);
    let error = preflight_typed_test_payload(&oversized_body)
        .expect_err("oversized body length must fail closed");
    let extent = novarocks_failure_contract::FailureProjection::failure_fact(&error)
        .representation()
        .unwrap();
    assert_eq!(extent.required, Some(oversized_body_length as u64));
    assert_eq!(extent.extent_limit, Some(oversized_body.len() as u64));

    let mut negative_buffer =
        encode_chunks(&[exchange_test_chunk("name")], true).expect("fresh payload");
    mutate_unique_record_batch_buffer_length(&mut negative_buffer, -1);
    let error = preflight_typed_test_payload(&negative_buffer)
        .expect_err("negative buffer length must fail closed");
    assert!(
        error.to_string().contains("buffer") && error.to_string().contains("negative"),
        "{error}"
    );

    let mut excessive_structure =
        encode_chunks(&[exchange_test_chunk("name")], true).expect("fresh payload");
    excessive_structure[6..10].copy_from_slice(
        &u32::try_from(MAX_TYPED_ROOT_RESULT_SCHEMA_FIELDS + 1)
            .unwrap()
            .to_le_bytes(),
    );
    let error = preflight_typed_test_payload(&excessive_structure)
        .expect_err("excessive slot structure must fail closed");
    assert!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error)
            .reason()
            .code()
            == 116,
        "{error}"
    );
}

#[test]
fn typed_root_result_preflight_preserves_frame_error_precedence() {
    use novarocks_arrow_ipc_frame::{PrefixError, RangeError};
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    fn prefix(error: &super::ExchangeFailure) -> PrefixError {
        let mut source: &(dyn std::error::Error + 'static) = error;
        loop {
            if let Some(source) = source.downcast_ref::<PrefixError>() {
                return *source;
            }
            source = source.source().expect("actual prefix author is retained");
        }
    }
    let payload = encode_chunks(&[exchange_test_chunk("name")], true).unwrap();
    let start = typed_test_arrow_payload_offset(&payload);
    let declared = u32::try_from(MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES + 1).unwrap();
    // Oversize, alignment and truncation coincide; the original first
    // guard is still the message metadata extent, now a machine fact.
    let mut oversized = payload.clone();
    oversized[start + 4..start + 8].copy_from_slice(&declared.to_le_bytes());
    let error = preflight_typed_test_payload(&oversized).unwrap_err();
    assert_eq!(error.failure_fact().reason().code(), 117);
    let extent = error.failure_fact().representation().unwrap();
    assert_eq!(extent.required, Some(declared as u64));
    assert_eq!(
        extent.extent_limit,
        Some(MAX_TYPED_ROOT_RESULT_MESSAGE_METADATA_BYTES as u64)
    );
    let mut bad_marker = oversized;
    bad_marker[start] = 0;
    let error = preflight_typed_test_payload(&bad_marker).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Data);
    assert_eq!(prefix(&error), PrefixError::NotContinuation);
    bad_marker.truncate(start + 7);
    assert_eq!(
        prefix(&preflight_typed_test_payload(&bad_marker).unwrap_err()),
        PrefixError::Range(RangeError::OutOfBounds)
    );
    // EOS trailing bytes still fail before missing schema/record batch.
    let mut early_end = payload[..start].to_vec();
    early_end.extend_from_slice(&ARROW_IPC_CONTINUATION_MARKER);
    early_end.extend_from_slice(&0_u32.to_le_bytes());
    early_end.push(0);
    assert_eq!(
        preflight_typed_test_payload(&early_end)
            .unwrap_err()
            .to_string(),
        "Arrow IPC terminal marker has trailing bytes"
    );
}

fn typed_test_arrow_payload_offset(payload: &[u8]) -> usize {
    let slot_count = u32::from_le_bytes(payload[6..10].try_into().unwrap()) as usize;
    10 + slot_count * std::mem::size_of::<u32>()
}

fn mutate_record_batch_body_length(payload: &mut [u8], replacement: i64) {
    let mut offset = typed_test_arrow_payload_offset(payload);
    loop {
        assert_eq!(payload[offset..offset + 4], ARROW_IPC_CONTINUATION_MARKER);
        let metadata_len =
            u32::from_le_bytes(payload[offset + 4..offset + 8].try_into().unwrap()) as usize;
        assert_ne!(metadata_len, 0, "record batch must precede terminal marker");
        let metadata_start = offset + 8;
        let metadata_end = metadata_start + metadata_len;
        let message = arrow::ipc::root_as_message(&payload[metadata_start..metadata_end])
            .expect("valid metadata before mutation");
        let body_len = usize::try_from(message.bodyLength()).unwrap();
        if message.header_type() == arrow::ipc::MessageHeader::RecordBatch {
            let needle = message.bodyLength().to_le_bytes();
            let matches = payload[metadata_start..metadata_end]
                .windows(needle.len())
                .enumerate()
                .filter_map(|(index, bytes)| (bytes == needle).then_some(index))
                .collect::<Vec<_>>();
            assert_eq!(
                matches.len(),
                1,
                "body length must have one metadata encoding"
            );
            let start = metadata_start + matches[0];
            payload[start..start + 8].copy_from_slice(&replacement.to_le_bytes());
            return;
        }
        offset = metadata_end + body_len;
    }
}

fn mutate_unique_record_batch_buffer_length(payload: &mut [u8], replacement: i64) {
    let mut offset = typed_test_arrow_payload_offset(payload);
    loop {
        assert_eq!(payload[offset..offset + 4], ARROW_IPC_CONTINUATION_MARKER);
        let metadata_len =
            u32::from_le_bytes(payload[offset + 4..offset + 8].try_into().unwrap()) as usize;
        assert_ne!(metadata_len, 0, "record batch must precede terminal marker");
        let metadata_start = offset + 8;
        let metadata_end = metadata_start + metadata_len;
        let message = arrow::ipc::root_as_message(&payload[metadata_start..metadata_end])
            .expect("valid metadata before mutation");
        let body_len = usize::try_from(message.bodyLength()).unwrap();
        if let Some(batch) = message.header_as_record_batch() {
            for descriptor in batch.buffers().expect("record-batch buffers").iter() {
                let needle = descriptor.0;
                let matches = payload[metadata_start..metadata_end]
                    .windows(needle.len())
                    .enumerate()
                    .filter_map(|(index, bytes)| (bytes == needle).then_some(index))
                    .collect::<Vec<_>>();
                if matches.len() == 1 {
                    let length_start = metadata_start + matches[0] + 8;
                    payload[length_start..length_start + 8]
                        .copy_from_slice(&replacement.to_le_bytes());
                    return;
                }
            }
            panic!("record batch must contain one uniquely encoded buffer descriptor");
        }
        offset = metadata_end + body_len;
    }
}

#[test]
fn bounded_writer_grows_from_spare_capacity_without_unbounded_extend() {
    let mut writer = BoundedPayloadWriter::try_new(10, 8).expect("bounded writer");
    writer.write_all(&[1; 4]).expect("initial write");
    assert_eq!(writer.buffer.len(), 4);
    assert_eq!(writer.buffer.capacity(), 8);

    writer.write_all(&[2; 6]).expect("bounded growth");
    assert_eq!(writer.buffer.len(), 10);
    assert_eq!(writer.buffer.capacity(), 10);
}

#[test]
fn bounded_writer_rejects_over_cap_without_mutating_buffer() {
    let mut writer = BoundedPayloadWriter::try_new(10, 8).expect("bounded writer");
    writer.write_all(&[1; 4]).expect("initial write");
    let before_len = writer.buffer.len();
    let before_capacity = writer.buffer.capacity();

    let error = writer
        .write_all(&[2; 7])
        .expect_err("write beyond cap must fail");
    assert!(error.to_string().contains("exceeds packet cap"));
    assert_eq!(writer.buffer.len(), before_len);
    assert_eq!(writer.buffer.capacity(), before_capacity);
}

#[test]
fn root_ipc_source_annotations_cannot_change_frozen_nested_domains() {
    use arrow::array::new_null_array;
    use novarocks_types::logical::{LogicalType, NR_LOGICAL_TYPE_KEY};
    fn marked(field: Field, marker: Option<&str>) -> Field {
        match marker {
            Some(value) => {
                field.with_metadata(HashMap::from([(NR_LOGICAL_TYPE_KEY.into(), value.into())]))
            }
            None => field,
        }
    }
    fn nested(kind: usize, marker: Option<&str>) -> Field {
        let leaf = || Arc::new(marked(Field::new("value", DataType::Binary, true), marker));
        let data_type = match kind {
            0 => DataType::Struct(vec![leaf()].into()),
            1 => DataType::List(leaf()),
            4 => DataType::LargeList(leaf()),
            2 | 3 => {
                let value = if kind == 2 {
                    leaf()
                } else {
                    Arc::new(Field::new("value", DataType::Binary, true))
                };
                let entries = Field::new(
                    "entries",
                    DataType::Struct(
                        vec![Arc::new(Field::new("key", DataType::Int32, false)), value].into(),
                    ),
                    false,
                );
                DataType::Map(
                    Arc::new(if kind == 3 {
                        marked(entries, marker)
                    } else {
                        entries
                    }),
                    false,
                )
            }
            _ => unreachable!(),
        };
        Field::new("root", data_type, true)
    }
    for kind in 0..5 {
        let declared = nested(
            kind,
            if kind == 3 {
                None
            } else {
                Some(LogicalType::Bitmap.metadata_value())
            },
        );
        use novarocks_type_contract::{
            TypedValueBinding, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
            ValueTypeStorageLifetime,
        };
        let storage = ValueTypeStorageLifetime::pure();
        let leaf = ValueTypeRef::try_new(
            if kind == 3 {
                ValueKind::Binary
            } else {
                ValueKind::Nominal(ValueLogicalType::Bitmap)
            },
            true,
            storage.clone(),
        )
        .unwrap();
        let root = match kind {
            0 => ValueKind::Struct(
                vec![ValueField::new("value", leaf, storage.clone())].into_boxed_slice(),
            ),
            1 | 4 => ValueKind::List(leaf),
            2 | 3 => ValueKind::Map {
                key: ValueTypeRef::try_new(
                    ValueKind::Scalar(DataType::Int32),
                    false,
                    storage.clone(),
                )
                .unwrap(),
                value: leaf,
            },
            _ => unreachable!(),
        };
        let value = ValueTypeRef::try_new(root, true, storage.clone()).unwrap();
        let layout = ValueLayout::try_new(declared.data_type(), storage.clone()).unwrap();
        let field = Arc::new(Field::new("root", layout.data_type().clone(), true));
        let binding = TypedValueBinding::try_new(value, layout).unwrap();
        let slot =
            ChunkSlotSchema::try_new_typed(SlotId::new(74), field, binding, None, storage.clone())
                .unwrap();
        let expected = Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).unwrap());
        for marker in [Some("hll"), Some("unknown-domain"), None, Some("bitmap")] {
            let source = nested(kind, marker);
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![source.clone()])),
                vec![new_null_array(source.data_type(), 1)],
            )
            .unwrap();
            let mut arrow = Vec::new();
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut arrow, batch.schema().as_ref())
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            drop(writer);
            let bytes = encode_exchange_payload_envelope(
                &arrow,
                Some(&ExchangeWireMeta {
                    slot_ids_by_index: vec![SlotId::new(73)],
                }),
            )
            .expect("exchange envelope fixture");
            let decoded = decode_root_result_chunks(&bytes, &expected).unwrap();
            assert_eq!(decoded[0].len(), 1);
            assert_eq!(
                decoded[0].columns()[0].data_type(),
                expected.slots()[0].data_type()
            );
            assert_eq!(decoded[0].chunk_schema().slot_ids(), &[SlotId::new(74)]);
            assert_eq!(
                decoded[0].chunk_schema().slots()[0].typed_binding(),
                expected.slots()[0].typed_binding()
            );
        }
    }
}

#[test]
fn m07_root_ipc_preserves_large_string_dictionary_without_changing_exchange_admission() {
    use arrow::array::{DictionaryArray, Int32Array, LargeStringArray};
    use arrow::datatypes::Int32Type;
    let array = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(1), None, Some(0)]),
            Arc::new(LargeStringArray::from(vec!["{}", "[]"])),
        )
        .unwrap(),
    ) as ArrayRef;
    let field = Field::new("wire", array.data_type().clone(), true);
    let chunk = Chunk::try_new_with_columns(
        Arc::new(
            crate::exec::chunk::ChunkSchema::try_new(vec![
                crate::exec::chunk::ChunkSlotSchema::new_with_field(
                    SlotId::new(72),
                    field,
                    None,
                    None,
                ),
            ])
            .unwrap(),
        ),
        vec![array],
    )
    .unwrap();
    let bytes = encode_chunks(&[chunk], true).unwrap();
    let expected = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(73),
            Field::new("status", DataType::LargeUtf8, true),
            None,
            None,
        )])
        .unwrap(),
    );
    let decoded = decode_root_result_chunks(&bytes, &plain_declaration(&expected)).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].len(), 3);
    assert_eq!(decoded[0].chunk_schema().slot_ids(), &[SlotId::new(73)]);
    assert_eq!(
        decoded[0].columns()[0].data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8))
    );
    assert_eq!(
        decoded[0].record_batch().schema().field(0).data_type(),
        decoded[0].columns()[0].data_type()
    );
    let key = ExchangeKey {
        finst_id_hi: 1703,
        finst_id_lo: 1704,
        node_id: 78,
    };
    register_expected_chunk_schema(key, 1, logical_status_schema(SlotId::new(72))).unwrap();
    let error = decode_chunks_for_sender(key, 9, 1, &bytes).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("recipe digest or version differs"),
        "{error}"
    );
}

#[test]
fn decode_chunks_for_sender_preserves_dictionary_carrier_with_logical_expected_schema() {
    let key = ExchangeKey {
        finst_id_hi: 1701,
        finst_id_lo: 1702,
        node_id: 77,
    };
    let chunk = dictionary_status_chunk(
        SlotId::new(72),
        vec![Some(0), Some(1), Some(0)],
        vec!["SHIPPED", "PAID"],
    );
    register_expected_chunk_schema(key, 1, logical_status_schema(SlotId::new(72)))
        .expect("register schema");

    let payload = online_unit(&[chunk], true).expect("encode dictionary payload");
    let decoded = decode_chunks_for_sender(key, 9, 1, &payload).expect("decode sender payload");

    assert_eq!(decoded.len(), 1);
    assert!(matches!(
        decoded[0].columns()[0].data_type(),
        DataType::Dictionary(_, _)
    ));
    assert_eq!(
        logical_utf8_values(&decoded[0].columns()[0]),
        vec![
            Some("SHIPPED".to_string()),
            Some("PAID".to_string()),
            Some("SHIPPED".to_string()),
        ]
    );

    cancel_exchange_key(key);
}

fn assert_value_dictionary_refusal(error: &super::ExchangeFailure) {
    use novarocks_failure_contract::FailureProjection;
    use novarocks_value_arrays::{ValueError, ValueViolation};
    assert_eq!(
        error.failure_fact(),
        ValueError::Invariant(ValueViolation::Dictionary).failure_fact()
    );
    let mut cause = Some(error as &dyn std::error::Error);
    while let Some(original) = cause {
        if let Some(value) = original.downcast_ref::<ValueError>() {
            assert!(matches!(
                value,
                ValueError::Invariant(ValueViolation::Dictionary)
            ));
            return;
        }
        cause = original.source();
    }
    panic!("exchange refusal lost its original value error");
}

#[test]
fn encode_chunks_rejects_dictionary_with_non_int32_key() {
    let dict = arrow::array::DictionaryArray::<arrow::datatypes::Int8Type>::new(
        arrow::array::Int8Array::from(vec![Some(0_i8), Some(1_i8)]),
        Arc::new(StringArray::from(vec!["PAID", "NEW"])),
    );
    let data_type = dict.data_type().clone();
    let chunk = dictionary_chunk_with_type(SlotId::new(73), data_type, Arc::new(dict) as ArrayRef);

    let err = encode_chunks(&[chunk], true).expect_err("unsupported key type");

    assert_value_dictionary_refusal(&err);
}

#[test]
fn encode_chunks_rejects_dictionary_with_non_string_values() {
    let dict = DictionaryArray::<Int32Type>::new(
        Int32Array::from(vec![Some(0), Some(1)]),
        Arc::new(Int32Array::from(vec![10, 20])),
    );
    let data_type = dict.data_type().clone();
    let chunk = dictionary_chunk_with_type(SlotId::new(74), data_type, Arc::new(dict) as ArrayRef);

    let err = encode_chunks(&[chunk], true).expect_err("unsupported value type");

    assert_value_dictionary_refusal(&err);
}

#[test]
fn encode_chunks_normalizes_field_name_only_schema_differences() {
    let chunks = vec![exchange_test_chunk("_cse_0"), exchange_test_chunk("_cse_2")];
    let bytes = encode_chunks(&chunks, true).expect("encode");
    let decoded = decode_chunks(&bytes).expect("decode");

    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].schema(), decoded[1].schema());
    assert_eq!(decoded[0].schema().field(3).name(), "_cse_0");
}

#[test]
fn encode_chunks_round_trip_zero_column_row_count() {
    let chunk = exchange_test_zero_column_chunk(1);
    let bytes = encode_chunks(&[chunk], true).expect("encode");
    let decoded = decode_chunks(&bytes).expect("decode");

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].len(), 1);
    assert_eq!(decoded[0].record_batch().num_columns(), 0);
}

#[test]
fn encode_chunks_normalizes_runtime_nullable_widening() {
    let key = ExchangeKey {
        finst_id_hi: 299,
        finst_id_lo: 300,
        node_id: 27,
    };
    let expected_chunk_schema = exchange_test_chunk("_cse_0").chunk_schema_ref();
    register_expected_chunk_schema(key, 1, expected_chunk_schema).expect("register schema");

    let strict = exchange_test_chunk("_cse_0");
    let schema = Arc::new(Schema::new(vec![
        Field::new("v1", DataType::Int32, true),
        Field::new("v2", DataType::Int32, true),
        Field::new("k1", DataType::Int32, true),
        Field::new("_cse_0", DataType::Utf8, true),
    ]));
    let widened =
        RecordBatch::try_new(schema, strict.record_batch().columns().to_vec()).expect("batch");
    let widened_chunk_schema = Arc::new(
        crate::exec::chunk::ChunkSchema::try_new(
            widened
                .schema()
                .fields()
                .iter()
                .zip(strict.chunk_schema().slots().iter())
                .map(|(field, slot)| slot.with_field(field.as_ref().clone()))
                .collect::<Result<Vec<_>, _>>()
                .expect("slot schemas"),
        )
        .expect("chunk schema"),
    );
    let widened = Chunk::try_new_with_chunk_schema(widened, widened_chunk_schema).expect("chunk");

    let prepared = plain_declaration(strict.chunk_schema());
    for source in [strict, widened] {
        let bytes = online_unit_as(&source, &prepared);
        let decoded =
            decode_chunks_for_sender(key, 3, 1, &bytes).expect("decode independent widened unit");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].len(), 1);
        for (ordinal, value) in [(0, 1), (1, 2), (2, 3)] {
            assert_eq!(
                decoded[0].columns()[ordinal]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                value
            );
        }
        assert_eq!(
            decoded[0].columns()[3]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "x"
        );
    }

    cancel_exchange_key(key);
}

#[test]
fn encode_chunks_preserves_decimal128_values_exceeding_declared_precision() {
    // Descriptor-authoritative exchange (pillar P3): the sender writes its
    // OWN type verbatim — it no longer widens decimal precision to 38. The
    // i128 payload is preserved regardless of the precision tag. With no
    // registered receiver descriptor (`decode_chunks`), the decoded type is
    // the sender's wire type; a registered descriptor must match exactly.
    let value = 100_000_000_000_000_000_000_i128;
    let chunk = decimal128_chunk_with_over_precision_value(value);

    let bytes = encode_chunks(&[chunk], true).expect("encode");
    let decoded = decode_chunks(&bytes).expect("decode");

    assert_eq!(decoded.len(), 1);
    let array = decoded[0]
        .record_batch()
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal128 array");
    assert_eq!(array.data_type(), &DataType::Decimal128(20, 2));
    assert!(!array.is_null(0));
    assert_eq!(array.value(0), value);
}

#[test]
fn decode_root_result_chunks_rejects_decimal_precision_drift() {
    let value = 100_000_000_000_000_000_000_i128;
    let chunk = decimal128_chunk_with_over_precision_value(value);
    let payload = encode_chunks(&[chunk], true).expect("encode");
    let expected_schema = Arc::new(
        ChunkSchema::try_new(vec![decimal_slot_schema(
            SlotId::new(55),
            "price",
            true,
            38,
            2,
            None,
        )])
        .expect("expected chunk schema"),
    );

    let err = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
        .expect_err("root result decode must reject decimal precision drift");

    assert!(err.to_string().contains("frozen value layout"), "err={err}");
    assert!(err.to_string().contains("column 0"), "err={err}");
    assert!(err.to_string().contains("Decimal128(38, 2)"), "err={err}");
    assert!(err.to_string().contains("Decimal128(20, 2)"), "err={err}");
}

#[test]
fn decode_chunks_for_sender_rejects_decimal_precision_drift() {
    let key = ExchangeKey {
        finst_id_hi: 601,
        finst_id_lo: 602,
        node_id: 33,
    };
    let expected_schema = Arc::new(
        ChunkSchema::try_new(vec![decimal_slot_schema(
            SlotId::new(55),
            "price",
            true,
            38,
            2,
            None,
        )])
        .expect("expected chunk schema"),
    );
    register_expected_chunk_schema(key, 1, expected_schema).expect("register expected schema");

    let chunk = decimal128_chunk_with_over_precision_value(1234_i128);
    let payload = online_unit(&[chunk], true).expect("encode drifted decimal chunk");

    let err = decode_chunks_for_sender(key, 3, 1, &payload)
        .expect_err("exchange decode must reject decimal precision drift");

    assert!(
        err.to_string().contains("recipe digest or version differs"),
        "err={err}"
    );
    cancel_exchange_key(key);
}

#[test]
fn decode_root_result_chunks_rejects_utf8_binary_type_drift() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![Some("abc")])) as ArrayRef],
    )
    .expect("record batch");
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(56)])
            .expect("chunk schema");
    let payload =
        encode_chunks(&[Chunk::new_with_chunk_schema(batch, chunk_schema)], true).expect("encode");
    let expected_schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(56),
            Field::new("payload", DataType::Binary, true),
            None,
            None,
        )])
        .expect("expected chunk schema"),
    );

    let err = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
        .expect_err("root result decode must reject Utf8/Binary drift");

    assert!(err.to_string().contains("frozen value layout"), "err={err}");
    assert!(err.to_string().contains("column 0"), "err={err}");
    assert!(err.to_string().contains("Binary"), "err={err}");
    assert!(err.to_string().contains("Utf8"), "err={err}");
}

#[test]
fn decode_root_result_chunks_preserves_exact_decimal_descriptor_type() {
    let value = 1234_i128;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Decimal128(38, 2),
        true,
    )]));
    let array = Decimal128Array::from(vec![Some(value)])
        .with_precision_and_scale(38, 2)
        .expect("decimal array");
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).expect("record batch");
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(55)])
            .expect("chunk schema");
    let payload =
        encode_chunks(&[Chunk::new_with_chunk_schema(batch, chunk_schema)], true).expect("encode");
    let expected_schema = Arc::new(
        ChunkSchema::try_new(vec![decimal_slot_schema(
            SlotId::new(55),
            "price",
            true,
            38,
            2,
            None,
        )])
        .expect("expected chunk schema"),
    );

    let decoded = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
        .expect("decode root");

    assert_eq!(decoded.len(), 1);
    assert_eq!(
        decoded[0].record_batch().schema().field(0).data_type(),
        &DataType::Decimal128(38, 2)
    );
    assert_eq!(
        decoded[0].chunk_schema().slots()[0].slot_id(),
        SlotId::new(55)
    );
}

#[test]
fn decode_root_result_chunks_uses_expected_schema_positionally() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("file", DataType::Utf8, false),
            Field::new("row_id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a.parquet"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![42])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
        ],
    )
    .expect("record batch");
    let wire_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(3), SlotId::new(1), SlotId::new(2)],
    )
    .expect("wire chunk schema");
    let payload = encode_chunks(&[Chunk::new_with_chunk_schema(batch, wire_schema)], true)
        .expect("encode root result");
    let expected_schema = Arc::new(
        ChunkSchema::try_new(vec![
            ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("file", DataType::Utf8, false),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(2),
                Field::new("row_id", DataType::Int64, false),
                None,
                None,
            ),
            ChunkSlotSchema::new_with_field(
                SlotId::new(3),
                Field::new("value", DataType::Utf8, true),
                None,
                None,
            ),
        ])
        .expect("expected root schema"),
    );

    let decoded = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
        .expect("decode root");

    assert_eq!(decoded.len(), 1);
    assert_eq!(
        decoded[0].chunk_schema().slot_ids(),
        &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
    );
    assert_eq!(decoded[0].record_batch().schema().field(0).name(), "file");
    assert_eq!(decoded[0].record_batch().schema().field(1).name(), "row_id");
    assert_eq!(decoded[0].record_batch().schema().field(2).name(), "value");
}

#[test]
fn native_exchange_publication_retains_descriptor_metadata_ownership() {
    use crate::exec::chunk::{RootArrayStorageLimits, borrowed_root_chunk_storage};
    use novarocks_types::arrow_metadata_owner::{
        ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
    };
    let owner = ArrowMetadataOwner::try_new(
        Vec::new(),
        MetadataOwnerLimits {
            entries: 0,
            construction_bytes: 0,
        },
    )
    .unwrap()
    .into_field("value".into(), DataType::Int64, false);
    let expected = Arc::new(
        ChunkSchema::try_new(vec![
            ChunkSlotSchema::try_new_with_metadata_origins(
                SlotId::new(73),
                Arc::clone(owner.field()),
                FieldMetadataOrigins::try_new(vec![owner], 1).unwrap(),
                None,
                None,
            )
            .unwrap(),
        ])
        .unwrap(),
    );
    let source = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "wire_value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
    )
    .unwrap();
    let unknown =
        ChunkSchema::try_ref_from_schema_and_slot_ids(source.schema().as_ref(), &[SlotId::new(5)])
            .unwrap();
    let original = Chunk::new_with_chunk_schema(source, unknown);
    let limits = RootArrayStorageLimits {
        bytes: 96 * 1024 * 1024,
        nodes: 65536,
        depth: 64,
    };
    assert!(borrowed_root_chunk_storage(&original, limits).is_err());
    let payload = online_unit(&[original], true).unwrap();
    let key = ExchangeKey {
        finst_id_hi: 91_735,
        finst_id_lo: 91_736,
        node_id: 73,
    };
    let expected = plain_declaration(&expected);
    let registry = ExecutionExchangeRegistry::default();
    registry
        .register_expected_chunk_schema(
            key,
            1,
            Arc::clone(&expected),
            crate::runtime::fragment::exchange::test_receiver_creation_factory(),
        )
        .unwrap();
    let decoded = registry
        .decode_chunks_for_sender(key, ExchangeSenderIdentity::local(3, 1), &payload)
        .unwrap();
    assert_eq!(decoded[0].chunk_schema().slot_ids(), &[SlotId::new(73)]);
    assert_eq!(
        decoded[0]
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert!(borrowed_root_chunk_storage(&decoded[0], limits).is_ok());
    assert!(Arc::ptr_eq(
        decoded[0].chunk_schema().slots()[0].field_ref(),
        expected.slots()[0].field_ref()
    ));
}

#[test]
fn decode_root_result_chunks_accepts_nested_map_field_metadata_drift() {
    let source_entries = StructArray::new(
        Fields::from(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ]),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(10), Some(20)])) as ArrayRef,
        ],
        None,
    );
    let source_entries_field = Arc::new(Field::new(
        "entries",
        source_entries.data_type().clone(),
        false,
    ));
    let map = Arc::new(MapArray::new(
        source_entries_field.clone(),
        OffsetBuffer::new(vec![0, 2].into()),
        source_entries,
        None,
        false,
    )) as ArrayRef;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(source_entries_field, false),
            true,
        )])),
        vec![map],
    )
    .expect("record batch");
    let wire_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(5)])
            .expect("wire chunk schema");
    let payload =
        encode_chunks(&[Chunk::new_with_chunk_schema(batch, wire_schema)], true).expect("encode");

    let key_field = Field::new("key", DataType::Int32, false).with_metadata(HashMap::from([(
        "PARQUET:field_id".to_string(),
        "12".to_string(),
    )]));
    let value_field = Field::new("value", DataType::Int32, true).with_metadata(HashMap::from([(
        "PARQUET:field_id".to_string(),
        "13".to_string(),
    )]));
    let target_entries = Arc::new(Field::new(
        "key_value",
        DataType::Struct(Fields::from(vec![key_field, value_field])),
        false,
    ));
    let expected_schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(5),
            Field::new("m", DataType::Map(target_entries, false), true),
            None,
            None,
        )])
        .expect("expected chunk schema"),
    );

    let decoded = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
        .expect("decode map field metadata drift");

    assert_eq!(decoded.len(), 1);
    assert_eq!(
        decoded[0].chunk_schema().slots()[0].slot_id(),
        SlotId::new(5)
    );
    let decoded_schema = decoded[0].record_batch().schema();
    let DataType::Map(entries, false) = decoded_schema.field(0).data_type() else {
        panic!("expected map");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("expected entries struct");
    };
    assert!(
        !fields[0].is_nullable(),
        "Map keys retain the frozen non-NULL contract"
    );
    assert_eq!(decoded[0].record_batch().column(0).len(), 1);
}

#[test]
fn decode_root_result_chunks_preserves_zero_column_row_count() {
    let chunk = exchange_test_zero_column_chunk(3);
    let payload = encode_chunks(&[chunk], true).expect("encode");
    let expected_schema = Arc::new(ChunkSchema::empty());

    let decoded = decode_root_result_chunks(&payload, &plain_declaration(&expected_schema))
        .expect("decode root");

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].len(), 3);
    assert_eq!(decoded[0].record_batch().num_columns(), 0);
}

#[test]
fn encode_chunks_rejects_slot_id_mismatch() {
    let first = exchange_test_chunk("_cse_0");
    let schema = Arc::new(Schema::new(vec![
        Field::new("v1", DataType::Int32, true),
        Field::new("v2", DataType::Int32, true),
        Field::new("k1", DataType::Int32, true),
        Field::new("_cse_2", DataType::Utf8, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(1)])),
        Arc::new(Int32Array::from(vec![Some(2)])),
        Arc::new(Int32Array::from(vec![Some(3)])),
        Arc::new(StringArray::from(vec![Some("x")])),
    ];
    let second = {
        let batch = RecordBatch::try_new(schema, columns).expect("record batch");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[
                SlotId::new(33),
                SlotId::new(34),
                SlotId::new(31),
                SlotId::new(99),
            ],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    };

    let err = encode_chunks(&[first, second], true).expect_err("should reject mismatched slot id");
    assert!(err.to_string().contains("exchange encode schema mismatch"));
}

#[test]
fn decode_chunks_for_sender_uses_slot_id_map_without_field_metadata() {
    let key = ExchangeKey {
        finst_id_hi: 99,
        finst_id_lo: 100,
        node_id: 7,
    };

    let expected_chunk_schema = exchange_test_chunk("_cse_0").chunk_schema_ref();
    register_expected_chunk_schema(key, 1, expected_chunk_schema).expect("register schema");

    let first = exchange_test_chunk_without_metadata("_cse_0");
    let first_bytes = online_unit(&[first], true).expect("encode first");
    let first_decoded = decode_chunks_for_sender(key, 3, 1, &first_bytes).expect("decode first");
    assert_eq!(first_decoded.len(), 1);
    assert_eq!(
        first_decoded[0].chunk_schema().slots()[3].slot_id(),
        SlotId::new(32)
    );
    assert_eq!(first_decoded[0].schema().field(3).name(), "_cse_0");

    let second = exchange_test_chunk_without_metadata("_cse_2");
    let second_bytes = online_unit(&[second], false).expect("encode second");
    let second_decoded = decode_chunks_for_sender(key, 3, 1, &second_bytes).expect("decode second");
    assert_eq!(second_decoded.len(), 1);
    assert_eq!(
        second_decoded[0].chunk_schema().slots()[3].slot_id(),
        SlotId::new(32)
    );
    assert_eq!(second_decoded[0].schema().field(3).name(), "_cse_0");

    cancel_exchange_key(key);
}

#[test]
fn decode_chunks_for_sender_rejects_partial_expected_schema_match() {
    let key = ExchangeKey {
        finst_id_hi: 1099,
        finst_id_lo: 1100,
        node_id: 77,
    };
    let expected_chunk_schema = exchange_test_chunk("_cse_0").chunk_schema_ref();
    register_expected_chunk_schema(key, 1, expected_chunk_schema).expect("register schema");

    let schema = Arc::new(Schema::new(vec![Field::new("v1", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef],
    )
    .expect("record batch");
    let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(33)],
    )
    .expect("chunk schema");
    let payload =
        online_unit(&[Chunk::new_with_chunk_schema(batch, chunk_schema)], true).expect("encode");

    let err = decode_chunks_for_sender(key, 3, 1, &payload)
        .expect_err("partial wire schema must not match registered expected schema");
    assert!(
        err.to_string().contains("recipe digest or version differs"),
        "err={err}"
    );

    cancel_exchange_key(key);
}

#[test]
fn decode_chunks_for_sender_accepts_runtime_nullable_widening() {
    let key = ExchangeKey {
        finst_id_hi: 199,
        finst_id_lo: 200,
        node_id: 17,
    };
    let expected_chunk_schema = exchange_test_chunk("_cse_0").chunk_schema_ref();
    register_expected_chunk_schema(key, 1, expected_chunk_schema.clone()).expect("register schema");

    let strict = exchange_test_chunk_without_metadata("_cse_0");
    let widened_schema = Arc::new(Schema::new(vec![
        Field::new("v1", DataType::Int32, true),
        Field::new("v2", DataType::Int32, true),
        Field::new("k1", DataType::Int32, true),
        Field::new("_cse_0", DataType::Utf8, true),
    ]));
    let widened = RecordBatch::try_new(widened_schema, strict.record_batch().columns().to_vec())
        .expect("batch");
    let widened_chunk_schema = Arc::new(
        crate::exec::chunk::ChunkSchema::try_new(
            widened
                .schema()
                .fields()
                .iter()
                .zip(strict.chunk_schema().slots().iter())
                .map(|(field, slot)| slot.with_field(field.as_ref().clone()))
                .collect::<Result<Vec<_>, _>>()
                .expect("slot schemas"),
        )
        .expect("chunk schema"),
    );
    let widened = Chunk::try_new_with_chunk_schema(widened, widened_chunk_schema).expect("chunk");

    let payload = online_unit_as(&widened, &plain_declaration(&expected_chunk_schema));
    let decoded = decode_chunks_for_sender(key, 3, 1, &payload).expect("decode widened chunk");

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].len(), widened.len());
    assert_eq!(
        decoded[0].chunk_schema().slots()[3]
            .typed_binding()
            .value_type()
            .nullable(),
        expected_chunk_schema.slots()[3].nullable()
    );

    cancel_exchange_key(key);
}

#[test]
fn decode_chunks_for_sender_rejects_numeric_for_opaque_descriptor() {
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};

    let key = ExchangeKey {
        finst_id_hi: 501,
        finst_id_lo: 502,
        node_id: 31,
    };

    let wire_schema = Arc::new(Schema::new(vec![Field::new(
        "__opaque_state",
        DataType::Int64,
        true,
    )]));
    let wire_batch = RecordBatch::try_new(
        wire_schema,
        vec![Arc::new(Int64Array::from(vec![Some(7_i64)])) as ArrayRef],
    )
    .expect("wire batch");
    let wire_chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        wire_batch.schema().as_ref(),
        &[SlotId::new(41)],
    )
    .expect("wire chunk schema");
    let wire_chunk = Chunk::new_with_chunk_schema(wire_batch, wire_chunk_schema);

    let expected_schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(41),
            Field::new("__opaque_state", DataType::Binary, true),
            None,
            Some(41),
        )])
        .expect("expected chunk schema"),
    );
    register_expected_chunk_schema(key, 1, expected_schema).expect("register expected schema");

    let payload = online_unit(&[wire_chunk], true).expect("encode drifted chunk");
    let err = decode_chunks_for_sender(key, 3, 1, &payload)
        .expect_err("numeric payload must not pass as an opaque aggregate state");
    assert!(
        err.to_string().contains("recipe digest or version differs"),
        "err={err}"
    );

    cancel_exchange_key(key);
}

#[test]
fn decode_chunks_for_sender_decodes_with_wire_meta_before_schema_registration() {
    let key = ExchangeKey {
        finst_id_hi: 201,
        finst_id_lo: 202,
        node_id: 19,
    };
    let payload = online_unit(&[exchange_test_chunk("_cse_0")], true).expect("encode");
    assert!(decode_chunks_for_sender(key, 7, 1, &payload).is_err());
    register_expected_chunk_schema(
        key,
        1,
        plain_declaration(exchange_test_chunk("_cse_0").chunk_schema()),
    )
    .unwrap();
    let decoded = decode_chunks_for_sender(key, 7, 1, &payload).expect("decode after registration");
    assert_eq!(decoded.len(), 1);
    assert_eq!(
        decoded[0].chunk_schema().slots()[3].slot_id(),
        SlotId::new(32)
    );
    assert_eq!(decoded[0].schema().field(3).name(), "_cse_0");

    cancel_exchange_key(key);
}

#[test]
fn installed_expected_schema_retags_queued_wire_chunks() {
    let source = exchange_test_chunk("_cse_0");
    let expected_slots = [
        SlotId::new(133),
        SlotId::new(134),
        SlotId::new(131),
        SlotId::new(132),
    ];
    let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        source.record_batch().schema().as_ref(),
        &expected_slots,
    )
    .unwrap();
    let f = UnitFixture::new(plain_declaration(&schema), 1);
    let frame = f.frame(0, 1, online_unit(&[source], true).unwrap(), false);
    f.register(&frame);
    f.receive(frame).unwrap();
    let Some(ExchangePopResult::Chunk(chunk)) = f.handle().try_pop_next_with_stats(1).unwrap()
    else {
        panic!("expected queued chunk")
    };
    assert_eq!(chunk.chunk_schema().slot_ids(), &expected_slots);
    assert_eq!(f.slots().snapshot().in_use, 0);
}

#[test]
fn installed_expected_schema_preserves_queued_zero_column_row_count() {
    let row_count = 3;
    let chunk = exchange_test_zero_column_chunk(row_count);
    let f = UnitFixture::new(plain_declaration(chunk.chunk_schema()), 1);
    let frame = f.frame(0, 1, online_unit(&[chunk], true).unwrap(), false);
    f.register(&frame);
    f.receive(frame).unwrap();
    let Some(ExchangePopResult::Chunk(chunk)) = f.handle().try_pop_next_with_stats(1).unwrap()
    else {
        panic!("expected queued zero-column chunk")
    };
    assert_eq!(chunk.len(), row_count);
    assert_eq!(chunk.record_batch().num_columns(), 0);
    assert_eq!(f.slots().snapshot().in_use, 0);
}

#[test]
fn eos_completion_counts_unique_sender_instances() {
    let f = UnitFixture::new(plain_declaration(&ChunkSchema::empty()), 2);
    let first = f.end(0, 1);
    assert_eq!(
        f.receive(first.clone()).unwrap().verdict,
        ExchangeAdmissionVerdict::Applied
    );
    let snapshot = f.registry.snapshot_receiver_state(f.key).unwrap();
    assert_eq!(snapshot.expected_senders, 2);
    assert_eq!(snapshot.finished_senders, 1);
    assert_eq!(
        f.receive(first).unwrap().verdict,
        ExchangeAdmissionVerdict::Idempotent
    );
    assert_eq!(
        f.registry
            .snapshot_receiver_state(f.key)
            .unwrap()
            .finished_senders,
        1
    );
    f.receive(f.end(1, 1)).unwrap();
    assert_eq!(
        f.registry
            .snapshot_receiver_state(f.key)
            .unwrap()
            .finished_senders,
        2
    );
}

#[test]
fn native_senders_keep_independent_wire_metadata_by_exact_identity() {
    let first = exchange_test_chunk("_cse_0");
    let second_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        first.record_batch().schema().as_ref(),
        &[
            SlotId::new(133),
            SlotId::new(134),
            SlotId::new(131),
            SlotId::new(132),
        ],
    )
    .unwrap();
    let second = Chunk::new_with_chunk_schema(first.record_batch().clone(), second_schema);
    let f = UnitFixture::new(plain_declaration(first.chunk_schema()), 2);
    let first_payload = online_unit(&[first], true).unwrap();
    let second_payload = online_unit(&[second], true).unwrap();
    for (sender, payload) in [(0, first_payload), (1, second_payload.clone())] {
        let frame = f.frame(sender, 1, payload, false);
        f.register(&frame);
        f.receive(frame)
            .expect("each exact sender establishes independent wire metadata");
    }
    let changed = f.frame(0, 2, second_payload, false);
    f.register(&changed);
    f.receive(changed)
        .expect("local source slot IDs cannot change the frozen edge recipe");
    assert!(f.task.accepted().is_none());
    assert_eq!(f.slots().facts(f.streams[0]).unwrap().custody_through, 2);
    assert_eq!(f.slots().facts(f.streams[1]).unwrap().custody_through, 1);
}

// Compiled fragments allocate slot ids per fragment. The sender projects
// (b, a) from its own slots [1, 0]; the receiver imports (b', a') into its
// slots [0, 1]. Every wire id exists in the receiver schema with the same
// type, so a slot-id binding silently swaps the two columns.
#[test]
fn positional_receiver_binds_a_reversed_slot_sender_by_wire_position() {
    use super::ExchangeColumnBinding;
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };

    let sender_schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
    ]));
    let sender_batch = RecordBatch::try_new(
        Arc::clone(&sender_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        ],
    )
    .expect("sender batch");
    let sender_chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        sender_schema.as_ref(),
        &[SlotId::new(1), SlotId::new(0)],
    )
    .expect("sender chunk schema");
    let payload = online_unit(
        &[Chunk::new_with_chunk_schema(
            sender_batch,
            sender_chunk_schema,
        )],
        true,
    )
    .expect("encode sender payload");
    let receiver_schema = Schema::new(vec![
        Field::new("b_import", DataType::Int64, false),
        Field::new("a_import", DataType::Int64, false),
    ]);
    let storage = ValueTypeStorageLifetime::pure();
    let slots = receiver_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let value =
                ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int64), false, storage.clone())
                    .unwrap();
            let binding = TypedValueBinding::try_new(
                value,
                ValueLayout::try_new(&DataType::Int64, storage.clone()).unwrap(),
            )
            .unwrap();
            ChunkSlotSchema::try_new_typed(
                SlotId::new(ordinal as u32),
                Arc::clone(field),
                binding,
                None,
                storage.clone(),
            )
            .unwrap()
        })
        .collect();
    let expected = Arc::new(ChunkSchema::try_new_typed(slots, storage).unwrap());
    let sender = ExchangeSenderIdentity::native(UniqueId::new(1, 1), 0);
    let column = |chunk: &Chunk, slot: u32| -> Vec<i64> {
        chunk
            .column_by_slot_id(SlotId::new(slot))
            .expect("slot column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column")
            .values()
            .to_vec()
    };
    let registry = Arc::new(ExecutionExchangeRegistry::default());

    let positional = ExchangeKey {
        finst_id_hi: 601,
        finst_id_lo: 602,
        node_id: 3,
    };
    registry
        .try_register_expected_chunk_schema_with_binding(
            positional,
            1,
            Arc::clone(&expected),
            ExchangeColumnBinding::Positional,
            crate::runtime::fragment::exchange::test_receiver_creation_factory(),
        )
        .expect("register positional receiver");
    let decoded = registry
        .decode_chunks_for_sender(positional, sender, &payload)
        .expect("positional decode");
    assert_eq!(decoded.len(), 1);
    assert!(decoded[0].chunk_schema().is_typed());
    assert!(Arc::ptr_eq(&decoded[0].chunk_schema_ref(), &expected));
    assert_eq!(
        decoded[0]
            .chunk_schema()
            .slots()
            .iter()
            .map(|slot| slot.slot_id())
            .collect::<Vec<_>>(),
        vec![SlotId::new(0), SlotId::new(1)]
    );
    assert_eq!(column(&decoded[0], 0), vec![10, 20]);
    assert_eq!(column(&decoded[0], 1), vec![1, 2]);

    // Once the declaration is installed, real Unit admission and handoff
    // retain the same positional retagging as the pure decoder.
    let late = ExchangeKey {
        node_id: 5,
        ..positional
    };
    assert!(
        registry
            .decode_chunks_for_sender(late, sender, &payload)
            .is_err()
    );
    registry
        .try_register_expected_chunk_schema_with_binding(
            late,
            1,
            Arc::clone(&expected),
            ExchangeColumnBinding::Positional,
            crate::runtime::fragment::exchange::test_receiver_creation_factory(),
        )
        .expect("late positional registration");
    let unit = UnitFixture::bind_registered(registry.clone(), late, 1);
    let frame = unit.frame(0, 1, payload.clone(), false);
    unit.register(&frame);
    unit.receive(frame).unwrap();
    let handle = unit.handle();
    let Some(ExchangePopResult::Chunk(retagged)) =
        handle.try_pop_next_with_stats(1).expect("pop queued chunk")
    else {
        panic!("expected the queued chunk");
    };
    assert_eq!(column(&retagged, 0), vec![10, 20]);
    assert_eq!(column(&retagged, 1), vec![1, 2]);
    assert!(retagged.chunk_schema().is_typed());
    assert!(Arc::ptr_eq(&retagged.chunk_schema_ref(), &expected));

    // The default registration has the same frozen ordinal contract; local
    // source IDs cannot swap equal-typed columns through another entrypoint.
    let default_key = ExchangeKey {
        node_id: 4,
        ..positional
    };
    registry
        .try_register_expected_chunk_schema(
            default_key,
            1,
            expected.clone(),
            crate::runtime::fragment::exchange::test_receiver_creation_factory(),
        )
        .unwrap();
    let decoded = registry
        .decode_chunks_for_sender(default_key, sender, &payload)
        .unwrap();
    assert_eq!(column(&decoded[0], 0), vec![10, 20]);
    assert_eq!(column(&decoded[0], 1), vec![1, 2]);
    assert!(
        registry
            .try_register_expected_chunk_schema(
                positional,
                1,
                expected,
                crate::runtime::fragment::exchange::test_receiver_creation_factory()
            )
            .unwrap_err()
            .to_string()
            .contains("already registered")
    );
}

fn native_stream_fixture() -> (UnitFixture, ExchangeUnitReceiverFrame, Chunk) {
    let chunk = exchange_test_chunk("k2");
    let f = UnitFixture::new(plain_declaration(chunk.chunk_schema()), 1);
    let frame = f.frame(
        0,
        1,
        online_unit(std::slice::from_ref(&chunk), true).unwrap(),
        false,
    );
    (f, frame, chunk)
}

#[test]
fn native_exchange_decode_failure_does_not_advance_and_unit_replays_are_idempotent() {
    // A real decoder refusal retains its Task source; physical lease release
    // neither advances custody nor turns that refusal into a retry success.
    let (invalid_fixture, frame, _) = native_stream_fixture();
    let invalid = invalid_fixture.frame(0, 1, vec![0xff], false);
    invalid_fixture.register(&invalid);
    let error = invalid_fixture
        .receive(invalid)
        .unwrap_err()
        .into_execution();
    let accepted = invalid_fixture.task.accepted().unwrap();
    assert!(
        accepted
            .originating_failure()
            .unwrap()
            .same_occurrence(&error)
    );
    assert_eq!(
        invalid_fixture
            .slots()
            .facts(frame.stream)
            .unwrap()
            .custody_through,
        0
    );
    assert_eq!(invalid_fixture.slots().snapshot().decoding, 0);
    assert_eq!(
        invalid_fixture.registry.receivers.lock().unwrap()[&invalid_fixture.key]
            .mu
            .lock()
            .unwrap()
            .recv_request_count,
        0
    );

    let (f, frame, _) = native_stream_fixture();
    let ungranted_gap = f.frame(0, 2, frame.payload.to_vec(), false);
    let ExchangeUnitItem::Unit { demand, .. } = ungranted_gap.item else {
        unreachable!()
    };
    assert!(
        f.registry
            .register_unit_demand(
                f.key,
                ExchangeDemandEntry {
                    stream: frame.stream,
                    operation: ExchangeDemandOperation::Register(demand),
                }
            )
            .is_err(),
        "demand registration cannot skip the first unit"
    );
    assert!(
        f.receive(ungranted_gap).is_err(),
        "an ungranted gap cannot create custody"
    );
    assert_eq!(f.slots().facts(frame.stream).unwrap().custody_through, 0);
    f.register(&frame);
    assert_eq!(
        f.receive(frame.clone()).unwrap().verdict,
        ExchangeAdmissionVerdict::Applied
    );
    assert_eq!(
        f.receive(frame.clone()).unwrap().verdict,
        ExchangeAdmissionVerdict::Idempotent
    );
    let end = f.end(0, 2);
    assert_eq!(
        f.receive(end.clone()).unwrap().verdict,
        ExchangeAdmissionVerdict::Applied
    );
    assert_eq!(
        f.receive(end).unwrap().verdict,
        ExchangeAdmissionVerdict::Idempotent
    );
    let after = f.frame(0, 3, frame.payload.to_vec(), false);
    let ExchangeUnitItem::Unit { demand, .. } = after.item else {
        unreachable!()
    };
    assert!(
        f.registry
            .register_unit_demand(
                f.key,
                ExchangeDemandEntry {
                    stream: frame.stream,
                    operation: ExchangeDemandOperation::Register(demand),
                }
            )
            .is_err(),
        "data beyond the observed End cannot acquire a grant"
    );
    assert!(f.receive(after).is_err());
    let received_requests = f.registry.receivers.lock().unwrap()[&f.key]
        .mu
        .lock()
        .unwrap()
        .recv_request_count;
    // The data Unit and the separate End each count once; neither replay
    // creates another request in the receiver's original statistics.
    assert_eq!(received_requests, 2);
    assert_eq!(
        f.registry
            .snapshot_receiver_state(f.key)
            .unwrap()
            .queued_chunks,
        1
    );

    // Granted units may arrive out of order, but neither custody nor the
    // Source queue can skip the missing prefix. This replaces the old wire
    // implementation's blanket gap refusal with the Unit contract.
    let (reordered, first, _) = native_stream_fixture();
    let second = reordered.frame(0, 2, first.payload.to_vec(), false);
    reordered.register(&first);
    reordered.register(&second);
    assert_eq!(reordered.receive(second).unwrap().facts.custody_through, 0);
    assert!(
        reordered
            .handle()
            .try_pop_next_with_stats(1)
            .unwrap()
            .is_none()
    );
    assert!(reordered.handle().complete_input_consumption(1).is_err());
    assert_eq!(reordered.receive(first).unwrap().facts.custody_through, 2);
    assert_eq!(
        reordered
            .registry
            .snapshot_receiver_state(reordered.key)
            .unwrap()
            .queued_chunks,
        2
    );
}

#[test]
fn native_exchange_invalid_schema_retains_its_task_failure_without_custody() {
    let (f, frame, _) = native_stream_fixture();
    let incompatible = exchange_test_zero_column_chunk(1);
    let invalid = f.frame(0, 1, online_unit(&[incompatible], true).unwrap(), false);
    f.register(&invalid);
    let error = f.receive(invalid).unwrap_err().into_execution();
    assert!(
        f.task
            .accepted()
            .unwrap()
            .originating_failure()
            .unwrap()
            .same_occurrence(&error)
    );
    assert_eq!(f.slots().facts(frame.stream).unwrap().custody_through, 0);
    assert_eq!(
        f.registry
            .snapshot_receiver_state(f.key)
            .unwrap()
            .queued_chunks,
        0
    );
}

#[test]
fn native_exchange_consumed_requires_source_exhaustion_and_retains_final_data_end_sequence() {
    use super::{ExchangeCloseDisposition, SourcedExchangePopResult};
    let (f, mut frame, chunk) = native_stream_fixture();
    let ExchangeUnitItem::Unit { end_after_unit, .. } = &mut frame.item else {
        unreachable!()
    };
    *end_after_unit = true;
    let stream = frame.stream;
    f.register(&frame);
    f.receive(frame).unwrap();
    let handle = f.handle();
    assert!(handle.has_output_or_finished(1));
    assert_eq!(
        f.registry.stream_close_facts(f.key, &[stream]).unwrap()[0].disposition,
        ExchangeCloseDisposition::Withdrawn
    );
    assert!(handle.complete_input_consumption(1).is_err());
    let Some(SourcedExchangePopResult::Chunk(data)) = handle.try_pop_sourced_with_stats(1).unwrap()
    else {
        panic!("source data is retained")
    };
    assert_eq!(data.source(), Some(stream.source()));
    assert_eq!(data.len(), chunk.len());
    assert!(matches!(
        handle.try_pop_sourced_with_stats(1).unwrap(),
        Some(SourcedExchangePopResult::Finished(_))
    ));
    assert_eq!(
        f.registry.stream_close_facts(f.key, &[stream]).unwrap()[0].disposition,
        ExchangeCloseDisposition::Withdrawn
    );
    handle.complete_input_consumption(1).unwrap();
    assert_eq!(
        f.registry.stream_close_facts(f.key, &[stream]).unwrap()[0].disposition,
        ExchangeCloseDisposition::Consumed {
            final_sequence: ExchangeUnitSequence::try_new(2).unwrap()
        }
    );
}

#[test]
fn native_exchange_empty_end_is_consumed_only_at_input_exhaustion() {
    use super::ExchangeCloseDisposition;
    let (f, _, _) = native_stream_fixture();
    let frame = f.end(0, 1);
    let stream = frame.stream;
    f.receive(frame).unwrap();
    assert_eq!(
        f.registry.stream_close_facts(f.key, &[stream]).unwrap()[0].disposition,
        ExchangeCloseDisposition::Withdrawn
    );
    f.handle().complete_input_consumption(1).unwrap();
    assert_eq!(
        f.registry.stream_close_facts(f.key, &[stream]).unwrap()[0].disposition,
        ExchangeCloseDisposition::Consumed {
            final_sequence: ExchangeUnitSequence::FIRST
        }
    );
}

#[test]
fn native_exchange_concurrent_same_sequence_has_one_admission() {
    let (f, frame, _) = native_stream_fixture();
    f.register(&frame);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let handles = (0..2)
        .map(|_| {
            let registry = f.registry.clone();
            let key = f.key;
            let barrier = barrier.clone();
            let frame = frame.clone();
            std::thread::spawn(move || {
                barrier.wait();
                registry.receive_unit(key, frame).unwrap().verdict
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let verdicts = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        verdicts
            .iter()
            .filter(|v| **v == ExchangeAdmissionVerdict::Applied)
            .count(),
        1
    );
    assert_eq!(
        verdicts
            .iter()
            .filter(|v| **v == ExchangeAdmissionVerdict::Idempotent)
            .count(),
        1
    );
    assert_eq!(
        f.registry.receivers.lock().unwrap()[&f.key]
            .mu
            .lock()
            .unwrap()
            .recv_request_count,
        1
    );
    assert_eq!(
        f.registry
            .snapshot_receiver_state(f.key)
            .unwrap()
            .queued_chunks,
        1
    );
}

// Instrument only the first real Arrow construction after metadata is cached.
// The original creator still owns every allocation, refusal and lifetime.
struct PausedDecoderFactory {
    inner: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    armed: std::sync::atomic::AtomicBool,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl crate::exec::pipeline::operator::InputConstructionFactory for PausedDecoderFactory {
    fn retain_decoded_source(
        &self,
        array: ArrayRef,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        work: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    ) -> Result<ArrayRef, crate::exec::pipeline::operator::ConstructionFailure> {
        self.inner.retain_decoded_source(array, storage, work)
    }

    fn observe_input_work(
        &self,
        units: usize,
    ) -> Result<(), crate::exec::pipeline::operator::ConstructionFailure> {
        self.inner.observe_input_work(units)
    }
    fn begin_typed_construction(
        &self,
        schema: &ChunkSchema,
    ) -> Result<
        Box<dyn crate::exec::pipeline::operator::InputConstructionOwner>,
        crate::exec::pipeline::operator::ConstructionFailure,
    > {
        if self.armed.swap(false, std::sync::atomic::Ordering::AcqRel) {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        self.inner.begin_typed_construction(schema)
    }
    fn begin_compiled_layout_construction(
        &self,
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<
        Box<dyn crate::exec::pipeline::operator::InputConstructionOwner>,
        crate::exec::pipeline::operator::ConstructionFailure,
    > {
        self.inner.begin_compiled_layout_construction(layout)
    }
}
fn paused_decoder_fixture(
    senders: usize,
) -> (
    UnitFixture,
    ExchangeUnitReceiverFrame,
    std::sync::mpsc::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (entered, observed) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let factory = Arc::new(PausedDecoderFactory {
        inner: crate::runtime::fragment::exchange::test_receiver_creation_factory(),
        armed: std::sync::atomic::AtomicBool::new(false),
        entered,
        release: std::sync::Mutex::new(released),
    });
    let chunk = exchange_test_chunk("k2");
    let f = UnitFixture::with_factory(
        plain_declaration(chunk.chunk_schema()),
        senders,
        factory.clone(),
    );
    let full = online_unit(std::slice::from_ref(&chunk), true).unwrap();
    for stream in &f.streams {
        f.registry
            .decode_chunks_for_sender(
                f.key,
                ExchangeSenderIdentity::native(
                    stream.source().fragment_instance_id(),
                    stream.source().sender_ordinal(),
                ),
                &full,
            )
            .unwrap();
    }
    // No metadata construction runs under the receiver lock on this payload:
    // both exact senders retain the metadata established immediately above.
    let frame = f.frame(0, 1, online_unit(&[chunk], false).unwrap(), false);
    f.register(&frame);
    factory
        .armed
        .store(true, std::sync::atomic::Ordering::Release);
    (f, frame, observed, release)
}
#[test]
fn native_exchange_one_stream_decoder_does_not_serialize_other_sources() {
    let (f, first, entered, release) = paused_decoder_fixture(2);
    let second = f.frame(1, 1, first.payload.to_vec(), false);
    f.register(&second);
    let registry = f.registry.clone();
    let key = f.key;
    let worker = std::thread::spawn(move || registry.receive_unit(key, first));
    entered
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("first actual decoder entered");
    assert_eq!(f.slots().snapshot().decoding, 1);
    let (sent, received) = std::sync::mpsc::channel();
    let registry = f.registry.clone();
    let other = std::thread::spawn(move || sent.send(registry.receive_unit(key, second)).unwrap());
    let result = received.recv_timeout(std::time::Duration::from_secs(2));
    release.send(()).unwrap();
    worker.join().unwrap().unwrap();
    other.join().unwrap();
    result
        .expect("another source progresses while one actual decoder is occupied")
        .unwrap();
    assert_eq!(
        f.registry
            .snapshot_receiver_state(f.key)
            .unwrap()
            .queued_chunks,
        2
    );
}
#[test]
fn native_exchange_cancellation_retains_slots_until_actual_decoder_exit() {
    let (f, frame, entered, release) = paused_decoder_fixture(1);
    let registry = f.registry.clone();
    let key = f.key;
    let worker = std::thread::spawn(move || registry.receive_unit(key, frame));
    entered
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("actual decoder entered");
    let slots = f.slots();
    f.registry.cancel_exchange_key(key);
    assert_eq!(slots.snapshot().decoding, 1);
    assert_eq!(slots.snapshot().in_use, 1);
    assert_eq!(
        slots.facts(f.streams[0]).unwrap().receiver_closed,
        Some(super::ExchangeCloseDisposition::Withdrawn)
    );
    release.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert_eq!(slots.snapshot().decoding, 0);
    assert_eq!(slots.snapshot().in_use, 0);
    assert!(
        f.task.accepted().is_none(),
        "normal cleanup cannot author a decoder failure"
    );
}
