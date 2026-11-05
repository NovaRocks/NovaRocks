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

//! Extend the real ordered-source payload through the existing partition,
//! writer and receiver owners. The literal multiset is independent of hash
//! placement and decoding. Sender and receiver creation responsibilities stay
//! separate. The receiver projection carries its original metadata; its bare
//! copied payload Buffer is also readable after that projection exits.
use super::*;
use crate::exec::operators::data_stream_sink::partition_chunk_by_hash_arrays_created;
use crate::runtime::exchange::{ExchangeColumnBinding, ExchangePopResult, online_unit};
use crate::runtime::task::io::exchange::test_support::Fixture;
use crate::runtime::task::io::{ExchangeReceiverKey, ExchangeReceiverRegistration};
use novarocks_types::UniqueId;

pub(super) fn retain_received_buffer(
    input: &Chunk,
    layout: &novarocks_local_program::StaticLayout,
    sender: &dyn InputConstructionFactory,
    control: &RuntimeKernelControl,
) -> (
    crate::exec::chunk::ChunkBatchProjection,
    arrow::buffer::Buffer,
    Arc<MemTracker>,
) {
    let parts = partition_chunk_by_hash_arrays_created(
        input,
        &[input.columns()[0].clone()],
        3,
        false,
        sender,
        &|| control.checkpoint_execution(),
    )
    .unwrap()
    .into_iter()
    .filter(|c| !c.is_empty())
    .collect::<Vec<_>>();
    assert_eq!(parts.iter().map(Chunk::len).sum::<usize>(), 4);
    let wire_schema = sender.create_compiled_chunk_schema(layout).unwrap();
    let recipe = online_unit::prepare_recipe(&wire_schema, &[], sender).unwrap();
    let tracker = MemTracker::new_root("complete-ordered-exchange-receiver");
    tracker.install_limit_once(64 << 20).unwrap();
    let fixture = Fixture::new();
    let destination = UniqueId::new(0xc28, 2);
    let source = UniqueId::new(0xc28, 1);
    let original = fixture.state(destination);
    let runtime = crate::runtime::runtime_state::RuntimeState::new(
        None,
        None,
        None,
        Some(destination),
        None,
        Some(tracker.clone()),
        Some(fixture.runtime.clone()),
        original.error_state().clone(),
    );
    let factory = construction(&tracker, &runtime);
    assert_eq!(layout.slots().len(), 2);
    // A separate BE owns an independent frozen declaration. Only the exact
    // destination slot mapping crosses this fixture boundary; producer type
    // storage must not be retained by receiver schemas or decoded buffers.
    let receiver_layout = crate::exec::chunk::typed_fixture::layout(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("receiver_key", DataType::Utf8, true),
            arrow::datatypes::Field::new("receiver_payload", DataType::FixedSizeBinary(16), true),
        ])),
        layout.slots(),
        &[
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        ],
    );
    let expected = factory
        .create_compiled_chunk_schema(&receiver_layout)
        .unwrap();
    assert_eq!(
        expected.slots()[0].typed_binding(),
        input.chunk_schema().slots()[0].typed_binding()
    );
    assert_eq!(
        expected.slots()[1].typed_binding(),
        input.chunk_schema().slots()[1].typed_binding()
    );
    let port = fixture.port.clone();
    let key = ExchangeReceiverKey {
        task_key: UniqueId::new(0xc28, 2),
        node_id: 7,
    };
    port.register(ExchangeReceiverRegistration {
        key,
        expected_senders: 1,
        wire_chunk_schema: expected.clone(),
        expected_chunk_schema: expected,
        column_binding: ExchangeColumnBinding::Positional,
        creation_factory: factory.clone(),
    })
    .unwrap();
    port.ensure_mem_tracker(key, &tracker).unwrap();
    let receiver_pin = port.receiver_pin(key, 1).unwrap();
    fixture.bind(key, &[source]);
    let stream = fixture.stream(source, destination, key.node_id, 0);
    let mut actual = Vec::new();
    let mut retained = None;
    let mut projection = None;
    for (ordinal, part) in parts.iter().enumerate() {
        let payload = online_unit::encode(&recipe, part, sender, control).unwrap();
        let demand = crate::runtime::task::io::exchange_unit::ExchangeUnitDemand {
            sequence: crate::runtime::task::io::exchange_unit::ExchangeUnitSequence::try_new(
                u64::try_from(ordinal + 1).unwrap(),
            )
            .unwrap(),
            wire_bytes: payload.len() as u64,
        };
        port.register_unit_demand(
            key,
            crate::runtime::task::io::exchange_unit::ExchangeDemandEntry {
                stream,
                operation:
                    crate::runtime::task::io::exchange_unit::ExchangeDemandOperation::Register(
                        demand,
                    ),
            },
        )
        .unwrap();
        port.receive_unit(
            key,
            crate::runtime::task::io::exchange_unit::ExchangeUnitReceiverFrame {
                stream,
                sender_count: 1,
                sender_id: 0,
                backend_number: 0,
                item: crate::runtime::task::io::exchange_unit::ExchangeUnitItem::Unit {
                    demand,
                    end_after_unit: ordinal + 1 == parts.len(),
                },
                payload: bytes::Bytes::from(payload),
                next_demand: None,
            },
        )
        .unwrap();
        match receiver_pin
            .try_pop_next_with_stats(1)
            .unwrap()
            .expect("synchronous delivered frame")
        {
            ExchangePopResult::Chunk(chunk) => {
                assert_eq!(
                    chunk.chunk_schema().slots()[0].typed_binding(),
                    input.chunk_schema().slots()[0].typed_binding()
                );
                assert_eq!(
                    chunk.chunk_schema().slots()[1].typed_binding(),
                    input.chunk_schema().slots()[1].typed_binding()
                );
                let keys = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = chunk.columns()[1]
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                for row in 0..chunk.len() {
                    let key = if keys.is_null(row) {
                        None
                    } else {
                        Some(keys.value(row).to_owned())
                    };
                    assert!(!values.is_null(row));
                    let bytes: [u8; 16] = values.value(row).try_into().unwrap();
                    actual.push((key, bytes));
                }
                retained = Some(chunk.columns()[1].to_data().buffers()[0].clone());
                projection = Some(chunk.into_batch_projection());
            }
            ExchangePopResult::Finished(_) => panic!("every real Unit must hand off its batch"),
        }
    }
    drop(parts);
    let ExchangePopResult::Finished(stats) = receiver_pin
        .try_pop_next_with_stats(1)
        .unwrap()
        .expect("the final Unit carries the real End")
    else {
        panic!("all partition batches must precede the final End");
    };
    assert_eq!(stats.rows_received, 4);
    actual.sort();
    let mut expected = vec![
        (None, 2_i128.to_be_bytes()),
        (None, 7_i128.to_be_bytes()),
        (Some("a".to_owned()), 1_i128.to_be_bytes()),
        (Some("a".to_owned()), 10_i128.to_be_bytes()),
    ];
    expected.sort();
    assert_eq!(actual, expected);
    let retained = retained.unwrap();
    let projection = projection.unwrap();
    port.remove(key);
    drop(receiver_pin);
    drop(port);
    drop(fixture);
    drop(original);
    drop(factory);
    drop(runtime);
    assert!(
        tracker.current() > 0,
        "the receiver typed projection retains its original construction stock"
    );
    (projection, retained, tracker)
}
