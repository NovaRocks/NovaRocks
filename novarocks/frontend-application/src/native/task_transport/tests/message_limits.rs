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

use super::*;
use crate::task_execution::creation::{
    AttemptTopology, CreateTaskIntent, TaskCreationFacts, TaskCreationSeed, TaskTopologyView,
};
use crate::task_execution::intent::{NativeRequestWindows, TaskOperationQueuePreparation};
use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
use novarocks_task_codec::TransportMessageLimits;
use prost::Message;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

fn create(backend: BackendProcessId, padding: usize) -> OperationIntent {
    let identity = identity(1, backend);
    let view = TaskTopologyView::new(Vec::new(), BTreeMap::new());
    let topology = Arc::new(AttemptTopology::try_new(Vec::new(), &[&view]).unwrap());
    let fragment = system_fragment(padding);
    let seed = TaskCreationSeed::new(
        topology,
        view,
        fragment,
        Arc::from([]),
        TaskCreationFacts {
            identity,
            participation: participation(backend),
            task_key: novarocks_types::UniqueId::new(1, 1),
            pipeline_dop: NonZeroUsize::new(1).unwrap(),
            split_plan_nodes: Vec::new(),
            instance_ordinal: 0,
            initial_scan_ranges: BTreeMap::new(),
        },
    );
    let lengths = seed.lengths().unwrap();
    let parts = seed.freeze(lengths).unwrap();
    OperationIntent::CreateTask(CreateTaskIntent::new(
        novarocks_execution::task_execution::OperationEnvelope::with_default_wait(
            TaskOperationId::new_v7(),
            OperationKind::CreateTask,
        ),
        identity,
        parts,
        None,
    ))
}

fn system_fragment(
    padding: usize,
) -> Arc<crate::native::fragment_encoder::frozen::FragmentArtifact> {
    use arrow::{
        array::BinaryArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use novarocks_physical_plan as p;
    use novarocks_type_contract::{
        CompilePhase, ExpressionControlFlow, FunctionValueType, SemanticParameters,
        ValueTypeStorageLifetime,
    };
    let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
    let node = p::NodeId::new(0);
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(1));
    let ty = FunctionValueType::new(DataType::Binary, false);
    let value = builder
        .add_value(
            ty,
            p::ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let field = Arc::new(Field::new("payload", DataType::Binary, false));
    let payload = vec![0x71; padding];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field.clone()])),
        vec![Arc::new(BinaryArray::from_iter_values(
            [payload.as_slice()],
        ))],
    )
    .unwrap();
    builder
        .add_system_relation(
            node,
            Box::from([value]),
            Arc::from([field]),
            p::SystemRelationData::Batches {
                batches: Arc::from([batch]),
                storage: ValueTypeStorageLifetime::pure(),
            },
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            p::FragmentSink::Noop,
            p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            p::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let flow = ExpressionControlFlow::try_new(
        vec![],
        vec![],
        fragment.expressions(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let expression_uses = p::PhysicalRootUses::try_new(&fragment, flow, vec![], &control).unwrap();
    let calls =
        p::FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &control).unwrap();
    let pruning = p::FrozenFragmentPruning::try_new(fragment.id(), vec![], &control).unwrap();
    let package = p::FragmentPackage::try_new(
        p::FragmentPackageInput {
            version: p::PlanVersionId::try_new([0x72; 16]).unwrap(),
            required: p::RequiredContracts::default(),
            fragment,
            expression_uses,
            calls,
            pruning,
            constants: p::ConstantPools::empty(),
            cuts: p::FragmentCuts::default(),
            result: None,
            parameters: SemanticParameters::default(),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
        &control,
    )
    .unwrap();
    crate::native::fragment_encoder::frozen::FragmentArtifact::freeze_package(
        &package,
        &BTreeMap::new(),
        &crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
            novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
            novarocks_plan_codec::physical_package_v2::PackageEncodeLimits::FROZEN,
        ),
        &control,
    )
    .unwrap()
}

fn budget(limits: TransportMessageLimits, target: usize) -> TransportBudget {
    let defaults = TransportBudget::DEFAULT;
    TransportBudget::new(
        defaults.max_batch_items(),
        target,
        limits,
        defaults.max_query_backend_queued_operations(),
        defaults.max_query_backend_queued_bytes(),
        defaults.max_backend_queued_operations(),
        defaults.max_backend_queued_bytes(),
        defaults.max_tasks_per_participation(),
        defaults.max_active_tasks_per_backend(),
        defaults.frontend_queue_residence(),
    )
    .unwrap()
}

fn minimum_windows(limits: TransportMessageLimits) -> TransportBudget {
    let initial = budget(limits, 48 * 1024 * 1024);
    let windows = NativeRequestWindows::for_transport(initial).unwrap();
    TransportBudget::new(
        initial.max_batch_items(),
        initial.batch_target_encoded_bytes(),
        limits,
        initial.max_query_backend_queued_operations(),
        windows.ordinary_queued_max_bytes + windows.ordinary_request_max_bytes,
        initial.max_backend_queued_operations(),
        windows.ordinary_queued_max_bytes
            + windows.ordinary_request_max_bytes
            + windows.control_reserved_bytes,
        initial.max_tasks_per_participation(),
        initial.max_active_tasks_per_backend(),
        initial.frontend_queue_residence(),
    )
    .unwrap()
}

#[test]
fn original_queue_bound_is_required_even_when_wire_length_alone_would_fit() {
    let transport = minimum_windows(TransportMessageLimits::DEFAULT);
    let windows = NativeRequestWindows::for_transport(transport).unwrap();
    assert!(windows.ordinary_queued_max_bytes > windows.ordinary_request_max_bytes);
    let understated = TransportBudget::new(
        transport.max_batch_items(),
        transport.batch_target_encoded_bytes(),
        transport.message_limits(),
        transport.max_query_backend_queued_operations(),
        transport.max_query_backend_queued_bytes() - 1,
        transport.max_backend_queued_operations(),
        transport.max_backend_queued_bytes(),
        transport.max_tasks_per_participation(),
        transport.max_active_tasks_per_backend(),
        transport.frontend_queue_residence(),
    )
    .unwrap();
    assert!(NativeRequestWindows::for_transport(understated).is_err());
}

#[test]
fn small_operations_keep_grouping_at_the_target_without_losing_the_next_head() {
    let backend = BackendProcessId::new_v7();
    let intents: Vec<_> = (1..=3).map(|task| cancel_intent(task, backend)).collect();
    let ids: Vec<_> = intents.iter().map(OperationIntent::operation_id).collect();
    let transport = budget(
        TransportMessageLimits::DEFAULT,
        2 * intents[0].queued_bytes(),
    );
    let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, transport);
    for intent in intents {
        dispatcher
            .enqueue(intent, MonotonicInstant::ORIGIN)
            .unwrap();
    }
    let first = dispatcher.take_batch().unwrap();
    assert_eq!(
        first
            .operations()
            .iter()
            .map(OperationIntent::operation_id)
            .collect::<Vec<_>>(),
        ids[..2]
    );
    let last = dispatcher.take_batch().unwrap();
    assert_eq!(last.operations()[0].operation_id(), ids[2]);
    assert!(dispatcher.take_batch().is_none());
}

pub(super) async fn close(fixture: SinkFixture) {
    let SinkFixture {
        mut loopback,
        sink,
        acks,
        mut runtime,
        governed,
    } = fixture;
    drop(sink);
    drop(acks);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    runtime.drain_original_apply_until(deadline).await.unwrap();
    runtime
        .drain_original_subscriptions_until(deadline)
        .await
        .unwrap();
    runtime.drain_native_outgoing_until(deadline).await.unwrap();
    drop(runtime);
    assert!(loopback.settle().await);
    drop(loopback);
    assert!(governed.finish().is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_create_above_the_old_grouping_target_reaches_the_production_decoder() {
    for (padding, limits) in [
        (49 * 1024 * 1024, TransportMessageLimits::DEFAULT),
        (
            65 * 1024 * 1024,
            TransportMessageLimits {
                ordinary_request_max_bytes: 96 * 1024 * 1024,
                ordinary_response_max_bytes: 96 * 1024 * 1024,
                ..TransportMessageLimits::DEFAULT
            },
        ),
    ] {
        let backend = BackendProcessId::new_v7();
        let intent = create(backend, padding);
        let bytes = intent.encoded_protobuf_bytes().unwrap();
        let transport = budget(limits, 48 * 1024 * 1024);
        assert!(bytes > transport.batch_target_encoded_bytes());
        assert!(bytes < transport.message_limits().ordinary_request_max_bytes);
        let fixture = sink_fixture_with_budget(Loopback::start().await, backend, transport);
        fixture
            .loopback
            .peer
            .expect_apply(ApplyAnswer::WithAck(vec![(
                OperationOutcome::Accepted,
                Some(proto::task_operation_verdict::Ack::CreateTask(
                    codec::encode_create_task_ack(&CreateTaskReceipt::new(
                        identity(1, backend),
                        Vec::new(),
                        TaskStatus::created(identity(1, backend)),
                    ))
                    .unwrap(),
                )),
            )]));
        let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, transport);
        dispatcher
            .enqueue(intent, MonotonicInstant::ORIGIN)
            .unwrap();
        let batch = dispatcher.take_batch().unwrap();
        assert_eq!(batch.operations().len(), 1);
        submit_batch(&fixture, batch);
        let acknowledgements = settled(&fixture.acks, 1).await;
        assert_eq!(
            acknowledgements[0].worker_outcome(),
            Some(OperationOutcome::Accepted)
        );
        let request = fixture.loopback.peer.applied().pop().unwrap();
        assert_eq!(request.encoded_len(), bytes);
        let encoded = request.encode_to_vec();
        novarocks_task_codec::resource_preflight::check_operation_batch(&encoded).unwrap();
        let decoded = novarocks_task_codec::operation::decode_operation_batch(
            &request,
            transport,
            novarocks_proto_codec::FieldPath::root("request"),
        )
        .unwrap();
        assert_eq!(decoded.len(), 1);
        drop(acknowledgements);
        close(fixture).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_message_limit_with_larger_queued_carrier_fits_minimum_windows() {
    let backend = BackendProcessId::new_v7();
    let intent = create(backend, 1024);
    let exact = intent.encoded_protobuf_bytes().unwrap();
    assert!(intent.queued_bytes() > exact);
    let limits = TransportMessageLimits {
        ordinary_request_max_bytes: exact,
        control_request_max_bytes: exact,
        ..TransportMessageLimits::DEFAULT
    };
    let transport = minimum_windows(limits);
    let windows = NativeRequestWindows::for_transport(transport).unwrap();
    assert_eq!(
        windows.control_reserved_bytes,
        windows.control_queued_max_bytes + exact
    );
    let fixture = sink_fixture_with_budget(Loopback::start().await, backend, transport);
    fixture
        .loopback
        .peer
        .expect_apply(ApplyAnswer::WithAck(vec![(
            OperationOutcome::Accepted,
            Some(proto::task_operation_verdict::Ack::CreateTask(
                codec::encode_create_task_ack(&CreateTaskReceipt::new(
                    identity(1, backend),
                    Vec::new(),
                    TaskStatus::created(identity(1, backend)),
                ))
                .unwrap(),
            )),
        )]));
    let mut dispatcher = OperationDispatcher::new(DispatchBudget::DEFAULT, transport);
    dispatcher
        .enqueue(intent, MonotonicInstant::ORIGIN)
        .unwrap();
    submit_batch(&fixture, dispatcher.take_batch().unwrap());
    let acknowledgements = settled(&fixture.acks, 1).await;
    assert_eq!(
        acknowledgements[0].worker_outcome(),
        Some(OperationOutcome::Accepted)
    );
    assert_eq!(fixture.loopback.peer.applied()[0].encoded_len(), exact);
    drop(acknowledgements);
    close(fixture).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn wire_limit_is_checked_before_any_queue_reservation() {
    let backend = BackendProcessId::new_v7();
    let intent = create(backend, 1024);
    let exact = intent.encoded_protobuf_bytes().unwrap();
    let limits = TransportMessageLimits {
        ordinary_request_max_bytes: exact - 1,
        control_request_max_bytes: exact - 1,
        ..TransportMessageLimits::DEFAULT
    };
    let transport = minimum_windows(limits);
    let fixture = sink_fixture_with_budget(Loopback::start().await, backend, transport);
    let before = fixture.runtime.task_transport_supervisor().snapshot();
    let mut preparation = TaskOperationQueuePreparation::new();
    let TaskOperationQueueAdmission::Rejected(error) = fixture
        .sink
        .try_reserve_queue(intent.queue_request(), &mut preparation)
    else {
        panic!("an indivisible operation must fail before reserving its queue");
    };
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
    assert_eq!(
        fixture.runtime.task_transport_supervisor().snapshot(),
        before
    );
    assert!(fixture.loopback.peer.applied().is_empty());
    drop(preparation);
    close(fixture).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_impossible_queued_carrier_is_rejected_without_reserving_the_original_window() {
    let backend = BackendProcessId::new_v7();
    let transport = TransportBudget::DEFAULT;
    let capacity = NativeRequestWindows::for_transport(transport)
        .unwrap()
        .empty_queue_capacity(false);
    let fixture = sink_fixture(Loopback::start().await, backend);
    let request = crate::task_execution::intent::TaskOperationQueueRequest::create_task(
        backend,
        capacity + 1,
        Ok(100),
    );
    let before = fixture.runtime.task_transport_supervisor().snapshot();
    let mut preparation = TaskOperationQueuePreparation::new();
    assert!(matches!(
        fixture.sink.try_reserve_queue(request, &mut preparation),
        TaskOperationQueueAdmission::Rejected(_)
    ));
    assert_eq!(
        fixture.runtime.task_transport_supervisor().snapshot(),
        before
    );
    assert!(fixture.loopback.peer.applied().is_empty());
    drop(preparation);
    close(fixture).await;
}
