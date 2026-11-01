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

//! Functional coverage of the real bounded root session and CPU pool. This
//! module does not establish Native host installation or distributed support.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal256Array, DictionaryArray, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow_buffer::i256;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use novarocks_execution::runtime::fragment::io::{
    ResultAbort, ResultWriteAdmission, RootInputAdmission, RootInputAuthority, RootInputPermit,
    RootInputReservationAdmission, RootProducerState, RootResultSession, RootResultWriteSpec,
    RootWorkExitProgress,
};
use novarocks_execution_contract::TaskIdentity;
use novarocks_execution_contract::root_lifetime::RootRetentionClose;
use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultRead};
use novarocks_native_adapter::root_result_session::{NativeRootResultSession, RootProducerPool};
use novarocks_result_contract::{
    ClientRenderSchema, FrozenRootOutput, NativeRenderType, RenderColumn, RenderField,
    RenderPresentation, RootOutputContract, RootProfileId, ScalarField, ScalarRecord, ScalarSchema,
    ScalarValue, ScalarValueType,
};
use novarocks_types::arrow_metadata_owner::{
    ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnedField, MetadataOwnerLimits,
};
use novarocks_types::{
    AttemptId, BackendProcessId, QueryExecutionId, QueryId, SlotId, StageId, TaskId,
};
use novarocks_worker::WorkerResultRetainedLimits;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use novarocks_worker::root_result_channel::{RootResultChannel, RootResultDelivery};

const DEADLINE: Duration = Duration::from_secs(5);

fn task() -> TaskIdentity {
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}

fn client(columns: usize) -> FrozenRootOutput {
    let columns = (0..columns)
        .map(|ordinal| RenderColumn {
            // Repeated output occurrences deliberately share one source slot.
            source_ordinal: 0,
            source_slot: Some(7),
            name: format!("value_{ordinal}"),
            field: RenderField {
                native_type: NativeRenderType::String,
                presentation: RenderPresentation::ScalarText,
                nullable: true,
            },
        })
        .collect();
    FrozenRootOutput::ClientRows(
        ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            columns,
            1,
        )
        .unwrap(),
    )
}

struct Fixture {
    pool: Arc<RootProducerPool>,
    budget: Arc<ResultRetainedBudget>,
    channel: Arc<RootResultChannel>,
    session: Arc<NativeRootResultSession>,
    health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
}

impl Fixture {
    fn new(output: FrozenRootOutput) -> Self {
        let limits = WorkerResultRetainedLimits::try_new(256 << 20, 512 << 20).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        let health =
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::new());
        let pool = RootProducerPool::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            1 << 20,
            Arc::clone(&budget),
            Arc::clone(&health),
        )
        .unwrap();
        let channel = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: task(),
                contract: Arc::new(RootOutputContract::new(RootProfileId::V1, output)),
            },
            Arc::clone(&budget),
            limits,
            Arc::clone(&health),
        )
        .unwrap();
        channel.mark_context_owned().unwrap();
        let session =
            NativeRootResultSession::try_open_for_local_encoding(Arc::clone(&channel), &pool)
                .unwrap();
        Self {
            pool,
            budget,
            channel,
            session,
            health,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // A failed assertion must not perform an unbounded join on the test
        // thread. Normal tests explicitly await shutdown under DEADLINE.
        let pool = Arc::clone(&self.pool);
        std::thread::spawn(move || {
            let _ = pool.shutdown();
        });
    }
}

async fn shutdown_result(pool: &Arc<RootProducerPool>) -> Result<(), String> {
    let pool = Arc::clone(pool);
    let (sent, received) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = sent.send(pool.shutdown());
    });
    tokio::time::timeout(DEADLINE, received)
        .await
        .expect("root pool shutdown exceeded deadline")
        .expect("shutdown owner exited without its result")
}

async fn shutdown(pool: &Arc<RootProducerPool>) {
    shutdown_result(pool)
        .await
        .expect("root pool shutdown failed");
}

async fn await_whole_exit(session: &NativeRootResultSession) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            match session.producer_exit_progress() {
                RootWorkExitProgress::Pending => tokio::time::sleep(Duration::from_millis(1)).await,
                RootWorkExitProgress::Exited => return,
                RootWorkExitProgress::Unresolved => {
                    panic!("normal Root job exit became unresolved")
                }
            }
        }
    })
    .await
    .expect("root whole-job exit exceeded deadline");
}

async fn shutdown_with_issuer_notification_unknown(fixture: &Fixture) {
    use novarocks_execution::runtime::execution_health::ExecutionExitOrigin;
    use novarocks_execution_contract::failure::{FailureClass, FailureReason, domain};
    shutdown_result(&fixture.pool)
        .await
        .expect("the actual pool CPU work joins independently of its issuer notification owner");
    assert!(fixture.pool.poll_failure().is_none());
    assert_eq!(
        fixture.session.producer_exit_progress(),
        RootWorkExitProgress::Unresolved,
        "the original issuer proof still participates after actual pool exit"
    );
    let original = fixture
        .health
        .failure()
        .expect("actual issuer notification fault");
    assert_eq!(
        original.origin(),
        ExecutionExitOrigin::Task {
            identity: fixture.channel.spec().task,
            phase: novarocks_execution_contract::TaskFailurePhase::Execution
        }
    );
    assert_eq!(original.fact().class(), FailureClass::Unexpected);
    assert_eq!(
        original.fact().reason(),
        FailureReason::new(domain::EXECUTION, std::num::NonZeroU16::new(611).unwrap())
    );
    fixture.session.abort(ResultAbort::Cancelled(
        "late issuer failure observation".into(),
    ));
    assert_eq!(fixture.health.failure(), Some(original));
    assert_eq!(
        fixture.session.producer_exit_progress(),
        RootWorkExitProgress::Unresolved
    );
    assert!(fixture.session.try_acquire_input().is_err());
}

async fn shutdown_with_channel_metadata_unknown(fixture: &Fixture, metadata_before_fault: usize) {
    use novarocks_execution::runtime::execution_health::ExecutionExitOrigin;
    use novarocks_execution_contract::failure::{FailureClass, FailureReason, domain};
    shutdown_result(&fixture.pool)
        .await
        .expect("the actual pool exits after channel-local notification catch");
    assert!(fixture.pool.poll_failure().is_none());
    assert_eq!(
        fixture.session.producer_exit_progress(),
        RootWorkExitProgress::Exited,
        "both original pool and issuer proofs have actually exited"
    );
    let original = fixture
        .health
        .failure()
        .expect("actual channel metadata fault");
    assert_eq!(
        original.origin(),
        ExecutionExitOrigin::Task {
            identity: fixture.channel.spec().task,
            phase: novarocks_execution_contract::TaskFailurePhase::Execution
        }
    );
    assert_eq!(original.fact().class(), FailureClass::Unexpected);
    assert_eq!(
        original.fact().reason(),
        FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(163).unwrap())
    );
    let snapshot = fixture.channel.try_ownership_snapshot().unwrap().unwrap();
    assert!(snapshot.metadata_holders > 0);
    assert!(snapshot.metadata_holders > metadata_before_fault);
    assert!(!fixture.channel.physical_idle());
    fixture.session.abort(ResultAbort::Cancelled(
        "late channel failure observation".into(),
    ));
    assert_eq!(fixture.health.failure(), Some(original));
    assert_eq!(
        fixture.session.producer_exit_progress(),
        RootWorkExitProgress::Exited
    );
    assert!(!fixture.channel.physical_idle());
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("root condition exceeded deadline");
}

async fn input(session: &NativeRootResultSession) -> RootInputPermit {
    tokio::time::timeout(DEADLINE, async {
        loop {
            match session.try_acquire_input().unwrap() {
                RootInputAdmission::Granted(permit) => return permit,
                RootInputAdmission::Blocked => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
    })
    .await
    .expect("input admission exceeded deadline")
}

async fn read(
    channel: &Arc<RootResultChannel>,
    wanted: Option<u64>,
    consumed: u64,
) -> RootResultDelivery {
    let spec = channel.spec();
    let request = RootResultRead::try_new(
        spec.task,
        spec.contract.profile(),
        spec.contract.kind(),
        wanted.map(|sequence| NonZeroU64::new(sequence).unwrap()),
        consumed,
        Duration::from_millis(1),
    )
    .unwrap();
    tokio::time::timeout(DEADLINE, channel.read(&request))
        .await
        .expect("root read exceeded deadline")
        .expect("root read failed")
}

fn owned_field(name: &str, data_type: DataType, nullable: bool) -> MetadataOwnedField {
    ArrowMetadataOwner::try_new(
        Vec::new(),
        MetadataOwnerLimits {
            entries: 0,
            construction_bytes: 0,
        },
    )
    .unwrap()
    .into_field(name.into(), data_type, nullable)
}

fn owned_chunk(array: ArrayRef) -> Chunk {
    owned_nested_chunk(array, Vec::new())
}

/// Nested child fields must come from known metadata owners too.
fn source_owned_nested_chunk(array: ArrayRef, children: Vec<MetadataOwnedField>) -> Chunk {
    let field = owned_field("value", array.data_type().clone(), true);
    let mut owners = vec![field.clone()];
    owners.extend(children);
    let nodes = owners.len();
    let origins = FieldMetadataOrigins::try_new(owners, nodes).unwrap();
    let slot = ChunkSlotSchema::try_new_with_metadata_origins(
        SlotId::new(7),
        field.field().clone(),
        origins,
        None,
        None,
    )
    .unwrap();
    let schema = Arc::new(ChunkSchema::try_new(vec![slot]).unwrap());
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap();
    Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}

fn owned_nested_chunk(array: ArrayRef, children: Vec<MetadataOwnedField>) -> Chunk {
    let source = source_owned_nested_chunk(array, children);
    let slot = &source.chunk_schema().slots()[0];
    // Keep the original source owner above and publish its complete binding
    // explicitly for the typed root consumer. The source facts are not a type.
    let slot = ChunkSlotSchema::try_new_typed(
        slot.slot_id(),
        Arc::clone(slot.field_ref()),
        slot.typed_binding().clone(),
        None,
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let schema = Arc::new(
        ChunkSchema::try_new_typed(
            vec![slot],
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_new(
        schema.arrow_schema_ref(),
        source.record_batch().columns().to_vec(),
    )
    .unwrap();
    Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}

fn strings(values: Vec<Option<&str>>) -> Chunk {
    owned_chunk(Arc::new(StringArray::from(values)))
}

fn body(delivery: &RootResultDelivery) -> &[u8] {
    let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
        panic!("expected retained Data");
    };
    data.body().as_ref()
}

fn assert_end(delivery: &RootResultDelivery, sequence: u64, rows: u64) {
    let RootReadOutcome::End(end) = &delivery.reply().outcome else {
        panic!("expected retained End");
    };
    assert_eq!(end.sequence.get(), sequence);
    assert_eq!(end.output_rows, rows);
}

#[tokio::test]
async fn client_dictionary_repeated_occurrences_and_null_have_independent_exact_bytes() {
    let fixture = Fixture::new(client(2));
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(1), None, Some(0)]),
        Arc::new(StringArray::from(vec!["x", "é"])),
    )
    .unwrap();
    assert!(matches!(dictionary.data_type(), DataType::Dictionary(_, _)));
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                owned_chunk(Arc::new(dictionary)),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    await_whole_exit(&fixture.session).await;
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    assert_eq!(fixture.channel.snapshot().offered_through, 0);
    let first = read(&fixture.channel, Some(1), 0).await;
    // Each row begins with u32LE rowTotal; cells use MySQL length-encoded
    // UTF-8 bytes (or 0xfb NULL). Expected bytes use no production encoder.
    let expected = [
        6, 0, 0, 0, 2, 0xc3, 0xa9, 2, 0xc3, 0xa9, 2, 0, 0, 0, 0xfb, 0xfb, 4, 0, 0, 0, 1, b'x', 1,
        b'x',
    ];
    assert_eq!(body(&first), expected);
    drop(first);
    let replay = read(&fixture.channel, Some(1), 0).await;
    assert_eq!(body(&replay), expected);
    drop(replay);
    assert_end(&read(&fixture.channel, Some(2), 0).await, 2, 3);
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn count_only_accepts_decimal256_without_client_cell_rendering() {
    let fixture = Fixture::new(FrozenRootOutput::CountOnly);
    // Decimal256 is deliberately unavailable for ClientRows presentation;
    // CountOnly must inspect its standard backing and count rows directly.
    let array = Decimal256Array::from(vec![
        Some(i256::from_i128(7)),
        None,
        Some(i256::from_i128(-9)),
    ])
    .with_precision_and_scale(76, 0)
    .unwrap();
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                owned_chunk(Arc::new(array)),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    await_whole_exit(&fixture.session).await;
    assert_eq!(fixture.channel.snapshot().data_positions, 0);
    assert_eq!(fixture.channel.snapshot().produced_through, 1);
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    assert_end(&read(&fixture.channel, Some(1), 0).await, 1, 3);
    assert_end(&read(&fixture.channel, Some(1), 0).await, 1, 3);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn one_original_input_position_and_same_task_foreign_issuer_are_distinct() {
    // Two CountOnly grants fit the same 256 MiB root budget. ClientRows would
    // require two 192 MiB grants and correctly block at the budget first.
    let fixture = Fixture::new(FrozenRootOutput::CountOnly);
    let permit = input(&fixture.session).await;
    let generation = permit.generation();
    assert!(matches!(
        fixture.session.try_acquire_input().unwrap(),
        RootInputAdmission::Blocked
    ));
    let foreign = RootInputAuthority::new(fixture.session.spec(), Arc::clone(&fixture.health));
    let RootInputReservationAdmission::Granted(mut reservation) = foreign.try_reserve().unwrap()
    else {
        panic!("foreign issuer reservation")
    };
    let ResultWriteAdmission::Granted(credit) = fixture
        .channel
        .try_reserve(foreign.required_bytes())
        .unwrap()
    else {
        panic!("foreign issuer uses the same existing retained budget");
    };
    let mut credit = Some(credit);
    let RootInputAdmission::Granted(foreign_permit) = reservation.try_bind(&mut credit).unwrap()
    else {
        panic!("foreign issuer has a free position");
    };
    let error = {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                strings(vec![Some("foreign")]),
                foreign_permit,
            ),
        );
        let original_array =
            Arc::as_ptr(submitted.as_ref().unwrap().chunk().record_batch().column(0));
        let generation = submitted.as_ref().unwrap().permit().generation();
        let error = fixture.session.submit_input(&mut submitted).unwrap_err();
        let returned = submitted
            .as_ref()
            .expect("preaccept refusal returns the original caller slot");
        assert!(std::ptr::eq(
            original_array,
            Arc::as_ptr(returned.chunk().record_batch().column(0))
        ));
        assert_eq!(returned.permit().generation(), generation);
        let (chunk, permit) = submitted.take().unwrap().into_cleanup_parts();
        drop(chunk);
        drop(permit);
        error
    };
    assert!(error.to_string().contains("different issuer or generation"));
    use novarocks_execution_contract::failure::FailureProjection;
    assert_eq!(
        error.failure_fact().reason().domain(),
        novarocks_execution_contract::failure::domain::NATIVE.get()
    );
    assert_eq!(error.failure_fact().reason().code(), 513);
    assert!(foreign.is_available());
    assert!(matches!(
        fixture.session.try_acquire_input().unwrap(),
        RootInputAdmission::Blocked
    ));
    drop(permit);
    let next = input(&fixture.session).await;
    assert!(next.generation() > generation);
    drop(next);
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn tiny_complete_batches_flush_before_finish_and_before_next_input() {
    let fixture = Fixture::new(client(1));
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                strings(vec![Some("a")]),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    wait_until(|| fixture.channel.snapshot().produced_through == 1).await;
    assert_eq!(
        fixture.session.producer_state(),
        RootProducerState::Accepting
    );
    let first = read(&fixture.channel, Some(1), 0).await;
    assert_eq!(body(&first), [2, 0, 0, 0, 1, b'a']);
    drop(first);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                strings(vec![Some("b")]),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    wait_until(|| fixture.channel.snapshot().produced_through == 2).await;
    assert_eq!(fixture.channel.snapshot().data_positions, 2);
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    await_whole_exit(&fixture.session).await;
    let second = read(&fixture.channel, Some(2), 0).await;
    assert_eq!(body(&second), [2, 0, 0, 0, 1, b'b']);
    drop(second);
    assert_end(&read(&fixture.channel, Some(3), 0).await, 3, 2);
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn client_single_row_has_an_exact_one_byte_continuation_before_end() {
    use novarocks_result_contract::{ClientRowProfile, ClientRowStreamCursor, RootProfileV1};
    let fixture = Fixture::new(client(1));
    let value = "x".repeat(RootProfileV1::SEGMENT_BYTES - 7);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                strings(vec![Some(&value)]),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    await_whole_exit(&fixture.session).await;
    let profile = ClientRowProfile::try_new(
        RootProfileV1::SEGMENT_BYTES,
        RootProfileV1::ROW_PAYLOAD_BYTES,
    )
    .unwrap();
    let first = read(&fixture.channel, Some(1), 0).await;
    assert_eq!(body(&first).len(), RootProfileV1::SEGMENT_BYTES);
    assert_eq!(
        &body(&first)[..8],
        &[0xfd, 0xff, 0x0f, 0x00, 0xfd, 0xf9, 0xff, 0x0f]
    );
    assert!(body(&first)[8..].iter().all(|byte| *byte == b'x'));
    let cursor = ClientRowStreamCursor::new()
        .validate_body(profile, body(&first))
        .unwrap()
        .after();
    assert_eq!(cursor.remaining(), 1);
    assert_eq!(cursor.completed_rows(), 0);
    drop(first);
    let second = read(&fixture.channel, Some(2), 0).await;
    assert_eq!(body(&second), b"x");
    let validated = cursor.validate_body(profile, body(&second)).unwrap();
    let spans = validated.payload_spans().collect::<Vec<_>>();
    assert_eq!(spans.len(), 1);
    assert!(spans[0].starts_row.is_none());
    assert!(spans[0].completes_row);
    assert_eq!(validated.after().remaining(), 0);
    assert_eq!(validated.after().completed_rows(), 1);
    validated.after().validate_end().unwrap();
    drop(second);
    assert_end(&read(&fixture.channel, Some(3), 0).await, 3, 1);
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    assert_eq!(fixture.channel.snapshot().data_positions, 2);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn empty_finish_has_end_without_a_prior_input_or_data_item() {
    for output in [client(1), FrozenRootOutput::CountOnly] {
        let fixture = Fixture::new(output);
        fixture.session.finish_input().unwrap();
        wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
        await_whole_exit(&fixture.session).await;
        assert!(fixture.session.try_acquire_input().is_err());
        assert_eq!(fixture.channel.snapshot().data_positions, 0);
        assert_end(&read(&fixture.channel, Some(1), 0).await, 1, 0);
        shutdown(&fixture.pool).await;
    }
}

#[tokio::test]
async fn ordinary_upstream_output_beyond_the_input_allowance_fails_after_pull() {
    use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
    let fixture = Fixture::new(client(1));
    // The upstream owner materialized this batch under its own scope. Its
    // standard allocation reserves more than one root input allowance even
    // though only one byte is visible; the root never extends its permit.
    let mut bytes = Vec::<u8>::with_capacity(97 << 20);
    bytes.push(b'x');
    let array = StringArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1])),
        Buffer::from_vec(bytes),
        None,
    )
    .unwrap();
    let error = {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                owned_chunk(Arc::new(array)),
                input(&fixture.session).await,
            ),
        );
        let original_array =
            Arc::as_ptr(submitted.as_ref().unwrap().chunk().record_batch().column(0));
        let generation = submitted.as_ref().unwrap().permit().generation();
        let error = fixture.session.submit_input(&mut submitted).unwrap_err();
        let returned = submitted
            .as_ref()
            .expect("preaccept refusal returns the original caller slot");
        assert!(std::ptr::eq(
            original_array,
            Arc::as_ptr(returned.chunk().record_batch().column(0))
        ));
        assert_eq!(returned.permit().generation(), generation);
        let (chunk, permit) = submitted.take().unwrap().into_cleanup_parts();
        drop(chunk);
        drop(permit);
        error
    };
    assert_original_root_storage_cause(
        &error,
        novarocks_execution::exec::chunk::RootArrayStorageError::CapacityExceeded,
    );
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
    assert_eq!(error.failure_fact().class(), FailureClass::Resource);
    // The refused input exits with its position; nothing was published.
    drop(input(&fixture.session).await);
    assert_eq!(fixture.channel.snapshot().produced_through, 0);
    assert_eq!(fixture.channel.snapshot().data_positions, 0);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn unknown_actual_schema_refuses_before_producer_and_drops_chunk_before_credit_wake() {
    let fixture = Fixture::new(client(1));
    let array: ArrayRef = Arc::new(StringArray::from(vec!["unproven"]));
    let weak = Arc::downgrade(&array);
    // This negative protects source provenance. A typed execution constructor
    // validates and canonicalizes an equivalent schema, so it is not unknown.
    let original = source_owned_nested_chunk(array, Vec::new());
    // Same field/schema contents, independently allocated top-level schema.
    // Generic Chunk construction preserves semantics but cannot mint an origin.
    let unknown_schema = Arc::new(original.record_batch().schema().as_ref().clone());
    let unknown_batch = RecordBatch::try_new(
        unknown_schema.clone(),
        original.record_batch().columns().to_vec(),
    )
    .unwrap();
    let unknown =
        Chunk::try_new_with_chunk_schema(unknown_batch, original.chunk_schema_ref()).unwrap();
    assert!(Arc::ptr_eq(
        &unknown.record_batch().schema(),
        &unknown_schema
    ));
    drop(original);
    let permit = input(&fixture.session).await;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let dropped_before_wake = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&callbacks);
    let actual_dropped = Arc::clone(&dropped_before_wake);
    let _subscription = fixture
        .session
        .writable_observable()
        .subscribe(Arc::new(move || {
            actual_dropped.store(weak.upgrade().is_none(), Ordering::SeqCst);
            observed.fetch_add(1, Ordering::SeqCst);
        }));
    let error = {
        let mut submitted =
            Some(novarocks_execution::runtime::fragment::io::RootResultInput::new(unknown, permit));
        let original_array =
            Arc::as_ptr(submitted.as_ref().unwrap().chunk().record_batch().column(0));
        let generation = submitted.as_ref().unwrap().permit().generation();
        let error = fixture.session.submit_input(&mut submitted).unwrap_err();
        let returned = submitted
            .as_ref()
            .expect("preaccept refusal returns the original caller slot");
        assert!(std::ptr::eq(
            original_array,
            Arc::as_ptr(returned.chunk().record_batch().column(0))
        ));
        assert_eq!(returned.permit().generation(), generation);
        assert_eq!(
            callbacks.load(Ordering::SeqCst),
            0,
            "refusal retains the caller input and credit"
        );
        let (chunk, permit) = submitted.take().unwrap().into_cleanup_parts();
        drop(chunk);
        drop(permit);
        error
    };
    assert_original_root_storage_cause(
        &error,
        novarocks_execution::exec::chunk::RootArrayStorageError::UnknownMetadataOwner,
    );
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert!(callbacks.load(Ordering::SeqCst) > 0);
    assert!(dropped_before_wake.load(Ordering::SeqCst));
    assert_eq!(
        fixture.session.producer_exit_progress(),
        RootWorkExitProgress::Pending
    );
    assert_eq!(fixture.channel.snapshot().produced_through, 0);
    assert_eq!(
        fixture.session.producer_state(),
        RootProducerState::Accepting
    );
    drop(input(&fixture.session).await);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn full_window_suspends_original_input_and_abort_exits_with_response_alias_held() {
    let fixture = Fixture::new(client(1));
    let value = "q".repeat(3 << 20);
    let array: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
    let weak = Arc::downgrade(&array);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                owned_chunk(array),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    wait_until(|| fixture.channel.snapshot().data_positions == 2).await;
    assert_eq!(fixture.channel.snapshot().produced_through, 2);
    assert!(weak.upgrade().is_some());
    assert_eq!(
        fixture.session.producer_exit_progress(),
        RootWorkExitProgress::Pending
    );
    assert!(matches!(
        fixture.session.try_acquire_input().unwrap(),
        RootInputAdmission::Blocked
    ));
    let delivery = read(&fixture.channel, Some(1), 0).await;
    let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
        panic!("expected Data")
    };
    let alias = data.body().clone();
    drop(delivery);
    fixture
        .session
        .abort(ResultAbort::Cancelled("test cancellation".into()));
    await_whole_exit(&fixture.session).await;
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert!(fixture.session.try_acquire_input().is_err());
    assert!(!fixture.channel.physical_idle());
    assert!(!alias.is_empty());
    // Drop the session's fixed metadata holder too: only the response alias
    // remains as the deliberately live physical result owner.
    let channel = Arc::clone(&fixture.channel);
    let pool = Arc::clone(&fixture.pool);
    drop(fixture);
    assert!(!channel.physical_idle());
    drop(alias);
    wait_until(|| channel.physical_idle()).await;
    shutdown(&pool).await;
}

#[tokio::test]
async fn pool_shutdown_drains_an_active_blocked_session_without_consumer_ack() {
    let fixture = Fixture::new(client(1));
    let value = "x".repeat(3 << 20);
    let array: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
    let weak = Arc::downgrade(&array);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                owned_chunk(array),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    wait_until(|| fixture.channel.snapshot().data_positions == 2).await;
    assert_eq!(
        fixture.session.producer_exit_progress(),
        RootWorkExitProgress::Pending
    );
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    shutdown(&fixture.pool).await;
    await_whole_exit(&fixture.session).await;
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert!(fixture.session.try_acquire_input().is_err());
    assert!(
        NativeRootResultSession::try_open_for_local_encoding(
            Arc::clone(&fixture.channel),
            &fixture.pool
        )
        .is_err()
    );
    fixture.channel.close(RootRetentionClose::ContextReleased);
}

#[tokio::test]
async fn repeated_cancel_issuer_observer_panics_keep_notification_unknown_after_original_input_exit()
 {
    let fixture = Fixture::new(client(1));
    let value = "x".repeat(3 << 20);
    let array: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
    let weak = Arc::downgrade(&array);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                owned_chunk(array),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    wait_until(|| fixture.channel.snapshot().data_positions == 2).await;
    let observed = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&observed);
    let original = weak.clone();
    let subscription = fixture
        .session
        .writable_observable()
        .subscribe(Arc::new(move || {
            if original.upgrade().is_some() {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                // The cap keeps a broken implementation's failure finite.
                // The oracle requires actual cleanup before it can exhaust
                // these repeated panics, rather than passing after retries.
                if call < 64 {
                    panic!("injected repeated cancellation observer panic");
                }
            }
        }));
    shutdown_with_issuer_notification_unknown(&fixture).await;
    assert!(weak.upgrade().is_none());
    assert!(
        observed.load(Ordering::SeqCst) > 0 && observed.load(Ordering::SeqCst) < 64,
        "original input exit depended on exhausting the observer's panic budget"
    );
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    drop(subscription);
    let available = (512 << 20) - fixture.pool.reserved_bytes() - (1 << 20);
    let ResultWriteAdmission::Granted(credit) =
        fixture.budget.try_reserve_process(available).unwrap()
    else {
        panic!("original input grant remained retained after its physical exit");
    };
    drop(credit);
}

#[tokio::test]
async fn count_only_panicking_issuer_observer_keeps_unknown_after_cpu_and_grant_exit() {
    let fixture = Fixture::new(FrozenRootOutput::CountOnly);
    let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let weak = Arc::downgrade(&array);
    let permit = input(&fixture.session).await;
    let fired = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&fired);
    let _subscription = fixture
        .session
        .writable_observable()
        .subscribe(Arc::new(move || {
            if !observed.swap(true, Ordering::SeqCst) {
                panic!("injected one-time writable observer failure");
            }
        }));
    // CountOnly retires the real input in the producer turn. The issuer
    // catches this notification fault independently of its exact grant receipt.
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                owned_chunk(array),
                permit,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    wait_until(|| fired.load(Ordering::SeqCst)).await;
    shutdown_with_issuer_notification_unknown(&fixture).await;
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert!(fixture.session.try_acquire_input().is_err());
    // A reservation of every byte apart from the still-live fixed pool and
    // channel proves the original 96 MiB grant has returned. No body allocation
    // accompanies this process credit, and no private testing counter is used.
    let available = (512 << 20) - fixture.pool.reserved_bytes() - (1 << 20);
    let ResultWriteAdmission::Granted(credit) =
        fixture.budget.try_reserve_process(available).unwrap()
    else {
        panic!("original input grant remained retained after its physical exit");
    };
    drop(credit);
}

#[tokio::test]
async fn cancel_after_a_normal_end_keeps_the_completed_terminal() {
    let fixture = Fixture::new(client(1));
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    await_whole_exit(&fixture.session).await;
    // A pool shutdown can still reach a job whose slot it has not yet
    // removed; cancelling a completed producer must not rewrite its End.
    fixture
        .session
        .abort(ResultAbort::Cancelled("late shutdown cancel".into()));
    assert_eq!(
        fixture.session.producer_state(),
        RootProducerState::ContextHeld
    );
    assert_end(&read(&fixture.channel, Some(1), 0).await, 1, 0);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn encoder_handoff_notification_panic_keeps_contextheld_and_channel_metadata_unknown() {
    let fixture = Fixture::new(client(1));
    let metadata_before_fault = fixture
        .channel
        .try_ownership_snapshot()
        .unwrap()
        .unwrap()
        .metadata_holders;
    let fired = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&fired);
    let channel = Arc::downgrade(&fixture.channel);
    let _subscription = fixture
        .channel
        .writable_observable()
        .subscribe(Arc::new(move || {
            if let Some(channel) = channel.upgrade()
                && channel.producer_state() == RootProducerState::ContextHeld
                && !observed.swap(true, Ordering::SeqCst)
            {
                // End has been published and the real producer guard has
                // handed off to its context. This notification still precedes
                // whole-pool-job proof. The channel's original finite metadata
                // owner catches it without invalidating the CPU job's exit.
                panic!("injected one-time completed producer exit notification failure");
            }
        }));
    fixture.session.finish_input().unwrap();
    wait_until(|| fired.load(Ordering::SeqCst)).await;
    shutdown_with_channel_metadata_unknown(&fixture, metadata_before_fault).await;
    assert_eq!(
        fixture.session.producer_state(),
        RootProducerState::ContextHeld
    );
    assert_eq!(fixture.channel.snapshot().data_positions, 0);
    assert_eq!(fixture.channel.snapshot().consumed_through, 0);
    assert_end(&read(&fixture.channel, Some(1), 0).await, 1, 0);
}

#[tokio::test]
async fn writable_snapshot_retains_fixed_metadata_after_last_session_arc_exits() {
    // Construct these owners directly: no Fixture session alias or automatic
    // pool shutdown may hide which owner keeps the metadata grant alive.
    let limits = WorkerResultRetainedLimits::try_new(256 << 20, 512 << 20).unwrap();
    let budget = ResultRetainedBudget::new(limits.per_process());
    let health = Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::new());
    let pool = RootProducerPool::try_new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(8).unwrap(),
        1 << 20,
        Arc::clone(&budget),
        Arc::clone(&health),
    )
    .unwrap();
    let channel = RootResultChannel::try_open(
        RootResultWriteSpec {
            task: task(),
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::CountOnly,
            )),
        },
        budget,
        limits,
        Arc::clone(&health),
    )
    .unwrap();
    channel.mark_context_owned().unwrap();
    let session =
        NativeRootResultSession::try_open_for_local_encoding(Arc::clone(&channel), &pool).unwrap();
    assert_eq!(
        session.producer_exit_progress(),
        RootWorkExitProgress::Pending
    );
    session.finish_input().unwrap();
    await_whole_exit(&session).await;
    let weak_session = Arc::downgrade(&session);
    let first = Arc::new(AtomicBool::new(true));
    let first_notification = Arc::clone(&first);
    let released_in_time = Arc::new(AtomicBool::new(false));
    let gate_result = Arc::clone(&released_in_time);
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let (release, release_rx) = std::sync::mpsc::channel();
    struct ReleaseNotification(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for ReleaseNotification {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let mut release = ReleaseNotification(Some(release));
    let release_rx = std::sync::Mutex::new(release_rx);
    let _subscription = session.writable_observable().subscribe(Arc::new(move || {
        if first_notification.swap(false, Ordering::SeqCst) {
            if let Ok(mut entered) = entered.lock()
                && let Some(entered) = entered.take()
            {
                let _ = entered.send(());
            }
            // The gate never panics and never blocks indefinitely. A
            // second notification from channel.close must pass through.
            let released = release_rx
                .lock()
                .ok()
                .is_some_and(|receiver| receiver.recv_timeout(DEADLINE).is_ok());
            gate_result.store(released, Ordering::SeqCst);
        }
    }));
    let writable = channel.writable_observable();
    let notification = std::thread::spawn(move || writable.notify_observers());
    let (joined, joined_rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = joined.send(notification.join());
    });
    tokio::time::timeout(DEADLINE, entered_rx)
        .await
        .expect("notification did not reach the observer gate")
        .expect("observer gate owner disappeared");
    assert_eq!(Arc::strong_count(&session), 1);
    drop(session);
    // The exported readiness token now also retains the fixed backing grant.
    // Drop it while the already captured notification is still blocked, so
    // the remaining holder below is the real callback/snapshot itself.
    drop(_subscription);
    assert!(
        weak_session.upgrade().is_none(),
        "the notification snapshot must not pin the Session itself"
    );
    channel.close(RootRetentionClose::ContextAborted);
    let owners = wait_until(|| channel.physical_idle());
    tokio::pin!(owners);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut owners)
            .await
            .is_err(),
        "the blocked callback snapshot still retains fixed metadata"
    );
    release.0.take().unwrap().send(()).unwrap();
    tokio::time::timeout(DEADLINE, joined_rx)
        .await
        .expect("notification join exceeded deadline")
        .expect("notification join owner disappeared")
        .expect("notification thread panicked");
    assert!(released_in_time.load(Ordering::SeqCst));
    tokio::time::timeout(DEADLINE, &mut owners)
        .await
        .expect("fixed metadata did not exit with the callback snapshot");
    assert!(channel.physical_idle());
    shutdown(&pool).await;
}

// This source-only fixture declares physical values explicitly. It does not
// infer nominal domains from metadata or from arrays, nor install a BE owner.
// The shared type-table codec creates the exact nested Field/type owners that
// both the typed ChunkSchema and its List/Map arrays retain.
fn decoded_fixture_schema(schema: &Schema, slots: &[u32]) -> Arc<ChunkSchema> {
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
        decode_execution_type_table_observed,
    };
    use novarocks_plan_codec::host_projection_v2::AdmissionRefusal;
    use novarocks_type_contract::{
        CompileCheckpoints, CompilePhase, FunctionValueType, ValueTypeStorageLifetime,
    };
    assert_eq!(schema.fields().len(), slots.len());
    assert!(schema.metadata().as_arc().is_none());
    // Both call sites author their original metadata-free physical contracts:
    // StatisticsArtifactV1 and the fixed RootWriteResultSchema, respectively.
    let declared: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            assert!(field.metadata().as_arc().is_none());
            FunctionValueType::new(field.data_type().clone(), field.is_nullable())
        })
        .collect();
    let roots: Vec<_> = slots
        .iter()
        .zip(&declared)
        .map(|(&id, value)| ExecutionTypeRoot { id, value })
        .collect();
    let limits = ExecutionTypeTableLimits::for_validated_root_count(roots.len()).unwrap();
    let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
    let mut encode_work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let encoded = ExecutionTypeTableBuilder::new(&roots)
        .encode_observed(
            limits,
            &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
            &mut encode_work,
        )
        .unwrap();
    encode_work.finish().unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let decoded = decode_execution_type_table_observed(
        encoded.wire(),
        limits,
        ValueTypeStorageLifetime::pure(),
        &mut |_| Ok::<_, AdmissionRefusal<()>>(()),
        &mut work,
    )
    .unwrap();
    let slots = slots
        .iter()
        .zip(schema.fields())
        .zip(&declared)
        .map(|((&id, field), expected)| {
            let value = decoded.get_observed(id, &mut work).unwrap().unwrap();
            assert_eq!(value, expected);
            let decoded_field = Arc::new(Field::new(
                field.name(),
                value.data_type().clone(),
                value.nullable(),
            ));
            ChunkSlotSchema::try_new_typed(
                SlotId::new(id),
                decoded_field,
                value.typed_binding().clone(),
                None,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap()
        })
        .collect();
    work.finish().unwrap();
    Arc::new(ChunkSchema::try_new_typed(slots, ValueTypeStorageLifetime::pure()).unwrap())
}

fn statistics_chunk(ids: &[i32], blob: &str, body: &[u8]) -> (Chunk, std::sync::Weak<BinaryArray>) {
    use arrow::array::{ListArray, MapArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", DataType::Utf8, false)),
            ]
            .into(),
        ),
        false,
    ));
    let schema = Schema::new(vec![
        Field::new(
            "input_fields",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            false,
        ),
        Field::new("blob_type", DataType::Utf8, false),
        Field::new("body", DataType::Binary, true),
        Field::new("properties", DataType::Map(entries, false), false),
    ]);
    let chunk_schema = decoded_fixture_schema(&schema, &[1, 2, 3, 4]);
    let decoded_schema = chunk_schema.arrow_schema_ref();
    let DataType::List(item) = decoded_schema.field(0).data_type() else {
        panic!("list");
    };
    let DataType::Map(entries, false) = decoded_schema.field(3).data_type() else {
        panic!("map");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("entries");
    };
    let empty = || Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
    let body_array = Arc::new(BinaryArray::from_iter_values([body]));
    let weak = Arc::downgrade(&body_array);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::new(vec![0, ids.len() as i32].into()),
            Arc::new(Int32Array::from(ids.to_vec())),
            None,
        )),
        Arc::new(StringArray::from(vec![blob])),
        body_array,
        Arc::new(MapArray::new(
            entries.clone(),
            OffsetBuffer::new(vec![0, 0].into()),
            StructArray::new(fields.clone(), vec![empty(), empty()], None),
            None,
            false,
        )),
    ];
    let batch = RecordBatch::try_new(chunk_schema.arrow_schema_ref(), columns).unwrap();
    (
        Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap(),
        weak,
    )
}

#[tokio::test]
async fn statistics_domain_emits_exact_opaque_records_across_two_original_inputs() {
    use novarocks_result_contract::InternalResultDomain;
    let fixture = Fixture::new(FrozenRootOutput::InternalFacts(
        InternalResultDomain::StatisticsArtifactV1,
    ));
    let (chunk, weak) = statistics_chunk(&[9, 7], "é", &[0, 255, 65]);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                chunk,
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    let second_permit = input(&fixture.session).await;
    assert!(weak.upgrade().is_none());
    let first = read(&fixture.channel, Some(1), 0).await;
    assert_eq!(
        body(&first),
        &[
            83, 84, 65, 49, 37, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0,
            0, 7, 0, 0, 0, 195, 169, 0, 255, 65
        ]
    );
    drop(first);
    let (chunk, _) = statistics_chunk(&[3], "x", &[]);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(chunk, second_permit),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    fixture.session.finish_input().unwrap();
    await_whole_exit(&fixture.session).await;
    let second = read(&fixture.channel, Some(2), 1).await;
    assert_eq!(
        body(&second),
        &[
            83, 84, 65, 49, 29, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0,
            0, 120
        ]
    );
    drop(second);
    assert_end(&read(&fixture.channel, Some(3), 2).await, 3, 2);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn statistics_record_can_cross_segments_and_cancel_releases_its_original_backing() {
    use novarocks_result_contract::InternalResultDomain;
    let fixture = Fixture::new(FrozenRootOutput::InternalFacts(
        InternalResultDomain::StatisticsArtifactV1,
    ));
    let body_bytes = vec![0xf3; 3 << 20];
    let (chunk, weak) = statistics_chunk(&[1], "theta", &body_bytes);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                chunk,
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    wait_until(|| fixture.channel.snapshot().data_positions == 2).await;
    assert_eq!(
        fixture.session.producer_exit_progress(),
        RootWorkExitProgress::Pending
    );
    assert!(weak.upgrade().is_some());
    let first = read(&fixture.channel, Some(1), 0).await;
    assert_eq!(&body(&first)[..4], b"STA1");
    assert_eq!(body(&first).len(), 1 << 20);
    drop(first);
    fixture.session.abort(ResultAbort::Cancelled(
        "cancel statistics continuation".into(),
    ));
    await_whole_exit(&fixture.session).await;
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    shutdown(&fixture.pool).await;
}

/// Fixed Root write result rows: (target, fragment) pairs plus an optional
/// SUMMARY, with the schema decoded through the Native layout like a BE task.
fn write_chunk(fragments: &[(i32, &[u8])], summary: Option<i64>) -> Chunk {
    use arrow::array::{Int8Array, ListArray, MapArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use novarocks_spi::connector::write_stack::root_write_result_schema;
    let schema = root_write_result_schema();
    let chunk_schema = decoded_fixture_schema(&schema, &[1, 2, 3, 4, 5, 6, 7, 8]);
    let decoded_schema = chunk_schema.arrow_schema_ref();
    let DataType::List(item) = decoded_schema.field(4).data_type() else {
        panic!("list");
    };
    let DataType::Map(entries, false) = decoded_schema.field(7).data_type() else {
        panic!("map");
    };
    let DataType::Struct(entry_fields) = entries.data_type() else {
        panic!("entries");
    };
    let rows = fragments.len() + usize::from(summary.is_some());
    let mut kinds = vec![2_i8; fragments.len()];
    let mut targets = fragments
        .iter()
        .map(|(target, _)| Some(*target))
        .collect::<Vec<_>>();
    let mut counts = vec![None; fragments.len()];
    let mut bodies = fragments
        .iter()
        .map(|(_, bytes)| Some(*bytes))
        .collect::<Vec<_>>();
    if let Some(count) = summary {
        kinds.push(1);
        targets.push(None);
        counts.push(Some(count));
        bodies.push(None);
    }
    let none_strings = || Arc::new(StringArray::from(vec![None::<&str>; rows])) as ArrayRef;
    let empty = || Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(kinds)),
        Arc::new(Int32Array::from(targets)),
        Arc::new(Int64Array::from(counts)),
        Arc::new(BinaryArray::from(bodies)),
        Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::from_lengths(vec![0; rows]),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Some(vec![false; rows].into()),
        )),
        none_strings(),
        Arc::new(BinaryArray::from(vec![None::<&[u8]>; rows])),
        Arc::new(MapArray::new(
            entries.clone(),
            OffsetBuffer::from_lengths(vec![0; rows]),
            StructArray::new(entry_fields.clone(), vec![empty(), empty()], None),
            Some(vec![false; rows].into()),
            false,
        )),
    ];
    let batch = RecordBatch::try_new(chunk_schema.arrow_schema_ref(), columns).unwrap();
    Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap()
}

#[tokio::test]
async fn write_commit_records_cross_inputs_and_end_after_their_summary() {
    use novarocks_native_adapter::root_write_commit_codec::WriteCommitRecordHeader;
    use novarocks_result_contract::InternalResultDomain;
    use novarocks_spi::connector::write_stack::RootRowKind;
    let fixture = Fixture::new(FrozenRootOutput::InternalFacts(
        InternalResultDomain::PreparedWriteCommitV1,
    ));
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                write_chunk(&[(0, b"a"), (1, b"bc")], None),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                write_chunk(&[], Some(3)),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    fixture.session.finish_input().unwrap();
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    let mut bytes = Vec::new();
    let mut sequence = 1;
    let rows = loop {
        let delivery = read(&fixture.channel, Some(sequence), sequence - 1).await;
        match &delivery.reply().outcome {
            RootReadOutcome::Data(data) => bytes.extend_from_slice(data.body().as_ref()),
            RootReadOutcome::End(end) => break end.output_rows,
            // The 1 ms long poll can expire before the producer publishes.
            RootReadOutcome::NotReady => continue,
            other => panic!("unexpected root outcome {other:?}"),
        }
        sequence += 1;
    };
    assert_eq!(rows, 3);
    let mut kinds = Vec::new();
    let mut rest = bytes.as_slice();
    while !rest.is_empty() {
        let header = WriteCommitRecordHeader::parse(rest).unwrap();
        kinds.push(header.kind());
        rest = &rest[header.record_bytes()..];
    }
    assert_eq!(
        kinds,
        [
            RootRowKind::PreparedFragment,
            RootRowKind::PreparedFragment,
            RootRowKind::Summary
        ]
    );
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn write_commit_without_summary_fails_instead_of_a_success_end() {
    use novarocks_result_contract::InternalResultDomain;
    let fixture = Fixture::new(FrozenRootOutput::InternalFacts(
        InternalResultDomain::PreparedWriteCommitV1,
    ));
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                write_chunk(&[(0, b"a")], None),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    fixture.session.finish_input().unwrap();
    await_whole_exit(&fixture.session).await;
    let RootProducerState::Failed(cause) = fixture.session.producer_state() else {
        panic!("missing SUMMARY cannot publish normal End")
    };
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
    assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(
        cause.failure_fact().reason().domain(),
        novarocks_execution_contract::failure::domain::NATIVE.get()
    );
    assert_eq!(cause.failure_fact().reason().code(), 206);
    let mut source = std::error::Error::source(&cause);
    let mut original = None;
    while let Some(cause) = source {
        if let Some(cause) = cause.downcast_ref::<novarocks_native_adapter::root_write_commit_codec::WriteCommitCodecError>() { original = Some(*cause); break; }
        source = cause.source();
    }
    assert_eq!(original, Some(novarocks_native_adapter::root_write_commit_codec::WriteCommitCodecError::MissingSummary));
    assert_eq!(fixture.session.channel().snapshot().end_sequence, None);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn every_internal_domain_opens_and_an_untyped_scalar_identity_has_no_channel() {
    use novarocks_result_contract::InternalResultDomain;
    let limits = WorkerResultRetainedLimits::try_new(256 << 20, 512 << 20).unwrap();
    let budget = ResultRetainedBudget::new(limits.per_process());
    let health = Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::new());
    let pool = RootProducerPool::try_new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(8).unwrap(),
        1 << 20,
        Arc::clone(&budget),
        Arc::clone(&health),
    )
    .unwrap();
    // A scalar domain identity without its typed schema has no channel.
    assert!(
        RootResultChannel::try_open(
            RootResultWriteSpec {
                task: task(),
                contract: Arc::new(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::InternalFacts(InternalResultDomain::ScalarValueV1),
                )),
            },
            Arc::clone(&budget),
            limits,
            Arc::clone(&health),
        )
        .is_err()
    );
    for domain in [
        InternalResultDomain::StatisticsArtifactV1,
        InternalResultDomain::PreparedWriteCommitV1,
        InternalResultDomain::CowSelectionArrowV1,
    ] {
        let channel = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: task(),
                contract: Arc::new(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::InternalFacts(domain),
                )),
            },
            Arc::clone(&budget),
            limits,
            Arc::clone(&health),
        )
        .unwrap();
        channel.mark_context_owned().unwrap();
        let session =
            NativeRootResultSession::try_open_for_local_encoding(Arc::clone(&channel), &pool)
                .unwrap_or_else(|error| panic!("{domain:?} producer is installed: {error}"));
        session.abort(ResultAbort::Cancelled(String::new()));
        drop(session);
        channel.close(RootRetentionClose::ContextReleased);
    }
    shutdown(&pool).await;
}

#[tokio::test]
async fn cow_selection_records_decode_back_to_their_batches() {
    use novarocks_native_adapter::root_cow_selection_codec::{
        CowSelectionDecoder, CowSelectionRecordHeader, CowSelectionRecordKind,
    };
    use novarocks_result_contract::InternalResultDomain;
    let fixture = Fixture::new(FrozenRootOutput::InternalFacts(
        InternalResultDomain::CowSelectionArrowV1,
    ));
    let inputs = [
        longs(vec![]),
        longs(vec![Some(7), None, Some(9)]),
        longs(vec![Some(-1)]),
    ];
    let expected = inputs[1..]
        .iter()
        .map(|chunk| chunk.record_batch().clone())
        .collect::<Vec<_>>();
    for chunk in inputs {
        {
            let mut submitted = Some(
                novarocks_execution::runtime::fragment::io::RootResultInput::new(
                    chunk,
                    input(&fixture.session).await,
                ),
            );
            fixture.session.submit_input(&mut submitted).unwrap();
            assert!(
                submitted.is_none(),
                "accepted input transfers its exact caller slot"
            );
        };
    }
    fixture.session.finish_input().unwrap();
    // Three records exceed the unacknowledged window: consume while reading.
    let mut bytes = Vec::new();
    let mut sequence = 1;
    let rows = loop {
        let delivery = read(&fixture.channel, Some(sequence), sequence - 1).await;
        match &delivery.reply().outcome {
            RootReadOutcome::Data(data) => bytes.extend_from_slice(data.body().as_ref()),
            RootReadOutcome::End(end) => break end.output_rows,
            // The 1 ms long poll can expire before the producer publishes.
            RootReadOutcome::NotReady => continue,
            other => panic!("unexpected root outcome {other:?}"),
        }
        sequence += 1;
    };
    assert_eq!(rows, 4);
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    let mut rest = bytes.as_slice();
    let mut schema = None;
    let mut batches = Vec::new();
    while !rest.is_empty() {
        let header = CowSelectionRecordHeader::parse(rest).unwrap();
        let (record, tail) = rest.split_at(header.record_bytes() as usize);
        match header.kind() {
            CowSelectionRecordKind::Schema => {
                schema = Some(CowSelectionDecoder::decode_schema(record).unwrap())
            }
            CowSelectionRecordKind::Batch => batches
                .push(CowSelectionDecoder::decode_batch(schema.as_ref().unwrap(), record).unwrap()),
        }
        rest = tail;
    }
    let decoded = batches
        .iter()
        .map(|batch| batch.column(0).clone())
        .collect::<Vec<_>>();
    let original = expected
        .iter()
        .map(|batch| batch.column(0).clone())
        .collect::<Vec<_>>();
    assert_eq!(decoded, original);
    shutdown(&fixture.pool).await;
}

fn scalar(field: ScalarField) -> (FrozenRootOutput, ScalarSchema) {
    let schema = ScalarSchema::try_new(field)
        .unwrap()
        .bind_native_slots(&[7])
        .unwrap();
    (FrozenRootOutput::ScalarValue(schema.clone()), schema)
}
fn nullable_i64() -> ScalarField {
    ScalarField {
        nullable: true,
        value_type: ScalarValueType::SignedInteger(64),
    }
}
fn longs(values: Vec<Option<i64>>) -> Chunk {
    owned_chunk(Arc::new(Int64Array::from(values)))
}
async fn scalar_record(fixture: &Fixture, schema: &ScalarSchema, rows: u64) -> ScalarRecord {
    wait_until(|| fixture.session.producer_state() == RootProducerState::ContextHeld).await;
    await_whole_exit(&fixture.session).await;
    assert_eq!(fixture.channel.snapshot().data_positions, 1);
    let data = read(&fixture.channel, Some(1), 0).await;
    let record = ScalarRecord::decode_owned(schema, body(&data)).unwrap();
    drop(data);
    assert_end(&read(&fixture.channel, Some(2), 1).await, 2, rows);
    record
}

#[tokio::test]
async fn scalar_value_is_published_once_at_sealed_end_after_empty_batches() {
    let (output, schema) = scalar(nullable_i64());
    let fixture = Fixture::new(output);
    for chunk in [longs(vec![]), longs(vec![Some(42)]), longs(vec![])] {
        {
            let mut submitted = Some(
                novarocks_execution::runtime::fragment::io::RootResultInput::new(
                    chunk,
                    input(&fixture.session).await,
                ),
            );
            fixture.session.submit_input(&mut submitted).unwrap();
            assert!(
                submitted.is_none(),
                "accepted input transfers its exact caller slot"
            );
        };
    }
    // The next permit proves every earlier input exited; the accepted row is
    // still unpublished until the sealed normal End.
    let held = input(&fixture.session).await;
    assert_eq!(fixture.channel.snapshot().produced_through, 0);
    drop(held);
    fixture.session.finish_input().unwrap();
    assert_eq!(
        scalar_record(&fixture, &schema, 1).await,
        ScalarRecord::Value(ScalarValue::SignedInteger {
            bits: 64,
            value: 42
        })
    );
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn scalar_zero_rows_is_unique_no_rows_and_one_null_row_is_a_value() {
    let (output, schema) = scalar(nullable_i64());
    let no_input = Fixture::new(output.clone());
    no_input.session.finish_input().unwrap();
    assert_eq!(
        scalar_record(&no_input, &schema, 0).await,
        ScalarRecord::NoRows
    );
    shutdown(&no_input.pool).await;

    let empty_batches = Fixture::new(output.clone());
    for _ in 0..2 {
        {
            let mut submitted = Some(
                novarocks_execution::runtime::fragment::io::RootResultInput::new(
                    longs(vec![]),
                    input(&empty_batches.session).await,
                ),
            );
            empty_batches.session.submit_input(&mut submitted).unwrap();
            assert!(
                submitted.is_none(),
                "accepted input transfers its exact caller slot"
            );
        };
    }
    empty_batches.session.finish_input().unwrap();
    assert_eq!(
        scalar_record(&empty_batches, &schema, 0).await,
        ScalarRecord::NoRows
    );
    shutdown(&empty_batches.pool).await;

    let null_row = Fixture::new(output);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                longs(vec![None]),
                input(&null_row.session).await,
            ),
        );
        null_row.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    null_row.session.finish_input().unwrap();
    assert_eq!(
        scalar_record(&null_row, &schema, 1).await,
        ScalarRecord::Value(ScalarValue::Null)
    );
    shutdown(&null_row.pool).await;
}

#[tokio::test]
async fn scalar_second_row_fails_before_any_record_is_published() {
    let (output, _) = scalar(nullable_i64());
    let across_batches = Fixture::new(output.clone());
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                longs(vec![Some(1)]),
                input(&across_batches.session).await,
            ),
        );
        across_batches.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                longs(vec![Some(2)]),
                input(&across_batches.session).await,
            ),
        );
        across_batches.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    await_whole_exit(&across_batches.session).await;
    assert!(matches!(
        across_batches.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert_eq!(across_batches.channel.snapshot().produced_through, 0);
    assert_eq!(across_batches.channel.snapshot().data_positions, 0);
    shutdown(&across_batches.pool).await;

    let one_batch = Fixture::new(output);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                longs(vec![Some(1), Some(2)]),
                input(&one_batch.session).await,
            ),
        );
        one_batch.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    await_whole_exit(&one_batch.session).await;
    assert!(matches!(
        one_batch.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert_eq!(one_batch.channel.snapshot().produced_through, 0);
    shutdown(&one_batch.pool).await;
}

#[tokio::test]
async fn scalar_cancel_after_an_accepted_row_publishes_neither_value_nor_no_rows() {
    let (output, _) = scalar(nullable_i64());
    let fixture = Fixture::new(output);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                longs(vec![Some(7)]),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    let held = input(&fixture.session).await;
    drop(held);
    fixture
        .session
        .abort(ResultAbort::Cancelled("cancel scalar root".into()));
    await_whole_exit(&fixture.session).await;
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert_eq!(fixture.channel.snapshot().produced_through, 0);
    assert_eq!(fixture.channel.snapshot().data_positions, 0);
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn scalar_nested_list_record_is_published_whole() {
    use arrow::array::ListArray;
    use arrow::buffer::OffsetBuffer;
    let (output, schema) = scalar(ScalarField {
        nullable: true,
        value_type: ScalarValueType::List(Box::new(ScalarField {
            nullable: true,
            value_type: ScalarValueType::String,
        })),
    });
    let fixture = Fixture::new(output);
    let item = owned_field("item", DataType::Utf8, true);
    let list = ListArray::try_new(
        Arc::clone(item.field()),
        OffsetBuffer::from_lengths([3]),
        Arc::new(StringArray::from(vec![Some("a"), None, Some("bc")])),
        None,
    )
    .unwrap();
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                owned_nested_chunk(Arc::new(list), vec![item]),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    fixture.session.finish_input().unwrap();
    assert_eq!(
        scalar_record(&fixture, &schema, 1).await,
        ScalarRecord::Value(ScalarValue::List(vec![
            ScalarValue::String("a".into()),
            ScalarValue::Null,
            ScalarValue::String("bc".into()),
        ]))
    );
    shutdown(&fixture.pool).await;
}

#[tokio::test]
async fn scalar_type_mismatch_fails_without_a_record() {
    let (output, _) = scalar(ScalarField {
        nullable: true,
        value_type: ScalarValueType::SignedInteger(32),
    });
    let fixture = Fixture::new(output);
    {
        let mut submitted = Some(
            novarocks_execution::runtime::fragment::io::RootResultInput::new(
                longs(vec![Some(1)]),
                input(&fixture.session).await,
            ),
        );
        fixture.session.submit_input(&mut submitted).unwrap();
        assert!(
            submitted.is_none(),
            "accepted input transfers its exact caller slot"
        );
    };
    await_whole_exit(&fixture.session).await;
    assert!(matches!(
        fixture.session.producer_state(),
        RootProducerState::Failed(_)
    ));
    assert_eq!(fixture.channel.snapshot().produced_through, 0);
    shutdown(&fixture.pool).await;
}

// The input fixtures independently establish the violated storage contract.
// Root session and IO must preserve that exact author, not restate its text.
fn assert_original_root_storage_cause(
    error: &novarocks_execution::runtime::fragment::io::FragmentIoError,
    expected: novarocks_execution::exec::chunk::RootArrayStorageError,
) {
    use novarocks_execution_contract::failure::FailureProjection;
    assert_eq!(error.failure_fact(), expected.failure_fact());
    let mut source = std::error::Error::source(error);
    let mut original = None;
    while let Some(cause) = source {
        if let Some(cause) =
            cause.downcast_ref::<novarocks_execution::exec::chunk::RootArrayStorageError>()
        {
            original = Some(*cause);
            break;
        }
        source = cause.source();
    }
    assert_eq!(original, Some(expected));
}
