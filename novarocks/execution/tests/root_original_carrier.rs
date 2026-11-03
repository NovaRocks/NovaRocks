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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, DictionaryArray, ListArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Int32Type};
use arrow::record_batch::RecordBatch;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks_execution::exec::operators::RootResultSinkFactory;
use novarocks_execution::exec::pipeline::driver::{DriverState, PipelineDriver};
use novarocks_execution::exec::pipeline::driver_step::DriverStep;
use novarocks_execution::exec::pipeline::input::PreparedInput;
use novarocks_execution::exec::pipeline::operator::{
    InputEncodingPolicy, Operator, OperatorLifecycle,
};
use novarocks_execution::exec::pipeline::operator_factory::OperatorFactory;
use novarocks_execution::exec::pipeline::shape::{
    SinkKernel, SinkStep, Source, SourceKernel, SourceStart, Transform,
};
use novarocks_execution::exec::pipeline::step::{NextStep, StepResult};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime::observable::Observable;
use novarocks_execution::runtime::profile::{OperatorProfiles, Profiler};
use novarocks_execution::runtime::runtime_state::RuntimeState;
use novarocks_execution::runtime::task::ExecutionResult;
use novarocks_execution::runtime::task::io::{
    ResultAbort, ResultWriteCredit, RootInputAdmission, RootInputAuthority, RootProducerState,
    RootResultInput, RootResultSession, RootResultWriteSpec, RootWorkExitProgress, TaskIoError,
};
use novarocks_execution_contract::TaskIdentity;
use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
use novarocks_types::SlotId;
use novarocks_types::logical::{LogicalType, field_with_logical_type};
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId};
use std::sync::atomic::{AtomicBool, Ordering};

type Received = Arc<Mutex<Option<Chunk>>>;

struct OneChunkSource {
    chunk: Option<Chunk>,
    root_authority: Option<Arc<RootInputAuthority>>,
}

impl OperatorLifecycle for OneChunkSource {
    fn name(&self) -> &str {
        "OriginalCarrierSource"
    }
}
impl SourceKernel for OneChunkSource {
    fn start(&self) -> SourceStart {
        if self.chunk.is_some() {
            SourceStart::Ready
        } else {
            SourceStart::Finished
        }
    }
    fn poll_output(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(authority) = &self.root_authority {
            assert!(
                !authority.is_available(),
                "root admission must precede production"
            );
        }
        Ok(StepResult::new(self.chunk.take(), NextStep::Finished, 0))
    }
}

struct OriginalSession {
    received: Received,
    spec: RootResultWriteSpec,
    authority: Arc<RootInputAuthority>,
    finished: AtomicBool,
    exit_observable: Arc<Observable>,
    exit_waker: Mutex<Option<std::task::Waker>>,
    arrays: Vec<ArrayRef>,
    schema: ChunkSchemaRef,
    source_tracker: Arc<MemTracker>,
    original_charge: i64,
}

struct OrdinaryReceiver {
    received: Received,
}
impl OperatorLifecycle for OrdinaryReceiver {
    fn name(&self) -> &str {
        "OrdinaryReceiver"
    }
}
impl InputEncodingPolicy for OrdinaryReceiver {}
impl SinkKernel for OrdinaryReceiver {
    fn consume(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let chunk = input.into_uncovered_chunk()?;
        assert_eq!(chunk.columns()[0].data_type(), &DataType::Utf8);
        let mut slot = self.received.lock().unwrap();
        assert!(
            slot.is_none(),
            "the finite source delivers exactly one input"
        );
        let rows = chunk.len() as u64;
        *slot = Some(chunk);
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
}

impl RootResultSession for OriginalSession {
    fn spec(&self) -> &RootResultWriteSpec {
        &self.spec
    }
    fn try_acquire_input(&self) -> Result<RootInputAdmission, TaskIoError> {
        let novarocks_execution::runtime::task::io::RootInputReservationAdmission::Granted(
            mut reservation,
        ) = self.authority.try_reserve()?
        else {
            return Ok(RootInputAdmission::Blocked);
        };
        let mut credit = Some(ResultWriteCredit::new(reservation.required_bytes(), |_| {}));
        let admission = reservation.try_bind(&mut credit);
        if !matches!(admission, Ok(RootInputAdmission::Granted(_))) {
            reservation.retire_credit(&mut credit);
        }
        admission
    }
    fn writable_observable(&self) -> Arc<Observable> {
        self.authority.observable()
    }
    fn submit_input(&self, offered: &mut Option<RootResultInput>) -> Result<(), TaskIoError> {
        let submitted = offered
            .as_ref()
            .expect("root submission owns its offered slot");
        assert!(self.authority.owns_original(submitted.permit()));
        if self.finished.load(Ordering::Acquire) {
            return Err(TaskIoError::new(
                novarocks_execution::runtime::task::io::TaskIoOperation::ResultWrite,
                novarocks_execution::runtime::task::io::TaskIoErrorKind::Cancelled,
                "fixture original input is closed",
            ));
        }
        assert_eq!(submitted.permit().task(), self.spec.task);
        let chunk = submitted.chunk();
        assert!(Arc::ptr_eq(&self.schema, &chunk.chunk_schema_ref()));
        assert_eq!(chunk.columns().len(), self.arrays.len());
        for (actual, original) in chunk.columns().iter().zip(&self.arrays) {
            assert!(
                Arc::ptr_eq(actual, original),
                "the carrier was reconstructed"
            );
            assert_eq!(actual.data_type(), original.data_type());
        }
        assert_eq!(
            chunk.columns()[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(
            self.source_tracker.current(),
            self.original_charge,
            "the original input accounting owner must survive the final edge"
        );
        // Build the Context-owned alias and acquire its empty slot before
        // admission; a failed check/panic still leaves the offered input owned
        // by its caller. No input job remains when submission returns.
        let context_payload = chunk.clone();
        let mut received = self.received.lock().unwrap();
        assert!(received.is_none());
        let submitted = offered
            .take()
            .expect("validated original input remains present");
        *received = Some(context_payload);
        drop(received);
        let (chunk, mut permit) = submitted.into_cleanup_parts();
        drop(chunk);
        assert_eq!(permit.retire(), RootWorkExitProgress::Exited);
        Ok(())
    }
    fn finish_input(&self) -> Result<(), TaskIoError> {
        assert!(self.authority.is_available());
        assert!(!self.finished.swap(true, Ordering::AcqRel));
        // Submission is synchronous; retained output is Context-owned, not a job.
        let exit_waker = self.exit_waker.lock().unwrap().clone();
        self.exit_observable.notify_observers();
        if let Some(waker) = exit_waker {
            waker.wake();
        }
        self.authority.observable().notify_observers();
        Ok(())
    }
    fn producer_state(&self) -> RootProducerState {
        if self.finished.load(Ordering::Acquire) {
            RootProducerState::ParticipationHeld
        } else {
            RootProducerState::Accepting
        }
    }
    fn producer_exit_observable(&self) -> Arc<Observable> {
        self.exit_observable.clone()
    }
    fn poll_producer_exit(&self, waker: &std::task::Waker) -> RootWorkExitProgress {
        let mut registered = self.exit_waker.lock().unwrap();
        if let Some(previous) = registered.as_ref() {
            if !previous.will_wake(waker) {
                return RootWorkExitProgress::Unresolved;
            }
        } else {
            *registered = Some(waker.clone());
        }
        let issuer = self.authority.input_exit_progress();
        if issuer == RootWorkExitProgress::Unresolved {
            RootWorkExitProgress::Unresolved
        } else if self.finished.load(Ordering::Acquire) && issuer == RootWorkExitProgress::Exited {
            RootWorkExitProgress::Exited
        } else {
            RootWorkExitProgress::Pending
        }
    }
    fn abort(&self, reason: ResultAbort) {
        panic!("unexpected root abort: {reason:?}")
    }
}

fn source_chunk() -> Chunk {
    let dictionary: ArrayRef = Arc::new(
        [Some("alpha"), None, Some("omega")]
            .into_iter()
            .collect::<DictionaryArray<Int32Type>>(),
    );
    let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(7), None]),
        None,
        Some(vec![Some(11)]),
    ]));
    let json_field = field_with_logical_type(
        Field::new("document", DataType::Utf8, false).with_metadata(HashMap::from([(
            "nested_origin".to_string(),
            "catalog-json-field".to_string(),
        )])),
        LogicalType::Json,
    );
    let nested_fields = Fields::from(vec![
        Field::new("items", list.data_type().clone(), true),
        json_field,
    ]);
    let nested: ArrayRef = Arc::new(
        StructArray::try_new(
            nested_fields,
            vec![
                list,
                Arc::new(StringArray::from(vec!["{}", "{\"x\":1}", "[]"])),
            ],
            None,
        )
        .unwrap(),
    );
    let arrays = vec![dictionary, nested];
    let fields = [
        Field::new("encoded", arrays[0].data_type().clone(), true).with_metadata(HashMap::from([
            ("carrier_origin".to_string(), "original".to_string()),
        ])),
        Field::new("nested", arrays[1].data_type().clone(), false).with_metadata(HashMap::from([
            ("nested_contract".to_string(), "preserved".to_string()),
        ])),
    ];
    let slots = fields
        .into_iter()
        .enumerate()
        .map(|(ordinal, field)| {
            ChunkSlotSchema::new_with_field(
                SlotId::new(ordinal as u32 + 1),
                field,
                None,
                Some(ordinal as i32 + 101),
            )
        })
        .collect();
    let schema = Arc::new(
        ChunkSchema::try_new_with_schema_metadata(
            slots,
            HashMap::from([("source_owner".to_string(), "original-fixture".to_string())]),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), arrays).unwrap();
    Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}

fn run_driver(
    chunk: Chunk,
    sink: Box<dyn Operator>,
    root_authority: Option<Arc<RootInputAuthority>>,
) -> OperatorProfiles {
    let profiler = Profiler::new("root-original-carrier");
    let profiles = vec![
        OperatorProfiles::new(profiler.child("Source")),
        OperatorProfiles::new(profiler.child("Receiver")),
    ];
    let sink_profile = profiles[1].clone();
    let runtime = Arc::new(RuntimeState::for_local_execution(
        None,
        None,
        None,
        None,
        None,
        Some(MemTracker::new_root("driver-accounting")),
        None,
    ));
    let mut driver = PipelineDriver::new(
        0,
        vec![
            Box::new(Source::new(OneChunkSource {
                chunk: Some(chunk),
                root_authority,
            })),
            sink,
        ],
        Some(profiler),
        profiles,
        runtime,
        None,
    );
    for _ in 0..16 {
        match driver.process(Duration::from_millis(20)) {
            DriverState::Finished => return sink_profile,
            DriverState::Ready | DriverState::Running => {}
            DriverState::Failed(error) => panic!("driver failed: {error}"),
            other => panic!("driver did not finish its finite input: {other:?}"),
        }
    }
    panic!("driver failed to finish within the bounded turn count")
}

#[test]
fn original_receiver_preserves_carrier_schema_and_accounting_with_profiler() {
    let mut chunk = source_chunk();
    let arrays = chunk.columns().to_vec();
    let schema = chunk.chunk_schema_ref();
    let source_tracker = MemTracker::new_root("precovered-original-input");
    chunk.transfer_to(&source_tracker);
    let original_charge = source_tracker.current();
    assert!(original_charge > 0);
    let received = Arc::new(Mutex::new(None));
    let spec = RootResultWriteSpec {
        task: TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(51, 52), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        ),
        contract: Arc::new(RootOutputContract::new(
            RootProfileId::V1,
            FrozenRootOutput::CountOnly,
        )),
    };
    let authority = Arc::new(RootInputAuthority::new(
        &spec,
        Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::new()),
    ));
    let session = Arc::new(OriginalSession {
        received: Arc::clone(&received),
        spec,
        authority: Arc::clone(&authority),
        finished: AtomicBool::new(false),
        exit_observable: Arc::new(Observable::new()),
        exit_waker: Mutex::new(None),
        arrays,
        schema: Arc::clone(&schema),
        source_tracker: Arc::clone(&source_tracker),
        original_charge,
    });
    let factory = RootResultSinkFactory::try_new(session, 1).unwrap();
    factory.bind_pipeline_dop(1).unwrap();
    let profiles = run_driver(chunk, factory.create(1, 0), Some(authority));
    let chunk = received.lock().unwrap().take().unwrap();
    assert!(Arc::ptr_eq(&schema, &chunk.chunk_schema_ref()));
    assert_eq!(profiles.unique.counter_value("DictInputColumns"), Some(0));
    assert_eq!(
        profiles.unique.counter_value("DictHydratedColumns"),
        Some(0)
    );
    assert_eq!(
        profiles.common.counter_value("OperatorPeakMemoryUsage"),
        Some(0)
    );
    assert_eq!(source_tracker.current(), original_charge);
    drop(chunk);
    assert_eq!(source_tracker.current(), 0);
}

#[test]
fn ordinary_receiver_still_hydrates_dictionary_and_records_profile() {
    use novarocks_type_contract::{
        FunctionValueType, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    // Root's original-carrier fixture above intentionally retains imported
    // metadata. This ordinary receiver has an explicit complete declaration,
    // including nested JSON; execution metadata cannot author that domain.
    let source = source_chunk();
    let storage = ValueTypeStorageLifetime::pure();
    let item =
        ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), true, storage.clone()).unwrap();
    let list = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    let json = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        false,
        storage.clone(),
    )
    .unwrap();
    let semantic = ValueTypeRef::try_new(
        ValueKind::Struct(
            vec![
                ValueField::new("items", list, storage.clone()),
                ValueField::new("document", json, storage.clone()),
            ]
            .into_boxed_slice(),
        ),
        false,
        storage.clone(),
    )
    .unwrap();
    let nested_layout = DataType::Struct(Fields::from(vec![
        Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new("document", DataType::Utf8, false),
    ]));
    let declarations = [
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::try_from_value(
            semantic,
            ValueLayout::try_new(&nested_layout, storage.clone()).unwrap(),
        )
        .unwrap(),
    ];
    let slots = source
        .chunk_schema()
        .slots()
        .iter()
        .zip(&declarations)
        .map(|(source, declaration)| {
            ChunkSlotSchema::try_new_typed(
                source.slot_id(),
                Arc::new(source.field().clone()),
                declaration.typed_binding().clone(),
                source.unique_id(),
                storage.clone(),
            )
            .unwrap()
        })
        .collect();
    let schema = Arc::new(ChunkSchema::try_new_typed(slots, storage).unwrap());
    let factory = novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("ordinary receiver typed source"),
        Arc::new(novarocks_execution::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let owner = factory.begin_typed_construction(&schema).unwrap();
    let chunk = Chunk::try_new_typed_with_columns_owned_observed::<
        novarocks_execution::exec::pipeline::operator::ConstructionFailure,
        _,
    >(
        schema,
        source.columns().to_vec(),
        source.len(),
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap();
    drop(source);
    let original_array = Arc::clone(&chunk.columns()[0]);
    let metadata = chunk.schema().metadata().clone();
    let received = Arc::new(Mutex::new(None));
    let sink = OrdinaryReceiver {
        received: Arc::clone(&received),
    };
    let mut receiver = Transform::hydrating(sink);
    receiver.set_construction_factory(factory);
    let profiles = run_driver(chunk, Box::new(receiver), None);
    let chunk = received.lock().unwrap().take().unwrap();
    assert!(!Arc::ptr_eq(&original_array, &chunk.columns()[0]));
    assert_eq!(chunk.schema().metadata(), &metadata);
    assert!(metadata.is_empty());
    assert!(matches!(
        chunk.chunk_schema().slots()[1]
            .typed_binding()
            .value_type()
            .child_type_at(1)
            .unwrap()
            .kind(),
        ValueKind::Nominal(ValueLogicalType::Json),
    ));
    let values = chunk.columns()[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "alpha");
    assert!(values.is_null(1));
    assert_eq!(values.value(2), "omega");
    assert_eq!(profiles.unique.counter_value("DictInputColumns"), Some(1));
    assert_eq!(
        profiles.unique.counter_value("DictHydratedColumns"),
        Some(1)
    );
    assert_eq!(profiles.unique.counter_value("DictHydratedRows"), Some(3));
}
