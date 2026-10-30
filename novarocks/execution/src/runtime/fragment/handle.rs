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

use std::sync::Arc;

use crate::exec::pipeline::executor::PreparedPipelineExecution;
use crate::runtime::execution_runtime::ExecutionRuntime;
use crate::runtime::fragment::error::{
    FragmentExecutionError, FragmentExecutionErrorKind, FragmentLaunchError,
    FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::fact::{FragmentCancelReason, FragmentOutcome, FragmentTerminalFact};
use crate::runtime::fragment::io::FragmentEventSink;
#[cfg(test)]
use crate::runtime::fragment::io::NoopFragmentEventSink;
use crate::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentResultWriter, ResultWriteSpec,
};
use crate::runtime::fragment::io::{
    ExchangeReceiverPort, FragmentCommitPort, UnavailableExchangeReceiverPort,
    UnavailableFragmentCommitPort,
};
use crate::runtime::fragment::resources::{FragmentResources, ResourceCleanupFaults};
use crate::runtime::fragment::runtime_state::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter::RuntimeFilterSessionRef;
use novarocks_types::QueryId;

pub struct FragmentPrepareContext {
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    runtime_filter: Option<RuntimeFilterSessionRef>,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    #[cfg(novarocks_dev_controls)]
    exchange_test_observer: Option<Arc<dyn crate::runtime::exchange::TestEventObserver>>,
    result_writer: Arc<dyn FragmentResultWriter>,
    root_result_session: Option<Arc<dyn crate::runtime::fragment::io::RootResultSession>>,
    event_sink: Arc<dyn FragmentEventSink>,
    result_spec: Option<ResultWriteSpec>,
    result_identity: Option<novarocks_execution_contract::TaskIdentity>,
    task_control: Option<Arc<crate::runtime::task_control::TaskExecutionControl>>,
    root_sink_dop: Option<i32>,
    execution_runtime: Option<Arc<ExecutionRuntime>>,
    query_memory: Option<crate::runtime::query_memory::QueryMemoryBinding>,
    commit_port: Arc<dyn FragmentCommitPort>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    receiver_construction_lifetime: ReceiverConstructionLifetime,
    /// The per-edge send permission this task's push sinks are bound by.
    ///
    /// Absent means the caller runs no closed-edge barrier, which is how
    /// every non-task caller and every test that does not exercise it builds
    /// a context. A task-protocol submission always supplies one, because the
    /// barrier is the only thing that stops a producer from sending to a
    /// destination that has not acknowledged its creation.
    edge_gates: Option<Arc<crate::runtime::fragment::io::exchange_edge::ExchangeEdgeGates>>,
    #[cfg(test)]
    prepare_failure: Option<PrepareFailurePoint>,
    #[cfg(test)]
    cleanup_faults: ResourceCleanupFaults,
    #[cfg(test)]
    start_failure: Option<StartFailurePoint>,
}

#[derive(Clone, Copy)]
enum ReceiverConstructionLifetime {
    KernelBound,
    RegistrationOwned,
}

type ReceiverConstruction = (
    Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    Option<crate::exec::pipeline::operator::input_construction::ReceiverInputConstructionStopOwner>,
);

#[cfg(test)]
mod owner_tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::exec::chunk::Chunk;
    use crate::exec::fragment::program::FragmentContractVersion;
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::fragment::io::{
        ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverPort,
        ExchangeReceiverRegistration, ExecutionRuntimeExchangeReceiverPort, FragmentIoError,
        FragmentResultSession, FragmentResultWriter, ResultAbort, ResultWriteSpec,
    };
    use crate::runtime::query_options::QueryOptions;
    use novarocks_types::{QueryId, SlotId, UniqueId};

    use super::{
        CompiledFragmentSubmission, FragmentCancelReason, FragmentOutcome, FragmentPrepareContext,
        RunningFragmentHandle, prepare_compiled_fragment,
    };

    /// Loan the actual registration to the two preparation rollback fixtures;
    /// all registration, decoding and cleanup stay on the original port.
    pub(in crate::runtime::fragment::handle) struct ReceiverConstructionPort {
        delegate: ExecutionRuntimeExchangeReceiverPort,
        cancelled: AtomicUsize,
        removed: AtomicUsize,
        pub(in crate::runtime::fragment::handle) registrations:
            Mutex<Vec<ExchangeReceiverRegistration>>,
    }

    impl ReceiverConstructionPort {
        pub(in crate::runtime::fragment::handle) fn new(
            runtime: Arc<crate::runtime::execution_runtime::ExecutionRuntime>,
        ) -> Arc<Self> {
            Arc::new(Self {
                delegate: ExecutionRuntimeExchangeReceiverPort::new(runtime),
                cancelled: AtomicUsize::new(0),
                removed: AtomicUsize::new(0),
                registrations: Mutex::new(Vec::new()),
            })
        }
    }

    impl ExchangeReceiverPort for ReceiverConstructionPort {
        fn register(
            &self,
            registration: ExchangeReceiverRegistration,
        ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
            self.delegate.register(registration.clone())?;
            self.registrations.lock().unwrap().push(registration);
            Ok(())
        }
        fn push(
            &self,
            key: ExchangeReceiverKey,
            frame: ExchangeReceiverFrame,
        ) -> Result<(), crate::runtime::exchange::ExchangeFailure> {
            self.delegate.push(key, frame)
        }
        fn cancel(&self, key: ExchangeReceiverKey) {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            self.delegate.cancel(key);
        }
        fn remove(&self, key: ExchangeReceiverKey) {
            self.removed.fetch_add(1, Ordering::SeqCst);
            self.delegate.remove(key);
        }
        fn cancel_fragment(&self, fragment_instance_id: UniqueId) {
            self.delegate.cancel_fragment(fragment_instance_id);
        }
        fn receiver_handle(
            &self,
            key: ExchangeReceiverKey,
            expected_senders: usize,
        ) -> Result<
            crate::runtime::exchange::ExchangeReceiverHandle,
            crate::runtime::exchange::ExchangeFailure,
        > {
            self.delegate.receiver_handle(key, expected_senders)
        }
        fn stream_close_facts(
            &self,
            key: ExchangeReceiverKey,
            expected: &[novarocks_execution_contract::ExchangeStreamIdentity],
        ) -> Result<
            Vec<novarocks_execution_contract::ExchangeStreamClose>,
            crate::runtime::exchange::ExchangeFailure,
        > {
            self.delegate.stream_close_facts(key, expected)
        }
        fn ensure_mem_tracker(
            &self,
            key: ExchangeReceiverKey,
            root: &Arc<crate::runtime::mem_tracker::MemTracker>,
        ) -> Result<
            Arc<crate::runtime::mem_tracker::MemTracker>,
            crate::runtime::exchange::ExchangeFailure,
        > {
            self.delegate.ensure_mem_tracker(key, root)
        }
        fn push_local(
            &self,
            key: ExchangeReceiverKey,
            sender_id: i32,
            backend_number: i32,
            chunks: Vec<Chunk>,
            eos: bool,
        ) {
            self.delegate
                .push_local(key, sender_id, backend_number, chunks, eos);
        }
        fn snapshot(
            &self,
            key: ExchangeReceiverKey,
        ) -> Option<crate::runtime::exchange::ExchangeReceiverSnapshot> {
            self.delegate.snapshot(key)
        }
    }

    fn empty_result_submission(finst_id: UniqueId) -> CompiledFragmentSubmission {
        submission(
            finst_id,
            crate::exec::chunk::typed_fixture::chunk(
                arrow::record_batch::RecordBatch::new_empty(Arc::new(Schema::empty())),
                &[],
                &[],
            ),
            novarocks_physical_plan::FragmentSink::Result,
        )
    }

    fn submission(
        finst_id: UniqueId,
        chunk: Chunk,
        sink: novarocks_physical_plan::FragmentSink,
    ) -> CompiledFragmentSubmission {
        let program = crate::exec::pipeline::builder::values_chunk_program(&chunk, sink);
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId::new(finst_id.high() - 2, finst_id.low() - 2),
            FragmentInstanceId::new(finst_id),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::MIN,
            BackendNum::try_new(1).unwrap(),
        );
        CompiledFragmentSubmission::try_new(
            program,
            super::super::scan::CompiledScanSources::new(),
            instance,
        )
        .unwrap()
    }

    fn static_submission(
        finst_id: UniqueId,
        chunk: Chunk,
        sink: novarocks_local_program::StaticSinkProgram,
    ) -> CompiledFragmentSubmission {
        let sink = match sink {
            novarocks_local_program::StaticSinkProgram::RootResult(contract) => {
                novarocks_physical_plan::FragmentSink::RootResult(Box::new(
                    contract.as_ref().clone(),
                ))
            }
            novarocks_local_program::StaticSinkProgram::Result => {
                novarocks_physical_plan::FragmentSink::Result
            }
            _ => panic!("lifecycle fixture requires an admitted result sink"),
        };
        submission(finst_id, chunk, sink)
    }

    fn one_row_chunk() -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![7]))],
        )
        .expect("one row batch");
        crate::exec::chunk::typed_fixture::chunk(
            batch,
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        )
    }

    struct RejectLegacyRootWriter;
    impl FragmentResultWriter for RejectLegacyRootWriter {
        fn open(
            &self,
            _: ResultWriteSpec,
        ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
            panic!("a frozen bounded root must not open the legacy result buffer");
        }
    }
    pub(super) struct BoundedRootSession {
        spec: crate::runtime::fragment::io::RootResultWriteSpec,
        authority: crate::runtime::fragment::io::RootInputAuthority,
        input: Mutex<Option<crate::runtime::fragment::io::RootResultInput>>,
        finishing: AtomicBool,
        exited: AtomicBool,
        exit_observable: Arc<crate::runtime::observable::Observable>,
        exit_waker: Mutex<Option<std::task::Waker>>,
        aborts: Mutex<Vec<ResultAbort>>,
    }
    impl BoundedRootSession {
        pub(super) fn new() -> Arc<Self> {
            use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, StageId, TaskId};
            let task = novarocks_execution_contract::TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(390, 391), AttemptId::new(1).unwrap()).unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            );
            let spec = crate::runtime::fragment::io::RootResultWriteSpec {
                task,
                contract: Arc::new(novarocks_result_contract::RootOutputContract::new(
                    novarocks_result_contract::RootProfileId::V1,
                    novarocks_result_contract::FrozenRootOutput::CountOnly,
                )),
            };
            Arc::new(Self {
                authority: crate::runtime::fragment::io::RootInputAuthority::new(
                    &spec,
                    Arc::new(crate::runtime::execution_health::ExecutionHealth::new()),
                ),
                spec,
                input: Mutex::new(None),
                finishing: AtomicBool::new(false),
                exited: AtomicBool::new(false),
                exit_observable: Arc::new(crate::runtime::observable::Observable::new()),
                exit_waker: Mutex::new(None),
                aborts: Mutex::new(Vec::new()),
            })
        }
        fn actual_exit(&self) {
            let input = self.input.lock().unwrap().take();
            if let Some(input) = input {
                let (chunk, mut permit) = input.into_cleanup_parts();
                drop(chunk);
                assert_eq!(
                    permit.retire(),
                    crate::runtime::fragment::io::RootWorkExitProgress::Exited
                );
            }
            self.exited.store(true, Ordering::Release);
            let exit_waker = self.exit_waker.lock().unwrap().clone();
            self.exit_observable.notify_observers();
            if let Some(waker) = exit_waker {
                waker.wake();
            }
            self.authority.observable().notify_observers();
        }
        pub(super) fn context(self: &Arc<Self>) -> FragmentPrepareContext {
            self.context_with_control(crate::runtime::task_control::test_support::control_for(
                self.spec.task,
            ))
        }
        fn context_with_control(
            self: &Arc<Self>,
            control: Arc<crate::runtime::task_control::TaskExecutionControl>,
        ) -> FragmentPrepareContext {
            FragmentPrepareContext {
                result_writer: Arc::new(RejectLegacyRootWriter),
                ..Default::default()
            }
            .with_task_control(control)
            .with_result_identity(self.spec.task)
            .with_root_result_session(
                Arc::clone(self) as Arc<dyn crate::runtime::fragment::io::RootResultSession>
            )
        }
        fn submission(&self) -> CompiledFragmentSubmission {
            static_submission(
                UniqueId::new(399, 400),
                one_row_chunk(),
                novarocks_local_program::StaticSinkProgram::RootResult(Arc::clone(
                    &self.spec.contract,
                )),
            )
        }
    }
    impl crate::runtime::fragment::io::RootResultSession for BoundedRootSession {
        fn spec(&self) -> &crate::runtime::fragment::io::RootResultWriteSpec {
            &self.spec
        }
        fn try_acquire_input(
            &self,
        ) -> Result<crate::runtime::fragment::io::RootInputAdmission, FragmentIoError> {
            let crate::runtime::fragment::io::RootInputReservationAdmission::Granted(
                mut reservation,
            ) = self.authority.try_reserve()?
            else {
                return Ok(crate::runtime::fragment::io::RootInputAdmission::Blocked);
            };
            let mut credit = Some(crate::runtime::fragment::io::ResultWriteCredit::new(
                reservation.required_bytes(),
                |_| {},
            ));
            let admission = reservation.try_bind(&mut credit);
            if !matches!(
                admission,
                Ok(crate::runtime::fragment::io::RootInputAdmission::Granted(_))
            ) {
                reservation.retire_credit(&mut credit);
            }
            admission
        }
        fn writable_observable(&self) -> Arc<crate::runtime::observable::Observable> {
            self.authority.observable()
        }
        fn submit_input(
            &self,
            offered: &mut Option<crate::runtime::fragment::io::RootResultInput>,
        ) -> Result<(), FragmentIoError> {
            let input = offered
                .as_ref()
                .expect("root submission owns its offered slot");
            assert!(self.authority.owns_original(input.permit()));
            if self.finishing.load(Ordering::Acquire)
                || self.exited.load(Ordering::Acquire)
                || !self.aborts.lock().unwrap().is_empty()
            {
                return Err(FragmentIoError::new(
                    crate::runtime::fragment::io::FragmentIoOperation::ResultWrite,
                    crate::runtime::fragment::io::FragmentIoErrorKind::Cancelled,
                    "fixture root input is closed",
                ));
            }
            let mut accepted = self.input.lock().unwrap();
            assert!(accepted.is_none());
            // All validation and locking precede this pure ownership transfer.
            *accepted = offered.take();
            Ok(())
        }
        fn finish_input(&self) -> Result<(), FragmentIoError> {
            self.finishing.store(true, Ordering::Release);
            Ok(())
        }
        fn producer_state(&self) -> crate::runtime::fragment::io::RootProducerState {
            use crate::runtime::fragment::io::RootProducerState;
            if self.exited.load(Ordering::Acquire) {
                RootProducerState::ContextHeld
            } else if self.finishing.load(Ordering::Acquire) {
                RootProducerState::Finishing
            } else {
                RootProducerState::Accepting
            }
        }
        fn producer_exit_observable(&self) -> Arc<crate::runtime::observable::Observable> {
            self.exit_observable.clone()
        }
        fn poll_producer_exit(
            &self,
            waker: &std::task::Waker,
        ) -> crate::runtime::fragment::io::RootWorkExitProgress {
            use crate::runtime::fragment::io::RootWorkExitProgress;
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
            } else if self.exited.load(Ordering::Acquire) && issuer == RootWorkExitProgress::Exited
            {
                RootWorkExitProgress::Exited
            } else {
                RootWorkExitProgress::Pending
            }
        }
        fn abort(&self, reason: ResultAbort) {
            let mut aborts = self.aborts.lock().unwrap();
            if aborts.is_empty() {
                aborts.push(reason);
            }
            drop(aborts);
            self.authority.close();
        }
    }
    #[test]
    fn bounded_root_requires_exact_session_without_opening_legacy_result() {
        let root = BoundedRootSession::new();
        let mut missing = root.context();
        missing.root_result_session = None;
        assert!(prepare_compiled_fragment(root.submission(), missing).is_err());
        assert!(root.aborts.lock().unwrap().is_empty());
        let wrong = BoundedRootSession::new();
        assert!(
            prepare_compiled_fragment(
                root.submission(),
                wrong.context().with_result_identity(root.spec.task)
            )
            .is_err()
        );
        assert_eq!(
            wrong.aborts.lock().unwrap().as_slice(),
            &[ResultAbort::PrepareRollback]
        );
    }
    #[test]
    fn bounded_root_prepare_rollback_aborts_the_exact_provisional_owner() {
        let root = BoundedRootSession::new();
        assert!(
            prepare_compiled_fragment(
                root.submission(),
                root.context()
                    .with_prepare_failure(super::PrepareFailurePoint::AfterResult)
            )
            .is_err()
        );
        assert_eq!(
            root.aborts.lock().unwrap().as_slice(),
            &[ResultAbort::PrepareRollback]
        );
        let root = BoundedRootSession::new();
        let dormant = prepare_compiled_fragment(root.submission(), root.context()).unwrap();
        drop(dormant);
        assert_eq!(
            root.aborts.lock().unwrap().as_slice(),
            &[ResultAbort::NeverStarted]
        );
    }
    #[test]
    fn bounded_root_success_waits_actual_producer_exit_then_preserves_context_ownership() {
        let root = BoundedRootSession::new();
        let running = prepare_compiled_fragment(root.submission(), root.context())
            .unwrap()
            .start();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !root.finishing.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(root.finishing.load(Ordering::Acquire));
        assert!(running.stopped_fact().is_none());
        assert!(root.input.lock().unwrap().is_some());
        root.actual_exit();
        let fact = running.join();
        assert!(matches!(fact.outcome(), FragmentOutcome::Succeeded));
        drop(running);
        assert!(
            root.aborts.lock().unwrap().is_empty(),
            "success and task-handle drop must not close the context-held root"
        );
    }

    #[test]
    fn bounded_root_cancellation_retains_host_input_until_actual_exit() {
        let root = BoundedRootSession::new();
        let task = crate::runtime::task_control::test_support::owner_for(root.spec.task);
        let control =
            Arc::new(task.view(novarocks_execution_contract::TaskFailurePhase::Execution));
        let running =
            prepare_compiled_fragment(root.submission(), root.context_with_control(control))
                .unwrap()
                .start();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !root.finishing.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(root.finishing.load(Ordering::Acquire));
        novarocks_execution_contract::TaskConclusionPort::submit(
            task.as_ref(),
            novarocks_execution_contract::TaskConclusionProposal::Canceled(
                novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded,
            ),
        );
        running.cancel(FragmentCancelReason::new("root cancellation probe"));
        assert!(running.stopped_fact().is_none());
        assert!(
            root.input.lock().unwrap().is_some(),
            "the admitted host input remains physically held during cancellation"
        );
        root.actual_exit();
        assert!(matches!(
            running.join().outcome(),
            FragmentOutcome::Cancelled { .. }
        ));
        assert_eq!(
            root.aborts.lock().unwrap().len(),
            1,
            "abort is a single logical decision"
        );
    }

    #[derive(Default)]
    struct ResultAbortGate {
        entered: AtomicBool,
        released: Mutex<bool>,
        cv: Condvar,
    }
    impl ResultAbortGate {
        fn wait(&self) {
            self.entered.store(true, Ordering::Release);
            let guard = self.released.lock().unwrap();
            drop(self.cv.wait_while(guard, |released| !*released).unwrap());
        }
        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.cv.notify_all();
        }
    }
    struct ResultAbortInterrupt {
        gate: Arc<ResultAbortGate>,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl ResultAbortInterrupt {
        fn join(&mut self) {
            self.gate.release();
            self.thread.take().unwrap().join().unwrap();
        }
    }
    impl Drop for ResultAbortInterrupt {
        fn drop(&mut self) {
            self.gate.release();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    struct BlockingResultSession {
        write_failure: Option<super::super::ExecutionFailure>,
        write_panic: bool,
        abort_gate: Mutex<Option<Arc<ResultAbortGate>>>,
        entered: AtomicBool,
        released: Mutex<bool>,
        released_cv: Condvar,
        aborted: AtomicBool,
    }

    impl BlockingResultSession {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                write_failure: None,
                write_panic: false,
                abort_gate: Mutex::new(None),
                entered: AtomicBool::new(false),
                released: Mutex::new(false),
                released_cv: Condvar::new(),
                aborted: AtomicBool::new(false),
            })
        }

        fn release(&self) {
            *self.released.lock().expect("result release lock") = true;
            self.released_cv.notify_all();
        }
    }

    impl FragmentResultSession for BlockingResultSession {
        fn reservation_bytes(
            &self,
            input: crate::runtime::fragment::io::ResultInputMetadata,
        ) -> Result<usize, FragmentIoError> {
            Ok(input.retained_bytes())
        }

        fn try_acquire(
            &self,
            bytes: usize,
        ) -> Result<crate::runtime::fragment::io::ResultWriteAdmission, FragmentIoError> {
            Ok(crate::runtime::fragment::io::ResultWriteAdmission::Granted(
                crate::runtime::fragment::io::ResultWriteCredit::new(bytes, |_| {}),
            ))
        }

        fn writable_observable(&self) -> Option<Arc<crate::runtime::observable::Observable>> {
            None
        }

        fn write_with_credit(
            &self,
            chunk: Chunk,
            credit: crate::runtime::fragment::io::ResultWriteCredit,
            _factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
            _control: &dyn novarocks_functions::KernelEvaluationControl,
        ) -> Result<(), crate::runtime::fragment::io::ResultWriteFailure> {
            debug_assert_eq!(credit.bytes(), chunk.logical_bytes());
            self.entered.store(true, Ordering::Release);
            let mut released = self.released.lock().expect("result release lock");
            while !*released && !self.aborted.load(Ordering::Acquire) {
                released = self
                    .released_cv
                    .wait(released)
                    .expect("result release wait");
            }
            drop(released);
            assert!(!self.write_panic, "deferred fragment result write panic");
            if let Some(failure) = &self.write_failure {
                return Err(failure.clone().into());
            }
            Ok(())
        }

        fn finish(&self) -> Result<(), FragmentIoError> {
            Ok(())
        }

        fn abort(&self, _reason: ResultAbort) {
            let gate = self.abort_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.wait();
            }
            self.aborted.store(true, Ordering::Release);
            self.released_cv.notify_all();
        }
    }

    struct BlockingResultWriter {
        session: Arc<BlockingResultSession>,
    }

    impl FragmentResultWriter for BlockingResultWriter {
        fn open(
            &self,
            _spec: ResultWriteSpec,
        ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
            Ok(self.session.clone())
        }
    }

    fn deferred_fragment_owner(
        id: UniqueId,
    ) -> Arc<crate::runtime::task_control::test_support::DeferredOwner> {
        use novarocks_execution_contract::TaskIdentity;
        use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, StageId, TaskId};
        crate::runtime::task_control::test_support::deferred_owner_for(TaskIdentity::new(
            QueryExecutionId::new(
                QueryId::new(id.high() - 2, id.low() - 2),
                AttemptId::new(1).unwrap(),
            )
            .unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        ))
    }
    fn deferred_fragment_with_receiver(
        id: UniqueId,
        owner: &Arc<crate::runtime::task_control::test_support::DeferredOwner>,
        session: Option<Arc<BlockingResultSession>>,
    ) -> (
        super::DormantFragmentHandle,
        Arc<ReceiverConstructionPort>,
        ExchangeReceiverKey,
    ) {
        use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
        let runtime = crate::runtime::execution_runtime::test_execution_runtime();
        let port = ReceiverConstructionPort::new(runtime.clone());
        let control = Arc::new(owner.view(TaskFailurePhase::Execution));
        let mut context = FragmentPrepareContext::default()
            .with_task_control(control)
            .with_result_identity(owner.identity())
            .with_execution_runtime(runtime)
            .with_exchange_receiver_port(port.clone());
        let input = if let Some(session) = session {
            context.result_writer = Arc::new(BlockingResultWriter { session });
            submission(
                id,
                one_row_chunk(),
                novarocks_physical_plan::FragmentSink::Result,
            )
        } else {
            empty_result_submission(id)
        };
        let mut dormant = prepare_compiled_fragment(input, context).unwrap();
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("deferred retirement fixture");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker, Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let declaration =
            crate::exec::chunk::typed_fixture::layout(Arc::new(Schema::empty()), &[], &[]);
        let schema = factory.create_compiled_chunk_schema(&declaration).unwrap();
        let key = ExchangeReceiverKey {
            fragment_instance_id: id,
            node_id: 73,
        };
        dormant
            .resources
            .acquire_compiled_exchange(
                vec![ExchangeReceiverRegistration {
                    key,
                    expected_senders: 1,
                    expected_chunk_schema: schema,
                    column_binding: crate::runtime::exchange::ExchangeColumnBinding::Positional,
                    creation_factory: factory,
                }],
                None,
            )
            .unwrap();
        (dormant, port, key)
    }
    fn wait_for_fragment_probe(mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !ready() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(ready(), "the actual driver reaches the controlled point");
    }
    #[test]
    fn actual_strict_input_failure_waits_for_deferred_task_settlement() {
        use novarocks_execution_contract::TaskConclusionPort;
        for consumed in [false, true] {
            let id = UniqueId::new(84100 + i64::from(consumed), 1);
            let owner = deferred_fragment_owner(id);
            let (dormant, port, key) = deferred_fragment_with_receiver(id, &owner, None);
            let source = UniqueId::new(id.high(), 99);
            let stream =
                crate::runtime::fragment::io::exchange::fixture_stream(source, id, key.node_id, 0);
            port.push(
                key,
                crate::runtime::fragment::io::ExchangeReceiverFrame {
                    stream,
                    source_fragment_instance_id: source,
                    sender_ordinal: 0,
                    sender_count: 1,
                    sender_id: 0,
                    backend_number: 0,
                    sequence: 0,
                    eos: true,
                    payload: Vec::new(),
                },
            )
            .unwrap();
            if consumed {
                port.receiver_handle(key, 1)
                    .unwrap()
                    .complete_input_consumption(1)
                    .unwrap();
            }
            let original: super::super::ExecutionFailure =
                novarocks_functions::KernelFailure::InvalidProgram(
                    novarocks_functions::KernelDiagnostic::new("required input remains unread"),
                )
                .into();
            let failure = original.clone();
            let receiver = port.clone();
            let dormant = dormant.with_success_validator(move || {
                let closes = receiver
                    .stream_close_facts(key, &[stream])
                    .map_err(|cause| cause.into_execution())?;
                if closes.iter().all(|close| {
                    matches!(
                        close.disposition,
                        novarocks_execution_contract::ExchangeCloseDisposition::Consumed { .. }
                    )
                }) {
                    Ok(())
                } else {
                    Err(failure)
                }
            });
            let running = dormant.start();
            wait_for_fragment_probe(|| owner.pending_settlements() > 0);
            assert!(
                owner.accepted().is_none(),
                "handoff does not imply an accepted conclusion"
            );
            assert!(running.stopped_fact().is_none());
            assert!(port.snapshot(key).is_some());
            assert_eq!(port.removed.load(Ordering::SeqCst), 0);
            owner.advance_all();
            let terminal = running.stopped_fact().unwrap();
            if consumed {
                assert!(matches!(terminal.outcome(), FragmentOutcome::Succeeded));
                assert_eq!(port.removed.load(Ordering::SeqCst), 1);
            } else {
                let FragmentOutcome::Failed(error) = terminal.outcome() else {
                    panic!("strict input failure wins after settlement");
                };
                assert!(error.cause().same_occurrence(&original));
                assert_eq!(port.cancelled.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[test]
    fn preaccepted_cancel_does_not_run_success_validator() {
        use novarocks_execution_contract::{
            CancelReason, TaskConclusionPort, TaskConclusionProposal,
        };
        let id = UniqueId::new(84102, 1);
        let owner = deferred_fragment_owner(id);
        let (dormant, port, _) = deferred_fragment_with_receiver(id, &owner, None);
        let invoked = Arc::new(AtomicUsize::new(0));
        let seen = invoked.clone();
        let dormant = dormant.with_success_validator(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            Err(novarocks_functions::KernelFailure::ResourceExhausted.into())
        });
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        owner.advance_all();
        let running = dormant.start();
        wait_for_fragment_probe(|| owner.pending_settlements() > 0);
        owner.advance_all();
        assert!(matches!(
            running.stopped_fact().unwrap().outcome(),
            FragmentOutcome::Cancelled { .. }
        ));
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert_eq!(port.cancelled.load(Ordering::SeqCst), 1);
    }

    // The actual result callback fails on the driver thread. The same owned
    // source is handed off before physical exit; retirement must wait for it.
    #[test]
    fn deferred_settlement_retires_as_failed_after_a_pre_exit_proposal() {
        use novarocks_execution_contract::TaskConclusionPort;
        let id = UniqueId::new(84031, 1);
        let owner = deferred_fragment_owner(id);
        let original: super::super::ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        let mut session = BlockingResultSession::new();
        Arc::get_mut(&mut session).unwrap().write_failure = Some(original.clone());
        let (dormant, port, key) =
            deferred_fragment_with_receiver(id, &owner, Some(session.clone()));
        let validator_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = validator_calls.clone();
        let dormant = dormant.with_success_validator(move || {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Err(novarocks_functions::KernelFailure::InstanceFailed.into())
        });
        let running = dormant.start();
        wait_for_fragment_probe(|| session.entered.load(Ordering::Acquire));
        session.release();
        wait_for_fragment_probe(|| {
            running.inner.pipeline.stopped_fact().is_some() && owner.pending_settlements() > 0
        });
        assert!(owner.accepted().is_none());
        assert!(
            running.stopped_fact().is_none(),
            "physical exit does not bypass settlement"
        );
        assert!(port.snapshot(key).is_some());
        assert_eq!(port.cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(port.removed.load(Ordering::SeqCst), 0);
        owner.advance_all();
        let fact = running.stopped_fact().unwrap();
        let FragmentOutcome::Failed(failure) = fact.outcome() else {
            panic!("the handed-off driver failure wins")
        };
        assert!(failure.cause().same_occurrence(&original));
        assert_eq!(
            validator_calls.load(Ordering::SeqCst),
            1,
            "an unsettled original proposal is not an accepted stop; validation cannot replace its cause"
        );
        assert!(port.snapshot(key).is_none());
        assert_eq!(port.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(port.removed.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn deferred_settlement_observes_the_refined_placeholder() {
        use novarocks_execution_contract::{
            AbortCause, TaskConclusionPort, TaskConclusionProposal,
        };
        let id = UniqueId::new(84032, 1);
        let owner = deferred_fragment_owner(id);
        let original: super::super::ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        let mut session = BlockingResultSession::new();
        Arc::get_mut(&mut session).unwrap().write_failure = Some(original.clone());
        let (dormant, port, _) = deferred_fragment_with_receiver(id, &owner, Some(session.clone()));
        let running = dormant.start();
        wait_for_fragment_probe(|| session.entered.load(Ordering::Acquire));
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        owner.advance_all();
        assert_eq!(owner.accepted().unwrap().neutral().generation().get(), 1);
        session.release();
        wait_for_fragment_probe(|| {
            running.inner.pipeline.stopped_fact().is_some() && owner.pending_settlements() > 0
        });
        assert!(running.stopped_fact().is_none());
        let settled = Arc::new(Mutex::new(None));
        let seen = settled.clone();
        owner.on_settled(Box::new(move |snapshot| *seen.lock().unwrap() = snapshot));
        owner.advance_all();
        let snapshot = settled.lock().unwrap().take().unwrap();
        assert_eq!(snapshot.neutral().generation().get(), 2);
        assert!(
            snapshot
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        let fact = running.stopped_fact().unwrap();
        let FragmentOutcome::Failed(failure) = fact.outcome() else {
            panic!("refined failure decides retirement")
        };
        assert!(failure.cause().same_occurrence(&original));
        assert_eq!(port.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(port.removed.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn deferred_settlement_without_proposal_retires_as_success() {
        use novarocks_execution_contract::TaskConclusionPort;
        let id = UniqueId::new(84033, 1);
        let owner = deferred_fragment_owner(id);
        let (dormant, port, key) = deferred_fragment_with_receiver(id, &owner, None);
        let running = dormant.start();
        wait_for_fragment_probe(|| {
            running.inner.pipeline.stopped_fact().is_some() && owner.pending_settlements() > 0
        });
        assert!(running.stopped_fact().is_none());
        assert!(port.snapshot(key).is_some());
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        owner.on_settled(Box::new(move |snapshot| {
            assert!(snapshot.is_none());
            seen.fetch_add(1, Ordering::SeqCst);
        }));
        owner.advance_all();
        assert!(matches!(
            running.stopped_fact().unwrap().outcome(),
            FragmentOutcome::Succeeded
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(owner.accepted().is_none());
        assert!(port.snapshot(key).is_none());
        assert_eq!(port.removed.load(Ordering::SeqCst), 1);
        assert_eq!(port.cancelled.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn cancellation_accepted_after_settlement_still_retires_as_cancelled() {
        use novarocks_execution_contract::{
            CancelReason, TaskConclusionPort, TaskConclusionProposal,
        };
        let id = UniqueId::new(84034, 1);
        let owner = deferred_fragment_owner(id);
        let session = BlockingResultSession::new();
        session.release();
        let gate = Arc::new(ResultAbortGate::default());
        let (dormant, port, key) =
            deferred_fragment_with_receiver(id, &owner, Some(session.clone()));
        let dormant = dormant.with_success_validator(|| Ok(()));
        let running = dormant.start();
        wait_for_fragment_probe(|| {
            running.inner.pipeline.stopped_fact().is_some() && owner.pending_settlements() > 0
        });
        // Install the blocking abort only after preparation and physical exit.
        // This guard drops before running, releases the gate and joins the owner
        // even when an assertion fails inside the controlled interleaving.
        let mut interrupt = ResultAbortInterrupt {
            gate: gate.clone(),
            thread: None,
        };
        *session.abort_gate.lock().unwrap() = Some(gate.clone());
        let lifecycle = running.inner.lifecycle.clone();
        interrupt.thread = Some(std::thread::spawn(move || {
            lifecycle.interrupt("controlled cancellation retirement")
        }));
        wait_for_fragment_probe(|| gate.entered.load(Ordering::Acquire));
        assert!(running.inner.lifecycle.state.lock().unwrap().advancing);
        owner.advance_all();
        assert!(
            running
                .inner
                .lifecycle
                .state
                .lock()
                .unwrap()
                .pipeline_result
                .is_some(),
            "the barrier ran while resources remain loaned"
        );
        assert!(running.stopped_fact().is_none());
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        owner.advance_all();
        interrupt.join();
        let fact = running.stopped_fact().unwrap();
        assert!(matches!(fact.outcome(), FragmentOutcome::Cancelled { .. }));
        assert_eq!(
            port.cancelled.load(Ordering::SeqCst),
            1,
            "retirement reads the later accepted cancellation"
        );
        assert_eq!(port.removed.load(Ordering::SeqCst), 0);
        assert!(port.snapshot(key).is_none());
    }
    // A real result callback panic must await settlement before fragment retirement.
    #[test]
    fn deferred_driver_write_panic_is_failed_only_after_fragment_settlement() {
        use novarocks_execution_contract::TaskConclusionPort;
        let id = UniqueId::new(84035, 1);
        let owner = deferred_fragment_owner(id);
        let mut session = BlockingResultSession::new();
        Arc::get_mut(&mut session).unwrap().write_panic = true;
        let (dormant, port, key) =
            deferred_fragment_with_receiver(id, &owner, Some(session.clone()));
        let running = dormant.start();
        wait_for_fragment_probe(|| session.entered.load(Ordering::Acquire));
        session.release();
        wait_for_fragment_probe(|| {
            running.inner.pipeline.stopped_fact().is_some() && owner.pending_settlements() > 0
        });
        assert!(owner.accepted().is_none());
        assert!(running.stopped_fact().is_none());
        assert!(port.snapshot(key).is_some());
        assert_eq!(port.cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(port.removed.load(Ordering::SeqCst), 0);
        assert!(owner.advance_one());
        let accepted = owner.accepted().unwrap();
        let original = accepted.originating_failure().unwrap().clone();
        assert!(
            original
                .detail()
                .contains("panic in driver execution: deferred fragment result write panic")
        );
        assert!(
            running.stopped_fact().is_none(),
            "acceptance alone does not run the later settlement barrier"
        );
        owner.advance_all();
        let fact = running
            .stopped_fact()
            .expect("settled fragment retires after actual driver exit");
        let FragmentOutcome::Failed(error) = fact.outcome() else {
            panic!("a real driver panic cannot retire as success")
        };
        assert!(error.cause().same_occurrence(&original));
        assert!(Arc::ptr_eq(&accepted, &owner.accepted().unwrap()));
        assert_eq!(owner.accepted().unwrap().neutral().generation().get(), 1);
        assert!(port.snapshot(key).is_none());
        assert_eq!(port.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(port.removed.load(Ordering::SeqCst), 0);
        assert!(
            session.aborted.load(Ordering::Acquire),
            "the result owner was actually aborted"
        );
    }
    fn start_blocked_result_fragment(
        finst_id: UniqueId,
        session: &Arc<BlockingResultSession>,
    ) -> RunningFragmentHandle {
        let context = FragmentPrepareContext {
            result_writer: Arc::new(BlockingResultWriter {
                session: Arc::clone(session),
            }),
            ..FragmentPrepareContext::default()
        };
        prepare_compiled_fragment(
            submission(
                finst_id,
                one_row_chunk(),
                novarocks_physical_plan::FragmentSink::Result,
            ),
            context,
        )
        .expect("result fragment prepares")
        .start()
    }

    /// Reuse the original registration and real receiver port while keeping
    /// the pipeline itself minimal: these tests isolate its cleanup owner.
    fn empty_result_with_receiver(
        id: UniqueId,
    ) -> (
        super::DormantFragmentHandle,
        Arc<dyn crate::runtime::fragment::io::ExchangeReceiverPort>,
        crate::runtime::fragment::io::ExchangeReceiverKey,
    ) {
        let context = FragmentPrepareContext::default();
        let port = Arc::clone(&context.exchange_receiver_port);
        let mut dormant = prepare_compiled_fragment(empty_result_submission(id), context).unwrap();
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("receiver retirement fixture");
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let declaration =
            crate::exec::chunk::typed_fixture::layout(Arc::new(Schema::empty()), &[], &[]);
        let schema = factory.create_compiled_chunk_schema(&declaration).unwrap();
        let key = crate::runtime::fragment::io::ExchangeReceiverKey {
            fragment_instance_id: id,
            node_id: 71,
        };
        dormant
            .resources
            .acquire_compiled_exchange(
                vec![crate::runtime::fragment::io::ExchangeReceiverRegistration {
                    key,
                    expected_senders: 1,
                    expected_chunk_schema: schema,
                    column_binding: crate::runtime::exchange::ExchangeColumnBinding::Positional,
                    creation_factory: factory,
                }],
                None,
            )
            .unwrap();
        (dormant, port, key)
    }

    #[test]
    fn direct_execution_keeps_original_receiver_terminal_cleanup_without_transfer() {
        for (ordinal, failed) in [false, true].into_iter().enumerate() {
            let (dormant, port, key) =
                empty_result_with_receiver(UniqueId::new(84_100, ordinal as i64));
            assert!(port.snapshot(key).is_some());
            let running = if failed {
                dormant.start_failed(novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new("original execution failure"),
                ))
            } else {
                dormant.start()
            };
            let fact = running.join();
            assert_eq!(matches!(fact.outcome(), FragmentOutcome::Failed(_)), failed);
            assert!(port.snapshot(key).is_none());
        }
    }

    #[test]
    fn plan_tree_receiver_prepare_rollback_preserves_primary_and_cleanup_diagnostics() {
        use crate::exec::fragment::program::FragmentNodeId;
        use crate::runtime::fragment::instance::ExchangeInputAssignment;
        use crate::runtime::fragment::resources::ResourceKind;

        let id = UniqueId::new(84_500, 1);
        // The existing fixture publishes a real distributed SQL gather package
        // and compiles its ExchangeSource through the production local compiler.
        // Its exchange wait remains the original one-second bound.
        let program = super::compiled_prepare::tests::sql_gather_receiver_program();
        assert_eq!(program.exchange_inputs().len(), 1);
        let (&compiled_node, input) = program.exchange_inputs().iter().next().unwrap();
        assert!(matches!(
            program.graph().nodes()[compiled_node.index()].kind(),
            novarocks_local_program::ProgramNodeKind::ExchangeSource { .. }
        ));
        let node = FragmentNodeId::new(i32::try_from(input.receiver_node).unwrap());
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId::new(84_498, 1),
            FragmentInstanceId::new(id),
            ScanAssignments::default(),
            ExchangeInputAssignments::new(BTreeMap::from([(
                node,
                ExchangeInputAssignment::new(NonZeroUsize::MIN),
            )])),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::MIN,
            BackendNum::try_new(1).unwrap(),
        );
        let submission = CompiledFragmentSubmission::try_new(
            program,
            super::super::scan::CompiledScanSources::new(),
            instance,
        )
        .unwrap();
        let runtime = crate::runtime::execution_runtime::test_execution_runtime();
        let port = ReceiverConstructionPort::new(runtime.clone());
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("receiver rollback");
        tracker.install_limit_once(1024 * 1024).unwrap();
        let mut context = FragmentPrepareContext::default()
            .with_execution_runtime(runtime)
            .with_exchange_receiver_port(port.clone())
            .with_registration_owned_exchange_receivers()
            .with_prepare_failure(super::PrepareFailurePoint::AfterExchange)
            .with_cleanup_failure(ResourceKind::Exchange);
        context.mem_tracker = Some(tracker.clone());
        let error = match prepare_compiled_fragment(submission, context) {
            Err(error) => error,
            Ok(_) => panic!("the original AfterExchange failure must refuse preparation"),
        };
        assert_eq!(error.stage(), super::FragmentLaunchStage::Register);
        assert_eq!(
            error.kind(),
            super::FragmentLaunchErrorKind::ResourceUnavailable
        );
        assert_eq!(
            error.detail(),
            super::PrepareFailurePoint::AfterExchange.detail()
        );
        let primary = novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new(
                super::PrepareFailurePoint::AfterExchange.detail(),
            ),
        );
        assert_eq!(
            error.cause().cause(),
            &crate::runtime::fragment::ExecutionFailureCause::Kernel(primary.clone()),
        );
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<novarocks_functions::KernelFailure>(),
            Some(&primary),
        );
        assert_eq!(
            error
                .cleanup_diagnostics()
                .iter()
                .map(|cause| cause.detail())
                .collect::<Vec<_>>(),
            &["injected exchange registration cleanup failure"]
        );
        let loans = port.registrations.lock().unwrap();
        assert_eq!(
            loans.len(),
            1,
            "the actual receiver was installed before failure"
        );
        let loan = &loans[0];
        assert_eq!(
            loan.key,
            ExchangeReceiverKey {
                fragment_instance_id: id,
                node_id: node.get()
            }
        );
        assert!(port.snapshot(loan.key).is_none());
        assert_eq!(
            loan.creation_factory.observe_input_work(1).unwrap_err(),
            crate::exec::pipeline::operator::ConstructionFailure::Caller(Box::new(
                novarocks_functions::KernelFailure::Cancelled.into(),
            )),
        );
        assert!(
            tracker.current() > 0,
            "the independent factory loan is still live"
        );
        drop(loans);
        port.registrations.lock().unwrap().clear();
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn transferred_receiver_survives_raw_terminal_until_explicit_retirement() {
        for (ordinal, failed) in [false, true].into_iter().enumerate() {
            let (mut dormant, port, key) =
                empty_result_with_receiver(UniqueId::new(84_101, ordinal as i64));
            let mut retirement = dormant
                .take_exchange_retirement()
                .expect("unique original receiver owner");
            assert!(dormant.take_exchange_retirement().is_none());
            let running = if failed {
                dormant.start_failed(novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new("original execution failure"),
                ))
            } else {
                dormant.start()
            };
            let fact = running.join();
            assert_eq!(matches!(fact.outcome(), FragmentOutcome::Failed(_)), failed);
            assert!(port.snapshot(key).is_some());
            retirement.record_terminal(&fact);
            assert!(
                port.snapshot(key).is_some(),
                "recording a fact cannot close physical ingress"
            );
            retirement.retire();
            assert!(port.snapshot(key).is_none());
        }
    }

    #[test]
    fn transferred_dormant_receiver_rolls_back_once_without_a_terminal_fact() {
        let (mut dormant, port, key) = empty_result_with_receiver(UniqueId::new(84_102, 1));
        let retirement = dormant.take_exchange_retirement().unwrap();
        drop(dormant);
        assert!(
            port.snapshot(key).is_some(),
            "dormant rollback no longer owns transferred ingress"
        );
        drop(retirement);
        assert!(port.snapshot(key).is_none());
    }

    #[test]
    fn stopped_preparation_drain_preserves_refused_owner_and_original_failure() {
        use novarocks_failure_contract::FailureProjection;
        let dormant = prepare_compiled_fragment(
            empty_result_submission(UniqueId::new(891, 892)),
            FragmentPrepareContext::default(),
        )
        .unwrap();
        let control = dormant.prepared.failure_control();
        let mut slot = Some(dormant);
        let original_address = slot.as_ref().unwrap() as *const super::DormantFragmentHandle;
        let refused = match super::DormantFragmentHandle::try_drain_stopped(&mut slot) {
            Err(error) => error,
            Ok(_) => panic!("healthy prepared work cannot enter stopped drain"),
        };
        assert_eq!(
            refused.failure_fact(),
            super::super::ExecutionFailure::from_domain(
                super::PreparedDrainRefusal::BeforeConclusion
            )
            .failure_fact()
        );
        assert_eq!(slot.as_ref().unwrap() as *const _, original_address);
        assert_eq!(slot.as_ref().unwrap().submitted_driver_count(), 0);
        let original: super::super::ExecutionFailure =
            novarocks_functions::KernelFailure::DeadlineExceeded.into();
        control.set_error(original.clone());
        let running = super::DormantFragmentHandle::try_drain_stopped(&mut slot).unwrap();
        assert!(slot.is_none());
        let fact = running.join();
        let FragmentOutcome::Failed(failure) = fact.outcome() else {
            panic!("the original stop remains the execution conclusion")
        };
        assert!(failure.cause().same_occurrence(&original));
        let missing = match super::DormantFragmentHandle::try_drain_stopped(&mut slot) {
            Err(error) => error,
            Ok(_) => panic!("the original owner transfers only once"),
        };
        assert_eq!(
            missing.failure_fact(),
            super::super::ExecutionFailure::from_domain(super::PreparedDrainRefusal::MissingOwner)
                .failure_fact()
        );
    }

    #[test]
    fn execution_owner_prepares_starts_and_freezes_an_empty_result_fragment() {
        let handle = prepare_compiled_fragment(
            empty_result_submission(UniqueId::new(91, 92)),
            FragmentPrepareContext::default(),
        )
        .expect("fragment prepares");
        assert!(matches!(
            handle.start().join().outcome(),
            FragmentOutcome::Succeeded
        ));
    }

    #[test]
    fn dropping_running_handle_cancels_a_blocked_result_write_without_waiting_for_the_client() {
        let session = BlockingResultSession::new();
        let running = start_blocked_result_fragment(UniqueId::new(93, 94), &session);

        let entered_deadline = Instant::now() + Duration::from_secs(1);
        while !session.entered.load(Ordering::Acquire) && Instant::now() < entered_deadline {
            std::thread::yield_now();
        }
        assert!(
            session.entered.load(Ordering::Acquire),
            "test sink must hold the driver inside an unfinished result write"
        );

        let (stopped_tx, stopped_rx) = mpsc::sync_channel(1);
        running.bind_test_publisher(move |fact| {
            stopped_tx
                .send(fact.clone())
                .expect("stopped fact receiver remains available");
        });
        let (drop_tx, drop_rx) = mpsc::sync_channel(1);
        let drop_join = std::thread::spawn(move || {
            drop(running);
            drop_tx
                .send(())
                .expect("drop completion receiver remains available");
        });

        let drop_returned = drop_rx.recv_timeout(Duration::from_millis(50)).is_ok();
        drop_join.join().expect("drop thread must not panic");
        assert!(
            drop_returned,
            "dropping a running handle must not join its driver"
        );

        let stopped = stopped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("result-session cancellation must unblock the driver");
        assert!(matches!(
            stopped.outcome(),
            FragmentOutcome::Cancelled { .. }
        ));
        assert!(
            session.aborted.load(Ordering::Acquire),
            "cancellation must abort the result session before actual driver stop"
        );
    }

    #[test]
    fn explicit_cancellation_unblocks_a_root_result_write_without_client_release() {
        let session = BlockingResultSession::new();
        let running = start_blocked_result_fragment(UniqueId::new(97, 98), &session);

        let entered_deadline = Instant::now() + Duration::from_secs(1);
        while !session.entered.load(Ordering::Acquire) && Instant::now() < entered_deadline {
            std::thread::yield_now();
        }
        assert!(
            session.entered.load(Ordering::Acquire),
            "test sink must hold the driver inside an unfinished result write"
        );

        let (stopped_tx, stopped_rx) = mpsc::sync_channel(1);
        running.bind_test_publisher(move |fact| {
            stopped_tx
                .send(fact.clone())
                .expect("stopped fact receiver remains available");
        });
        running.cancel(FragmentCancelReason::new("test cancellation"));

        let stopped = stopped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("explicit cancellation must unblock the root driver");
        assert!(matches!(
            stopped.outcome(),
            FragmentOutcome::Cancelled { .. }
        ));
        assert!(
            session.aborted.load(Ordering::Acquire),
            "explicit cancellation must abort the result session"
        );
    }

    #[test]
    fn stopped_observer_panic_before_terminal_freeze_is_isolated() {
        let session = BlockingResultSession::new();
        let context = FragmentPrepareContext {
            result_writer: Arc::new(BlockingResultWriter {
                session: Arc::clone(&session),
            }),
            ..FragmentPrepareContext::default()
        };
        let running = prepare_compiled_fragment(
            submission(
                UniqueId::new(95, 96),
                one_row_chunk(),
                novarocks_physical_plan::FragmentSink::Result,
            ),
            context,
        )
        .expect("result fragment prepares")
        .start();

        let entered_deadline = Instant::now() + Duration::from_secs(1);
        while !session.entered.load(Ordering::Acquire) && Instant::now() < entered_deadline {
            std::thread::yield_now();
        }
        assert!(
            session.entered.load(Ordering::Acquire),
            "test sink must hold the driver before terminal freeze"
        );

        let first_owner = prepare_compiled_fragment(
            empty_result_submission(UniqueId::new(98_601, 1)),
            FragmentPrepareContext::default(),
        )
        .unwrap()
        .start();
        let last_owner = prepare_compiled_fragment(
            empty_result_submission(UniqueId::new(98_601, 2)),
            FragmentPrepareContext::default(),
        )
        .unwrap()
        .start();
        let first = Arc::new(AtomicUsize::new(0));
        let first_callback = Arc::clone(&first);
        first_owner.bind_test_publisher(move |_| {
            first_callback.fetch_add(1, Ordering::SeqCst);
        });
        running.bind_test_publisher(|_| {
            panic!("injected fragment terminal observer panic");
        });
        let last = Arc::new(AtomicUsize::new(0));
        let last_callback = Arc::clone(&last);
        let (last_tx, last_rx) = mpsc::sync_channel(1);
        last_owner.bind_test_publisher(move |_| {
            last_callback.fetch_add(1, Ordering::SeqCst);
            last_tx
                .send(())
                .expect("last observer receiver remains available");
        });

        session.release();
        let terminal = running.join();
        last_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("observer delivery completes after terminal freeze");
        assert!(matches!(terminal.outcome(), FragmentOutcome::Succeeded));
        assert_eq!(running.stopped_fact(), Some(terminal));
        first_owner.join();
        last_owner.join();
        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(last.load(Ordering::SeqCst), 1);
    }

    struct OpaqueCompletionPanic(Arc<AtomicUsize>);
    impl Drop for OpaqueCompletionPanic {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("opaque payload must remain in original custody");
        }
    }
    struct UnknownResultHolder {
        drops: Arc<AtomicUsize>,
        payload_drops: Arc<AtomicUsize>,
        _charge: std::mem::ManuallyDrop<crate::runtime::mem_tracker::TrackedBytes>,
    }
    impl Drop for UnknownResultHolder {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(OpaqueCompletionPanic(self.payload_drops.clone()));
        }
    }
    impl FragmentResultSession for UnknownResultHolder {
        fn reservation_bytes(
            &self,
            input: crate::runtime::fragment::io::ResultInputMetadata,
        ) -> Result<usize, FragmentIoError> {
            Ok(input.retained_bytes())
        }
        fn try_acquire(
            &self,
            _: usize,
        ) -> Result<crate::runtime::fragment::io::ResultWriteAdmission, FragmentIoError> {
            unreachable!("registration-only result fixture")
        }
        fn writable_observable(&self) -> Option<Arc<crate::runtime::observable::Observable>> {
            None
        }
        fn write_with_credit(
            &self,
            _: Chunk,
            _: crate::runtime::fragment::io::ResultWriteCredit,
            _: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
            _: &dyn novarocks_functions::KernelEvaluationControl,
        ) -> Result<(), crate::runtime::fragment::io::ResultWriteFailure> {
            unreachable!("registration-only result fixture")
        }
        fn finish(&self) -> Result<(), FragmentIoError> {
            Ok(())
        }
        fn abort(&self, _: ResultAbort) {}
    }
    struct SingleResultHolderWriter(Mutex<Option<Arc<dyn FragmentResultSession>>>);
    impl FragmentResultWriter for SingleResultHolderWriter {
        fn open(
            &self,
            _: ResultWriteSpec,
        ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .take()
                .expect("one original registration"))
        }
    }
    struct CommitHolderGate {
        registered: AtomicBool,
        rollback_count: AtomicUsize,
        drops: AtomicUsize,
        entered: AtomicBool,
        released: Mutex<bool>,
        cv: Condvar,
    }
    struct GatedCommitPort {
        gate: Arc<CommitHolderGate>,
        tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
    }
    struct GatedCommitLease {
        gate: Arc<CommitHolderGate>,
        charge: Option<crate::runtime::mem_tracker::TrackedBytes>,
    }
    impl crate::runtime::fragment::io::FragmentCommitPort for GatedCommitPort {
        fn acquire(
            &self,
            _: UniqueId,
        ) -> super::super::ExecutionResult<Box<dyn crate::runtime::fragment::io::FragmentCommitLease>>
        {
            assert!(!self.gate.registered.swap(true, Ordering::AcqRel));
            Ok(Box::new(GatedCommitLease {
                gate: self.gate.clone(),
                charge: Some(
                    crate::runtime::mem_tracker::TrackedBytes::try_new(256, self.tracker.clone())
                        .unwrap(),
                ),
            }))
        }
    }
    impl crate::runtime::fragment::io::FragmentCommitLease for GatedCommitLease {
        fn add_load_stats(
            &mut self,
            _: crate::runtime::fragment::io::FragmentSinkLoadStats,
        ) -> super::super::ExecutionResult<()> {
            Ok(())
        }
        fn add_tablet_commit_info(
            &mut self,
            _: crate::runtime::fragment::io::TabletCommitInfo,
        ) -> super::super::ExecutionResult<()> {
            Ok(())
        }
        fn add_tablet_fail_info(
            &mut self,
            _: crate::runtime::fragment::io::TabletFailInfo,
        ) -> super::super::ExecutionResult<()> {
            Ok(())
        }
        fn finish(
            &mut self,
        ) -> super::super::ExecutionResult<crate::runtime::fragment::io::FragmentCommitReport>
        {
            unreachable!("no write commit in retirement")
        }
        fn handoff(&mut self) -> super::super::ExecutionResult<()> {
            unreachable!("no write commit in retirement")
        }
        fn rollback(&mut self) -> super::super::ExecutionResult<()> {
            self.gate.rollback_count.fetch_add(1, Ordering::SeqCst);
            assert!(self.gate.registered.swap(false, Ordering::AcqRel));
            Ok(())
        }
    }
    impl Drop for GatedCommitLease {
        fn drop(&mut self) {
            self.gate.drops.fetch_add(1, Ordering::SeqCst);
            self.gate.entered.store(true, Ordering::Release);
            self.gate.cv.notify_all();
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.cv.wait(released).unwrap();
            }
            drop(released);
            drop(self.charge.take());
        }
    }

    #[test]
    fn unknown_result_holder_does_not_block_healthy_commit_holder_or_revoke_body_fact() {
        let id = UniqueId::new(98_602, 1);
        let gate = Arc::new(CommitHolderGate {
            registered: AtomicBool::new(false),
            rollback_count: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
            entered: AtomicBool::new(false),
            released: Mutex::new(false),
            cv: Condvar::new(),
        });
        let healthy_tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("healthy fragment holder");
        let unknown_tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("unresolved fragment holder");
        let result_drops = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let context = FragmentPrepareContext {
            commit_port: Arc::new(GatedCommitPort {
                gate: gate.clone(),
                tracker: healthy_tracker.clone(),
            }),
            ..FragmentPrepareContext::default()
        };
        let mut dormant = prepare_compiled_fragment(empty_result_submission(id), context).unwrap();
        let writer: Arc<dyn FragmentResultWriter> = Arc::new(SingleResultHolderWriter(Mutex::new(
            Some(Arc::new(UnknownResultHolder {
                drops: result_drops.clone(),
                payload_drops: payload_drops.clone(),
                _charge: std::mem::ManuallyDrop::new(
                    crate::runtime::mem_tracker::TrackedBytes::try_new(
                        512,
                        unknown_tracker.clone(),
                    )
                    .unwrap(),
                ),
            })),
        )));
        dormant
            .resources
            .acquire_result_for_static(
                Some(&novarocks_local_program::StaticSinkProgram::Result),
                &writer,
                ResultWriteSpec::new(
                    id,
                    crate::runtime::fragment::io::ResultPresentation::MysqlText,
                    None,
                    false,
                ),
            )
            .unwrap();
        drop(writer);
        let control = dormant.prepared.failure_control();
        let original = super::super::ExecutionFailure::from(
            novarocks_functions::KernelFailure::DeadlineExceeded,
        );
        let running = dormant.start_failed(original.clone());
        let deadline = Instant::now() + Duration::from_secs(2);
        while !gate.entered.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::yield_now();
        }
        // Record observations, then release every real gate before assertions.
        let entered = gate.entered.load(Ordering::Acquire);
        let stopped_body = running.inner.pipeline.stopped_fact();
        let terminal_while_held = running.stopped_fact();
        let charged_while_held = healthy_tracker.current();
        *gate.released.lock().unwrap() = true;
        gate.cv.notify_all();
        running
            .inner
            .pipeline
            .join()
            .expect_err("original deadline is unchanged");
        let deadline = Instant::now() + Duration::from_secs(2);
        while running.inner.lifecycle.state.lock().unwrap().advancing && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let advancing = running.inner.lifecycle.state.lock().unwrap().advancing;
        assert!(
            entered,
            "the healthy sibling reached its actual Box destructor"
        );
        assert!(
            stopped_body.is_some(),
            "all original driver work exited first"
        );
        assert!(terminal_while_held.is_none());
        assert_eq!(
            charged_while_held, 256,
            "a returned rollback is not holder exit"
        );
        assert!(
            !advancing,
            "the actual finisher callback must return after gate release"
        );
        assert_eq!(healthy_tracker.current(), 0);
        assert_eq!(unknown_tracker.current(), 512);
        assert!(!gate.registered.load(Ordering::Acquire));
        assert_eq!(gate.rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
        assert_eq!(result_drops.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
        assert!(
            running.stopped_fact().is_none(),
            "unknown holder cannot publish FragmentTerminal"
        );
        let observed = control.error().unwrap();
        assert!(observed.same_occurrence(&original));
        assert_eq!(
            control.local_exit_fault().unwrap().fact().reason(),
            super::super::ExecutionReason::FragmentResourceHolderDropPanicked.reason()
        );
        running.inner.lifecycle.advance();
        assert_eq!(gate.rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
        assert_eq!(result_drops.load(Ordering::SeqCst), 1);
    }

    struct PublisherCaptureDrop {
        drops: Arc<AtomicUsize>,
        payload_drops: Arc<AtomicUsize>,
    }
    impl Drop for PublisherCaptureDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(OpaqueCompletionPanic(self.payload_drops.clone()));
        }
    }

    #[test]
    fn terminal_publisher_keeps_both_opaque_payloads_without_revoking_physical_facts() {
        let dormant = prepare_compiled_fragment(
            empty_result_submission(UniqueId::new(98_604, 1)),
            FragmentPrepareContext::default(),
        )
        .unwrap();
        let control = dormant.prepared.failure_control();
        let weak_control = Arc::downgrade(&control);
        let running = dormant.start();
        let frozen = running.join();
        let body = running.inner.pipeline.stopped_fact().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let capture = PublisherCaptureDrop {
            drops: drops.clone(),
            payload_drops: payload_drops.clone(),
        };
        let called = calls.clone();
        let primary_drops = payload_drops.clone();
        let mut publisher: Option<super::FragmentTerminalPublisher> = Some(Box::new(move |_| {
            let _capture_is_still_owned = &capture;
            called.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(OpaqueCompletionPanic(primary_drops.clone()));
        }));
        running.bind_terminal_publisher(&mut publisher).unwrap();
        assert!(publisher.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
        assert_eq!(running.stopped_fact(), Some(frozen.clone()));
        assert_eq!(running.inner.pipeline.stopped_fact(), Some(body));
        assert!(
            control.error().is_none(),
            "metadata faults cannot become TaskRequired failures"
        );
        assert_eq!(
            control.local_exit_fault().unwrap().fact().reason(),
            super::super::ExecutionReason::TerminalPublisherCallPanicked.reason()
        );
        let terminal = running.stopped_fact().unwrap();
        running
            .inner
            .lifecycle
            .publisher
            .invoke(&terminal, &control);
        drop(running);
        drop(control);
        let deadline = Instant::now() + Duration::from_secs(2);
        while weak_control.upgrade().is_some() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(
            weak_control.upgrade().is_none(),
            "pure metadata quarantine must not retain the Task control owner"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn duplicate_terminal_publisher_preserves_its_original_slot_and_fact() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let running = prepare_compiled_fragment(
            empty_result_submission(UniqueId::new(98_603, 1)),
            FragmentPrepareContext::default(),
        )
        .unwrap()
        .start();
        let fact = running.join();
        let called = Arc::new(AtomicUsize::new(0));
        let target = called.clone();
        running.bind_test_publisher(move |_| {
            target.fetch_add(1, Ordering::SeqCst);
        });
        let mut rejected: Option<super::FragmentTerminalPublisher> =
            Some(Box::new(|_| panic!("a refused role must never run")));
        let error = running.bind_terminal_publisher(&mut rejected).unwrap_err();
        assert!(rejected.is_some());
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            error.failure_fact().reason(),
            super::super::ExecutionReason::CompletionRoleBindingConflict.reason()
        );
        assert_eq!(called.load(Ordering::SeqCst), 1);
        assert_eq!(running.stopped_fact(), Some(fact));
    }

    /// The terminal profile carries the memory hierarchy the fragment charged
    /// against.
    ///
    /// This is the seam that makes the later charge migration auditable: while
    /// charges still live on trackers and accounts are nearly empty, the only
    /// way to check that a byte moved from one tier to the other is to be able
    /// to read both. The tree was reachable in wave-1 but had no caller.
    #[test]
    fn the_terminal_profile_carries_the_memory_hierarchy() {
        let process = crate::runtime::mem_tracker::process_mem_tracker();
        let query = crate::runtime::mem_tracker::MemTracker::new_child("query_test", &process);
        let fragment = crate::runtime::mem_tracker::MemTracker::new_child("fragment_test", &query);
        fragment.consume(4096);

        let context = FragmentPrepareContext {
            profiler: Some(crate::runtime::profile::fragment_root_profiler(0)),
            mem_tracker: Some(Arc::clone(&fragment)),
            ..FragmentPrepareContext::default()
        };
        // A real execution runtime, so the fragment runs through the driver
        // executor production uses: the test executor never publishes the
        // stopped fact the terminal path depends on.
        let context = context
            .with_execution_runtime(crate::runtime::execution_runtime::test_execution_runtime());
        let terminal =
            prepare_compiled_fragment(empty_result_submission(UniqueId::new(99, 100)), context)
                .expect("noop fragment prepares")
                .start()
                .join();

        let profile = terminal.profile().expect("a profiled fragment reports one");
        let memory = profile
            .root
            .children
            .iter()
            .find(|child| child.name == "MemTracker")
            .expect("the terminal profile must carry the memory hierarchy");
        assert_eq!(
            memory.info_strings.get("Label").map(String::as_str),
            Some("fragment_test"),
            "the attached tree must be rooted at the fragment's own tracker"
        );
        assert!(
            memory
                .children
                .iter()
                .any(|child| child.name == "CommonMetrics"
                    && child
                        .counters
                        .iter()
                        .any(|counter| counter.name == "CurrentMemoryBytes")),
            "the tree must report the bytes actually charged, got: {memory:?}"
        );

        fragment.release(4096);
    }

    #[test]
    fn stopped_observer_panic_after_terminal_freeze_is_isolated() {
        let running = prepare_compiled_fragment(
            empty_result_submission(UniqueId::new(97, 98)),
            FragmentPrepareContext::default(),
        )
        .expect("fragment prepares")
        .start();
        let terminal = running.join();

        let first_owner = prepare_compiled_fragment(
            empty_result_submission(UniqueId::new(98_601, 1)),
            FragmentPrepareContext::default(),
        )
        .unwrap()
        .start();
        let last_owner = prepare_compiled_fragment(
            empty_result_submission(UniqueId::new(98_601, 2)),
            FragmentPrepareContext::default(),
        )
        .unwrap()
        .start();
        let first = Arc::new(AtomicUsize::new(0));
        let first_callback = Arc::clone(&first);
        let expected = first_owner.join();
        first_owner.bind_test_publisher(move |fact| {
            assert_eq!(fact, &expected);
            first_callback.fetch_add(1, Ordering::SeqCst);
        });
        running.bind_test_publisher(|_| {
            panic!("injected immediate fragment terminal observer panic");
        });
        let last = Arc::new(AtomicUsize::new(0));
        let last_callback = Arc::clone(&last);
        let expected = last_owner.join();
        last_owner.bind_test_publisher(move |fact| {
            assert_eq!(fact, &expected);
            last_callback.fetch_add(1, Ordering::SeqCst);
        });

        first_owner.join();
        last_owner.join();
        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(last.load(Ordering::SeqCst), 1);
        assert_eq!(running.stopped_fact(), Some(terminal));
    }
}

#[cfg(test)]
struct TestFragmentCommitPort;

#[cfg(test)]
impl FragmentCommitPort for TestFragmentCommitPort {
    fn acquire(
        &self,
        _fragment_instance_id: novarocks_types::UniqueId,
    ) -> crate::runtime::fragment::ExecutionResult<
        Box<dyn crate::runtime::fragment::io::FragmentCommitLease>,
    > {
        Ok(Box::new(TestFragmentCommitLease))
    }
}

#[cfg(test)]
struct TestFragmentCommitLease;

#[cfg(test)]
impl crate::runtime::fragment::io::FragmentCommitLease for TestFragmentCommitLease {
    fn add_load_stats(
        &mut self,
        _stats: crate::runtime::fragment::io::FragmentSinkLoadStats,
    ) -> super::ExecutionResult<()> {
        Ok(())
    }

    fn add_tablet_commit_info(
        &mut self,
        _info: crate::runtime::fragment::io::TabletCommitInfo,
    ) -> super::ExecutionResult<()> {
        Ok(())
    }

    fn add_tablet_fail_info(
        &mut self,
        _info: crate::runtime::fragment::io::TabletFailInfo,
    ) -> super::ExecutionResult<()> {
        Ok(())
    }

    fn finish(
        &mut self,
    ) -> super::ExecutionResult<crate::runtime::fragment::io::FragmentCommitReport> {
        Ok(crate::runtime::fragment::io::FragmentCommitReport::default())
    }

    fn handoff(&mut self) -> super::ExecutionResult<()> {
        Ok(())
    }

    fn rollback(&mut self) -> super::ExecutionResult<()> {
        Ok(())
    }
}

#[cfg(test)]
impl Default for FragmentPrepareContext {
    fn default() -> Self {
        Self {
            profiler: None,
            mem_tracker: None,
            runtime_filter: None,
            exchange_transmitter:
                crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
            #[cfg(novarocks_dev_controls)]
            exchange_test_observer: None,
            edge_gates: None,
            result_writer: crate::runtime::fragment::io::result::discard_result_writer(),
            event_sink: Arc::new(NoopFragmentEventSink),
            root_result_session: None,
            result_spec: None,
            result_identity: None,
            task_control: None,
            root_sink_dop: None,
            execution_runtime: Some(crate::runtime::execution_runtime::test_execution_runtime()),
            query_memory: None,
            commit_port: Arc::new(TestFragmentCommitPort),
            exchange_receiver_port:
                crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port(),
            receiver_construction_lifetime: ReceiverConstructionLifetime::KernelBound,
            #[cfg(test)]
            prepare_failure: None,
            #[cfg(test)]
            cleanup_faults: ResourceCleanupFaults::default(),
            #[cfg(test)]
            start_failure: None,
        }
    }
}

#[allow(
    dead_code,
    reason = "Failure-injection builders are retained for fragment lifecycle regression coverage."
)]
impl FragmentPrepareContext {
    pub fn new(
        profiler: Option<Profiler>,
        mem_tracker: Option<Arc<MemTracker>>,
        runtime_filter: Option<RuntimeFilterSessionRef>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self {
            profiler,
            mem_tracker,
            runtime_filter,
            exchange_transmitter,
            #[cfg(novarocks_dev_controls)]
            exchange_test_observer: None,
            result_writer,
            event_sink,
            edge_gates: None,
            root_result_session: None,
            result_spec: None,
            result_identity: None,
            task_control: None,
            root_sink_dop: None,
            execution_runtime: None,
            query_memory: None,
            commit_port: Arc::new(UnavailableFragmentCommitPort),
            exchange_receiver_port: Arc::new(UnavailableExchangeReceiverPort),
            receiver_construction_lifetime: ReceiverConstructionLifetime::KernelBound,
            #[cfg(test)]
            prepare_failure: None,
            #[cfg(test)]
            cleanup_faults: ResourceCleanupFaults::default(),
            #[cfg(test)]
            start_failure: None,
        }
    }

    pub fn with_query_memory(
        mut self,
        binding: Option<crate::runtime::query_memory::QueryMemoryBinding>,
    ) -> Self {
        self.query_memory = binding;
        self
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_exchange_test_observer(
        mut self,
        observer: Option<Arc<dyn crate::runtime::exchange::TestEventObserver>>,
    ) -> Self {
        self.exchange_test_observer = observer;
        self
    }

    pub fn with_execution_runtime(mut self, runtime: Arc<ExecutionRuntime>) -> Self {
        self.execution_runtime = Some(runtime);
        self
    }

    /// Native tasks keep authorized ingress alive until their inbound gate
    /// retires the registration, independently of local kernel quiescence.
    pub fn with_registration_owned_exchange_receivers(mut self) -> Self {
        self.receiver_construction_lifetime = ReceiverConstructionLifetime::RegistrationOwned;
        self
    }

    pub(super) fn receiver_construction(
        &self,
        runtime_state: &RuntimeState,
    ) -> Result<ReceiverConstruction, crate::exec::pipeline::operator::ConstructionFailure> {
        use crate::exec::pipeline::operator::input_construction::{
            ConstructionFailure, try_receiver_input_construction_factory,
            try_runtime_input_construction_factory,
        };
        let tracker = runtime_state
            .mem_tracker()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let profile = crate::exec::pipeline::pipeline::representation_construction_profile()?;
        match self.receiver_construction_lifetime {
            ReceiverConstructionLifetime::KernelBound => try_runtime_input_construction_factory(
                profile,
                tracker,
                runtime_state.error_state(),
            )
            .map(|factory| (factory, None)),
            ReceiverConstructionLifetime::RegistrationOwned => {
                try_receiver_input_construction_factory(
                    profile,
                    tracker,
                    runtime_state.error_state(),
                )
                .map(|(factory, stop)| (factory, Some(stop)))
            }
        }
    }

    /// Binds this fragment's push sinks to the per-edge send permission its
    /// task holds.
    ///
    /// Every frozen edge starts closed, so a sink built without this sends the
    /// moment it has rows and the barrier exists only on paper.
    pub fn with_edge_gates(
        mut self,
        gates: Arc<crate::runtime::fragment::io::exchange_edge::ExchangeEdgeGates>,
    ) -> Self {
        self.edge_gates = Some(gates);
        self
    }

    /// An exact task-owned provisional bounded root session. Opening and
    /// credit admission are host responsibilities; execution never falls
    /// back to the legacy result writer for a frozen RootResult program.
    pub fn with_root_result_session(
        mut self,
        session: Arc<dyn crate::runtime::fragment::io::RootResultSession>,
    ) -> Self {
        self.root_result_session = Some(session);
        self
    }

    pub fn with_task_control(
        mut self,
        control: Arc<crate::runtime::task_control::TaskExecutionControl>,
    ) -> Self {
        self.task_control = Some(control);
        self
    }

    pub fn with_result_identity(
        mut self,
        identity: novarocks_execution_contract::TaskIdentity,
    ) -> Self {
        self.result_identity = Some(identity);
        self
    }

    pub fn with_fragment_commit_port(mut self, port: Arc<dyn FragmentCommitPort>) -> Self {
        self.commit_port = port;
        self
    }

    pub fn with_exchange_receiver_port(mut self, port: Arc<dyn ExchangeReceiverPort>) -> Self {
        self.exchange_receiver_port = port;
        self
    }

    /// Builds a context for callers that do not participate in native
    /// runtime-filter execution (including backend integration tests).
    pub fn without_runtime_filter(
        profiler: Option<Profiler>,
        mem_tracker: Option<Arc<MemTracker>>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self::new(
            profiler,
            mem_tracker,
            None,
            exchange_transmitter,
            result_writer,
            event_sink,
        )
    }

    pub fn new_with_execution_overrides(
        profiler: Option<Profiler>,
        mem_tracker: Option<Arc<MemTracker>>,
        result_spec: Option<ResultWriteSpec>,
        root_sink_dop: Option<i32>,
        _group_execution_scan_dop: Option<i32>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self {
            profiler,
            mem_tracker,
            runtime_filter: None,
            exchange_transmitter,
            #[cfg(novarocks_dev_controls)]
            exchange_test_observer: None,
            result_writer,
            event_sink,
            edge_gates: None,
            root_result_session: None,
            result_spec,
            result_identity: None,
            task_control: None,
            root_sink_dop,
            execution_runtime: None,
            query_memory: None,
            commit_port: Arc::new(UnavailableFragmentCommitPort),
            exchange_receiver_port: Arc::new(UnavailableExchangeReceiverPort),
            receiver_construction_lifetime: ReceiverConstructionLifetime::KernelBound,
            #[cfg(test)]
            prepare_failure: None,
            #[cfg(test)]
            cleanup_faults: ResourceCleanupFaults::default(),
            #[cfg(test)]
            start_failure: None,
        }
    }

    fn cleanup_faults(&self) -> ResourceCleanupFaults {
        #[cfg(test)]
        {
            self.cleanup_faults.clone()
        }
        #[cfg(not(test))]
        {
            ResourceCleanupFaults::default()
        }
    }

    #[cfg(test)]
    fn with_prepare_failure(mut self, failure: PrepareFailurePoint) -> Self {
        self.prepare_failure = Some(failure);
        self
    }

    #[cfg(test)]
    fn with_cleanup_failure(
        mut self,
        resource: crate::runtime::fragment::resources::ResourceKind,
    ) -> Self {
        self.cleanup_faults = self.cleanup_faults.with_failure(resource);
        self
    }

    fn fail_if_injected(&self, point: PrepareFailurePoint) -> Result<(), FragmentLaunchError> {
        #[cfg(test)]
        if self.prepare_failure == Some(point) {
            return Err(FragmentLaunchError::from_failure(
                FragmentLaunchStage::Register,
                FragmentLaunchErrorKind::ResourceUnavailable,
                novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new(point.detail()),
                )
                .into(),
            ));
        }
        let _ = point;
        Ok(())
    }

    #[cfg(test)]
    fn with_start_failure(mut self, failure: StartFailurePoint) -> Self {
        self.start_failure = Some(failure);
        self
    }

    fn start_failure(&self) -> Option<StartFailurePoint> {
        #[cfg(test)]
        {
            self.start_failure
        }
        #[cfg(not(test))]
        {
            None
        }
    }
}

#[expect(
    clippy::enum_variant_names,
    reason = "Failure injection points intentionally describe the lifecycle point after which they fire."
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrepareFailurePoint {
    AfterSinkCommit,
    AfterResult,
    AfterExchange,
}

#[allow(
    dead_code,
    reason = "Failure-point descriptions are retained for lifecycle fault-injection diagnostics."
)]
impl PrepareFailurePoint {
    const fn detail(self) -> &'static str {
        match self {
            Self::AfterSinkCommit => "injected failure after sink commit registration",
            Self::AfterResult => "injected failure after result registration",
            Self::AfterExchange => "injected failure after exchange registration",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Start failure injection remains available for lifecycle regression coverage."
)]
enum StartFailurePoint {
    AfterSubmit,
}

#[allow(
    dead_code,
    reason = "Failure-point descriptions are retained for lifecycle fault-injection diagnostics."
)]
impl StartFailurePoint {
    const fn detail(self) -> &'static str {
        match self {
            Self::AfterSubmit => "injected partial start failure",
        }
    }
}

type FragmentSuccessValidator = Box<dyn FnOnce() -> super::ExecutionResult<()> + Send + 'static>;

#[allow(
    dead_code,
    reason = "The dormant handle retains fault-injection state for lifecycle regression coverage."
)]
pub struct DormantFragmentHandle {
    prepared: PreparedPipelineExecution,
    resources: FragmentResources,
    query_id: QueryId,
    fragment_instance_id: novarocks_types::UniqueId,
    profiler: Option<Profiler>,
    /// This fragment's memory tracker, carried so the terminal profile can
    /// report the hierarchy the fragment actually charged against.
    mem_tracker: Option<Arc<MemTracker>>,
    start_failure: Option<StartFailurePoint>,
    success_validator: Option<FragmentSuccessValidator>,
}

#[derive(Debug)]
enum PreparedDrainRefusal {
    MissingOwner,
    BeforeConclusion,
}
impl std::fmt::Display for PreparedDrainRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MissingOwner => "stopped preparation drain has no original dormant owner",
            Self::BeforeConclusion => {
                "prepared work drain requires its original accepted stop conclusion"
            }
        })
    }
}
impl std::error::Error for PreparedDrainRefusal {}
impl novarocks_failure_contract::FailureProjection for PreparedDrainRefusal {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use super::ExecutionReason as R;
        let reason = match self {
            Self::MissingOwner => R::PreparedDrainMissingOwner,
            Self::BeforeConclusion => R::PreparedDrainBeforeConclusion,
        };
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            reason.reason(),
        )
    }
}

impl DormantFragmentHandle {
    /// Extend the original post-stop, pre-settlement success validation. This
    /// author checks facts only; the original Task owner still decides the
    /// outcome. Validators compose instead of replacing earlier obligations.
    pub fn with_success_validator(
        mut self,
        validator: impl FnOnce() -> super::ExecutionResult<()> + Send + 'static,
    ) -> Self {
        let previous = self.success_validator.take();
        self.success_validator = Some(Box::new(move || {
            if let Some(previous) = previous {
                previous()?;
            }
            validator()
        }));
        self
    }
    /// Transfer the original already-stopped prepared work to the existing
    /// executor cleanup path. A refusal leaves the caller's owner untouched.
    /// This does not activate producers or author another Task conclusion.
    pub fn try_drain_stopped(
        slot: &mut Option<Self>,
    ) -> super::ExecutionResult<RunningFragmentHandle> {
        let dormant = slot.as_ref().ok_or_else(|| {
            super::ExecutionFailure::from_domain(PreparedDrainRefusal::MissingOwner)
        })?;
        if !dormant.prepared.failure_control().is_stopped() {
            return Err(super::ExecutionFailure::from_domain(
                PreparedDrainRefusal::BeforeConclusion,
            ));
        }
        let dormant = slot.take().expect("the checked original dormant owner");
        Ok(dormant.start())
    }
    /// Transfer the same physical registrations to the task's inbound-close
    /// owner before installation. Without this explicit transfer the fragment
    /// retains its original terminal cleanup and preparation rollback.
    pub fn take_exchange_retirement(
        &mut self,
    ) -> Option<super::resources::ExchangeReceiverRetirement> {
        self.resources.take_exchange_retirement()
    }

    pub const fn submitted_driver_count(&self) -> usize {
        self.prepared.submitted_driver_count()
    }

    pub fn start(self) -> RunningFragmentHandle {
        #[cfg(test)]
        let initial_failure = self.start_failure.map(|failure| {
            super::ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new(failure.detail()),
            ))
        });
        #[cfg(not(test))]
        let initial_failure = None;
        self.start_with_initial_failure(initial_failure)
    }

    /// Enter the running lifecycle with a terminal execution failure already latched.
    ///
    /// Drivers are still submitted and drained through the normal terminal-fact path.
    pub fn start_failed(self, error: impl Into<super::ExecutionFailure>) -> RunningFragmentHandle {
        self.start_with_initial_failure(Some(error.into()))
    }

    fn start_with_initial_failure(
        self,
        initial_failure: Option<super::ExecutionFailure>,
    ) -> RunningFragmentHandle {
        let Self {
            prepared,
            resources,
            query_id,
            fragment_instance_id,
            profiler,
            mem_tracker,
            mut success_validator,
            ..
        } = self;
        let control = prepared.failure_control();
        let finisher = prepared.resources_finisher();
        let lifecycle = Arc::new(RunningFragmentLifecycle {
            control: Arc::clone(&control),
            state: std::sync::Mutex::new(RunningFragmentState {
                resources: Some(resources.into_runtime()),
                pipeline_result: None,
                interrupt: None,
                advancing: false,
                terminal: None,
            }),
            terminal_cv: std::sync::Condvar::new(),
            publisher: crate::exec::pipeline::global_driver_executor::once_role::OnceRole::new(
                crate::exec::pipeline::global_driver_executor::once_role::Role::TerminalPublisher,
            ),
            finisher: Arc::downgrade(&finisher),
            query_id,
            fragment_instance_id,
            profiler,
            mem_tracker,
        });
        let lifecycle_on_stop = Arc::clone(&lifecycle);
        let mut role: Option<
            crate::exec::pipeline::global_driver_executor::once_role::Callback<
                crate::exec::pipeline::global_driver_executor::FragmentStoppedFact,
            >,
        > = Some(Box::new(move |stopped| {
            let mut result = stopped.conclusion();
            // An observed original cancellation/failure keeps its source and
            // does not acquire a synthetic strict-input failure. A not-yet-
            // visible proposal still competes in the same owner order below.
            if result.is_ok()
                && !lifecycle_on_stop.control.is_stopped()
                && let Some(validator) = success_validator.take()
                && let Err(cause) = validator()
            {
                lifecycle_on_stop.control.set_error(cause.clone());
                result = Err(cause);
            }
            // Handoff has no verdict. The existing barrier settles the new
            // proposal before resources read the current owner at retirement.
            lifecycle_on_stop.settle_after_stop(result)
        }));
        // The original lifecycle and its mandatory finisher exist before any
        // driver can finish, including a zero-driver execution.
        prepared
            .bind_resources_finisher(&mut role)
            .expect("one resource owner per dormant fragment");
        let pipeline = match initial_failure {
            Some(error) => prepared.start_failed(error),
            None => prepared.start(),
        };
        RunningFragmentHandle {
            inner: Arc::new(RunningFragmentInner {
                pipeline,
                lifecycle,
            }),
        }
    }
}

#[derive(Clone)]
pub struct RunningFragmentHandle {
    inner: Arc<RunningFragmentInner>,
}

struct RunningFragmentInner {
    pipeline: crate::exec::pipeline::executor::RunningPipelineExecution,
    lifecycle: Arc<RunningFragmentLifecycle>,
}

struct RunningFragmentLifecycle {
    control: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    state: std::sync::Mutex<RunningFragmentState>,
    terminal_cv: std::sync::Condvar,
    publisher: Arc<
        crate::exec::pipeline::global_driver_executor::once_role::OnceRole<FragmentTerminalFact>,
    >,
    finisher: std::sync::Weak<
        crate::exec::pipeline::global_driver_executor::once_role::OnceRole<
            crate::exec::pipeline::global_driver_executor::FragmentStoppedFact,
        >,
    >,
    query_id: QueryId,
    fragment_instance_id: novarocks_types::UniqueId,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
}

struct RunningFragmentState {
    resources: Option<super::resources::RuntimeFragmentResources>,
    pipeline_result: Option<super::ExecutionResult<()>>,
    interrupt: Option<String>,
    advancing: bool,
    terminal: Option<FragmentTerminalFact>,
}

/// The sole application publisher, distinct from the original resource finisher.
pub type FragmentTerminalPublisher = Box<dyn FnMut(&FragmentTerminalFact) + Send + 'static>;

// A foreign call may unwind through advance. Restore the original bounded
// resource cells before the outer named finisher catches that unwind.
struct ResourceLoan<'a> {
    owner: &'a RunningFragmentLifecycle,
    resources: Option<super::resources::RuntimeFragmentResources>,
    active: bool,
}
impl Drop for ResourceLoan<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.resources = self.resources.take();
        state.advancing = false;
    }
}

impl RunningFragmentHandle {
    pub fn fragment_instance_id(&self) -> novarocks_types::UniqueId {
        self.inner.lifecycle.fragment_instance_id
    }
    pub fn submitted_driver_count(&self) -> usize {
        self.inner.pipeline.submitted_driver_count()
    }
    pub fn cancel(&self, reason: FragmentCancelReason) {
        if self.stopped_fact().is_some() {
            return;
        }
        self.inner.pipeline.cancel(reason.clone());
        self.inner.lifecycle.interrupt(reason.detail());
    }
    pub fn join(&self) -> FragmentTerminalFact {
        let result = self.inner.pipeline.join();
        self.inner.lifecycle.settle_after_stop(result);
        let mut state = self
            .inner
            .lifecycle
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Unknown resource exit is not a terminal fact. Its original scope is
        // retained and process health closes admission; elapsed time is no proof.
        loop {
            if let Some(fact) = &state.terminal {
                return fact.clone();
            }
            state = self
                .inner
                .lifecycle
                .terminal_cv
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
    }
    /// A hint from the current published state, which can lag handed-off
    /// proposals. Only the settled lifecycle publishes a terminal outcome.
    pub fn conclusion(&self) -> Option<FragmentOutcome> {
        self.inner
            .pipeline
            .conclusion()
            .map(|result| outcome_from_result(result, &self.inner.lifecycle.control))
    }
    pub fn stopped_fact(&self) -> Option<FragmentTerminalFact> {
        self.inner
            .lifecycle
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .terminal
            .clone()
    }
    #[cfg(test)]
    fn bind_test_publisher(&self, publisher: impl FnMut(&FragmentTerminalFact) + Send + 'static) {
        let mut publisher: Option<FragmentTerminalPublisher> = Some(Box::new(publisher));
        self.bind_terminal_publisher(&mut publisher)
            .expect("one publisher per independent test fragment");
    }

    /// Bind exactly one Native publisher. Rejection preserves the caller slot.
    pub fn bind_terminal_publisher(
        &self,
        publisher: &mut Option<FragmentTerminalPublisher>,
    ) -> super::ExecutionResult<()> {
        self.inner
            .lifecycle
            .publisher
            .bind_notification(publisher)?;
        if let Some(fact) = self.stopped_fact() {
            self.inner
                .lifecycle
                .publisher
                .invoke(&fact, &self.inner.lifecycle.control);
        }
        Ok(())
    }
}

impl RunningFragmentLifecycle {
    fn interrupt(self: &Arc<Self>, reason: &str) {
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.terminal.is_some() {
                return;
            }
            if state.interrupt.is_none() {
                state.interrupt = Some(reason.to_owned());
            }
        }
        self.advance();
    }

    // Design: ADR-0193 (docs/adr/ADR-0193-task-conclusion-proposals-and-settled-points.md)
    fn settle_after_stop(self: &Arc<Self>, result: super::ExecutionResult<()>) -> bool {
        let lifecycle = Arc::clone(self);
        self.control.on_settled(Box::new(move |_snapshot| {
            // Settlement is a barrier, not a frozen outcome. Retirement keeps
            // reading the current owner snapshot at the actual decision point.
            lifecycle.finish_from_stopped(result);
        }));
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .terminal
            .is_some()
    }

    fn finish_from_stopped(self: &Arc<Self>, result: super::ExecutionResult<()>) -> bool {
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.terminal.is_some() {
                return true;
            }
            if state.pipeline_result.is_none() {
                state.pipeline_result = Some(result);
            }
        }
        self.advance();
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .terminal
            .is_some()
    }

    fn advance(self: &Arc<Self>) {
        let resources = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.advancing || state.terminal.is_some() {
                return;
            }
            state.advancing = true;
            state.resources.take()
        };
        let mut loan = ResourceLoan {
            owner: self,
            resources,
            active: true,
        };
        loop {
            let (interrupt, result) = {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                (state.interrupt.take(), state.pipeline_result.clone())
            };
            let Some(resources) = loan.resources.as_mut() else {
                return;
            };
            if let Some(reason) = interrupt {
                resources.interrupt(&reason, &self.control);
            }
            if let Some(result) = result {
                let outcome = outcome_from_result(result, &self.control);
                if !resources.retire(&outcome, &self.control) {
                    return;
                }
                if let (Some(profiler), Some(tracker)) =
                    (self.profiler.as_ref(), self.mem_tracker.as_ref())
                {
                    crate::runtime::profile::attach_mem_tracker_tree(profiler, tracker);
                }
                let fact = FragmentTerminalFact::new(
                    self.query_id,
                    self.fragment_instance_id,
                    outcome,
                    self.profiler.as_ref().map(Profiler::to_native_tree),
                );
                {
                    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    state.terminal = Some(fact.clone());
                    self.terminal_cv.notify_all();
                }
                drop(loan);
                // Terminal publication is metadata; its failure cannot revoke
                // either physical pipeline proof or retired resource proof.
                self.publisher.invoke(&fact, &self.control);
                if let Some(finisher) = self.finisher.upgrade() {
                    finisher.complete_deferred(&self.control);
                }
                return;
            }
            // If cancellation or stop raced the loan, consume that request in
            // this original scope. The check and loan return share the mutex.
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.interrupt.is_some() || state.pipeline_result.is_some() {
                continue;
            }
            state.resources = loan.resources.take();
            state.advancing = false;
            loan.active = false;
            drop(state);
            return;
        }
    }
}

fn outcome_from_result(
    result: super::ExecutionResult<()>,
    control: &crate::runtime::runtime_state::RuntimeErrorState,
) -> FragmentOutcome {
    use novarocks_execution_contract::{TaskConclusion, TerminationDetail};
    if let Some(accepted) = control.task_conclusion() {
        match accepted.neutral().conclusion() {
            TaskConclusion::Terminated(TerminationDetail::Failed(_)) => {
                return FragmentOutcome::Failed(FragmentExecutionError::from_failure(
                    FragmentExecutionErrorKind::Pipeline,
                    accepted
                        .originating_failure()
                        .expect("accepted failure has its actual source")
                        .clone(),
                ));
            }
            TaskConclusion::Terminated(TerminationDetail::Canceled(reason)) => {
                return FragmentOutcome::Cancelled {
                    reason: FragmentCancelReason::new(reason.as_str()),
                };
            }
            TaskConclusion::Terminated(TerminationDetail::Aborted(reason)) => {
                return FragmentOutcome::Cancelled {
                    reason: FragmentCancelReason::new(reason.as_str()),
                };
            }
            TaskConclusion::Succeeded => {}
        }
    }
    match result {
        Ok(()) => FragmentOutcome::Succeeded,
        Err(error) => {
            // Only this local controller's exact typed stop cause supplies a
            // local cancellation annotation. No text/class or parallel latch
            // can turn another originating failure into cancellation.
            if let super::ExecutionFailureCause::Domain(source) = error.cause()
                && let Some(reason) = source.downcast_ref::<FragmentCancelReason>()
            {
                return FragmentOutcome::Cancelled {
                    reason: reason.clone(),
                };
            }
            FragmentOutcome::Failed(FragmentExecutionError::from_failure(
                FragmentExecutionErrorKind::Pipeline,
                error,
            ))
        }
    }
}

impl Drop for RunningFragmentInner {
    fn drop(&mut self) {
        if self
            .lifecycle
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .terminal
            .is_none()
        {
            let reason = FragmentCancelReason::new("running fragment handle dropped");
            self.pipeline.cancel(reason.clone());
            self.lifecycle.interrupt(reason.detail());
        }
    }
}

#[path = "compiled_prepare.rs"]
pub(super) mod compiled_prepare;
pub use compiled_prepare::{
    CompiledFragmentSubmission, CompiledWriterBindings, compiled_sink_kind,
    prepare_compiled_fragment, prepare_compiled_fragment_with_metadata_host,
};

#[cfg(test)]
mod typed_failure_tests {
    use super::*;
    use novarocks_functions::{KernelDiagnostic, KernelFailure};
    #[test]
    fn typed_failure_fragment_outcome_preserves_original_cause_and_source() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InstanceFailed,
            KernelFailure::Internal(KernelDiagnostic::new("internal")),
            KernelFailure::InvalidProgram(KernelDiagnostic::new("invalid")),
            KernelFailure::Operational(KernelDiagnostic::new("operational")),
        ] {
            let FragmentOutcome::Failed(error) = outcome_from_result(
                Err(cause.clone().into()),
                &crate::runtime::runtime_state::RuntimeErrorState::default(),
            ) else {
                panic!("unrequested kernel cancellation remains a failure");
            };
            assert_eq!(error.kind(), FragmentExecutionErrorKind::Pipeline);
            assert_eq!(
                error.cause().cause(),
                &super::super::ExecutionFailureCause::Kernel(cause.clone())
            );
            assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .source()
                    .unwrap()
                    .downcast_ref::<KernelFailure>(),
                Some(&cause)
            );
        }
    }
    #[test]
    fn fragment_outcome_uses_the_actual_task_acceptance_in_both_cancel_failure_orders() {
        use crate::runtime::{runtime_state::RuntimeErrorState, task_control::test_support::Owner};
        use novarocks_execution_contract::{
            AbortCause, CancelReason, TaskConclusionPort, TaskConclusionProposal, TaskFailurePhase,
        };
        let owner = Owner::new();
        let control =
            RuntimeErrorState::for_task(Arc::new(owner.view(TaskFailurePhase::Execution)));
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let late: super::super::ExecutionFailure = KernelFailure::ResourceExhausted.into();
        control.set_error(late.clone());
        assert_eq!(
            outcome_from_result(Err(late), &control),
            FragmentOutcome::Cancelled {
                reason: FragmentCancelReason::new(CancelReason::UpstreamNoLongerNeeded.as_str())
            }
        );
        let owner = Owner::new();
        let control =
            RuntimeErrorState::for_task(Arc::new(owner.view(TaskFailurePhase::Execution)));
        let original: super::super::ExecutionFailure = KernelFailure::ResourceExhausted.into();
        control.set_error(original.clone());
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let observation: super::super::ExecutionFailure =
            FragmentCancelReason::new("late stand-down").into();
        let FragmentOutcome::Failed(failure) = outcome_from_result(Err(observation), &control)
        else {
            panic!("the originating resource failure must remain primary");
        };
        assert!(failure.cause().same_occurrence(&original));
        let owner = Owner::new();
        let control =
            RuntimeErrorState::for_task(Arc::new(owner.view(TaskFailurePhase::Execution)));
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let observation = control.error().unwrap();
        assert!(matches!(
            outcome_from_result(Err(observation), &control),
            FragmentOutcome::Cancelled { .. }
        ));
        let original: super::super::ExecutionFailure = KernelFailure::ResourceExhausted.into();
        control.set_error(original.clone());
        let FragmentOutcome::Failed(failure) = outcome_from_result(Ok(()), &control) else {
            panic!("legal refinement reads the actual shared origin");
        };
        assert!(failure.cause().same_occurrence(&original));
    }
    #[test]
    fn local_fragment_stop_annotation_belongs_only_to_its_exact_typed_cause() {
        let control = crate::runtime::runtime_state::RuntimeErrorState::default();
        let reason = FragmentCancelReason::new("local owner stopped");
        assert_eq!(
            outcome_from_result(Err(reason.clone().into()), &control),
            FragmentOutcome::Cancelled { reason }
        );
        assert!(matches!(
            outcome_from_result(Err(KernelFailure::ResourceExhausted.into()), &control),
            FragmentOutcome::Failed(_)
        ));
        assert_eq!(
            outcome_from_result(Ok(()), &control),
            FragmentOutcome::Succeeded
        );
    }
}

#[cfg(test)]
fn compiled_root_result_context_fixture() -> FragmentPrepareContext {
    let mut context = owner_tests::BoundedRootSession::new().context();
    context.root_sink_dop = Some(1);
    context
}
