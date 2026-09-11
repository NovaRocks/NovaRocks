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

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use crate::exec::fragment::program::FragmentProgram;
use crate::exec::pipeline::executor::{
    PreparedPipelineExecution, prepare_report_neutral_pipeline_execution,
};
use crate::runtime::execution_runtime::ExecutionRuntime;
use crate::runtime::fragment::error::{
    FragmentExecutionError, FragmentExecutionErrorKind, FragmentLaunchError,
    FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::exchange::materialize_exchange_bindings;
use crate::runtime::fragment::fact::{FragmentCancelReason, FragmentOutcome, FragmentTerminalFact};
use crate::runtime::fragment::io::FragmentEventSink;
#[cfg(test)]
use crate::runtime::fragment::io::NoopFragmentEventSink;
use crate::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentResultWriter, ResultPresentation, ResultWriteSpec,
};
use crate::runtime::fragment::io::{
    ExchangeReceiverPort, FragmentCommitPort, ScanRegistrationPort,
    UnavailableExchangeReceiverPort, UnavailableFragmentCommitPort,
};
use crate::runtime::fragment::resources::{FragmentResources, ResourceCleanupFaults};
use crate::runtime::fragment::runtime_state::{
    RuntimeStateInputs, apply_query_option_overrides, build_runtime_state,
};
use crate::runtime::fragment::scan::materialize_scan_bindings;
use crate::runtime::fragment::sink::materialize_fragment_sink_with_result;
use crate::runtime::fragment::submission::FragmentSubmission;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime_filter::RuntimeFilterSessionRef;
use novarocks_types::QueryId;

pub struct FragmentPrepareContext {
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    runtime_filter: Option<RuntimeFilterSessionRef>,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    result_writer: Arc<dyn FragmentResultWriter>,
    event_sink: Arc<dyn FragmentEventSink>,
    result_spec: Option<ResultWriteSpec>,
    result_identity: Option<novarocks_execution_contract::TaskIdentity>,
    root_sink_dop: Option<i32>,
    group_execution_scan_dop: Option<i32>,
    debug_exec_node_output: bool,
    execution_runtime: Option<Arc<ExecutionRuntime>>,
    scan_registration: Option<Arc<dyn ScanRegistrationPort>>,
    commit_port: Arc<dyn FragmentCommitPort>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
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

#[cfg(test)]
mod owner_tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        FragmentContractVersion, FragmentProgram, FragmentProgramOptions, FragmentSinkSpec,
        RuntimeFilterContract,
    };
    use crate::exec::fragment::sink::FragmentSinkProgram;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::fragment::io::{
        FragmentIoError, FragmentResultSession, FragmentResultWriter, ResultAbort, ResultWriteSpec,
    };
    use crate::runtime::fragment::submission::FragmentSubmission;
    use crate::runtime::query_options::QueryOptions;
    use novarocks_types::{QueryId, SlotId, UniqueId};

    use super::{FragmentOutcome, FragmentPrepareContext, prepare_fragment};

    fn noop_submission(finst_id: UniqueId) -> FragmentSubmission {
        submission(finst_id, Chunk::default(), FragmentSinkProgram::Noop)
    }

    fn submission(
        finst_id: UniqueId,
        chunk: Chunk,
        sink: FragmentSinkProgram,
    ) -> FragmentSubmission {
        let program = Arc::new(FragmentProgram::new(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 19 }),
                },
            },
            FragmentSinkSpec::try_new(sink).expect("valid sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        ));
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId::new(finst_id.high() - 2, finst_id.low() - 2),
            FragmentInstanceId::new(finst_id),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(1).expect("one driver"),
            BackendNum::try_new(1).expect("backend number"),
        );
        FragmentSubmission::try_new(program, instance).expect("valid submission")
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
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("one row chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    struct BlockingResultSession {
        entered: AtomicBool,
        released: Mutex<bool>,
        released_cv: Condvar,
        aborted: AtomicBool,
    }

    impl BlockingResultSession {
        fn new() -> Arc<Self> {
            Arc::new(Self {
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
        fn reservation_bytes(&self, chunk: &Chunk) -> Result<usize, FragmentIoError> {
            Ok(chunk.logical_bytes())
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
        ) -> Result<(), FragmentIoError> {
            debug_assert_eq!(credit.bytes(), chunk.logical_bytes());
            self.entered.store(true, Ordering::Release);
            let mut released = self.released.lock().expect("result release lock");
            while !*released {
                released = self
                    .released_cv
                    .wait(released)
                    .expect("result release wait");
            }
            Ok(())
        }

        fn finish(&self) -> Result<(), FragmentIoError> {
            Ok(())
        }

        fn abort(&self, _reason: ResultAbort) {
            self.aborted.store(true, Ordering::Release);
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

    #[test]
    fn execution_owner_prepares_starts_and_freezes_a_noop_fragment() {
        let handle = prepare_fragment(
            noop_submission(UniqueId::new(91, 92)),
            FragmentPrepareContext::default(),
        )
        .expect("fragment prepares");
        assert!(matches!(
            handle.start().join().outcome(),
            FragmentOutcome::Succeeded
        ));
    }

    #[test]
    fn dropping_running_handle_is_non_blocking_and_cleanup_waits_for_actual_stop() {
        let session = BlockingResultSession::new();
        let mut context = FragmentPrepareContext::default();
        context.result_writer = Arc::new(BlockingResultWriter {
            session: Arc::clone(&session),
        });
        let running = prepare_fragment(
            submission(
                UniqueId::new(93, 94),
                one_row_chunk(),
                FragmentSinkProgram::Result,
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
            "test sink must hold the driver inside an unfinished result write"
        );

        let (stopped_tx, stopped_rx) = mpsc::sync_channel(1);
        running.subscribe_stopped(move |fact| {
            stopped_tx
                .send(fact)
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
        let stopped_before_release = stopped_rx.recv_timeout(Duration::from_millis(20)).is_ok();
        let aborted_before_release = session.aborted.load(Ordering::Acquire);

        session.release();
        drop_join.join().expect("drop thread must not panic");
        assert!(
            drop_returned,
            "dropping a running handle must not join its driver"
        );
        assert!(
            !stopped_before_release,
            "cancellation must not publish actual stop while the driver is still in write"
        );
        assert!(
            !aborted_before_release,
            "result ownership must remain registered until the driver actually stops"
        );

        let stopped = stopped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("driver release must publish actual stop");
        assert!(matches!(
            stopped.outcome(),
            FragmentOutcome::Cancelled { .. }
        ));
        assert!(
            session.aborted.load(Ordering::Acquire),
            "result registration must be cancelled after actual driver stop"
        );
    }

    #[test]
    fn stopped_observer_panic_before_terminal_freeze_is_isolated() {
        let session = BlockingResultSession::new();
        let mut context = FragmentPrepareContext::default();
        context.result_writer = Arc::new(BlockingResultWriter {
            session: Arc::clone(&session),
        });
        let running = prepare_fragment(
            submission(
                UniqueId::new(95, 96),
                one_row_chunk(),
                FragmentSinkProgram::Result,
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

        let first = Arc::new(AtomicUsize::new(0));
        let first_callback = Arc::clone(&first);
        running.subscribe_stopped(move |_| {
            first_callback.fetch_add(1, Ordering::SeqCst);
        });
        running.subscribe_stopped(|_| {
            panic!("injected fragment terminal observer panic");
        });
        let last = Arc::new(AtomicUsize::new(0));
        let last_callback = Arc::clone(&last);
        let (last_tx, last_rx) = mpsc::sync_channel(1);
        running.subscribe_stopped(move |_| {
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
        assert_eq!(first.load(Ordering::SeqCst), 1);
        assert_eq!(last.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn stopped_observer_panic_after_terminal_freeze_is_isolated() {
        let running = prepare_fragment(
            noop_submission(UniqueId::new(97, 98)),
            FragmentPrepareContext::default(),
        )
        .expect("fragment prepares")
        .start();
        let terminal = running.join();

        let first = Arc::new(AtomicUsize::new(0));
        let first_callback = Arc::clone(&first);
        let expected = terminal.clone();
        running.subscribe_stopped(move |fact| {
            assert_eq!(fact, expected);
            first_callback.fetch_add(1, Ordering::SeqCst);
        });
        running.subscribe_stopped(|_| {
            panic!("injected immediate fragment terminal observer panic");
        });
        let last = Arc::new(AtomicUsize::new(0));
        let last_callback = Arc::clone(&last);
        let expected = terminal.clone();
        running.subscribe_stopped(move |fact| {
            assert_eq!(fact, expected);
            last_callback.fetch_add(1, Ordering::SeqCst);
        });

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
    ) -> Result<Box<dyn crate::runtime::fragment::io::FragmentCommitLease>, String> {
        Ok(Box::new(TestFragmentCommitLease))
    }
}

#[cfg(test)]
struct TestFragmentCommitLease;

#[cfg(test)]
impl crate::runtime::fragment::io::FragmentCommitLease for TestFragmentCommitLease {
    fn add_load_stats(&mut self, _stats: crate::runtime::fragment::io::FragmentSinkLoadStats) {}

    fn add_tablet_commit_info(
        &mut self,
        _info: crate::runtime::fragment::io::TabletCommitInfo,
    ) -> Result<(), String> {
        Ok(())
    }

    fn add_tablet_fail_info(
        &mut self,
        _info: crate::runtime::fragment::io::TabletFailInfo,
    ) -> Result<(), String> {
        Ok(())
    }

    fn finish(
        self: Box<Self>,
    ) -> Result<crate::runtime::fragment::io::FragmentCommitReport, String> {
        Ok(crate::runtime::fragment::io::FragmentCommitReport::default())
    }

    fn handoff(self: Box<Self>) -> Result<(), String> {
        Ok(())
    }

    fn rollback(self: Box<Self>) -> Result<(), String> {
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
            edge_gates: None,
            result_writer: crate::runtime::fragment::io::result::discard_result_writer(),
            event_sink: Arc::new(NoopFragmentEventSink),
            result_spec: None,
            result_identity: None,
            root_sink_dop: None,
            group_execution_scan_dop: None,
            debug_exec_node_output: false,
            execution_runtime: Some(crate::runtime::execution_runtime::test_execution_runtime()),
            scan_registration: None,
            commit_port: Arc::new(TestFragmentCommitPort),
            exchange_receiver_port:
                crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port(),
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
            result_writer,
            event_sink,
            edge_gates: None,
            result_spec: None,
            result_identity: None,
            root_sink_dop: None,
            group_execution_scan_dop: None,
            debug_exec_node_output: false,
            execution_runtime: None,
            scan_registration: None,
            commit_port: Arc::new(UnavailableFragmentCommitPort),
            exchange_receiver_port: Arc::new(UnavailableExchangeReceiverPort),
            #[cfg(test)]
            prepare_failure: None,
            #[cfg(test)]
            cleanup_faults: ResourceCleanupFaults::default(),
            #[cfg(test)]
            start_failure: None,
        }
    }

    pub fn with_execution_runtime(mut self, runtime: Arc<ExecutionRuntime>) -> Self {
        self.execution_runtime = Some(runtime);
        self
    }

    pub fn with_debug_exec_node_output(mut self, enabled: bool) -> Self {
        self.debug_exec_node_output = enabled;
        self
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

    pub fn with_scan_registration_port(mut self, port: Arc<dyn ScanRegistrationPort>) -> Self {
        self.scan_registration = Some(port);
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

    #[expect(
        clippy::too_many_arguments,
        reason = "Fault-injection construction keeps independently-owned execution services explicit."
    )]
    pub fn new_with_execution_overrides(
        profiler: Option<Profiler>,
        mem_tracker: Option<Arc<MemTracker>>,
        result_spec: Option<ResultWriteSpec>,
        root_sink_dop: Option<i32>,
        group_execution_scan_dop: Option<i32>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self {
            profiler,
            mem_tracker,
            runtime_filter: None,
            exchange_transmitter,
            result_writer,
            event_sink,
            edge_gates: None,
            result_spec,
            result_identity: None,
            root_sink_dop,
            group_execution_scan_dop,
            debug_exec_node_output: false,
            execution_runtime: None,
            scan_registration: None,
            commit_port: Arc::new(UnavailableFragmentCommitPort),
            exchange_receiver_port: Arc::new(UnavailableExchangeReceiverPort),
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
            return Err(FragmentLaunchError::new(
                FragmentLaunchStage::Register,
                FragmentLaunchErrorKind::ResourceUnavailable,
                point.detail(),
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
    start_failure: Option<StartFailurePoint>,
}

impl DormantFragmentHandle {
    pub const fn submitted_driver_count(&self) -> usize {
        self.prepared.submitted_driver_count()
    }

    pub fn start(self) -> RunningFragmentHandle {
        #[cfg(test)]
        let initial_failure = self
            .start_failure
            .map(|failure| failure.detail().to_string());
        #[cfg(not(test))]
        let initial_failure = None;
        self.start_with_initial_failure(initial_failure)
    }

    /// Enter the running lifecycle with a terminal execution failure already latched.
    ///
    /// Drivers are still submitted and drained through the normal terminal-fact path.
    pub fn start_failed(self, error: impl Into<String>) -> RunningFragmentHandle {
        self.start_with_initial_failure(Some(error.into()))
    }

    fn start_with_initial_failure(self, initial_failure: Option<String>) -> RunningFragmentHandle {
        let Self {
            prepared,
            resources,
            query_id,
            fragment_instance_id,
            profiler,
            ..
        } = self;
        let pipeline = match initial_failure {
            Some(error) => prepared.start_failed(error),
            None => prepared.start(),
        };
        let lifecycle = Arc::new(RunningFragmentLifecycle {
            state: std::sync::Mutex::new(RunningFragmentState {
                resources,
                cancel_reason: None,
                terminal: None,
                stopped_observers: Vec::new(),
            }),
            query_id,
            fragment_instance_id,
            profiler,
        });
        let lifecycle_on_stop = Arc::clone(&lifecycle);
        pipeline.subscribe_stopped(move |stopped| {
            lifecycle_on_stop.freeze_terminal(stopped.conclusion());
        });
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
    state: std::sync::Mutex<RunningFragmentState>,
    query_id: QueryId,
    fragment_instance_id: novarocks_types::UniqueId,
    profiler: Option<Profiler>,
}

struct RunningFragmentState {
    resources: FragmentResources,
    cancel_reason: Option<FragmentCancelReason>,
    terminal: Option<FragmentTerminalFact>,
    stopped_observers: Vec<FragmentTerminalObserver>,
}

type FragmentTerminalObserver = Box<dyn FnOnce(FragmentTerminalFact) + Send + 'static>;

fn invoke_terminal_observer(observer: FragmentTerminalObserver, terminal: FragmentTerminalFact) {
    // The terminal fact is immutable and retained before notification. Isolate
    // every callback so one faulty observer cannot block the remaining owners.
    if catch_unwind(AssertUnwindSafe(|| observer(terminal))).is_err() {
        tracing::error!("fragment terminal observer panicked after the fact was frozen");
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
        let mut state = self
            .inner
            .lifecycle
            .state
            .lock()
            .expect("running fragment state lock");
        if state.terminal.is_some() {
            return;
        }
        if self.inner.pipeline.cancel(reason.detail().to_string()) {
            state.cancel_reason = Some(reason);
        }
    }

    pub fn join(&self) -> FragmentTerminalFact {
        let result = self.inner.pipeline.join();
        self.inner.lifecycle.freeze_terminal(result)
    }

    /// Returns the execution conclusion before actual stop when failure or
    /// cancellation has already won. Success is known only after stop.
    pub fn conclusion(&self) -> Option<FragmentOutcome> {
        self.inner.pipeline.conclusion().map(|result| {
            let state = self
                .inner
                .lifecycle
                .state
                .lock()
                .expect("running fragment state lock");
            outcome_from_result(result, state.cancel_reason.clone())
        })
    }

    pub fn stopped_fact(&self) -> Option<FragmentTerminalFact> {
        self.inner
            .lifecycle
            .state
            .lock()
            .expect("running fragment state lock")
            .terminal
            .clone()
    }

    /// Registers a one-shot observer for actual local stop and resource
    /// convergence. Registration after stop invokes the observer immediately.
    pub fn subscribe_stopped(&self, observer: impl FnOnce(FragmentTerminalFact) + Send + 'static) {
        self.inner.lifecycle.subscribe_stopped(Box::new(observer));
    }

    pub fn handoff_sink_commit(&self) {
        self.inner
            .lifecycle
            .state
            .lock()
            .expect("running fragment state lock")
            .resources
            .handoff_sink_commit();
    }
}

impl RunningFragmentLifecycle {
    fn freeze_terminal(&self, result: Result<(), String>) -> FragmentTerminalFact {
        let (fact, observers) = {
            let mut state = self.state.lock().expect("running fragment state lock");
            if let Some(fact) = state.terminal.as_ref() {
                return fact.clone();
            }
            let outcome = outcome_from_result(result, state.cancel_reason.clone());
            match &outcome {
                FragmentOutcome::Succeeded => state.resources.finish_success(),
                FragmentOutcome::Failed(error) => {
                    state.resources.finish_failure(error.to_string());
                }
                FragmentOutcome::Cancelled { reason } => {
                    state
                        .resources
                        .finish_cancelled(reason.detail().to_string());
                }
            }
            let fact = FragmentTerminalFact::new(
                self.query_id,
                self.fragment_instance_id,
                outcome,
                self.profiler.as_ref().map(Profiler::to_native_tree),
            );
            state.terminal = Some(fact.clone());
            let observers = std::mem::take(&mut state.stopped_observers);
            (fact, observers)
        };
        for observer in observers {
            invoke_terminal_observer(observer, fact.clone());
        }
        fact
    }

    fn subscribe_stopped(&self, observer: FragmentTerminalObserver) {
        let mut observer = Some(observer);
        let terminal = {
            let mut state = self.state.lock().expect("running fragment state lock");
            match state.terminal.clone() {
                Some(terminal) => Some(terminal),
                None => {
                    state
                        .stopped_observers
                        .push(observer.take().expect("stopped observer is available"));
                    None
                }
            }
        };
        if let Some(terminal) = terminal {
            invoke_terminal_observer(
                observer
                    .take()
                    .expect("stopped observer was not registered"),
                terminal,
            );
        }
    }
}

fn outcome_from_result(
    result: Result<(), String>,
    cancel_reason: Option<FragmentCancelReason>,
) -> FragmentOutcome {
    match result {
        Ok(()) => FragmentOutcome::Succeeded,
        Err(error) => match cancel_reason {
            Some(reason) => FragmentOutcome::Cancelled { reason },
            None => FragmentOutcome::Failed(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Pipeline,
                error,
            )),
        },
    }
}

impl Drop for RunningFragmentInner {
    fn drop(&mut self) {
        let reason = FragmentCancelReason::new("running fragment handle dropped");
        {
            let mut state = self
                .lifecycle
                .state
                .lock()
                .expect("running fragment state lock");
            if state.terminal.is_none() && self.pipeline.cancel(reason.detail().to_string()) {
                state.cancel_reason = Some(reason);
            }
        }
    }
}

pub fn prepare_fragment(
    submission: FragmentSubmission,
    context: FragmentPrepareContext,
) -> Result<DormantFragmentHandle, FragmentLaunchError> {
    let program: &Arc<FragmentProgram> = submission.program();
    let instance = submission.instance();
    let query_id = instance.query_id();
    let finst_id = instance.fragment_instance_id().get();
    let logical_pipeline_dop = i32::try_from(instance.pipeline_dop().get()).map_err(|_| {
        FragmentLaunchError::new(
            FragmentLaunchStage::BuildPipelines,
            FragmentLaunchErrorKind::PipelineBuild,
            format!(
                "pipeline DOP {} exceeds runtime representation",
                instance.pipeline_dop()
            ),
        )
    })?;
    let pipeline_dop = crate::runtime::exec_env::calc_pipeline_dop(logical_pipeline_dop);
    let mut resources = FragmentResources::new(
        Arc::clone(&context.commit_port),
        Arc::clone(&context.exchange_receiver_port),
        context.cleanup_faults(),
    );
    let prepare_result = (|| {
        resources.acquire_sink_commit(finst_id)?;
        context.fail_if_injected(PrepareFailurePoint::AfterSinkCommit)?;
        let mut result_spec = context.result_spec.clone().unwrap_or_else(|| {
            ResultWriteSpec::new(
                finst_id,
                ResultPresentation::MysqlText,
                None,
                instance.runtime_options().typed_result_sink(),
            )
        });
        if let Some(identity) = context.result_identity {
            result_spec = result_spec.with_task_identity(identity);
        }
        resources.acquire_result(program, &context.result_writer, result_spec)?;
        context.fail_if_injected(PrepareFailurePoint::AfterResult)?;
        resources.acquire_exchange(program, instance)?;
        context.fail_if_injected(PrepareFailurePoint::AfterExchange)?;

        let runtime_state = build_runtime_state(
            RuntimeStateInputs {
                query_options: apply_query_option_overrides(
                    Some(instance.runtime_options().query_options().clone()),
                    context.execution_runtime.as_deref(),
                ),
                query_id: Some(query_id),
                fragment_instance_id: Some(finst_id),
                backend_num: Some(instance.backend_num().get()),
                mem_tracker: context.mem_tracker.clone(),
                runtime_filter_session: context.runtime_filter.clone(),
                execution_runtime: context.execution_runtime.clone(),
                scan_registration: context.scan_registration.clone(),
            },
            context.profiler.as_ref(),
        )
        .map_err(|error| {
            FragmentLaunchError::new(
                FragmentLaunchStage::BuildRuntimeState,
                FragmentLaunchErrorKind::ResourceUnavailable,
                error,
            )
        })?;
        let materialized_sink = materialize_fragment_sink_with_result(
            program,
            instance,
            Arc::clone(&context.exchange_transmitter),
            resources.result_session(),
            context.edge_gates.clone(),
        )?;
        let sink = materialized_sink.factory;
        let _group_execution_scan_dop = context.group_execution_scan_dop;
        let exchange_bindings = materialize_exchange_bindings(
            program,
            instance,
            Arc::clone(&context.exchange_receiver_port),
        );
        let scan_bindings = materialize_scan_bindings(program, instance)?;
        prepare_report_neutral_pipeline_execution(
            program.plan().clone(),
            context.debug_exec_node_output,
            Duration::from_millis(50),
            sink,
            exchange_bindings,
            scan_bindings,
            Some((finst_id.high(), finst_id.low())),
            context.profiler.clone(),
            pipeline_dop,
            runtime_state,
            context.root_sink_dop,
            context.runtime_filter.clone(),
            Arc::clone(&context.event_sink),
        )
        .map_err(|error| {
            FragmentLaunchError::new(
                FragmentLaunchStage::BuildPipelines,
                FragmentLaunchErrorKind::PipelineBuild,
                error,
            )
        })
    })();
    match prepare_result {
        Ok(prepared) => Ok(DormantFragmentHandle {
            prepared,
            resources,
            query_id,
            fragment_instance_id: finst_id,
            profiler: context.profiler.clone(),
            start_failure: context.start_failure(),
        }),
        Err(error) => Err(error.with_cleanup_diagnostics(resources.rollback())),
    }
}
