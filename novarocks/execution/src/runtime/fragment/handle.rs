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
use crate::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentResultWriter, ResultPresentation, ResultWriteSpec,
};
use crate::runtime::fragment::io::{
    ExchangeReceiverPort, FragmentCommitPort, ScanRegistrationPort,
    UnavailableExchangeReceiverPort, UnavailableFragmentCommitPort,
};
use crate::runtime::fragment::io::{FragmentEventSink, FragmentLookupClient};
#[cfg(test)]
use crate::runtime::fragment::io::{NoopFragmentEventSink, UnavailableFragmentLookupClient};
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
    lookup_client: Arc<dyn FragmentLookupClient>,
    result_writer: Arc<dyn FragmentResultWriter>,
    event_sink: Arc<dyn FragmentEventSink>,
    result_spec: Option<ResultWriteSpec>,
    root_sink_dop: Option<i32>,
    group_execution_scan_dop: Option<i32>,
    debug_exec_node_output: bool,
    execution_runtime: Option<Arc<ExecutionRuntime>>,
    scan_registration: Option<Arc<dyn ScanRegistrationPort>>,
    commit_port: Arc<dyn FragmentCommitPort>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
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
    use std::sync::Arc;

    use crate::exec::chunk::Chunk;
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
    use crate::runtime::fragment::submission::FragmentSubmission;
    use crate::runtime::query_options::QueryOptions;
    use novarocks_types::{QueryId, UniqueId};

    use super::{FragmentOutcome, FragmentPrepareContext, prepare_fragment};

    fn noop_submission(finst_id: UniqueId) -> FragmentSubmission {
        let program = Arc::new(FragmentProgram::new(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode {
                        chunk: Chunk::default(),
                        node_id: 19,
                    }),
                },
            },
            FragmentSinkSpec::try_new(FragmentSinkProgram::Noop).expect("noop sink"),
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

    fn add_tablet_commit_info(&mut self, _info: crate::runtime::fragment::io::TabletCommitInfo) {}

    fn add_tablet_fail_info(&mut self, _info: crate::runtime::fragment::io::TabletFailInfo) {}

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
            lookup_client: Arc::new(UnavailableFragmentLookupClient),
            result_writer: crate::runtime::fragment::io::result::discard_result_writer(),
            event_sink: Arc::new(NoopFragmentEventSink),
            result_spec: None,
            root_sink_dop: None,
            group_execution_scan_dop: None,
            debug_exec_node_output: false,
            execution_runtime: None,
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

impl FragmentPrepareContext {
    pub fn new(
        profiler: Option<Profiler>,
        mem_tracker: Option<Arc<MemTracker>>,
        runtime_filter: Option<RuntimeFilterSessionRef>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self {
            profiler,
            mem_tracker,
            runtime_filter,
            exchange_transmitter,
            lookup_client,
            result_writer,
            event_sink,
            result_spec: None,
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

    pub fn with_scan_registration_port(mut self, port: Arc<dyn ScanRegistrationPort>) -> Self {
        self.scan_registration = Some(port);
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
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self::new(
            profiler,
            mem_tracker,
            None,
            exchange_transmitter,
            lookup_client,
            result_writer,
            event_sink,
        )
    }

    pub fn new_with_execution_overrides(
        profiler: Option<Profiler>,
        mem_tracker: Option<Arc<MemTracker>>,
        result_spec: Option<ResultWriteSpec>,
        root_sink_dop: Option<i32>,
        group_execution_scan_dop: Option<i32>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> Self {
        Self {
            profiler,
            mem_tracker,
            runtime_filter: None,
            exchange_transmitter,
            lookup_client,
            result_writer,
            event_sink,
            result_spec,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrepareFailurePoint {
    AfterSinkCommit,
    AfterResult,
    AfterExchange,
}

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
enum StartFailurePoint {
    AfterSubmit,
}

impl StartFailurePoint {
    const fn detail(self) -> &'static str {
        match self {
            Self::AfterSubmit => "injected partial start failure",
        }
    }
}

pub struct DormantFragmentHandle {
    prepared: PreparedPipelineExecution,
    resources: FragmentResources,
    query_id: QueryId,
    fragment_instance_id: novarocks_types::UniqueId,
    profiler: Option<Profiler>,
    statistics_sink: Option<crate::exec::operators::StatisticsSinkHandle>,
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
            statistics_sink,
            ..
        } = self;
        let pipeline = match initial_failure {
            Some(error) => prepared.start_failed(error),
            None => prepared.start(),
        };
        RunningFragmentHandle {
            inner: Arc::new(RunningFragmentInner {
                pipeline,
                state: std::sync::Mutex::new(RunningFragmentState {
                    resources,
                    cancel_reason: None,
                    terminal: None,
                    statistics_sink,
                }),
                query_id,
                fragment_instance_id,
                profiler,
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
    state: std::sync::Mutex<RunningFragmentState>,
    query_id: QueryId,
    fragment_instance_id: novarocks_types::UniqueId,
    profiler: Option<Profiler>,
}

struct RunningFragmentState {
    resources: FragmentResources,
    cancel_reason: Option<FragmentCancelReason>,
    terminal: Option<FragmentTerminalFact>,
    statistics_sink: Option<crate::exec::operators::StatisticsSinkHandle>,
}

impl RunningFragmentHandle {
    pub fn fragment_instance_id(&self) -> novarocks_types::UniqueId {
        self.inner.fragment_instance_id
    }

    pub fn submitted_driver_count(&self) -> usize {
        self.inner.pipeline.submitted_driver_count()
    }

    pub fn cancel(&self, reason: FragmentCancelReason) {
        let mut state = self
            .inner
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
        self.inner.freeze_terminal(result)
    }

    pub fn handoff_sink_commit(&self) {
        self.inner
            .state
            .lock()
            .expect("running fragment state lock")
            .resources
            .handoff_sink_commit();
    }

    pub fn take_connector_staged_report_frames(
        &self,
    ) -> Vec<novarocks_spi::connector::ConnectorStagedReportFrame> {
        self.inner.pipeline.take_connector_staged_report_frames()
    }
}

impl RunningFragmentInner {
    fn freeze_terminal(&self, result: Result<(), String>) -> FragmentTerminalFact {
        let mut state = self.state.lock().expect("running fragment state lock");
        if let Some(fact) = state.terminal.as_ref() {
            return fact.clone();
        }
        let mut outcome = match result {
            Ok(()) => FragmentOutcome::Succeeded,
            Err(error) => match state.cancel_reason.clone() {
                Some(reason) => FragmentOutcome::Cancelled { reason },
                None => FragmentOutcome::Failed(FragmentExecutionError::new(
                    FragmentExecutionErrorKind::Pipeline,
                    error,
                )),
            },
        };
        let statistics_payload = if matches!(outcome, FragmentOutcome::Succeeded) {
            match state.statistics_sink.as_ref() {
                None => Vec::new(),
                Some(handle) => match handle.take_fragment_payload() {
                    Ok(Some(payload)) => payload.to_vec(),
                    Ok(None) => {
                        outcome = FragmentOutcome::Failed(FragmentExecutionError::new(
                            FragmentExecutionErrorKind::Pipeline,
                            "statistics sink completed without a terminal partial",
                        ));
                        Vec::new()
                    }
                    Err(error) => {
                        outcome = FragmentOutcome::Failed(FragmentExecutionError::new(
                            FragmentExecutionErrorKind::Pipeline,
                            format!("statistics sink failed to encode terminal partial: {error}"),
                        ));
                        Vec::new()
                    }
                },
            }
        } else {
            Vec::new()
        };
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
            statistics_payload,
        );
        state.terminal = Some(fact.clone());
        fact
    }
}

impl Drop for RunningFragmentInner {
    fn drop(&mut self) {
        let reason = FragmentCancelReason::new("running fragment handle dropped");
        {
            let mut state = self.state.lock().expect("running fragment state lock");
            if state.terminal.is_none() && self.pipeline.cancel(reason.detail().to_string()) {
                state.cancel_reason = Some(reason);
            }
        }
        let result = self.pipeline.join();
        let _ = self.freeze_terminal(result);
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
        let result_spec = context.result_spec.clone().unwrap_or_else(|| {
            ResultWriteSpec::new(
                finst_id,
                ResultPresentation::MysqlText,
                None,
                instance.runtime_options().typed_result_sink(),
            )
        });
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
                connector_staged_report_collector: program
                    .sink()
                    .program()
                    .connector_staged_report_collector(),
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
        )?;
        let statistics_sink = materialized_sink.statistics_handle;
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
            Arc::clone(&context.lookup_client),
        )
        .map_err(|error| {
            FragmentLaunchError::new(
                FragmentLaunchStage::BuildPipelines,
                FragmentLaunchErrorKind::PipelineBuild,
                error,
            )
        })
        .map(|prepared| (prepared, statistics_sink))
    })();
    match prepare_result {
        Ok((prepared, statistics_sink)) => Ok(DormantFragmentHandle {
            prepared,
            resources,
            query_id,
            fragment_instance_id: finst_id,
            profiler: context.profiler.clone(),
            statistics_sink,
            start_failure: context.start_failure(),
        }),
        Err(error) => Err(error.with_cleanup_diagnostics(resources.rollback())),
    }
}
