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

use crate::common::config::debug_exec_node_output;
use crate::exec::fragment::program::FragmentProgram;
use crate::exec::pipeline::executor::{
    PreparedPipelineExecution, prepare_report_neutral_pipeline_execution,
};
use crate::runtime::fragment::error::{
    FragmentExecutionError, FragmentExecutionErrorKind, FragmentLaunchError,
    FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::exchange::materialize_exchange_bindings;
use crate::runtime::fragment::fact::{FragmentCancelReason, FragmentOutcome, FragmentTerminalFact};
use crate::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentEventSink, FragmentLookupClient, FragmentResultWriter,
    LoadTrackingLogSink, ResultPresentation, ResultWriteSpec,
};
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
use crate::runtime::query_context::QueryId;
use crate::runtime_filter::service::NativeRuntimeFilterExecutionContext;

pub struct FragmentPrepareContext {
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    runtime_filter: Option<NativeRuntimeFilterExecutionContext>,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    lookup_client: Arc<dyn FragmentLookupClient>,
    result_writer: Arc<dyn FragmentResultWriter>,
    event_sink: Arc<dyn FragmentEventSink>,
    load_tracking_sink: Option<Arc<dyn LoadTrackingLogSink>>,
    result_spec: Option<ResultWriteSpec>,
    root_sink_dop: Option<i32>,
    group_execution_scan_dop: Option<i32>,
    #[cfg(test)]
    prepare_failure: Option<PrepareFailurePoint>,
    #[cfg(test)]
    cleanup_faults: ResourceCleanupFaults,
    #[cfg(test)]
    start_failure: Option<StartFailurePoint>,
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
            result_writer: crate::runtime::fragment::io::result::in_process_test_result_writer(),
            event_sink: Arc::new(NoopFragmentEventSink),
            load_tracking_sink: None,
            result_spec: None,
            root_sink_dop: None,
            group_execution_scan_dop: None,
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
    pub(crate) fn new(
        profiler: Option<Profiler>,
        mem_tracker: Option<Arc<MemTracker>>,
        runtime_filter: Option<NativeRuntimeFilterExecutionContext>,
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
            load_tracking_sink: None,
            result_spec: None,
            root_sink_dop: None,
            group_execution_scan_dop: None,
            #[cfg(test)]
            prepare_failure: None,
            #[cfg(test)]
            cleanup_faults: ResourceCleanupFaults::default(),
            #[cfg(test)]
            start_failure: None,
        }
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

    pub fn with_load_tracking_sink(mut self, sink: Arc<dyn LoadTrackingLogSink>) -> Self {
        self.load_tracking_sink = Some(sink);
        self
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
            load_tracking_sink: None,
            result_spec,
            root_sink_dop,
            group_execution_scan_dop,
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
    fragment_instance_id: crate::common::types::UniqueId,
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
    fragment_instance_id: crate::common::types::UniqueId,
    profiler: Option<Profiler>,
}

struct RunningFragmentState {
    resources: FragmentResources,
    cancel_reason: Option<FragmentCancelReason>,
    terminal: Option<FragmentTerminalFact>,
    statistics_sink: Option<crate::exec::operators::StatisticsSinkHandle>,
}

impl RunningFragmentHandle {
    pub fn fragment_instance_id(&self) -> crate::common::types::UniqueId {
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
    let mut resources = FragmentResources::new(context.cleanup_faults());
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
                query_options: apply_query_option_overrides(Some(
                    instance.runtime_options().query_options().clone(),
                )),
                query_id: Some(query_id),
                fragment_instance_id: Some(finst_id),
                backend_num: Some(instance.backend_num().get()),
                mem_tracker: context.mem_tracker.clone(),
                native_runtime_filter_context: context.runtime_filter.clone(),
                load_tracking_sink: context.load_tracking_sink.clone(),
                connector_staged_report_collector: program
                    .sink()
                    .program()
                    .connector_staged_report_collector(),
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
        let exchange_bindings = materialize_exchange_bindings(program, instance);
        let scan_bindings = materialize_scan_bindings(program, instance)?;
        prepare_report_neutral_pipeline_execution(
            program.plan().clone(),
            debug_exec_node_output(),
            Duration::from_millis(50),
            sink,
            exchange_bindings,
            scan_bindings,
            Some((finst_id.hi, finst_id.lo)),
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::{StatisticsMetric, StatisticsMetricRequest};

    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        ExchangeInputContract, FragmentContractVersion, FragmentNodeId, FragmentProgram,
        FragmentProgramOptions, FragmentSinkSpec, RuntimeFilterContract,
    };
    use crate::exec::fragment::sink::{
        DataStreamSinkProgram, FragmentSinkProgram, StatisticsSinkProgram,
    };
    use crate::exec::node::exchange_source::ExchangeSourceNode;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::operators::DataStreamPartitionType;
    use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
    use crate::runtime::exchange::{ExchangeKey, snapshot_receiver_state};
    use crate::runtime::fragment::fact::{FragmentCancelReason, FragmentOutcome};
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
        FragmentInstanceSpec, FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::fragment::submission::FragmentSubmission;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::{result_buffer, sink_commit};

    use super::StartFailurePoint;
    use super::{FragmentPrepareContext, PrepareFailurePoint, prepare_fragment};
    use crate::runtime::fragment::resources::ResourceKind;

    fn result_exchange_submission(finst_id: UniqueId) -> FragmentSubmission {
        result_exchange_submission_with(
            finst_id,
            NonZeroUsize::new(1).expect("one sender"),
            QueryOptions::default(),
        )
    }

    fn result_exchange_submission_with(
        finst_id: UniqueId,
        expected_senders: NonZeroUsize,
        query_options: QueryOptions,
    ) -> FragmentSubmission {
        let node_id = FragmentNodeId::new(17);
        let schema = Arc::new(ChunkSchema::empty());
        let program = Arc::new(FragmentProgram::new(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::ExchangeSource(ExchangeSourceNode::new(
                        node_id.get(),
                        Duration::from_secs(30),
                        Arc::clone(&schema),
                    )),
                },
            },
            FragmentSinkSpec::try_new(FragmentSinkProgram::Result).expect("result sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::from([(node_id, ExchangeInputContract::new(schema))]),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        ));
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId {
                hi: finst_id.hi - 2,
                lo: finst_id.lo - 2,
            },
            FragmentInstanceId::new(finst_id),
            ScanAssignments::default(),
            ExchangeInputAssignments::new(BTreeMap::from([(
                node_id,
                ExchangeInputAssignment::new(expected_senders),
            )])),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(query_options, None, false),
            NonZeroUsize::new(1).expect("one driver"),
            BackendNum::try_new(1).expect("backend number"),
        );
        FragmentSubmission::try_new(program, instance).expect("valid submission")
    }

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
            QueryId {
                hi: finst_id.hi - 2,
                lo: finst_id.lo - 2,
            },
            FragmentInstanceId::new(finst_id),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), None, false),
            NonZeroUsize::new(1).expect("one driver"),
            BackendNum::try_new(1).expect("backend number"),
        );
        FragmentSubmission::try_new(program, instance).expect("valid submission")
    }

    fn statistics_submission(finst_id: UniqueId) -> FragmentSubmission {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        let chunk = Chunk::new_with_chunk_schema(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 2]))])
                .expect("values batch"),
            chunk_schema,
        );
        let program = Arc::new(FragmentProgram::new(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 29 }),
                },
            },
            FragmentSinkSpec::try_new(FragmentSinkProgram::Statistics(StatisticsSinkProgram::new(
                StatisticsMetricRequest::try_new(vec![
                    StatisticsMetric::RowCount,
                    StatisticsMetric::ThetaNdv { column: "v".into() },
                ])
                .expect("statistics metrics"),
            )))
            .expect("statistics sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        ));
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId {
                hi: finst_id.hi - 2,
                lo: finst_id.lo - 2,
            },
            FragmentInstanceId::new(finst_id),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), None, false),
            NonZeroUsize::new(1).expect("one driver"),
            BackendNum::try_new(1).expect("backend number"),
        );
        FragmentSubmission::try_new(program, instance).expect("valid statistics submission")
    }

    fn data_stream_submission(
        finst_id: UniqueId,
        backend_num: i32,
        query_options: QueryOptions,
    ) -> FragmentSubmission {
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
            FragmentSinkSpec::try_new(FragmentSinkProgram::DataStream(
                DataStreamSinkProgram::try_new(
                    17,
                    Vec::new(),
                    DataStreamPartitionType::Unpartitioned,
                    Vec::new(),
                    Vec::new(),
                    None,
                    ExprArena::default(),
                )
                .expect("data stream program"),
            ))
            .expect("data stream sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::new(BTreeSet::new(), BTreeSet::new()),
        ));
        let destination = FragmentDestination::new(
            UniqueId {
                hi: finst_id.hi + 2,
                lo: finst_id.lo + 2,
            },
            RuntimeEndpoint::new("127.0.0.1", 1).expect("test endpoint"),
        );
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            QueryId {
                hi: finst_id.hi - 2,
                lo: finst_id.lo - 2,
            },
            FragmentInstanceId::new(finst_id),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::StreamDestinations {
                destinations: vec![destination],
                sender_id: Some(11),
            },
            FragmentRuntimeOptions::new(query_options, None, false),
            NonZeroUsize::new(1).expect("one driver"),
            BackendNum::try_new(backend_num).expect("backend number"),
        );
        FragmentSubmission::try_new(program, instance).expect("valid submission")
    }

    fn exchange_key(finst_id: UniqueId) -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: finst_id.hi,
            finst_id_lo: finst_id.lo,
            node_id: 17,
        }
    }

    fn expect_prepare_error(
        result: Result<
            super::DormantFragmentHandle,
            crate::runtime::fragment::error::FragmentLaunchError,
        >,
    ) -> crate::runtime::fragment::error::FragmentLaunchError {
        match result {
            Ok(_) => panic!("expected injected prepare failure"),
            Err(error) => error,
        }
    }

    #[test]
    fn statistics_sink_attaches_one_partial_to_the_terminal_fact_without_result_io() {
        let finst_id = UniqueId {
            hi: 72_051,
            lo: 72_052,
        };
        let running = prepare_fragment(
            statistics_submission(finst_id),
            FragmentPrepareContext::default(),
        )
        .expect("statistics fragment prepares")
        .start();
        let fact = running.join();

        assert!(matches!(fact.outcome(), FragmentOutcome::Succeeded));
        assert!(
            !fact.statistics_payload().is_empty(),
            "statistics terminal fact must carry one bounded partial"
        );
        assert!(
            !result_buffer::is_registered(finst_id),
            "statistics collection must not open user result I/O"
        );
        let partial = crate::query_execution::statistics::StatisticsCollectionFinalizer::
            try_from_fragment_payload(fact.statistics_payload())
            .expect("typed statistics partial");
        assert!(matches!(
            partial
                .metric_states(
                    &StatisticsMetricRequest::try_new(vec![StatisticsMetric::RowCount])
                        .expect("row metric"),
                )
                .get(&StatisticsMetric::RowCount),
            Some(novarocks_spi::connector::StatisticsMetricState::Available(
                novarocks_spi::connector::StatisticsMetricValue::U64(3)
            ))
        ));
    }

    #[test]
    fn prepare_defers_submission_and_dormant_drop_rolls_back_all_registrations() {
        let finst_id = UniqueId {
            hi: 72_001,
            lo: 72_002,
        };
        let dormant = prepare_fragment(
            result_exchange_submission(finst_id),
            FragmentPrepareContext::default(),
        )
        .expect("fragment prepares");

        assert_eq!(dormant.submitted_driver_count(), 0);
        assert!(sink_commit::is_registered(finst_id));
        assert!(result_buffer::is_registered(finst_id));
        assert!(snapshot_receiver_state(exchange_key(finst_id)).is_some());

        drop(dormant);

        assert!(!sink_commit::is_registered(finst_id));
        assert!(!result_buffer::is_registered(finst_id));
        assert!(snapshot_receiver_state(exchange_key(finst_id)).is_none());
    }

    #[test]
    fn prepare_failure_at_each_registration_boundary_rolls_back_acquired_resources() {
        for (offset, failure) in [
            (0, PrepareFailurePoint::AfterSinkCommit),
            (10, PrepareFailurePoint::AfterResult),
            (20, PrepareFailurePoint::AfterExchange),
        ] {
            let finst_id = UniqueId {
                hi: 72_101 + offset,
                lo: 72_102 + offset,
            };
            let error = expect_prepare_error(prepare_fragment(
                result_exchange_submission(finst_id),
                FragmentPrepareContext::default().with_prepare_failure(failure),
            ));

            assert_eq!(error.detail(), failure.detail());
            assert!(error.cleanup_diagnostics().is_empty());
            assert!(!sink_commit::is_registered(finst_id));
            assert!(!result_buffer::is_registered(finst_id));
            assert!(snapshot_receiver_state(exchange_key(finst_id)).is_none());
        }
    }

    #[test]
    fn prepare_error_keeps_primary_failure_and_attaches_cleanup_diagnostics() {
        let finst_id = UniqueId {
            hi: 72_201,
            lo: 72_202,
        };
        let failure = PrepareFailurePoint::AfterExchange;
        let error = expect_prepare_error(prepare_fragment(
            result_exchange_submission(finst_id),
            FragmentPrepareContext::default()
                .with_prepare_failure(failure)
                .with_cleanup_failure(ResourceKind::Result),
        ));

        assert_eq!(error.detail(), failure.detail());
        assert_eq!(
            error.cleanup_diagnostics(),
            &["injected result registration cleanup failure".to_string()]
        );
        assert!(!sink_commit::is_registered(finst_id));
        assert!(!result_buffer::is_registered(finst_id));
        assert!(snapshot_receiver_state(exchange_key(finst_id)).is_none());
    }

    #[test]
    fn duplicate_exchange_registration_does_not_rollback_the_existing_owner() {
        let finst_id = UniqueId {
            hi: 72_251,
            lo: 72_252,
        };
        let key = exchange_key(finst_id);
        crate::runtime::exchange::register_expected_chunk_schema(
            key,
            1,
            Arc::new(ChunkSchema::empty()),
        )
        .expect("existing exchange owner registers");

        let error = expect_prepare_error(prepare_fragment(
            result_exchange_submission_with(
                finst_id,
                NonZeroUsize::new(7).expect("seven senders"),
                QueryOptions::default(),
            ),
            FragmentPrepareContext::default(),
        ));

        assert!(error.detail().contains("already registered"));
        assert!(!sink_commit::is_registered(finst_id));
        assert!(!result_buffer::is_registered(finst_id));
        let snapshot = snapshot_receiver_state(key).expect("existing exchange owner remains");
        assert_eq!(
            snapshot.expected_senders, 1,
            "failed duplicate acquisition must not mutate the existing owner"
        );
        crate::runtime::exchange::cancel_exchange_key(key);
    }

    #[test]
    fn kernel_handle_never_invokes_legacy_progress_reporter() {
        let mut query_options = QueryOptions::default();
        query_options.enable_profile = true;
        query_options.runtime_profile_report_interval = Some(1);
        let priming_state = crate::runtime::runtime_state::RuntimeState::new(
            Some(query_options.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(!priming_state.should_report_exec_state());
        std::thread::sleep(Duration::from_millis(1_050));

        let finst_id = UniqueId {
            hi: 72_271,
            lo: 72_272,
        };
        let running = prepare_fragment(
            result_exchange_submission_with(
                finst_id,
                NonZeroUsize::new(1).expect("one sender"),
                query_options,
            ),
            FragmentPrepareContext::default(),
        )
        .expect("fragment prepares")
        .start();
        assert!(running.submitted_driver_count() > 0);
        std::thread::sleep(Duration::from_millis(100));

        running.cancel(FragmentCancelReason::new("test cleanup"));
        let _ = running.join();
    }

    #[test]
    fn kernel_data_stream_eos_preserves_backend_identity_without_legacy_reporting() {
        let mut query_options = QueryOptions::default();
        query_options.enable_profile = true;
        query_options.runtime_profile_report_interval = Some(1);
        let priming_state = crate::runtime::runtime_state::RuntimeState::new(
            Some(query_options.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(!priming_state.should_report_exec_state());
        std::thread::sleep(Duration::from_millis(1_050));
        let finst_id = UniqueId {
            hi: 72_281,
            lo: 72_282,
        };
        let running = prepare_fragment(
            data_stream_submission(finst_id, 37, query_options),
            FragmentPrepareContext::default(),
        )
        .expect("fragment prepares")
        .start();

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let be_number = loop {
            if let Some(be_number) = crate::exec::operators::take_eos_be_number_for_test(finst_id) {
                break be_number;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "data stream sink did not create an EOS payload"
            );
            std::thread::sleep(Duration::from_millis(10));
        };

        running.cancel(FragmentCancelReason::new("test cleanup"));
        let _ = running.join();
        assert_eq!(
            be_number, 37,
            "EOS sender identity must preserve the submission backend number"
        );
    }

    #[test]
    fn start_and_repeated_join_freeze_the_same_success_fact() {
        let finst_id = UniqueId {
            hi: 72_301,
            lo: 72_302,
        };
        let running =
            prepare_fragment(noop_submission(finst_id), FragmentPrepareContext::default())
                .expect("fragment prepares")
                .start();

        assert!(running.submitted_driver_count() > 0);
        let first = running.join();
        let second = running.join();

        assert_eq!(first, second);
        assert_eq!(first.fragment_instance_id(), finst_id);
        assert_eq!(
            first.query_id(),
            QueryId {
                hi: finst_id.hi - 2,
                lo: finst_id.lo - 2,
            }
        );
        assert!(matches!(first.outcome(), FragmentOutcome::Succeeded));
        assert!(sink_commit::is_registered(finst_id));
        drop(running);
        assert!(!sink_commit::is_registered(finst_id));
    }

    #[test]
    fn repeated_cancel_and_join_keep_the_first_cancel_reason() {
        let finst_id = UniqueId {
            hi: 72_401,
            lo: 72_402,
        };
        let running = prepare_fragment(
            result_exchange_submission(finst_id),
            FragmentPrepareContext::default(),
        )
        .expect("fragment prepares")
        .start();

        running.cancel(FragmentCancelReason::new("adapter cancelled"));
        running.cancel(FragmentCancelReason::new("later cancel"));
        let first = running.join();
        let second = running.join();

        assert_eq!(first, second);
        assert!(matches!(
            first.outcome(),
            FragmentOutcome::Cancelled { reason }
                if reason.detail() == "adapter cancelled"
        ));
        assert!(snapshot_receiver_state(exchange_key(finst_id)).is_none());
        let result_buffer::TryFetchResult::Error(error) = result_buffer::try_fetch(finst_id) else {
            panic!("cancelled result buffer must expose terminal error");
        };
        assert!(matches!(
            error.kind,
            result_buffer::FetchErrorKind::Cancelled
        ));
    }

    #[test]
    fn success_wins_over_late_cancel() {
        let finst_id = UniqueId {
            hi: 72_501,
            lo: 72_502,
        };
        let running =
            prepare_fragment(noop_submission(finst_id), FragmentPrepareContext::default())
                .expect("fragment prepares")
                .start();

        let succeeded = running.join();
        running.cancel(FragmentCancelReason::new("too late"));

        assert_eq!(running.join(), succeeded);
        assert!(matches!(succeeded.outcome(), FragmentOutcome::Succeeded));
    }

    #[test]
    fn partial_start_failure_wins_over_late_cancel_and_drains() {
        let finst_id = UniqueId {
            hi: 72_601,
            lo: 72_602,
        };
        let running = prepare_fragment(
            result_exchange_submission(finst_id),
            FragmentPrepareContext::default().with_start_failure(StartFailurePoint::AfterSubmit),
        )
        .expect("fragment prepares")
        .start();

        let failed = running.join();
        running.cancel(FragmentCancelReason::new("too late"));

        assert_eq!(running.join(), failed);
        assert!(matches!(
            failed.outcome(),
            FragmentOutcome::Failed(error)
                if error.detail() == "injected partial start failure"
        ));
        assert!(snapshot_receiver_state(exchange_key(finst_id)).is_none());
    }

    #[test]
    fn concurrent_join_observes_the_cancel_winner() {
        let finst_id = UniqueId {
            hi: 72_701,
            lo: 72_702,
        };
        let running = prepare_fragment(
            result_exchange_submission(finst_id),
            FragmentPrepareContext::default(),
        )
        .expect("fragment prepares")
        .start();
        let waiter = running.clone();
        let join = std::thread::spawn(move || waiter.join());

        running.cancel(FragmentCancelReason::new("race cancel"));
        let fact = join.join().expect("join thread");

        assert!(matches!(
            fact.outcome(),
            FragmentOutcome::Cancelled { reason }
                if reason.detail() == "race cancel"
        ));
        assert_eq!(running.join(), fact);
    }
}
