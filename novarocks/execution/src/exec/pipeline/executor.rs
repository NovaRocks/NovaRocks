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
//! Top-level pipeline executor entrypoint.
//!
//! Responsibilities:
//! - Builds runtime pipeline context and executes one plan fragment to completion.
//! - Bridges fragment context, driver executor, and terminal sink orchestration.
//!
//! Key exported interfaces:
//! - Functions: fixed native and compat pipeline execution entrypoints.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::preparation_metadata::{
    CompiledMetadataMode, CompiledSchemaMetadataScope, DirectCompiledSchemaMetadataScope,
};

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::exec::node::scan::ScanOp;
use crate::exec::node::{ExecPlan, LocalRuntimeBindings};
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::runtime::runtime_state::RuntimeState;
use tracing::{info, warn};

use super::builder::{
    PipelineGraph, build_compiled_pipeline_graph, build_compiled_pipeline_graph_with_metadata_host,
    build_native_pipeline_graph_for_exec_plan_with_runtime_settings,
    build_native_pipeline_graph_for_local_program_with_runtime_settings,
};
use super::dependency::DependencyManager;
use super::fragment_context::FragmentContext;
use super::global_driver_executor::{DriverTask, FragmentCompletion, FragmentStoppedFact};
use super::operator_factory::OperatorFactory;
use super::pipeline::Pipeline;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::fragment::io::FragmentEventSink;
use novarocks_local_program::{KernelAbiVersion, LocalProgramGraph};

use crate::runtime::profile::{Profiler, ScopedTimer};

#[cfg(test)]
fn test_driver_executor() -> &'static super::global_driver_executor::GlobalDriverExecutor {
    static EXECUTOR: std::sync::OnceLock<super::global_driver_executor::GlobalDriverExecutor> =
        std::sync::OnceLock::new();
    EXECUTOR.get_or_init(|| super::global_driver_executor::GlobalDriverExecutor::new(1))
}

/// A fully materialized pipeline that has not submitted any drivers to the global executor.
pub struct PreparedPipelineExecution {
    tasks: Vec<DriverTask>,
    completion: Arc<FragmentCompletion>,
    fragment_ctx: Arc<FragmentContext>,
    runtime_state: Arc<RuntimeState>,
    fragment_profiler: Option<Profiler>,
    terminal_scan_ops: Vec<Arc<dyn ScanOp>>,
}

impl PreparedPipelineExecution {
    pub fn driver_count(&self) -> usize {
        self.tasks.len()
    }

    pub const fn submitted_driver_count(&self) -> usize {
        0
    }

    /// Submit this execution exactly once by consuming its dormant task collection.
    pub fn start(self) -> RunningPipelineExecution {
        self.start_with_initial_failure(None)
    }

    /// Submit this execution with a terminal failure already latched.
    pub fn start_failed(self, error: impl Into<ExecutionFailure>) -> RunningPipelineExecution {
        self.start_with_initial_failure(Some(error.into()))
    }

    fn start_with_initial_failure(
        self,
        initial_failure: Option<ExecutionFailure>,
    ) -> RunningPipelineExecution {
        let Self {
            tasks,
            completion,
            fragment_ctx,
            runtime_state,
            fragment_profiler,
            terminal_scan_ops,
        } = self;
        let submitted_driver_count = tasks.len();
        let fragment_wall_timer = fragment_profiler
            .as_ref()
            .map(|p| p.scoped_timer("FragmentWallTime"));
        if let Some(error) = initial_failure
            && completion.fail(error.clone())
        {
            fragment_ctx.set_final_status(error);
            terminate_scan_ops(&terminal_scan_ops);
        }
        let admitted = if let Some(runtime) = runtime_state.execution_runtime() {
            runtime.driver_executor().submit(tasks)
        } else {
            #[cfg(test)]
            {
                test_driver_executor().submit(tasks)
            }
            #[cfg(not(test))]
            panic!("prepared execution requires an ExecutionRuntime");
        };
        if !admitted {
            // Rejection latches a failure and retains any pending cleanup until
            // actual stop. Release scan resources alongside that failed start.
            terminate_scan_ops(&terminal_scan_ops);
        }
        let fragment_wall_timer = Arc::new(Mutex::new(fragment_wall_timer));
        let timer_on_stop = Arc::clone(&fragment_wall_timer);
        completion.subscribe_stopped(Box::new(move |_| {
            timer_on_stop
                .lock()
                .expect("fragment wall timer lock")
                .take();
        }));
        RunningPipelineExecution {
            completion,
            fragment_ctx,
            runtime_state,
            submitted_driver_count,
            fragment_wall_timer,
            terminal_scan_ops,
        }
    }
}

/// A submitted fragment-local pipeline execution.
pub struct RunningPipelineExecution {
    completion: Arc<FragmentCompletion>,
    fragment_ctx: Arc<FragmentContext>,
    runtime_state: Arc<RuntimeState>,
    submitted_driver_count: usize,
    fragment_wall_timer: Arc<Mutex<Option<ScopedTimer>>>,
    terminal_scan_ops: Vec<Arc<dyn ScanOp>>,
}

impl RunningPipelineExecution {
    pub const fn submitted_driver_count(&self) -> usize {
        self.submitted_driver_count
    }

    /// Locally cancel this fragment and wake any blocked drivers so join can drain them.
    pub fn cancel(&self, err: impl Into<ExecutionFailure>) -> bool {
        let err = err.into();
        let won = self.completion.fail(err.clone());
        if won {
            self.fragment_ctx.set_final_status(err);
            terminate_scan_ops(&self.terminal_scan_ops);
        }
        won
    }

    pub fn fail(&self, err: impl Into<ExecutionFailure>) -> bool {
        self.cancel(err)
    }

    /// Returns the local execution conclusion as soon as it is known.
    ///
    /// An error can be visible while submitted drivers are still draining.
    pub fn conclusion(&self) -> Option<ExecutionResult<()>> {
        self.completion.conclusion()
    }

    /// Returns actual-stop proof only after every submitted driver has exited.
    pub fn stopped_fact(&self) -> Option<FragmentStoppedFact> {
        self.completion.stopped_fact()
    }

    /// Registers a one-shot callback for actual driver stop without a
    /// check/register lost-wakeup window.
    pub fn subscribe_stopped(&self, observer: impl FnOnce(FragmentStoppedFact) + Send + 'static) {
        self.completion.subscribe_stopped(Box::new(observer));
    }

    /// Drain submitted drivers and return their local terminal result.
    pub fn join(&self) -> ExecutionResult<()> {
        let timeout_error = self
            .runtime_state
            .query_options()
            .and_then(|opts| opts.query_timeout)
            .filter(|secs| *secs > 0)
            .map(|secs| format!("query timed out after {} ms", secs * 1000));
        let result = match timeout_error {
            Some(err) => {
                let timeout = Duration::from_secs(
                    self.runtime_state
                        .query_options()
                        .and_then(|opts| opts.query_timeout)
                        .expect("timeout was checked") as u64,
                );
                let fragment_ctx = Arc::clone(&self.fragment_ctx);
                let timeout_error = err.clone();
                self.completion
                    .wait_timeout_with_local_cancel(timeout, err, move || {
                        fragment_ctx.set_final_status(timeout_error);
                        terminate_scan_ops(&self.terminal_scan_ops);
                    })
            }
            None => self.completion.wait(),
        };
        if let Err(err) = &result {
            self.fragment_ctx.set_final_status(err.clone());
        }
        debug_assert!(
            self.fragment_wall_timer
                .lock()
                .expect("fragment wall timer lock")
                .is_none(),
            "stopped observer must close the fragment wall timer"
        );
        result
    }
}

/// Execute one plan fragment through pipeline runtime and return the terminal sink outcome.
#[expect(
    clippy::too_many_arguments,
    reason = "The public executor is the application boundary for independently-owned fragment services."
)]
pub fn execute_native_plan_with_pipeline(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: std::sync::Arc<RuntimeState>,
    query_id: Option<novarocks_types::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
) -> ExecutionResult<()> {
    execute_native_plan_with_pipeline_with_root_sink_dop(
        plan,
        debug,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        query_id,
        fe_addr,
        backend_num,
        None,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "Root-sink parallelism is added to the complete fragment execution contract."
)]
pub(crate) fn execute_native_plan_with_pipeline_with_root_sink_dop(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: std::sync::Arc<RuntimeState>,
    query_id: Option<novarocks_types::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
) -> ExecutionResult<()> {
    let runtime_filter_session = runtime_state.runtime_filter_session().cloned();
    execute_plan_with_pipeline(
        plan,
        debug,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        query_id,
        fe_addr,
        backend_num,
        root_sink_dop,
        runtime_filter_session,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_pipeline_execution(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    query_id: Option<novarocks_types::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
    runtime_filter_session: Option<crate::runtime_filter::RuntimeFilterSessionRef>,
    event_sink: Arc<dyn FragmentEventSink>,
) -> ExecutionResult<PreparedPipelineExecution> {
    prepare_pipeline_execution_inner(
        plan,
        debug,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        query_id,
        fe_addr,
        backend_num,
        root_sink_dop,
        runtime_filter_session,
        event_sink,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_report_neutral_pipeline_execution(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    root_sink_dop: Option<i32>,
    runtime_filter_session: Option<crate::runtime_filter::RuntimeFilterSessionRef>,
    event_sink: Arc<dyn FragmentEventSink>,
) -> ExecutionResult<PreparedPipelineExecution> {
    prepare_pipeline_execution_inner(
        plan,
        debug,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        None,
        None,
        None,
        root_sink_dop,
        runtime_filter_session,
        event_sink,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_pipeline_execution_inner(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    query_id: Option<novarocks_types::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
    runtime_filter_session: Option<crate::runtime_filter::RuntimeFilterSessionRef>,
    event_sink: Arc<dyn FragmentEventSink>,
    report_neutral: bool,
) -> ExecutionResult<PreparedPipelineExecution> {
    let dep_manager = DependencyManager::new();
    let terminal_scan_ops = scan_bindings.terminal_ops();
    // Use the FE-calculated DOP as the base graph DOP. Some terminal sinks can
    // request a narrower root pipeline when their finalization state must be local.
    let graph = build_native_pipeline_graph_for_exec_plan_with_runtime_settings(
        &plan,
        debug,
        dep_manager.clone(),
        exchange_finst_id,
        exchange_bindings,
        scan_bindings,
        pipeline_dop,
        root_sink_dop,
        runtime_filter_session,
        runtime_state
            .execution_runtime()
            .ok_or_else(|| "native pipeline execution requires an execution runtime".to_string())?
            .function_set()
            .clone(),
        runtime_state
            .execution_runtime()
            .map(|runtime| runtime.config().operator_buffer_chunks)
            .unwrap_or(1),
        runtime_state
            .execution_runtime()
            .map(|runtime| runtime.config().local_exchange_buffer_mem_limit_per_driver)
            .unwrap_or(1),
        runtime_state
            .execution_runtime()
            .map(|runtime| runtime.config().local_exchange_max_buffered_rows)
            .unwrap_or(-1),
        runtime_state.error_state(),
    )?;

    prepare_pipeline_execution_from_graph(
        graph,
        time_slice,
        sink,
        terminal_scan_ops,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        query_id,
        fe_addr,
        backend_num,
        event_sink,
        report_neutral,
    )
}

/// Prepare drivers from one frozen LocalProgramGraph and its exact Task capabilities.
/// The program profile is authoritative for graph DOP and root sink placement.
#[expect(
    clippy::too_many_arguments,
    reason = "Native runtime dependencies are explicit"
)]
pub(crate) fn prepare_report_neutral_local_program_pipeline_execution(
    program: &LocalProgramGraph,
    bindings: &LocalRuntimeBindings,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    root_sink_dop: Option<i32>,
    runtime_filter_session: Option<crate::runtime_filter::RuntimeFilterSessionRef>,
    event_sink: Arc<dyn FragmentEventSink>,
) -> ExecutionResult<PreparedPipelineExecution> {
    let profile = program.profile();
    if profile.kernel_abi() != KernelAbiVersion::CURRENT {
        return Err(format!(
            "local program kernel ABI mismatch: frozen {:?}, runtime {:?}",
            profile.kernel_abi(),
            KernelAbiVersion::CURRENT,
        )
        .into());
    }
    if usize::try_from(pipeline_dop).ok() != Some(profile.pipeline_dop().get())
        || root_sink_dop.and_then(|dop| usize::try_from(dop).ok())
            != profile.root_sink_dop().map(|dop| dop.get())
    {
        return Err(format!(
            "local program profile mismatch: requested dop={pipeline_dop} root_sink_dop={root_sink_dop:?}, frozen dop={} root_sink_dop={:?}",
            profile.pipeline_dop(),
            profile.root_sink_dop(),
        ).into());
    }
    let dep_manager = DependencyManager::new();
    let terminal_scan_ops = scan_bindings.terminal_ops();
    let execution_runtime = runtime_state.execution_runtime().ok_or_else(|| {
        "native local program execution requires an execution runtime".to_string()
    })?;
    let graph = build_native_pipeline_graph_for_local_program_with_runtime_settings(
        program,
        bindings,
        debug,
        dep_manager,
        exchange_finst_id,
        exchange_bindings,
        scan_bindings,
        pipeline_dop,
        root_sink_dop,
        runtime_filter_session,
        execution_runtime.function_set().clone(),
        runtime_state.error_state(),
        execution_runtime.config().operator_buffer_chunks,
        execution_runtime
            .config()
            .local_exchange_buffer_mem_limit_per_driver,
        execution_runtime.config().local_exchange_max_buffered_rows,
    )?;
    prepare_pipeline_execution_from_graph(
        graph,
        time_slice,
        sink,
        terminal_scan_ops,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        None,
        None,
        None,
        event_sink,
        true,
    )
}

/// Prepare drivers from one compiled LocalProgram (local-compiler output).
/// Its expressions run only through compiled roots. The program profile is
/// authoritative for graph DOP and root sink placement.
///
/// `sink` is the root sink materialized for the program's static sink, and
/// `exchange_bindings` binds exactly its compiled exchange sources, keyed by
/// receiver node. Every binding belongs to `exchange_finst_id`, the fragment
/// instance this program runs as. The program reads no scan: a compiled scan
/// runs only with its Task's bound operations, through
/// [`prepare_compiled_program_pipeline_execution_with_profiler`].
#[expect(
    clippy::too_many_arguments,
    reason = "The compiled program and its Task capabilities are independent inputs"
)]
pub(crate) fn prepare_compiled_program_pipeline_execution(
    program: Arc<novarocks_local_program::LocalProgram>,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    exchange_finst_id: Option<(i64, i64)>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    event_sink: Arc<dyn FragmentEventSink>,
) -> ExecutionResult<PreparedPipelineExecution> {
    prepare_compiled_program_pipeline_execution_with_profiler(
        program,
        time_slice,
        sink,
        exchange_bindings,
        ScanBindings::default(),
        crate::runtime::fragment::CompiledWriterBindings::default(),
        exchange_finst_id,
        None,
        pipeline_dop,
        runtime_state,
        event_sink,
    )
}

/// As [`prepare_compiled_program_pipeline_execution`], reporting into the
/// Task's profiler. `scan_bindings` binds exactly the program's compiled
/// scans, keyed by physical scan node; the prepared execution retains their
/// terminal hooks so an abort reaches parked readers. `writer_bindings` binds
/// exactly the program's compiled TableWriter and TableFinish nodes. The
/// program's runtime-filter sites bind to `runtime_state`'s runtime-filter
/// session, which every such site requires.
#[expect(
    clippy::too_many_arguments,
    reason = "The compiled program and its Task capabilities are independent inputs"
)]
pub(crate) fn prepare_compiled_program_pipeline_execution_with_profiler(
    program: Arc<novarocks_local_program::LocalProgram>,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    writer_bindings: crate::runtime::fragment::CompiledWriterBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    event_sink: Arc<dyn FragmentEventSink>,
) -> ExecutionResult<PreparedPipelineExecution> {
    prepare_compiled_program_pipeline_execution_in(
        program,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        writer_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        event_sink,
        &mut CompiledMetadataMode::<DirectCompiledSchemaMetadataScope>::Direct,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "The compiled program and its Task capabilities are independent inputs"
)]
pub(crate) fn prepare_compiled_program_pipeline_execution_with_profiler_and_metadata_host<
    H: CompiledSchemaMetadataScope,
>(
    program: Arc<novarocks_local_program::LocalProgram>,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    writer_bindings: crate::runtime::fragment::CompiledWriterBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    event_sink: Arc<dyn FragmentEventSink>,
    host: &mut H,
) -> ExecutionResult<PreparedPipelineExecution> {
    prepare_compiled_program_pipeline_execution_in(
        program,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        writer_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        event_sink,
        &mut CompiledMetadataMode::Hosted(host),
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "The compiled program and its Task capabilities are independent inputs"
)]
pub(crate) fn prepare_compiled_program_pipeline_execution_in<H: CompiledSchemaMetadataScope>(
    program: Arc<novarocks_local_program::LocalProgram>,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    writer_bindings: crate::runtime::fragment::CompiledWriterBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    event_sink: Arc<dyn FragmentEventSink>,
    metadata: &mut CompiledMetadataMode<'_, H>,
) -> ExecutionResult<PreparedPipelineExecution> {
    for node_id in exchange_bindings.node_ids() {
        let binding = exchange_bindings
            .get(node_id)
            .ok_or_else(|| format!("exchange binding for node {node_id} disappeared"))?;
        if exchange_finst_id != Some((binding.key.finst_id_hi, binding.key.finst_id_lo)) {
            return Err(format!(
                "compiled exchange binding for node {node_id} belongs to fragment instance {}, not {exchange_finst_id:?}",
                binding.key.finst_uuid(),
            )
            .into());
        }
    }
    let profile = program.graph().profile();
    if profile.kernel_abi() != KernelAbiVersion::CURRENT {
        return Err(format!(
            "compiled program kernel ABI mismatch: frozen {:?}, runtime {:?}",
            profile.kernel_abi(),
            KernelAbiVersion::CURRENT,
        )
        .into());
    }
    if usize::try_from(pipeline_dop).ok() != Some(profile.pipeline_dop().get()) {
        return Err(format!(
            "compiled program profile mismatch: requested dop={pipeline_dop}, frozen dop={}",
            profile.pipeline_dop(),
        )
        .into());
    }
    let root_sink_dop = profile
        .root_sink_dop()
        .map(|dop| i32::try_from(dop.get()))
        .transpose()
        .map_err(|_| "compiled root sink width exceeds i32".to_string())?;
    let execution_runtime = runtime_state
        .execution_runtime()
        .ok_or_else(|| "compiled program execution requires an execution runtime".to_string())?;
    let terminal_scan_ops = scan_bindings.terminal_ops();
    let graph = match metadata {
        CompiledMetadataMode::Direct => build_compiled_pipeline_graph(
            &program,
            exchange_bindings,
            scan_bindings,
            writer_bindings,
            runtime_state.runtime_filter_session().cloned(),
            DependencyManager::new(),
            pipeline_dop,
            root_sink_dop,
            execution_runtime.function_set().clone(),
            runtime_state.error_state(),
        )?,
        CompiledMetadataMode::Hosted(host) => build_compiled_pipeline_graph_with_metadata_host(
            &program,
            exchange_bindings,
            scan_bindings,
            writer_bindings,
            runtime_state.runtime_filter_session().cloned(),
            DependencyManager::new(),
            pipeline_dop,
            root_sink_dop,
            execution_runtime.function_set().clone(),
            runtime_state.error_state(),
            *host,
        )?,
    };
    prepare_pipeline_execution_from_graph(
        graph,
        time_slice,
        sink,
        terminal_scan_ops,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        None,
        None,
        None,
        event_sink,
        true,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "The graph and its Task runtime context are independent inputs"
)]
fn prepare_pipeline_execution_from_graph(
    graph: PipelineGraph,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    terminal_scan_ops: Vec<Arc<dyn ScanOp>>,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    query_id: Option<novarocks_types::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    event_sink: Arc<dyn FragmentEventSink>,
    report_neutral: bool,
) -> ExecutionResult<PreparedPipelineExecution> {
    let ctx = Arc::new(if report_neutral {
        FragmentContext::new_report_neutral(
            profiler.clone(),
            Arc::clone(&runtime_state),
            exchange_finst_id,
            event_sink,
        )
    } else {
        FragmentContext::new(
            profiler.clone(),
            Arc::clone(&runtime_state),
            exchange_finst_id,
            query_id,
            fe_addr,
            backend_num,
        )
    });
    let mut sink = Some(sink);

    // Collect all drivers
    let mut all_drivers = Vec::new();
    for pipeline_plan in graph.pipelines {
        let mut factories = pipeline_plan.factories;
        if pipeline_plan.id == graph.root_id {
            if !pipeline_plan.needs_sink {
                return Err("root pipeline missing sink requirement".to_string().into());
            }
            let root_sink = sink
                .take()
                .ok_or_else(|| "root pipeline sink already attached".to_string())?;
            factories.push(root_sink);
        } else if pipeline_plan.needs_sink {
            return Err("non-root pipeline requires sink".to_string().into());
        }

        let pipeline = Pipeline::new(pipeline_plan.id, factories, pipeline_plan.dop);
        let drivers = pipeline.instantiate_drivers(&ctx)?;
        all_drivers.extend(drivers);
    }

    if sink.is_some() {
        return Err("root pipeline sink not attached".to_string().into());
    }

    // Fixed time slice: 10ms (similar to StarRocks)
    const TIME_SLICE_MS: u64 = 10;
    let time_slice_fixed = Duration::from_millis(TIME_SLICE_MS);

    let num_threads = runtime_state
        .execution_runtime()
        .map(|runtime| runtime.config().driver_threads)
        .unwrap_or(1);

    // Use a shared global executor across fragments, following StarRocks' design.
    // When `num_threads <= 1`, keep the caller-provided time slice for backward compatibility.
    let effective_time_slice = if num_threads > 1 {
        info!(
            "Using global executor: threads={}, dop={}, time_slice={}ms",
            num_threads, pipeline_dop, TIME_SLICE_MS
        );
        time_slice_fixed
    } else {
        info!("Using global executor: threads=1, dop={}", pipeline_dop);
        time_slice
    };

    let completion = FragmentCompletion::new(all_drivers.len());
    let mut tasks = Vec::with_capacity(all_drivers.len());
    for driver in all_drivers {
        let task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            Arc::clone(&ctx),
            effective_time_slice,
        );
        tasks.push(task);
    }
    Ok(PreparedPipelineExecution {
        tasks,
        completion,
        fragment_ctx: ctx,
        runtime_state,
        fragment_profiler: profiler,
        terminal_scan_ops,
    })
}

fn terminate_scan_ops(scan_ops: &[Arc<dyn ScanOp>]) {
    for scan_op in scan_ops {
        if let Err(error) = scan_op.terminate() {
            warn!("connector scan terminal cleanup failed: {error}");
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_plan_with_pipeline(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    query_id: Option<novarocks_types::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
    runtime_filter_session: Option<crate::runtime_filter::RuntimeFilterSessionRef>,
) -> ExecutionResult<()> {
    prepare_pipeline_execution(
        plan,
        debug,
        time_slice,
        sink,
        exchange_bindings,
        scan_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        query_id,
        fe_addr,
        backend_num,
        root_sink_dop,
        runtime_filter_session,
        Arc::new(crate::runtime::fragment::io::NoopFragmentEventSink),
    )?
    .start()
    .join()
}

#[cfg(test)]
mod tests {
    use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock, mpsc};
    use std::time::{Duration, Instant};

    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature, AggregateNode};
    use crate::exec::node::analytic::{
        AnalyticNode, AnalyticOutputColumn, WindowAggregateBinding, WindowBoundary, WindowFrame,
        WindowFunctionKind, WindowFunctionSpec, WindowType,
    };
    use crate::exec::node::join::{JoinDistributionMode, JoinNode, JoinType};
    use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
    use crate::exec::pipeline::driver::PipelineDriver;
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::{DriverTask, FragmentCompletion};
    use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
    use crate::exec::pipeline::schedule::observer::Observable;

    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_state::RuntimeState;
    use crate::runtime::{ExecutionRuntime, ExecutionRuntimeConfig};
    use novarocks_types::QueryId;
    use novarocks_types::SlotId;

    use super::{
        PreparedPipelineExecution, execute_native_plan_with_pipeline, prepare_pipeline_execution,
    };

    fn test_execution_runtime() -> Arc<ExecutionRuntime> {
        static RUNTIME: OnceLock<Arc<ExecutionRuntime>> = OnceLock::new();
        Arc::clone(RUNTIME.get_or_init(|| {
            Arc::new(
                ExecutionRuntime::new(
                    ExecutionRuntimeConfig {
                        driver_threads: 1,
                        exchange_wait_ms: 120_000,
                        exchange_io_threads: 1,
                        exchange_io_max_inflight_bytes: 1024,
                        exchange_max_transmit_batched_bytes: 1024,
                        operator_buffer_chunks: 1,
                        local_exchange_buffer_mem_limit_per_driver: 1024,
                        local_exchange_max_buffered_rows: 1024,
                        runtime_filter_scan_wait_time_ms_override: None,
                        runtime_filter_wait_timeout_ms_override: None,
                        sink_io_worker_threads: 1,
                        sink_io_max_blocking_threads: 1,
                    },
                    crate::runtime::execution_runtime::test_execution_function_set(),
                    crate::runtime::execution_runtime::test_memory_authority(),
                )
                .expect("test execution runtime"),
            )
        }))
    }

    fn test_runtime_state() -> Arc<RuntimeState> {
        Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            Some(test_execution_runtime()),
        ))
    }

    struct ParkedSourceOperator {
        observable: Arc<Observable>,
        ready: Arc<AtomicBool>,
        cancel_calls: Arc<AtomicUsize>,
    }

    struct PanicOperator;

    struct ActivationProbe {
        activations: Arc<AtomicUsize>,
        cancels: Arc<AtomicUsize>,
    }

    impl Operator for ActivationProbe {
        fn name(&self) -> &str {
            "ActivationProbe"
        }

        fn activate(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            self.activations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn cancel(&mut self) {
            self.cancels.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Operator for ParkedSourceOperator {
        fn name(&self) -> &str {
            "ParkedSourceOperator"
        }

        fn cancel(&mut self) {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for ParkedSourceOperator {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            self.ready.load(Ordering::Acquire)
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> ExecutionResult<()> {
            unreachable!("never-ready source must not receive input")
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            unreachable!("never-ready source must not produce output")
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            Ok(())
        }

        fn source_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    impl Operator for PanicOperator {
        fn name(&self) -> &str {
            "PanicOperator"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for PanicOperator {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            panic!("injected pipeline panic")
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> ExecutionResult<()> {
            unreachable!("panic source must not receive input")
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            unreachable!("panic source must not produce output")
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
            Ok(())
        }
    }

    fn manually_prepared_execution(
        driver: PipelineDriver,
        runtime_state: Arc<RuntimeState>,
        query_id: Option<QueryId>,
    ) -> PreparedPipelineExecution {
        let fragment_ctx = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            None,
            query_id,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1);
        let task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            Arc::clone(&fragment_ctx),
            Duration::from_millis(10),
        );
        PreparedPipelineExecution {
            tasks: vec![task],
            completion,
            fragment_ctx,
            runtime_state,
            fragment_profiler: None,
            terminal_scan_ops: Vec::new(),
        }
    }

    struct TypedFailureSource {
        error: ExecutionFailure,
        during_activation: bool,
        failure_signals: Arc<AtomicUsize>,
    }
    impl Operator for TypedFailureSource {
        fn name(&self) -> &str {
            "TypedFailureSource"
        }
        fn activate(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
            if self.during_activation {
                Err(self.error.clone())
            } else {
                Ok(())
            }
        }
        fn on_driver_failure(&mut self) {
            self.failure_signals.fetch_add(1, Ordering::SeqCst);
        }
        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }
        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }
    impl ProcessorOperator for TypedFailureSource {
        fn need_input(&self) -> bool {
            false
        }
        fn has_output(&self) -> bool {
            true
        }
        fn push_chunk(&mut self, _: &RuntimeState, _: Chunk) -> ExecutionResult<()> {
            panic!("source must not receive input")
        }
        fn pull_chunk(&mut self, _: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
            Err(self.error.clone())
        }
        fn set_finishing(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
            Ok(())
        }
    }
    fn run_typed_failure_source(
        error: ExecutionFailure,
        during_activation: bool,
    ) -> ExecutionFailure {
        use crate::exec::operators::NoopSinkFactory;
        use crate::exec::pipeline::operator_factory::OperatorFactory;
        let runtime_state = test_runtime_state();
        let failure_signals = Arc::new(AtomicUsize::new(0));
        let driver = PipelineDriver::new(
            7,
            vec![
                Box::new(TypedFailureSource {
                    error: error.clone(),
                    during_activation,
                    failure_signals: Arc::clone(&failure_signals),
                }),
                NoopSinkFactory::new().create(1, 7),
            ],
            None,
            vec![],
            Arc::clone(&runtime_state),
            None,
        );
        let running = manually_prepared_execution(driver, Arc::clone(&runtime_state), None).start();
        let original = running.join().expect_err("the actual operator failed");
        assert_eq!(original.cause(), error.cause());
        assert_eq!(failure_signals.load(Ordering::SeqCst), 1);
        assert_eq!(running.conclusion(), Some(Err(original.clone())));
        assert_eq!(
            running
                .stopped_fact()
                .expect("all actual drivers stopped")
                .conclusion(),
            Err(original.clone())
        );
        assert_eq!(
            runtime_state.error().expect("same runtime latch").cause(),
            error.cause()
        );
        assert!(!running.fail("later identical-looking failure".to_owned()));
        assert_eq!(running.join(), Err(original.clone()));
        assert_eq!(failure_signals.load(Ordering::SeqCst), 1);
        original
    }
    #[test]
    fn typed_failure_all_kernel_causes_survive_activation_pull_completion_and_runtime_latches() {
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        let diagnostic = || KernelDiagnostic::new("ResourceExhausted: identical diagnostic text");
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(diagnostic()),
            KernelFailure::Internal(diagnostic()),
            KernelFailure::Operational(diagnostic()),
            KernelFailure::InstanceFailed,
        ] {
            for activation in [true, false] {
                let result = run_typed_failure_source(error.clone().into(), activation);
                assert_eq!(
                    std::error::Error::source(&result)
                        .unwrap()
                        .downcast_ref::<KernelFailure>(),
                    Some(&error)
                );
                if !activation {
                    assert_eq!(
                        result.context(),
                        Some(crate::runtime::fragment::ExecutionFailureContext {
                            operator_ordinal: 0,
                            operation: crate::runtime::fragment::PipelineOperation::Pull,
                        })
                    );
                }
            }
        }
    }
    #[test]
    fn typed_failure_required_root_preserves_original_sparse_row_journal_and_typed_site() {
        use crate::runtime::fragment::{ExecutionFailureCause, RequiredExpressionRowError};
        use novarocks_functions::{RowDataError, Selection};
        use novarocks_local_program::{
            ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
        };
        let site = ProgramExpressionRootSite::Node {
            node: ProgramNodeId::new(41),
            role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
        };
        let error = RowDataError::new(1, "required row error is not a successful SQL NULL");
        let selection = Selection::try_sparse(101, &[0, 50, 100]).unwrap();
        let required = RequiredExpressionRowError::try_new(site, selection, error.clone()).unwrap();
        assert_eq!(required.batch_row(), 50);
        assert_eq!(required.root(), site);
        assert_eq!(required.error(), &error);
        let failure = run_typed_failure_source(required.clone().into(), false);
        assert_eq!(
            failure.cause(),
            &ExecutionFailureCause::RequiredRow(required)
        );
        assert!(matches!(
            RequiredExpressionRowError::try_new(site, selection, RowDataError::new(3, "outside")),
            Err(novarocks_functions::KernelFailure::Internal(_))
        ));
    }

    struct PrepareFailureFactory {
        error: ExecutionFailure,
        in_bind: bool,
    }
    impl crate::exec::pipeline::operator_factory::OperatorFactory for PrepareFailureFactory {
        fn name(&self) -> &str {
            "PrepareFailureFactory"
        }
        fn is_source(&self) -> bool {
            true
        }
        fn create(&self, _: i32, _: i32) -> Box<dyn Operator> {
            Box::new(Self {
                error: self.error.clone(),
                in_bind: self.in_bind,
            })
        }
    }
    impl Operator for PrepareFailureFactory {
        fn name(&self) -> &str {
            "PrepareFailureOperator"
        }
        fn prepare(&mut self) -> ExecutionResult<()> {
            if self.in_bind {
                Ok(())
            } else {
                Err(self.error.clone())
            }
        }
        fn bind_runtime_state(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
            Err(self.error.clone())
        }
    }
    #[test]
    fn typed_failure_real_pipeline_preparation_and_binding_preserve_original_kernel_cause() {
        use crate::exec::pipeline::pipeline::Pipeline;
        for in_bind in [false, true] {
            let original: ExecutionFailure =
                novarocks_functions::KernelFailure::ResourceExhausted.into();
            let context = Arc::new(FragmentContext::new(
                None,
                test_runtime_state(),
                None,
                None,
                None,
                None,
            ));
            let pipeline = Pipeline::new(
                41,
                vec![Box::new(PrepareFailureFactory {
                    error: original.clone(),
                    in_bind,
                })],
                1,
            );
            let error = pipeline
                .instantiate_drivers(&context)
                .err()
                .expect("actual preparation failed");
            assert_eq!(error, original);
            assert_eq!(error.context(), None);
        }
    }

    fn chunk_schema_of(schema: &Arc<Schema>, slot_ids: &[SlotId]) -> ChunkSchemaRef {
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)
            .expect("chunk schema")
    }

    fn single_values_plan() -> ExecPlan {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![7]))],
        )
        .expect("values batch");
        let chunk =
            Chunk::try_new_with_chunk_schema(batch, chunk_schema_of(&schema, &[SlotId::new(1)]))
                .expect("values chunk");

        ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 1 }),
            },
        }
    }

    #[test]
    fn prepared_pipeline_defers_submission_until_start_then_joins() {
        let handle = ResultSinkHandle::new();
        let prepared = prepare_pipeline_execution(
            single_values_plan(),
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            test_runtime_state(),
            None,
            None,
            None,
            None,
            None,
            Arc::new(crate::runtime::fragment::io::NoopFragmentEventSink),
        )
        .expect("prepare pipeline without submitting drivers");

        assert_eq!(prepared.driver_count(), 1);
        assert_eq!(prepared.submitted_driver_count(), 0);
        assert!(handle.take_chunks().is_empty());

        let running = prepared.start();
        assert_eq!(running.submitted_driver_count(), 1);
        running
            .join()
            .expect("started pipeline must finish locally");

        assert_eq!(
            handle.take_chunks().iter().map(Chunk::len).sum::<usize>(),
            1
        );
    }

    #[test]
    fn cancelled_sleep_physically_joins_before_the_next_single_thread_pipeline() {
        use crate::exec::expr::LiteralValue;
        use crate::exec::expr::function::FunctionKind;
        use crate::exec::fragment::sink::FragmentSinkProgram;
        use crate::exec::node::ExternalSinkRequirement;
        use crate::exec::node::project::ProjectNode;
        use novarocks_local_program as lp;
        use std::collections::BTreeMap;
        use std::num::NonZeroUsize;

        // This dedicated executor has exactly one physical execution thread.
        let runtime = Arc::new(
            ExecutionRuntime::new(
                test_execution_runtime().config().clone(),
                crate::runtime::execution_runtime::test_execution_function_set(),
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .unwrap(),
        );
        let slot = SlotId::new(1);
        let input_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(Int64Array::from(vec![1; 4096]))],
        )
        .unwrap();
        let chunk =
            Chunk::try_new_with_chunk_schema(batch, chunk_schema_of(&input_schema, &[slot]))
                .unwrap();
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "sleep",
            DataType::Boolean,
            false,
        )]));
        let mut arena = ExprArena::default();
        let seconds = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(60)), DataType::Int64);
        let sleep = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Object("sleep"),
                args: vec![seconds],
            },
            DataType::Boolean,
        );
        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Project(ProjectNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 1 }),
                    }),
                    node_id: 2,
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: vec![sleep],
                    expr_slot_ids: vec![slot],
                    expr_slot_schemas: None,
                    output_indices: None,
                    output_chunk_schema: chunk_schema_of(&output_schema, &[slot]),
                }),
            },
        };
        let prepare = |plan: ExecPlan, schema: Arc<Schema>| {
            let layout = lp::StaticLayout::try_new_exact(
                schema,
                Arc::from([slot]),
                vec![(lp::StaticFieldSchema::new(None, vec![]), None)],
            )
            .unwrap();
            let profile = lp::CompileProfile::new(
                NonZeroUsize::new(1).unwrap(),
                None,
                layout.identity().unwrap(),
                lp::KernelAbiVersion::CURRENT,
            );
            let (program, bindings) = plan
                .into_local_program_and_bindings(
                    profile,
                    BTreeMap::new(),
                    vec![ExternalSinkRequirement::Result],
                    FragmentSinkProgram::Result.into_static().unwrap(),
                )
                .unwrap();
            let state = Arc::new(RuntimeState::new(
                None,
                None,
                None,
                None,
                None,
                None,
                Some(Arc::clone(&runtime)),
            ));
            let output = ResultSinkHandle::new();
            let prepared = super::prepare_report_neutral_local_program_pipeline_execution(
                &program,
                &bindings,
                false,
                Duration::from_millis(10),
                Box::new(ResultSinkFactory::new(output.clone())),
                ExchangeBindings::default(),
                ScanBindings::default(),
                None,
                None,
                1,
                Arc::clone(&state),
                None,
                None,
                Arc::new(crate::runtime::fragment::io::NoopFragmentEventSink),
            )
            .unwrap();
            (prepared, state, output)
        };
        let (prepared, state, output) = prepare(plan, output_schema);
        let running = prepared.start();
        let deadline = Instant::now() + Duration::from_secs(2);
        while state.error_state().waiting_count() == 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let entered_sleep = state.error_state().waiting_count() == 1;
        assert!(running.cancel("cancel actual SLEEP".to_string()));
        let (tx, rx) = mpsc::channel();
        let join = std::thread::spawn(move || tx.send(running.join()).unwrap());
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("cancel must drain the physical SLEEP driver");
        join.join().unwrap();
        assert!(
            entered_sleep,
            "the single execution thread must enter SLEEP before cancellation"
        );
        assert_eq!(result, Err("cancel actual SLEEP".into()));
        assert_eq!(state.error_state().waiting_count(), 0);
        assert!(
            output.take_chunks().is_empty(),
            "cancelled expression must publish no partial chunk"
        );

        let next = single_values_plan();
        let schema = match &next.root.kind {
            ExecNodeKind::Values(values) => values.chunk.batch.schema(),
            _ => unreachable!(),
        };
        let (prepared, _, output) = prepare(next, schema);
        let running = prepared.start();
        let (tx, rx) = mpsc::channel();
        let join = std::thread::spawn(move || tx.send(running.join()).unwrap());
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(2))
                .expect("the freed single execution thread must run the next pipeline"),
            Ok(())
        );
        join.join().unwrap();
        assert_eq!(
            output.take_chunks().iter().map(Chunk::len).sum::<usize>(),
            1
        );
    }

    #[test]
    fn failed_start_cleans_prepared_driver_without_activation() {
        let runtime_state = test_runtime_state();
        let activations = Arc::new(AtomicUsize::new(0));
        let cancels = Arc::new(AtomicUsize::new(0));
        let driver = PipelineDriver::new(
            1,
            vec![Box::new(ActivationProbe {
                activations: Arc::clone(&activations),
                cancels: Arc::clone(&cancels),
            })],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );

        let running = manually_prepared_execution(driver, runtime_state, None)
            .start_failed("injected prestart failure".to_string());
        assert_eq!(running.join(), Err("injected prestart failure".into()));
        assert_eq!(activations.load(Ordering::SeqCst), 0);
        assert_eq!(cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn running_pipeline_cancel_wakes_local_driver_and_drains() {
        let runtime_state = test_runtime_state();
        let observable = Arc::new(Observable::new());
        let ready = Arc::new(AtomicBool::new(false));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let driver = PipelineDriver::new(
            1,
            vec![Box::new(ParkedSourceOperator {
                observable: Arc::clone(&observable),
                ready,
                cancel_calls: Arc::clone(&cancel_calls),
            })],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );
        let schedule_state = driver.schedule_state();
        let running = manually_prepared_execution(driver, runtime_state, None).start();

        let deadline = Instant::now() + Duration::from_secs(1);
        while (!schedule_state.is_in_blocked() || observable.num_observers() == 0)
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            schedule_state.is_in_blocked() && observable.num_observers() > 0,
            "test driver must be genuinely parked behind its controlled source observable before cancellation"
        );

        running.cancel("local cancel".to_string());
        assert_eq!(
            running.join(),
            Err("local cancel".into()),
            "cancel must remain a fragment-local terminal result after the submitted driver drains"
        );
        assert_eq!(
            cancel_calls.load(Ordering::SeqCst),
            1,
            "an externally cancelled parked driver must cancel its operators before drop"
        );
    }

    #[test]
    fn running_pipeline_timeout_wakes_parked_driver_then_drains() {
        let runtime_state = Arc::new(RuntimeState::new(
            Some(QueryOptions {
                query_timeout: Some(1),
                ..Default::default()
            }),
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        let observable = Arc::new(Observable::new());
        let ready = Arc::new(AtomicBool::new(false));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let driver = PipelineDriver::new(
            3,
            vec![Box::new(ParkedSourceOperator {
                observable: Arc::clone(&observable),
                ready: Arc::clone(&ready),
                cancel_calls,
            })],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );
        let schedule_state = driver.schedule_state();
        let running = manually_prepared_execution(driver, Arc::clone(&runtime_state), None).start();

        let park_deadline = Instant::now() + Duration::from_secs(1);
        while (!schedule_state.is_in_blocked() || observable.num_observers() == 0)
            && Instant::now() < park_deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            schedule_state.is_in_blocked() && observable.num_observers() > 0,
            "timeout test driver must be parked before join"
        );

        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let join = std::thread::spawn(move || {
            result_tx
                .send(running.join())
                .expect("test receiver remains available");
        });
        let initial_result = result_rx.recv_timeout(Duration::from_millis(1_250));
        let returned_before_cleanup = initial_result.is_ok();

        if schedule_state.is_in_blocked() {
            runtime_state
                .error_state()
                .set_error("test cleanup after timeout".to_string());
            ready.store(true, Ordering::Release);
            let notifier = observable.defer_notify();
            notifier.arm();
        }

        let result = initial_result
            .or_else(|_| result_rx.recv_timeout(Duration::from_secs(1)))
            .expect("timeout must wake the parked driver and finish join");
        join.join().expect("join thread must not panic");

        assert!(
            returned_before_cleanup,
            "timeout must wake the parked driver without test-side recovery"
        );
        assert_eq!(result, Err("query timed out after 1000 ms".into()));
        assert!(
            !schedule_state.is_in_blocked(),
            "timeout join must return only after the parked driver drains"
        );
    }

    #[test]
    fn driver_panic_is_a_local_error_and_does_not_cancel_the_query() {
        let query_id = QueryId::new(92_001, 92_002);
        let runtime_state = Arc::new(RuntimeState::new(
            None,
            None,
            Some(query_id),
            None,
            None,
            None,
            Some(test_execution_runtime()),
        ));
        let driver = PipelineDriver::new(
            2,
            vec![Box::new(PanicOperator)],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );

        let error = manually_prepared_execution(driver, runtime_state, Some(query_id))
            .start()
            .join()
            .expect_err("driver panic must become a fragment-local error");
        assert!(
            error
                .detail()
                .contains("panic in driver execution: injected pipeline panic")
        );
        assert!(error.detail().contains("injected pipeline panic"));
    }

    #[test]
    fn group_by_sum_is_correct_with_dop_2() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let keys = Arc::new(Int32Array::from(vec![1, 1, 2, 3, 3, 3])) as arrow::array::ArrayRef;
        let vals = Arc::new(Int32Array::from(vec![10, 20, 5, 7, 8, 9])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![keys, vals]).expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Aggregate(AggregateNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    group_by: vec![k],
                    functions: vec![AggFunction {
                        name: "sum".to_string(),
                        inputs: vec![v],
                        input_is_intermediate: false,
                        types: Some(AggTypeSignature {
                            intermediate_type: None,
                            output_type: Some(DataType::Int64),
                            input_arg_type: None,
                        }),
                        ..Default::default()
                    }],
                    resolved_aggregates: vec![
                        crate::exec::expr::agg::test_builtin_execution_function_set()
                            .catalog()
                            .resolve_aggregate_trusted("sum", &[DataType::Int32])
                            .expect("resolved builtin aggregate"),
                    ],
                    need_finalize: true,
                    input_is_intermediate: false,
                    output_chunk_schema: chunk_schema_of(
                        &Arc::new(Schema::new(vec![
                            Field::new("k", DataType::Int32, false),
                            Field::new("sum", DataType::Int64, true),
                        ])),
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    runtime_filter_spec: crate::exec::node::aggregate::AggregateRuntimeFilterSpec {
                        topn_producers: Vec::new(),
                    },
                    streaming_preaggregation_mode: None,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = test_runtime_state();
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut out: HashMap<i32, i64> = HashMap::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            assert_eq!(chunk.columns().len(), 2);
            let k_col = chunk.column_by_slot_id(SlotId::new(1)).expect("k column");
            let k_arr = k_col
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("k Int32");
            let v_col = chunk.column_by_slot_id(SlotId::new(2)).expect("sum column");
            if let Some(sum_arr) = v_col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..chunk.len() {
                    out.insert(k_arr.value(i), sum_arr.value(i));
                }
            } else if let Some(sum_arr) = v_col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..chunk.len() {
                    out.insert(k_arr.value(i), sum_arr.value(i) as i64);
                }
            } else {
                panic!("unexpected sum column type: {:?}", v_col.data_type());
            }
        }

        assert_eq!(out.get(&1).copied(), Some(30));
        assert_eq!(out.get(&2).copied(), Some(5));
        assert_eq!(out.get(&3).copied(), Some(24));
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn nljoin_inner_with_conjunct_is_correct() {
        let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let left_arr = Arc::new(Int32Array::from(vec![1, 3])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_arr]).expect("left batch");

        let right_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let right_arr = Arc::new(Int32Array::from(vec![2, 4])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_arr]).expect("right batch");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let mut arena = ExprArena::default();
        let a = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let b = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let pred = arena.push_typed(ExprNode::Lt(a, b), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: NestedLoopJoinType::Inner,
                    join_conjunct: Some(pred),
                    left_chunk_schema: chunk_schema_of(&left_schema, &[SlotId::new(1)]),
                    right_chunk_schema: chunk_schema_of(&right_schema, &[SlotId::new(2)]),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = test_runtime_state();
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut pairs = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let a_arr = chunk
                .columns()
                .first()
                .expect("a column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("a Int32");
            let b_arr = chunk
                .columns()
                .get(1)
                .expect("b column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("b Int32");
            for i in 0..chunk.len() {
                pairs.push((a_arr.value(i), b_arr.value(i)));
            }
        }

        assert_eq!(pairs, vec![(1, 2), (1, 4), (3, 4)]);
    }

    #[test]
    fn nljoin_left_outer_emits_null_extended_rows() {
        let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let left_arr = Arc::new(Int32Array::from(vec![1, 3, 5])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_arr]).expect("left batch");

        let right_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let right_arr = Arc::new(Int32Array::from(vec![2, 4])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_arr]).expect("right batch");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
        ]));

        let mut arena = ExprArena::default();
        let a = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let b = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let pred = arena.push_typed(ExprNode::Lt(a, b), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: NestedLoopJoinType::LeftOuter,
                    join_conjunct: Some(pred),
                    left_chunk_schema: chunk_schema_of(&left_schema, &[SlotId::new(1)]),
                    right_chunk_schema: chunk_schema_of(&right_schema, &[SlotId::new(2)]),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = test_runtime_state();
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let a_arr = chunk
                .columns()
                .first()
                .expect("a column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("a Int32");
            let b_arr = chunk
                .columns()
                .get(1)
                .expect("b column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("b Int32");
            for i in 0..chunk.len() {
                let b = if b_arr.is_valid(i) {
                    Some(b_arr.value(i))
                } else {
                    None
                };
                rows.push((a_arr.value(i), b));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![(1, Some(2)), (1, Some(4)), (3, Some(4)), (5, None)]
        );
    }

    #[test]
    fn nljoin_full_outer_with_empty_left_emits_unmatched_build() {
        let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let left_batch = RecordBatch::new_empty(Arc::clone(&left_schema));

        let right_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let right_arr = Arc::new(Int32Array::from(vec![2, 4])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_arr]).expect("right batch");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, false),
        ]));

        let mut arena = ExprArena::default();
        let a = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let b = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let pred = arena.push_typed(ExprNode::Lt(a, b), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: NestedLoopJoinType::FullOuter,
                    join_conjunct: Some(pred),
                    left_chunk_schema: chunk_schema_of(&left_schema, &[SlotId::new(1)]),
                    right_chunk_schema: chunk_schema_of(&right_schema, &[SlotId::new(2)]),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = test_runtime_state();
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let a_arr = chunk
                .columns()
                .first()
                .expect("a column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("a Int32");
            let b_arr = chunk
                .columns()
                .get(1)
                .expect("b column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("b Int32");
            for i in 0..chunk.len() {
                let a = if a_arr.is_valid(i) {
                    Some(a_arr.value(i))
                } else {
                    None
                };
                rows.push((a, b_arr.value(i)));
            }
        }
        rows.sort();
        assert_eq!(rows, vec![(None, 2), (None, 4)]);
    }

    #[test]
    fn hash_left_outer_residual_treats_false_as_no_match() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let left_k = Arc::new(Int32Array::from(vec![1, 1, 2])) as arrow::array::ArrayRef;
        let left_v = Arc::new(Int32Array::from(vec![10, 20, 30])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_k, left_v]).expect("left");

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));
        let right_k = Arc::new(Int32Array::from(vec![1, 1, 3])) as arrow::array::ArrayRef;
        let right_w = Arc::new(Int32Array::from(vec![100, 5, 7])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_k, right_w]).expect("right");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
            Field::new("k", DataType::Int32, true),
            Field::new("w", DataType::Int32, true),
        ]));

        let mut arena = ExprArena::default();
        let key_left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let key_right = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1), SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(3), SlotId::new(4)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: JoinType::LeftOuter,
                    distribution_mode: JoinDistributionMode::Partitioned,
                    left_chunk_schema: chunk_schema_of(
                        &left_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    right_chunk_schema: chunk_schema_of(
                        &right_schema,
                        &[SlotId::new(3), SlotId::new(4)],
                    ),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[
                            SlotId::new(1),
                            SlotId::new(2),
                            SlotId::new(3),
                            SlotId::new(4),
                        ],
                    ),
                    probe_keys: vec![key_left],
                    build_keys: vec![key_right],
                    eq_null_safe: vec![false],
                    residual_predicate: Some(residual),
                    runtime_filter_execution: crate::exec::node::join::JoinRuntimeFilterExecution {
                        producers: Vec::new(),
                    },
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = test_runtime_state();
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let k1 = chunk
                .columns()
                .first()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let v = chunk
                .columns()
                .get(1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let k2 = chunk
                .columns()
                .get(2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let w = chunk
                .columns()
                .get(3)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..chunk.len() {
                let rk = if k2.is_valid(i) {
                    Some(k2.value(i))
                } else {
                    None
                };
                let rw = if w.is_valid(i) {
                    Some(w.value(i))
                } else {
                    None
                };
                rows.push((k1.value(i), v.value(i), rk, rw));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (1, 10, Some(1), Some(100)),
                (1, 20, Some(1), Some(100)),
                (2, 30, None, None)
            ]
        );
    }

    #[test]
    fn hash_right_outer_emits_unmatched_probe_rows() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let left_k = Arc::new(Int32Array::from(vec![1])) as arrow::array::ArrayRef;
        let left_v = Arc::new(Int32Array::from(vec![10])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_k, left_v]).expect("left");

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));
        let right_k = Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef;
        let right_w = Arc::new(Int32Array::from(vec![100, 200])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_k, right_w]).expect("right");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));

        let mut arena = ExprArena::default();
        let key_left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let key_right = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1), SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(3), SlotId::new(4)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: JoinType::RightOuter,
                    distribution_mode: JoinDistributionMode::Partitioned,
                    left_chunk_schema: chunk_schema_of(
                        &left_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    right_chunk_schema: chunk_schema_of(
                        &right_schema,
                        &[SlotId::new(3), SlotId::new(4)],
                    ),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[
                            SlotId::new(1),
                            SlotId::new(2),
                            SlotId::new(3),
                            SlotId::new(4),
                        ],
                    ),
                    probe_keys: vec![key_left],
                    build_keys: vec![key_right],
                    eq_null_safe: vec![false],
                    residual_predicate: None,
                    runtime_filter_execution: crate::exec::node::join::JoinRuntimeFilterExecution {
                        producers: Vec::new(),
                    },
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = test_runtime_state();
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let lk = chunk
                .columns()
                .first()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let lv = chunk
                .columns()
                .get(1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rk = chunk
                .columns()
                .get(2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rw = chunk
                .columns()
                .get(3)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..chunk.len() {
                let left = if lk.is_valid(i) && lv.is_valid(i) {
                    Some((lk.value(i), lv.value(i)))
                } else {
                    None
                };
                rows.push((left, rk.value(i), rw.value(i)));
            }
        }
        rows.sort_by_key(|r| r.1);
        assert_eq!(rows, vec![(Some((1, 10)), 1, 100), (None, 2, 200)]);
    }

    #[test]
    fn hash_full_outer_with_empty_left_emits_unmatched_build() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let left_batch = RecordBatch::new_empty(Arc::clone(&left_schema));

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));
        let right_k = Arc::new(Int32Array::from(vec![1])) as arrow::array::ArrayRef;
        let right_w = Arc::new(Int32Array::from(vec![100])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_k, right_w]).expect("right");

        let join_scope_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
            Field::new("k", DataType::Int32, false),
            Field::new("w", DataType::Int32, false),
        ]));

        let mut arena = ExprArena::default();
        let key_left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let key_right = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = left_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(1), SlotId::new(2)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: {
                                let batch = right_batch;
                                let chunk_schema =
                                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                                        batch.schema().as_ref(),
                                        &[SlotId::new(3), SlotId::new(4)],
                                    )
                                    .expect("chunk schema")
                                ;
                                Chunk::new_with_chunk_schema(batch, chunk_schema)
                            },
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: JoinType::FullOuter,
                    distribution_mode: JoinDistributionMode::Broadcast,
                    left_chunk_schema: chunk_schema_of(
                        &left_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    right_chunk_schema: chunk_schema_of(
                        &right_schema,
                        &[SlotId::new(3), SlotId::new(4)],
                    ),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_scope_schema,
                        &[
                            SlotId::new(1),
                            SlotId::new(2),
                            SlotId::new(3),
                            SlotId::new(4),
                        ],
                    ),
                    probe_keys: vec![key_left],
                    build_keys: vec![key_right],
                    eq_null_safe: vec![false],
                    residual_predicate: None,
                    runtime_filter_execution: crate::exec::node::join::JoinRuntimeFilterExecution {
                        producers: Vec::new(),
                    },
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = test_runtime_state();
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let lk = chunk
                .columns()
                .first()
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let lv = chunk
                .columns()
                .get(1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rk = chunk
                .columns()
                .get(2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rw = chunk
                .columns()
                .get(3)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..chunk.len() {
                let left_is_null = !lk.is_valid(i) && !lv.is_valid(i);
                rows.push((left_is_null, rk.value(i), rw.value(i)));
            }
        }
        rows.sort_by_key(|r| r.1);
        assert_eq!(rows, vec![(true, 1, 100)]);
    }

    #[test]
    fn analytic_row_number_rank_sum_is_correct() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("o", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let k = Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2])) as arrow::array::ArrayRef;
        let o = Arc::new(Int32Array::from(vec![1, 1, 2, 1, 2])) as arrow::array::ArrayRef;
        let v = Arc::new(Int32Array::from(vec![10, 20, 5, 7, 8])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![k, o, v]).expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };
        let analytic_output_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("o", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
            Field::new("row_number", DataType::Int64, true),
            Field::new("rank", DataType::Int64, true),
            Field::new("sum", DataType::Int64, true),
        ]));
        let analytic_output_chunk_schema = chunk_schema_of(
            &analytic_output_schema,
            &[
                SlotId::new(1),
                SlotId::new(2),
                SlotId::new(3),
                SlotId::new(4),
                SlotId::new(5),
                SlotId::new(6),
            ],
        );

        let mut arena = ExprArena::default();
        let k_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let o_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let v_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Analytic(AnalyticNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    partition_exprs: vec![k_expr],
                    order_by_exprs: vec![o_expr],
                    functions: vec![
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::RowNumber,
                            args: vec![],
                            return_type: DataType::Int64,
                            aggregate_binding: None,
                        },
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::Rank,
                            args: vec![],
                            return_type: DataType::Int64,
                            aggregate_binding: None,
                        },
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::Sum,
                            args: vec![v_expr],
                            return_type: DataType::Int64,
                            aggregate_binding: Some(WindowAggregateBinding {
                                function_name: "sum".to_string(),
                                resolved:
                                    crate::exec::expr::agg::test_builtin_execution_function_set()
                                        .catalog()
                                        .resolve_aggregate_trusted("sum", &[DataType::Int32])
                                        .expect("resolved sum window aggregate"),
                            }),
                        },
                    ],
                    window: Some(WindowFrame {
                        window_type: WindowType::Rows,
                        start: None,
                        end: Some(WindowBoundary::CurrentRow),
                    }),
                    output_columns: vec![
                        AnalyticOutputColumn::InputSlotId(SlotId::new(1)),
                        AnalyticOutputColumn::InputSlotId(SlotId::new(2)),
                        AnalyticOutputColumn::InputSlotId(SlotId::new(3)),
                        AnalyticOutputColumn::Window(0),
                        AnalyticOutputColumn::Window(1),
                        AnalyticOutputColumn::Window(2),
                    ],
                    output_chunk_schema: analytic_output_chunk_schema,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = test_runtime_state();
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let mut out_rows: Vec<(i32, i32, i32, i64, i64, i64)> = Vec::new();
        for c in handle.take_chunks() {
            if c.is_empty() {
                continue;
            }
            let cols = c.columns();
            assert_eq!(cols.len(), 6);
            let k_arr = cols[0].as_any().downcast_ref::<Int32Array>().unwrap();
            let o_arr = cols[1].as_any().downcast_ref::<Int32Array>().unwrap();
            let v_arr = cols[2].as_any().downcast_ref::<Int32Array>().unwrap();
            let rn_arr = cols[3].as_any().downcast_ref::<Int64Array>().unwrap();
            let r_arr = cols[4].as_any().downcast_ref::<Int64Array>().unwrap();
            let sum_arr = cols[5].as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..c.len() {
                out_rows.push((
                    k_arr.value(i),
                    o_arr.value(i),
                    v_arr.value(i),
                    rn_arr.value(i),
                    r_arr.value(i),
                    sum_arr.value(i),
                ));
            }
        }

        // Preserve input order within each partition.
        assert_eq!(
            out_rows,
            vec![
                (1, 1, 10, 1, 1, 10),
                (1, 1, 20, 2, 1, 30),
                (1, 2, 5, 3, 3, 35),
                (2, 1, 7, 1, 1, 7),
                (2, 2, 8, 2, 2, 15),
            ]
        );
    }

    #[test]
    fn mixed_merge_and_update_aggregates_work() {
        // An integer SUM state is its exact DECIMAL(38, 0) intermediate.
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("sum_state", DataType::Decimal128(38, 0), false),
        ]));
        let c1 = Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef;
        let sum_state = Arc::new(
            arrow::array::Decimal128Array::from(vec![30_i128, 5_i128])
                .with_precision_and_scale(38, 0)
                .expect("decimal state"),
        ) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![c1, sum_state]).expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };

        let mut arena = ExprArena::default();
        let c1_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let sum_expr = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            DataType::Decimal128(38, 0),
        );

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Aggregate(AggregateNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    group_by: vec![],
                    functions: vec![
                        AggFunction {
                            name: "count".to_string(),
                            inputs: vec![c1_expr],
                            input_is_intermediate: false,
                            types: Some(AggTypeSignature {
                                intermediate_type: None,
                                output_type: Some(DataType::Int64),
                                input_arg_type: None,
                            }),
                            ..Default::default()
                        },
                        AggFunction {
                            name: "sum".to_string(),
                            inputs: vec![sum_expr],
                            input_is_intermediate: true,
                            types: Some(AggTypeSignature {
                                intermediate_type: None,
                                output_type: Some(DataType::Int64),
                                input_arg_type: None,
                            }),
                            ..Default::default()
                        },
                    ],
                    resolved_aggregates: vec![
                        crate::exec::expr::agg::test_builtin_execution_function_set()
                            .catalog()
                            .resolve_aggregate_trusted("count", &[DataType::Int32])
                            .expect("resolved builtin aggregate"),
                        crate::exec::expr::agg::test_builtin_execution_function_set()
                            .catalog()
                            .resolve_aggregate_trusted("sum", &[DataType::Int32])
                            .expect("resolved builtin aggregate"),
                    ],
                    need_finalize: true,
                    input_is_intermediate: false,
                    output_chunk_schema: chunk_schema_of(
                        &Arc::new(Schema::new(vec![
                            Field::new("k", DataType::Int32, true),
                            Field::new("sum", DataType::Int64, true),
                        ])),
                        &[SlotId::new(3), SlotId::new(4)],
                    ),
                    runtime_filter_spec: crate::exec::node::aggregate::AggregateRuntimeFilterSpec {
                        topn_producers: Vec::new(),
                    },
                    streaming_preaggregation_mode: None,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = test_runtime_state();
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            2,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let mut out_count = None;
        let mut out_sum = None;
        for chunk in handle.take_chunks() {
            if chunk.is_empty() {
                continue;
            }
            let count_col = chunk
                .column_by_slot_id(SlotId::new(3))
                .expect("count column");
            let sum_col = chunk.column_by_slot_id(SlotId::new(4)).expect("sum column");
            let count_arr = count_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count Int64");
            let sum_arr = sum_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("sum Int64");
            out_count = Some(count_arr.value(0));
            out_sum = Some(sum_arr.value(0));
        }

        assert_eq!(out_count, Some(2));
        assert_eq!(out_sum, Some(35));
    }
}
