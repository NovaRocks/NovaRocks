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
//! - Compiled program preparation and common driver lifecycle owners.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use novarocks_local_program::KernelAbiVersion;

use std::sync::Arc;
use std::time::Duration;

use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::runtime::runtime_state::RuntimeState;
use tracing::info;

use super::builder::{
    CompiledOrderedFilterContext, PipelineGraph, build_compiled_pipeline_graph_typed,
};
use super::dependency::DependencyManager;
use super::fragment_context::FragmentContext;
use super::global_driver_executor::{
    DriverBatch, DriverTask, FragmentCompletion, FragmentStoppedFact,
};
use super::operator_factory::OperatorFactory;
use super::pipeline::Pipeline;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::fragment::io::FragmentEventSink;

use crate::runtime::profile::Profiler;

#[cfg(test)]
fn test_driver_executor() -> &'static super::global_driver_executor::GlobalDriverExecutor {
    static EXECUTOR: std::sync::OnceLock<super::global_driver_executor::GlobalDriverExecutor> =
        std::sync::OnceLock::new();
    EXECUTOR.get_or_init(|| super::global_driver_executor::GlobalDriverExecutor::new(1))
}

/// A fully materialized pipeline that has not submitted any drivers to the global executor.
pub struct PreparedPipelineExecution {
    tasks: DriverBatch,
    completion: Arc<FragmentCompletion>,
    fragment_ctx: Arc<FragmentContext>,
    runtime_state: Arc<RuntimeState>,
    fragment_profiler: Option<Profiler>,
}

impl PreparedPipelineExecution {
    pub(crate) fn failure_control(&self) -> Arc<crate::runtime::runtime_state::RuntimeErrorState> {
        self.runtime_state.error_state()
    }

    pub(crate) fn resources_finisher(
        &self,
    ) -> Arc<super::global_driver_executor::once_role::OnceRole<FragmentStoppedFact>> {
        self.completion.resources_finisher()
    }

    pub(crate) fn bind_resources_finisher(
        &self,
        slot: &mut Option<super::global_driver_executor::once_role::Callback<FragmentStoppedFact>>,
    ) -> ExecutionResult<()> {
        self.completion.bind_resources_finisher(slot)
    }

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
        } = self;
        let submitted_driver_count = tasks.len();
        let control = runtime_state.error_state();
        tasks.defer_activation();
        // The original batch was installed during preparation. Every start
        // callback borrows it, so unwind cannot discard a raw driver Vec.
        tasks.metadata(0, &control, || {
            let mut fragment_wall_timer = fragment_profiler
                .as_ref()
                .map(|p| p.scoped_timer("FragmentWallTime"));
            let mut wall_time: Option<
                super::global_driver_executor::once_role::Callback<FragmentStoppedFact>,
            > = Some(Box::new(move |_| {
                drop(fragment_wall_timer.take());
                true
            }));
            completion
                .bind_wall_time(&mut wall_time)
                .expect("one wall-time owner per prepared pipeline");
        });
        let mut start_work_exited = true;
        if let Some(error) = initial_failure {
            // Keep the actual source before the accepting call. A clone is
            // offered through the same control; no catch invents another cause.
            let proposal = tasks.install_initial_failure(error);
            let handed_off = tasks
                .metadata(1, &control, || fragment_ctx.set_final_status(proposal))
                .is_some();
            if handed_off {
                start_work_exited = tasks.dispose_initial_failure(&control);
            } else {
                // Unwind cannot distinguish a pre-handoff panic from a
                // post-handoff notification panic. Retain the original source
                // and start-work obligation rather than infer acceptance.
                start_work_exited = false;
            }
        }
        if start_work_exited {
            if tasks.len() == 0 {
                completion.activate();
            }
            let _ = if let Some(runtime) = runtime_state.execution_runtime() {
                runtime.driver_executor().submit_owned(&tasks)
            } else {
                #[cfg(test)]
                {
                    test_driver_executor().submit_owned(&tasks)
                }
                #[cfg(not(test))]
                {
                    // Missing runtime is an invariant of preparation, not a
                    // reason to drop original drivers or author a Task cause.
                    tasks.record_invariant(&control);
                    tasks.drain(false);
                    false
                }
            };
        } else {
            // Cancellation runs on the original slots; it does not activate
            // ordinary provider work or turn a failed publication into success.
            tasks.drain(false);
        }

        RunningPipelineExecution {
            completion,
            fragment_ctx,
            runtime_state,
            submitted_driver_count,
        }
    }
}

/// A submitted fragment-local pipeline execution.
pub struct RunningPipelineExecution {
    completion: Arc<FragmentCompletion>,
    fragment_ctx: Arc<FragmentContext>,
    runtime_state: Arc<RuntimeState>,
    submitted_driver_count: usize,
}

impl RunningPipelineExecution {
    pub const fn submitted_driver_count(&self) -> usize {
        self.submitted_driver_count
    }

    /// Locally cancel this fragment and wake any blocked drivers so join can drain them.
    pub fn cancel(&self, err: impl Into<ExecutionFailure>) {
        let err = err.into();
        self.fragment_ctx.set_final_status(err);
    }

    pub fn fail(&self, err: impl Into<ExecutionFailure>) {
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

    /// Drain submitted drivers and return their local terminal result.
    pub fn join(&self) -> ExecutionResult<()> {
        let timeout_error = self
            .runtime_state
            .query_options()
            .and_then(|opts| opts.query_timeout)
            .filter(|secs| *secs > 0)
            .map(|secs| {
                crate::runtime::fragment::ExecutionFailure::from(
                    super::driver_failure::DriverFailure::DeadlineExceeded(
                        novarocks_failure_contract::BoundedDetail::truncating_display(
                            format_args!("query timed out after {} ms", i64::from(secs) * 1000),
                        ),
                    ),
                )
            });
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
                    })
            }
            None => self.completion.wait(),
        };
        if let Err(err) = &result {
            self.fragment_ctx.set_final_status(err.clone());
        }
        result
    }
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
#[cfg(test)]
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
#[cfg(test)]
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
    prepare_compiled_program_pipeline_execution_with_profiler_and_schema_preparation(
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
        None,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "Program, task capabilities, and schema preparation remain independently owned inputs."
)]
#[cfg(test)]
pub(crate) fn prepare_compiled_program_pipeline_execution_with_profiler_and_schema_preparation(
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
    schema_preparation: Option<
        &mut dyn crate::runtime::preparation_metadata::CompiledSchemaPreparationScope,
    >,
) -> ExecutionResult<PreparedPipelineExecution> {
    prepare_compiled_program_with_sink_materializer(
        program,
        time_slice,
        |_| Ok(crate::runtime::fragment::sink::MaterializedCompiledSink::single(sink)),
        exchange_bindings,
        scan_bindings,
        writer_bindings,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        event_sink,
        schema_preparation,
    )
    .map_err(|error| error.cause().clone())
}

#[expect(
    clippy::too_many_arguments,
    reason = "Program, Task capabilities, and schema preparation remain independently owned inputs."
)]
// Design: ADR-0206 (docs/adr/ADR-0206-frozen-write-placement-and-task-completion-assembly.md)
pub(crate) fn prepare_compiled_program_with_sink_materializer(
    program: Arc<novarocks_local_program::LocalProgram>,
    time_slice: Duration,
    materialize: impl FnOnce(
        std::num::NonZeroUsize,
    ) -> Result<
        crate::runtime::fragment::sink::MaterializedCompiledSink,
        crate::runtime::fragment::error::FragmentLaunchError,
    >,
    exchange_bindings: ExchangeBindings,
    scan_bindings: ScanBindings,
    writer_bindings: crate::runtime::fragment::CompiledWriterBindings,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    event_sink: Arc<dyn FragmentEventSink>,
    schema_preparation: Option<
        &mut dyn crate::runtime::preparation_metadata::CompiledSchemaPreparationScope,
    >,
) -> Result<PreparedPipelineExecution, crate::runtime::fragment::error::FragmentLaunchError> {
    let mut graph = (|| -> ExecutionResult<PipelineGraph> {
    for node_id in exchange_bindings.node_ids() {
        let binding = exchange_bindings.get(node_id).ok_or_else(|| {
            crate::runtime::fragment::ExecutionFailure::from(
                crate::exec::fragment::error::FragmentBindingError::new(
                    crate::exec::fragment::error::FragmentBindingTarget::ExchangeNode(node_id),
                    crate::exec::fragment::error::FragmentBindingErrorKind::MissingAssignment,
                    format!("exchange binding for node {node_id} disappeared"),
                ),
            )
        })?;
        if exchange_finst_id != Some((binding.key.finst_id_hi, binding.key.finst_id_lo)) {
            return Err(crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "compiled exchange binding for node {node_id} belongs to fragment instance {}, not {exchange_finst_id:?}",
                    binding.key.finst_uuid(),
                ),
            ));
        }
    }
    let profile = program.graph().profile();
    if profile.kernel_abi() != KernelAbiVersion::CURRENT {
        return Err(crate::exec::fragment::error::program_binding_failure(
            crate::exec::fragment::error::FragmentBindingErrorKind::ContractVersionMismatch,
            format!(
                "compiled program kernel ABI mismatch: frozen {:?}, runtime {:?}",
                profile.kernel_abi(),
                KernelAbiVersion::CURRENT,
            ),
        ));
    }
    if usize::try_from(pipeline_dop).ok() != Some(profile.pipeline_dop().get()) {
        return Err(crate::exec::fragment::error::program_binding_failure(
            crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
            format!(
                "compiled program profile mismatch: requested dop={pipeline_dop}, frozen dop={}",
                profile.pipeline_dop(),
            ),
        ));
    }
    let root_sink_dop = profile
        .root_sink_dop()
        .map(|dop| i32::try_from(dop.get()))
        .transpose()
        .map_err(|_| {
            crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                "compiled root sink width exceeds i32",
            )
        })?;
    let execution_runtime = runtime_state
        .execution_runtime()
        .ok_or(crate::runtime::runtime_state::RuntimeBindingFailure::MissingExecutionRuntime)?;
    let ordered = CompiledOrderedFilterContext::for_program(&program, &runtime_state)?;
    let graph = build_compiled_pipeline_graph_typed(
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
        ordered.as_ref(),
        execution_runtime.config().local_exchange_buffer_mem_limit_per_driver,
        execution_runtime.config().local_exchange_max_buffered_rows,
    )?;
        Ok(graph)
    })().map_err(compiled_pipeline_launch_error)?;
    let mut roots = graph
        .pipelines
        .iter()
        .filter(|pipeline| pipeline.id == graph.root_id);
    let root = roots.next().ok_or_else(|| {
        compiled_pipeline_launch_error(crate::exec::fragment::error::program_binding_failure(
            crate::exec::fragment::error::FragmentBindingErrorKind::MissingAssignment,
            "compiled computing graph has no root pipeline",
        ))
    })?;
    let width = usize::try_from(root.dop)
        .ok()
        .and_then(std::num::NonZeroUsize::new);
    if roots.next().is_some() || !root.needs_sink || width.is_none() {
        return Err(compiled_pipeline_launch_error(
            crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                "compiled computing graph has invalid root geometry",
            ),
        ));
    }
    let sink = materialize(width.expect("positive original root width was checked"))?;
    if let Some(summary) = sink.summary {
        graph
            .append_summary_pipeline(summary.source, summary.sink)
            .map_err(compiled_pipeline_launch_error)?;
    }
    prepare_pipeline_execution_from_graph(
        graph,
        time_slice,
        sink.root,
        exchange_finst_id,
        profiler,
        pipeline_dop,
        runtime_state,
        None,
        None,
        None,
        event_sink,
        true,
        schema_preparation,
    )
    .map_err(compiled_pipeline_launch_error)
}

fn compiled_pipeline_launch_error(
    cause: ExecutionFailure,
) -> crate::runtime::fragment::error::FragmentLaunchError {
    use crate::runtime::fragment::error::{
        FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
    };
    FragmentLaunchError::from_failure(
        FragmentLaunchStage::BuildPipelines,
        FragmentLaunchErrorKind::PipelineBuild,
        cause,
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
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: Arc<RuntimeState>,
    query_id: Option<novarocks_types::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    event_sink: Arc<dyn FragmentEventSink>,
    report_neutral: bool,
    mut schema_preparation: Option<
        &mut dyn crate::runtime::preparation_metadata::CompiledSchemaPreparationScope,
    >,
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
                return Err(crate::exec::fragment::error::program_binding_failure(
                    crate::exec::fragment::error::FragmentBindingErrorKind::MissingAssignment,
                    "root pipeline missing sink requirement",
                ));
            }
            let root_sink = sink.take().ok_or_else(|| {
                crate::exec::fragment::error::program_binding_failure(
                    crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                    "root pipeline sink already attached",
                )
            })?;
            factories.push(root_sink);
        } else if pipeline_plan.needs_sink {
            return Err(crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::MissingAssignment,
                "non-root pipeline requires sink",
            ));
        }

        let pipeline = Pipeline::new(pipeline_plan.id, factories, pipeline_plan.dop);
        let drivers = match schema_preparation.as_mut() {
            Some(observer) => {
                pipeline.instantiate_drivers_with_schema_preparation(&ctx, Some(*observer))?
            }
            None => pipeline.instantiate_drivers(&ctx)?,
        };
        all_drivers.extend(drivers);
    }

    if sink.is_some() {
        return Err(crate::exec::fragment::error::program_binding_failure(
            crate::exec::fragment::error::FragmentBindingErrorKind::MissingAssignment,
            "root pipeline sink not attached",
        ));
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

    let completion =
        FragmentCompletion::new_prepared(all_drivers.len(), runtime_state.error_state());
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
        tasks: DriverBatch::new(tasks),
        completion,
        fragment_ctx: ctx,
        runtime_state,
        fragment_profiler: profiler,
    })
}

#[cfg(test)]
mod tests {
    use crate::exec::chunk::Chunk;
    use crate::exec::pipeline::operator::OperatorLifecycle;
    use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, OnceLock, mpsc};
    use std::time::{Duration, Instant};

    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
    use crate::exec::pipeline::driver::PipelineDriver;
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::{
        DriverBatch, DriverTask, FragmentCompletion,
    };
    use crate::exec::pipeline::operator::Operator;
    use crate::exec::pipeline::schedule::observer::Observable;

    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_state::RuntimeState;
    use crate::runtime::{ExecutionRuntime, ExecutionRuntimeConfig};
    use novarocks_types::QueryId;
    use novarocks_types::SlotId;

    use super::{
        PreparedPipelineExecution, prepare_compiled_program_pipeline_execution_with_profiler,
    };
    use crate::exec::pipeline::operator_factory::OperatorFactory;

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
        Arc::new(RuntimeState::for_local_execution(
            None,
            None,
            None,
            None,
            None,
            None,
            Some(test_execution_runtime()),
        ))
    }

    fn test_runtime_state_with_construction_owner(label: &'static str) -> Arc<RuntimeState> {
        Arc::new(RuntimeState::for_local_execution(
            None,
            None,
            None,
            None,
            None,
            Some(crate::runtime::mem_tracker::MemTracker::new_root(label)),
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

    impl OperatorLifecycle for ActivationProbe {
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

    impl crate::exec::pipeline::shape::SourceKernel for ActivationProbe {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            unreachable!("a rejected driver cannot run work")
        }
    }
    impl OperatorLifecycle for ParkedSourceOperator {
        fn name(&self) -> &str {
            "ParkedSourceOperator"
        }

        fn cancel(&mut self) {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl crate::exec::pipeline::shape::SourceKernel for ParkedSourceOperator {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            let generation = self.observable.generation();
            assert!(
                !self.ready.load(Ordering::Acquire),
                "never-ready source must not produce output"
            );
            Ok(crate::exec::pipeline::step::StepResult::new(
                None,
                crate::exec::pipeline::step::NextStep::Wait(
                    crate::exec::pipeline::step::WaitFact {
                        reason: crate::exec::pipeline::step::WaitReason::Input,
                        observable: self.observable.clone(),
                        observed_generation: generation,
                        deadline: None,
                    },
                ),
                0,
            ))
        }
    }

    impl OperatorLifecycle for PanicOperator {
        fn name(&self) -> &str {
            "PanicOperator"
        }
    }

    impl crate::exec::pipeline::shape::SourceKernel for PanicOperator {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            panic!("injected pipeline panic")
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
        let completion = FragmentCompletion::new_prepared(1, runtime_state.error_state());
        let task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            Arc::clone(&fragment_ctx),
            Duration::from_millis(10),
        );
        PreparedPipelineExecution {
            tasks: DriverBatch::new(vec![task]),
            completion,
            fragment_ctx,
            runtime_state,
            fragment_profiler: None,
        }
    }

    struct TypedFailureSource {
        error: ExecutionFailure,
        during_activation: bool,
        failure_signals: Arc<AtomicUsize>,
    }
    impl OperatorLifecycle for TypedFailureSource {
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
    }
    impl crate::exec::pipeline::shape::SourceKernel for TypedFailureSource {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            Err(self.error.clone())
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
                Box::new(crate::exec::pipeline::shape::Source::new(
                    TypedFailureSource {
                        error: error.clone(),
                        during_activation,
                        failure_signals: Arc::clone(&failure_signals),
                    },
                )),
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
        let previous = running.conclusion().unwrap().unwrap_err();
        running.fail(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("later identical-looking failure"),
        ));
        assert!(
            running
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&previous)
        );
        assert_eq!(running.join(), Err(original.clone()));
        assert_eq!(failure_signals.load(Ordering::SeqCst), 1);
        original
    }
    #[test]
    fn initial_failure_start_does_not_read_its_own_submission() {
        use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
        let owner = crate::runtime::task_control::test_support::DeferredOwner::new();
        let control = Arc::new(owner.view(TaskFailurePhase::Execution));
        let runtime_state = Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            Some(test_execution_runtime()),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::for_task(
                control.clone(),
            )),
        ));
        let cancels = Arc::new(AtomicUsize::new(0));
        let driver = PipelineDriver::new(
            1,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                ParkedSourceOperator {
                    observable: Arc::new(Observable::new()),
                    ready: Arc::new(AtomicBool::new(false)),
                    cancel_calls: cancels.clone(),
                },
            ))],
            None,
            Vec::new(),
            runtime_state.clone(),
            None,
        );
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        let running = manually_prepared_execution(driver, runtime_state.clone(), None)
            .start_failed(original.clone());
        assert!(
            owner.accepted().is_none(),
            "handoff returns before any adjudication"
        );
        assert!(!control.is_stopped());
        owner.advance_all();
        let deadline = Instant::now() + Duration::from_secs(2);
        while running.stopped_fact().is_none() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(
            running.stopped_fact().is_some(),
            "a normally returned handoff releases start work and permits actual driver exit"
        );
        assert!(running.join().unwrap_err().same_occurrence(&original));
        assert_eq!(cancels.load(Ordering::SeqCst), 1);
        owner.advance_all();
        assert!(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        assert!(
            runtime_state
                .error_state()
                .exit_health_reporter()
                .failure()
                .is_none()
        );
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
            Box::new(crate::exec::pipeline::shape::Source::new(Self {
                error: self.error.clone(),
                in_bind: self.in_bind,
            }))
        }
    }
    impl OperatorLifecycle for PrepareFailureFactory {
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
    impl crate::exec::pipeline::shape::SourceKernel for PrepareFailureFactory {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            panic!("preparation failure must prevent data work")
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

    fn single_values_program() -> Arc<novarocks_local_program::LocalProgram> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![7]))]).unwrap();
        let chunk = crate::exec::chunk::typed_fixture::chunk(
            batch,
            &[SlotId::new(1)],
            &[novarocks_functions::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        );
        crate::exec::pipeline::builder::values_chunk_program(
            &chunk,
            novarocks_physical_plan::FragmentSink::Result,
        )
    }

    struct ScanHolderExitState {
        claims: AtomicUsize,
        terminations: AtomicUsize,
        closed: AtomicBool,
        armed: AtomicBool,
        end_of_stream: bool,
        data_waker: std::sync::Mutex<Option<std::task::Waker>>,
        exit_waker: std::sync::Mutex<Option<std::task::Waker>>,
        drop_entered: mpsc::Sender<()>,
        drop_release: std::sync::Mutex<mpsc::Receiver<()>>,
    }
    impl ScanHolderExitState {
        fn close(&self) {
            self.closed.store(true, Ordering::Release);
            let data = self.data_waker.lock().unwrap().take();
            let exit = self.exit_waker.lock().unwrap().take();
            for waker in [data, exit].into_iter().flatten() {
                waker.wake();
            }
        }
    }

    struct ScanHolderDropRelease(Option<mpsc::Sender<()>>);
    impl ScanHolderDropRelease {
        fn release(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }
    impl Drop for ScanHolderDropRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    struct ScanHolderStaticSource {
        state: Arc<ScanHolderExitState>,
        payload: std::sync::Mutex<Option<Chunk>>,
    }
    impl crate::exec::node::scan::ScanSource for ScanHolderStaticSource {
        fn bind(
            &self,
            _: crate::exec::node::scan::BoundScanRanges,
        ) -> ExecutionResult<Arc<dyn crate::exec::node::scan::ScanOp>> {
            let payload = self
                .payload
                .lock()
                .unwrap()
                .take()
                .expect("one original bound scan owner");
            Ok(Arc::new(ScanHolderBoundOp {
                state: self.state.clone(),
                payload: Some(payload),
            }))
        }
    }

    struct ScanHolderBoundOp {
        state: Arc<ScanHolderExitState>,
        payload: Option<Chunk>,
    }
    impl Drop for ScanHolderBoundOp {
        fn drop(&mut self) {
            if self.state.armed.load(Ordering::Acquire) {
                let _ = self.state.drop_entered.send(());
                let _ = self.state.drop_release.lock().unwrap().recv();
            }
            drop(self.payload.take());
        }
    }
    impl crate::exec::node::scan::ScanOp for ScanHolderBoundOp {
        fn stream_source(&self) -> Arc<dyn crate::exec::node::scan::ScanStreamSource> {
            Arc::new(ScanHolderStreamSource(self.state.clone()))
        }
        fn terminate(&self) -> ExecutionResult<()> {
            self.state.terminations.fetch_add(1, Ordering::SeqCst);
            self.state.close();
            Ok(())
        }
    }

    struct ScanHolderStreamSource(Arc<ScanHolderExitState>);
    impl crate::exec::node::scan::ScanStreamSource for ScanHolderStreamSource {
        fn claim(
            &self,
            _: novarocks_spi::connector::read_stack::ConnectorPollBudget,
            _: Option<crate::runtime::profile::RuntimeProfile>,
            _: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
        ) -> ExecutionResult<crate::exec::node::scan::ScanOutputStream> {
            self.0.claims.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(ScanHolderMetadataStream(self.0.clone())))
        }
    }

    // This stream has no async reader job or data: its bound Op owns the real
    // Chunk. Only final Op destruction can release that actual Arrow payload.
    struct ScanHolderMetadataStream(Arc<ScanHolderExitState>);
    impl futures::Stream for ScanHolderMetadataStream {
        type Item = ExecutionResult<Chunk>;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let mut waker = self.0.data_waker.lock().unwrap();
            *waker = Some(cx.waker().clone());
            if self.0.end_of_stream || self.0.closed.load(Ordering::Acquire) {
                std::task::Poll::Ready(None)
            } else {
                std::task::Poll::Pending
            }
        }
    }
    impl crate::exec::node::scan::ScanChunkStream for ScanHolderMetadataStream {
        fn take_exit_observation(
            self: std::pin::Pin<&mut Self>,
            _: Arc<dyn crate::runtime::fragment::ExecutionFailureReporter>,
        ) -> ExecutionResult<crate::exec::node::scan::ScanExitObservation> {
            let state = self.0.clone();
            Ok(crate::exec::node::scan::ScanExitObservation {
                body_proof: crate::exec::node::scan::ScanExitBodyProof::Included,
                actual_exit: Box::pin(futures::future::poll_fn(move |cx| {
                    let mut waker = state.exit_waker.lock().unwrap();
                    *waker = Some(cx.waker().clone());
                    if state.closed.load(Ordering::Acquire) {
                        std::task::Poll::Ready(crate::exec::node::scan::ScanExitProof::Exited)
                    } else {
                        std::task::Poll::Pending
                    }
                })),
            })
        }
        fn close(
            self: std::pin::Pin<Box<Self>>,
            _: Arc<dyn crate::runtime::fragment::ExecutionFailureReporter>,
        ) -> futures::future::BoxFuture<'static, ExecutionResult<()>> {
            self.0.close();
            Box::pin(async { Ok(()) })
        }
    }

    type ScanHolderFixture = (
        Arc<novarocks_local_program::LocalProgram>,
        Arc<ScanHolderStaticSource>,
        Arc<ScanHolderExitState>,
        std::sync::Weak<dyn Array>,
        mpsc::Receiver<()>,
        ScanHolderDropRelease,
    );

    fn scan_holder_fixture(end_of_stream: bool) -> ScanHolderFixture {
        let (entered_send, entered) = mpsc::channel();
        let (release, release_recv) = mpsc::channel();
        let state = Arc::new(ScanHolderExitState {
            claims: AtomicUsize::new(0),
            terminations: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            armed: AtomicBool::new(false),
            end_of_stream,
            data_waker: std::sync::Mutex::new(None),
            exit_waker: std::sync::Mutex::new(None),
            drop_entered: entered_send,
            drop_release: std::sync::Mutex::new(release_recv),
        });
        let program = crate::runtime::fragment::scan::compiled_fixture::scan_program(1, false);
        let chunk = crate::runtime::fragment::scan::compiled_fixture::scan_chunk(
            &program,
            &[7, 11, 19],
            &[7, 11, 19],
        );
        let payload = Arc::downgrade(&chunk.columns()[0]);
        let source = Arc::new(ScanHolderStaticSource {
            state: state.clone(),
            payload: std::sync::Mutex::new(Some(chunk)),
        });
        (
            program,
            source,
            state,
            payload,
            entered,
            ScanHolderDropRelease(Some(release)),
        )
    }

    fn prepare_bound_scan_holder(
        program: Arc<novarocks_local_program::LocalProgram>,
        source: &ScanHolderStaticSource,
    ) -> PreparedPipelineExecution {
        let op = crate::exec::node::scan::ScanSource::bind(
            source,
            crate::exec::node::scan::BoundScanRanges::None,
        )
        .unwrap();
        let mut bindings = ScanBindings::default();
        bindings.insert(
            crate::runtime::fragment::scan::compiled_fixture::SCAN_NODE,
            op,
        );
        prepare_compiled_program_pipeline_execution_with_profiler(
            program,
            Duration::from_millis(10),
            Box::new(crate::exec::operators::NoopSinkFactory::new()),
            ExchangeBindings::default(),
            bindings,
            crate::runtime::fragment::CompiledWriterBindings::default(),
            None,
            None,
            1,
            test_runtime_state_with_construction_owner("scan-holder-exit"),
            Arc::new(crate::runtime::fragment::io::NoopFragmentEventSink),
        )
        .unwrap()
    }

    #[test]
    fn bound_scan_control_cancel_joins_its_final_capture_while_running_handle_survives() {
        let (plan, source, state, payload, entered, mut release) = scan_holder_fixture(false);
        let prepared = prepare_bound_scan_holder(plan, &source);
        state.armed.store(true, Ordering::Release);
        let running = prepared.start();
        let deadline = Instant::now() + Duration::from_secs(2);
        while state.claims.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let claimed_before_cancel = state.claims.load(Ordering::SeqCst);
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        running.cancel(original.clone());
        running.cancel(original.clone());
        let drop_started = entered.recv_timeout(Duration::from_secs(2)).is_ok();
        let stopped_before_drop_return = running.stopped_fact();
        let real_data_still_held = payload.upgrade().is_some();
        release.release();
        let result = running.join();
        let data_retired_before_handle_drop = payload.upgrade().is_none();
        drop(running);
        assert_eq!(claimed_before_cancel, 1);
        assert!(
            drop_started,
            "the original driver must own the final bound scan Drop"
        );
        assert!(stopped_before_drop_return.is_none());
        assert!(real_data_still_held);
        assert!(
            data_retired_before_handle_drop,
            "a retained Running handle owns no scan data"
        );
        assert_eq!(state.terminations.load(Ordering::SeqCst), 1);
        assert!(result.unwrap_err().same_occurrence(&original));
    }

    #[test]
    fn bound_scan_normal_eos_joins_its_final_capture_without_canceling_the_source() {
        let (plan, source, state, payload, entered, mut release) = scan_holder_fixture(true);
        let prepared = prepare_bound_scan_holder(plan, &source);
        state.armed.store(true, Ordering::Release);
        let running = prepared.start();
        let drop_started = entered.recv_timeout(Duration::from_secs(2)).is_ok();
        let stopped_before_drop_return = running.stopped_fact();
        let real_data_still_held = payload.upgrade().is_some();
        release.release();
        let result = running.join();
        let data_retired_before_handle_drop = payload.upgrade().is_none();
        drop(running);
        assert!(drop_started);
        assert!(stopped_before_drop_return.is_none());
        assert!(real_data_still_held);
        assert!(data_retired_before_handle_drop);
        assert_eq!(state.claims.load(Ordering::SeqCst), 1);
        assert_eq!(state.terminations.load(Ordering::SeqCst), 0);
        result.unwrap();
    }

    #[test]
    fn stopped_dormant_scan_joins_unclaimed_data_through_its_original_driver() {
        use crate::exec::fragment::program::{FragmentContractVersion, FragmentNodeId};
        use crate::runtime::fragment::{
            BackendNum, CompiledFragmentSubmission, DormantFragmentHandle,
            ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec, FragmentOutcome,
            FragmentPrepareContext, FragmentRuntimeOptions, FragmentSinkAssignment,
            ScanAssignments,
        };
        use std::collections::BTreeMap;
        use std::num::NonZeroUsize;
        let (program, source, state, payload, entered, mut release) = scan_holder_fixture(false);
        let scan = *program.scan_inputs().keys().next().unwrap();
        let physical_scan = crate::runtime::fragment::scan::compiled_fixture::SCAN_NODE;
        let sources =
            BTreeMap::from([(scan, source as Arc<dyn crate::exec::node::scan::ScanSource>)]);
        let owner = crate::runtime::task_control::test_support::Owner::new();
        let control =
            Arc::new(owner.view(novarocks_execution_contract::TaskFailurePhase::Preparation));
        let identity = control.identity();
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            identity.query_execution_id().query_id(),
            FragmentInstanceId::new(novarocks_types::UniqueId::new(98_711, 1)),
            ScanAssignments::try_new(BTreeMap::from([(
                FragmentNodeId::new(physical_scan),
                crate::exec::node::scan::BoundScanRanges::None,
            )]))
            .unwrap(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::MIN,
            BackendNum::try_new(1).unwrap(),
        );
        let submission = CompiledFragmentSubmission::try_new(program, sources, instance).unwrap();
        let context = FragmentPrepareContext::default()
            .with_execution_runtime(test_execution_runtime())
            .with_task_control(control.clone())
            .with_result_identity(identity);
        let dormant =
            crate::runtime::fragment::prepare_compiled_fragment(submission, context).unwrap();
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        control.report_failure(original.clone());
        let _ = control.accepted().unwrap();
        state.armed.store(true, Ordering::Release);
        let mut slot = Some(dormant);
        let running = DormantFragmentHandle::try_drain_stopped(&mut slot).unwrap();
        let drop_started = entered.recv_timeout(Duration::from_secs(2)).is_ok();
        let terminal_before_drop_return = running.stopped_fact();
        let real_data_still_held = payload.upgrade().is_some();
        release.release();
        let terminal = running.join();
        let data_retired_before_handle_drop = payload.upgrade().is_none();
        drop(running);
        assert!(slot.is_none());
        assert!(drop_started);
        assert!(terminal_before_drop_return.is_none());
        assert!(real_data_still_held);
        assert!(data_retired_before_handle_drop);
        assert_eq!(state.claims.load(Ordering::SeqCst), 0);
        assert_eq!(state.terminations.load(Ordering::SeqCst), 1);
        match terminal.outcome() {
            FragmentOutcome::Failed(error) => assert!(error.cause().same_occurrence(&original)),
            other => panic!("the original preparation failure must remain selected: {other:?}"),
        }
    }

    #[test]
    fn prepared_pipeline_defers_submission_until_start_then_joins() {
        let handle = ResultSinkHandle::new();
        let prepared = super::prepare_compiled_program_pipeline_execution(
            single_values_program(),
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            None,
            1,
            test_runtime_state_with_construction_owner("compiled deferred driver fixture"),
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

    // This is a test-only pipeline lifecycle stimulus, not an SQL SLEEP
    // implementation. The normal receiver owns accepted/prepared input while
    // the driver blocks in the exact RuntimeErrorState interruptible waiter.
    struct CancellableWaitFactory;
    impl OperatorFactory for CancellableWaitFactory {
        fn name(&self) -> &str {
            "CancellableWait"
        }
        fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
            Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                CancellableWait {
                    runtime_error: None,
                },
            ))
        }
    }
    struct CancellableWait {
        runtime_error: Option<Arc<crate::runtime::runtime_state::RuntimeErrorState>>,
    }
    impl OperatorLifecycle for CancellableWait {
        fn name(&self) -> &str {
            "CancellableWait"
        }
        fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
            self.runtime_error = Some(state.error_state());
            Ok(())
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for CancellableWait {}
    impl crate::exec::pipeline::shape::TransformKernel for CancellableWait {
        fn apply(
            &mut self,
            _ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            input: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            let chunk = input.into_uncovered_chunk()?;
            let runtime_error = self.runtime_error.as_ref().ok_or_else(|| {
                ExecutionFailure::from_domain(
                    crate::runtime::runtime_state::RuntimeBindingFailure::MissingExecutionRuntime,
                )
            })?;
            // Keep the original 60-second wait. Only cancellation can let this
            // fixture meet the two-second physical-join deadline below.
            runtime_error.wait_interruptibly(Duration::from_secs(60))?;
            let rows = u64::try_from(chunk.len()).expect("fixture rows fit u64");
            Ok(crate::exec::pipeline::step::StepResult::new(
                Some(chunk),
                crate::exec::pipeline::step::NextStep::NeedInput,
                rows,
            ))
        }
    }

    #[test]
    fn cancelled_sleep_physically_joins_before_the_next_single_thread_pipeline() {
        // The historical test name is retained as a tracking key. The
        // stimulus is now CancellableWait, not compiled SLEEP capability;
        // both use this same real runtime waiting/cancellation owner.
        // This dedicated executor has exactly one physical execution thread.
        let runtime = Arc::new(
            ExecutionRuntime::new(
                test_execution_runtime().config().clone(),
                crate::runtime::execution_runtime::test_execution_function_set(),
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .unwrap(),
        );
        assert_eq!(runtime.config().driver_threads, 1);
        let slot = SlotId::new(1);
        let input_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(Int64Array::from(vec![1; 4096]))],
        )
        .unwrap();
        let input_type = novarocks_functions::FunctionValueType::new(DataType::Int64, false);
        let chunk = crate::exec::chunk::typed_fixture::chunk(batch, &[slot], &[input_type]);
        let prepare = |factories: Vec<Box<dyn OperatorFactory>>| {
            let state = Arc::new(RuntimeState::new(
                None,
                None,
                None,
                None,
                None,
                Some(crate::runtime::mem_tracker::MemTracker::new_root(
                    "cancellation pipeline actual construction",
                )),
                Some(Arc::clone(&runtime)),
                Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
            ));
            let output = ResultSinkHandle::new();
            let graph = super::PipelineGraph {
                root_id: 0,
                pipelines: vec![crate::exec::pipeline::builder::PipelinePlan {
                    id: 0,
                    factories,
                    dop: 1,
                    needs_sink: true,
                }],
            };
            let prepared = super::prepare_pipeline_execution_from_graph(
                graph,
                Duration::from_millis(10),
                Box::new(ResultSinkFactory::new(output.clone())),
                None,
                None,
                1,
                Arc::clone(&state),
                None,
                None,
                None,
                Arc::new(crate::runtime::fragment::io::NoopFragmentEventSink),
                true,
                None,
            )
            .unwrap();
            (prepared, state, output)
        };
        let (prepared, state, output) = prepare(vec![
            Box::new(crate::exec::operators::ValuesSourceFactory::new(chunk, 1)),
            Box::new(CancellableWaitFactory),
        ]);
        let running = prepared.start();
        let deadline = Instant::now() + Duration::from_secs(2);
        while state.error_state().waiting_count() == 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let entered_wait = state.error_state().waiting_count() == 1;
        let original_cancel: ExecutionFailure =
            crate::runtime::fragment::FragmentCancelReason::new("cancel actual SLEEP".to_string())
                .into();
        running.cancel(original_cancel.clone());
        assert!(state.error().unwrap().same_occurrence(&original_cancel));
        let cancellation = state.error().expect("cancel installed its actual cause");
        let (tx, rx) = mpsc::channel();
        let join = std::thread::spawn(move || {
            let result = running.join();
            let stopped = running
                .stopped_fact()
                .expect("join proves physical driver exit");
            assert_eq!(stopped.conclusion(), result);
            assert_eq!(running.conclusion(), Some(result.clone()));
            tx.send(result).unwrap();
        });
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("cancel must drain the physical waiting driver");
        join.join().unwrap();
        assert!(
            entered_wait,
            "the single execution thread must enter its actual wait before cancellation"
        );
        assert!(result.as_ref().unwrap_err().same_occurrence(&cancellation));
        assert_eq!(
            result.map_err(|source| source.detail().to_owned()),
            Err("cancel actual SLEEP".to_owned())
        );
        assert_eq!(state.error_state().waiting_count(), 0);
        assert!(
            output.take_chunks().is_empty(),
            "cancelled transform must publish no partial chunk"
        );

        let next_type = novarocks_functions::FunctionValueType::new(DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![7]))],
        )
        .unwrap();
        let next = crate::exec::chunk::typed_fixture::chunk(batch, &[slot], &[next_type]);
        let (prepared, _, output) = prepare(vec![Box::new(
            crate::exec::operators::ValuesSourceFactory::new(next, 1),
        )]);
        let running = prepared.start();
        let (tx, rx) = mpsc::channel();
        let join = std::thread::spawn(move || {
            let result = running.join();
            let stopped = running
                .stopped_fact()
                .expect("join proves physical driver exit");
            assert_eq!(stopped.conclusion(), result);
            assert_eq!(running.conclusion(), Some(result.clone()));
            tx.send(result).unwrap();
        });
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
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                ActivationProbe {
                    activations: Arc::clone(&activations),
                    cancels: Arc::clone(&cancels),
                },
            ))],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );

        let running = manually_prepared_execution(driver, runtime_state, None).start_failed(
            novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new("injected prestart failure"),
            ),
        );
        assert_eq!(
            running.join().map_err(|source| source.detail().to_owned()),
            Err("injected prestart failure".to_owned())
        );
        assert_eq!(activations.load(Ordering::SeqCst), 0);
        assert_eq!(cancels.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn running_pipeline_cancel_wakes_local_driver_and_drains() {
        let runtime_state = test_runtime_state();
        let observable = Arc::new(Observable::try_bounded(1).unwrap());
        let ready = Arc::new(AtomicBool::new(false));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let driver = PipelineDriver::new(
            1,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                ParkedSourceOperator {
                    observable: Arc::clone(&observable),
                    ready,
                    cancel_calls: Arc::clone(&cancel_calls),
                },
            ))],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );
        let schedule_state = driver.schedule_state();
        let running = manually_prepared_execution(driver, runtime_state, None).start();

        let deadline = Instant::now() + Duration::from_secs(1);
        while !schedule_state.is_in_blocked() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            schedule_state.is_in_blocked(),
            "test driver must be genuinely parked before cancellation: parked={} observers={} conclusion={:?} error={:?}",
            schedule_state.is_in_blocked(),
            observable.num_observers(),
            running.conclusion(),
            running.runtime_state.error()
        );
        assert!(
            matches!(
                observable.try_subscribe(Arc::new(|| {})),
                Err(crate::runtime::observable::ObservableCapacityError::CapacityExceeded)
            ),
            "the parked driver owns the source's only scoped wake slot"
        );

        running.cancel(crate::runtime::fragment::FragmentCancelReason::new(
            "local cancel".to_string(),
        ));
        assert_eq!(
            running.join().map_err(|source| source.detail().to_owned()),
            Err("local cancel".to_owned()),
            "cancel must remain a fragment-local terminal result after the submitted driver drains"
        );
        assert_eq!(
            cancel_calls.load(Ordering::SeqCst),
            1,
            "an externally cancelled parked driver must cancel its operators before drop"
        );
        let _released = observable
            .try_subscribe(Arc::new(|| {}))
            .expect("actual driver exit releases the source slot");
    }

    #[test]
    fn running_pipeline_timeout_wakes_parked_driver_then_drains() {
        let runtime_state = Arc::new(RuntimeState::for_local_execution(
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
        let observable = Arc::new(Observable::try_bounded(1).unwrap());
        let ready = Arc::new(AtomicBool::new(false));
        let cancel_calls = Arc::new(AtomicUsize::new(0));
        let driver = PipelineDriver::new(
            3,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                ParkedSourceOperator {
                    observable: Arc::clone(&observable),
                    ready: Arc::clone(&ready),
                    cancel_calls,
                },
            ))],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );
        let schedule_state = driver.schedule_state();
        let running = manually_prepared_execution(driver, Arc::clone(&runtime_state), None).start();

        let park_deadline = Instant::now() + Duration::from_secs(1);
        while !schedule_state.is_in_blocked() && Instant::now() < park_deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            schedule_state.is_in_blocked(),
            "timeout driver must be parked before join: parked={} observers={} conclusion={:?} error={:?}",
            schedule_state.is_in_blocked(),
            observable.num_observers(),
            running.conclusion(),
            running.runtime_state.error()
        );
        assert!(
            matches!(
                observable.try_subscribe(Arc::new(|| {})),
                Err(crate::runtime::observable::ObservableCapacityError::CapacityExceeded)
            ),
            "the parked driver owns the source's only scoped wake slot"
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
                .set_error(novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new("test cleanup after timeout"),
                ));
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
        assert_eq!(
            result.map_err(|source| source.detail().to_owned()),
            Err("query timed out after 1000 ms".to_owned())
        );
        assert!(
            !schedule_state.is_in_blocked(),
            "timeout join must return only after the parked driver drains"
        );
        let _released = observable
            .try_subscribe(Arc::new(|| {}))
            .expect("timeout drain releases the source slot");
    }

    #[test]
    fn driver_panic_is_a_local_error_and_does_not_cancel_the_query() {
        let query_id = QueryId::new(92_001, 92_002);
        let runtime_state = Arc::new(RuntimeState::for_local_execution(
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
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                PanicOperator,
            ))],
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
    #[derive(Debug)]
    struct StartSourceStorage {
        bytes: Vec<u8>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for StartSourceStorage {
        fn drop(&mut self) {
            assert_eq!(self.bytes.len(), 4096);
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct StartSecondary {
        _storage: Arc<StartSourceStorage>,
    }
    struct StartPrimary {
        storage: Option<Arc<StartSourceStorage>>,
        control: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    }
    impl Drop for StartPrimary {
        fn drop(&mut self) {
            assert!(self.control.local_exit_fault().is_some());
            std::panic::panic_any(StartSecondary {
                _storage: self.storage.take().unwrap(),
            });
        }
    }
    #[derive(Debug)]
    struct StartSourceProbe {
        storage: Option<Arc<StartSourceStorage>>,
        entered: Arc<AtomicBool>,
        gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
        unknown: bool,
        control: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    }
    impl std::fmt::Display for StartSourceProbe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("test source with actual retained storage")
        }
    }
    impl std::error::Error for StartSourceProbe {}
    impl novarocks_failure_contract::FailureProjection for StartSourceProbe {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_failure_contract::FailureFact::new(
                novarocks_failure_contract::FailureClass::Unexpected,
                novarocks_failure_contract::FailureReason::new(
                    novarocks_failure_contract::domain::EXECUTION,
                    std::num::NonZeroU16::new(65000).unwrap(),
                ),
            )
        }
    }
    impl Drop for StartSourceProbe {
        fn drop(&mut self) {
            self.entered.store(true, Ordering::SeqCst);
            if self.unknown {
                std::panic::panic_any(StartPrimary {
                    storage: self.storage.take(),
                    control: self.control.clone(),
                });
            }
            let (lock, cv) = &*self.gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cv.wait(open).unwrap();
            }
            drop(open);
            drop(self.storage.take());
        }
    }
    struct StartKernelProbe {
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for StartKernelProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl OperatorLifecycle for StartKernelProbe {
        fn name(&self) -> &str {
            "start_actual_storage_probe"
        }
    }
    impl crate::exec::pipeline::shape::SourceKernel for StartKernelProbe {
        fn poll_output(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::exec::pipeline::step::StepResult::new(
                None,
                crate::exec::pipeline::step::NextStep::Finished,
                0,
            ))
        }
    }
    type StartSourceProbeFixture = (
        PreparedPipelineExecution,
        ExecutionFailure,
        Arc<crate::runtime::runtime_state::RuntimeErrorState>,
        Arc<AtomicBool>,
        Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
        std::sync::Weak<StartSourceStorage>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    );

    fn start_source_probe(unknown: bool) -> StartSourceProbeFixture {
        let runtime = Arc::new(RuntimeState::default());
        let control = runtime.error_state();
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        control.set_error(original);
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let driver = PipelineDriver::new(
            912,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                StartKernelProbe {
                    polls: polls.clone(),
                    drops: drops.clone(),
                },
            ))],
            None,
            Vec::new(),
            runtime.clone(),
            None,
        );
        let prepared = manually_prepared_execution(driver, runtime, None);
        let entered = Arc::new(AtomicBool::new(false));
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let data_drops = Arc::new(AtomicUsize::new(0));
        let storage = Arc::new(StartSourceStorage {
            bytes: vec![7; 4096],
            dropped: data_drops.clone(),
        });
        let weak = Arc::downgrade(&storage);
        let source = ExecutionFailure::from_domain(StartSourceProbe {
            storage: Some(storage),
            entered: entered.clone(),
            gate: gate.clone(),
            unknown,
            control: control.clone(),
        });
        (
            prepared, source, control, entered, gate, weak, data_drops, polls, drops,
        )
    }
    #[test]
    fn original_failure_source_drop_exits_before_prepared_driver_handoff() {
        let (prepared, source, control, entered, gate, storage, data_drops, polls, drops) =
            start_source_probe(false);
        let original = control.error().unwrap();
        let starter = std::thread::spawn(move || prepared.start_failed(source));
        let end = Instant::now() + Duration::from_secs(3);
        while !entered.load(Ordering::SeqCst) && Instant::now() < end {
            std::thread::yield_now();
        }
        let entered = entered.load(Ordering::SeqCst);
        let original_custody = storage.upgrade().is_some()
            && data_drops.load(Ordering::SeqCst) == 0
            && drops.load(Ordering::SeqCst) == 0
            && polls.load(Ordering::SeqCst) == 0;
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        let running = starter
            .join()
            .expect("real start body returned after source gate");
        let result = running.join();
        assert!(entered && original_custody);
        assert!(result.unwrap_err().same_occurrence(&original));
        assert_eq!(data_drops.load(Ordering::SeqCst), 1);
        assert!(storage.upgrade().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn unknown_original_failure_capture_does_not_mint_stopped_after_real_driver_drop() {
        let (prepared, source, control, entered, _gate, storage, data_drops, polls, drops) =
            start_source_probe(true);
        let original = control.error().unwrap();
        let running = prepared.start_failed(source);
        let end = Instant::now() + Duration::from_secs(3);
        while drops.load(Ordering::SeqCst) != 1 && Instant::now() < end {
            std::thread::yield_now();
        }
        assert!(entered.load(Ordering::SeqCst));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert!(storage.upgrade().is_some());
        assert_eq!(data_drops.load(Ordering::SeqCst), 0);
        assert!(
            running.stopped_fact().is_none(),
            "real driver receipt is retained behind source-capture exit"
        );
        assert!(control.error().unwrap().same_occurrence(&original));
        assert_eq!(
            control.local_exit_fault().unwrap().fact().reason(),
            novarocks_failure_contract::FailureReason::new(
                novarocks_failure_contract::domain::EXECUTION,
                std::num::NonZeroU16::new(617).unwrap()
            )
        );
    }
}
