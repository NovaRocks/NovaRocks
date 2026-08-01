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

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::common::app_config;
use crate::exec::node::ExecPlan;
use crate::exec::node::scan::ScanOp;
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::novarocks_logging::{info, warn};
use crate::runtime::runtime_state::RuntimeState;

use super::builder::build_native_pipeline_graph_for_exec_plan_with_root_sink_dop_and_runtime_filter_context_and_lookup_client;
use super::dependency::DependencyManager;
use super::fragment_context::FragmentContext;
use super::global_driver_executor::{DriverTask, FragmentCompletion, global_driver_executor};
use super::operator_factory::OperatorFactory;
use super::pipeline::Pipeline;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::fragment::io::{
    FragmentEventSink, FragmentLookupClient, UnavailableFragmentLookupClient,
};

use crate::runtime::profile::{Profiler, ScopedTimer};

/// A fully materialized pipeline that has not submitted any drivers to the global executor.
pub(crate) struct PreparedPipelineExecution {
    tasks: Vec<DriverTask>,
    completion: Arc<FragmentCompletion>,
    fragment_ctx: Arc<FragmentContext>,
    runtime_state: Arc<RuntimeState>,
    fragment_profiler: Option<Profiler>,
    terminal_scan_ops: Vec<Arc<dyn ScanOp>>,
}

impl PreparedPipelineExecution {
    pub(crate) fn driver_count(&self) -> usize {
        self.tasks.len()
    }

    pub(crate) const fn submitted_driver_count(&self) -> usize {
        0
    }

    /// Submit this execution exactly once by consuming its dormant task collection.
    pub(crate) fn start(self) -> RunningPipelineExecution {
        self.start_with_initial_failure(None)
    }

    /// Submit this execution with a terminal failure already latched.
    pub(crate) fn start_failed(self, error: String) -> RunningPipelineExecution {
        self.start_with_initial_failure(Some(error))
    }

    fn start_with_initial_failure(
        self,
        initial_failure: Option<String>,
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
        global_driver_executor().submit(tasks);
        RunningPipelineExecution {
            completion,
            fragment_ctx,
            runtime_state,
            submitted_driver_count,
            fragment_wall_timer: Mutex::new(fragment_wall_timer),
            terminal_scan_ops,
        }
    }
}

/// A submitted fragment-local pipeline execution.
pub(crate) struct RunningPipelineExecution {
    completion: Arc<FragmentCompletion>,
    fragment_ctx: Arc<FragmentContext>,
    runtime_state: Arc<RuntimeState>,
    submitted_driver_count: usize,
    fragment_wall_timer: Mutex<Option<ScopedTimer>>,
    terminal_scan_ops: Vec<Arc<dyn ScanOp>>,
}

impl RunningPipelineExecution {
    pub(crate) const fn submitted_driver_count(&self) -> usize {
        self.submitted_driver_count
    }

    pub(crate) fn take_connector_staged_report_frames(
        &self,
    ) -> Vec<novarocks_spi::connector::ConnectorStagedReportFrame> {
        self.runtime_state.take_connector_staged_report_frames()
    }

    /// Locally cancel this fragment and wake any blocked drivers so join can drain them.
    pub(crate) fn cancel(&self, err: String) -> bool {
        let won = self.completion.fail(err.clone());
        if won {
            self.fragment_ctx.set_final_status(err);
            terminate_scan_ops(&self.terminal_scan_ops);
        }
        won
    }

    pub(crate) fn fail(&self, err: String) -> bool {
        self.cancel(err)
    }

    /// Drain submitted drivers and return their local terminal result.
    pub(crate) fn join(&self) -> Result<(), String> {
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
        self.fragment_wall_timer
            .lock()
            .expect("fragment wall timer lock")
            .take();
        result
    }
}

/// Execute one plan fragment through pipeline runtime and return the terminal sink outcome.
pub(crate) fn execute_native_plan_with_pipeline(
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
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
) -> Result<(), String> {
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
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
) -> Result<(), String> {
    let runtime_filter_context = runtime_state.native_runtime_filter_context().cloned();
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
        runtime_filter_context,
    )
}

pub(crate) fn execute_compat_plan_with_pipeline(
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
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
) -> Result<(), String> {
    execute_compat_plan_with_pipeline_with_root_sink_dop(
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

pub(crate) fn execute_compat_plan_with_pipeline_with_root_sink_dop(
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
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
) -> Result<(), String> {
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
        None,
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
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
    runtime_filter_context: Option<
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    >,
    event_sink: Arc<dyn FragmentEventSink>,
) -> Result<PreparedPipelineExecution, String> {
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
        runtime_filter_context,
        event_sink,
        Arc::new(UnavailableFragmentLookupClient),
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_report_neutral_pipeline_execution(
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
    runtime_filter_context: Option<
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    >,
    event_sink: Arc<dyn FragmentEventSink>,
    lookup_client: Arc<dyn FragmentLookupClient>,
) -> Result<PreparedPipelineExecution, String> {
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
        runtime_filter_context,
        event_sink,
        lookup_client,
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
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
    runtime_filter_context: Option<
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    >,
    event_sink: Arc<dyn FragmentEventSink>,
    lookup_client: Arc<dyn FragmentLookupClient>,
    report_neutral: bool,
) -> Result<PreparedPipelineExecution, String> {
    let dep_manager = DependencyManager::new();
    let terminal_scan_ops = scan_bindings.terminal_ops();
    // Use the FE-calculated DOP as the base graph DOP. Some terminal sinks can
    // request a narrower root pipeline when their finalization state must be local.
    let graph =
        build_native_pipeline_graph_for_exec_plan_with_root_sink_dop_and_runtime_filter_context_and_lookup_client(
            &plan,
            debug,
            dep_manager.clone(),
            exchange_finst_id,
            exchange_bindings,
            scan_bindings,
            pipeline_dop,
            root_sink_dop,
            runtime_filter_context,
            lookup_client,
        )?;

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
                return Err("root pipeline missing sink requirement".to_string());
            }
            let root_sink = sink
                .take()
                .ok_or_else(|| "root pipeline sink already attached".to_string())?;
            factories.push(root_sink);
        } else if pipeline_plan.needs_sink {
            return Err("non-root pipeline requires sink".to_string());
        }

        let pipeline = Pipeline::new(pipeline_plan.id, factories, pipeline_plan.dop);
        let drivers = pipeline.instantiate_drivers(&ctx)?;
        all_drivers.extend(drivers);
    }

    if sink.is_some() {
        return Err("root pipeline sink not attached".to_string());
    }

    // Fixed time slice: 10ms (similar to StarRocks)
    const TIME_SLICE_MS: u64 = 10;
    let time_slice_fixed = Duration::from_millis(TIME_SLICE_MS);

    // Get executor thread count from config
    let num_threads = app_config::config()
        .ok()
        .map(|c| c.runtime.actual_exec_threads())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });

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
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<RuntimeEndpoint>,
    backend_num: Option<i32>,
    root_sink_dop: Option<i32>,
    runtime_filter_context: Option<
        crate::runtime_filter::service::NativeRuntimeFilterExecutionContext,
    >,
) -> Result<(), String> {
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
        runtime_filter_context,
        Arc::new(crate::runtime::fragment::io::NoopFragmentEventSink),
    )?
    .start()
    .join()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::{Duration, Instant};

    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature, AggregateNode};
    use crate::exec::node::analytic::{
        AnalyticNode, AnalyticOutputColumn, WindowBoundary, WindowFrame, WindowFunctionKind,
        WindowFunctionSpec, WindowType,
    };
    use crate::exec::node::join::{
        JoinDistributionMode, JoinNode, JoinRuntimeFilterExecution,
        JoinRuntimeFilterProducerBinding, JoinType,
    };
    use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
    use crate::exec::node::runtime_filter::{
        RuntimeFilterConsumerBinding, RuntimeFilterConsumerNode, RuntimeFilterExecutionContract,
        RuntimeFilterExecutionReduction,
    };
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
    use crate::exec::pipeline::driver::PipelineDriver;
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::{DriverTask, FragmentCompletion};
    use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
    use crate::exec::pipeline::schedule::observer::Observable;
    use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
    use crate::runtime::query_context::{QueryId, query_context_manager};
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
    use crate::runtime::runtime_state::RuntimeState;
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, CompletionRequirement, ConsumerActivation, ContributionKind,
    };
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;

    use super::{
        PreparedPipelineExecution, execute_native_plan_with_pipeline, prepare_pipeline_execution,
    };

    struct ParkedSourceOperator {
        observable: Arc<Observable>,
        ready: Arc<AtomicBool>,
        cancel_calls: Arc<AtomicUsize>,
    }

    struct PanicOperator;

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

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            unreachable!("never-ready source must not receive input")
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            unreachable!("never-ready source must not produce output")
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
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

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            unreachable!("panic source must not receive input")
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            unreachable!("panic source must not produce output")
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
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
            Arc::new(RuntimeState::default()),
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
    fn running_pipeline_cancel_wakes_local_driver_and_drains() {
        let runtime_state = Arc::new(RuntimeState::default());
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
            Err("local cancel".to_string()),
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
        assert_eq!(result, Err("query timed out after 1000 ms".to_string()));
        assert!(
            !schedule_state.is_in_blocked(),
            "timeout join must return only after the parked driver drains"
        );
    }

    #[test]
    fn driver_panic_is_a_local_error_and_does_not_cancel_the_query() {
        let query_id = QueryId {
            hi: 92_001,
            lo: 92_002,
        };
        let context_manager = query_context_manager();
        context_manager
            .ensure_native_context(
                query_id,
                false,
                Duration::from_secs(1),
                Duration::from_secs(5),
            )
            .expect("create independent query context");
        let runtime_state = Arc::new(RuntimeState::new(
            None,
            None,
            Some(query_id),
            None,
            None,
            None,
            None,
            None,
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
        assert!(error.contains("panic in driver execution: injected pipeline panic"));
        assert!(
            !context_manager.is_query_canceled(query_id),
            "pipeline completion must not cancel an independently-owned query"
        );
        context_manager.cancel_query(query_id, "test cleanup".to_string());
    }

    #[test]
    fn dormant_native_filter_fails_open_for_local_shard_missing_key() {
        let query_id = QueryId { hi: 80_007, lo: 29 };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let lifecycle = RuntimeFilterLifecycleRegistry::global();
        lifecycle.remove_query(query_key);
        let context_manager = query_context_manager();
        let deployment_lifecycle = RuntimeFilterQueryLifecycleOptions {
            delivery_expire: Duration::from_secs(1),
            query_expire: Duration::from_secs(5),
            transport_retry_interval: Duration::from_millis(200),
            transport_max_attempts: 3,
            transport_deadline: Duration::from_secs(5),
            transport_max_pending_entries: 128,
            transport_max_pending_bytes: 1024 * 1024,
        };
        context_manager
            .ensure_native_context(
                query_id,
                false,
                deployment_lifecycle.delivery_expire,
                deployment_lifecycle.query_expire,
            )
            .expect("create native query context");
        context_manager
            .install_runtime_filter_deployment(
                query_id,
                deployment_lifecycle,
                crate::runtime::query_context::runtime_filter_service_lifecycle_tests::participant_install_with_expected_producer_instances(
                    BTreeSet::from([
                        UniqueId { hi: 70, lo: 30 },
                        UniqueId { hi: 70, lo: 31 },
                    ]),
                ),
            )
            .expect("install query-owned runtime-filter Service");
        for _ in 0..2 {
            context_manager
                .get_or_register_native(
                    query_id,
                    false,
                    Duration::from_secs(1),
                    Duration::from_secs(5),
                )
                .expect("register shared NativeService query context");
        }
        let initial_lifecycle = lifecycle
            .snapshot(query_key)
            .expect("query context installs a lifecycle event sink");
        let installed_filter_count = initial_lifecycle.filters.len();
        let installed_channel_event_count = initial_lifecycle.channel_events.len();

        let full_build_domain = BTreeSet::from([11_i64, 29]);
        let local_producer_domain = BTreeSet::from([11_i64]);
        let consumer_input = vec![11_i64, 29];
        assert!(full_build_domain.contains(&29));
        assert!(!local_producer_domain.contains(&29));
        assert_eq!(
            consumer_input
                .iter()
                .copied()
                .filter(|key| local_producer_domain.contains(key))
                .collect::<Vec<_>>(),
            vec![11],
            "a local-only artifact would incorrectly reject a valid remote-shard match"
        );

        let probe_schema = Arc::new(Schema::new(vec![Field::new(
            "probe_key",
            DataType::Int64,
            false,
        )]));
        let build_schema = Arc::new(Schema::new(vec![Field::new(
            "build_key",
            DataType::Int64,
            false,
        )]));
        let join_schema = Arc::new(Schema::new(vec![
            Field::new("probe_key", DataType::Int64, false),
            Field::new("build_key", DataType::Int64, false),
        ]));
        let probe_batch = RecordBatch::try_new(
            Arc::clone(&probe_schema),
            vec![Arc::new(Int64Array::from(vec![11]))],
        )
        .expect("probe batch");
        let build_batch = RecordBatch::try_new(
            Arc::clone(&build_schema),
            vec![Arc::new(Int64Array::from(vec![11]))],
        )
        .expect("local build batch");
        let mut producer_arena = ExprArena::default();
        let probe_expr =
            producer_arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let build_expr =
            producer_arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);
        let membership_schema = ArtifactMembershipSchema::new(
            &DataType::Int64,
            crate::runtime_filter::model::contract::NullSemantics::NeverMatches,
        )
        .expect("membership schema");
        let contract = RuntimeFilterExecutionContract::Membership {
            canonical_schema: Arc::from(membership_schema.canonical_bytes()),
            schema_digest: membership_schema.digest().bytes(),
        };
        let producer_plan = ExecPlan {
            arena: producer_arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::try_new_with_chunk_schema(
                                probe_batch,
                                chunk_schema_of(&probe_schema, &[SlotId::new(1)]),
                            )
                            .expect("probe chunk"),
                            node_id: 1,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::try_new_with_chunk_schema(
                                build_batch,
                                chunk_schema_of(&build_schema, &[SlotId::new(2)]),
                            )
                            .expect("build chunk"),
                            node_id: 2,
                        }),
                    }),
                    node_id: 3,
                    join_type: JoinType::Inner,
                    distribution_mode: JoinDistributionMode::Partitioned,
                    left_chunk_schema: chunk_schema_of(&probe_schema, &[SlotId::new(1)]),
                    right_chunk_schema: chunk_schema_of(&build_schema, &[SlotId::new(2)]),
                    join_scope_chunk_schema: chunk_schema_of(
                        &join_schema,
                        &[SlotId::new(1), SlotId::new(2)],
                    ),
                    probe_keys: vec![probe_expr],
                    build_keys: vec![build_expr],
                    eq_null_safe: vec![false],
                    residual_predicate: None,
                    runtime_filter_execution: JoinRuntimeFilterExecution {
                        producers: vec![JoinRuntimeFilterProducerBinding {
                            binding_id: 3,
                            channel_id: 1,
                            build_expr_id: build_expr,
                            build_key_index: 0,
                            contribution_kinds: BTreeSet::from([
                                ContributionKind::ValueDomainDelta,
                                ContributionKind::ProducerClosed,
                            ]),
                            completion_requirement: CompletionRequirement::ProducerClosed,
                            contract: contract.clone(),
                            reduction: RuntimeFilterExecutionReduction::SetUnion,
                        }],
                    },
                }),
            },
        };
        let producer_handle = ResultSinkHandle::new();
        execute_native_plan_with_pipeline(
            producer_plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(producer_handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            Arc::new(
                RuntimeState::default().with_native_runtime_filter_context(Some(
                    context_manager
                        .runtime_filter_context_for_native_execution(
                            query_id,
                            UniqueId { hi: 70, lo: 30 },
                        )
                        .expect("producer runtime-filter context"),
                )),
            ),
            Some(query_id),
            None,
            None,
        )
        .expect("execute dormant producer fragment");
        assert_eq!(
            producer_handle
                .take_chunks()
                .iter()
                .map(Chunk::len)
                .sum::<usize>(),
            1
        );
        let producer_lifecycle = lifecycle
            .snapshot(query_key)
            .expect("producer shares the query lifecycle event sink");
        assert_eq!(producer_lifecycle.filters.len(), installed_filter_count);
        assert!(
            producer_lifecycle.channel_events.len() >= installed_channel_event_count,
            "producer activity may add structured channel events without creating legacy records"
        );

        let consumer_schema = Arc::new(Schema::new(vec![Field::new(
            "consumer_key",
            DataType::Int64,
            false,
        )]));
        let consumer_batch = RecordBatch::try_new(
            Arc::clone(&consumer_schema),
            vec![Arc::new(Int64Array::from(consumer_input.clone()))],
        )
        .expect("consumer batch");
        let mut consumer_arena = ExprArena::default();
        let consumer_expr =
            consumer_arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int64);
        let consumer_plan = ExecPlan {
            arena: consumer_arena,
            root: ExecNode {
                kind: ExecNodeKind::RuntimeFilterConsumer(RuntimeFilterConsumerNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::try_new_with_chunk_schema(
                                consumer_batch,
                                chunk_schema_of(&consumer_schema, &[SlotId::new(4)]),
                            )
                            .expect("consumer chunk"),
                            node_id: 4,
                        }),
                    }),
                    owner_node_id: 4,
                    bindings: vec![RuntimeFilterConsumerBinding {
                        binding_id: 4,
                        channel_id: 1,
                        expr_id: consumer_expr,
                        activation: ConsumerActivation::BlockingSnapshot,
                        capabilities: BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                        contract,
                        reduction: RuntimeFilterExecutionReduction::SetUnion,
                    }],
                }),
            },
        };
        let consumer_handle = ResultSinkHandle::new();
        execute_native_plan_with_pipeline(
            consumer_plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(consumer_handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            1,
            Arc::new(
                RuntimeState::default().with_native_runtime_filter_context(Some(
                    context_manager
                        .runtime_filter_context_for_native_execution(
                            query_id,
                            UniqueId { hi: 70, lo: 40 },
                        )
                        .expect("consumer runtime-filter context"),
                )),
            ),
            Some(query_id),
            None,
            None,
        )
        .expect("execute dormant consumer fragment");

        let output = consumer_handle
            .take_chunks()
            .into_iter()
            .flat_map(|chunk| {
                chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("consumer output Int64")
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(output, consumer_input);
        let consumer_lifecycle = lifecycle
            .snapshot(query_key)
            .expect("consumer shares the query lifecycle event sink");
        assert_eq!(consumer_lifecycle.filters.len(), installed_filter_count);
        assert!(
            consumer_lifecycle.channel_events.len() >= installed_channel_event_count,
            "consumer fail-open may add structured channel events without creating legacy records"
        );
        context_manager.cancel_query(query_id, "test cleanup".to_string());
        context_manager.finish_fragment(query_id);
        context_manager.finish_fragment(query_id);
        lifecycle.remove_query(query_key);
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
        let runtime_state = Arc::new(RuntimeState::default());
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
        let runtime_state = Arc::new(RuntimeState::default());
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
        let runtime_state = Arc::new(RuntimeState::default());
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
        let runtime_state = Arc::new(RuntimeState::default());
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
        let runtime_state = Arc::new(RuntimeState::default());
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
        let runtime_state = Arc::new(RuntimeState::default());
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
        let runtime_state = Arc::new(RuntimeState::default());
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
                        },
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::Rank,
                            args: vec![],
                            return_type: DataType::Int64,
                        },
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::Sum,
                            args: vec![v_expr],
                            return_type: DataType::Int64,
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
        let runtime_state = Arc::new(RuntimeState::default());
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("sum_state", DataType::Int64, false),
        ]));
        let c1 = Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef;
        let sum_state = Arc::new(Int64Array::from(vec![30_i64, 5_i64])) as arrow::array::ArrayRef;
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
        let sum_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);

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
        let runtime_state = Arc::new(RuntimeState::default());
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
