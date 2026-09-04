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

//! Reproducible NCP-8 composite write microbenchmark.
//!
//! This is a measurement harness, not a latency gate. It drives the public
//! `TableWriter` and `TableFinish` factories with the same input and provider
//! fixture. The two cases differ only in whether the frozen auxiliary plan is
//! empty or contains the Iceberg-owned Theta aggregate.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, BinaryArray, Int8Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_connector_iceberg_functions::{
    ICEBERG_THETA_AGGREGATE_NAME, iceberg_theta_registration, validate_compact_theta,
};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema};
use novarocks_execution::exec::expr::agg::{
    ExecutionFunctionSetBuilder, SealedExecutionFunctionSet,
    contribute_builtin_aggregate_implementations,
};
use novarocks_execution::exec::expr::{ExprArena, ExprNode, LiteralValue};
use novarocks_execution::exec::node::table_finish::TableFinishNode;
use novarocks_execution::exec::node::table_write_aggregate::{
    WriterFinalAggregateCall, WriterFinalAggregatePlan, WriterGroupedUnpivotMapping,
    WriterGroupedUnpivotPlan, WriterPartialAggregateCall, WriterPartialAggregatePlan,
};
use novarocks_execution::exec::node::table_write_relation::{
    ConnectorCommitFragmentCarrierValidator, ConnectorCommitFragmentEncoder,
    RootWriteResultRelationSchema, WRITE_RELATION_TARGET_SLOT, WriterMultiplexRelationSchema,
};
use novarocks_execution::exec::node::table_writer::{
    TableWriterInputProjection, TableWriterNode, TableWriterPhysicalContextTemplate,
};
use novarocks_execution::exec::node::unpivot::UnpivotConstant;
use novarocks_execution::exec::node::values::ValuesNode;
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_execution::exec::operators::{
    TableFinishOperatorFactory, TableWriterOperatorFactory,
};
use novarocks_execution::exec::pipeline::operator::Operator;
use novarocks_execution::exec::pipeline::operator_factory::OperatorFactory;
use novarocks_execution::runtime::execution_runtime::ExecutionSpillStorageConfig;
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime::profile::{OperatorProfiles, RuntimeProfile};
use novarocks_execution::runtime::runtime_state::RuntimeState;
use novarocks_execution::runtime::{ExecutionRuntime, ExecutionRuntimeConfig};
use novarocks_functions::ResolvedAggregateSignature;
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution, ProviderWriteRuntime, ROOT_WRITE_RESULT_BLOB_TYPE_INDEX,
    ROOT_WRITE_RESULT_BODY_INDEX, ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
    ROOT_WRITE_RESULT_KIND_INDEX, ROOT_WRITE_RESULT_PROPERTIES_INDEX,
    ROOT_WRITE_RESULT_ROW_COUNT_INDEX, ROOT_WRITE_RESULT_TARGET_INDEX, RootRowKind,
    RootWriteResultSchema, WriteRuntimeAdapter, WriteTargetOrdinal, WriterAuxiliaryChannel,
    WriterMultiplexSchema, root_write_result_column_id,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCancellation, ConnectorError,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId, ConnectorRequestContext,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use novarocks_types::SlotId;
use serde_json::{Value, json};

const INPUT_SLOT: SlotId = SlotId::new(1);
const THETA_PARTIAL_SLOT: SlotId = SlotId::new(10_000);
const THETA_FINAL_SLOT: SlotId = SlotId::new(10_001);
const GROUPING_OUTPUT_SLOT: SlotId = SlotId::new(10_002);
const POLL_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug)]
enum AuxiliaryCase {
    Empty,
    Theta,
}

impl AuxiliaryCase {
    const ALL: [Self; 2] = [Self::Empty, Self::Theta];

    const fn name(self) -> &'static str {
        match self {
            Self::Empty => "empty_auxiliary",
            Self::Theta => "theta_auxiliary",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    rows: usize,
    batch_rows: usize,
    cardinality: usize,
    warmup_rounds: usize,
    measurement_rounds: usize,
    append_delay_us: u64,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let rows = env_usize("NOVAROCKS_BENCH_ROWS", 262_144)?;
        let batch_rows = env_usize("NOVAROCKS_BENCH_BATCH_ROWS", 4_096)?;
        let cardinality = env_usize("NOVAROCKS_BENCH_CARDINALITY", rows.min(100_000))?;
        let warmup_rounds = env_usize("NOVAROCKS_BENCH_WARMUP_ROUNDS", 2)?;
        let measurement_rounds = env_usize("NOVAROCKS_BENCH_MEASUREMENT_ROUNDS", 5)?;
        let append_delay_us = env_u64("NOVAROCKS_BENCH_APPEND_DELAY_US", 0)?;
        if rows == 0 || batch_rows == 0 || cardinality == 0 || measurement_rounds == 0 {
            return Err(
                "rows, batch rows, cardinality, and measurement rounds must be positive"
                    .to_string(),
            );
        }
        if cardinality > i32::MAX as usize {
            return Err("cardinality must fit i32".to_string());
        }
        Ok(Self {
            rows,
            batch_rows,
            cardinality,
            warmup_rounds,
            measurement_rounds,
            append_delay_us,
        })
    }
}

struct BenchProvider {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
}

impl BenchProvider {
    fn new() -> Arc<Self> {
        let instance_id =
            ConnectorInstanceId::parse("ncp8_benchmark").expect("benchmark instance id");
        Arc::new(Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("benchmark").expect("provider id"),
                instance_id: instance_id.clone(),
            },
            catalog_handle: CatalogHandle::new(instance_id, CatalogVersion::from_bytes([8; 32])),
        })
    }
}

impl ProviderWriteRuntime for BenchProvider {
    type CommitHandle = ();
    type WriterHandle = BenchWriterRecipe;
    type CommitFragment = BenchFragment;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

#[derive(Clone, Debug)]
struct BenchWriterRecipe;

#[derive(Debug)]
struct BenchFragment(Vec<u8>);

#[derive(Default)]
struct ProviderCounters {
    open_calls: AtomicUsize,
    append_calls: AtomicUsize,
    appended_rows: AtomicUsize,
    finish_calls: AtomicUsize,
    abort_calls: AtomicUsize,
}

struct BenchWriteExecution {
    adapter: WriteRuntimeAdapter<BenchProvider>,
    counters: Arc<ProviderCounters>,
    append_delay_us: u64,
}

#[async_trait::async_trait]
impl ConnectorWriteExecution for BenchWriteExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        self.adapter.binding().catalog_handle()
    }

    async fn open_writer(
        &self,
        _request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        self.counters.open_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(BenchBatchWriter {
            adapter: self.adapter.clone(),
            counters: Arc::clone(&self.counters),
            append_delay_us: self.append_delay_us,
            rows: 0,
        }))
    }
}

struct BenchBatchWriter {
    adapter: WriteRuntimeAdapter<BenchProvider>,
    counters: Arc<ProviderCounters>,
    append_delay_us: u64,
    rows: usize,
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for BenchBatchWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        if self.append_delay_us != 0 {
            tokio::time::sleep(Duration::from_micros(self.append_delay_us)).await;
        }
        self.counters.append_calls.fetch_add(1, Ordering::Relaxed);
        self.counters
            .appended_rows
            .fetch_add(batch.num_rows(), Ordering::Relaxed);
        self.rows += batch.num_rows();
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        self.counters.finish_calls.fetch_add(1, Ordering::Relaxed);
        let bytes = u64::try_from(self.rows)
            .unwrap_or(u64::MAX)
            .to_le_bytes()
            .to_vec();
        Ok(vec![
            self.adapter.wrap_commit_fragment(BenchFragment(bytes)),
        ])
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        self.counters.abort_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct BenchFragmentEncoder(WriteRuntimeAdapter<BenchProvider>);

impl ConnectorCommitFragmentEncoder for BenchFragmentEncoder {
    fn encode(
        &self,
        _target: WriteTargetOrdinal,
        fragment: &ConnectorCommitFragment,
    ) -> Result<Vec<u8>, ConnectorError> {
        Ok(self.0.commit_fragment(fragment)?.0.clone())
    }
}

struct AcceptCarrier;

impl ConnectorCommitFragmentCarrierValidator for AcceptCarrier {
    fn validate(&self, _target: WriteTargetOrdinal, _encoded: &[u8]) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[derive(Default)]
struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

struct BuiltPlans {
    writer: TableWriterOperatorFactory,
    finish: TableFinishOperatorFactory,
}

struct RunResult {
    value: Value,
    theta_body: Option<Vec<u8>>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("table_write_statistics benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = Config::from_env()?;
    let function_set = build_function_set()?;
    let runtime = build_runtime(Arc::clone(&function_set))?;
    let inputs = build_inputs(config)?;

    emit(json!({
        "record": "config",
        "schema_version": 1,
        "package_version": env!("CARGO_PKG_VERSION"),
        "rows": config.rows,
        "batch_rows": config.batch_rows,
        "input_batches": inputs.len(),
        "cardinality": config.cardinality,
        "warmup_rounds": config.warmup_rounds,
        "measurement_rounds": config.measurement_rounds,
        "append_delay_us": config.append_delay_us,
        "available_parallelism": std::thread::available_parallelism().map(usize::from).ok(),
        "cpu_clock": "getrusage_process_user_plus_system",
        "peak_memory": "query_mem_tracker_accounted_bytes",
        "production_object_read_calls": null,
        "production_object_read_calls_note": "not observable through the execution write API",
    }));

    for case in AuxiliaryCase::ALL {
        for iteration in 0..config.warmup_rounds {
            let result = run_once(
                case,
                "warmup",
                iteration,
                config,
                &inputs,
                Arc::clone(&function_set),
                Arc::clone(&runtime),
            )?;
            validate_theta(case, result.theta_body.as_deref())?;
            emit(result.value);
        }
    }

    let mut samples = Vec::new();
    for iteration in 0..config.measurement_rounds {
        for case in AuxiliaryCase::ALL {
            let result = run_once(
                case,
                "measurement",
                iteration,
                config,
                &inputs,
                Arc::clone(&function_set),
                Arc::clone(&runtime),
            )?;
            validate_theta(case, result.theta_body.as_deref())?;
            emit(result.value.clone());
            samples.push(result.value);
        }
    }
    for case in AuxiliaryCase::ALL {
        emit(summarize(case, &samples)?);
    }
    Ok(())
}

fn build_function_set() -> Result<Arc<SealedExecutionFunctionSet>, String> {
    let mut builder = ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
        .map_err(|error| format!("register built-in function metadata: {error}"))?;
    contribute_builtin_aggregate_implementations(&mut builder)
        .map_err(|error| format!("register built-in aggregate implementations: {error}"))?;
    builder
        .register_typed_aggregate(
            iceberg_theta_registration()
                .map_err(|error| format!("build Iceberg Theta registration: {error}"))?,
        )
        .map_err(|error| format!("register Iceberg Theta aggregate: {error}"))?;
    builder
        .seal()
        .map(Arc::new)
        .map_err(|error| format!("seal benchmark function set: {error}"))
}

fn build_runtime(
    function_set: Arc<SealedExecutionFunctionSet>,
) -> Result<Arc<ExecutionRuntime>, String> {
    ExecutionRuntime::new(
        ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 8,
            spill_io_threads: 1,
            spill_io_queue_capacity: 8,
            spill_storage: ExecutionSpillStorageConfig::default(),
            exchange_wait_ms: 120_000,
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1 << 20,
            exchange_max_transmit_batched_bytes: 1 << 20,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1 << 20,
            local_exchange_max_buffered_rows: 65_536,
            connector_io_tasks_per_scan_operator: 1,
            scan_submit_fail_max: 1,
            scan_submit_fail_timeout_ms: 1,
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            sink_io_worker_threads: 1,
            sink_io_max_blocking_threads: 1,
        },
        function_set,
    )
    .map(Arc::new)
    .map_err(|error| format!("build execution runtime: {error}"))
}

fn build_inputs(config: Config) -> Result<Vec<Chunk>, String> {
    let schema = input_schema();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[INPUT_SLOT])?;
    let mut chunks = Vec::with_capacity(config.rows.div_ceil(config.batch_rows));
    let mut offset = 0usize;
    while offset < config.rows {
        let end = (offset + config.batch_rows).min(config.rows);
        let values = (offset..end)
            .map(|row| i32::try_from(row % config.cardinality).expect("bounded cardinality"))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
        )
        .map_err(|error| format!("build input batch: {error}"))?;
        chunks.push(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&chunk_schema),
        )?);
        offset = end;
    }
    Ok(chunks)
}

fn run_once(
    case: AuxiliaryCase,
    phase: &'static str,
    iteration: usize,
    config: Config,
    inputs: &[Chunk],
    function_set: Arc<SealedExecutionFunctionSet>,
    runtime: Arc<ExecutionRuntime>,
) -> Result<RunResult, String> {
    let provider = BenchProvider::new();
    let adapter = WriteRuntimeAdapter::new(provider);
    let provider_counters = Arc::new(ProviderCounters::default());
    let execution: Arc<dyn ConnectorWriteExecution> = Arc::new(BenchWriteExecution {
        adapter: adapter.clone(),
        counters: Arc::clone(&provider_counters),
        append_delay_us: config.append_delay_us,
    });
    let plans = build_plans(case, &adapter, execution, Arc::clone(&function_set))?;
    let query_tracker = MemTracker::new_root(format!("ncp8_{}_{}", case.name(), iteration));
    let state = RuntimeState::new(
        None,
        None,
        None,
        None,
        None,
        Some(Arc::clone(&query_tracker)),
        None,
        None,
        Some(runtime),
        None,
    );
    let writer_profiles = OperatorProfiles::new(RuntimeProfile::new("BenchmarkTableWriter"));
    let finish_profiles = OperatorProfiles::new(RuntimeProfile::new("BenchmarkTableFinish"));
    let mut writer = plans.writer.create(1, 0);
    writer.set_mem_tracker(Arc::clone(&query_tracker));
    writer.set_profiles(writer_profiles.clone());
    writer.prepare()?;
    writer.bind_runtime_state(&state)?;
    let mut finish = plans.finish.create(1, 0);
    finish.set_mem_tracker(Arc::clone(&query_tracker));
    finish.set_profiles(finish_profiles.clone());
    finish.prepare()?;
    finish.bind_runtime_state(&state)?;

    let cpu_start = process_cpu_time_ns();
    let wall_start = Instant::now();
    let deadline = wall_start + POLL_TIMEOUT;
    let mut input_index = 0usize;
    let mut driver_writer_blocked_polls = 0u64;
    let mut driver_finish_blocked_polls = 0u64;
    let mut writer_output_batches = 0u64;

    while input_index < inputs.len() {
        check_progress(deadline, &state)?;
        if writer
            .as_processor_ref()
            .expect("writer processor")
            .has_output()
        {
            if let Some(chunk) = writer
                .as_processor_mut()
                .expect("writer processor")
                .pull_chunk(&state)?
            {
                writer_output_batches += 1;
                push_finish_chunk(
                    &mut finish,
                    &state,
                    chunk,
                    deadline,
                    &mut driver_finish_blocked_polls,
                )?;
            }
            continue;
        }
        if writer
            .as_processor_ref()
            .expect("writer processor")
            .need_input()
        {
            writer
                .as_processor_mut()
                .expect("writer processor")
                .push_chunk(&state, inputs[input_index].clone())?;
            input_index += 1;
        } else {
            driver_writer_blocked_polls += 1;
            std::thread::yield_now();
        }
    }

    writer
        .as_processor_mut()
        .expect("writer processor")
        .set_finishing(&state)?;
    while !writer.is_finished() {
        check_progress(deadline, &state)?;
        if writer
            .as_processor_ref()
            .expect("writer processor")
            .has_output()
        {
            if let Some(chunk) = writer
                .as_processor_mut()
                .expect("writer processor")
                .pull_chunk(&state)?
            {
                writer_output_batches += 1;
                push_finish_chunk(
                    &mut finish,
                    &state,
                    chunk,
                    deadline,
                    &mut driver_finish_blocked_polls,
                )?;
            }
        } else {
            driver_writer_blocked_polls += 1;
            std::thread::yield_now();
        }
    }

    finish
        .as_processor_mut()
        .expect("finish processor")
        .set_finishing(&state)?;
    let mut root_output_batches = 0u64;
    let mut root_logical_bytes = 0u64;
    let mut summary_rows = 0usize;
    let mut prepared_fragment_rows = 0usize;
    let mut artifact_rows = 0usize;
    let mut published_row_count = None;
    let mut theta_body = None;
    while !finish.is_finished() {
        check_progress(deadline, &state)?;
        if finish
            .as_processor_ref()
            .expect("finish processor")
            .has_output()
        {
            if let Some(chunk) = finish
                .as_processor_mut()
                .expect("finish processor")
                .pull_chunk(&state)?
            {
                root_output_batches += 1;
                root_logical_bytes =
                    root_logical_bytes.saturating_add(chunk.logical_bytes() as u64);
                inspect_root_chunk(
                    &chunk,
                    &mut summary_rows,
                    &mut prepared_fragment_rows,
                    &mut artifact_rows,
                    &mut published_row_count,
                    &mut theta_body,
                )?;
            }
        } else {
            driver_finish_blocked_polls += 1;
            std::thread::yield_now();
        }
    }
    writer.close()?;
    finish.close()?;
    drop(writer);
    drop(finish);
    let wall_ns = u64::try_from(wall_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let cpu_ns = process_cpu_time_ns().saturating_sub(cpu_start);
    let peak_memory_bytes = query_tracker.peak();
    let current_memory_bytes_after_drop = query_tracker.current();

    if summary_rows != 1
        || prepared_fragment_rows != 1
        || published_row_count != Some(config.rows as i64)
        || artifact_rows != usize::from(matches!(case, AuxiliaryCase::Theta))
        || provider_counters.appended_rows.load(Ordering::Relaxed) != config.rows
        || provider_counters.abort_calls.load(Ordering::Relaxed) != 0
    {
        return Err(format!(
            "invalid benchmark output for {}: summary={summary_rows}, fragments={prepared_fragment_rows}, artifacts={artifact_rows}, row_count={published_row_count:?}",
            case.name()
        ));
    }
    if current_memory_bytes_after_drop != 0 {
        return Err(format!(
            "benchmark query memory did not return to zero for {}: {current_memory_bytes_after_drop}",
            case.name()
        ));
    }

    let throughput = config.rows as f64 / (wall_ns as f64 / 1_000_000_000.0);
    Ok(RunResult {
        value: json!({
            "record": "sample",
            "schema_version": 1,
            "case": case.name(),
            "phase": phase,
            "iteration": iteration,
            "rows": config.rows,
            "wall_time_ns": wall_ns,
            "process_cpu_time_ns": cpu_ns,
            "throughput_rows_per_second": throughput,
            "peak_accounted_memory_bytes": peak_memory_bytes,
            "current_accounted_memory_bytes_after_drop": current_memory_bytes_after_drop,
            "driver_writer_blocked_polls": driver_writer_blocked_polls,
            "driver_finish_blocked_polls": driver_finish_blocked_polls,
            "writer_queue_blocked_checks": counter(&writer_profiles, "WriterQueueBlockedChecks"),
            "composite_writer_blocked_checks": counter(&writer_profiles, "CompositeWriterBlockedChecks"),
            "writer_queue_peak_batches": counter(&writer_profiles, "WriterQueuePeakBatches"),
            "writer_queue_peak_rows": counter(&writer_profiles, "WriterQueuePeakRows"),
            "writer_queue_peak_bytes": counter(&writer_profiles, "WriterQueuePeakBytes"),
            "writer_partial_rows": counter(&writer_profiles, "WriterPartialRows"),
            "writer_partial_bytes": counter(&writer_profiles, "WriterPartialBytes"),
            "writer_partial_aggregate_peak_bytes": counter(&writer_profiles, "WriterPartialAggregatePeakBytes"),
            "final_aggregate_blocked_count": counter(&finish_profiles, "FinalAggregateBlockedCount"),
            "final_aggregate_cpu_time_ns": counter(&finish_profiles, "FinalAggregateCpuTime"),
            "writer_multiplex_rows": counter(&finish_profiles, "WriterMultiplexRows"),
            "writer_multiplex_bytes": counter(&finish_profiles, "WriterMultiplexBytes"),
            "root_output_rows": counter(&finish_profiles, "RootOutputRows"),
            "root_output_bytes": counter(&finish_profiles, "RootOutputBytes"),
            "root_output_logical_bytes_observed": root_logical_bytes,
            "writer_output_batches": writer_output_batches,
            "root_output_batches": root_output_batches,
            "provider_open_calls": provider_counters.open_calls.load(Ordering::Relaxed),
            "provider_append_calls": provider_counters.append_calls.load(Ordering::Relaxed),
            "provider_finish_calls": provider_counters.finish_calls.load(Ordering::Relaxed),
            "provider_abort_calls": provider_counters.abort_calls.load(Ordering::Relaxed),
        }),
        theta_body,
    })
}

fn build_plans(
    case: AuxiliaryCase,
    adapter: &WriteRuntimeAdapter<BenchProvider>,
    execution: Arc<dyn ConnectorWriteExecution>,
    function_set: Arc<SealedExecutionFunctionSet>,
) -> Result<BuiltPlans, String> {
    let (writer_relation, partial_plan, final_plan, arena) = match case {
        AuxiliaryCase::Empty => (
            WriterMultiplexRelationSchema::empty(),
            WriterPartialAggregatePlan::default(),
            WriterFinalAggregatePlan::default(),
            ExprArena::default(),
        ),
        AuxiliaryCase::Theta => build_theta_plan(&function_set)?,
    };
    let target = WriteTargetOrdinal::try_new(0).map_err(|error| error.to_string())?;
    let mut projection_arena = ExprArena::default();
    let projection_expr =
        projection_arena.push_typed(ExprNode::SlotId(INPUT_SLOT), DataType::Int32);
    let projection = TableWriterInputProjection::try_new(
        projection_arena,
        vec![projection_expr],
        input_schema(),
    )
    .map_err(|error| error.to_string())?;
    let writer_node = TableWriterNode::try_new_with_relation(
        values_input(),
        2,
        adapter.wrap_writer_handle(BenchWriterRecipe),
        target,
        execution,
        input_schema(),
        projection,
        TableWriterPhysicalContextTemplate::new([1; 16], 1, [2; 16], 0),
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(3600),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| error.to_string())?,
        Arc::new(BenchFragmentEncoder(adapter.clone())),
        writer_relation.clone(),
        partial_plan,
    )
    .map_err(|error| error.to_string())?;
    let writer = TableWriterOperatorFactory::try_new(&writer_node, function_set)
        .map_err(|error| format!("build writer factory: {error}"))?;
    let finish_node = TableFinishNode::try_new_with_relations(
        vec![*values_input()],
        3,
        vec![target],
        Arc::new(AcceptCarrier),
        writer_relation,
        RootWriteResultRelationSchema::try_new(RootWriteResultSchema::new())?,
        final_plan,
    )
    .map_err(|error| error.to_string())?;
    Ok(BuiltPlans {
        writer,
        finish: TableFinishOperatorFactory::new_with_arena(&finish_node, Arc::new(arena)),
    })
}

fn build_theta_plan(
    function_set: &SealedExecutionFunctionSet,
) -> Result<
    (
        WriterMultiplexRelationSchema,
        WriterPartialAggregatePlan,
        WriterFinalAggregatePlan,
        ExprArena,
    ),
    String,
> {
    let resolved = function_set
        .catalog()
        .resolve_aggregate_trusted(ICEBERG_THETA_AGGREGATE_NAME, &[DataType::Int32])
        .map_err(|error| format!("resolve Iceberg Theta aggregate: {error}"))?;
    ensure_theta_types(&resolved)?;
    let channel = WriterAuxiliaryChannel::try_new(
        THETA_PARTIAL_SLOT.0,
        "theta_partial",
        resolved.intermediate_type.clone(),
    )
    .map_err(|error| error.to_string())?;
    let relation = WriterMultiplexRelationSchema::try_new(
        WriterMultiplexSchema::try_new(vec![channel]).map_err(|error| error.to_string())?,
    )?;
    let partial = WriterPartialAggregatePlan {
        calls: vec![WriterPartialAggregateCall {
            input_slot_id: INPUT_SLOT,
            function_name: Arc::from(ICEBERG_THETA_AGGREGATE_NAME),
            resolved: resolved.clone(),
            intermediate_slot_id: THETA_PARTIAL_SLOT,
        }],
    };
    let final_call = WriterFinalAggregateCall {
        function_name: Arc::from(ICEBERG_THETA_AGGREGATE_NAME),
        resolved,
        intermediate_input_slot_id: THETA_PARTIAL_SLOT,
        final_output_slot_id: THETA_FINAL_SLOT,
    };
    let mut arena = ExprArena::default();
    let blob_type = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(
            "iceberg/apache-datasketches-theta-v1/default-seed/ordered-compact".to_string(),
        )),
        DataType::Utf8,
    );
    let final_plan = WriterFinalAggregatePlan {
        calls: vec![final_call],
        unpivot: Some(WriterGroupedUnpivotPlan {
            grouping_input_slot_id: WRITE_RELATION_TARGET_SLOT,
            grouping_output_slot_id: GROUPING_OUTPUT_SLOT,
            passthrough_output_slot_id: SlotId::new(root_write_result_column_id(
                ROOT_WRITE_RESULT_TARGET_INDEX,
            )),
            value_output_slot_id: SlotId::new(root_write_result_column_id(
                ROOT_WRITE_RESULT_BODY_INDEX,
            )),
            literal_output_slot_ids: vec![
                SlotId::new(root_write_result_column_id(
                    ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
                )),
                SlotId::new(root_write_result_column_id(
                    ROOT_WRITE_RESULT_BLOB_TYPE_INDEX,
                )),
                SlotId::new(root_write_result_column_id(
                    ROOT_WRITE_RESULT_PROPERTIES_INDEX,
                )),
            ],
            mappings: vec![WriterGroupedUnpivotMapping {
                grouping_key: 0,
                input_value_slot_id: THETA_FINAL_SLOT,
                constants: vec![
                    UnpivotConstant::Int32List(vec![1]),
                    UnpivotConstant::Scalar {
                        expr_id: blob_type,
                        nullable: false,
                    },
                    UnpivotConstant::Utf8Map(Vec::new()),
                ],
            }],
            max_output_rows: 1024,
            max_output_bytes: 16 * 1024 * 1024,
        }),
    };
    Ok((relation, partial, final_plan, arena))
}

fn ensure_theta_types(resolved: &ResolvedAggregateSignature) -> Result<(), String> {
    if resolved.intermediate_type != DataType::Binary || resolved.output_type != DataType::Binary {
        return Err(format!(
            "unexpected Theta types: intermediate={:?}, output={:?}",
            resolved.intermediate_type, resolved.output_type
        ));
    }
    Ok(())
}

fn push_finish_chunk(
    finish: &mut Box<dyn Operator>,
    state: &RuntimeState,
    chunk: Chunk,
    deadline: Instant,
    blocked_polls: &mut u64,
) -> Result<(), String> {
    loop {
        check_progress(deadline, state)?;
        if finish
            .as_processor_ref()
            .expect("finish processor")
            .need_input()
        {
            return finish
                .as_processor_mut()
                .expect("finish processor")
                .push_chunk(state, chunk);
        }
        *blocked_polls = blocked_polls.saturating_add(1);
        std::thread::yield_now();
    }
}

fn inspect_root_chunk(
    chunk: &Chunk,
    summary_rows: &mut usize,
    prepared_fragment_rows: &mut usize,
    artifact_rows: &mut usize,
    published_row_count: &mut Option<i64>,
    theta_body: &mut Option<Vec<u8>>,
) -> Result<(), String> {
    let kinds = chunk.batch.column(ROOT_WRITE_RESULT_KIND_INDEX);
    let kinds = kinds
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| "root kind column is not Int8".to_string())?;
    let row_counts = chunk.batch.column(ROOT_WRITE_RESULT_ROW_COUNT_INDEX);
    let row_counts = row_counts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "root row-count column is not Int64".to_string())?;
    let bodies = chunk.batch.column(ROOT_WRITE_RESULT_BODY_INDEX);
    let bodies = bodies
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "root body column is not Binary".to_string())?;
    for row in 0..chunk.len() {
        match RootRowKind::from_wire(kinds.value(row)).map_err(|error| error.to_string())? {
            RootRowKind::Summary => {
                *summary_rows += 1;
                if row_counts.is_null(row) {
                    return Err("root summary row count is null".to_string());
                }
                *published_row_count = Some(row_counts.value(row));
            }
            RootRowKind::PreparedFragment => *prepared_fragment_rows += 1,
            RootRowKind::ArtifactDraft => {
                *artifact_rows += 1;
                if bodies.is_null(row) {
                    return Err("root artifact body is null".to_string());
                }
                *theta_body = Some(bodies.value(row).to_vec());
            }
        }
    }
    Ok(())
}

fn validate_theta(case: AuxiliaryCase, body: Option<&[u8]>) -> Result<(), String> {
    match (case, body) {
        (AuxiliaryCase::Empty, None) => Ok(()),
        (AuxiliaryCase::Theta, Some(body)) => validate_compact_theta(body)
            .map_err(|error| format!("benchmark produced invalid compact Theta: {error}")),
        (AuxiliaryCase::Empty, Some(_)) => {
            Err("empty auxiliary benchmark produced an artifact body".to_string())
        }
        (AuxiliaryCase::Theta, None) => {
            Err("Theta auxiliary benchmark produced no artifact body".to_string())
        }
    }
}

fn values_input() -> Box<ExecNode> {
    Box::new(ExecNode {
        kind: ExecNodeKind::Values(ValuesNode {
            chunk: Chunk::default(),
            node_id: 1,
        }),
    })
}

fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
}

fn counter(profiles: &OperatorProfiles, name: &str) -> i64 {
    profiles.common.counter_value(name).unwrap_or(0)
}

fn check_progress(deadline: Instant, state: &RuntimeState) -> Result<(), String> {
    if let Some(error) = state.error() {
        return Err(format!("execution runtime failed: {error}"));
    }
    if Instant::now() >= deadline {
        return Err("benchmark watchdog expired while waiting for operator progress".to_string());
    }
    Ok(())
}

fn summarize(case: AuxiliaryCase, samples: &[Value]) -> Result<Value, String> {
    let selected = samples
        .iter()
        .filter(|sample| sample["case"] == case.name())
        .collect::<Vec<_>>();
    let mut throughputs = selected
        .iter()
        .map(|sample| {
            sample["throughput_rows_per_second"]
                .as_f64()
                .ok_or_else(|| "sample throughput is not numeric".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut cpu = selected
        .iter()
        .map(|sample| {
            sample["process_cpu_time_ns"]
                .as_u64()
                .ok_or_else(|| "sample CPU time is not numeric".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut peak = selected
        .iter()
        .map(|sample| {
            sample["peak_accounted_memory_bytes"]
                .as_i64()
                .ok_or_else(|| "sample peak memory is not numeric".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    throughputs.sort_by(f64::total_cmp);
    cpu.sort_unstable();
    peak.sort_unstable();
    Ok(json!({
        "record": "summary",
        "schema_version": 1,
        "case": case.name(),
        "measurement_rounds": selected.len(),
        "median_throughput_rows_per_second": throughputs[throughputs.len() / 2],
        "median_process_cpu_time_ns": cpu[cpu.len() / 2],
        "max_peak_accounted_memory_bytes": peak.last().copied().unwrap_or(0),
        "performance_gate": null,
        "performance_gate_note": "measurement only; compare distributions outside the harness",
    }))
}

fn process_cpu_time_ns() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the supplied `rusage` on success. The
    // pointer is valid for the duration of the call and not retained.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return 0;
    }
    // SAFETY: the successful getrusage call initialized the value.
    let usage = unsafe { usage.assume_init() };
    timeval_ns(usage.ru_utime).saturating_add(timeval_ns(usage.ru_stime))
}

fn timeval_ns(value: libc::timeval) -> u64 {
    let seconds = u64::try_from(value.tv_sec).unwrap_or(0);
    let micros = u64::try_from(value.tv_usec).unwrap_or(0);
    seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(micros.saturating_mul(1_000))
}

fn emit(value: Value) {
    println!("{value}");
}

fn env_usize(name: &str, default: usize) -> Result<usize, String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<usize>()
        .map_err(|error| format!("parse {name}: {error}"))
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    std::env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u64>()
        .map_err(|error| format!("parse {name}: {error}"))
}
