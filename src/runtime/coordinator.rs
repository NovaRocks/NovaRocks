//! Execution coordinator for multi-fragment SQL execution.
//!
//! Wires and runs:
//! - CTE produce fragments (multicast to consumer exchange nodes)
//! - `Stream` producer fragments, each with a `DATA_STREAM_SINK` that fans out
//!   to every instance of the consumer fragment
//! - The root fragment via the dispatcher (result sink)
//!
//! All instance placement (instance counts, finst ids, backend index,
//! scan-range splits, destinations, prober params, per-exchange sender counts)
//! is owned by [`FragmentScheduler`]. The coordinator translates each placement
//! into a `TExecPlanFragmentParams` and submits it through the
//! `FragmentDispatcher`. `InProcessDispatcher` runs everything in-process;
//! `RemoteDispatcher` routes per-instance to remote BEs.
//!
//! At a single backend (all-in-one / 1FE+1BE), the scheduler produces one
//! instance per fragment and this path reproduces the prior single-instance
//! wiring exactly.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanBuilder, Decimal128Array, LargeBinaryArray,
    LargeBinaryBuilder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::data_sinks;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::novarocks_logging::debug;
use crate::partitions;
use crate::planner;
use crate::runtime::dispatcher::{FetchOutcome, FragmentDispatcher};
use crate::runtime::exec_params::build_exec_plan_fragment_params;
use crate::runtime::query_state::QueryState;
use crate::runtime::scheduler::{FragmentScheduler, topological_sort_bottom_up};
use crate::runtime::write_coordinator::{
    WriteAbortInput, WriteCommitInput, WriteCoordinator, WriterKey, register_query,
    unregister_query,
};
use crate::runtime_filter;
use crate::sql::analysis::cte::CteId;
use crate::sql::codegen::{
    FragmentEdge, FragmentEdgeKind, FragmentId, MultiFragmentBuildResult, RuntimeFilterPlanResult,
};
use crate::types;

use crate::runtime::query_result::{QueryResult, QueryResultColumn};

/// Result of a coordinated execution, exposing the writer-side outcome to the
/// engine layer. `write_commit` is set when writers reported a commit input on
/// the success path. `write_abort` is set when writer-side coordination fails
/// after the root result has been produced and the write coordinator can build
/// an abort input for the engine layer.
#[derive(Debug)]
pub(crate) struct CoordinatedQueryResult {
    pub(crate) query_result: QueryResult,
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) write_abort: Option<WriteAbortInput>,
}

/// Coordinates multi-fragment query execution across one or more backends.
///
/// Drives all fragment wiring from [`FragmentScheduler`] placements and submits
/// every instance through the `FragmentDispatcher`. Results are collected by
/// polling the dispatcher for the root fragment's chunks.
pub(crate) struct ExecutionCoordinator {
    build_result: MultiFragmentBuildResult,
    dispatcher: Arc<dyn FragmentDispatcher>,
    scheduler: Arc<FragmentScheduler>,
    query_options: Option<crate::internal_service::TQueryOptions>,
}

impl ExecutionCoordinator {
    pub(crate) fn new(
        build_result: MultiFragmentBuildResult,
        dispatcher: Arc<dyn FragmentDispatcher>,
        scheduler: Arc<FragmentScheduler>,
        query_options: Option<crate::internal_service::TQueryOptions>,
    ) -> Self {
        Self {
            build_result,
            dispatcher,
            scheduler,
            query_options,
        }
    }

    pub(crate) fn execute_with_write_outcome(self) -> Result<CoordinatedQueryResult, String> {
        let MultiFragmentBuildResult {
            mut fragment_results,
            root_fragment_id,
            edges,
            rf_plan,
        } = self.build_result;
        let query_options = self.query_options;
        let dispatcher = self.dispatcher;
        let scheduler = self.scheduler;

        // ---------------------------------------------------------------
        // 1. Allocate query id and run the scheduler.
        // ---------------------------------------------------------------
        use std::sync::atomic::{AtomicI64, Ordering};
        static NEXT_QUERY_BASE: AtomicI64 = AtomicI64::new(100);
        let query_base = NEXT_QUERY_BASE.fetch_add(1000, Ordering::Relaxed);
        // Use query_base for both hi and lo so the scheduler's
        // `root_backend_idx = query_id.lo % n` scatters across backends per
        // query instead of always landing on backend 1 % n.
        let query_id = types::TUniqueId::new(query_base, query_base);

        debug!(
            "coordinator topology: fragments={} edges={} root={} backends={}",
            fragment_results.len(),
            edges.len(),
            root_fragment_id,
            scheduler.backends().len()
        );
        for e in &edges {
            debug!(
                "coordinator edge: frag {} -> frag {} (exch_node={}, kind={:?}, part={:?})",
                e.source_fragment_id,
                e.target_fragment_id,
                e.target_exchange_node_id,
                match &e.edge_kind {
                    FragmentEdgeKind::Stream => "Stream",
                    FragmentEdgeKind::CteMulticast { .. } => "CteMulticast",
                },
                e.output_partition.type_,
            );
        }

        let live: Vec<(usize, std::net::SocketAddr)> =
            match crate::runtime::backend_registry::backend_registry() {
                Some(reg) => reg
                    .live_endpoints()
                    .into_iter()
                    .map(|(be_id, ep)| (be_id as usize, ep))
                    .collect(),
                None => scheduler.backends().iter().copied().enumerate().collect(),
            };
        let mut plan =
            scheduler.assign_with_live(&fragment_results, &edges, query_id.clone(), &live)?;
        scheduler.fill_destinations_with_live(&mut plan, &edges, &live)?;
        if let Some(rf) = rf_plan.as_ref() {
            scheduler.fill_runtime_filter_params_with_live(&mut plan, rf, &live)?;
        }
        scheduler.fill_per_exch_num_senders(&mut plan, &edges);

        // ---------------------------------------------------------------
        // 2. Build per-edge / CTE consumer indices used for sink wiring.
        // ---------------------------------------------------------------
        // Stream producer fragment id -> its single outgoing edge index.
        let mut stream_edge_by_source: BTreeMap<FragmentId, &FragmentEdge> = BTreeMap::new();
        // CTE id -> list of (consumer_fragment_id, exchange_node_id, partition).
        let mut cte_consumers: BTreeMap<CteId, Vec<(FragmentId, i32, partitions::TDataPartition)>> =
            BTreeMap::new();

        for e in &edges {
            match &e.edge_kind {
                FragmentEdgeKind::Stream => {
                    if stream_edge_by_source
                        .insert(e.source_fragment_id, e)
                        .is_some()
                    {
                        return Err(format!(
                            "fragment {} has multiple outgoing Stream edges; \
                             stream fan-out is not supported",
                            e.source_fragment_id
                        ));
                    }
                }
                FragmentEdgeKind::CteMulticast { cte_id } => {
                    cte_consumers.entry(*cte_id).or_default().push((
                        e.target_fragment_id,
                        e.target_exchange_node_id,
                        e.output_partition.clone(),
                    ));
                }
            }
        }
        // CTE consumers may also be expressed via `cte_exchange_nodes` on the
        // consumer fragment when no explicit edge carries them.
        for fr in &fragment_results {
            for (cte_id, exchange_node_id) in &fr.cte_exchange_nodes {
                let consumers = cte_consumers.entry(*cte_id).or_default();
                if !consumers
                    .iter()
                    .any(|(fid, nid, _)| *fid == fr.fragment_id && *nid == *exchange_node_id)
                {
                    consumers.push((fr.fragment_id, *exchange_node_id, unpartitioned_partition()));
                }
            }
        }

        // ---------------------------------------------------------------
        // 3. Inject the designated runtime-filter merge node into descriptors.
        // ---------------------------------------------------------------
        // The merge node is the backend that hosts the (single) root instance.
        // At one backend this equals the local exchange address, matching the
        // prior `dispatcher.exchange_addr()` behavior exactly.
        let merge_addr = backend_to_network_addr(&live, plan.root_backend_idx)?;
        if rf_plan.is_some() {
            inject_runtime_filter_merge_nodes(&mut fragment_results, &merge_addr);
        }

        // ---------------------------------------------------------------
        // 4. Translate every placement into a fragment params and submit.
        // ---------------------------------------------------------------
        let pipeline_dop = crate::runtime::dispatcher::compute_pipeline_dop();
        let needs_fragment_status_report = dispatcher.needs_fragment_status_report();
        let mut novarocks_report_addr: Option<types::TNetworkAddress> = None;

        // Snapshot the per-consumer-fragment instance destinations for CTE
        // multicast sub-sinks (each consumer fans out to all of its instances).
        let consumer_dests: BTreeMap<FragmentId, Vec<data_sinks::TPlanFragmentDestination>> = plan
            .by_fragment
            .iter()
            .map(|(fid, insts)| {
                let dests: Result<Vec<_>, String> = insts
                    .iter()
                    .map(|inst| {
                        let addr = live_backend_addr(&live, inst.backend_idx)?;
                        Ok(data_sinks::TPlanFragmentDestination::new(
                            inst.finst_id.clone(),
                            None::<types::TNetworkAddress>,
                            Some(types::TNetworkAddress::new(
                                addr.ip().to_string(),
                                addr.port() as i32,
                            )),
                            None::<i32>,
                        ))
                    })
                    .collect();
                dests.map(|d| (*fid, d))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        let fr_by_id: BTreeMap<FragmentId, &crate::sql::codegen::FragmentBuildResult> =
            fragment_results
                .iter()
                .map(|fr| (fr.fragment_id, fr))
                .collect();

        // Build a fragment-id -> instance count map from the scheduling plan.
        // This is used by build_instance_runtime_filter_params to set the correct
        // builder_number for each runtime filter id (must equal the number of
        // build-side instances, not a hardcoded 1).
        let instance_counts: BTreeMap<FragmentId, usize> = plan
            .by_fragment
            .iter()
            .map(|(&fid, insts)| (fid, insts.len()))
            .collect();

        let mut tracker = InFlightTracker::default();
        // Collect submissions by fragment, then submit consumers before
        // producers. This ensures downstream exchange receivers/result buffers
        // are registered before an upstream producer can fail or send data.
        let mut submissions_by_fragment: BTreeMap<
            FragmentId,
            Vec<(usize, crate::internal_service::TExecPlanFragmentParams)>,
        > = BTreeMap::new();
        let mut expected_writers = Vec::new();

        for (&fragment_id, placements) in &plan.by_fragment {
            let fr = *fr_by_id
                .get(&fragment_id)
                .ok_or_else(|| format!("fragment {fragment_id} missing from build results"))?;
            let is_root = fragment_id == root_fragment_id;
            let stream_edge = stream_edge_by_source.get(&fragment_id).copied();

            // Classify the fragment once.
            if !is_root && fr.cte_id.is_none() && stream_edge.is_none() {
                return Err(format!(
                    "fragment {fragment_id} is neither root, CTE producer, nor stream producer; \
                     stream fan-out is not supported in standalone coordinator"
                ));
            }

            for placement in placements {
                // Build the output sink for this fragment class.
                let (output_sink, fragment_partition, exec_destinations) = if is_root {
                    (fr.output_sink.clone(), unpartitioned_partition(), None)
                } else if let Some(edge) = stream_edge {
                    let stream_sink = data_sinks::TDataStreamSink::new(
                        edge.target_exchange_node_id,
                        edge.output_partition.clone(),
                        None::<bool>,
                        None::<bool>,
                        None::<i32>,
                        None::<Vec<i32>>,
                        None::<i64>,
                    );
                    let output_sink = wrap_data_stream_sink(stream_sink);
                    (
                        output_sink,
                        edge.output_partition.clone(),
                        Some(placement.destinations.clone()),
                    )
                } else {
                    // CTE producer.
                    let cte_id = fr
                        .cte_id
                        .ok_or_else(|| "CTE fragment missing cte_id".to_string())?;
                    let consumers = cte_consumers.get(&cte_id).cloned().unwrap_or_default();
                    if consumers.is_empty() {
                        return Err(format!("CTE fragment (cte_id={cte_id}) has no consumers"));
                    }
                    let mut sinks = Vec::with_capacity(consumers.len());
                    let mut destinations = Vec::with_capacity(consumers.len());
                    for (consumer_fragment_id, exchange_node_id, partition) in &consumers {
                        let stream_sink = data_sinks::TDataStreamSink::new(
                            *exchange_node_id,
                            partition.clone(),
                            None::<bool>,
                            None::<bool>,
                            None::<i32>,
                            None::<Vec<i32>>,
                            None::<i64>,
                        );
                        sinks.push(stream_sink);
                        let dests = consumer_dests
                            .get(consumer_fragment_id)
                            .cloned()
                            .ok_or_else(|| {
                                format!(
                                    "CTE consumer fragment {consumer_fragment_id} has no placements"
                                )
                            })?;
                        destinations.push(dests);
                    }
                    let multi_cast_sink =
                        data_sinks::TMultiCastDataStreamSink::new(sinks, destinations);
                    let output_sink = wrap_multi_cast_sink(multi_cast_sink);
                    // Multicast carries its own destinations on the sub-sinks.
                    (output_sink, unpartitioned_partition(), None)
                };

                let fragment_report_addr = if data_sink_requires_write_report(&output_sink)
                    || needs_fragment_status_report
                {
                    if novarocks_report_addr.is_none() {
                        novarocks_report_addr = Some(local_coordinator_report_addr()?);
                    }
                    novarocks_report_addr.clone()
                } else {
                    None
                };

                let thrift_fragment = planner::TPlanFragment::new(
                    Some(fr.plan.clone()),
                    fr.output_exprs.clone(),
                    Some(output_sink),
                    fragment_partition,
                    None::<i64>,
                    None::<i64>,
                    fr.query_global_dicts.clone(),
                    None::<Vec<crate::data::TGlobalDict>>,
                    None::<planner::TCacheParam>,
                    fr.query_global_dict_exprs.clone(),
                    None::<planner::TGroupExecutionParam>,
                );

                let mut exec_params = fr.exec_params.clone();
                exec_params.query_id = query_id.clone();
                exec_params.fragment_instance_id = placement.finst_id.clone();
                exec_params.per_node_scan_ranges = placement.scan_ranges.clone();
                exec_params.per_exch_num_senders = placement.per_exch_num_senders.clone();
                exec_params.destinations = exec_destinations;
                if let Some(rf) = rf_plan.as_ref() {
                    exec_params.runtime_filter_params = Some(build_instance_runtime_filter_params(
                        rf,
                        &placement.runtime_filter_prober_params,
                        &instance_counts,
                    ));
                }

                let params = build_exec_plan_fragment_params(
                    fr,
                    thrift_fragment,
                    exec_params,
                    query_options.clone(),
                    pipeline_dop,
                    Some(placement.instance_index as i32),
                    fragment_report_addr,
                );

                if is_write_sink(&params) {
                    let exec = params.params.as_ref().ok_or_else(|| {
                        "write sink fragment missing exec params in coordinator".to_string()
                    })?;
                    expected_writers.push(WriterKey {
                        query_id: exec.query_id.clone(),
                        fragment_instance_id: exec.fragment_instance_id.clone(),
                        backend_num: placement.instance_index as i32,
                    });
                }

                submissions_by_fragment
                    .entry(fragment_id)
                    .or_default()
                    .push((placement.backend_idx, params));
            }
        }

        if !submissions_by_fragment.contains_key(&root_fragment_id) {
            return Err("root fragment produced no placement".to_string());
        }
        let mut submissions: Vec<(usize, crate::internal_service::TExecPlanFragmentParams)> =
            Vec::new();
        for fragment_id in topological_sort_bottom_up(&fragment_results, &edges)?
            .into_iter()
            .rev()
        {
            if let Some(mut fragment_submissions) = submissions_by_fragment.remove(&fragment_id) {
                submissions.append(&mut fragment_submissions);
            }
        }
        if !submissions_by_fragment.is_empty() {
            return Err(format!(
                "submissions remained for unknown fragments: {:?}",
                submissions_by_fragment.keys().collect::<Vec<_>>()
            ));
        }

        let (write_coordinator, _write_registration) = if expected_writers.is_empty() {
            (None, None)
        } else {
            let write = register_query(query_id.clone(), expected_writers)?;
            (
                Some(write),
                Some(RegisteredWriteCoordinator::new(query_id.clone())),
            )
        };

        let timeout_ms = query_options
            .as_ref()
            .and_then(|q| q.query_timeout)
            .map(|t| t as i64 * 1000)
            .unwrap_or(300_000); // 5 minute default

        let fetch_result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            submissions,
            plan.root_backend_idx,
            plan.root_finst_id.clone(),
            &query_id,
            timeout_ms,
            write_coordinator.as_ref(),
        )?;

        if let Some(commit) = fetch_result.write_commit.as_ref() {
            tracing::info!(
                target: "novarocks::write_coordinator",
                write_hi = commit.write_id.hi,
                write_lo = commit.write_id.lo,
                writers = commit.writers.len(),
                "write coordinator commit input ready"
            );
        }

        let root_fragment = fr_by_id
            .get(&root_fragment_id)
            .ok_or_else(|| "root fragment not found in build results".to_string())?;
        let chunks = coerce_fetch_chunks_to_output_columns(
            fetch_result.chunks,
            &root_fragment.output_columns,
        )?;
        let query_result = QueryResult {
            columns: root_fragment
                .output_columns
                .iter()
                .map(|c| QueryResultColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    logical_type: None,
                })
                .collect(),
            chunks,
        };
        Ok(CoordinatedQueryResult {
            query_result,
            write_commit: fetch_result.write_commit,
            write_abort: fetch_result.write_abort,
        })
    }

    /// Backward-compatible entry point: runs the coordinated execution and
    /// returns only the query result, discarding the writer outcome. Existing
    /// callers that do not participate in the Iceberg write lifecycle use this.
    pub(crate) fn execute(self) -> Result<QueryResult, String> {
        self.execute_with_write_outcome()
            .and_then(query_result_or_write_abort_error)
    }
}

fn query_result_or_write_abort_error(
    outcome: CoordinatedQueryResult,
) -> Result<QueryResult, String> {
    if let Some(abort) = outcome.write_abort {
        return Err(abort.reason);
    }
    Ok(outcome.query_result)
}

fn coerce_fetch_chunks_to_output_columns(
    chunks: Vec<Chunk>,
    output_columns: &[crate::sql::codegen::OutputColumn],
) -> Result<Vec<Chunk>, String> {
    chunks
        .into_iter()
        .map(|chunk| coerce_fetch_chunk_to_output_columns(chunk, output_columns))
        .collect()
}

fn coerce_fetch_chunk_to_output_columns(
    chunk: Chunk,
    output_columns: &[crate::sql::codegen::OutputColumn],
) -> Result<Chunk, String> {
    if output_columns.is_empty() || chunk.batch.num_columns() != output_columns.len() {
        return Ok(chunk);
    }

    let schema = chunk.batch.schema();
    let already_aligned =
        schema
            .fields()
            .iter()
            .zip(output_columns.iter())
            .all(|(field, output)| {
                field.name() == &output.name
                    && field.data_type() == &output.data_type
                    && field.is_nullable() == output.nullable
            });
    if already_aligned {
        return Ok(chunk);
    }

    let mut arrays = Vec::with_capacity(output_columns.len());
    let mut fields = Vec::with_capacity(output_columns.len());
    for (idx, output) in output_columns.iter().enumerate() {
        let source = chunk.batch.column(idx);
        let array = if source.data_type() == &output.data_type {
            source.clone()
        } else if is_binary_like_result_column(source.data_type()) {
            coerce_binary_like_result_column(source, &output.data_type, idx)?
        } else {
            return Ok(chunk);
        };
        let field_type = array.data_type().clone();
        let field_nullable = output.nullable || array.null_count() > 0;
        arrays.push(array);
        fields.push(Field::new(output.name.clone(), field_type, field_nullable));
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("coerce remote fetch result batch failed: {e}"))?;
    let slot_ids = (1..=batch.num_columns())
        .map(|idx| {
            u32::try_from(idx)
                .map(SlotId::new)
                .map_err(|_| "too many remote fetch result columns".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

fn is_binary_like_result_column(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::LargeBinary)
}

enum BinaryLikeColumn<'a> {
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
}

impl<'a> BinaryLikeColumn<'a> {
    fn try_new(array: &'a ArrayRef, col_idx: usize) -> Result<Self, String> {
        match array.data_type() {
            DataType::Binary => array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .map(Self::Binary)
                .ok_or_else(|| format!("remote fetch column {col_idx} is not BinaryArray")),
            DataType::LargeBinary => array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .map(Self::LargeBinary)
                .ok_or_else(|| format!("remote fetch column {col_idx} is not LargeBinaryArray")),
            other => Err(format!(
                "remote fetch column {col_idx} is not binary-like: {other:?}"
            )),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Binary(array) => array.len(),
            Self::LargeBinary(array) => array.len(),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Binary(array) => array.is_null(row),
            Self::LargeBinary(array) => array.is_null(row),
        }
    }

    fn value(&self, row: usize) -> &[u8] {
        match self {
            Self::Binary(array) => array.value(row),
            Self::LargeBinary(array) => array.value(row),
        }
    }
}

fn coerce_binary_like_result_column(
    source: &ArrayRef,
    target_type: &DataType,
    col_idx: usize,
) -> Result<ArrayRef, String> {
    let source = BinaryLikeColumn::try_new(source, col_idx)?;
    match target_type {
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for row in 0..source.len() {
                if source.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(source.value(row));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::LargeBinary => {
            let mut builder = LargeBinaryBuilder::new();
            for row in 0..source.len() {
                if source.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(source.value(row));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in 0..source.len() {
                if source.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(parse_mysql_bool(source.value(row), col_idx)?);
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Null => {
            for row in 0..source.len() {
                if !source.is_null(row) {
                    return Err(format!(
                        "remote fetch column {col_idx} expected NULL_TYPE but row {row} has non-null mysql text"
                    ));
                }
            }
            Ok(arrow::array::new_null_array(target_type, source.len()))
        }
        DataType::Utf8 => binary_like_to_string_array(&source, col_idx),
        DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            binary_like_to_string_array(&source, col_idx)
        }
        dt if crate::common::largeint::is_largeint_data_type(dt) => {
            binary_like_to_largeint_array(&source, col_idx)
        }
        DataType::Decimal128(precision, scale) => {
            binary_like_to_decimal128_array(&source, *precision, *scale, col_idx)
        }
        _ => {
            let strings = binary_like_to_string_array(&source, col_idx)?;
            arrow::compute::cast(&strings, target_type).map_err(|e| {
                format!(
                    "coerce remote fetch column {col_idx} from mysql text to {target_type:?} failed: {e}"
                )
            })
        }
    }
}

fn binary_like_to_string_array(
    source: &BinaryLikeColumn<'_>,
    col_idx: usize,
) -> Result<ArrayRef, String> {
    let mut builder = StringBuilder::new();
    for row in 0..source.len() {
        if source.is_null(row) {
            builder.append_null();
            continue;
        }
        let text = std::str::from_utf8(source.value(row)).map_err(|e| {
            format!("remote fetch column {col_idx} row {row} is not valid UTF-8: {e}")
        })?;
        builder.append_value(text);
    }
    Ok(Arc::new(builder.finish()))
}

fn binary_like_to_largeint_array(
    source: &BinaryLikeColumn<'_>,
    col_idx: usize,
) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(source.len());
    for row in 0..source.len() {
        if source.is_null(row) {
            values.push(None);
            continue;
        }
        let text = std::str::from_utf8(source.value(row)).map_err(|e| {
            format!("remote fetch column {col_idx} row {row} is not valid UTF-8: {e}")
        })?;
        let value = text.trim().parse::<i128>().map_err(|e| {
            format!("coerce remote fetch column {col_idx} row {row} to LARGEINT failed: {e}")
        })?;
        values.push(Some(value));
    }
    crate::common::largeint::array_from_i128(&values)
}

fn binary_like_to_decimal128_array(
    source: &BinaryLikeColumn<'_>,
    declared_precision: u8,
    scale: i8,
    col_idx: usize,
) -> Result<ArrayRef, String> {
    if declared_precision > 38 {
        return Err(format!(
            "remote fetch column {col_idx} Decimal128 precision exceeds 38: {declared_precision}"
        ));
    }
    let mut values = Vec::with_capacity(source.len());
    let mut requires_wide_precision = false;
    for row in 0..source.len() {
        if source.is_null(row) {
            values.push(None);
            continue;
        }
        let value = parse_mysql_decimal_text_to_i128(source.value(row), scale, col_idx, row)?;
        if decimal128_precision(value) > declared_precision {
            requires_wide_precision = true;
        }
        values.push(Some(value));
    }
    let precision = if requires_wide_precision {
        38
    } else {
        declared_precision
    };
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| {
            format!(
                "coerce remote fetch column {col_idx} to Decimal128({precision}, {scale}) failed: {e}"
            )
        })?;
    Ok(Arc::new(array))
}

fn parse_mysql_decimal_text_to_i128(
    bytes: &[u8],
    scale: i8,
    col_idx: usize,
    row: usize,
) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!(
            "remote fetch column {col_idx} has unsupported negative decimal scale: {scale}"
        ));
    }
    let text = std::str::from_utf8(bytes).map_err(|e| {
        format!("remote fetch column {col_idx} row {row} decimal text is not valid UTF-8: {e}")
    })?;
    let text = text.trim();
    if text.is_empty() {
        return Err(format!(
            "remote fetch column {col_idx} row {row} has empty decimal text"
        ));
    }

    let (negative, unsigned) = match text.as_bytes()[0] {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    if unsigned.is_empty() {
        return Err(format!(
            "remote fetch column {col_idx} row {row} has invalid decimal text: {text}"
        ));
    }

    let mut parts = unsigned.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some() {
        return Err(format!(
            "remote fetch column {col_idx} row {row} has invalid decimal text: {text}"
        ));
    }
    if whole.is_empty() && fraction.is_none_or(str::is_empty) {
        return Err(format!(
            "remote fetch column {col_idx} row {row} has invalid decimal text: {text}"
        ));
    }
    let fraction = fraction.unwrap_or_default();
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "remote fetch column {col_idx} row {row} has invalid decimal text: {text}"
        ));
    }

    let scale = usize::try_from(scale)
        .map_err(|_| format!("remote fetch column {col_idx} has unsupported scale"))?;
    if fraction.len() > scale {
        return Err(format!(
            "remote fetch column {col_idx} row {row} decimal scale exceeds target scale {scale}: {text}"
        ));
    }
    let mut digits = String::with_capacity(whole.len().saturating_add(scale));
    digits.push_str(whole);
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n('0', scale - fraction.len()));
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    if digits.len() > 38 {
        return Err(format!(
            "remote fetch column {col_idx} row {row} decimal precision exceeds Decimal128 capacity: {text}"
        ));
    }
    let value = digits.parse::<i128>().map_err(|e| {
        format!("remote fetch column {col_idx} row {row} decimal parse failed: {e}")
    })?;
    Ok(if negative { -value } else { value })
}

fn decimal128_precision(value: i128) -> u8 {
    let abs = value.unsigned_abs();
    if abs == 0 {
        return 1;
    }
    u8::try_from(abs.to_string().len()).unwrap_or(38)
}

fn parse_mysql_bool(bytes: &[u8], col_idx: usize) -> Result<bool, String> {
    match bytes {
        b"1" => Ok(true),
        b"0" => Ok(false),
        _ if bytes.eq_ignore_ascii_case(b"true") => Ok(true),
        _ if bytes.eq_ignore_ascii_case(b"false") => Ok(false),
        _ => Err(format!(
            "coerce remote fetch column {col_idx} to Boolean failed: {:?}",
            String::from_utf8_lossy(bytes)
        )),
    }
}

/// An `UNPARTITIONED` data partition (the common default).
fn unpartitioned_partition() -> partitions::TDataPartition {
    partitions::TDataPartition::new(
        partitions::TPartitionType::UNPARTITIONED,
        None::<Vec<crate::exprs::TExpr>>,
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    )
}

/// Wrap a `TDataStreamSink` in a DATA_STREAM_SINK `TDataSink`.
fn wrap_data_stream_sink(stream_sink: data_sinks::TDataStreamSink) -> data_sinks::TDataSink {
    data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::DATA_STREAM_SINK,
        Some(stream_sink),
        None::<data_sinks::TResultSink>,
        None::<data_sinks::TMysqlTableSink>,
        None::<data_sinks::TExportSink>,
        None::<data_sinks::TOlapTableSink>,
        None::<data_sinks::TMemoryScratchSink>,
        None::<data_sinks::TMultiCastDataStreamSink>,
        None::<data_sinks::TSchemaTableSink>,
        None::<data_sinks::TIcebergTableSink>,
        None::<data_sinks::THiveTableSink>,
        None::<data_sinks::TTableFunctionTableSink>,
        None::<data_sinks::TDictionaryCacheSink>,
        None::<Vec<Box<data_sinks::TDataSink>>>,
        None::<i64>,
        None::<data_sinks::TSplitDataStreamSink>,
    )
}

/// Wrap a `TMultiCastDataStreamSink` in a MULTI_CAST_DATA_STREAM_SINK `TDataSink`.
fn wrap_multi_cast_sink(
    multi_cast_sink: data_sinks::TMultiCastDataStreamSink,
) -> data_sinks::TDataSink {
    data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::MULTI_CAST_DATA_STREAM_SINK,
        None::<data_sinks::TDataStreamSink>,
        None::<data_sinks::TResultSink>,
        None::<data_sinks::TMysqlTableSink>,
        None::<data_sinks::TExportSink>,
        None::<data_sinks::TOlapTableSink>,
        None::<data_sinks::TMemoryScratchSink>,
        Some(multi_cast_sink),
        None::<data_sinks::TSchemaTableSink>,
        None::<data_sinks::TIcebergTableSink>,
        None::<data_sinks::THiveTableSink>,
        None::<data_sinks::TTableFunctionTableSink>,
        None::<data_sinks::TDictionaryCacheSink>,
        None::<Vec<Box<data_sinks::TDataSink>>>,
        None::<i64>,
        None::<data_sinks::TSplitDataStreamSink>,
    )
}

fn is_write_sink(params: &crate::internal_service::TExecPlanFragmentParams) -> bool {
    params
        .fragment
        .as_ref()
        .and_then(|fragment| fragment.output_sink.as_ref())
        .map(data_sink_requires_write_report)
        .unwrap_or(false)
}

fn data_sink_requires_write_report(sink: &data_sinks::TDataSink) -> bool {
    matches!(
        sink.type_,
        data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
            | data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
            | data_sinks::TDataSinkType::HIVE_TABLE_SINK
            | data_sinks::TDataSinkType::OLAP_TABLE_SINK
    )
}

fn uses_result_buffer_sink(params: &crate::internal_service::TExecPlanFragmentParams) -> bool {
    matches!(
        params
            .fragment
            .as_ref()
            .and_then(|fragment| fragment.output_sink.as_ref())
            .map(|sink| sink.type_),
        Some(data_sinks::TDataSinkType::RESULT_SINK)
    )
}

fn root_uses_result_buffer(
    submissions: &[(usize, crate::internal_service::TExecPlanFragmentParams)],
    root_finst_id: &types::TUniqueId,
) -> Result<bool, String> {
    let root = submissions
        .iter()
        .map(|(_, params)| params)
        .find(|params| {
            params
                .params
                .as_ref()
                .map(|exec| {
                    exec.fragment_instance_id.hi == root_finst_id.hi
                        && exec.fragment_instance_id.lo == root_finst_id.lo
                })
                .unwrap_or(false)
        })
        .ok_or_else(|| {
            format!(
                "root fragment {}/{} is missing from submissions",
                root_finst_id.hi, root_finst_id.lo
            )
        })?;
    Ok(uses_result_buffer_sink(root))
}

struct RegisteredWriteCoordinator {
    query_id: types::TUniqueId,
}

impl RegisteredWriteCoordinator {
    fn new(query_id: types::TUniqueId) -> Self {
        Self { query_id }
    }
}

impl Drop for RegisteredWriteCoordinator {
    fn drop(&mut self) {
        unregister_query(&self.query_id);
    }
}

fn validate_write_commit_ready(
    write: &Arc<Mutex<WriteCoordinator>>,
) -> Result<WriteCommitInput, String> {
    write.lock().expect("write coordinator lock").commit_input()
}

/// Convert `backends[idx]` into a `TNetworkAddress`.
fn backend_to_network_addr(
    live: &[(usize, SocketAddr)],
    idx: usize,
) -> Result<types::TNetworkAddress, String> {
    let addr = live_backend_addr(live, idx)?;
    Ok(types::TNetworkAddress::new(
        addr.ip().to_string(),
        addr.port() as i32,
    ))
}

fn live_backend_addr(
    live: &[(usize, SocketAddr)],
    backend_idx: usize,
) -> Result<SocketAddr, String> {
    live.iter()
        .find_map(|(idx, addr)| (*idx == backend_idx).then_some(*addr))
        .ok_or_else(|| format!("backend index {backend_idx} missing from live snapshot"))
}

fn local_coordinator_report_addr() -> Result<types::TNetworkAddress, String> {
    let cfg = crate::novarocks_config::config()
        .map_err(|e| format!("cannot read coordinator config: {e}"))?;
    let host = crate::common::network::advertise_host().unwrap_or_else(|_| cfg.server.host.clone());
    let port =
        crate::service::grpc_server::grpc_server_bound_port().unwrap_or(cfg.server.grpc_port);
    Ok(types::TNetworkAddress::new(host, port as i32))
}

/// Assemble the per-instance `TRuntimeFilterParams` from scheduler-provided
/// prober params plus the global builder-number map.
///
/// `instance_counts` maps fragment id to the number of instances the scheduler
/// assigned to it. For each build fragment, every filter id it produces must
/// wait for exactly that many partial filters before the merge node broadcasts.
/// Hardcoding 1 here would cause the merge to broadcast after the first
/// partial, silently dropping N-1 partials and producing an incomplete bloom
/// filter at N > 1 instances (wrong join results).
fn build_instance_runtime_filter_params(
    rf_plan: &RuntimeFilterPlanResult,
    id_to_prober_params: &BTreeMap<i32, Vec<runtime_filter::TRuntimeFilterProberParams>>,
    instance_counts: &BTreeMap<FragmentId, usize>,
) -> runtime_filter::TRuntimeFilterParams {
    let mut builder_number: BTreeMap<i32, i32> = BTreeMap::new();
    for (build_frag_id, filter_ids) in &rf_plan.build_side_filters {
        let n_builders = instance_counts
            .get(build_frag_id)
            .map(|&n| n as i32)
            .unwrap_or(1);
        for fid in filter_ids {
            builder_number.insert(*fid, n_builders);
        }
    }
    runtime_filter::TRuntimeFilterParams::new(
        id_to_prober_params.clone(),
        builder_number,
        16_i64 * 1024 * 1024,
        None::<std::collections::BTreeSet<i32>>,
    )
}

/// Inject the designated runtime-filter merge node into every descriptor that
/// has remote targets.
///
/// This mutates the per-fragment `hash_join_node.build_runtime_filters`
/// descriptors in place (these are what actually ship to the BE). The merge
/// node is the backend hosting the root instance; at one backend it equals the
/// local exchange address (prior behavior).
fn inject_runtime_filter_merge_nodes(
    fragment_results: &mut [crate::sql::codegen::FragmentBuildResult],
    merge_addr: &types::TNetworkAddress,
) {
    for fr in fragment_results.iter_mut() {
        for node in fr.plan.nodes.iter_mut() {
            if let Some(ref mut hj) = node.hash_join_node
                && let Some(ref mut rf_descs) = hj.build_runtime_filters
            {
                for desc in rf_descs.iter_mut() {
                    if desc.has_remote_targets == Some(true) {
                        desc.runtime_filter_merge_nodes = Some(vec![merge_addr.clone()]);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// In-flight instance tracking (per-backend cancellation)
// ---------------------------------------------------------------------------

/// Tracks submitted fragment instances grouped by backend so that, on any
/// failure, cancellation can fan out to every backend that accepted work.
#[derive(Default)]
pub(crate) struct InFlightTracker {
    pub(crate) by_backend: BTreeMap<usize, Vec<types::TUniqueId>>,
}

impl InFlightTracker {
    /// Record that `finst_id` was submitted to `backend_idx`.
    pub(crate) fn record_submitted(&mut self, backend_idx: usize, finst_id: types::TUniqueId) {
        self.by_backend
            .entry(backend_idx)
            .or_default()
            .push(finst_id);
    }

    /// Cancel every recorded instance on its backend. Idempotent.
    pub(crate) fn cancel_all(&self, dispatcher: &dyn FragmentDispatcher) {
        for (idx, ids) in &self.by_backend {
            dispatcher.cancel_fragments(*idx, ids);
        }
    }
}

pub(crate) fn poll_write_failure_and_cancel(
    write: &Arc<Mutex<WriteCoordinator>>,
    tracker: &InFlightTracker,
    dispatcher: &dyn FragmentDispatcher,
) -> Result<(), String> {
    let reason = {
        write
            .lock()
            .expect("write coordinator lock")
            .failed_reason()
    };
    let Some(reason) = reason else {
        return Ok(());
    };

    tracker.cancel_all(dispatcher);
    write
        .lock()
        .expect("write coordinator lock")
        .mark_canceled_except_finished(reason.clone());
    Err(reason)
}

#[derive(Default)]
struct StandaloneQueryFailureRegistry {
    active: BTreeSet<(i64, i64)>,
    failures: BTreeMap<(i64, i64), String>,
}

fn standalone_query_failures() -> &'static Mutex<StandaloneQueryFailureRegistry> {
    static REGISTRY: OnceLock<Mutex<StandaloneQueryFailureRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(StandaloneQueryFailureRegistry::default()))
}

fn query_failure_key(query_id: &types::TUniqueId) -> (i64, i64) {
    (query_id.hi, query_id.lo)
}

pub(crate) fn record_standalone_query_failure(
    query_id: crate::runtime::query_context::QueryId,
    error: String,
) {
    let key = (query_id.hi, query_id.lo);
    let mut guard = standalone_query_failures()
        .lock()
        .expect("standalone query failure registry lock");
    if guard.active.contains(&key) {
        guard.failures.entry(key).or_insert(error);
    }
}

fn take_standalone_query_failure(query_id: &types::TUniqueId) -> Option<String> {
    standalone_query_failures()
        .lock()
        .expect("standalone query failure registry lock")
        .failures
        .remove(&query_failure_key(query_id))
}

struct StandaloneQueryFailureGuard {
    key: (i64, i64),
}

impl StandaloneQueryFailureGuard {
    fn register(query_id: &types::TUniqueId) -> Self {
        let key = query_failure_key(query_id);
        let mut guard = standalone_query_failures()
            .lock()
            .expect("standalone query failure registry lock");
        guard.failures.remove(&key);
        guard.active.insert(key);
        Self { key }
    }
}

impl Drop for StandaloneQueryFailureGuard {
    fn drop(&mut self) {
        let mut guard = standalone_query_failures()
            .lock()
            .expect("standalone query failure registry lock");
        guard.active.remove(&self.key);
        guard.failures.remove(&self.key);
    }
}

#[derive(Debug)]
pub(crate) struct SubmitAndFetchResult {
    pub(crate) chunks: Vec<crate::exec::chunk::Chunk>,
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) write_abort: Option<WriteAbortInput>,
}

struct QueryStateRegistrationGuard {
    query_id: crate::runtime::query_context::QueryId,
}

impl Drop for QueryStateRegistrationGuard {
    fn drop(&mut self) {
        crate::runtime::query_state::in_flight_table().forget(self.query_id);
    }
}

// ---------------------------------------------------------------------------
// Submit-and-fetch orchestration (testable helper)
// ---------------------------------------------------------------------------

/// Submit each `(backend_idx, params)` through the dispatcher in order, tracking
/// accepted instances per backend, then poll the root fragment until EOF.
///
/// On any submit failure or fetch error, all already-submitted instances are
/// cancelled (fanned out per backend) before the error is returned.
pub(crate) fn submit_and_fetch_loop(
    dispatcher: &Arc<dyn FragmentDispatcher>,
    tracker: &mut InFlightTracker,
    submissions: Vec<(usize, crate::internal_service::TExecPlanFragmentParams)>,
    root_backend_idx: usize,
    root_finst_id: types::TUniqueId,
    query_id: &types::TUniqueId,
    timeout_ms: i64,
    write_coordinator: Option<&Arc<Mutex<WriteCoordinator>>>,
) -> Result<SubmitAndFetchResult, String> {
    const REMOTE_FETCH_POLL_INTERVAL_MS: i64 = 300;
    let root_uses_result_buffer = root_uses_result_buffer(&submissions, &root_finst_id)?;
    let runtime_query_id = crate::runtime::query_context::QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };
    let _query_state_guard = QueryStateRegistrationGuard {
        query_id: runtime_query_id,
    };
    let _failure_guard = StandaloneQueryFailureGuard::register(&query_id);

    for (backend_idx, p) in submissions {
        let finst_id = p
            .params
            .as_ref()
            .map(|ep| types::TUniqueId::new(ep.fragment_instance_id.hi, ep.fragment_instance_id.lo))
            .unwrap_or_else(|| types::TUniqueId::new(0, 0));
        if let Err(e) = dispatcher.submit_fragment(backend_idx, p) {
            tracker.cancel_all(dispatcher.as_ref());
            return Err(e);
        }
        crate::service::metrics_http::observe_fragment_scheduled();
        if let Some(registry) = crate::runtime::backend_registry::backend_registry() {
            registry
                .record_scheduled_fragment(backend_idx as crate::runtime::backend_registry::BeId);
        }
        tracker.record_submitted(backend_idx, finst_id.clone());
        crate::runtime::query_state::in_flight_table().register(
            crate::runtime::query_context::QueryId {
                hi: query_id.hi,
                lo: query_id.lo,
            },
            crate::common::types::UniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            },
            backend_idx,
        );
    }

    let mut chunks = Vec::new();
    let timeout = std::time::Duration::from_millis(timeout_ms.max(0) as u64);
    let deadline = std::time::Instant::now() + timeout;
    if root_uses_result_buffer {
        loop {
            if let Some(write) = write_coordinator {
                if let Err(e) = poll_write_failure_and_cancel(write, tracker, dispatcher.as_ref()) {
                    let abort = write.lock().expect("write coordinator lock").abort_input();
                    let Some(abort) = abort else {
                        return Err(e);
                    };
                    return Ok(SubmitAndFetchResult {
                        chunks,
                        write_commit: None,
                        write_abort: Some(abort),
                    });
                }
            }
            if let Some(err) = take_standalone_query_failure(&query_id) {
                tracker.cancel_all(dispatcher.as_ref());
                return Err(err);
            }
            if crate::runtime::query_cancel::client_disconnected() {
                tracker.cancel_all(dispatcher.as_ref());
                return Err("client disconnected".to_string());
            }
            if crate::runtime::query_state::in_flight_table().state(runtime_query_id)
                == Some(QueryState::Failed)
            {
                let reason = crate::runtime::query_state::in_flight_table()
                    .failure_reason(runtime_query_id)
                    .unwrap_or_else(|| format!("query {} failed", runtime_query_id));
                tracker.cancel_all(dispatcher.as_ref());
                return Err(reason);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                tracker.cancel_all(dispatcher.as_ref());
                return Err(format!("query timed out after {timeout_ms} ms"));
            }
            let remaining_ms = deadline
                .saturating_duration_since(now)
                .as_millis()
                .min(i64::MAX as u128) as i64;
            let fetch_wait_ms = remaining_ms.clamp(1, REMOTE_FETCH_POLL_INTERVAL_MS);
            match dispatcher.fetch_result(root_backend_idx, root_finst_id.clone(), fetch_wait_ms) {
                Err(e) => {
                    tracker.cancel_all(dispatcher.as_ref());
                    return Err(e);
                }
                Ok(FetchOutcome::Ready(chunk)) => chunks.push(chunk),
                Ok(FetchOutcome::NotReady) => continue,
                Ok(FetchOutcome::Eof) => break,
                Ok(FetchOutcome::Err(e)) => {
                    tracker.cancel_all(dispatcher.as_ref());
                    return Err(e);
                }
            }
        }
    } else if write_coordinator.is_none() {
        tracker.cancel_all(dispatcher.as_ref());
        return Err(format!(
            "root fragment {}/{} does not produce a result buffer and has no write coordinator",
            root_finst_id.hi, root_finst_id.lo
        ));
    } else if crate::runtime::query_cancel::client_disconnected() {
        tracker.cancel_all(dispatcher.as_ref());
        return Err("client disconnected".to_string());
    } else if std::time::Instant::now() >= deadline {
        tracker.cancel_all(dispatcher.as_ref());
        return Err(format!("query timed out after {timeout_ms} ms"));
    }

    let (write_commit, write_abort) = if let Some(write) = write_coordinator {
        match wait_for_write_commit_ready(write, tracker, dispatcher.as_ref(), deadline, timeout_ms)
        {
            Ok(commit) => (Some(commit), None),
            Err(e) => {
                let abort = write.lock().expect("write coordinator lock").abort_input();
                let Some(abort) = abort else {
                    return Err(e);
                };
                (None, Some(abort))
            }
        }
    } else {
        (None, None)
    };

    Ok(SubmitAndFetchResult {
        chunks,
        write_commit,
        write_abort,
    })
}

fn wait_for_write_commit_ready(
    write: &Arc<Mutex<WriteCoordinator>>,
    tracker: &InFlightTracker,
    dispatcher: &dyn FragmentDispatcher,
    deadline: std::time::Instant,
    timeout_ms: i64,
) -> Result<WriteCommitInput, String> {
    const WRITE_COMMIT_POLL_INTERVAL_MS: i64 = 10;

    loop {
        poll_write_failure_and_cancel(write, tracker, dispatcher)?;

        if crate::runtime::query_cancel::client_disconnected() {
            tracker.cancel_all(dispatcher);
            return Err("client disconnected".to_string());
        }

        let commit_error = match validate_write_commit_ready(write) {
            Ok(commit) => return Ok(commit),
            Err(e) => e,
        };
        #[cfg(test)]
        notify_write_commit_wait_observer(&commit_error);

        let now = std::time::Instant::now();
        if now >= deadline {
            let reason = format!(
                "query timed out after {timeout_ms} ms waiting for write final reports: {commit_error}"
            );
            tracker.cancel_all(dispatcher);
            write
                .lock()
                .expect("write coordinator lock")
                .mark_canceled_except_finished(reason.clone());
            return Err(reason);
        }

        let remaining_ms = deadline
            .saturating_duration_since(now)
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let sleep_ms = remaining_ms.clamp(1, WRITE_COMMIT_POLL_INTERVAL_MS);
        std::thread::sleep(std::time::Duration::from_millis(sleep_ms as u64));
    }
}

#[cfg(test)]
struct WriteCommitWaitObserverGuard;

#[cfg(test)]
struct WriteCommitWaitObserver {
    expected_error_substring: String,
    tx: std::sync::mpsc::Sender<String>,
}

#[cfg(test)]
impl Drop for WriteCommitWaitObserverGuard {
    fn drop(&mut self) {
        *write_commit_wait_observer()
            .lock()
            .expect("write commit wait observer lock") = None;
    }
}

#[cfg(test)]
fn write_commit_wait_observer() -> &'static Mutex<Option<WriteCommitWaitObserver>> {
    static OBSERVER: std::sync::OnceLock<Mutex<Option<WriteCommitWaitObserver>>> =
        std::sync::OnceLock::new();
    OBSERVER.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn set_write_commit_wait_observer(
    expected_error_substring: impl Into<String>,
    tx: std::sync::mpsc::Sender<String>,
) -> WriteCommitWaitObserverGuard {
    let mut observer = write_commit_wait_observer()
        .lock()
        .expect("write commit wait observer lock");
    assert!(
        observer.is_none(),
        "write commit wait observer already registered"
    );
    *observer = Some(WriteCommitWaitObserver {
        expected_error_substring: expected_error_substring.into(),
        tx,
    });
    WriteCommitWaitObserverGuard
}

#[cfg(test)]
fn notify_write_commit_wait_observer(commit_error: &str) {
    let observer = write_commit_wait_observer()
        .lock()
        .expect("write commit wait observer lock");
    if let Some(observer) = observer.as_ref()
        && commit_error.contains(&observer.expected_error_substring)
    {
        let _ = observer.tx.send(commit_error.to_string());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::runtime::dispatcher::{FetchOutcome, FragmentDispatcher};
    use crate::runtime::write_coordinator::{
        FragmentExecStatusReport, WriteCoordinator, WriterKey, write_registry_test_guard,
    };
    use crate::{status, status_code};
    use arrow::array::{
        BinaryArray, Decimal128Array, FixedSizeBinaryArray, Int32Array, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn coerces_remote_binary_fetch_chunk_to_root_output_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_0", DataType::Binary, true),
            Field::new("col_1", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(BinaryArray::from_vec(vec![b"east".as_slice()])),
                Arc::new(BinaryArray::from_vec(vec![&[1_u8, 2, 3][..]])),
            ],
        )
        .expect("remote binary batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            schema.as_ref(),
            &[SlotId::new(0), SlotId::new(1)],
        )
        .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let columns = vec![
            crate::sql::codegen::OutputColumn {
                name: "region".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            crate::sql::codegen::OutputColumn {
                name: "__agg_state_c".to_string(),
                data_type: DataType::Binary,
                nullable: false,
            },
        ];

        let chunks =
            coerce_fetch_chunks_to_output_columns(vec![chunk], &columns).expect("coerce chunks");
        let batch = &chunks[0].batch;
        assert_eq!(batch.schema().field(0).name(), "region");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(1).name(), "__agg_state_c");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Binary);
        let region = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region string array");
        assert_eq!(region.value(0), "east");
        let state = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("state binary array");
        assert_eq!(state.value(0), &[1_u8, 2, 3]);
    }

    #[test]
    fn keeps_remote_complex_fetch_chunk_as_mysql_text() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            DataType::Binary,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from_vec(vec![
                b"{\"a\":1}".as_slice(),
            ]))],
        )
        .expect("remote binary batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(0)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let columns = vec![crate::sql::codegen::OutputColumn {
            name: "payload".to_string(),
            data_type: DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into()),
            nullable: true,
        }];

        let chunks =
            coerce_fetch_chunks_to_output_columns(vec![chunk], &columns).expect("coerce chunks");
        let batch = &chunks[0].batch;
        assert_eq!(batch.schema().field(0).name(), "payload");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        let payload = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload string array");
        assert_eq!(payload.value(0), "{\"a\":1}");
    }

    #[test]
    fn remote_fetch_chunk_schema_allows_actual_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            DataType::Binary,
            true,
        )]));
        let values: Vec<Option<&[u8]>> = vec![Some(b"1".as_slice()), None];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from(values))],
        )
        .expect("remote binary batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(0)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let columns = vec![crate::sql::codegen::OutputColumn {
            name: "col1".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }];

        let chunks =
            coerce_fetch_chunks_to_output_columns(vec![chunk], &columns).expect("coerce chunks");
        let batch = &chunks[0].batch;
        assert!(batch.schema().field(0).is_nullable());
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        assert_eq!(values.value(0), 1);
        assert!(values.is_null(1));
    }

    #[test]
    fn coerces_remote_null_typed_fetch_chunk_to_null_array() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            DataType::Binary,
            true,
        )]));
        let values: Vec<Option<&[u8]>> = vec![None, None, None];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from(values))],
        )
        .expect("remote binary batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(0)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let columns = vec![crate::sql::codegen::OutputColumn {
            name: "only_null".to_string(),
            data_type: DataType::Null,
            nullable: true,
        }];

        let chunks =
            coerce_fetch_chunks_to_output_columns(vec![chunk], &columns).expect("coerce chunks");
        let batch = &chunks[0].batch;
        assert_eq!(batch.schema().field(0).name(), "only_null");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Null);
        assert_eq!(batch.column(0).len(), 3);
    }

    #[test]
    fn coerces_remote_largeint_fetch_chunk_from_mysql_text() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            DataType::Binary,
            true,
        )]));
        let values: Vec<Option<&[u8]>> =
            vec![Some(b"128".as_slice()), Some(b"-5".as_slice()), None];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from(values))],
        )
        .expect("remote binary batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(0)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let columns = vec![crate::sql::codegen::OutputColumn {
            name: "big_value".to_string(),
            data_type: DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH),
            nullable: true,
        }];

        let chunks =
            coerce_fetch_chunks_to_output_columns(vec![chunk], &columns).expect("coerce chunks");
        let batch = &chunks[0].batch;
        assert_eq!(batch.schema().field(0).name(), "big_value");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(crate::common::largeint::LARGEINT_BYTE_WIDTH)
        );
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("largeint array");
        assert_eq!(
            crate::common::largeint::i128_from_be_bytes(values.value(0)).unwrap(),
            128
        );
        assert_eq!(
            crate::common::largeint::i128_from_be_bytes(values.value(1)).unwrap(),
            -5
        );
        assert!(values.is_null(2));
    }

    #[test]
    fn coerces_remote_decimal_fetch_chunk_without_declared_precision_loss() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_0",
            DataType::Binary,
            true,
        )]));
        let values: Vec<Option<&[u8]>> = vec![
            Some(b"1000000000000000000.00".as_slice()),
            Some(b"1.23".as_slice()),
            None,
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from(values))],
        )
        .expect("remote binary batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(0)])
                .expect("chunk schema");
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).expect("chunk");

        let columns = vec![crate::sql::codegen::OutputColumn {
            name: "price".to_string(),
            data_type: DataType::Decimal128(20, 2),
            nullable: true,
        }];

        let chunks =
            coerce_fetch_chunks_to_output_columns(vec![chunk], &columns).expect("coerce chunks");
        let batch = &chunks[0].batch;
        assert_eq!(batch.schema().field(0).name(), "price");
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(38, 2)
        );
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal array");
        assert_eq!(values.value(0), 100_000_000_000_000_000_000_i128);
        assert_eq!(values.value(1), 123_i128);
        assert!(values.is_null(2));
    }

    // -----------------------------------------------------------------------
    // Simple mock (all-success, Eof on fetch)
    // -----------------------------------------------------------------------

    /// Mock dispatcher that records submitted fragment instance IDs and
    /// immediately returns `Eof` for `fetch_result`.
    struct MockDispatcher {
        submitted_finst_ids: Mutex<Vec<(i64, i64)>>,
    }

    impl MockDispatcher {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                submitted_finst_ids: Mutex::new(Vec::new()),
            })
        }

        fn submitted_count(&self) -> usize {
            self.submitted_finst_ids.lock().unwrap().len()
        }
    }

    impl FragmentDispatcher for MockDispatcher {
        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::internal_service::TExecPlanFragmentParams,
        ) -> Result<(), String> {
            let finst = params
                .params
                .as_ref()
                .map(|p| (p.fragment_instance_id.hi, p.fragment_instance_id.lo))
                .unwrap_or((0, 0));
            self.submitted_finst_ids.lock().unwrap().push(finst);
            Ok(())
        }

        fn fetch_result(
            &self,
            _backend_idx: usize,
            _finst_id: types::TUniqueId,
            _max_wait_ms: i64,
        ) -> Result<FetchOutcome, String> {
            Ok(FetchOutcome::Eof)
        }

        fn cancel_fragments(&self, _backend_idx: usize, _finst_ids: &[types::TUniqueId]) {}

        fn backend_count(&self) -> usize {
            1
        }
    }

    // -----------------------------------------------------------------------
    // Controllable mock for I2 / I3 / I4 scenarios
    // -----------------------------------------------------------------------

    enum FetchBehavior {
        Eof,
        Err(String),
        NotReady,
    }

    struct ControllableDispatcher {
        /// All submitted finst ids (in order).
        submitted: Mutex<Vec<types::TUniqueId>>,
        /// All cancelled finst ids (accumulated across cancel_fragments calls).
        cancelled: Mutex<Vec<types::TUniqueId>>,
        /// Number of submits completed so far.
        submit_count: AtomicUsize,
        /// Number of fetch_result calls completed so far.
        fetch_count: AtomicUsize,
        /// Fail when submit_count reaches this value (1-indexed).
        fail_on_submit: Option<usize>,
        fetch_behavior: FetchBehavior,
    }

    impl ControllableDispatcher {
        fn succeeds_always_eof() -> Arc<Self> {
            Arc::new(Self {
                submitted: Mutex::new(Vec::new()),
                cancelled: Mutex::new(Vec::new()),
                submit_count: AtomicUsize::new(0),
                fetch_count: AtomicUsize::new(0),
                fail_on_submit: None,
                fetch_behavior: FetchBehavior::Eof,
            })
        }

        fn fails_on_submit_n(n: usize) -> Arc<Self> {
            Arc::new(Self {
                submitted: Mutex::new(Vec::new()),
                cancelled: Mutex::new(Vec::new()),
                submit_count: AtomicUsize::new(0),
                fetch_count: AtomicUsize::new(0),
                fail_on_submit: Some(n),
                fetch_behavior: FetchBehavior::Eof,
            })
        }

        fn fetch_returns_err(msg: impl Into<String>) -> Arc<Self> {
            Arc::new(Self {
                submitted: Mutex::new(Vec::new()),
                cancelled: Mutex::new(Vec::new()),
                submit_count: AtomicUsize::new(0),
                fetch_count: AtomicUsize::new(0),
                fail_on_submit: None,
                fetch_behavior: FetchBehavior::Err(msg.into()),
            })
        }

        fn fetch_returns_not_ready() -> Arc<Self> {
            Arc::new(Self {
                submitted: Mutex::new(Vec::new()),
                cancelled: Mutex::new(Vec::new()),
                submit_count: AtomicUsize::new(0),
                fetch_count: AtomicUsize::new(0),
                fail_on_submit: None,
                fetch_behavior: FetchBehavior::NotReady,
            })
        }

        fn submitted_ids(&self) -> Vec<types::TUniqueId> {
            self.submitted.lock().unwrap().clone()
        }

        fn cancelled_ids(&self) -> Vec<types::TUniqueId> {
            self.cancelled.lock().unwrap().clone()
        }

        fn fetch_count(&self) -> usize {
            self.fetch_count.load(Ordering::SeqCst)
        }
    }

    struct QueryStateFailureDispatcher {
        submitted: Mutex<Vec<types::TUniqueId>>,
        cancelled: Mutex<Vec<types::TUniqueId>>,
        fetch_count: AtomicUsize,
        first_fetch: AtomicBool,
        failure_reason: String,
    }

    impl QueryStateFailureDispatcher {
        fn new(reason: impl Into<String>) -> Arc<Self> {
            Arc::new(Self {
                submitted: Mutex::new(Vec::new()),
                cancelled: Mutex::new(Vec::new()),
                fetch_count: AtomicUsize::new(0),
                first_fetch: AtomicBool::new(true),
                failure_reason: reason.into(),
            })
        }

        fn cancelled_ids(&self) -> Vec<types::TUniqueId> {
            self.cancelled.lock().unwrap().clone()
        }

        fn submitted_ids(&self) -> Vec<types::TUniqueId> {
            self.submitted.lock().unwrap().clone()
        }

        fn fetch_count(&self) -> usize {
            self.fetch_count.load(Ordering::SeqCst)
        }
    }

    struct RecordingWaitDispatcher {
        submitted: Mutex<Vec<types::TUniqueId>>,
        fetch_waits_ms: Mutex<Vec<i64>>,
        fetch_count: AtomicUsize,
    }

    struct EofSignalDispatcher {
        submitted: Mutex<Vec<types::TUniqueId>>,
        cancelled: Mutex<Vec<types::TUniqueId>>,
        eof_tx: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    }

    impl RecordingWaitDispatcher {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                submitted: Mutex::new(Vec::new()),
                fetch_waits_ms: Mutex::new(Vec::new()),
                fetch_count: AtomicUsize::new(0),
            })
        }

        fn fetch_waits_ms(&self) -> Vec<i64> {
            self.fetch_waits_ms.lock().unwrap().clone()
        }
    }

    impl EofSignalDispatcher {
        fn new(eof_tx: std::sync::mpsc::Sender<()>) -> Arc<Self> {
            Arc::new(Self {
                submitted: Mutex::new(Vec::new()),
                cancelled: Mutex::new(Vec::new()),
                eof_tx: Mutex::new(Some(eof_tx)),
            })
        }

        fn cancelled_ids(&self) -> Vec<types::TUniqueId> {
            self.cancelled.lock().unwrap().clone()
        }
    }

    impl FragmentDispatcher for ControllableDispatcher {
        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::internal_service::TExecPlanFragmentParams,
        ) -> Result<(), String> {
            let n = self.submit_count.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_on_submit == Some(n) {
                return Err(format!("mock: submit failed on call {n}"));
            }
            let finst_id = params
                .params
                .as_ref()
                .map(|ep| {
                    types::TUniqueId::new(ep.fragment_instance_id.hi, ep.fragment_instance_id.lo)
                })
                .unwrap_or_else(|| types::TUniqueId::new(0, 0));
            self.submitted.lock().unwrap().push(finst_id);
            Ok(())
        }

        fn fetch_result(
            &self,
            _backend_idx: usize,
            _finst_id: types::TUniqueId,
            _max_wait_ms: i64,
        ) -> Result<FetchOutcome, String> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            match &self.fetch_behavior {
                FetchBehavior::Eof => Ok(FetchOutcome::Eof),
                FetchBehavior::Err(msg) => Ok(FetchOutcome::Err(msg.clone())),
                FetchBehavior::NotReady => Ok(FetchOutcome::NotReady),
            }
        }

        fn cancel_fragments(&self, _backend_idx: usize, finst_ids: &[types::TUniqueId]) {
            self.cancelled.lock().unwrap().extend_from_slice(finst_ids);
        }

        fn backend_count(&self) -> usize {
            1
        }
    }

    impl FragmentDispatcher for QueryStateFailureDispatcher {
        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::internal_service::TExecPlanFragmentParams,
        ) -> Result<(), String> {
            let finst_id = params
                .params
                .as_ref()
                .map(|ep| {
                    types::TUniqueId::new(ep.fragment_instance_id.hi, ep.fragment_instance_id.lo)
                })
                .unwrap_or_else(|| types::TUniqueId::new(0, 0));
            self.submitted.lock().unwrap().push(finst_id);
            Ok(())
        }

        fn fetch_result(
            &self,
            _backend_idx: usize,
            finst_id: types::TUniqueId,
            _max_wait_ms: i64,
        ) -> Result<FetchOutcome, String> {
            self.fetch_count.fetch_add(1, Ordering::SeqCst);
            if self.first_fetch.swap(false, Ordering::SeqCst) {
                let finst = crate::common::types::UniqueId {
                    hi: finst_id.hi,
                    lo: finst_id.lo,
                };
                crate::runtime::query_state::in_flight_table()
                    .on_fragment_done(finst, Err(self.failure_reason.clone()));
                Ok(FetchOutcome::NotReady)
            } else {
                panic!("fetch_result called after query state failure should have been observed");
            }
        }

        fn cancel_fragments(&self, _backend_idx: usize, finst_ids: &[types::TUniqueId]) {
            self.cancelled.lock().unwrap().extend_from_slice(finst_ids);
        }

        fn backend_count(&self) -> usize {
            1
        }
    }

    impl FragmentDispatcher for RecordingWaitDispatcher {
        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::internal_service::TExecPlanFragmentParams,
        ) -> Result<(), String> {
            let finst_id = params
                .params
                .as_ref()
                .map(|ep| {
                    types::TUniqueId::new(ep.fragment_instance_id.hi, ep.fragment_instance_id.lo)
                })
                .unwrap_or_else(|| types::TUniqueId::new(0, 0));
            self.submitted.lock().unwrap().push(finst_id);
            Ok(())
        }

        fn fetch_result(
            &self,
            _backend_idx: usize,
            _finst_id: types::TUniqueId,
            max_wait_ms: i64,
        ) -> Result<FetchOutcome, String> {
            self.fetch_waits_ms.lock().unwrap().push(max_wait_ms);
            let call = self.fetch_count.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Ok(FetchOutcome::NotReady)
            } else {
                Ok(FetchOutcome::Eof)
            }
        }

        fn cancel_fragments(&self, _backend_idx: usize, _finst_ids: &[types::TUniqueId]) {}

        fn backend_count(&self) -> usize {
            1
        }
    }

    impl FragmentDispatcher for EofSignalDispatcher {
        fn submit_fragment(
            &self,
            _backend_idx: usize,
            params: crate::internal_service::TExecPlanFragmentParams,
        ) -> Result<(), String> {
            let finst_id = params
                .params
                .as_ref()
                .map(|ep| {
                    types::TUniqueId::new(ep.fragment_instance_id.hi, ep.fragment_instance_id.lo)
                })
                .unwrap_or_else(|| types::TUniqueId::new(0, 0));
            self.submitted.lock().unwrap().push(finst_id);
            Ok(())
        }

        fn fetch_result(
            &self,
            _backend_idx: usize,
            _finst_id: types::TUniqueId,
            _max_wait_ms: i64,
        ) -> Result<FetchOutcome, String> {
            if let Some(tx) = self.eof_tx.lock().unwrap().take() {
                tx.send(()).expect("signal root EOF");
            }
            Ok(FetchOutcome::Eof)
        }

        fn cancel_fragments(&self, _backend_idx: usize, finst_ids: &[types::TUniqueId]) {
            self.cancelled.lock().unwrap().extend_from_slice(finst_ids);
        }

        fn backend_count(&self) -> usize {
            1
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_params_with_finst(
        hi: i64,
        lo: i64,
    ) -> crate::internal_service::TExecPlanFragmentParams {
        use crate::{data_sinks, internal_service, partitions, types};

        let result_sink = data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::RESULT_SINK,
            None::<data_sinks::TDataStreamSink>,
            None::<data_sinks::TResultSink>,
            None::<data_sinks::TMysqlTableSink>,
            None::<data_sinks::TExportSink>,
            None::<data_sinks::TOlapTableSink>,
            None::<data_sinks::TMemoryScratchSink>,
            None::<data_sinks::TMultiCastDataStreamSink>,
            None::<data_sinks::TSchemaTableSink>,
            None::<data_sinks::TIcebergTableSink>,
            None::<data_sinks::THiveTableSink>,
            None::<data_sinks::TTableFunctionTableSink>,
            None::<data_sinks::TDictionaryCacheSink>,
            None::<Vec<Box<data_sinks::TDataSink>>>,
            None::<i64>,
            None::<data_sinks::TSplitDataStreamSink>,
        );
        let fragment = crate::planner::TPlanFragment::new(
            None::<crate::plan_nodes::TPlan>,
            None::<Vec<crate::exprs::TExpr>>,
            Some(result_sink),
            partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ),
            None::<i64>,
            None::<i64>,
            None::<Vec<crate::data::TGlobalDict>>,
            None::<Vec<crate::data::TGlobalDict>>,
            None::<crate::planner::TCacheParam>,
            None::<std::collections::BTreeMap<i32, crate::exprs::TExpr>>,
            None::<crate::planner::TGroupExecutionParam>,
        );
        let exec_params = internal_service::TPlanFragmentExecParams {
            query_id: types::TUniqueId::new(hi, lo),
            fragment_instance_id: types::TUniqueId::new(hi, lo),
            per_node_scan_ranges: Default::default(),
            per_exch_num_senders: Default::default(),
            destinations: None,
            sender_id: None,
            num_senders: None,
            send_query_statistics_with_every_batch: None,
            use_vectorized: None,
            runtime_filter_params: None,
            instances_number: None,
            enable_exchange_pass_through: None,
            node_to_per_driver_seq_scan_ranges: None,
            enable_exchange_perf: None,
            pipeline_sink_dop: None,
            report_when_finish: None,
            exec_debug_options: None,
        };
        internal_service::TExecPlanFragmentParams::new(
            internal_service::InternalServiceVersion::V1,
            Some(fragment),
            None::<crate::descriptors::TDescriptorTable>,
            Some(exec_params),
            None::<types::TNetworkAddress>,
            None::<i32>,
            None::<internal_service::TQueryGlobals>,
            None::<internal_service::TQueryOptions>,
            None::<bool>,
            None::<types::TResourceInfo>,
            None::<String>,
            None::<String>,
            None::<i64>,
            None::<internal_service::TLoadErrorHubInfo>,
            None::<bool>,
            None::<i32>,
            None::<std::collections::BTreeMap<types::TPlanNodeId, i32>>,
            None::<crate::work_group::TWorkGroup>,
            None::<bool>,
            None::<i32>,
            None::<bool>,
            None::<bool>,
            None::<internal_service::TAdaptiveDopParam>,
            None::<i32>,
            None::<internal_service::TPredicateTreeParams>,
            None::<Vec<i32>>,
            None::<i32>,
            None::<types::TNetworkAddress>,
        )
    }

    fn make_params_with_sink_type(
        sink_type: data_sinks::TDataSinkType,
    ) -> crate::internal_service::TExecPlanFragmentParams {
        let mut params = make_params_with_finst(30, 40);
        params
            .fragment
            .as_mut()
            .expect("fragment")
            .output_sink
            .as_mut()
            .expect("output sink")
            .type_ = sink_type;
        params
    }

    fn make_params_with_finst_and_sink_type(
        hi: i64,
        lo: i64,
        sink_type: data_sinks::TDataSinkType,
    ) -> crate::internal_service::TExecPlanFragmentParams {
        let mut params = make_params_with_finst(hi, lo);
        params
            .fragment
            .as_mut()
            .expect("fragment")
            .output_sink
            .as_mut()
            .expect("output sink")
            .type_ = sink_type;
        params
    }

    fn is_write_sink_for_test(params: &crate::internal_service::TExecPlanFragmentParams) -> bool {
        super::is_write_sink(params)
    }

    fn id(hi: i64, lo: i64) -> types::TUniqueId {
        types::TUniqueId::new(hi, lo)
    }

    fn runtime_query_id(hi: i64, lo: i64) -> crate::runtime::query_context::QueryId {
        crate::runtime::query_context::QueryId { hi, lo }
    }

    fn writer_key(
        query_hi: i64,
        query_lo: i64,
        finst_hi: i64,
        finst_lo: i64,
        backend_num: i32,
    ) -> WriterKey {
        WriterKey {
            query_id: id(query_hi, query_lo),
            fragment_instance_id: id(finst_hi, finst_lo),
            backend_num,
        }
    }

    fn ok_status() -> status::TStatus {
        status::TStatus::new(status_code::TStatusCode::OK, None)
    }

    fn err_status(msg: &str) -> status::TStatus {
        status::TStatus::new(
            status_code::TStatusCode::INTERNAL_ERROR,
            Some(vec![msg.to_string()]),
        )
    }

    fn write_report(
        writer: &WriterKey,
        done: bool,
        status: status::TStatus,
        path: &str,
    ) -> FragmentExecStatusReport {
        let sink_commit_infos = if path.is_empty() {
            Vec::new()
        } else {
            vec![types::TSinkCommitInfo {
                iceberg_data_file: Some(types::TIcebergDataFile {
                    path: Some(path.to_string()),
                    record_count: Some(3),
                    file_size_in_bytes: Some(30),
                    ..Default::default()
                }),
                ..Default::default()
            }]
        };
        FragmentExecStatusReport {
            query_id: writer.query_id.clone(),
            fragment_instance_id: writer.fragment_instance_id.clone(),
            backend_num: writer.backend_num,
            done,
            status,
            sink_commit_infos,
            tablet_commit_infos: Vec::new(),
            tablet_fail_infos: Vec::new(),
            load_counters: Default::default(),
            loaded_rows: 3,
            loaded_bytes: 30,
            filtered_rows: 0,
        }
    }

    // -----------------------------------------------------------------------
    // Original regression tests
    // -----------------------------------------------------------------------

    /// Wrap a list of params as single-backend submissions (backend_idx=0).
    fn single_backend(
        params: Vec<crate::internal_service::TExecPlanFragmentParams>,
    ) -> Vec<(usize, crate::internal_service::TExecPlanFragmentParams)> {
        params.into_iter().map(|p| (0usize, p)).collect()
    }

    /// Verify that `ExecutionCoordinator::new` accepts `Arc<dyn FragmentDispatcher>`
    /// (regression guard against re-introduction of exchange_host/port parameters).
    #[test]
    fn constructor_accepts_dispatcher() {
        let dispatcher: Arc<dyn FragmentDispatcher> = MockDispatcher::new();
        // Just verify the trait object is usable for the submit/cancel surface.
        assert_eq!(dispatcher.backend_count(), 1);
    }

    /// Verify that `MockDispatcher::submitted_count` starts at zero.
    #[test]
    fn mock_dispatcher_starts_empty() {
        let d = MockDispatcher::new();
        assert_eq!(d.submitted_count(), 0);
    }

    // -----------------------------------------------------------------------
    // InFlightTracker tests
    // -----------------------------------------------------------------------

    #[test]
    fn in_flight_tracker_groups_by_backend() {
        let mut tracker = InFlightTracker::default();
        tracker.record_submitted(0, types::TUniqueId::new(1, 10));
        tracker.record_submitted(1, types::TUniqueId::new(1, 20));
        tracker.record_submitted(0, types::TUniqueId::new(1, 11));

        assert_eq!(tracker.by_backend.len(), 2, "two distinct backends");
        assert_eq!(
            tracker.by_backend[&0].len(),
            2,
            "backend 0 got two instances"
        );
        assert_eq!(
            tracker.by_backend[&1].len(),
            1,
            "backend 1 got one instance"
        );
    }

    /// Mock dispatcher that records the (backend_idx, finst_ids) of every
    /// cancel_fragments call so cancel fan-out can be asserted.
    struct RecordingCancelDispatcher {
        cancels: Mutex<Vec<(usize, Vec<types::TUniqueId>)>>,
    }

    impl RecordingCancelDispatcher {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                cancels: Mutex::new(Vec::new()),
            })
        }
    }

    impl FragmentDispatcher for RecordingCancelDispatcher {
        fn submit_fragment(
            &self,
            _backend_idx: usize,
            _params: crate::internal_service::TExecPlanFragmentParams,
        ) -> Result<(), String> {
            Ok(())
        }

        fn fetch_result(
            &self,
            _backend_idx: usize,
            _finst_id: types::TUniqueId,
            _max_wait_ms: i64,
        ) -> Result<FetchOutcome, String> {
            Ok(FetchOutcome::Eof)
        }

        fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[types::TUniqueId]) {
            self.cancels
                .lock()
                .unwrap()
                .push((backend_idx, finst_ids.to_vec()));
        }

        fn backend_count(&self) -> usize {
            2
        }
    }

    #[test]
    fn in_flight_tracker_cancel_all_fans_out_per_backend() {
        let dispatcher = RecordingCancelDispatcher::new();
        let mut tracker = InFlightTracker::default();
        tracker.record_submitted(0, types::TUniqueId::new(7, 1));
        tracker.record_submitted(1, types::TUniqueId::new(7, 2));
        tracker.record_submitted(1, types::TUniqueId::new(7, 3));

        tracker.cancel_all(dispatcher.as_ref());

        let cancels = dispatcher.cancels.lock().unwrap();
        assert_eq!(cancels.len(), 2, "one cancel call per backend");
        let backend0 = cancels
            .iter()
            .find(|(idx, _)| *idx == 0)
            .expect("backend 0");
        let backend1 = cancels
            .iter()
            .find(|(idx, _)| *idx == 1)
            .expect("backend 1");
        assert_eq!(backend0.1.len(), 1, "backend 0 cancels 1 instance");
        assert_eq!(backend1.1.len(), 2, "backend 1 cancels 2 instances");
    }

    #[test]
    fn write_sink_detection_marks_supported_write_sinks() {
        for sink_type in [
            data_sinks::TDataSinkType::ICEBERG_TABLE_SINK,
            data_sinks::TDataSinkType::ICEBERG_DELETE_SINK,
            data_sinks::TDataSinkType::HIVE_TABLE_SINK,
            data_sinks::TDataSinkType::OLAP_TABLE_SINK,
        ] {
            let params = make_params_with_sink_type(sink_type);
            assert!(
                is_write_sink_for_test(&params),
                "{sink_type:?} must register with the write coordinator"
            );
        }

        for sink_type in [
            data_sinks::TDataSinkType::NOOP_SINK,
            data_sinks::TDataSinkType::RESULT_SINK,
            data_sinks::TDataSinkType::DATA_STREAM_SINK,
        ] {
            let params = make_params_with_sink_type(sink_type);
            assert!(
                !is_write_sink_for_test(&params),
                "{sink_type:?} must not register with the write coordinator"
            );
        }
    }

    #[test]
    fn write_failure_seen_by_coordinator_cancels_inflight_fragments() {
        let mut guard = write_registry_test_guard();
        let query_id = id(710, 711);
        let writer = writer_key(710, 711, 712, 713, 0);
        let write = guard
            .register_query(query_id.clone(), vec![writer.clone()])
            .expect("register writer");

        write
            .lock()
            .expect("write coordinator lock")
            .apply_report(write_report(&writer, true, err_status("writer failed"), ""))
            .expect("failed writer report");

        let dispatcher = RecordingCancelDispatcher::new();
        let mut tracker = InFlightTracker::default();
        let submitted = id(710, 900);
        tracker.record_submitted(0, submitted.clone());

        let err = poll_write_failure_and_cancel(&write, &tracker, dispatcher.as_ref())
            .expect_err("writer failure must propagate");

        assert!(err.contains("writer failed"), "{err}");
        let cancels = dispatcher.cancels.lock().unwrap();
        assert_eq!(cancels.len(), 1, "writer failure cancels submitted work");
        assert_eq!(cancels[0].0, 0);
        assert_eq!(cancels[0].1, vec![submitted]);
        let commit_err = write
            .lock()
            .expect("write coordinator lock")
            .commit_input()
            .expect_err("failed writer must block commit");
        assert!(commit_err.contains("writer failed"), "{commit_err}");
    }

    #[test]
    fn write_commit_readiness_helper_accepts_finished_writers() {
        let query_id = id(720, 721);
        let writer = writer_key(720, 721, 722, 723, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id.clone(), vec![writer.clone()])
                .expect("write coordinator"),
        ));
        write
            .lock()
            .expect("write coordinator lock")
            .apply_report(write_report(
                &writer,
                true,
                ok_status(),
                "s3://warehouse/data.parquet",
            ))
            .expect("writer report");

        let commit = validate_write_commit_ready(&write).expect("commit input");

        assert_eq!(commit.write_id, query_id);
        assert_eq!(commit.writers.len(), 1);
        assert_eq!(commit.writers[0].writer_key, writer);
    }

    #[test]
    fn write_commit_readiness_helper_rejects_missing_final_report() {
        let query_id = id(730, 731);
        let writer = writer_key(730, 731, 732, 733, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id, vec![writer]).expect("write coordinator"),
        ));

        let err = validate_write_commit_ready(&write)
            .expect_err("missing writer final report must block commit readiness");

        assert!(err.contains("missing writer final report"), "{err}");
    }

    #[test]
    fn delayed_write_final_report_after_root_eof_is_accepted() {
        let query_id = id(740, 741);
        let writer = writer_key(740, 741, 742, 743, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id, vec![writer.clone()]).expect("write coordinator"),
        ));
        let (eof_tx, eof_rx) = std::sync::mpsc::channel();
        let inner = EofSignalDispatcher::new(eof_tx);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner;
        let write_for_report = Arc::clone(&write);
        let writer_for_report = writer.clone();
        let report_thread = std::thread::spawn(move || {
            eof_rx.recv().expect("root EOF signal");
            write_for_report
                .lock()
                .expect("write coordinator lock")
                .apply_report(write_report(
                    &writer_for_report,
                    true,
                    ok_status(),
                    "s3://warehouse/delayed.parquet",
                ))
                .expect("delayed writer report");
        });

        let root_finst_id = types::TUniqueId::new(740, 1);
        let params = single_backend(vec![
            make_params_with_finst(740, 10),
            make_params_with_finst(740, 1),
        ]);
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &types::TUniqueId::new(740, 741),
            1_000,
            Some(&write),
        );

        report_thread.join().expect("delayed report thread");
        assert!(
            result.is_ok(),
            "delayed writer final report after root EOF must be accepted, got {result:?}"
        );
        let output = result.expect("delayed final report succeeds");
        assert!(output.chunks.is_empty());
        assert!(output.write_commit.is_some());
        assert!(output.write_abort.is_none());
    }

    #[test]
    fn write_failure_during_post_eof_wait_surfaces_abort_and_cancels_submitted_fragments() {
        let query_id = id(760, 761);
        let writer = writer_key(760, 761, 762, 763, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id.clone(), vec![writer.clone()])
                .expect("write coordinator"),
        ));
        let (eof_tx, _eof_rx) = std::sync::mpsc::channel();
        let inner = EofSignalDispatcher::new(eof_tx);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let (wait_tx, wait_rx) = std::sync::mpsc::channel();
        let _wait_observer = set_write_commit_wait_observer(
            format!("query={}/{}", query_id.hi, query_id.lo),
            wait_tx,
        );
        let write_for_report = Arc::clone(&write);
        let writer_for_report = writer.clone();
        let report_thread = std::thread::spawn(move || {
            let wait_error = wait_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("post-EOF write wait signal");
            assert!(
                wait_error.contains("missing writer final report"),
                "{wait_error}"
            );
            write_for_report
                .lock()
                .expect("write coordinator lock")
                .apply_report(write_report(
                    &writer_for_report,
                    true,
                    err_status("delayed writer failure"),
                    "",
                ))
                .expect("delayed writer failure report");
        });

        let root_finst_id = types::TUniqueId::new(760, 1);
        let params = single_backend(vec![
            make_params_with_finst(760, 10),
            make_params_with_finst(760, 1),
        ]);
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &query_id,
            1_000,
            Some(&write),
        )
        .expect("writer failure during post-EOF wait must surface write abort");

        report_thread.join().expect("delayed report thread");
        assert!(result.write_commit.is_none());
        let abort = result
            .write_abort
            .expect("writer failure must surface write abort");
        assert!(
            abort.reason.contains("delayed writer failure"),
            "{}",
            abort.reason
        );
        assert_eq!(abort.incomplete_writers, vec![writer]);
        assert_eq!(
            inner.cancelled_ids(),
            vec![
                types::TUniqueId::new(760, 10),
                types::TUniqueId::new(760, 1)
            ],
            "post-EOF writer failure must cancel all submitted fragments"
        );
        let commit_err = write
            .lock()
            .expect("write coordinator lock")
            .commit_input()
            .expect_err("failed writer must block commit");
        assert!(
            commit_err.contains("delayed writer failure"),
            "{commit_err}"
        );
    }

    #[test]
    fn write_failure_before_root_eof_surfaces_abort_with_completed_writer_outputs() {
        let query_id = id(780, 781);
        let writer_ok = writer_key(780, 781, 782, 783, 0);
        let writer_failed = writer_key(780, 781, 784, 785, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id, vec![writer_ok.clone(), writer_failed.clone()])
                .expect("write coordinator"),
        ));
        write
            .lock()
            .expect("write coordinator lock")
            .apply_report(write_report(
                &writer_ok,
                true,
                ok_status(),
                "s3://warehouse/pre-eof-ok.parquet",
            ))
            .expect("finished writer report");
        write
            .lock()
            .expect("write coordinator lock")
            .apply_report(write_report(
                &writer_failed,
                true,
                err_status("pre-EOF writer failure"),
                "",
            ))
            .expect("failed writer report");

        let inner = ControllableDispatcher::fetch_returns_not_ready();
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(780, 1);
        let params = single_backend(vec![
            make_params_with_finst(780, 10),
            make_params_with_finst(780, 1),
        ]);
        let mut tracker = InFlightTracker::default();

        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &types::TUniqueId::new(780, 781),
            1_000,
            Some(&write),
        )
        .expect("pre-EOF writer failure must surface write abort");

        assert!(result.write_commit.is_none());
        let abort = result
            .write_abort
            .expect("pre-EOF writer failure must return abort input");
        assert!(
            abort.reason.contains("pre-EOF writer failure"),
            "{}",
            abort.reason
        );
        assert_eq!(abort.completed_writer_outputs.len(), 1);
        assert_eq!(
            abort.completed_writer_outputs[0].sink_commit_infos[0]
                .iceberg_data_file
                .as_ref()
                .and_then(|file| file.path.as_deref()),
            Some("s3://warehouse/pre-eof-ok.parquet")
        );
        assert_eq!(abort.incomplete_writers, vec![writer_failed]);
        assert_eq!(
            inner.cancelled_ids(),
            vec![
                types::TUniqueId::new(780, 10),
                types::TUniqueId::new(780, 1)
            ],
            "pre-EOF writer failure must cancel submitted fragments"
        );
        assert_eq!(
            inner.fetch_count(),
            0,
            "write failure should be observed before the next root fetch"
        );
    }

    #[test]
    fn standalone_report_failure_cancels_inflight_fragments() {
        let inner = ControllableDispatcher::fetch_returns_not_ready();
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(786, 1);
        let params = single_backend(vec![
            make_params_with_finst(786, 10),
            make_params_with_finst(786, 1),
        ]);
        let mut tracker = InFlightTracker::default();

        let report_thread = std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_millis(20));
            record_standalone_query_failure(
                crate::runtime::query_context::QueryId { hi: 786, lo: 1 },
                "remote fragment failed".to_string(),
            );
        });

        let err = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &types::TUniqueId::new(786, 1),
            1_000,
            None,
        )
        .expect_err("standalone report failure must surface before timeout");

        report_thread.join().expect("report thread");
        assert!(err.contains("remote fragment failed"), "{err}");
        assert_eq!(
            inner.cancelled_ids(),
            vec![
                types::TUniqueId::new(786, 10),
                types::TUniqueId::new(786, 1)
            ],
            "standalone report failure must cancel all submitted fragments"
        );
    }

    #[test]
    fn write_only_root_waits_for_commit_without_fetching_result_buffer() {
        let query_id = id(790, 791);
        let writer = writer_key(790, 791, 790, 1, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id.clone(), vec![writer.clone()])
                .expect("write coordinator"),
        ));
        write
            .lock()
            .expect("write coordinator lock")
            .apply_report(write_report(
                &writer,
                true,
                ok_status(),
                "s3://warehouse/write-only-root.parquet",
            ))
            .expect("writer report");

        let inner = ControllableDispatcher::fetch_returns_err("no result for this query");
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = writer.fragment_instance_id.clone();
        let params = single_backend(vec![
            make_params_with_finst(790, 10),
            make_params_with_finst_and_sink_type(
                790,
                1,
                data_sinks::TDataSinkType::ICEBERG_TABLE_SINK,
            ),
        ]);
        let mut tracker = InFlightTracker::default();

        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &query_id,
            1_000,
            Some(&write),
        )
        .expect("write-only root should use writer reports instead of result fetch");

        assert!(result.chunks.is_empty());
        assert!(result.write_commit.is_some());
        assert!(result.write_abort.is_none());
        assert_eq!(
            inner.fetch_count(),
            0,
            "write-only roots do not create result buffers and must not be fetched"
        );
    }

    #[test]
    fn missing_write_final_report_after_root_eof_times_out_and_cancels() {
        let query_id = id(750, 751);
        let writer = writer_key(750, 751, 752, 753, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id, vec![writer.clone()]).expect("write coordinator"),
        ));
        let inner = ControllableDispatcher::succeeds_always_eof();
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(750, 1);
        let params = single_backend(vec![
            make_params_with_finst(750, 10),
            make_params_with_finst(750, 1),
        ]);
        let mut tracker = InFlightTracker::default();

        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &types::TUniqueId::new(750, 751),
            25,
            Some(&write),
        )
        .expect("missing writer final report after EOF must surface write abort");

        assert!(result.write_commit.is_none());
        let abort = result
            .write_abort
            .expect("missing final report timeout must create abort input");
        assert!(abort.reason.contains("timed out"), "{}", abort.reason);
        assert!(
            abort.reason.contains("missing writer final report"),
            "{}",
            abort.reason
        );
        let cancelled = inner.cancelled_ids();
        assert_eq!(
            cancelled.len(),
            2,
            "missing final report timeout must cancel all submitted fragments"
        );
        assert_eq!(abort.completed_writer_outputs.len(), 0);
        assert_eq!(abort.incomplete_writers, vec![writer]);
    }

    #[test]
    fn legacy_query_result_wrapper_returns_write_abort_reason_as_error() {
        let query_result = QueryResult {
            columns: Vec::new(),
            chunks: Vec::new(),
        };
        let err = query_result_or_write_abort_error(CoordinatedQueryResult {
            query_result,
            write_commit: None,
            write_abort: Some(WriteAbortInput {
                write_id: id(770, 771),
                reason: "write abort reason".to_string(),
                completed_writer_outputs: Vec::new(),
                incomplete_writers: Vec::new(),
            }),
        })
        .expect_err("legacy query-result wrapper must not hide write aborts");

        assert_eq!(err, "write abort reason");
    }

    // -----------------------------------------------------------------------
    // I4: submit_and_fetch_loop orchestration tests
    // -----------------------------------------------------------------------

    /// I4: submit_and_fetch_loop submits all fragments in order and returns
    /// empty chunks when the dispatcher returns Eof immediately.
    #[test]
    fn execute_submits_all_fragments_and_fetches_to_eof() {
        let inner = ControllableDispatcher::succeeds_always_eof();
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let query_id = types::TUniqueId::new(1, 99);
        let runtime_query_id = runtime_query_id(query_id.hi, query_id.lo);
        let root_finst_id = types::TUniqueId::new(1, 1);
        let params = single_backend(vec![
            make_params_with_finst(1, 10),
            make_params_with_finst(1, 1),
        ]);
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &query_id,
            100,
            None,
        );
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let output = result.unwrap();
        assert!(
            output.chunks.is_empty(),
            "expected no chunks from Eof dispatcher"
        );
        assert!(
            output.write_commit.is_none(),
            "non-write query should not produce write commit input"
        );
        assert!(
            output.write_abort.is_none(),
            "non-write query should not produce write abort input"
        );
        assert_eq!(
            inner.submitted_ids().len(),
            2,
            "both fragments must be submitted"
        );
        assert_eq!(
            crate::runtime::query_state::in_flight_table().state(runtime_query_id),
            None,
            "submit_and_fetch_loop must forget query_state entries on success"
        );
    }

    /// I2: When the second submit fails, the coordinator cancels the first
    /// fragment instance and returns an error.
    #[test]
    fn execute_cancels_already_submitted_on_submit_failure() {
        let inner = ControllableDispatcher::fails_on_submit_n(2);
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(2, 1);
        let params = single_backend(vec![
            make_params_with_finst(2, 10),
            make_params_with_finst(2, 1),
        ]);
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &types::TUniqueId::new(2, 99),
            100,
            None,
        );
        assert!(result.is_err(), "expected Err on submit failure");
        let submitted = inner.submitted_ids();
        assert_eq!(
            submitted.len(),
            1,
            "only the first fragment should be submitted"
        );
        assert_eq!(submitted[0].hi, 2);
        assert_eq!(submitted[0].lo, 10);
        let cancelled = inner.cancelled_ids();
        assert_eq!(
            cancelled.len(),
            1,
            "the first submitted fragment must be cancelled"
        );
        assert_eq!(cancelled[0].hi, 2);
        assert_eq!(cancelled[0].lo, 10);
        assert_eq!(
            crate::runtime::query_state::in_flight_table().state(runtime_query_id(2, 99)),
            None,
            "submit_and_fetch_loop must forget query_state entries on submit failure"
        );
    }

    /// I3: When fetch returns FetchOutcome::Err, all submitted fragment
    /// instances are cancelled before the error propagates.
    #[test]
    fn execute_cancels_all_submitted_on_fetch_error() {
        let inner = ControllableDispatcher::fetch_returns_err("boom");
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(3, 1);
        let params = single_backend(vec![
            make_params_with_finst(3, 10),
            make_params_with_finst(3, 1),
        ]);
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &types::TUniqueId::new(3, 99),
            100,
            None,
        );
        assert!(result.is_err(), "expected Err on fetch error");
        let err = result.unwrap_err();
        assert!(
            err.contains("boom"),
            "error message should contain 'boom', got: {err}"
        );
        let submitted = inner.submitted_ids();
        assert_eq!(
            submitted.len(),
            2,
            "both fragments must be submitted before fetch"
        );
        let cancelled = inner.cancelled_ids();
        assert_eq!(
            cancelled.len(),
            2,
            "all submitted fragments must be cancelled on fetch error"
        );
    }

    #[test]
    fn query_timeout_triggers_cancel() {
        let inner = ControllableDispatcher::fetch_returns_not_ready();
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(4, 1);
        let params = single_backend(vec![
            make_params_with_finst(4, 10),
            make_params_with_finst(4, 1),
        ]);
        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &types::TUniqueId::new(4, 99),
            10,
            None,
        );
        assert!(result.is_err(), "expected timeout error");
        let err = result.unwrap_err();
        assert!(
            err.contains("timed out"),
            "error should explain timeout, got: {err}"
        );
        assert!(
            inner.fetch_count() > 0,
            "positive timeout must exercise the NotReady fetch loop"
        );
        let cancelled = inner.cancelled_ids();
        assert_eq!(
            cancelled.len(),
            2,
            "all submitted fragments must be cancelled on timeout"
        );
    }

    #[test]
    fn execute_aborts_before_second_fetch_when_query_state_failed() {
        let inner = QueryStateFailureDispatcher::new("remote query failed");
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(8, 1);
        let params = single_backend(vec![
            make_params_with_finst(8, 10),
            make_params_with_finst(8, 1),
        ]);
        let mut tracker = InFlightTracker::default();

        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &types::TUniqueId::new(8, 99),
            100,
            None,
        );

        let err = result.expect_err("query state failure must propagate");
        assert!(
            err.contains("remote query failed"),
            "error should contain failure reason, got: {err}"
        );
        assert_eq!(
            inner.fetch_count(),
            1,
            "coordinator must stop before the second fetch after observing query failure"
        );
        assert_eq!(
            inner.submitted_ids().len(),
            2,
            "both fragments should have been submitted before failure is observed"
        );
        assert_eq!(
            inner.cancelled_ids().len(),
            2,
            "query_state failure must cancel all submitted fragments"
        );
    }

    #[test]
    fn fetch_loop_caps_remote_waits_below_full_timeout() {
        let inner = RecordingWaitDispatcher::new();
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(6, 1);
        let params = single_backend(vec![
            make_params_with_finst(6, 10),
            make_params_with_finst(6, 1),
        ]);

        let mut tracker = InFlightTracker::default();
        let result = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            &types::TUniqueId::new(6, 99),
            300_000,
            None,
        );

        assert!(result.is_ok(), "expected fetch loop to finish after Eof");
        let waits = inner.fetch_waits_ms();
        assert!(
            !waits.is_empty(),
            "expected fetch_result to be called at least once"
        );
        assert!(
            waits[0] <= 500,
            "expected fetch wait to be capped to a short poll, got {} ms",
            waits[0]
        );
    }

    #[test]
    fn client_disconnect_triggers_cancel() {
        let inner = ControllableDispatcher::fetch_returns_not_ready();
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(5, 1);
        let params = single_backend(vec![
            make_params_with_finst(5, 10),
            make_params_with_finst(5, 1),
        ]);
        let disconnected = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let result =
            crate::runtime::query_cancel::with_client_disconnect_signal(disconnected, || {
                let mut tracker = InFlightTracker::default();
                submit_and_fetch_loop(
                    &dispatcher,
                    &mut tracker,
                    params,
                    0,
                    root_finst_id,
                    &types::TUniqueId::new(5, 99),
                    100,
                    None,
                )
            });

        let err = result.expect_err("expected client disconnect error");
        assert!(
            err.contains("client disconnected"),
            "disconnect error should be explicit, got: {err}"
        );
        let cancelled = inner.cancelled_ids();
        assert_eq!(
            cancelled.len(),
            2,
            "all submitted fragments must be cancelled on disconnect"
        );
    }
}
