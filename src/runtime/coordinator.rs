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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::data_sinks;
use crate::novarocks_logging::debug;
use crate::partitions;
use crate::planner;
use crate::runtime::dispatcher::{FetchOutcome, FragmentDispatcher};
use crate::runtime::exec_params::build_exec_plan_fragment_params;
use crate::runtime::scheduler::FragmentScheduler;
use crate::runtime::write_coordinator::{
    WriteCommitInput, WriteCoordinator, WriterKey, register_query, unregister_query,
};
use crate::runtime_filter;
use crate::sql::analysis::cte::CteId;
use crate::sql::codegen::{
    FragmentEdge, FragmentEdgeKind, FragmentId, MultiFragmentBuildResult, RuntimeFilterPlanResult,
};
use crate::types;

use crate::runtime::query_result::{QueryResult, QueryResultColumn};

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

    pub(crate) fn execute(self) -> Result<QueryResult, String> {
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

        let mut plan = scheduler.assign(&fragment_results, &edges, query_id.clone())?;
        scheduler.fill_destinations(&mut plan, &edges);
        if let Some(rf) = rf_plan.as_ref() {
            scheduler.fill_runtime_filter_params(&mut plan, rf);
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
        let merge_addr = backend_to_network_addr(scheduler.backends(), plan.root_backend_idx)?;
        if rf_plan.is_some() {
            inject_runtime_filter_merge_nodes(&mut fragment_results, &merge_addr);
        }

        // ---------------------------------------------------------------
        // 4. Translate every placement into a fragment params and submit.
        // ---------------------------------------------------------------
        let pipeline_dop = crate::runtime::dispatcher::compute_pipeline_dop();
        let novarocks_report_addr = local_coordinator_report_addr().ok();

        // Snapshot the per-consumer-fragment instance destinations for CTE
        // multicast sub-sinks (each consumer fans out to all of its instances).
        let consumer_dests: BTreeMap<FragmentId, Vec<data_sinks::TPlanFragmentDestination>> = plan
            .by_fragment
            .iter()
            .map(|(fid, insts)| {
                let dests: Result<Vec<_>, String> = insts
                    .iter()
                    .map(|inst| {
                        let addr = scheduler.backends().get(inst.backend_idx).ok_or_else(|| {
                            format!(
                                "backend idx {} out of range ({} backends)",
                                inst.backend_idx,
                                scheduler.backends().len()
                            )
                        })?;
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
        // Collect all (backend_idx, params) so submission order is deterministic
        // (non-root fragments first, root last) — matching prior behavior so the
        // half-submit-failure cancel semantics are preserved.
        let mut submissions: Vec<(usize, crate::internal_service::TExecPlanFragmentParams)> =
            Vec::new();
        let mut root_submission: Option<(usize, crate::internal_service::TExecPlanFragmentParams)> =
            None;
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

                let thrift_fragment = planner::TPlanFragment::new(
                    Some(fr.plan.clone()),
                    None::<Vec<crate::exprs::TExpr>>,
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
                    novarocks_report_addr.clone(),
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

                if is_root {
                    root_submission = Some((placement.backend_idx, params));
                } else {
                    submissions.push((placement.backend_idx, params));
                }
            }
        }

        let root_submission =
            root_submission.ok_or_else(|| "root fragment produced no placement".to_string())?;
        // Root last: keep prior submission ordering (producers before root).
        submissions.push(root_submission);

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
        Ok(QueryResult {
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
            chunks: fetch_result.chunks,
        })
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
        .map(|sink| {
            matches!(
                sink.type_,
                data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
                    | data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
                    | data_sinks::TDataSinkType::HIVE_TABLE_SINK
                    | data_sinks::TDataSinkType::OLAP_TABLE_SINK
            )
        })
        .unwrap_or(false)
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
    backends: &[std::net::SocketAddr],
    idx: usize,
) -> Result<types::TNetworkAddress, String> {
    let addr = backends.get(idx).ok_or_else(|| {
        format!(
            "backend index {idx} out of range ({} backends)",
            backends.len()
        )
    })?;
    Ok(types::TNetworkAddress::new(
        addr.ip().to_string(),
        addr.port() as i32,
    ))
}

fn local_coordinator_report_addr() -> Result<types::TNetworkAddress, String> {
    let cfg = crate::novarocks_config::config()
        .map_err(|e| format!("cannot read coordinator config: {e}"))?;
    let host = crate::common::network::advertise_host().unwrap_or_else(|_| cfg.server.host.clone());
    Ok(types::TNetworkAddress::new(
        host,
        cfg.server.http_port as i32,
    ))
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

#[derive(Debug)]
pub(crate) struct SubmitAndFetchResult {
    pub(crate) chunks: Vec<crate::exec::chunk::Chunk>,
    pub(crate) write_commit: Option<WriteCommitInput>,
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
    timeout_ms: i64,
    write_coordinator: Option<&Arc<Mutex<WriteCoordinator>>>,
) -> Result<SubmitAndFetchResult, String> {
    const REMOTE_FETCH_POLL_INTERVAL_MS: i64 = 300;

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
        tracker.record_submitted(backend_idx, finst_id);
    }

    let mut chunks = Vec::new();
    let timeout = std::time::Duration::from_millis(timeout_ms.max(0) as u64);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(write) = write_coordinator {
            poll_write_failure_and_cancel(write, tracker, dispatcher.as_ref())?;
        }
        if crate::runtime::query_cancel::client_disconnected() {
            tracker.cancel_all(dispatcher.as_ref());
            return Err("client disconnected".to_string());
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

    let write_commit = if let Some(write) = write_coordinator {
        Some(wait_for_write_commit_ready(
            write,
            tracker,
            dispatcher.as_ref(),
            deadline,
            timeout_ms,
        )?)
    } else {
        None
    };

    Ok(SubmitAndFetchResult {
        chunks,
        write_commit,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::runtime::dispatcher::{FetchOutcome, FragmentDispatcher};
    use crate::runtime::write_coordinator::{
        FragmentExecStatusReport, WriteCoordinator, WriterKey, register_query, test_clear_registry,
        unregister_query,
    };
    use crate::{status, status_code};

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

    struct RecordingWaitDispatcher {
        submitted: Mutex<Vec<types::TUniqueId>>,
        fetch_waits_ms: Mutex<Vec<i64>>,
        fetch_count: AtomicUsize,
    }

    struct EofSignalDispatcher {
        submitted: Mutex<Vec<types::TUniqueId>>,
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
                eof_tx: Mutex::new(Some(eof_tx)),
            })
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

        fn cancel_fragments(&self, _backend_idx: usize, _finst_ids: &[types::TUniqueId]) {}

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

        let noop_sink = data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::NOOP_SINK,
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
            Some(noop_sink),
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

    fn is_write_sink_for_test(params: &crate::internal_service::TExecPlanFragmentParams) -> bool {
        super::is_write_sink(params)
    }

    #[allow(dead_code)]
    struct WriteRegistryTestGuard(std::sync::MutexGuard<'static, ()>);

    impl Drop for WriteRegistryTestGuard {
        fn drop(&mut self) {
            test_clear_registry();
        }
    }

    fn write_registry_test_guard() -> WriteRegistryTestGuard {
        static REGISTRY_TEST_LOCK: Mutex<()> = Mutex::new(());
        let guard = REGISTRY_TEST_LOCK
            .lock()
            .expect("coordinator write registry test lock");
        test_clear_registry();
        WriteRegistryTestGuard(guard)
    }

    fn id(hi: i64, lo: i64) -> types::TUniqueId {
        types::TUniqueId::new(hi, lo)
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
        let _guard = write_registry_test_guard();
        let query_id = id(710, 711);
        let writer = writer_key(710, 711, 712, 713, 0);
        let write =
            register_query(query_id.clone(), vec![writer.clone()]).expect("register writer");

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

        unregister_query(&query_id);
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
            std::thread::sleep(std::time::Duration::from_millis(50));
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
    }

    #[test]
    fn missing_write_final_report_after_root_eof_times_out_and_cancels() {
        let query_id = id(750, 751);
        let writer = writer_key(750, 751, 752, 753, 0);
        let write = Arc::new(Mutex::new(
            WriteCoordinator::new(query_id, vec![writer]).expect("write coordinator"),
        ));
        let inner = ControllableDispatcher::succeeds_always_eof();
        let dispatcher: Arc<dyn FragmentDispatcher> = inner.clone();
        let root_finst_id = types::TUniqueId::new(750, 1);
        let params = single_backend(vec![
            make_params_with_finst(750, 10),
            make_params_with_finst(750, 1),
        ]);
        let mut tracker = InFlightTracker::default();

        let err = submit_and_fetch_loop(
            &dispatcher,
            &mut tracker,
            params,
            0,
            root_finst_id,
            25,
            Some(&write),
        )
        .expect_err("missing writer final report after EOF must time out");

        assert!(err.contains("timed out"), "{err}");
        assert!(err.contains("missing writer final report"), "{err}");
        let cancelled = inner.cancelled_ids();
        assert_eq!(
            cancelled.len(),
            2,
            "missing final report timeout must cancel all submitted fragments"
        );
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
        assert_eq!(
            inner.submitted_ids().len(),
            2,
            "both fragments must be submitted"
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
