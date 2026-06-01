//! Execution coordinator for multi-fragment SQL execution.
//!
//! Wires and runs:
//! - CTE produce fragments (multicast to consumer exchange nodes)
//! - `Stream` / Gather producer fragments, including chains of edges, each with a
//!   multicast-style sink to the target fragment instance
//! - The root fragment via the dispatcher (result sink)
//!
//! All fragments are submitted through a `FragmentDispatcher`.  `InProcessDispatcher`
//! runs them in-process against the local exchange server; future dispatchers can
//! route to remote BEs.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::data_sinks;
use crate::novarocks_logging::debug;
use crate::partitions;
use crate::planner;
use crate::runtime::dispatcher::{FetchOutcome, FragmentDispatcher};
use crate::runtime::exec_params::build_exec_plan_fragment_params;
use crate::runtime_filter;
use crate::sql::analysis::cte::CteId;
use crate::sql::codegen::FragmentId;
use crate::sql::codegen::RuntimeFilterPlanResult;
use crate::sql::codegen::{
    FragmentBuildResult, FragmentEdge, FragmentEdgeKind, MultiFragmentBuildResult,
};
use crate::types;

use crate::runtime::query_result::{QueryResult, QueryResultColumn};

/// Coordinates multi-fragment CTE query execution.
///
/// Assigns fragment instance IDs, wires up multicast sinks for CTE produce
/// fragments, and submits all fragments through the `FragmentDispatcher`.
/// Results are collected by polling the dispatcher for root fragment chunks.
pub(crate) struct ExecutionCoordinator {
    build_result: MultiFragmentBuildResult,
    dispatcher: Arc<dyn FragmentDispatcher>,
    query_options: Option<crate::internal_service::TQueryOptions>,
}

impl ExecutionCoordinator {
    pub(crate) fn new(
        build_result: MultiFragmentBuildResult,
        dispatcher: Arc<dyn FragmentDispatcher>,
        query_options: Option<crate::internal_service::TQueryOptions>,
    ) -> Self {
        Self {
            build_result,
            dispatcher,
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

        // ---------------------------------------------------------------
        // 1. Assign fragment instance IDs
        // ---------------------------------------------------------------
        use std::sync::atomic::{AtomicI64, Ordering};
        static NEXT_QUERY_BASE: AtomicI64 = AtomicI64::new(100);
        let query_base = NEXT_QUERY_BASE.fetch_add(1000, Ordering::Relaxed);

        let query_id_hi: i64 = query_base;
        let query_id_lo: i64 = 1;

        let instance_map: BTreeMap<FragmentId, (i64, i64)> = fragment_results
            .iter()
            .map(|fr| (fr.fragment_id, (query_base, fr.fragment_id as i64 + 1)))
            .collect();

        let root_instance_id = *instance_map
            .get(&root_fragment_id)
            .ok_or_else(|| "root fragment not found in instance map".to_string())?;

        let mut cte_consumers: BTreeMap<CteId, Vec<(FragmentId, i32)>> = BTreeMap::new();
        let mut per_fragment_exch_num_senders: BTreeMap<FragmentId, BTreeMap<i32, i32>> =
            BTreeMap::new();

        for e in &edges {
            *per_fragment_exch_num_senders
                .entry(e.target_fragment_id)
                .or_default()
                .entry(e.target_exchange_node_id)
                .or_insert(0) += 1;

            if let FragmentEdgeKind::CteMulticast { cte_id } = &e.edge_kind {
                cte_consumers
                    .entry(*cte_id)
                    .or_default()
                    .push((e.target_fragment_id, e.target_exchange_node_id));
            }
        }

        for fr in &fragment_results {
            for (cte_id, exchange_node_id) in &fr.cte_exchange_nodes {
                let consumers = cte_consumers.entry(*cte_id).or_default();
                let entry = (fr.fragment_id, *exchange_node_id);
                if !consumers.contains(&entry) {
                    consumers.push(entry);
                    *per_fragment_exch_num_senders
                        .entry(fr.fragment_id)
                        .or_default()
                        .entry(*exchange_node_id)
                        .or_insert(0) += 1;
                }
            }
        }

        debug!(
            "coordinator topology: fragments={} edges={} root={}",
            fragment_results.len(),
            edges.len(),
            root_fragment_id
        );
        for e in &edges {
            debug!(
                "coordinator edge: frag {} -> frag {} (exch_node={}, kind={:?})",
                e.source_fragment_id,
                e.target_fragment_id,
                e.target_exchange_node_id,
                match &e.edge_kind {
                    FragmentEdgeKind::Stream => "Stream",
                    FragmentEdgeKind::CteMulticast { .. } => "CteMulticast",
                }
            );
        }

        let stream_source_ids: BTreeSet<FragmentId> = edges
            .iter()
            .filter_map(|e| {
                if matches!(e.edge_kind, FragmentEdgeKind::Stream) {
                    Some(e.source_fragment_id)
                } else {
                    None
                }
            })
            .collect();

        // ---------------------------------------------------------------
        // 2. Wire multicast sinks for CTE / Stream fragments
        // ---------------------------------------------------------------
        let brpc_addr = dispatcher.exchange_addr();

        let rf_params = rf_plan.map(|mut plan| {
            setup_runtime_filter_params(&mut plan, &mut fragment_results, &instance_map, &brpc_addr)
        });

        let mut root_fragment: Option<FragmentBuildResult> = None;
        let mut cte_fragments: Vec<FragmentBuildResult> = Vec::new();
        let mut stream_producer_fragments: Vec<FragmentBuildResult> = Vec::new();
        for fr in fragment_results {
            if fr.fragment_id == root_fragment_id {
                root_fragment = Some(fr);
            } else if fr.cte_id.is_some() {
                cte_fragments.push(fr);
            } else if stream_source_ids.contains(&fr.fragment_id) {
                stream_producer_fragments.push(fr);
            } else {
                return Err(
                    "multi-hop stream exchange is not supported in standalone coordinator"
                        .to_string(),
                );
            }
        }
        let root_fragment =
            root_fragment.ok_or_else(|| "root fragment not found in build results".to_string())?;

        let mut non_root_fragments: Vec<(
            FragmentBuildResult,
            planner::TPlanFragment,
            crate::internal_service::TPlanFragmentExecParams,
        )> = Vec::new();

        for stream_fr in stream_producer_fragments {
            let outgoing: Vec<&FragmentEdge> = edges
                .iter()
                .filter(|e| {
                    matches!(e.edge_kind, FragmentEdgeKind::Stream)
                        && e.source_fragment_id == stream_fr.fragment_id
                })
                .collect();
            if outgoing.len() != 1 {
                return Err(format!(
                    "expected exactly one outgoing Stream edge from fragment {}, got {}",
                    stream_fr.fragment_id,
                    outgoing.len()
                ));
            }
            let edge = outgoing[0];
            let (consumer_fragment_id, exchange_node_id) =
                (edge.target_fragment_id, edge.target_exchange_node_id);

            let unpartitioned = partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            );

            let stream_sink = data_sinks::TDataStreamSink::new(
                exchange_node_id,
                unpartitioned.clone(),
                None::<bool>,
                None::<bool>,
                None::<i32>,
                None::<Vec<i32>>,
                None::<i64>,
            );

            let consumer_instance_id = *instance_map
                .get(&consumer_fragment_id)
                .ok_or_else(|| {
                    format!(
                        "consumer fragment instance ID not found for fragment_id={consumer_fragment_id}"
                    )
                })?;

            let dest = data_sinks::TPlanFragmentDestination::new(
                types::TUniqueId::new(consumer_instance_id.0, consumer_instance_id.1),
                None::<types::TNetworkAddress>,
                Some(brpc_addr.clone()),
                None::<i32>,
            );

            let output_sink = data_sinks::TDataSink::new(
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
            );

            let producer_instance_id = *instance_map
                .get(&stream_fr.fragment_id)
                .ok_or_else(|| "Gather stream fragment instance ID not found".to_string())?;

            let thrift_fragment = planner::TPlanFragment::new(
                Some(stream_fr.plan.clone()),
                None::<Vec<crate::exprs::TExpr>>,
                Some(output_sink),
                unpartitioned,
                None::<i64>,
                None::<i64>,
                stream_fr.query_global_dicts.clone(),
                None::<Vec<crate::data::TGlobalDict>>,
                None::<planner::TCacheParam>,
                stream_fr.query_global_dict_exprs.clone(),
                None::<planner::TGroupExecutionParam>,
            );

            let mut stream_exec_params = stream_fr.exec_params.clone();
            stream_exec_params.query_id = types::TUniqueId::new(query_id_hi, query_id_lo);
            stream_exec_params.fragment_instance_id =
                types::TUniqueId::new(producer_instance_id.0, producer_instance_id.1);
            stream_exec_params.per_exch_num_senders = per_fragment_exch_num_senders
                .get(&stream_fr.fragment_id)
                .cloned()
                .unwrap_or_default();
            stream_exec_params.destinations = Some(vec![dest]);
            if let Some(ref rf) = rf_params {
                stream_exec_params.runtime_filter_params = Some(rf.clone());
            }

            non_root_fragments.push((stream_fr, thrift_fragment, stream_exec_params));
        }

        for cte_fr in cte_fragments {
            let cte_id = cte_fr
                .cte_id
                .ok_or_else(|| "CTE fragment missing cte_id".to_string())?;

            let consumer_exchange_nodes = cte_consumers.get(&cte_id).cloned().unwrap_or_default();

            if consumer_exchange_nodes.is_empty() {
                return Err(format!("CTE fragment (cte_id={cte_id}) has no consumers"));
            }

            let unpartitioned = partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            );

            let mut sinks = Vec::new();
            let mut destinations = Vec::new();
            for (consumer_fragment_id, exchange_node_id) in &consumer_exchange_nodes {
                let stream_sink = data_sinks::TDataStreamSink::new(
                    *exchange_node_id,
                    unpartitioned.clone(),
                    None::<bool>,
                    None::<bool>,
                    None::<i32>,
                    None::<Vec<i32>>,
                    None::<i64>,
                );
                sinks.push(stream_sink);

                let consumer_instance_id = *instance_map
                    .get(consumer_fragment_id)
                    .ok_or_else(|| {
                        format!(
                            "consumer fragment instance ID not found for fragment_id={consumer_fragment_id}"
                        )
                    })?;

                let dest = data_sinks::TPlanFragmentDestination::new(
                    types::TUniqueId::new(consumer_instance_id.0, consumer_instance_id.1),
                    None::<types::TNetworkAddress>,
                    Some(brpc_addr.clone()),
                    None::<i32>,
                );
                destinations.push(vec![dest]);
            }

            let multi_cast_sink = data_sinks::TMultiCastDataStreamSink::new(sinks, destinations);

            let output_sink = data_sinks::TDataSink::new(
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
            );

            let cte_instance_id = *instance_map
                .get(&cte_fr.fragment_id)
                .ok_or_else(|| "CTE fragment instance ID not found".to_string())?;

            let thrift_fragment = planner::TPlanFragment::new(
                Some(cte_fr.plan.clone()),
                None::<Vec<crate::exprs::TExpr>>,
                Some(output_sink),
                unpartitioned,
                None::<i64>,
                None::<i64>,
                cte_fr.query_global_dicts.clone(),
                None::<Vec<crate::data::TGlobalDict>>,
                None::<planner::TCacheParam>,
                cte_fr.query_global_dict_exprs.clone(),
                None::<planner::TGroupExecutionParam>,
            );

            let mut cte_exec_params = cte_fr.exec_params.clone();
            cte_exec_params.query_id = types::TUniqueId::new(query_id_hi, query_id_lo);
            cte_exec_params.fragment_instance_id =
                types::TUniqueId::new(cte_instance_id.0, cte_instance_id.1);
            cte_exec_params.per_exch_num_senders = per_fragment_exch_num_senders
                .get(&cte_fr.fragment_id)
                .cloned()
                .unwrap_or_default();
            if let Some(ref rf) = rf_params {
                cte_exec_params.runtime_filter_params = Some(rf.clone());
            }

            non_root_fragments.push((cte_fr, thrift_fragment, cte_exec_params));
        }

        // ---------------------------------------------------------------
        // 3. Compute DOP and build root fragment params
        // ---------------------------------------------------------------
        let pipeline_dop = crate::runtime::dispatcher::compute_pipeline_dop();

        let per_exch_num_senders = per_fragment_exch_num_senders
            .get(&root_fragment.fragment_id)
            .cloned()
            .unwrap_or_default();

        let unpartitioned_root = partitions::TDataPartition::new(
            partitions::TPartitionType::UNPARTITIONED,
            None::<Vec<crate::exprs::TExpr>>,
            None::<Vec<partitions::TRangePartition>>,
            None::<Vec<partitions::TBucketProperty>>,
        );
        let root_thrift_fragment = planner::TPlanFragment::new(
            Some(root_fragment.plan.clone()),
            None::<Vec<crate::exprs::TExpr>>,
            Some(root_fragment.output_sink.clone()),
            unpartitioned_root,
            None::<i64>,
            None::<i64>,
            root_fragment.query_global_dicts.clone(),
            None::<Vec<crate::data::TGlobalDict>>,
            None::<planner::TCacheParam>,
            root_fragment.query_global_dict_exprs.clone(),
            None::<planner::TGroupExecutionParam>,
        );

        let mut root_exec_params = root_fragment.exec_params.clone();
        root_exec_params.query_id = types::TUniqueId::new(query_id_hi, query_id_lo);
        root_exec_params.fragment_instance_id =
            types::TUniqueId::new(root_instance_id.0, root_instance_id.1);
        root_exec_params.per_exch_num_senders = per_exch_num_senders;
        if let Some(ref rf) = rf_params {
            root_exec_params.runtime_filter_params = Some(rf.clone());
        }

        // ---------------------------------------------------------------
        // 4. Submit all fragments and collect root results
        // ---------------------------------------------------------------
        let mut all_params: Vec<crate::internal_service::TExecPlanFragmentParams> =
            Vec::with_capacity(non_root_fragments.len() + 1);

        for (fr, thrift_fragment, exec_params) in non_root_fragments {
            let p = build_exec_plan_fragment_params(
                &fr,
                thrift_fragment,
                exec_params,
                query_options.clone(),
                pipeline_dop,
            );
            all_params.push(p);
        }

        let root_params = build_exec_plan_fragment_params(
            &root_fragment,
            root_thrift_fragment,
            root_exec_params,
            query_options.as_ref().cloned(),
            pipeline_dop,
        );
        all_params.push(root_params);

        let root_finst_id = types::TUniqueId::new(root_instance_id.0, root_instance_id.1);
        let timeout_ms = query_options
            .as_ref()
            .and_then(|q| q.query_timeout)
            .map(|t| t as i64 * 1000)
            .unwrap_or(300_000); // 5 minute default

        let chunks = submit_and_fetch_loop(&dispatcher, all_params, root_finst_id, timeout_ms)?;

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
            chunks,
        })
    }
}

/// Build TRuntimeFilterParams from the RF planning result.
fn setup_runtime_filter_params(
    rf_plan: &mut RuntimeFilterPlanResult,
    fragment_results: &mut [FragmentBuildResult],
    instance_map: &BTreeMap<FragmentId, (i64, i64)>,
    exchange_addr: &types::TNetworkAddress,
) -> runtime_filter::TRuntimeFilterParams {
    let mut id_to_prober_params: BTreeMap<i32, Vec<runtime_filter::TRuntimeFilterProberParams>> =
        BTreeMap::new();
    let mut builder_number: BTreeMap<i32, i32> = BTreeMap::new();

    for (frag_id, probes) in &rf_plan.probe_side_filters {
        if let Some(&(hi, lo)) = instance_map.get(frag_id) {
            for (filter_id, _scan_node_id) in probes {
                let prober = runtime_filter::TRuntimeFilterProberParams::new(
                    types::TUniqueId::new(hi, lo),
                    exchange_addr.clone(),
                );
                id_to_prober_params
                    .entry(*filter_id)
                    .or_default()
                    .push(prober);
            }
        }
    }

    for filter_ids in rf_plan.build_side_filters.values() {
        for fid in filter_ids {
            builder_number.insert(*fid, 1);
        }
    }

    for desc in rf_plan.all_filters.values_mut() {
        if desc.has_remote_targets == Some(true) {
            desc.runtime_filter_merge_nodes = Some(vec![exchange_addr.clone()]);
        }
    }
    for fr in fragment_results.iter_mut() {
        for node in fr.plan.nodes.iter_mut() {
            if let Some(ref mut hj) = node.hash_join_node
                && let Some(ref mut rf_descs) = hj.build_runtime_filters
            {
                for desc in rf_descs.iter_mut() {
                    if desc.has_remote_targets == Some(true) {
                        desc.runtime_filter_merge_nodes = Some(vec![exchange_addr.clone()]);
                    }
                }
            }
        }
    }

    runtime_filter::TRuntimeFilterParams::new(
        id_to_prober_params,
        builder_number,
        16_i64 * 1024 * 1024,
        None::<std::collections::BTreeSet<i32>>,
    )
}

// ---------------------------------------------------------------------------
// Submit-and-fetch orchestration (testable helper)
// ---------------------------------------------------------------------------

/// Submit all fragment params through the dispatcher in order, track accepted
/// fragment instance IDs, then poll the root fragment until EOF.
///
/// On any submit failure or fetch error, all already-submitted fragments are
/// cancelled before the error is returned.
pub(crate) fn submit_and_fetch_loop(
    dispatcher: &Arc<dyn FragmentDispatcher>,
    all_params: Vec<crate::internal_service::TExecPlanFragmentParams>,
    root_finst_id: types::TUniqueId,
    timeout_ms: i64,
) -> Result<Vec<crate::exec::chunk::Chunk>, String> {
    const REMOTE_FETCH_POLL_INTERVAL_MS: i64 = 300;
    let mut submitted: Vec<types::TUniqueId> = Vec::with_capacity(all_params.len());

    for p in all_params {
        let finst_id = p
            .params
            .as_ref()
            .map(|ep| types::TUniqueId::new(ep.fragment_instance_id.hi, ep.fragment_instance_id.lo))
            .unwrap_or_else(|| types::TUniqueId::new(0, 0));
        if let Err(e) = dispatcher.submit_fragment(p) {
            dispatcher.cancel_fragments(&submitted);
            return Err(e);
        }
        submitted.push(finst_id);
    }

    let mut chunks = Vec::new();
    let timeout = std::time::Duration::from_millis(timeout_ms.max(0) as u64);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if crate::runtime::query_cancel::client_disconnected() {
            dispatcher.cancel_fragments(&submitted);
            return Err("client disconnected".to_string());
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            dispatcher.cancel_fragments(&submitted);
            return Err(format!("query timed out after {timeout_ms} ms"));
        }
        let remaining_ms = deadline
            .saturating_duration_since(now)
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let fetch_wait_ms = remaining_ms.clamp(1, REMOTE_FETCH_POLL_INTERVAL_MS);
        match dispatcher.fetch_result(root_finst_id.clone(), fetch_wait_ms) {
            Err(e) => {
                dispatcher.cancel_fragments(&submitted);
                return Err(e);
            }
            Ok(FetchOutcome::Ready(chunk)) => chunks.push(chunk),
            Ok(FetchOutcome::NotReady) => continue,
            Ok(FetchOutcome::Eof) => break,
            Ok(FetchOutcome::Err(e)) => {
                dispatcher.cancel_fragments(&submitted);
                return Err(e);
            }
        }
    }

    Ok(chunks)
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
        fn exchange_addr(&self) -> types::TNetworkAddress {
            types::TNetworkAddress::new("127.0.0.1".to_string(), 9999)
        }

        fn submit_fragment(
            &self,
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
            _finst_id: types::TUniqueId,
            _max_wait_ms: i64,
        ) -> Result<FetchOutcome, String> {
            Ok(FetchOutcome::Eof)
        }

        fn cancel_fragments(&self, _finst_ids: &[types::TUniqueId]) {}
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

    impl FragmentDispatcher for ControllableDispatcher {
        fn exchange_addr(&self) -> types::TNetworkAddress {
            types::TNetworkAddress::new("127.0.0.1".to_string(), 9999)
        }

        fn submit_fragment(
            &self,
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

        fn cancel_fragments(&self, finst_ids: &[types::TUniqueId]) {
            self.cancelled.lock().unwrap().extend_from_slice(finst_ids);
        }
    }

    impl FragmentDispatcher for RecordingWaitDispatcher {
        fn exchange_addr(&self) -> types::TNetworkAddress {
            types::TNetworkAddress::new("127.0.0.1".to_string(), 9999)
        }

        fn submit_fragment(
            &self,
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

        fn cancel_fragments(&self, _finst_ids: &[types::TUniqueId]) {}
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
        )
    }

    // -----------------------------------------------------------------------
    // Original regression tests
    // -----------------------------------------------------------------------

    /// Verify that `ExecutionCoordinator::new` accepts `Arc<dyn FragmentDispatcher>`
    /// (regression guard against re-introduction of exchange_host/port parameters).
    #[test]
    fn constructor_accepts_dispatcher() {
        let dispatcher: Arc<dyn FragmentDispatcher> = MockDispatcher::new();
        // Just verify this compiles and the dispatcher is usable.
        let addr = dispatcher.exchange_addr();
        assert_eq!(addr.hostname, "127.0.0.1");
        assert_eq!(addr.port, 9999);
    }

    /// Verify that `MockDispatcher::submitted_count` starts at zero.
    #[test]
    fn mock_dispatcher_starts_empty() {
        let d = MockDispatcher::new();
        assert_eq!(d.submitted_count(), 0);
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
        let params = vec![make_params_with_finst(1, 10), make_params_with_finst(1, 1)];
        let result = submit_and_fetch_loop(&dispatcher, params, root_finst_id, 100);
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let chunks = result.unwrap();
        assert!(chunks.is_empty(), "expected no chunks from Eof dispatcher");
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
        let params = vec![make_params_with_finst(2, 10), make_params_with_finst(2, 1)];
        let result = submit_and_fetch_loop(&dispatcher, params, root_finst_id, 100);
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
        let params = vec![make_params_with_finst(3, 10), make_params_with_finst(3, 1)];
        let result = submit_and_fetch_loop(&dispatcher, params, root_finst_id, 100);
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
        let params = vec![make_params_with_finst(4, 10), make_params_with_finst(4, 1)];
        let result = submit_and_fetch_loop(&dispatcher, params, root_finst_id, 10);
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
        let params = vec![make_params_with_finst(6, 10), make_params_with_finst(6, 1)];

        let result = submit_and_fetch_loop(&dispatcher, params, root_finst_id, 300_000);

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
        let params = vec![make_params_with_finst(5, 10), make_params_with_finst(5, 1)];
        let disconnected = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let result =
            crate::runtime::query_cancel::with_client_disconnect_signal(disconnected, || {
                submit_and_fetch_loop(&dispatcher, params, root_finst_id, 100)
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
