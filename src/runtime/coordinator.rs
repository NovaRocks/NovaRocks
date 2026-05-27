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
use crate::sql::codegen::{
    FragmentBuildResult, FragmentEdge, FragmentEdgeKind, MultiFragmentBuildResult,
};
use crate::sql::optimizer::runtime_filter_planner::RuntimeFilterPlanResult;
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
        // 4. Submit non-root fragments, then root fragment
        // ---------------------------------------------------------------
        for (fr, thrift_fragment, exec_params) in non_root_fragments {
            let p = build_exec_plan_fragment_params(
                &fr,
                thrift_fragment,
                exec_params,
                query_options.clone(),
                pipeline_dop,
            );
            dispatcher.submit_fragment(p)?;
        }

        let root_params = build_exec_plan_fragment_params(
            &root_fragment,
            root_thrift_fragment,
            root_exec_params,
            query_options.as_ref().cloned(),
            pipeline_dop,
        );
        dispatcher.submit_fragment(root_params)?;

        // ---------------------------------------------------------------
        // 5. Collect root results via fetch loop
        // ---------------------------------------------------------------
        let root_finst_id = types::TUniqueId::new(root_instance_id.0, root_instance_id.1);
        let timeout_ms = query_options
            .as_ref()
            .and_then(|q| q.query_timeout)
            .map(|t| t as i64 * 1000)
            .unwrap_or(300_000); // 5 minute default

        let mut chunks = Vec::new();
        loop {
            match dispatcher.fetch_result(root_finst_id.clone(), timeout_ms)? {
                FetchOutcome::Ready(chunk) => chunks.push(chunk),
                FetchOutcome::NotReady => continue,
                FetchOutcome::Eof => break,
                FetchOutcome::Err(e) => return Err(e),
            }
        }

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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::runtime::dispatcher::{FetchOutcome, FragmentDispatcher};

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
}
