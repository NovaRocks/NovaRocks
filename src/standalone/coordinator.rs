//! Execution coordinator for multi-fragment standalone SQL execution.
//!
//! Wires and runs:
//! - CTE produce fragments (multicast to consumer exchange nodes)
//! - `Stream` / Gather producer fragments, including chains of edges, each with a
//!   multicast-style sink to the target fragment instance
//! - The root fragment in the foreground (result sink)
//!
//! All fragments run in-process against the local exchange server.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::data_sinks;
use crate::exec::expr::ExprArena;
use crate::runtime_filter;
use crate::sql::cascades::runtime_filter_planner::RuntimeFilterPlanResult;
use crate::exec::node::{ExecPlan, push_down_local_runtime_filters};
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::executor::execute_plan_with_pipeline;
use crate::lower::fragment::execute_fragment;
use crate::lower::layout::{build_tuple_slot_order, reorder_tuple_slots};
use crate::lower::thrift::lower_plan;
use crate::partitions;
use crate::planner;
use crate::runtime::runtime_state::RuntimeState;
use crate::sql::cte::CteId;
use crate::sql::fragment::FragmentId;
use crate::sql::physical::{
    FragmentBuildResult, FragmentEdge, FragmentEdgeKind, MultiFragmentBuildResult,
};
use crate::types;

use super::engine::{QueryResult, QueryResultColumn};

/// Coordinates multi-fragment CTE query execution.
///
/// Assigns fragment instance IDs, wires up multicast sinks for CTE produce
/// fragments, spawns CTE fragments in background threads, and executes the
/// root fragment in the foreground to collect results.
pub(crate) struct ExecutionCoordinator {
    build_result: MultiFragmentBuildResult,
    exchange_host: String,
    exchange_port: u16,
}

impl ExecutionCoordinator {
    pub(crate) fn new(
        build_result: MultiFragmentBuildResult,
        exchange_host: String,
        exchange_port: u16,
    ) -> Self {
        Self {
            build_result,
            exchange_host,
            exchange_port,
        }
    }

    pub(crate) fn execute(self) -> Result<QueryResult, String> {
        let MultiFragmentBuildResult {
            mut fragment_results,
            root_fragment_id,
            edges,
            rf_plan,
        } = self.build_result;
        let exchange_host = self.exchange_host;
        let exchange_port = self.exchange_port;

        // ---------------------------------------------------------------
        // 1. Assign fragment instance IDs
        // ---------------------------------------------------------------
        // Use a monotonically increasing base to ensure exchange keys are
        // unique across queries within the same process.
        use std::sync::atomic::{AtomicI64, Ordering};
        static NEXT_QUERY_BASE: AtomicI64 = AtomicI64::new(100);
        let query_base = NEXT_QUERY_BASE.fetch_add(1000, Ordering::Relaxed);

        let query_id_hi: i64 = query_base;
        let query_id_lo: i64 = 1;

        // Build a lookup: fragment_id -> (hi, lo) instance ID.
        let instance_map: BTreeMap<FragmentId, (i64, i64)> = fragment_results
            .iter()
            .map(|fr| (fr.fragment_id, (query_base, fr.fragment_id as i64 + 1)))
            .collect();

        // Find the root fragment instance ID
        let root_instance_id = *instance_map
            .get(&root_fragment_id)
            .ok_or_else(|| "root fragment not found in instance map".to_string())?;

        // Aggregate all fragment-local CTE consumer exchange nodes before
        // moving fragments into root/CTE buckets.
        let mut cte_consumers: BTreeMap<CteId, Vec<(FragmentId, i32)>> = BTreeMap::new();
        let mut per_fragment_exch_num_senders: BTreeMap<FragmentId, BTreeMap<i32, i32>> =
            BTreeMap::new();
        for fr in &fragment_results {
            for (cte_id, exchange_node_id) in &fr.cte_exchange_nodes {
                cte_consumers
                    .entry(*cte_id)
                    .or_default()
                    .push((fr.fragment_id, *exchange_node_id));
                per_fragment_exch_num_senders
                    .entry(fr.fragment_id)
                    .or_default()
                    .insert(*exchange_node_id, 1);
            }
        }

        // Every fragment exchange that receives a Gather `Stream` edge must record one sender.
        for e in &edges {
            if matches!(e.edge_kind, FragmentEdgeKind::Stream) {
                per_fragment_exch_num_senders
                    .entry(e.target_fragment_id)
                    .or_default()
                    .insert(e.target_exchange_node_id, 1);
            }
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
        // 2. Wire multicast sinks for CTE fragments
        // ---------------------------------------------------------------
        // For each CTE produce fragment, build TMultiCastDataStreamSink with
        // one TDataStreamSink per consumer exchange node across all fragments.
        let brpc_addr = types::TNetworkAddress::new(exchange_host, exchange_port as i32);

        // ---------------------------------------------------------------
        // Runtime filter parameter assembly
        // ---------------------------------------------------------------
        let rf_params = match rf_plan {
            Some(mut plan) => Some(setup_runtime_filter_params(
                &mut plan,
                &mut fragment_results,
                &instance_map,
                &brpc_addr,
            )),
            None => None,
        };

        // Separate root, CTE produce fragments, and Stream-edge producers (Gather splits).
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

        let mut cte_thrift_fragments: Vec<(
            FragmentBuildResult,
            planner::TPlanFragment,
            crate::internal_service::TPlanFragmentExecParams,
        )> = Vec::new();

        // Non-CTE fragments that are sources of `Stream` edges (Gather splits), including chains.
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

            let consumer_exchange_nodes =
                vec![(edge.target_fragment_id, edge.target_exchange_node_id)];

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
                    None::<bool>,     // ignore_not_found
                    None::<bool>,     // is_merge
                    None::<i32>,      // dest_dop
                    None::<Vec<i32>>, // output_columns
                    None::<i64>,      // limit
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
                    None::<types::TNetworkAddress>, // deprecated_server
                    Some(brpc_addr.clone()),        // brpc_server
                    None::<i32>,                    // pipeline_driver_sequence
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

            let producer_instance_id = *instance_map
                .get(&stream_fr.fragment_id)
                .ok_or_else(|| "Gather stream fragment instance ID not found".to_string())?;

            let thrift_fragment = planner::TPlanFragment::new(
                Some(stream_fr.plan.clone()),
                None::<Vec<crate::exprs::TExpr>>, // output_exprs
                Some(output_sink),
                partitions::TDataPartition::new(
                    partitions::TPartitionType::UNPARTITIONED,
                    None::<Vec<crate::exprs::TExpr>>,
                    None::<Vec<partitions::TRangePartition>>,
                    None::<Vec<partitions::TBucketProperty>>,
                ),
                None::<i64>, // min_reservation_bytes
                None::<i64>, // initial_reservation_total_claims
                None::<Vec<crate::data::TGlobalDict>>,
                None::<Vec<crate::data::TGlobalDict>>,
                None::<planner::TCacheParam>,
                None::<BTreeMap<i32, crate::exprs::TExpr>>,
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
            if let Some(ref rf) = rf_params {
                stream_exec_params.runtime_filter_params = Some(rf.clone());
            }

            cte_thrift_fragments.push((stream_fr, thrift_fragment, stream_exec_params));
        }

        for cte_fr in cte_fragments {
            let cte_id = cte_fr
                .cte_id
                .ok_or_else(|| "CTE fragment missing cte_id".to_string())?;

            let consumer_exchange_nodes = cte_consumers.get(&cte_id).cloned().unwrap_or_default();

            if consumer_exchange_nodes.is_empty() {
                return Err(format!("CTE fragment (cte_id={cte_id}) has no consumers"));
            }

            // Build one TDataStreamSink + destinations per consumer
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
                    None::<bool>,     // ignore_not_found
                    None::<bool>,     // is_merge
                    None::<i32>,      // dest_dop
                    None::<Vec<i32>>, // output_columns
                    None::<i64>,      // limit
                );
                sinks.push(stream_sink);

                let consumer_instance_id = *instance_map
                    .get(consumer_fragment_id)
                    .ok_or_else(|| {
                        format!(
                            "consumer fragment instance ID not found for fragment_id={consumer_fragment_id}"
                        )
                    })?;

                // Each sink has one destination: the owning fragment instance
                // for the exchange node that consumes this CTE output.
                let dest = data_sinks::TPlanFragmentDestination::new(
                    types::TUniqueId::new(consumer_instance_id.0, consumer_instance_id.1),
                    None::<types::TNetworkAddress>, // deprecated_server
                    Some(brpc_addr.clone()),        // brpc_server
                    None::<i32>,                    // pipeline_driver_sequence
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

            // Build TPlanFragment for the CTE fragment
            let thrift_fragment = planner::TPlanFragment::new(
                Some(cte_fr.plan.clone()),
                None::<Vec<crate::exprs::TExpr>>, // output_exprs
                Some(output_sink),
                partitions::TDataPartition::new(
                    partitions::TPartitionType::UNPARTITIONED,
                    None::<Vec<crate::exprs::TExpr>>,
                    None::<Vec<partitions::TRangePartition>>,
                    None::<Vec<partitions::TBucketProperty>>,
                ),
                None::<i64>, // min_reservation_bytes
                None::<i64>, // initial_reservation_total_claims
                None::<Vec<crate::data::TGlobalDict>>,
                None::<Vec<crate::data::TGlobalDict>>,
                None::<planner::TCacheParam>,
                None::<BTreeMap<i32, crate::exprs::TExpr>>,
                None::<planner::TGroupExecutionParam>,
            );

            // Build exec_params for the CTE fragment
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

            cte_thrift_fragments.push((cte_fr, thrift_fragment, cte_exec_params));
        }

        // ---------------------------------------------------------------
        // 3. Compute per_exch_num_senders for the root fragment
        // ---------------------------------------------------------------
        let per_exch_num_senders = per_fragment_exch_num_senders
            .get(&root_fragment.fragment_id)
            .cloned()
            .unwrap_or_default();

        // ---------------------------------------------------------------
        // 4. Spawn CTE fragments in background threads
        // ---------------------------------------------------------------
        let pipeline_dop = std::thread::available_parallelism()
            .map(|p| p.get().min(4))
            .unwrap_or(4) as i32;

        let mut cte_handles: Vec<std::thread::JoinHandle<Result<(), String>>> = Vec::new();

        for (cte_fr, thrift_fragment, cte_exec_params) in cte_thrift_fragments {
            let desc_tbl = cte_fr.desc_tbl.clone();
            let dop = pipeline_dop;
            let handle = std::thread::spawn(move || {
                let result = execute_fragment(
                    &thrift_fragment,
                    Some(&desc_tbl),
                    Some(&cte_exec_params),
                    None, // query_opts
                    None, // session_time_zone
                    dop,
                    None, // group_execution_scan_dop
                    None, // db_name
                    None, // profiler
                    None, // last_query_id
                    None, // fe_addr
                    None, // backend_num
                    None, // mem_tracker
                );
                result.map(|_| ())
            });
            cte_handles.push(handle);
        }

        // ---------------------------------------------------------------
        // 5. Execute root fragment in foreground (Option B: ResultSinkHandle)
        // ---------------------------------------------------------------
        let desc_tbl = root_fragment.desc_tbl.clone();
        let plan = root_fragment.plan.clone();
        let mut root_exec_params = root_fragment.exec_params.clone();
        root_exec_params.query_id = types::TUniqueId::new(query_id_hi, query_id_lo);
        root_exec_params.fragment_instance_id =
            types::TUniqueId::new(root_instance_id.0, root_instance_id.1);
        root_exec_params.per_exch_num_senders = per_exch_num_senders;
        if let Some(ref rf) = rf_params {
            root_exec_params.runtime_filter_params = Some(rf.clone());
        }

        let mut tuple_slots = build_tuple_slot_order(Some(&desc_tbl));
        reorder_tuple_slots(&mut tuple_slots, Some(&desc_tbl));
        let layout_hints = tuple_slots.clone();

        let mut arena = ExprArena::default();
        let connectors = crate::connector::ConnectorRegistry::default();
        let lowered = lower_plan(
            &plan,
            &mut arena,
            &tuple_slots,
            Some(&desc_tbl),
            None, // query_global_dicts
            None, // query_global_dict_exprs
            Some(&root_exec_params),
            None, // query_opts
            None, // db_name
            &connectors,
            &layout_hints,
            None, // last_query_id
            None, // fe_addr
        )?;

        let mut exec_plan = ExecPlan {
            arena,
            root: lowered.node,
        };
        push_down_local_runtime_filters(&mut exec_plan.root, &exec_plan.arena);

        let handle = ResultSinkHandle::new();
        let exchange_finst_id = Some((root_instance_id.0, root_instance_id.1));

        let root_query_id = Some(crate::runtime::query_context::QueryId {
            hi: query_id_hi,
            lo: query_id_lo,
        });
        let root_finst_id = Some(crate::common::types::UniqueId {
            hi: root_instance_id.0,
            lo: root_instance_id.1,
        });
        let root_runtime_state = Arc::new(RuntimeState::new(
            None, // query_options
            None, // cache_options
            root_query_id,
            root_exec_params.runtime_filter_params.clone(),
            root_finst_id,
            None, // backend_num
            None, // mem_tracker
            None, // spill_config
            None, // spill_manager
        ));

        execute_plan_with_pipeline(
            exec_plan,
            false,
            std::time::Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            exchange_finst_id,
            None, // profiler
            pipeline_dop,
            root_runtime_state,
            root_query_id,
            None, // fe_addr
            None, // backend_num
        )?;

        // ---------------------------------------------------------------
        // 6. Wait for background producer threads (Gather stream + CTE) to complete
        // ---------------------------------------------------------------
        for jh in cte_handles {
            match jh.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(format!("CTE fragment execution failed: {e}")),
                Err(panic_payload) => {
                    let msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic".to_string()
                    };
                    return Err(format!("CTE fragment thread panicked: {msg}"));
                }
            }
        }

        // ---------------------------------------------------------------
        // 7. Collect results
        // ---------------------------------------------------------------
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
            chunks: handle.take_chunks(),
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

    // Populate prober params for each probe-side filter.
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

    // Builder number: always 1 in standalone (single instance per fragment).
    for (_, filter_ids) in &rf_plan.build_side_filters {
        for fid in filter_ids {
            builder_number.insert(*fid, 1);
        }
    }

    // Patch merge node addresses on remote filter descriptions.
    for (_filter_id, desc) in rf_plan.all_filters.iter_mut() {
        if desc.has_remote_targets == Some(true) {
            desc.runtime_filter_merge_nodes = Some(vec![exchange_addr.clone()]);
        }
    }
    // Also patch inside the plan nodes.
    for fr in fragment_results.iter_mut() {
        for node in fr.plan.nodes.iter_mut() {
            if let Some(ref mut hj) = node.hash_join_node {
                if let Some(ref mut rf_descs) = hj.build_runtime_filters {
                    for desc in rf_descs.iter_mut() {
                        if desc.has_remote_targets == Some(true) {
                            desc.runtime_filter_merge_nodes =
                                Some(vec![exchange_addr.clone()]);
                        }
                    }
                }
            }
        }
    }

    runtime_filter::TRuntimeFilterParams::new(
        id_to_prober_params,
        builder_number,
        16_i64 * 1024 * 1024, // runtime_filter_max_size: 16MB default
        None::<std::collections::BTreeSet<i32>>,
    )
}
